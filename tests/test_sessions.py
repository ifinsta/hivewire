"""Managed session views against disposable Git boards and actual owned workers."""

from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest import mock

from test_hardening import HardeningCase, core
from test_processes import ProcessCase

import swarm_sessions  # noqa: E402


class SessionViews(HardeningCase):
    def setUp(self):
        super().setUp()
        self.join("ann")
        self.execution_id = "a" * 32
        with self.board() as sw, sw.tx():
            sw.db.execute(
                "INSERT INTO executions(id,agent,task,epoch,state,started_at,exit_code,policy,output) VALUES(?,?,?,?,?,?,?,?,?)",
                (self.execution_id, "ann", "T1", 1, "failed", core.iso(), 7, '{}', json.dumps({"path": str(self.root / "secret")})),
            )
            self.log = sw.dir / "logs" / (self.execution_id + ".log")
        self.log.write_text("first line\nworker evidence\n", encoding="utf-8")

    def test_ec345_closed_sessions_stay_visible_with_exit_and_task_audit(self):
        data = self.json_command("sessions", "--once", "--json")["result"]
        self.assertEqual(data["sessions"][0]["id"], self.execution_id)
        self.assertEqual(data["sessions"][0]["exit_code"], 7)
        self.assertEqual(data["sessions"][0]["state"], "failed")
        with self.board() as sw:
            before = sw.db.total_changes
            details = swarm_sessions.detail(sw.db, sw.dir, self.execution_id)
            self.assertEqual(details["task"]["id"], "T1")
            self.assertEqual(details["worker"]["text"], "first line\nworker evidence")
            self.assertEqual(sw.db.total_changes, before, "session snapshots do not mutate the board")

    def test_ec346_log_excerpt_is_bounded_and_follow_cursors_do_not_repeat(self):
        self.log.write_bytes(b"ignored prefix\n" * 10000 + b"final evidence\n")
        with self.board() as sw:
            capture = swarm_sessions.log_tail(sw.dir, self.execution_id, lines=2)
            self.assertEqual(len(capture["text"].splitlines()), 2)
            self.assertLessEqual(len(capture["text"]), swarm_sessions.MAX_READ)
            self.assertTrue(capture["truncated"])
            cursor = capture["next_offset"]
            with self.log.open("ab") as fh:
                fh.write(b"next evidence\n")
            second = swarm_sessions.log_tail(sw.dir, self.execution_id, after_bytes=cursor)
            self.assertEqual(second["text"], "next evidence")
            self.assertEqual(swarm_sessions.log_tail(sw.dir, self.execution_id, after_bytes=second["next_offset"])["text"], "")
        observed = self.json_command("execution", "logs", self.execution_id, "--lines", "1", "--json")["result"]
        self.assertEqual(observed["text"], "next evidence")

    def test_ec346_forged_paths_and_terminal_escape_sequences_do_not_escape_the_view(self):
        (self.root / "secret").write_text("private file must never be served", encoding="utf-8")
        self.log.write_text("\x1b[2Jclear\x1b]52;c;payload\x07\napi_key=supersecret\n", encoding="utf-8")
        result = self.json_command("execution", "logs", self.execution_id, "--json")["result"]
        self.assertNotIn("private file", result["text"])
        self.assertNotIn("\x1b", result["text"])
        self.assertNotIn("\x07", result["text"])
        self.assertNotIn("supersecret", result["text"])
        with self.board() as sw:
            self.assertFalse(swarm_sessions.log_tail(sw.dir, "../../secret")["available"])
            details = swarm_sessions.detail(sw.db, sw.dir, self.execution_id)
            screen = swarm_sessions.render(swarm_sessions.listing(sw.db), 0, details, width=48, height=12)
        self.assertLessEqual(len(screen.splitlines()), 12)
        self.assertTrue(all(len(line) <= 48 for line in screen.splitlines()))
        self.assertNotIn("\x1b", screen)

    def test_ec346_linked_captures_are_not_served(self):
        secret = self.root / "secret"
        secret.write_text("do not serve", encoding="utf-8")
        self.log.unlink()
        try:
            self.log.symlink_to(secret)
        except OSError:
            self.skipTest("creating file symlinks requires host permission")
        with self.board() as sw:
            result = swarm_sessions.log_tail(sw.dir, self.execution_id)
        self.assertFalse(result["available"])
        self.assertNotIn("do not serve", result["text"])

    def test_ec348_start_prompt_lists_installed_clis_and_accepts_defaults(self):
        installed = lambda exe: "/bin/" + exe if exe in ("claude", "codex") else None  # noqa: E731
        with self.board() as sw, mock.patch.object(core.shutil, "which", installed):
            self.assertEqual(core.spawnable_kinds(sw), ["claude", "codex"])
            with mock.patch("builtins.input", side_effect=["", "", ""]), redirect_stdout(io.StringIO()) as shown:
                self.assertEqual(core.ask_worker_start(sw), ("claude", "", "claude-1"))
            self.assertIn("1) claude", shown.getvalue())
            self.assertIn("2) codex", shown.getvalue())
            # A wrong kind, a malformed name and a taken name each re-ask instead of aborting the viewer.
            answers = ["gemini", "2", "T1", "Bad Name", "ann", "my-worker"]
            with mock.patch("builtins.input", side_effect=answers), redirect_stdout(io.StringIO()) as shown:
                self.assertEqual(core.ask_worker_start(sw), ("codex", "T1", "my-worker"))
            self.assertIn("is not installed here", shown.getvalue())
            self.assertIn("Names start with a letter", shown.getvalue())
            self.assertIn("already in the pool", shown.getvalue())

    def test_ec348_start_prompt_says_what_to_do_when_no_cli_is_installed(self):
        with self.board() as sw, mock.patch.object(core.shutil, "which", lambda exe: None):
            with self.assertRaises(core.SwarmError) as refused:
                core.ask_worker_start(sw)
            self.assertIn("hivewire probe", str(refused.exception))

    def test_ec348_empty_viewer_names_the_real_command(self):
        text = swarm_sessions.render({"sessions": []}, 0, None, width=120, height=12)
        self.assertIn("HIVEWIRE SESSIONS", text)
        self.assertIn("hivewire spawn CLI --name NAME --task TASK", text)
        self.assertNotIn("swarm spawn", text)

    def test_ec349_agents_hides_offline_agents_that_hold_nothing_until_all(self):
        for name in ("gone", "owner", "busy"):
            self.join(name)
        with self.board() as sw, sw.tx():
            for name in ("gone", "owner", "busy"):
                sw.update_agent(name, state="offline", offline_at=core.iso(), offline_reason="silent")
            sw.update_agent("owner", task="T1")
            sw.db.execute(
                "INSERT INTO executions(id,agent,task,epoch,state,started_at,policy) VALUES(?,?,?,?,?,?,?)",
                ("b" * 32, "busy", "T1", 1, "unknown", core.iso(), "{}"),
            )
            self.assertEqual(core.retired_agents(sw), {"gone"})
        shown = self.sw("agents")
        self.assertNotIn("\n gone ", shown)
        self.assertIn("owner", shown)
        self.assertIn("busy", shown)
        self.assertIn("ann", shown)
        self.assertIn("1 offline agents holding nothing are hidden; `hivewire agents --all`", shown)
        everything = self.sw("agents", "--all")
        self.assertIn("\n gone ", everything)
        self.assertNotIn("are hidden", everything)

    def test_ec350_prune_archives_only_long_gone_agents_that_hold_nothing_and_keeps_history(self):
        for name in ("gone", "recent", "owner"):
            self.join(name)
        long_ago = core.iso(core.utcnow() - core.timedelta(minutes=500))
        with self.board() as sw, sw.tx():
            for name in ("gone", "recent", "owner"):
                sw.update_agent(name, state="offline", offline_at=core.iso(), offline_reason="silent")
            sw.update_agent("gone", offline_at=long_ago)
            # Timestamps have one-second resolution; ensure strictly positive age.
            sw.update_agent("recent", offline_at=core.iso(core.utcnow() - core.timedelta(seconds=2)))
            sw.update_agent("owner", offline_at=long_ago, task="T1")
        # A recently silent agent may still resume, and an owner of unfinished work is never archived.
        self.assertIn("would archive 1 agent(s)", self.sw("agents", "--prune", "--dry-run"))
        self.assertIn("gone", self.sw("agents", "--all"))
        self.assertIn("archived 1 agent(s): gone", self.sw("agents", "--prune"))
        listing = self.sw("agents", "--all")
        self.assertNotIn("gone ", listing)
        self.assertIn("recent", listing)
        self.assertIn("owner", listing)
        with self.board() as sw:
            row = sw.agent("gone")
            self.assertEqual((row["state"], row["offline_reason"]), ("left", "pruned"))
            self.assertEqual(sw.db.execute("SELECT count(*) FROM events WHERE kind='agents_pruned'").fetchone()[0], 1)
        self.assertIn("nothing to prune", self.sw("agents", "--prune"))
        self.assertIn("archived 1 agent(s): recent", self.sw("agents", "--prune", "--older-than", "0"))
        self.assertIn("owner", self.sw("agents", "--all"))
        self.sw("join", "--as", "gone", "--kind", "claude")  # an archived name is free to come back

    def test_ec350_only_the_coordinator_or_a_human_may_prune(self):
        self.join("bob")
        with self.board() as sw, sw.tx():
            sw.update_agent("bob", state="offline", offline_at=core.iso(core.utcnow() - core.timedelta(minutes=500)))
        self.assertIn("coordinator", self.sw("--as", "bob", "agents", "--prune", ok=False))

    def test_ec345_json_never_enters_a_key_loop_and_pipes_do_not_start_workers(self):
        for argv in (("sessions",), ("execution", "logs", self.execution_id, "--follow")):
            with self.subTest(argv=argv):
                result = self.json_command(*argv, "--json", ok=False)
                self.assertIn("without prompts", result["error"]["message"])
        self.assertIn("No terminal window", self.sw("sessions", "--agents", "fake", ok=False))
        self.assertEqual(len(self.json_command("sessions", "--once", "--json")["result"]["sessions"]), 1)
        self.assertIn("one terminal", self.sw("console", "--managed", "--dry-run"))
        self.json_command("sessions", "--agents", "fake", "--unattended", "--once", "--json", ok=False)
        with self.board() as sw, sw.tx():
            sw.update_agent("ann", reviewing=99)
        self.json_command("sessions", "--once", "--as", "ann", "--json")
        self.json_command("sessions", "--agents", "fake", "--once", "--as", "ann", "--json", ok=False)

    def test_ec347_captured_helpers_use_no_window_on_windows(self):
        done = subprocess.CompletedProcess(["git"], 0, "ok", "")
        with mock.patch.object(core.subprocess, "run", return_value=done) as run:
            core.run(["git", "status"], cwd=self.root)
            flags = run.call_args.kwargs.get("creationflags", 0)
        self.assertEqual(bool(flags & 0x08000000), os.name == "nt")
        self.assertNotIn("interactive", core.background_flags())


class SessionLifetime(ProcessCase):
    def test_ec345_detach_and_rejoin_preserve_the_same_live_worker_and_work(self):
        code = self.holding_program()
        self.configure_worker(code="print('retained worker output', flush=True); " + code)
        try:
            self.sw("spawn", "hold", "--name", "bob", "--task", "T1")
            self.wait_for(lambda: self.ready.exists())
            before = self.execution()
            with self.board() as sw:
                args = argparse.Namespace(once=False, agents="", unattended=False, limit=200, as_=None, json=False)
                output = io.StringIO()
                keys = mock.MagicMock()
                keys.read.return_value = "q"
                with redirect_stdout(output), mock.patch.object(output, "isatty", return_value=True), \
                     mock.patch.object(sys.stdin, "isatty", return_value=True), \
                     mock.patch.object(swarm_sessions, "TerminalKeys") as factory:
                    factory.return_value.__enter__.return_value = keys
                    core.cmd_sessions(sw, args)
                self.assertIn("Workers continue", output.getvalue())
            data = self.json_command("sessions", "--agents", "hold", "--once", "--json")["result"]
            self.assertEqual([s["id"] for s in data["sessions"]], [before["id"]])
            self.assertTrue(self.running(int(self.ready.read_text())))
            self.assertEqual(self.task("T1")["assignee"], "bob")
            self.assertTrue((Path(self.task("T1")["worktree"]) / "unfinished.txt").exists())
        finally:
            self.cleanup_worker()

    def test_ec345_headless_manager_starts_workers_and_normal_stop_retains_the_log(self):
        code = self.holding_program()
        self.configure_worker(code="print('auditable evidence', flush=True); " + code)
        try:
            data = self.json_command("sessions", "--agents", "hold", "--once", "--json")["result"]
            self.assertEqual(len(data["sessions"]), 1)
            row = data["sessions"][0]
            self.wait_for(lambda: self.ready.exists())
            self.sw("execution", "stop", row["id"], "--reason", "Operator finished inspecting the worker")
            self.wait_for(lambda: self.execution()["state"] in core.EXECUTION_TERMINAL)
            capture = self.json_command("execution", "logs", row["id"], "--json")["result"]
            self.assertIn("auditable evidence", capture["text"])
            self.assertEqual(capture["state"], "terminated")
        finally:
            self.cleanup_worker()
