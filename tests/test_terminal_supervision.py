"""Actual foreground jobs/groups, policy stops and POSIX controlling-terminal I/O."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

import test_processes as processes
from test_hardening import HardeningCase, core, fixtures


class TerminalSupervision(HardeningCase):
    wait_for = processes.ProcessCase.wait_for
    running = processes.ProcessCase.running
    execution = processes.ProcessCase.execution
    holding_program = processes.ProcessCase.holding_program
    configure_worker = processes.ProcessCase.configure_worker

    def configure_terminal(self, action="terminate", *, descendants=False, parent_exits=False, code=None):
        code = code or self.holding_program(descendants=descendants, parent_exits=parent_exits)
        self.configure_worker(action, code=code)
        config = self.root / ".agents/swarm/config.toml"
        # Adapter templates use format_map; protect literal Python dictionary braces.
        code = code.replace("{", "{{").replace("}", "}}")
        config.write_text(config.read_text(encoding="utf-8") + "\ninteractive = " + json.dumps([sys.executable, "-c", code, "{prompt}"]) + "\n", encoding="utf-8")

    def command(self):
        return [sys.executable, str(fixtures.SWARM), "launch", "hold", "--name", "worker", "--task", "T1"]

    def exercise_stop(self, *, cooperative=False, parent_exits=False):
        self.configure_terminal("cooperative" if cooperative else "terminate", descendants=True, parent_exits=parent_exits)
        log = Path(self.tmp.name) / "terminal.log"
        with log.open("wb") as output:
            caller = subprocess.Popen(self.command(), cwd=self.root, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT)
            unrelated = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                self.wait_for(lambda: self.execution() and self.execution()["state"] == "running" and self.ready.exists() and self.child_pid.exists())
                row = self.execution()
                worker, child = int(self.ready.read_text()), int(self.child_pid.read_text())
                self.assertEqual(row["supervisor_pid"], caller.pid)
                self.assertEqual(json.loads(row["policy"])["transport"], "terminal")
                self.assertIsNone(row["output"])  # No invented terminal capture/log proof.
                if parent_exits:
                    self.wait_for(lambda: not self.running(worker))
                    self.assertTrue(self.running(child))
                    self.assertIsNone(caller.poll())
                self.sw("execution", "stop", row["id"], "--reason", "Preserve terminal work before reassigning this bounded task")
                if cooperative:
                    time.sleep(0.2)
                    self.assertIsNone(caller.poll())
                    self.assertTrue(self.running(child))
                    self.release.touch()
                caller.wait(timeout=20)
                self.assertEqual(caller.returncode, 0, log.read_text(encoding="utf-8", errors="replace"))
                self.assertEqual(self.execution()["state"], "exited" if cooperative else "terminated")
                self.assertFalse(self.running(child))
                self.assertIsNone(unrelated.poll())
                self.assertNotEqual(self.task("T1")["state"], "done")
                self.assertEqual((Path(self.task("T1")["worktree"]) / "unfinished.txt").read_text(), "preserve unfinished work")
            finally:
                self.release.touch()
                if caller.poll() is None:
                    self.sw("execution", "stop", self.execution()["id"], "--reason", "Finish the actual fixture terminal safely", ok=None)
                try:
                    caller.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    caller.kill()
                    caller.wait(timeout=10)
                if unrelated.poll() is None:
                    unrelated.kill()
                unrelated.wait(timeout=10)

    def test_ec326_terminal_terminate_policy_stops_its_group_and_preserves_unrelated_process(self):
        self.exercise_stop()

    def test_ec326_terminal_cooperative_policy_waits_for_actual_group_exit(self):
        self.exercise_stop(cooperative=True)

    def test_ec326_terminal_parent_exit_does_not_hide_its_live_descendant(self):
        self.exercise_stop(parent_exits=True)

    def test_ec308_ec327_terminal_creation_commit_failure_retains_unknown_intent_and_work(self):
        self.configure_terminal()
        args = core.build_parser().parse_args(self.command()[2:])
        def fail(name):
            if name == "before_interactive_start_commit":
                raise sqlite3.OperationalError("controlled terminal commit failure")
        with self.board() as sw, mock.patch.object(core, "failpoint", side_effect=fail):
            with self.assertRaisesRegex(sqlite3.OperationalError, "controlled terminal"):
                core.cmd_launch(sw, args)
            row = sw.db.execute("SELECT * FROM executions ORDER BY rowid DESC LIMIT 1").fetchone()
            self.assertEqual(row["state"], "unknown")
            self.assertEqual(sw.task("T1")["assignee"], "worker")
            self.assertNotEqual(sw.task("T1")["state"], "done")
        if self.ready.exists():
            self.wait_for(lambda: not self.running(int(self.ready.read_text())))

    def caller_loss(self, action):
        self.configure_terminal(action, descendants=True)
        log = Path(self.tmp.name) / "caller-loss.log"
        with log.open("wb") as output:
            caller = subprocess.Popen(self.command(), cwd=self.root, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT)
            try:
                self.wait_for(lambda: self.ready.exists() and self.child_pid.exists())
                worker, child = int(self.ready.read_text()), int(self.child_pid.read_text())
                caller.kill()
                caller.wait(timeout=10)
                if action == "cooperative":
                    self.assertTrue(self.running(child))
                    self.release.touch()
                self.wait_for(lambda: not self.running(worker) and not self.running(child))
                def lost():
                    self.sw("status")
                    return self.execution()["state"] == "unknown"
                self.wait_for(lost)
                row = self.execution()
                self.assertEqual(row["state"], "unknown")
                self.join("bob")
                self.assertIn("uncertain", self.sw("claim", "T1", "--take-over", "--as", "bob", ok=False))
                self.sw("execution", "reconcile", row["id"], "--confirm-stopped", "--reason", "Observed actual caller exit and every fixture worker and descendant stopped")
                self.assertEqual(self.execution()["state"], "reconciled")
                self.sw("claim", "T1", "--take-over", "--as", "bob")
                self.assertEqual((Path(self.task("T1")["worktree"]) / "unfinished.txt").read_text(), "preserve unfinished work")
            finally:
                self.release.touch()
                if caller.poll() is None:
                    caller.kill()
                caller.wait(timeout=10)

    def test_ec327_os_killed_terminal_launcher_contains_terminate_group_and_retains_unknown_proof(self):
        self.caller_loss("terminate")

    def test_ec327_os_killed_cooperative_launcher_preserves_processes_until_their_observed_exit(self):
        self.caller_loss("cooperative")

    @unittest.skipIf(os.name == "nt", "POSIX controlling PTY; Windows uses the inherited console")
    def test_ec328_exit_drain_preserves_all_buffered_output_across_short_reads(self):
        # Use a pipe for the drain loop. A PTY can discard queued output when
        # its last writer closes. The next test exercises an actual PTY.
        payload = b"BEGIN-TERMINAL-TAIL" + b"x" * 400 + b"END-TERMINAL-TAIL"
        reader, writer = os.pipe()
        bridge = core.TerminalBridge()
        bridge.master = reader
        captured = bytearray()
        original_read = os.read

        def forward(_fd, data):
            captured.extend(data)
            return len(data)

        try:
            self.assertEqual(os.write(writer, payload), len(payload))
            os.close(writer)
            writer = None
            with mock.patch.object(bridge, "resize"), mock.patch.object(
                core.os, "read", side_effect=lambda fd, size: original_read(fd, min(size, 127))
            ), mock.patch.object(core.os, "write", side_effect=forward):
                bridge.drain()
            self.assertEqual(captured, payload)
            self.assertFalse(bridge.output_open)
            self.assertFalse(bridge.input_open)
        finally:
            if writer is not None:
                os.close(writer)
            os.close(reader)


    @unittest.skipIf(os.name == "nt", "POSIX controlling PTY; Windows uses the inherited console")
    def test_ec328_posix_terminal_drains_the_final_output_after_the_worker_exits(self):
        import pty
        import select

        payload = "BEGIN-TERMINAL-TAIL" + "x" * 5000 + "END-TERMINAL-TAIL"
        code = "import sys;sys.stdout.write('BEGIN-TERMINAL-TAIL'+'x'*5000+'END-TERMINAL-TAIL');sys.stdout.flush()"
        self.configure_terminal("terminate", code=code)
        master, slave = pty.openpty()
        caller = subprocess.Popen(self.command(), cwd=self.root, stdin=slave, stdout=slave, stderr=slave)
        os.close(slave)
        captured = bytearray()
        try:
            until = time.monotonic() + 30
            while time.monotonic() < until:
                if not select.select([master], [], [], 0.05)[0]:
                    if caller.poll() is not None:
                        break
                    continue
                try:
                    data = os.read(master, 8192)
                except OSError:
                    break
                if not data:
                    break
                captured.extend(data)
            caller.wait(timeout=10)
            self.assertEqual(caller.returncode, 0, captured.decode("utf-8", errors="replace")[-1000:])
            self.assertTrue(payload.encode() in captured, f"Final terminal output missing; captured {len(captured)} bytes")
        finally:
            if caller.poll() is None:
                caller.kill()
            caller.wait(timeout=10)
            os.close(master)

    @unittest.skipIf(os.name == "nt", "POSIX controlling PTY; Windows uses the inherited console")
    def test_ec328_posix_terminal_has_controlling_tty_input_resize_and_restoration(self):
        import fcntl
        import pty
        import struct
        import termios

        ready, result = (Path(self.tmp.name) / name for name in ("tty-ready", "tty-result"))
        code = (
            "import json,os,sys;from pathlib import Path;"
            f"Path({str(ready)!r}).touch();line=sys.stdin.readline().strip();"
            f"Path({str(result)!r}).write_text(json.dumps({{'tty':sys.stdin.isatty(),'cols':os.get_terminal_size(0).columns,'line':line}}));"
            "Path('unfinished.txt').write_text('preserve terminal work')"
        )
        self.configure_terminal("cooperative", code=code)
        master, slave = pty.openpty()
        before = termios.tcgetattr(slave)
        fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
        startup_errors = Path(self.tmp.name) / "terminal-startup.stderr"
        with startup_errors.open("wb") as errors:
            caller = subprocess.Popen(self.command(), cwd=self.root, stdin=slave, stdout=slave, stderr=errors)
        os.close(slave)
        try:
            try:
                self.wait_for(ready.exists)
            except AssertionError as exc:
                details = startup_errors.read_text(encoding="utf-8", errors="replace")[-2000:]
                raise AssertionError(f"{exc}\nTerminal caller exit: {caller.poll()}\n{details}") from exc
            os.write(master, b"hello-terminal\n")
            self.wait_for(result.exists)
            caller.wait(timeout=15)
            self.assertEqual(caller.returncode, 0)
            self.assertEqual(json.loads(result.read_text()), {"tty": True, "cols": 80, "line": "hello-terminal"})
            self.assertEqual(termios.tcgetattr(master), before)
            self.assertEqual(self.execution()["state"], "exited")
        finally:
            if caller.poll() is None:
                caller.kill()
            caller.wait(timeout=10)
            os.close(master)
