"""A merge whose process is proven gone frees the queue; nothing short of proof does."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import unittest
from unittest import mock

from test_hardening import HardeningCase, core


def dead_identity():
    """The identity of a process that has really exited on this host."""
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    ident = core.process_identity(proc.pid)
    proc.kill()
    proc.wait()
    return ident


class DeadMergeLease(HardeningCase):
    def lease(self, proc, groups=()):
        with self.board() as sw, sw.tx():
            sw.set_meta(
                "merge_lock",
                json.dumps({"agent": "ann", "task": "T1", "at": core.iso(), "attempt": "a" * 16, "proc": proc, "groups": list(groups)}),
            )

    def lock(self):
        with self.board() as sw:
            return sw.meta("merge_lock")

    def test_ec382_a_lease_whose_process_exited_is_broken(self):
        self.join("ann")
        self.lease(dead_identity())
        self.sw("status", "--as", "ann")
        self.assertIsNone(self.lock())
        with self.board() as sw:
            detail = sw.db.execute("SELECT detail FROM events WHERE kind='merge_lock_broken'").fetchone()[0]
        self.assertIn("proven", detail)

    def test_ec382_a_live_merge_is_not_broken(self):
        self.join("ann")
        self.lease(core.process_identity(core.os.getpid()))
        self.sw("status", "--as", "ann")
        self.assertIsNotNone(self.lock())

    def test_ec382_a_live_owned_group_keeps_the_lease(self):
        self.join("ann")
        self.lease(dead_identity(), groups=[core.process_identity(core.os.getpid())])
        self.sw("status", "--as", "ann")
        self.assertIsNotNone(self.lock())

    def test_ec382_only_proof_counts(self):
        me = core.process_identity(core.os.getpid())
        self.assertFalse(core.proven_gone(me))
        different = {**me, "start": "previous-process-start"}
        if me["start"] is not None:
            self.assertTrue(core.proven_gone(different), "a reused PID with an observed start time")
        with mock.patch.object(core, "process_start", return_value="current-process-start"):
            self.assertTrue(core.proven_gone(different), "a different process under the same pid")
        self.assertFalse(core.proven_gone({**dead_identity(), "ns": "another-host/boot"}), "another namespace")
        with mock.patch.object(core, "process_start", return_value=None):  # access denied or unknown
            self.assertFalse(core.proven_gone({**me, "start": "1"}))
        self.assertFalse(core.proven_gone({**me, "start": None}), "an unknown start time proves nothing")

    def test_ec382_the_namespace_names_the_pid_namespace(self):
        ns = core.namespace_id()
        self.assertEqual(ns, core.namespace_id(), "the namespace is stable for this process")
        if os.name == "nt":
            self.assertRegex(ns, r"boot-\d+")
        else:
            try:
                pid_ns = os.readlink("/proc/self/ns/pid")
            except OSError:
                self.assertNotIn("pid:[", ns)
            else:
                self.assertEqual(ns.rsplit("/", 1)[-1], pid_ns)

    @unittest.skipUnless(os.name != "nt", "process groups are a POSIX concept")
    def test_ec382_a_group_that_outlives_its_leader_is_not_gone(self):
        leader = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import subprocess, sys, time\n"
                "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
                "print(child.pid, flush=True)\n"
                "time.sleep(60)\n",
            ],
            stdout=subprocess.PIPE,
            start_new_session=True,
            text=True,
        )
        try:
            child_pid = int(leader.stdout.readline().strip())
            ident = core.process_identity(leader.pid, leader.pid)
            leader.kill()
            leader.wait()
            self.assertFalse(core.proven_gone(ident), "the leader is gone but its group still holds a descendant")
            os.kill(child_pid, signal.SIGKILL)
            deadline = time.monotonic() + 10
            while not core.proven_gone(ident) and time.monotonic() < deadline:
                try:  # when the test process is PID 1, the orphan is reparented here and must be reaped
                    os.waitpid(child_pid, os.WNOHANG)
                except ChildProcessError:
                    pass
                time.sleep(0.05)
            self.assertTrue(core.proven_gone(ident), "once the whole group is gone, the identity is proven dead")
        finally:
            if leader.poll() is None:
                leader.kill()
                leader.wait()
            leader.stdout.close()

    def test_ec382_the_queue_hold_is_keyed_to_its_own_attempt(self):
        self.join("ann")
        self.lease(dead_identity())
        with self.board() as sw:
            self.assertFalse(core.holds_queue(sw, "T1"), "a process that merged nothing holds nothing")
            self.assertTrue(core.queue_held(sw, "T1"), "the queue is held for the task, whoever asks")
            sw.merge_attempt = "b" * 16
            self.assertFalse(core.holds_queue(sw, "T1"), "a later attempt's lease is not this process's")
            sw.merge_attempt = "a" * 16
            self.assertTrue(core.holds_queue(sw, "T1"))
            self.assertFalse(core.holds_queue(sw, "T2"), "another task's merge is not this one")
            self.assertFalse(core.queue_held(sw, "T2"))

    def test_ec383_a_tool_run_whose_supervisor_is_gone_is_settled_by_the_engine(self):
        self.join("ann")
        with self.board() as sw, sw.tx():
            sw.db.execute(
                "INSERT INTO usage_runs(id,task,owner,kind,state,reserved_tokens,reserved_micros,reserved_seconds,created_at,heartbeat_at,supervisor_pid) "
                "VALUES('run1','T1','ann','tool','unknown',0,0,60,?,?,1)",
                (core.iso(), core.iso()),
            )
            sw.set_meta("usage_supervisor:run1", json.dumps(dead_identity()))
        self.sw("status", "--as", "ann")
        row = self.row_of("SELECT state, detail FROM usage_runs WHERE id='run1'")
        self.assertEqual(row[0], "charged")
        self.assertIn("engine-verified: supervisor gone", row[1])

    def test_ec383_a_run_with_a_token_reservation_still_needs_the_human(self):
        self.join("ann")
        with self.board() as sw, sw.tx():
            sw.db.execute(
                "INSERT INTO usage_runs(id,task,owner,kind,state,reserved_tokens,reserved_micros,reserved_seconds,created_at,heartbeat_at,supervisor_pid) "
                "VALUES('run2','T1','ann','tool','unknown',500,0,60,?,?,1)",
                (core.iso(), core.iso()),
            )
            sw.set_meta("usage_supervisor:run2", json.dumps(dead_identity()))
        self.sw("status", "--as", "ann")
        self.assertEqual(self.row_of("SELECT state FROM usage_runs WHERE id='run2'")[0], "unknown")

    @unittest.skipUnless(os.name != "nt", "process groups are a POSIX concept")
    def test_ec383_a_live_owned_group_keeps_a_tool_run_unsettled(self):
        self.join("ann")
        with self.board() as sw, sw.tx():
            sw.db.execute(
                "INSERT INTO usage_runs(id,task,owner,kind,state,reserved_tokens,reserved_micros,reserved_seconds,created_at,heartbeat_at,supervisor_pid) "
                "VALUES('run3','T1','ann','tool','unknown',0,0,60,?,?,1)",
                (core.iso(), core.iso()),
            )
            sw.set_meta("usage_supervisor:run3", json.dumps(dead_identity()))
            sw.set_meta("usage_group:run3", json.dumps([core.process_identity(os.getpid(), os.getpid())]))
        self.sw("status", "--as", "ann")
        self.assertEqual(self.row_of("SELECT state FROM usage_runs WHERE id='run3'")[0], "unknown")
        with self.board() as sw, sw.tx():
            sw.set_meta("usage_group:run3", None)
        self.sw("status", "--as", "ann")
        row = self.row_of("SELECT state, detail FROM usage_runs WHERE id='run3'")
        self.assertEqual(row[0], "charged")
        self.assertIn("owned group gone", row[1])

    def row_of(self, sql):
        with self.board() as sw:
            return sw.db.execute(sql).fetchone()

    def test_ec384_merge_detach_runs_in_the_background_and_lands(self):
        self.join("ann")
        self.to_ready("ann", "T1")
        out = self.sw("merge", "--detach", "--as", "ann")
        self.assertIn("runs in the background", out)
        attempt = out.split("merge attempt ", 1)[1].split()[0]
        deadline = time.monotonic() + 120
        while self.task("T1")["state"] != "done" and time.monotonic() < deadline:
            time.sleep(1)
        self.assertEqual(self.task("T1")["state"], "done")
        with self.board() as sw:
            started = sw.db.execute("SELECT detail FROM events WHERE kind='merge_started'").fetchone()[0]
        self.assertIn(attempt, started)

    def test_ec392_the_recorded_identity_is_the_group_leader_not_the_worker(self):
        """On POSIX the worker is only a member of the group its anchor leads; recording the worker made the group
        probe vacuous (killpg on a non-leader is always 'no such process')."""
        import tempfile
        from pathlib import Path
        from types import SimpleNamespace

        worker = SimpleNamespace(pid=4242, proc=SimpleNamespace(pid=4200), job=None)
        ident = core.owned_identity(worker)
        if os.name == "nt":
            self.assertEqual((ident["pid"], "group" in ident), (4242, False), "a job object bounds the group on Windows")
            return
        self.assertEqual((ident["pid"], ident["group"]), (4200, 4200))
        with tempfile.TemporaryFile() as out:
            child = core.OwnedProcess([sys.executable, "-c", "import time; time.sleep(30)"], Path.cwd(), dict(os.environ), out)
            try:
                recorded = core.owned_identity(child)
                self.assertEqual(recorded["group"], os.getpgid(child.pid), "the recorded group is the one the worker is in")
                self.assertFalse(core.proven_gone(recorded), "a live group is not gone")
            finally:
                child.kill()
                for _ in range(100):
                    if not child.active():
                        break
                    time.sleep(0.05)
                child.close()
