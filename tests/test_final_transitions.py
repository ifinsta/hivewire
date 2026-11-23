"""Additional actual OS-kill coverage for architecture, roles and control commits."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import test_claim_crashes as crashes
import test_transaction_crashes as transactions
from test_hardening import HardeningCase, fixtures


class FinalTransitions(HardeningCase):
    kill = crashes.ClaimCrashes.kill
    boundary = transactions.TransactionCrashes.boundary

    @contextmanager
    def paused(self, point, *command):
        # Several operations share one transaction label; stale readiness is not a new child acknowledgement.
        (Path(self.tmp.name) / (point + ".json")).unlink(missing_ok=True)
        with crashes.ClaimCrashes.paused(self, point, *command) as child:
            yield child

    def snapshot(self):
        db = self.db()
        try:
            tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
            snapshot = {}
            for table in tables:
                cursor = db.execute(f'SELECT * FROM "{table}"')
                # The command preflight can refresh attention liveness across a
                # minute boundary, independently of the transition being killed.
                volatile = (
                    {index for index, column in enumerate(cursor.description)
                     if column[0] in ("last_seen_at", "last_scan_at")}
                    if table == "attention_items" else set()
                )
                snapshot[table] = sorted(
                    [tuple(value for index, value in enumerate(row) if index not in volatile) for row in cursor],
                    key=repr,
                )
            return snapshot
        finally:
            db.close()

    def row(self, table, where="1", params=()):
        with self.board() as sw:
            r = sw.db.execute(f"SELECT * FROM {table} WHERE {where} ORDER BY 1 DESC LIMIT 1", params).fetchone()
            return dict(r) if r else None

    def test_ec329_architecture_approval_and_goal_assignment_commit_kills_preserve_authority(self):
        self.join("ann")
        self.join("bob")
        self.sw("goal", "new", "GX", "Deliver the versioned source fixture")
        fixtures.Goals.answer_all(self, "GX")
        self.sw("goal", "outcome", "GX", "The source fixture exposes the accepted version", "--check", "Run the source fixture verification command")
        self.sw("goal", "task", "GX", "X-work", "Implement the versioned source fixture", "--serves", "1", "--scope", "src/", "--done-when", "The versioned source fixture verification passes")
        self.architecture("GX", approve=False)
        self.boundary(["goal", "architecture", "GX", "--approve"], "cmd_goal_architecture",
                      lambda: self.assertIsNotNone(self.row("goals", "id='GX'")["architecture_approval"]))
        self.assertEqual(json.loads(self.row("goals", "id='GX'")["architecture_approval"])["by"], "human")
        self.boundary(["goal", "commit", "GX", "--assign"], "cmd_goal_commit",
                      lambda: self.assertEqual(self.row("goals", "id='GX'")["state"], "active"))
        task = self.task("X-work")
        self.assertEqual(task["state"], "open")
        self.assertIsNotNone(task["reserved_for"])
        self.assertIsNone(task["assignee"])
        self.assertIsNone(task["worktree"])

    def test_ec329_counter_decision_and_acceptance_commit_kills_keep_one_bounded_offer(self):
        self.join("ann")
        self.sw("contract", "offer", "T1", "ann", "--as", "human")
        deadline = self.row("task_contracts")["decision_at"]
        self.boundary(["contract", "counter", "T1", "--as", "ann", "--reason", "The fixture needs a reviewed validation allowance"], "cmd_contract",
                      lambda: self.assertEqual(self.row("task_contracts")["state"], "countered"))
        self.boundary(["contract", "decide", "T1", "--active-time-min", "35", "--reason", "Allow five more minutes for the source fixture check", "--as", "human"], "cmd_contract",
                      lambda: self.assertEqual(self.row("task_contracts")["state"], "offered"))
        self.boundary(["contract", "accept", "T1", "--as", "ann"], "cmd_contract",
                      lambda: self.assertEqual(self.row("task_contracts")["state"], "accepted"))
        self.assertEqual(self.row("task_contracts")["decision_at"], deadline)
        self.assertEqual(self.row("task_contracts")["counter_count"], 1)
        self.assertIsNone(self.task("T1")["assignee"])

    def test_ec329_assignment_manual_locks_and_leave_release_commit_kills_keep_single_owner(self):
        self.join("ann")
        self.join("bob")
        self.boundary(["assign", "T1", "bob"], "cmd_assign",
                      lambda: self.assertEqual(self.task("T1")["reserved_for"], "bob"))
        self.sw("claim", "T1", "--as", "bob")
        self.boundary(["lock", "migrations", "--as", "bob"], "cmd_lock",
                      lambda: self.assertEqual(self.row("locks", "name='migrations'")["agent"], "bob"))
        self.boundary(["unlock", "migrations", "--as", "bob"], "cmd_unlock",
                      lambda: self.assertIsNone(self.row("locks", "name='migrations'")))
        self.boundary(["leave", "--as", "bob", "--release", "Retain this assignment and branch for a bounded successor"], "cmd_leave",
                      lambda: self.assertEqual(self.row("agents", "name='bob'")["state"], "left"))
        self.assertEqual(self.task("T1")["state"], "open")
        self.assertIsNone(self.task("T1")["assignee"])
        self.assertIsNotNone(self.task("T1")["branch"])

    def test_ec329_message_and_interrupt_ack_complete_resume_commit_kills_keep_control(self):
        self.join("ann")
        self.join("bob")
        self.sw("claim", "T1", "--as", "bob")
        self.sw("plan", "--as", "bob", *fixtures.PLAN)
        self.sw("start", "--as", "bob")
        self.sw("send", "bob", "Inspect the accepted interface contract before continuing", "--as", "ann", "--priority", "critical")
        message = self.row("messages")["id"]
        receipt = self.message_receipt(message, "bob")
        self.boundary(["bus", "ack", str(message), "--as", "bob", "--consumer", receipt["consumer"], "--token", receipt["token"], "--json"], "cmd_bus",
                      lambda: self.assertIsNotNone(self.row("message_recipients", "msg=? AND agent='bob'", (message,))["acked_at"]))
        self.sw("interrupt", "request", "bob", "--mode", "pause", "--reason", "Pause at a checkpoint before validating the interface")
        control = self.row("interrupts")["id"]
        epoch = self.task("T1")["epoch"]
        self.boundary(["interrupt", "ack", str(control), "--as", "bob", "--epoch", str(epoch)], "cmd_interrupt",
                      lambda: self.assertEqual(self.row("interrupts")["state"], "acknowledged"))
        self.sw("checkpoint", "--as", "bob", "--epoch", str(epoch), "--note", "The planned interface and current source are retained", "--next", "Resume the accepted implementation after operator review")
        checkpoint = self.row("checkpoints")["id"]
        self.boundary(["interrupt", "complete", str(control), "--as", "bob", "--epoch", str(epoch), "--checkpoint", str(checkpoint), "--note", "Retained the current implementation checkpoint before yielding control"], "cmd_interrupt",
                      lambda: self.assertEqual(self.row("interrupts")["state"], "completed"))
        self.assertEqual(self.task("T1")["state"], "interrupted")
        self.boundary(["interrupt", "resume", str(control), "--epoch", str(epoch)], "cmd_interrupt",
                      lambda: self.assertIsNotNone(self.row("interrupts")["resumed_at"]))
        self.assertEqual(self.task("T1")["state"], "planning")
        self.sw("step", "1", "--as", "bob", "--evidence", "Attempt work before revalidating the resumed plan", ok=False)
        self.sw("start", "--as", "bob")
        self.assertEqual(self.task("T1")["state"], "in_progress")
