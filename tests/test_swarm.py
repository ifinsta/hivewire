"""Tests for hivewire.py, each against a real git repository in a temporary directory.

    python -m unittest discover -s tests -v

Test names carry the edge-case ids from EDGE-CASES.md they prove.
"""

from __future__ import annotations

from contextlib import closing

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from pathlib import Path

SWARM = Path(__file__).resolve().parents[1] / "skills" / "agents-collaboration" / "hivewire.py"
sys.path.insert(0, str(SWARM.parent))
import swarm_core as swarm_module  # noqa: E402

CONFIG = """
[project]
target = "main"
branch_prefix = "swarm/"
backlog = "backlog.md"
check_remote = false

[swarm]
max_agents = {max_agents}

[gates]
commands = [["python", "-c", "import pathlib,sys; sys.exit(1 if pathlib.Path('FAIL').exists() or (pathlib.Path('a.txt').exists() and pathlib.Path('b.txt').exists()) else 0)"]]

[hotspots]
config = {{ paths = ["config/**"] }}
migrations = {{ paths = ["app/migrations/**"], always = true }}

[adapters.fake]
version = ["python", "--version"]
auth = []
run = ["python", "-c", "print('{{prompt}}')"]
interactive = ["python", "-c", "print('{{prompt}}')"]
model = []
unattended = []
verified = true

[adapters.fakeplain]
version = ["python", "--version"]
auth = []
run = ["python", "-c", "print(1)"]
interactive = ["python", "-c", "print(1)"]
model = []
unattended = []
verified = true
"""

BACKLOG = """# Backlog

| Id | Title | Scope | Needs | Locks | Caps |
|---|---|---|---|---|---|
| T1 | First task | src/ | — | — | |
| T2 | Second task | docs/ | — | — | |
| T3 | Needs T1 | src/ | T1 | — | |
| T4 | Cluster only | ops/ | — | — | gpu-cluster |
| T5 | Exclusive | src/ | — | config | |
| T6 | Also exclusive | lib/ | — | config | |
"""

PLAN = [
    "--set",
    "outcome=a file exists",
    "--set",
    "acceptance=gates pass",
    "--set",
    "scope=src/",
    "--set",
    "out_of_scope=everything else",
    "--set",
    "dependencies=none",
    "--set",
    "risks=low",
    "--set",
    "rollback=git revert",
    "--set",
    "unknowns=none",
    "--step",
    "write the source file",
    "--step",
    "commit the source file",
]


class SwarmCase(unittest.TestCase):
    max_agents = 5

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "repo"
        self.root.mkdir()
        env = {
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@t",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@t",
        }
        os.environ.update(env)
        os.environ.pop("SWARM_AS", None)
        self.git("init", "-q", "-b", "main")
        (self.root / ".agents" / "swarm").mkdir(parents=True)
        (self.root / ".agents" / "swarm" / "config.toml").write_text(
            CONFIG.format(max_agents=self.max_agents), encoding="utf-8"
        )
        (self.root / "backlog.md").write_text(BACKLOG, encoding="utf-8")
        (self.root / "config").mkdir()
        (self.root / "config" / "settings.txt").write_text("one\ntwo\n")
        (self.root / ".gitignore").write_text("__pycache__/\n")
        self.git("add", ".")
        self.git("commit", "-qm", "init")

    def tearDown(self):
        subprocess.run(["git", "worktree", "prune"], cwd=self.root, capture_output=True, check=False)
        try:
            self.tmp.cleanup()
        except PermissionError:  # Windows keeps SQLite files a moment longer
            time.sleep(0.5)
            self.tmp.cleanup()

    def git(self, *args, cwd=None):
        return subprocess.run(["git", *args], cwd=cwd or self.root, capture_output=True, text=True, check=True).stdout

    def sw(self, *args, cwd=None, ok=True, env=None, input_text=None):
        done = subprocess.run(
            [sys.executable, str(SWARM), *args],
            cwd=cwd or self.root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
            input=input_text,
            env={**os.environ, **(env or {})},
        )
        out = done.stdout + done.stderr
        if ok is True:
            self.assertEqual(done.returncode, 0, out)
        elif ok is False:
            self.assertNotEqual(done.returncode, 0, out)
        return out

    def message_receipt(self, message, who="human"):
        receipts = getattr(self, "_message_receipts", {})
        key = (who, int(message))
        if key not in receipts:
            result = json.loads(self.sw("bus", "receive", "--consumer", f"test:{who}", "--limit", "1000", "--as", who, "--json"))["result"]
            for delivery in result["deliveries"]:
                receipts[(who, delivery["receipt"]["message_id"])] = delivery["receipt"]
            self._message_receipts = receipts
        return receipts.get(key, {"consumer": f"test:{who}", "token": "no-addressed-receipt"})

    def ack_message(self, message, who="human", ok=True):
        receipt = self.message_receipt(message, who)
        return self.sw("bus", "ack", str(message), "--consumer", receipt["consumer"], "--token", receipt["token"], "--as", who, "--json", ok=ok)

    def db(self):
        return sqlite3.connect(self.root / ".git" / "swarm" / "swarm.db")

    def task(self, tid):
        con = self.db()
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
        con.close()
        return row

    def join(self, name, caps=""):
        return self.sw("--as", name, "join", "--kind", "claude", "--caps", caps)

    def architecture(self, goal, risk="medium", approve=True):
        answers = {
            "current_system": "The repository has a tracked backlog and a Python verification gate.",
            "constraints": "Keep the existing Python workflow and preserve independent task branches.",
            "options": "Extend the current modules; a rewrite would discard the established verification gate.",
            "decision": "Extend the existing implementation in small independently verified changes.",
            "boundaries": "Each task owns its declared files and exposes the outcome named in its done-check.",
            "failure_modes": "Tests may fail or a worker may disappear; preserve branches and hand off the work.",
            "rollout": "Integrate each change through the current rebase and verification gate.",
            "rollback": "Revert the task commit through the same verified merge workflow.",
            "acceptance_checks": "Run the configured Python gate and record evidence for every outcome.",
            "unresolved_decisions": "none",
        }
        args = [x for k, v in answers.items() for x in ("--set", f"{k}={v}")]
        self.sw("goal", "architecture", goal, "--risk", risk, "--evidence", "backlog.md", *args)
        if approve:
            self.sw("goal", "architecture", goal, "--approve")

    def to_ready(self, name, tid, filename="src/file.txt", content="x\n"):
        self.sw("--as", name, "claim", tid)
        wt = Path(self.task(tid)["worktree"])
        self.sw("--as", name, "plan", *PLAN, cwd=wt)
        self.sw("--as", name, "start", cwd=wt)
        (wt / filename).parent.mkdir(parents=True, exist_ok=True)
        (wt / filename).write_text(content)
        self.sw("--as", name, "step", "1", "--evidence", f"wrote {filename}", cwd=wt)
        self.git("add", ".", cwd=wt)
        self.git("commit", "-qm", "work", cwd=wt)
        self.sw("--as", name, "step", "2", "--evidence", "committed the work", cwd=wt)
        self.sw("--as", name, "submit", "--evidence", "gates pass locally", cwd=wt)
        return wt


class JoiningAndRoles(SwarmCase):
    def test_ec001_ec015_solo_then_peers(self):
        out = self.join("ann")
        self.assertIn("SOLO", out)
        self.assertIn("coordinator", out)
        out = self.join("bob")
        self.assertIn("peers: ann", out)
        self.assertIn("worker", out)

    def test_ec001_duplicate_live_name_refused_and_resume_allowed(self):
        self.join("ann")
        self.assertIn("already live", self.sw("--as", "ann", "join", "--kind", "opencode", ok=False))
        self.sw("--as", "ann", "join", "--kind", "opencode", "--resume")

    def test_ec014_bad_names_refused(self):
        for bad in ("Ann", "human", "x", "a b"):
            self.sw("--as", bad, "join", "--kind", "claude", ok=False)

    def test_ec017_coordinator_promoted_when_coordinator_goes_silent(self):
        self.join("ann")
        self.join("bob")
        con = self.db()
        con.execute("UPDATE agents SET heartbeat_at='2020-01-01T00:00:00+00:00' WHERE name='ann'")
        con.commit()
        con.close()
        out = self.sw("agents")
        self.assertIn("*bob", out)

    def test_ec022_human_cannot_be_impersonated_by_an_agent(self):
        self.join("ann")
        out = self.sw("--as", "human", "pause", "x", ok=False, env={"SWARM_AS": "ann"})
        self.assertIn("may not act as the human", out)


class FullSwarm(SwarmCase):
    max_agents = 2

    def test_ec005_swarm_cap(self):
        self.join("ann")
        self.join("bob")
        self.assertIn("full", self.join_fail("cat"))

    def join_fail(self, name):
        return self.sw("--as", name, "join", "--kind", "claude", ok=False)


class Claiming(SwarmCase):
    def test_ec033_concurrent_claims_have_one_winner(self):
        for n in ("ann", "bob", "cat", "dan"):
            self.join(n)
        results = []

        def go(name):
            done = subprocess.run(
                [sys.executable, str(SWARM), "--as", name, "claim", "T2"],
                cwd=self.root,
                capture_output=True,
                text=True,
                check=False,
            )
            results.append(done.returncode)

        threads = [threading.Thread(target=go, args=(n,)) for n in ("ann", "bob", "cat", "dan")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(results).count(0), 1, results)

    def test_ec034_one_task_at_a_time(self):
        self.join("ann")
        self.sw("--as", "ann", "claim", "T1")
        self.assertIn("still hold T1", self.sw("--as", "ann", "next", ok=False))
        self.assertIn("still hold T1", self.sw("--as", "ann", "claim", "T2", ok=False))

    def test_ec036_dependencies_must_be_done(self):
        self.join("ann")
        self.assertIn("needs T1", self.sw("--as", "ann", "claim", "T3", ok=False))

    def test_ec029_ec038_capabilities_are_checked(self):
        self.join("ann")
        self.assertIn("gpu-cluster", self.sw("--as", "ann", "claim", "T4", ok=False))
        self.join("bob", caps="gpu-cluster")
        self.sw("--as", "bob", "claim", "T4")

    def test_ec035_reservations_are_respected(self):
        self.join("ann")
        self.join("bob")
        self.sw("--as", "ann", "assign", "T2", "bob")
        self.assertIn("assigned to bob", self.sw("--as", "ann", "claim", "T2", ok=False))
        self.assertIn("claimed T2", self.sw("--as", "bob", "next"))

    def test_ec065_declared_locks_are_taken_at_claim(self):
        self.join("ann")
        self.join("bob")
        self.sw("--as", "ann", "claim", "T5")
        self.sw("--as", "bob", "claim", "T2")
        self.assertIn("held by ann", self.sw("--as", "bob", "lock", "config", ok=False, cwd=self.root))
        self.join("cat")
        self.assertIn("needs lock config", self.sw("--as", "cat", "claim", "T6", ok=False))

    def test_ec072_cannot_act_from_another_agents_worktree(self):
        self.join("ann")
        self.join("bob")
        self.sw("--as", "ann", "claim", "T1")
        self.sw("--as", "bob", "claim", "T2")
        wt_ann = Path(self.task("T1")["worktree"])
        self.assertIn("worktree", self.sw("--as", "bob", "plan", *PLAN, cwd=wt_ann, ok=False))


class Planning(SwarmCase):
    def setUp(self):
        super().setUp()
        self.join("ann")
        self.sw("--as", "ann", "claim", "T1")
        self.wt = Path(self.task("T1")["worktree"])

    def test_ec044_no_start_without_plan(self):
        self.assertIn("No plan", self.sw("--as", "ann", "start", cwd=self.wt, ok=False))

    def test_ec045_every_critical_question_must_be_answered(self):
        out = self.sw(
            "--as", "ann", "plan", "--set", "outcome=x", "--step", "do the whole thing", cwd=self.wt, ok=False
        )
        self.assertIn("acceptance", out)
        self.assertIn("rollback", out)

    def test_ec046_ec050_ec051_unknown_opens_a_question_and_answers_force_a_replan(self):
        plan = [p if p != "risks=low" else "risks=unknown" for p in PLAN]
        out = self.sw("--as", "ann", "plan", *plan, cwd=self.wt)
        self.assertIn("critical question", out)
        self.assertEqual(self.task("T1")["state"], "awaiting_answers")
        self.assertIn("open", self.sw("--as", "ann", "start", cwd=self.wt, ok=False))
        # solo: the question goes to the human, and an agent may only relay the human's words
        self.assertIn("for the human", self.sw("--as", "ann", "answer", "1", "it is fine", ok=False))
        self.sw("--as", "ann", "answer", "1", "--from-human", "low risk, go ahead")
        self.assertIn("re-run `hivewire plan`", self.sw("--as", "ann", "start", cwd=self.wt, ok=False))
        self.sw("--as", "ann", "plan", *PLAN, cwd=self.wt)
        self.sw("--as", "ann", "start", cwd=self.wt)

    def test_ec047_listed_unknowns_must_be_asked(self):
        plan = [p if p != "unknowns=none" else "unknowns=the API shape" for p in PLAN]
        self.assertIn("Ask each one", self.sw("--as", "ann", "plan", *plan, cwd=self.wt, ok=False))

    def test_ec056_ec057_steps_in_order_with_evidence(self):
        self.sw("--as", "ann", "plan", *PLAN, cwd=self.wt)
        self.sw("--as", "ann", "start", cwd=self.wt)
        self.assertIn(
            "in order", self.sw("--as", "ann", "step", "2", "--evidence", "did the second", cwd=self.wt, ok=False)
        )
        self.assertIn("proves", self.sw("--as", "ann", "step", "1", "--evidence", "ok", cwd=self.wt, ok=False))
        self.sw("--as", "ann", "step", "1", "--evidence", "wrote src/file.txt", cwd=self.wt)

    def test_ec054_critical_question_mid_work_pauses_the_task(self):
        self.sw("--as", "ann", "plan", *PLAN, cwd=self.wt)
        self.sw("--as", "ann", "start", cwd=self.wt)
        self.sw("--as", "ann", "ask", "Should this be async?", "--critical", cwd=self.wt)
        self.assertEqual(self.task("T1")["state"], "awaiting_answers")
        self.assertIn(
            "in progress", self.sw("--as", "ann", "step", "1", "--evidence", "wrote the file", cwd=self.wt, ok=False)
        )

    def test_ec060_overlapping_scope_needs_acknowledgement(self):
        self.sw("--as", "ann", "plan", *PLAN, cwd=self.wt)
        self.join("bob")
        with closing(self.db()) as db, db:
            db.execute("UPDATE tasks SET scope_hint='src/' WHERE id='T2'")
        self.sw("--as", "bob", "claim", "T2")
        wt2 = Path(self.task("T2")["worktree"])
        self.assertIn("overlaps", self.sw("--as", "bob", "plan", *PLAN, cwd=wt2, ok=False))
        self.sw("--as", "bob", "plan", *PLAN, "--ack-overlap", "different files under src/", cwd=wt2)


class Submitting(SwarmCase):
    def setUp(self):
        super().setUp()
        self.join("ann")
        self.sw("status")
        with closing(self.db()) as db, db:
            db.execute("UPDATE tasks SET scope_hint='src/, config/, app/migrations/' WHERE id='T1'")
        self.sw("--as", "ann", "claim", "T1")
        self.wt = Path(self.task("T1")["worktree"])
        self.sw("--as", "ann", "plan", *PLAN, "--set", "scope=src/, config/, app/migrations/", "--ack-overlap", "n/a", cwd=self.wt)
        self.sw("--as", "ann", "start", cwd=self.wt)
        self.sw("--as", "ann", "step", "1", "--evidence", "wrote the file", cwd=self.wt)
        self.sw("--as", "ann", "step", "2", "--evidence", "committed the file", cwd=self.wt)

    def commit(self, rel, text):
        (self.wt / rel).parent.mkdir(parents=True, exist_ok=True)
        (self.wt / rel).write_text(text)
        self.git("add", ".", cwd=self.wt)
        self.git("commit", "-qm", "w", cwd=self.wt)

    def test_ec070_nothing_to_submit(self):
        self.assertIn("changes nothing", self.sw("--as", "ann", "submit", "--evidence", "e", cwd=self.wt, ok=False))

    def test_ec069_dirty_worktree_refused(self):
        self.commit("src/a.txt", "a")
        (self.wt / "src" / "b.txt").write_text("b")
        self.assertIn("untracked", self.sw("--as", "ann", "submit", "--evidence", "e", cwd=self.wt, ok=False))

    def test_ec061_out_of_contract_files_require_revised_terms(self):
        self.commit("src/a.txt", "a")
        self.commit("other/x.txt", "x")
        self.assertIn(
            "outside the approved scope", self.sw("--as", "ann", "submit", "--evidence", "e", cwd=self.wt, ok=False)
        )
        self.assertIn("contract scope add", self.sw("--as", "ann", "submit", "--evidence", "e", "--out-of-scope-reason", "fixture needed", cwd=self.wt, ok=False))

    def test_ec062_ec063_hotspot_rules(self):
        self.commit("src/a.txt", "a")
        (self.wt / "config" / "settings.txt").write_text("one\n")  # a deletion: non-additive
        self.git("commit", "-qam", "edit", cwd=self.wt)
        out = self.sw(
            "--as", "ann", "submit", "--evidence", "e", "--out-of-scope-reason", "settings", cwd=self.wt, ok=False
        )
        self.assertIn("hotspot 'config'", out)
        self.sw("--as", "ann", "lock", "config", cwd=self.wt)
        self.sw("--as", "ann", "submit", "--evidence", "e", "--out-of-scope-reason", "settings", cwd=self.wt)

    def test_ec062_additive_hotspot_edit_needs_no_lock(self):
        self.commit("src/a.txt", "a")
        (self.wt / "config" / "settings.txt").write_text("one\ntwo\nthree\n")
        self.git("commit", "-qam", "append", cwd=self.wt)
        self.sw("--as", "ann", "submit", "--evidence", "e", "--out-of-scope-reason", "settings", cwd=self.wt)

    def test_ec063_migrations_always_locked(self):
        self.commit("app/migrations/0002.py", "m")
        out = self.sw("--as", "ann", "submit", "--evidence", "e", "--out-of-scope-reason", "m", cwd=self.wt, ok=False)
        self.assertIn("any change to hotspot 'migrations'", out)


class Merging(SwarmCase):
    def test_ec099_ec086_merge_joins_the_target_then_frees_the_agent(self):
        self.join("ann")
        self.to_ready("ann", "T1")
        out = self.sw("--as", "ann", "merge")
        self.assertIn("merged T1", out)
        self.assertEqual(self.git("show", "main:src/file.txt"), "x\n")
        self.assertEqual(self.task("T1")["state"], "done")
        self.assertFalse(Path(self.task("T1")["worktree"]).exists())
        self.assertEqual(self.git("branch", "--list", "swarm/T1").strip(), "")
        self.join("bob")
        self.sw("--as", "bob", "claim", "T3")  # T3 needed T1, now done
        self.assertIn("claimed", self.sw("--as", "ann", "next"))

    def test_ec089_gates_run_on_the_rebased_tree(self):
        self.join("ann")
        self.join("bob")
        wt_a = self.to_ready("ann", "T1", "src/a.txt")
        self.sw("--as", "ann", "plan", *PLAN, "--ack-overlap", "x", cwd=wt_a, ok=None)
        with closing(self.db()) as db, db:
            db.execute("UPDATE tasks SET scope_hint='docs/, b.txt' WHERE id='T2'")
        self.sw("--as", "bob", "claim", "T2")
        wt_b = Path(self.task("T2")["worktree"])
        plan_b = [p if p != "scope=src/" else "scope=docs/,b.txt" for p in PLAN]
        self.sw("--as", "bob", "plan", *plan_b, cwd=wt_b)
        self.sw("--as", "bob", "start", cwd=wt_b)
        (wt_b / "b.txt").write_text("b")
        self.sw("--as", "bob", "step", "1", "--evidence", "wrote b.txt file", cwd=wt_b)
        self.git("add", ".", cwd=wt_b)
        self.git("commit", "-qm", "b", cwd=wt_b)
        self.sw("--as", "bob", "step", "2", "--evidence", "committed b.txt", cwd=wt_b)
        self.sw("--as", "bob", "submit", "--evidence", "gates pass alone", cwd=wt_b)
        self.sw("--as", "ann", "merge")
        # a.txt was only in src/a.txt; make the semantic clash explicit: a root a.txt lands on main
        (self.root / "a.txt").write_text("a")
        self.git("add", "a.txt")
        self.git("commit", "-qm", "human adds a.txt")
        out = self.sw("--as", "bob", "merge")
        self.assertIn("Gates failed", out)
        self.assertEqual(self.task("T2")["state"], "gates_failed")

    def test_ec088_conflict_returns_needs_rebase_and_touches_nothing(self):
        self.join("ann")
        self.join("bob")
        self.to_ready("ann", "T1", "src/same.txt", "ann\n")
        with closing(self.db()) as db, db:
            db.execute("UPDATE tasks SET scope_hint='src/' WHERE id='T2'")
        self.sw("--as", "bob", "claim", "T2")
        wt_b = Path(self.task("T2")["worktree"])
        self.sw("--as", "bob", "plan", *PLAN, "--ack-overlap", "we coordinate", cwd=wt_b)
        self.sw("--as", "bob", "start", cwd=wt_b)
        (wt_b / "src").mkdir(exist_ok=True)
        (wt_b / "src" / "same.txt").write_text("bob\n")
        self.sw("--as", "bob", "step", "1", "--evidence", "wrote same.txt", cwd=wt_b)
        self.git("add", ".", cwd=wt_b)
        self.git("commit", "-qm", "b", cwd=wt_b)
        self.sw("--as", "bob", "step", "2", "--evidence", "committed same.txt", cwd=wt_b)
        self.sw("--as", "bob", "submit", "--evidence", "gates pass", cwd=wt_b)
        self.sw("--as", "ann", "merge")
        main_before = self.git("rev-parse", "main")
        out = self.sw("--as", "bob", "merge")
        self.assertIn("conflicts", out)
        self.assertEqual(self.task("T2")["state"], "needs_rebase")
        self.assertEqual(self.git("rev-parse", "main"), main_before)

    def test_ec091_dirty_target_checkout_is_never_overwritten(self):
        self.join("ann")
        self.to_ready("ann", "T1", "src/file.txt", "agent\n")
        (self.root / "src").mkdir()
        (self.root / "src" / "file.txt").write_text("human's unsaved work\n")
        out = self.sw("--as", "ann", "merge")
        self.assertIn("Nothing was touched", out)
        self.assertEqual((self.root / "src" / "file.txt").read_text(), "human's unsaved work\n")
        self.assertEqual(self.task("T1")["state"], "merge_blocked")

    def test_ec091_unrelated_dirt_in_target_checkout_is_kept(self):
        self.join("ann")
        self.to_ready("ann", "T1")
        (self.root / "config" / "settings.txt").write_text("human edit\n")
        self.sw("--as", "ann", "merge")
        self.assertEqual((self.root / "config" / "settings.txt").read_text(), "human edit\n")
        self.assertTrue((self.root / "src" / "file.txt").exists())

    def test_ec086_merge_queue_is_single(self):
        self.join("ann")
        self.join("bob")
        self.to_ready("ann", "T1")
        con = self.db()
        con.execute(
            "INSERT INTO meta(key,value) VALUES('merge_lock', ?)",
            (json.dumps({"agent": "bob", "task": "T2", "at": swarm_module.iso()}),),
        )
        con.commit()
        con.close()
        self.assertIn("merge queue is busy", self.sw("--as", "ann", "merge", ok=False))

    def test_ec087_stale_merge_lock_is_broken(self):
        self.join("ann")
        self.to_ready("ann", "T1")
        con = self.db()
        con.execute(
            "INSERT INTO meta(key,value) VALUES('merge_lock', ?)",
            (json.dumps({"agent": "bob", "task": "T2", "at": "2020-01-01T00:00:00+00:00"}),),
        )
        con.commit()
        con.close()
        self.assertIn("merged T1", self.sw("--as", "ann", "merge"))

    def test_ec124_pause_stops_claims_and_merges(self):
        self.join("ann")
        self.to_ready("ann", "T1")
        self.sw("--as", "ann", "pause", "release freeze")
        self.assertIn("paused", self.sw("--as", "ann", "merge", ok=False))
        self.sw("--as", "ann", "resume")
        self.sw("--as", "ann", "merge")


class Liveness(SwarmCase):
    def test_ec002_ec004_ec040_ec041_silent_holder_is_reclaimed_and_fenced(self):
        self.join("ann")
        self.join("bob")
        self.sw("--as", "ann", "claim", "T1")
        self.sw("--as", "ann", "plan", *PLAN, cwd=Path(self.task("T1")["worktree"]))  # real work: kept for a take-over
        con = self.db()
        con.execute("UPDATE agents SET heartbeat_at='2020-01-01T00:00:00+00:00' WHERE name='ann'")
        con.commit()
        self.sw("status")
        con.execute("UPDATE agents SET offline_at='2020-01-01T00:00:00+00:00' WHERE name='ann'")
        con.commit()
        con.close()
        self.sw("status")
        self.assertEqual(self.task("T1")["state"], "orphaned")
        self.assertIn("--take-over", self.sw("--as", "bob", "claim", "T1", ok=False))
        self.sw("--as", "bob", "claim", "T1", "--take-over")
        self.assertIn("--resume", self.sw("--as", "ann", "plan", *PLAN, ok=False))  # fenced while offline
        out = self.sw("--as", "ann", "join", "--kind", "claude", "--resume")
        self.assertNotIn("you hold T1", out)
        self.assertIn("bob took over T1", self.sw("--as", "ann", "inbox"))
        self.assertIn("holds no task", self.sw("--as", "ann", "plan", *PLAN, ok=False))

    def test_ec009_ec111_ec184_an_agent_that_exits_untouched_returns_its_task_to_the_pool(self):
        release = Path(self.tmp.name) / "allow-worker-exit"
        code = f"import time;from pathlib import Path\np=Path({str(release)!r})\nwhile not p.exists(): time.sleep(0.01)"
        config = self.root / ".agents" / "swarm" / "config.toml"
        config.write_text(
            config.read_text(encoding="utf-8")
            + "\n[adapters.waitexit]\nversion = " + json.dumps([sys.executable, "--version"])
            + "\nauth = []\nrun = " + json.dumps([sys.executable, "-c", code])
            + "\nmodel = []\nunattended = []\nverified = true\n",
            encoding="utf-8",
        )
        self.join("ann")
        out = self.sw("spawn", "waitexit", "--name", "fay", "--task", "T2")
        self.assertIn("spawned fay", out)
        wt = self.worktree_of("T2")
        try:
            self.assertTrue((wt / ".swarm-brief.md").exists())
            self.assertEqual(self.git("status", "--porcelain", cwd=wt), "")  # the brief is ignored, never committed
        finally:
            # Audit the live worktree before allowing the worker's automatic cleanup.
            release.touch()
        for _ in range(50):
            time.sleep(0.2)
            self.sw("status")
            if self.task("T2")["state"] == "open":
                break
        self.assertEqual(self.task("T2")["state"], "open")
        self.assertFalse(wt.exists())
        self.assertIn("back in the pool", self.sw("monitor", "--observe", "--once", "--replay"))
        self.assertIn("claimed T2", self.sw("--as", "ann", "claim", "T2"))

    def test_ec185_work_left_behind_is_offered_by_next(self):
        self.join("ann")
        self.join("bob")
        self.sw("--as", "ann", "claim", "T1")
        self.sw("--as", "ann", "plan", *PLAN, cwd=self.worktree_of("T1"))
        con = self.db()
        con.execute("UPDATE agents SET heartbeat_at='2020-01-01T00:00:00+00:00' WHERE name='ann'")
        con.execute("UPDATE tasks SET state='done' WHERE id IN ('T2','T4','T5','T6')")
        con.commit()
        self.sw("status")  # ann goes offline
        con.execute("UPDATE agents SET offline_at='2020-01-01T00:00:00+00:00' WHERE name='ann'")
        con.commit()
        con.close()
        self.sw("status")  # past the reclaim window
        self.assertEqual(self.task("T1")["state"], "orphaned")  # it has a plan: it keeps its work
        out = self.sw("--as", "bob", "next")
        self.assertIn("left behind by ann", out)
        self.assertEqual(self.task("T1")["assignee"], "bob")

    def worktree_of(self, tid):
        return Path(self.task(tid)["worktree"])

    def test_ec010_pid_alive(self):
        self.assertTrue(swarm_module.pid_alive(os.getpid()))
        self.assertFalse(swarm_module.pid_alive(0))
        self.assertFalse(swarm_module.pid_alive(4_000_000))

    def test_ec012_leave_while_holding(self):
        self.join("ann")
        self.sw("--as", "ann", "claim", "T1")
        self.assertIn("--release", self.sw("--as", "ann", "leave", ok=False))
        self.sw("--as", "ann", "leave", "--release", "nothing done yet, branch is empty")
        self.assertEqual(self.task("T1")["state"], "open")

    def test_ec118_one_monitor(self):
        self.join("ann")
        self.assertIn("SWARM MONITOR", self.sw("monitor", "--once"))
        con = self.db()
        con.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES('monitor', ?)",
            (json.dumps({"who": "pid 1", "at": swarm_module.iso()}),),
        )
        con.commit()
        con.close()
        self.assertIn("already running", self.sw("monitor", "--once", ok=False))


class Backlog(SwarmCase):
    def test_ec024_cycles_refused(self):
        (self.root / "backlog.md").write_text(
            BACKLOG.replace("| T1 | First task | src/ | — |", "| T1 | First task | src/ | T3 |"), encoding="utf-8"
        )
        self.assertIn("cycle", self.sw("init", ok=False))

    def test_ec026_duplicate_ids_refused(self):
        (self.root / "backlog.md").write_text(BACKLOG + "| T1 | Again | x | — | — | |\n")
        self.assertIn("Duplicate", self.sw("init", ok=False))

    def test_ec028_workers_propose_coordinator_approves(self):
        self.join("ann")
        self.join("bob")
        self.assertIn("proposed", self.sw("--as", "bob", "add", "T9", "Follow-up found in T1"))
        self.assertEqual(self.task("T9")["state"], "proposed")
        self.sw("--as", "ann", "approve", "T9")
        self.assertEqual(self.task("T9")["state"], "open")


class Environment(SwarmCase):
    BASE = {
        "os": "linux",
        "os_release": "Linux 6.8",
        "machine": "x86_64",
        "wsl": False,
        "shell": "bash",
        "python": "python3",
        "python_version": "3.12.0",
        "python3_is_store_stub": False,
        "tools": {"git": True, "bash": True, "make": True, "docker": True, "sudo": True},
        "encoding": "UTF-8",
        "utf8_mode": False,
        "git_autocrlf": "(unset)",
        "git_longpaths": "(unset)",
        "case_insensitive_fs": False,
        "path_sep": "/",
    }

    def rules(self, **over):
        return " ".join(swarm_module.environment_rules({**self.BASE, **over}))

    def test_ec128_windows_is_never_described_as_linux(self):
        text = self.rules(
            os="windows",
            os_release="Windows 11",
            shell="git-bash (MINGW64)",
            python="python",
            python3_is_store_stub=True,
            encoding="cp1252",
            tools={"git": True, "bash": True, "make": False, "docker": False},
        )
        for fact in (
            "not Linux",
            "Git Bash",
            "never `python3`",
            "cp1252",
            "no `make`",
            "Docker is not installed",
            "Start-Sleep",
            ".cmd shims",
            "MAX_PATH",
        ):
            self.assertIn(fact, text)

    def test_ec129_a_plain_linux_host_gets_no_windows_rules(self):
        text = self.rules()
        self.assertNotIn("Windows", text)
        self.assertNotIn("cp1252", text)

    def test_ec130_macos_and_wsl_rules(self):
        self.assertIn("sed -i ''", self.rules(os="macos", os_release="Darwin 24"))
        self.assertIn("WSL", self.rules(wsl=True))

    def test_ec131_capabilities_come_from_the_machine(self):
        caps = swarm_module.environment_caps({**self.BASE})
        self.assertIn("linux-docker", caps)
        self.assertNotIn("linux-docker", swarm_module.environment_caps({**self.BASE, "os": "windows"}))
        out = self.join("ann")
        here = swarm_module.detect_environment()["os"]
        self.assertIn(here, out)
        self.assertIn("hivewire environment", out)

    def test_ec132_gates_can_differ_by_os(self):
        here = swarm_module.detect_environment()["os"]
        cfg = self.root / ".agents" / "swarm" / "config.toml"
        cfg.write_text(
            cfg.read_text(encoding="utf-8")
            + f'\n[gates.{here}]\ncommands = [["python", "-c", "import sys; sys.exit(3)"]]\n',
            encoding="utf-8",
        )
        self.git("commit", "-qam", "os gates")
        self.join("ann")
        self.to_ready("ann", "T1")
        self.assertIn("exited 3", self.sw("--as", "ann", "merge"))

    def test_ec133_environment_command_and_json(self):
        data = json.loads(self.sw("environment", "--json"))["result"]
        self.assertIn("rules", data)
        self.assertIn(data["os"], ("windows", "linux", "macos"))


class FirstRealRun(SwarmCase):
    """Edge cases found when real agents used v0.1.0 in a repository with work already in flight."""

    def test_ec042_ec141_branch_from_outside_the_board_needs_adopt(self):
        self.git("branch", "swarm/T1")
        self.git("commit", "--allow-empty", "-qm", "outside work")  # on main; branch exists beside it
        self.join("ann")
        out = self.sw("--as", "ann", "claim", "T1", ok=False)
        self.assertIn("--adopt", out)
        self.assertNotIn("T1", self.sw("--as", "ann", "next").split("\n")[0])  # next skips it
        self.sw("--as", "ann", "release", "picked the wrong one, giving it back")
        self.sw("--as", "ann", "claim", "T1", "--adopt")
        self.assertIn("adopted", self.sw("log", "--task", "T1"))

    def test_ec142_rubber_stamped_steps_are_flagged(self):
        self.join("ann")
        self.to_ready("ann", "T1")
        self.assertIn("rapid_completion", self.sw("log", "--task", "T1"))

    def test_ec143_only_the_setter_or_a_human_lifts_a_pause(self):
        self.join("ann")
        self.join("bob")
        self.sw("--as", "ann", "pause", "two boards are live")
        self.sw("--as", "ann", "leave")
        self.assertIn("only they or a human", self.sw("--as", "bob", "resume", ok=False))
        self.sw("resume")  # a human, no --as

    def test_ec144_relative_gate_paths_resolve_from_the_worktree(self):
        here = swarm_module.detect_environment()["os"]
        worktree = Path(self.tmp.name) / "repo-wt" / "T1"
        rel = os.path.relpath(sys.executable, worktree.resolve()).replace("\\", "/")
        cfg = self.root / ".agents" / "swarm" / "config.toml"
        cfg.write_text(
            cfg.read_text(encoding="utf-8") + f'\n[gates.{here}]\ncommands = [["{rel}", "-c", "print(1)"]]\n',
            encoding="utf-8",
        )
        self.git("commit", "-qam", "relative gate")
        self.join("ann")
        self.to_ready("ann", "T1")
        self.assertIn("merged T1", self.sw("--as", "ann", "merge"))

    def test_ec147_work_merged_outside_the_board_is_recorded(self):
        self.join("ann")
        self.sw("--as", "ann", "claim", "T1")
        (self.root / "landed.txt").write_text("x", encoding="utf-8")
        self.git("add", "landed.txt")
        self.git("commit", "-qm", "landed elsewhere")
        sha = self.git("rev-parse", "HEAD").strip()
        self.git("branch", "elsewhere")
        self.git("commit", "--allow-empty", "-qm", "not yet")
        self.sw("--as", "ann", "release", "the work landed elsewhere, giving it back")
        outside = self.git("rev-parse", "elsewhere").strip()
        self.assertIn("recorded", self.sw("record", "T1", "--commit", outside))
        self.assertEqual(self.task("T1")["state"], "done")
        self.sw("--as", "ann", "claim", "T3")  # T3 needed T1
        self.git("checkout", "-q", "-b", "side", sha)
        self.git("commit", "--allow-empty", "-qm", "only on a side branch")
        side = self.git("rev-parse", "HEAD").strip()
        self.git("checkout", "-q", "main")
        self.assertIn("not in main's history", self.sw("record", "T2", "--commit", side, ok=False))

    def test_ec145_dry_run_spawn_changes_nothing(self):
        out = self.sw("spawn", "fake", "--name", "fay", "--task", "T2", "--dry-run")
        self.assertIn("would run", out)
        self.assertEqual(self.task("T2")["state"], "open")
        self.assertNotIn("fay", self.sw("agents"))


class Console(SwarmCase):
    """v0.2.0: the console, interactive agents, worktree files, version and update."""

    PANES = [{"title": f"p{i}", "argv": ["python", "hivewire.py", f"cmd{i}"]} for i in range(4)]

    def test_ec148_git_ignored_local_files_follow_into_worktrees(self):
        cfg = self.root / ".agents" / "swarm" / "config.toml"
        cfg.write_text(
            cfg.read_text(encoding="utf-8") + '\n[worktree]\ncopy = [".env", "local.txt"]\n', encoding="utf-8"
        )
        (self.root / ".gitignore").write_text("__pycache__/\n.env\n", encoding="utf-8")
        self.git("commit", "-qam", "worktree copy")
        (self.root / ".env").write_text("SECRET=x\n", encoding="utf-8")
        (self.root / "local.txt").write_text("not ignored\n", encoding="utf-8")
        self.join("ann")
        out = self.sw("--as", "ann", "claim", "T1")
        self.assertIn("copied    .env", out)
        self.assertIn("not copied: local.txt", out)
        wt = Path(self.task("T1")["worktree"])
        self.assertTrue((wt / ".env").exists())
        self.assertFalse((wt / "local.txt").exists())
        self.assertEqual(self.git("status", "--porcelain", cwd=wt), "")

    def test_ec149_the_coordinator_opens_the_workspace_without_asking(self):
        self.assertIn("OPEN WORKSPACE NOW", self.join("ann"))
        self.join("bob")
        self.assertNotIn("OPEN WORKSPACE NOW", self.sw("--as", "bob", "whoami"))
        self.sw("--as", "ann", "console", "--decline")
        self.assertNotIn("OPEN WORKSPACE NOW", self.sw("--as", "ann", "whoami"))

    def test_ec150_console_observes_when_a_monitor_runs(self):
        self.assertNotIn("--observe", self.sw("console", "--dry-run"))
        con = self.db()
        con.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES('monitor', ?)",
            (json.dumps({"who": "pid 1", "at": swarm_module.iso()}),),
        )
        con.commit()
        con.close()
        self.assertIn("--observe", self.sw("console", "--dry-run"))
        self.assertIn("observing", self.sw("monitor", "--observe", "--once"))

    def test_ec151_a_pane_command_with_a_separator_is_refused(self):
        bad = [{"title": "x", "argv": ["python", "a;b"]}]
        with self.assertRaises(swarm_module.SwarmError):
            swarm_module.check_pane_commands(bad)
        swarm_module.check_pane_commands(self.PANES)

    def test_ec152_every_platform_gets_a_layout(self):
        plan = swarm_module.plan_console
        has = lambda *tools: lambda t: t in tools  # noqa: E731
        wt = plan(self.PANES, "C:/repo", "windows", has("wt"))
        self.assertEqual(wt["kind"], "windows-terminal")
        self.assertEqual(wt["steps"][0][0], "wt")
        self.assertEqual(wt["steps"][0].count("split-pane"), 3)
        self.assertEqual(wt["steps"][0].count("C:/repo"), 4)
        self.assertEqual(plan(self.PANES, "C:/repo", "windows", has())["kind"], "separate-windows")
        tmux = plan(self.PANES, "/repo", "linux", has("tmux", "gnome-terminal"), display=True)
        self.assertEqual(tmux["kind"], "tmux")
        self.assertEqual(tmux["window"][:2], ["gnome-terminal", "--"])
        self.assertEqual(sum(1 for s in tmux["steps"] if "split-window" in s), 3)
        self.assertIsNone(plan(self.PANES, "/repo", "linux", has("tmux"), display=False)["window"])
        mac = plan(self.PANES, "/repo", "macos", has())
        self.assertEqual(mac["kind"], "separate-windows")
        self.assertTrue(all(step[0] == "osascript" for step in mac["steps"]))
        self.assertEqual(plan(self.PANES, "/repo", "linux", has(), display=False)["kind"], "manual")

    def test_ec153_ec155_launch_prepares_nothing_on_a_dry_run(self):
        out = self.sw("launch", "fake", "--name", "fay", "--task", "T2", "--dry-run")
        self.assertIn("would run fay interactively", out)
        self.assertEqual(self.task("T2")["state"], "open")
        self.assertNotIn("fay", self.sw("agents"))
        self.assertIn("paste this one", self.sw("launch", "fakeplain", "--name", "pia", "--dry-run"))

    def test_ec154_agent_panes_get_distinct_names(self):
        out = self.sw("console", "--agents", "fake,fake", "--dry-run")
        self.assertIn("fake-1 (fake)", out)
        self.assertIn("fake-2 (fake)", out)
        self.assertIn("5 panes", out)

    def test_dashboard_renders_once(self):
        self.join("ann")
        out = self.sw("dashboard", "--once")
        self.assertIn("SWARM BOARD", out)
        self.assertIn("recent", out)
        self.assertIn("next up: T1", out)

    def test_ec158_version_and_update(self):
        self.assertIn(swarm_module.__version__, self.sw("version"))
        out = self.sw("update", "--dry-run")
        self.assertIn("install.py", out)
        self.assertIn("--project", out)


class Goals(SwarmCase):
    """v0.3.0: from a piece of work to assigned tasks, through hard questions, backward and forward."""

    def answer_all(self, goal, skip=()):
        args = []
        for key in swarm_module.GOAL_KEYS:
            if key not in skip:
                args += ["--set", f"{key}=" + ("none" if key == "unknowns" else f"a considered answer about {key}")]
        self.sw("goal", "answer", goal, *args)

    def build(self, caps_docs=""):
        self.sw("goal", "new", "GL", "Ship the health endpoint")
        self.answer_all("GL")
        self.sw(
            "goal",
            "outcome",
            "GL",
            "GET /health answers 200 with the version",
            "--check",
            "curl localhost/health returns 200",
        )
        self.sw(
            "goal",
            "outcome",
            "GL",
            "the health endpoint is covered by tests",
            "--check",
            "python -m pytest tests/health passes",
        )
        self.sw(
            "goal",
            "task",
            "GL",
            "H-api",
            "Add the endpoint",
            "--serves",
            "1",
            "--done-when",
            "the endpoint returns 200 locally",
        )
        self.sw(
            "goal",
            "task",
            "GL",
            "H-tests",
            "Test the endpoint",
            "--serves",
            "2",
            "--needs",
            "H-api",
            "--done-when",
            "the endpoint tests pass on this machine",
        )
        extra = ["--caps", caps_docs] if caps_docs else []
        self.sw(
            "goal",
            "task",
            "GL",
            "H-docs",
            "Document the endpoint",
            "--serves",
            "1",
            "--done-when",
            "the API guide lists /health and its answer",
            *extra,
        )
        self.architecture("GL")

    def test_ec159_no_breakdown_before_the_hard_questions(self):
        self.sw("goal", "new", "GQ", "Make sign-in faster")
        self.assertIn("unanswered questions", self.sw("goal", "review", "GQ", ok=False))
        self.assertIn("too thin", self.sw("goal", "answer", "GQ", "--set", "done_looks_like=faster", ok=False))
        self.answer_all("GQ", skip=("riskiest_assumption",))
        self.sw("goal", "answer", "GQ", "--set", "riskiest_assumption=unknown")
        self.sw(
            "goal",
            "outcome",
            "GQ",
            "sign-in completes in under one second",
            "--check",
            "the sign-in timing test reports under 1000 ms",
        )
        self.sw(
            "goal",
            "task",
            "GQ",
            "Q-cache",
            "Cache the discovery document",
            "--serves",
            "1",
            "--done-when",
            "the timing test passes twice in a row",
        )
        out = self.sw("goal", "review", "GQ", ok=False)
        self.assertIn("is open", out)
        self.assertIn("review first", self.sw("goal", "commit", "GQ", ok=False))
        self.sw("answer", "1", "the provider's response time; measure it before caching anything")
        self.architecture("GQ")
        self.sw("goal", "review", "GQ")

    def test_ec173_an_unknown_is_not_counted_as_answered(self):
        self.sw("goal", "new", "GU", "Make exports resumable")
        self.answer_all("GU", skip=("wrong_if",))
        out = self.sw("goal", "answer", "GU", "--set", "wrong_if=unknown")
        self.assertIn("14 of 15 questions answered with substance", out)
        self.assertIn("waiting on: wrong_if", out)
        self.assertIn("became Q1, asked of human", out)

    def test_backward_and_forward_views_then_commit(self):
        self.join("ann")
        self.build()
        out = self.sw("goal", "review", "GL")
        self.assertIn("wave 1 (now): H-api, H-docs", out)
        self.assertIn("wave 2 (after wave 1): H-tests", out)
        self.assertIn("<- H-tests", out)
        self.assertIn("review passed", out)
        self.assertIn("is draft", self.sw("--as", "ann", "claim", "H-api", ok=False))  # drafts are not work yet
        self.sw("goal", "commit", "GL")
        self.assertIn("claimed H-api", self.sw("--as", "ann", "claim", "H-api"))

    def test_ec160_work_that_serves_no_outcome_is_refused(self):
        self.build()
        out = self.sw(
            "goal",
            "task",
            "GL",
            "H-extra",
            "Refactor the router",
            "--serves",
            "9",
            "--done-when",
            "the router is tidier than before",
            ok=False,
        )
        self.assertIn("serves no outcome", out)

    def test_ec161_an_outcome_nobody_works_toward(self):
        self.build()
        self.sw(
            "goal",
            "outcome",
            "GL",
            "operators are alerted when health fails",
            "--check",
            "stopping the service raises an alert within a minute",
        )
        self.assertIn("outcome 3 has no task", self.sw("goal", "review", "GL", ok=False))

    def test_ec162_a_task_without_a_done_check(self):
        self.build()
        self.assertIn(
            "--done-when",
            self.sw("goal", "task", "GL", "H-x", "Something", "--serves", "1", "--done-when", "ok", ok=False),
        )

    def test_ec163_large_tasks_must_be_split(self):
        self.build()
        self.sw(
            "goal",
            "task",
            "GL",
            "H-big",
            "Rewrite monitoring",
            "--serves",
            "1",
            "--size",
            "L",
            "--done-when",
            "every monitor reports through the new pipeline",
        )
        self.assertIn("split it", self.sw("goal", "review", "GL", ok=False))

    def test_ec164_a_task_needing_something_that_does_not_exist(self):
        self.build()
        self.assertIn(
            "not a task",
            self.sw(
                "goal",
                "task",
                "GL",
                "H-y",
                "Wire it",
                "--serves",
                "1",
                "--needs",
                "H-nowhere",
                "--done-when",
                "the wiring test passes",
                ok=False,
            ),
        )

    def test_ec165_ec166_previous_and_next(self):
        self.build()
        self.sw("goal", "commit", "GL")
        con = self.db()
        con.execute(
            "UPDATE tasks SET state='done', merged_sha='abc1234567890', submit=? WHERE id='H-api'",
            (
                json.dumps(
                    {"changelog": "added GET /health", "evidence": "curl returned 200", "files": ["src/api/health.py"]}
                ),
            ),
        )
        con.commit()
        con.close()
        prev = self.sw("context", "--task", "H-tests")
        for text in (
            "serves outcome 2",
            "<- H-api",
            "produced: added GET /health",
            "proved by: curl returned 200",
            "src/api/health.py",
        ):
            self.assertIn(text, prev)
        nxt = self.sw("context", "--task", "H-api")
        self.assertIn("-> H-tests", nxt)
        self.assertIn("it must then be able to: the endpoint tests pass", nxt)
        self.join("ann")
        self.assertIn("builds on (previous):", self.sw("--as", "ann", "claim", "H-tests"))

    def test_ec167_the_first_wave_goes_to_machines_that_can_verify_it(self):
        self.join("ann")
        self.join("bob", caps="gpu-x")
        self.build(caps_docs="gpu-x")
        out = self.sw("--as", "ann", "goal", "commit", "GL", "--assign")
        self.assertIn("H-docs -> bob", out)
        self.assertIn("H-api -> ann", out)
        self.assertNotIn("H-tests ->", out)  # it waits for H-api

    def test_ec169_forged_done_rows_and_prose_do_not_close_an_unverified_goal(self):
        self.build()
        self.sw("goal", "commit", "GL")
        con = self.db()
        con.execute("UPDATE tasks SET state='done' WHERE goal='GL'")
        con.commit()
        con.close()
        out = self.sw("goal", "close", "GL", "--evidence", "1=curl returned 200 with v0.3.0", ok=False)
        self.assertIn("no landed completion", out)
        self.sw(
            "goal",
            "close",
            "GL",
            "--evidence",
            "1=curl returned 200 with v0.3.0",
            "--evidence",
            "2=pytest tests/health: 4 passed",
            ok=False,
        )
        self.assertIn("active", self.sw("goal", "list"))


class Output(SwarmCase):
    def test_ec170_text_outside_the_legacy_code_page_survives_a_pipe(self):
        self.join("ann")
        self.sw("--as", "ann", "add", "T-utf", "Map the flow \u2192 extraction \u00a72 \u2713")
        out = self.sw("status", "--all")
        self.assertIn("Map the flow \u2192 extraction \u00a72 \u2713", out)
        self.sw("--as", "ann", "send", "*", "done \u2192 next")
        self.join("bob")
        self.sw("--as", "ann", "send", "bob", "rebase \u2192 now")
        self.assertIn("rebase \u2192 now", self.sw("--as", "bob", "inbox"))


class ExampleContent(SwarmCase):
    OLD_EXAMPLE = """| Id | Title | Scope | Needs | Locks | Caps |
|---|---|---|---|---|---|
| EX-docs | Write the contributor quick start | `docs/` | — | — | |
| EX-api | Add the health endpoint | `src/api/` | — | — | |
"""

    def test_ec171_tasks_imported_from_the_old_example_are_withdrawn(self):
        self.sw("status")  # imports the real backlog
        con = self.db()
        for tid, title in (("EX-docs", "Write the contributor quick start"), ("EX-api", "Add the health endpoint")):
            con.execute(
                "INSERT INTO tasks(id,title,state,needs,caps,locks,ord,created_at,updated_at) "
                "VALUES(?,?,'open','[]','[]','[]',99,'x','x')",
                (tid, title),
            )
        con.commit()
        con.close()
        (self.root / "backlog.md").write_text(self.OLD_EXAMPLE, encoding="utf-8")
        out = self.sw("sync")
        self.assertIn("example backlog", out)
        self.assertNotIn("EX-", self.sw("status"))

    def test_ec172_a_missing_backlog_file_is_reported_not_fatal(self):
        (self.root / "backlog.md").unlink()
        self.assertIn("does not exist", self.sw("status"))


class ReadableConsole(SwarmCase):
    """v0.4.0: what a person sees in the console."""

    NARROW = {"COLUMNS": "60", "LINES": "18"}

    def test_ec174_views_fit_their_pane(self):
        self.join("ann")
        self.join("bob")
        self.sw("--as", "ann", "claim", "T1")
        board = self.sw("dashboard", "--once", env=self.NARROW).rstrip("\n").split("\n")
        self.assertLessEqual(len(board), 18)
        self.assertTrue(all(len(line) <= 60 for line in board), board)
        self.assertIn("SWARM BOARD", board[0])
        events = self.sw("monitor", "--observe", "--once", "--replay", env=self.NARROW).splitlines()
        self.assertTrue(all(len(line) <= 60 for line in events), events)

    def test_ec175_events_read_as_sentences_in_local_time(self):
        self.join("ann")
        self.sw("--as", "ann", "claim", "T1")
        out = self.sw("monitor", "--observe", "--once", "--replay")
        self.assertIn("ann joined: claude; caps", out)
        self.assertIn("ann claimed T1", out)
        self.assertNotIn("+00:00", out)
        self.assertNotIn("['", out)
        internals = {
            "kind": "goal_task",
            "agent": "x",
            "task": "T9",
            "at": swarm_module.iso(),
            "detail": "G, serves [2], needs ['T1']",
        }
        text = swarm_module.describe_event(internals)[1]
        self.assertNotIn("[", text)
        claimed = {"kind": "claimed", "agent": "x", "task": "T9", "at": swarm_module.iso(), "detail": "epoch 3"}
        self.assertEqual(swarm_module.describe_event(claimed)[1], "x claimed T9")
        old = {
            "kind": "agent_joined",
            "agent": "x",
            "task": None,
            "at": swarm_module.iso(),
            "detail": "claude sonnet caps=['bash', 'windows']",
        }
        self.assertEqual(swarm_module.describe_event(old)[1], "x joined: claude sonnet caps bash, windows")

    def test_ec177_leaving_the_launcher_says_how_to_come_back(self):
        out = self.sw("launcher", input_text="q\n")
        self.assertIn("Launcher closed", out)
        self.assertIn("launcher", out.split("Launcher closed", 1)[1])

    def test_ec178_unsupervised_no_task_launch_is_refused(self):
        out = self.sw("launch", "fake", "--name", "fay", "--no-task", "--dry-run", ok=False)
        self.assertIn("unrecognized arguments", out)
        self.assertNotIn("fay", self.sw("agents"))

    def test_ec179_the_launcher_offers_a_chosen_task(self):
        out = self.sw("launcher", input_text="fake\nt\n1\n\n", env={"WT_SESSION": "", "TMUX": ""})
        self.assertIn("1. T1", out)
        self.assertIn("fake-1 (fake) takes T1", out)

    def test_ec180_the_board_gets_the_big_pane_and_agents_their_own_tab(self):
        panes = [
            {"title": "swarm board", "argv": ["python", "s.py", "dashboard"], "tab": "swarm"},
            {"title": "swarm monitor", "argv": ["python", "s.py", "monitor"], "tab": "swarm"},
            {"title": "start agents", "argv": ["python", "s.py", "launcher"], "tab": "swarm"},
            {"title": "a-1", "argv": ["python", "s.py", "launch", "a"], "tab": "agents"},
        ]
        wt = swarm_module.plan_console(panes, "C:/repo", "windows", lambda t: t == "wt")["steps"][0]
        self.assertEqual(wt[wt.index("--title") + 1], "swarm board")
        self.assertEqual(wt.count("new-tab"), 2)
        self.assertIn("focus-tab", wt)
        tmux = swarm_module.plan_console(panes, "/repo", "linux", lambda t: t == "tmux", display=False)["steps"]
        self.assertTrue(any("main-vertical" in step for step in tmux))
        self.assertTrue(any(step[1] == "new-window" for step in tmux))


class Adapters(unittest.TestCase):
    def test_kilocode_is_a_supported_cli(self):
        ad = swarm_module.DEFAULTS["adapters"]["kilocode"]
        self.assertEqual(ad["run"][:2], ["kilocode", "--auto"])
        self.assertIn("{prompt}", ad["run"])
        self.assertIn("{cwd}", ad["run"])
        self.assertEqual(ad["unattended"], ["--yolo"])
        self.assertEqual(ad["auth"], [])  # `kilocode auth` starts an interactive login: never run it in a probe
        self.assertIn("{prompt}", swarm_module.INTERACTIVE["kilocode"])

    def test_ec182_a_cli_that_changed_its_options_is_caught(self):
        self.assertEqual(swarm_module.missing_options(["python", "-c", "x"], ["python", "--help"]), [])
        gone = swarm_module.missing_options(["python", "--no-such-option", "x"], ["python", "--help"])
        self.assertEqual(gone, ["--no-such-option"])
        self.assertEqual(swarm_module.missing_options(["x", "--y"], ["no-such-cli-anywhere", "--help"]), [])

    def test_qodercli_starts_with_its_instructions(self):
        self.assertIn("{prompt}", swarm_module.INTERACTIVE["qodercli"])
        self.assertNotIn("--max-turns", swarm_module.DEFAULTS["adapters"]["qodercli"]["run"])

    def test_ec181_no_adapter_uses_a_cli_s_own_branching(self):
        """A CLI's own parallel or worktree mode would make branches beside the swarm's worktree."""
        own_branching = {
            "kilocode": {"--parallel", "-p", "--existing-branch", "-eb"},
            "qodercli": {"--worktree", "--branch", "--path"},
        }
        for kind, flags in own_branching.items():
            ad = swarm_module.DEFAULTS["adapters"][kind]
            used = set(ad["run"]) | set(ad["unattended"]) | set(swarm_module.INTERACTIVE[kind])
            self.assertFalse(used & flags, f"{kind} uses its own branching: {used & flags}")


class LongGates(SwarmCase):
    """v0.4.2: a gate that runs long keeps its merge alive; a merge that lost the queue never moves main."""

    def gate(self, code):
        here = swarm_module.detect_environment()["os"]
        cfg = self.root / ".agents" / "swarm" / "config.toml"
        text = cfg.read_text(encoding="utf-8").replace("[gates]", "[gates]\nkeepalive_sec = 1", 1)
        cfg.write_text(text + f"\n[gates.{here}]\ncommands = [['python', '-c', '{code}']]\n", encoding="utf-8")
        self.git("commit", "-qam", "gate")

    def wait_for(self, probe, seconds=90):
        """Poll until `probe()` is truthy. The tests wait for conditions, never for a fixed time: under load a fixed
        sleep is a race."""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = probe()
            if value:
                return value
            time.sleep(0.2)
        self.fail("timed out waiting for a condition")

    def lock_at(self):
        con = self.db()
        row = con.execute("SELECT value FROM meta WHERE key='merge_lock'").fetchone()
        con.close()
        return json.loads(row[0])["at"] if row else None

    def test_ec187_a_long_gate_keeps_the_merging_agent_alive(self):
        # The gate blocks until the test releases it, so how long the merge takes cannot matter.
        hold = 'import os,time;f=os.environ["SWARM_HOLD"];t=time.time();'
        hold += "[time.sleep(0.1) for _ in iter(lambda: not os.path.exists(f) and time.time()-t<120, False)]"
        self.gate(hold)
        self.join("ann")
        self.to_ready("ann", "T1")
        release = Path(self.tmp.name) / "release-gate"
        proc = subprocess.Popen(
            [sys.executable, str(SWARM), "merge", "--as", "ann"],
            cwd=self.root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            env={**os.environ, "SWARM_HOLD": str(release)},
        )
        try:
            first = self.wait_for(self.lock_at)  # the merge has taken the queue and is inside its gate
            renewed = self.wait_for(lambda: (at := self.lock_at()) and at != first and at)
            con = self.db()
            beat = con.execute("SELECT heartbeat_at FROM agents WHERE name='ann'").fetchone()[0]
            con.close()
            self.assertGreater(renewed, first)  # the lease was renewed while the gate was still running
            self.assertGreaterEqual(beat, first)
            release.write_text("go", encoding="utf-8")
            out = proc.communicate(timeout=120)[0]
        finally:
            if proc.poll() is None:  # a failed assertion must not leave the merge holding the database
                release.write_text("go", encoding="utf-8")
                proc.kill()
                proc.communicate()
        self.assertIn("merged T1", out)

    def test_ec188_a_merge_that_lost_the_queue_never_moves_main(self):
        steal = (
            'import sqlite3,json,os;c=sqlite3.connect(os.environ["SWARM_DB"]);'
            'c.execute("update meta set value=? where key=?",(json.dumps({"agent":"zed","task":"T9",'
            '"at":"2099-01-01T00:00:00+00:00"}),"merge_lock"));c.commit()'
        )
        self.gate(steal)
        self.join("ann")
        self.to_ready("ann", "T1")
        before = self.git("rev-parse", "main")
        out = self.sw("--as", "ann", "merge", env={"SWARM_DB": str(self.root / ".git" / "swarm" / "swarm.db")})
        self.assertIn("lost its place in the merge queue", out)
        self.assertEqual(self.git("rev-parse", "main"), before)
        self.assertEqual(self.task("T1")["state"], "ready_to_merge")


class CrashRecovery(SwarmCase):
    """v0.4.3: the target moved, the worker ended, the board never heard."""

    def merge_dies(self, who, point):
        done = subprocess.run(
            [sys.executable, str(SWARM), "merge", "--as", who],
            cwd=self.root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
            env={**os.environ, "SWARM_FAILPOINT": point},
        )
        self.assertEqual(done.returncode, 97, done.stdout + done.stderr)

    def completions(self, tid):
        con = self.db()
        n = con.execute(
            "SELECT COUNT(*) FROM events WHERE task=? AND kind IN ('merged','merge_recovered')", (tid,)
        ).fetchone()[0]
        con.close()
        return n

    def meta(self, key):
        con = self.db()
        row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        con.close()
        return row[0] if row else None

    def test_ec189_ec192_ec194_the_target_moved_and_the_worker_ended(self):
        self.join("ann")
        self.to_ready("ann", "T1")
        self.merge_dies("ann", "after_target_moved")
        self.assertEqual(self.git("show", "main:src/file.txt"), "x\n")  # git has the work
        self.assertEqual(self.task("T1")["state"], "ready_to_merge")  # the board does not know
        self.assertIsNotNone(self.task("T1")["merge_intent"])
        self.assertIsNotNone(self.meta("merge_lock"))
        self.sw("status")  # any command sweeps
        t = self.task("T1")
        self.assertEqual(t["state"], "done")
        self.assertEqual(t["merged_sha"], self.git("rev-parse", "main").strip())
        self.assertIsNone(t["merge_intent"])
        self.assertIsNone(self.meta("merge_lock"))
        self.assertEqual(self.completions("T1"), 1)
        for _ in range(3):
            self.sw("status")
        self.assertEqual(self.completions("T1"), 1)  # exactly once, however often it is checked
        self.assertIn("already merged", self.sw("--as", "ann", "merge"))  # a retry after the lost acknowledgement
        self.assertEqual(self.completions("T1"), 1)
        self.assertIn("claimed T3", self.sw("--as", "ann", "claim", "T3"))  # and its dependents are unblocked

    def test_ec190_the_worker_ended_before_the_target_moved(self):
        self.join("ann")
        self.to_ready("ann", "T1")
        before = self.git("rev-parse", "main")
        self.merge_dies("ann", "after_intent")
        self.assertEqual(self.git("rev-parse", "main"), before)
        con = self.db()
        con.execute("UPDATE agents SET heartbeat_at='2020-01-01T00:00:00+00:00' WHERE name='ann'")
        con.commit()
        con.close()
        self.sw("status")  # ann is silent: the merge cannot still be running
        t = self.task("T1")
        self.assertNotEqual(t["state"], "done")
        self.assertIsNone(t["merge_intent"])
        self.assertIsNone(self.meta("merge_lock"))
        self.assertEqual(self.git("rev-parse", "main"), before)
        self.sw("--as", "ann", "join", "--kind", "claude", "--resume")
        self.assertEqual(self.task("T1")["state"], "planning")
        self.sw("--as", "ann", "start")
        self.sw("--as", "ann", "submit", "--evidence", "Retained committed work is ready for the configured merge gate")
        self.assertIn("merged T1", self.sw("--as", "ann", "merge"))

    def test_ec191_recovery_trusts_only_git(self):
        self.join("ann")
        self.to_ready("ann", "T1")
        tip = self.git("rev-parse", "swarm/T1").strip()
        for bogus in ("0123456789abcdef0123456789abcdef01234567", "--all", "not-a-sha", "", tip):
            con = self.db()
            con.execute("UPDATE tasks SET merge_intent=? WHERE id='T1'", (json.dumps({"sha": bogus}),))
            con.commit()
            con.close()
            self.sw("status")
            self.assertEqual(self.task("T1")["state"], "ready_to_merge", repr(bogus))
        self.assertNotIn("src/file.txt", self.git("ls-tree", "-r", "--name-only", "main"))

    def test_ec192_completing_twice_changes_nothing_the_second_time(self):
        self.join("ann")
        self.to_ready("ann", "T1")
        sw = swarm_module.Swarm(self.root)
        try:
            with sw.tx():
                first = swarm_module.complete_merge(sw, "T1", "a" * 40, "msg", "log", ("merged", "first"))
            with sw.tx():
                second = swarm_module.complete_merge(sw, "T1", "b" * 40, "msg", "log", ("merge_recovered", "second"))
        finally:
            sw.db.close()
        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(self.task("T1")["merged_sha"], "a" * 40)
        self.assertEqual(self.completions("T1"), 1)

    def test_ec193_a_dead_holders_queue_lease_is_broken_at_once(self):
        self.join("ann")
        self.join("bob")
        con = self.db()
        con.execute(
            "INSERT INTO meta(key,value) VALUES('merge_lock', ?)",
            (json.dumps({"agent": "ann", "task": "T1", "at": swarm_module.iso()}),),
        )
        con.execute("UPDATE agents SET heartbeat_at='2020-01-01T00:00:00+00:00' WHERE name='ann'")
        con.commit()
        con.close()
        self.sw("status")
        self.assertIsNone(self.meta("merge_lock"))  # a fresh lease, but its holder is gone
        self.assertIn("its holder is offline", self.sw("log"))


class Latency(SwarmCase):
    """v0.4.4: a command must stay cheap. Pinned by counting work (git processes, modules loaded), never by timing it:
    a timing assertion fails at random on a busy machine."""

    def in_repo(self, code):
        script = f"import sys; sys.path.insert(0, {str(SWARM.parent)!r}); import swarm_core\n" + code
        done = subprocess.run(
            [sys.executable, "-c", script], cwd=self.root, capture_output=True, text=True, encoding="utf-8", check=False
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        return done.stdout.strip().splitlines()

    def measure(self, argv):
        return self.in_repo(f"""
import contextlib, io, json, subprocess
from pathlib import Path
calls = []
real = subprocess.run
def counting(*a, **k):
    calls.append(a[0] if a else k.get("args"))
    return real(*a, **k)
subprocess.run = counting
with contextlib.redirect_stdout(io.StringIO()):
    swarm_core.main({argv!r})
git_calls = [c for c in calls if isinstance(c, (list, tuple)) and c and Path(c[0]).name.lower() in ("git", "git.exe")]
print(len(git_calls))
print(" ".join(m for m in ("urllib.request", "http.client", "ssl", "textwrap", "tomllib") if m in sys.modules) or "-")
print(json.dumps([c for c in calls if c not in git_calls]))
""")

    def test_ec195_a_command_starts_no_git_process_and_loads_no_network_code(self):
        self.join("ann")
        for argv in (["version"], ["status"], ["agents"], ["whoami", "--as", "ann"]):
            git_calls, loaded, other = self.measure(argv)
            self.assertEqual(git_calls, "0", f"{argv}: git was started {git_calls} time(s)")
            self.assertEqual(loaded, "-", f"{argv} imported {loaded} at start-up")
            self.assertEqual(json.loads(other), [], f"{argv} started a non-Git command")
        git_calls, loaded, other = self.measure(["environment", "--json"])
        self.assertEqual(git_calls, "1", "the environment probe asks git once (core.autocrlf and core.longpaths)")
        self.assertEqual(loaded, "-")
        # Python 3.11's Windows platform.win32_ver() invokes the native `ver` query.
        # Count that bounded OS probe explicitly; it is not an additional Git call.
        allowed_other = [[], ["ver"]] if os.name == "nt" and sys.version_info < (3, 12) else [[]]
        self.assertIn(json.loads(other), allowed_other)

    def test_ec199_the_config_is_parsed_once_and_a_change_is_noticed(self):
        self.join("ann")
        cache = self.root / ".git" / "swarm" / "config.cache.json"
        self.assertTrue(cache.exists())
        self.assertEqual(self.measure(["status"])[1], "-")  # tomllib is not even imported
        self.assertIn("1 of 5", self.sw("status"))
        cfg = self.root / ".agents" / "swarm" / "config.toml"
        cfg.write_text(cfg.read_text(encoding="utf-8").replace("max_agents = 5", "max_agents = 3"), encoding="utf-8")
        self.assertIn("1 of 3", self.sw("status"))  # the edit invalidated the cache
        cache.write_text("{not json", encoding="utf-8")  # a damaged cache is rebuilt, never trusted
        self.assertIn("1 of 3", self.sw("status"))

    def test_which_many_agrees_with_shutil_which(self):
        names = ["python", "git", "definitely-not-installed-anywhere-xyz"]
        found = swarm_module.which_many(names)
        for name in names:
            want, got = shutil.which(name), found[name]
            self.assertEqual(bool(got), bool(want), name)
            if want and got:
                self.assertTrue(os.path.samefile(want, got), (want, got))

    def test_ec198_the_git_directory_is_found_without_git_and_agrees_with_git(self):
        (self.root / "src" / "deep").mkdir(parents=True)
        wt = Path(self.tmp.name) / "linked"
        self.git("worktree", "add", "-q", "-b", "side", str(wt), "main")
        (wt / "sub").mkdir()
        for where in (self.root, self.root / "src" / "deep", wt, wt / "sub"):
            fast = swarm_module.find_git_common(where)
            real = Path(self.git("rev-parse", "--path-format=absolute", "--git-common-dir", cwd=where).strip())
            self.assertIsNotNone(fast, where)
            self.assertTrue(os.path.samefile(fast, real), (where, fast, real))
        board = swarm_module.Swarm(wt / "sub")
        try:
            self.assertTrue(os.path.samefile(board.main_root, self.root))
            self.assertTrue(os.path.samefile(board.common, self.root / ".git"))
        finally:
            board.db.close()
        with mock.patch.dict(os.environ, {"GIT_DIR": str(self.root / ".git")}):
            self.assertIsNone(swarm_module.find_git_common(self.root))  # unusual setups ask git
        self.assertIsNone(swarm_module.find_git_common(Path(self.tmp.name)))  # not a repository

    def test_the_os_name_needs_no_environment_probe(self):
        self.assertEqual(swarm_module.current_os(), swarm_module.detect_environment()["os"])


class ReusedWorktree(SwarmCase):
    """v0.4.4: cleanup removes a merged task's worktree only while it is still that task's."""

    def test_ec200_a_reused_worktree_is_never_removed_by_cleanup(self):
        cfg = self.root / ".agents" / "swarm" / "config.toml"
        cfg.write_text(cfg.read_text(encoding="utf-8") + "\n[merge]\ncleanup = false\n", encoding="utf-8")
        self.git("commit", "-qam", "keep worktrees after a merge")
        self.join("ann")
        self.to_ready("ann", "T1")
        self.sw("--as", "ann", "merge")
        wt = Path(self.task("T1")["worktree"])
        self.assertTrue(wt.exists())
        self.git("checkout", "-q", "-b", "other-work", cwd=wt)  # an agent reuses the directory for something else
        (wt / "mine.txt").write_text("work in progress\n", encoding="utf-8")
        self.git("add", "mine.txt", cwd=wt)
        self.git("commit", "-qm", "wip", cwd=wt)  # a clean tree: only the branch check protects it
        con = self.db()
        con.execute("UPDATE tasks SET cleanup_pending=1 WHERE id='T1'")
        con.commit()
        con.close()
        out = self.sw("doctor", "--fix")
        self.assertIn("in use for other work", out)
        self.assertTrue(wt.exists())
        self.assertEqual(self.git("rev-parse", "--abbrev-ref", "HEAD", cwd=wt).strip(), "other-work")
        self.assertTrue((wt / "mine.txt").exists())
        self.assertEqual(self.task("T1")["cleanup_pending"], 0)
        self.assertIn("old worktree was kept", self.sw("monitor", "--observe", "--once", "--replay"))
        self.assertEqual(
            self.git("branch", "--list", "swarm/T1").strip(), ""
        )  # the merged task branch is still cleaned


class Messages(SwarmCase):
    def test_ec102_ec103_pings_broadcasts_and_unread_footer(self):
        self.join("ann")
        self.join("bob")
        self.sw("--as", "ann", "send", "bob", "are you free?", "--kind", "question")
        self.assertIn("unread", self.sw("--as", "bob", "status"))
        out = self.sw("--as", "bob", "inbox")
        self.assertIn("are you free?", out)
        with closing(self.db()) as db, db:
            message = db.execute("SELECT MAX(id) FROM messages WHERE sender='ann' AND recipient='bob'").fetchone()[0]
        self.ack_message(message, "bob")
        self.sw("send", "ann", "ready", "--kind", "decision", "--reply-to", str(message), "--as", "bob")
        self.assertIn("ready", self.sw("--as", "ann", "inbox"))
        self.sw("--as", "ann", "send", "*", "freeze at five")
        self.join("cat")
        self.assertNotIn("freeze", self.sw("--as", "cat", "inbox"))  # joined after the broadcast
        self.assertIn("freeze", self.sw("--as", "bob", "inbox"))


if __name__ == "__main__":
    unittest.main()
