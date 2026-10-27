"""The engine's own integration commit never strands on a branch: every merge exit that does not land drops it."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path

import test_generated_paths as generated_paths
from test_generated_paths import VERSION_HOOK
from test_hardening import core
from test_oversight import OversightCase

FAILING_HOOK = [
    sys.executable,
    "-c",
    "from pathlib import Path; Path('VERSION').write_text('2\\n'); raise SystemExit(1)",
]


class IntegrationDiscard(OversightCase):
    config = generated_paths.GeneratedPaths.config
    with_version_hook = generated_paths.GeneratedPaths.with_version_hook

    @contextmanager
    def board(self):
        sw = core.Swarm(self.root)
        try:
            yield sw
        finally:
            sw.db.close()

    def ready(self, hook=VERSION_HOOK):
        self.with_version_hook(hook=hook)
        self.join("ann")
        return self.to_ready("ann", "T1")

    def version(self, wt):
        return (wt / "VERSION").read_text().strip()

    def test_ec393_a_raise_after_the_intent_keeps_the_marker_until_recovery_clears_it(self):
        wt = self.ready()
        out = self.sw("merge", "--as", "ann", env={"SWARM_FAILPOINT": "after_intent"}, ok=False)
        self.assertEqual(self.version(wt), "2", "the process died with the engine's commit on the branch")
        with self.board() as sw:
            self.assertIsNotNone(sw.task("T1")["merge_intent"], out)
            core.discard_integration(sw, "T1", "ann")
            self.assertIsNotNone(sw.meta("integration:T1"), "an intent is still set: recovery owns it, keep the marker")
        self.assertEqual(self.version(wt), "2")
        self.sw("--as", "ann", "submit", "--evidence", "The merge died after its intent; nothing else changed", cwd=wt)
        self.assertEqual(self.version(wt), "1", "recovery cleared the intent, then the engine's commit was dropped")
        self.sw("merge", "--as", "ann")
        self.assertEqual(self.task("T1")["state"], "done")
        self.assertEqual(self.git("show", "main:VERSION").strip(), "2", "bumped once")

    def test_ec393_a_failed_hook_leaves_no_edit_for_the_author_to_clean_up(self):
        wt = self.ready(hook=FAILING_HOOK)
        self.assertIn("merge message hook failed", self.sw("merge", "--as", "ann"))
        self.sw("--as", "ann", "submit", "--evidence", "The hook failed; the branch is as submitted", cwd=wt)
        self.assertEqual(self.version(wt), "1")

    def test_ec393_a_dirty_main_checkout_does_not_strand_the_engine_commit(self):
        wt = self.ready()
        (self.root / "VERSION").write_text("a local edit in the main checkout\n", encoding="utf-8")
        self.assertIn("changes that the merge would overwrite", self.sw("merge", "--as", "ann"))
        self.sw("--as", "ann", "submit", "--evidence", "The fast-forward was refused; the work is unchanged", cwd=wt)
        self.assertEqual(self.version(wt), "1")
        self.git("checkout", "--", "VERSION")
        self.sw("merge", "--as", "ann")
        self.assertEqual(self.task("T1")["state"], "done")
        self.assertEqual(self.git("show", "main:VERSION").strip(), "2", "bumped once, not twice")

    def test_ec393_a_submit_during_a_live_merge_leaves_it_alone(self):
        wt = self.ready()
        with self.board() as sw, sw.tx():
            sw.set_meta("merge_lock", json.dumps({"agent": "ann", "task": "T1", "at": core.iso(), "attempt": "b" * 16}))
            sw.set_meta("integration:T1", json.dumps({"sha": None, "source": self.git("rev-parse", "HEAD", cwd=wt).strip(), "epoch": 1}))
        with self.board() as sw:
            core.discard_integration(sw, "T1", "ann", attempt=None)
            self.assertIsNotNone(sw.meta("integration:T1"), "another attempt holds the queue for this task")
        self.assertEqual(Path(wt).exists(), True)

    def test_ec394_an_authors_uncommitted_edit_is_never_reset_away(self):
        """A merge killed during its hook leaves a pre-hook marker. The author's own edit must survive the next submit."""
        wt = self.ready()
        source = self.git("rev-parse", "HEAD", cwd=wt).strip()
        with self.board() as sw, sw.tx():
            sw.set_meta("integration:T1", json.dumps({"sha": None, "source": source, "epoch": self.task("T1")["epoch"]}))
        edit = wt / "src" / "file.txt"
        edit.write_text("the author's own uncommitted work\n", encoding="utf-8")
        out = self.sw("--as", "ann", "submit", "--evidence", "Submitting after a killed merge", cwd=wt, ok=False)
        self.assertIn("uncommitted", out)
        self.assertEqual(edit.read_text(encoding="utf-8"), "the author's own uncommitted work\n")

    def test_ec394_the_hooks_own_edit_is_dropped_only_when_the_tree_is_exactly_what_it_left(self):
        wt = self.ready(hook=FAILING_HOOK)
        edit = wt / "src" / "file.txt"
        self.assertIn("merge message hook failed", self.sw("merge", "--as", "ann"))
        self.assertEqual(self.version(wt), "1", "the failed hook's own write was dropped")
        with self.board() as sw:
            self.assertIsNone(sw.meta("integration:T1"))
        self.assertTrue(edit.exists())
        kept = self.git("for-each-ref", "--format=%(refname)", "refs/swarm/discarded/T1/", cwd=wt).split()
        self.assertEqual(len(kept), 1, "what was dropped is kept under a ref")
        self.assertIn("2", self.git("show", f"{kept[0]}:VERSION", cwd=wt), "the ref holds the hook's own edit")

    def test_ec394_a_fingerprint_that_does_not_match_discards_nothing(self):
        wt = self.ready()
        source = self.git("rev-parse", "HEAD", cwd=wt).strip()
        edit = wt / "src" / "file.txt"
        edit.write_text("what the hook left\n", encoding="utf-8")
        with self.board() as sw, sw.tx():
            sw.set_meta(
                "integration:T1",
                json.dumps({"sha": None, "source": source, "epoch": self.task("T1")["epoch"], "dirty": core.tracked_diff_hash(wt)}),
            )
        edit.write_text("what the author wrote after the hook returned\n", encoding="utf-8")
        with self.board() as sw:
            core.discard_integration(sw, "T1", "ann")
        self.assertEqual(edit.read_text(encoding="utf-8"), "what the author wrote after the hook returned\n")
        self.assertEqual(self.git("for-each-ref", "refs/swarm/discarded/", cwd=wt).strip(), "")

    def test_ec394_a_git_failure_fingerprinting_is_the_engines_error_not_an_empty_hash(self):
        with self.assertRaises(core.SwarmError):
            core.tracked_diff_hash(Path(self.tmp.name))

    def test_ec394_a_raise_after_the_commit_still_drops_the_engine_commit(self):
        hook = [
            sys.executable,
            "-c",
            "from pathlib import Path; Path('VERSION').write_text('2\\n'); p = Path('backlog.md'); p.write_text(p.read_text() + 'x\\n')",
        ]
        wt = self.ready(hook=hook)
        out = self.sw("merge", "--as", "ann", ok=False)
        self.assertIn("outside", out.lower() + self.sw("status"), out)
        self.assertEqual(self.version(wt), "1", "the raise rolled back the checks, not the engine's commit")
        self.sw("--as", "ann", "submit", "--evidence", "The merge refused the hook's out-of-scope write", cwd=wt)
