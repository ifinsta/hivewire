"""Worktree preservation at automatic return and delayed cleanup boundaries."""

from __future__ import annotations

import io
from pathlib import Path
from unittest import mock

from test_hardening import HardeningCase
from test_processes import ProcessCase
import test_swarm as fixtures

core = fixtures.swarm_module


class LeftoverWorktree(HardeningCase):
    """A removal that stops halfway leaves a folder Git no longer knows; it must never break other commands."""

    def leftover(self, *, with_file):
        self.config("[merge]\ncleanup = false")
        self.join("ann")
        tree = self.to_ready("ann", "T1")
        self.sw("merge", "--as", "ann")
        # What a Windows removal that hit a locked folder leaves: Git's side is gone, the folder is not.
        self.git("worktree", "remove", "--force", str(tree))
        tree.mkdir()
        if with_file:
            (tree / "kept.txt").write_text("not Git's to delete", encoding="utf-8")
        with self.board() as sw, sw.tx():
            sw.update_task("T1", cleanup_pending=1)
        self.assertFalse((tree / ".git").exists())
        return tree

    def test_ec352_next_survives_an_empty_leftover_folder_and_removes_it(self):
        tree = self.leftover(with_file=False)
        self.join("bob")
        claimed = self.sw("next", "--as", "bob")
        self.assertIn("claimed", claimed)
        self.assertFalse(tree.exists())
        self.assertEqual(self.task("T1")["cleanup_pending"], 0)
        self.assertFalse(self.git("branch", "--list", "swarm/T1").strip(), "the merged branch is deleted too")

    def test_ec352_a_leftover_folder_with_files_is_kept_and_reported_never_deleted(self):
        tree = self.leftover(with_file=True)
        self.join("bob")
        self.assertIn("claimed", self.sw("next", "--as", "bob"))
        self.assertEqual((tree / "kept.txt").read_text(encoding="utf-8"), "not Git's to delete")
        self.assertEqual(self.task("T1")["cleanup_pending"], 0)
        with self.board() as sw:
            detail = sw.db.execute("SELECT detail FROM events WHERE kind='cleanup_skipped' AND task='T1'").fetchone()[0]
        self.assertIn("no longer a Git worktree", detail)

    def test_ec352_doctor_fix_and_a_failing_cleanup_do_not_abort_next(self):
        tree = self.leftover(with_file=True)
        self.json_command("doctor", "--fix", "--json")
        self.assertTrue((tree / "kept.txt").exists())
        with self.board() as sw, sw.tx():
            sw.update_task("T1", cleanup_pending=1)
        self.join("bob")
        with mock.patch.object(core, "cleanup", side_effect=core.SwarmError("simulated failure")):
            with self.board() as sw:
                out = io.StringIO()
                with mock.patch("sys.stdout", out):
                    core.cmd_next(sw, mock.Mock(as_="bob"))
        self.assertIn("cleanup of T1 skipped: simulated failure", out.getvalue())
        self.assertTrue(self.task("T2")["assignee"] == "bob" or self.task("T3")["assignee"] == "bob")


class IgnoredWork(HardeningCase):
    def ignore(self, pattern):
        path = self.root / ".gitignore"
        path.write_text(path.read_text(encoding="utf-8") + pattern + "\n", encoding="utf-8")
        self.git("commit", "-qam", "ignore local artifacts")

    def test_ec251_ignored_files_are_work_even_without_a_plan_or_visible_git_dirt(self):
        self.ignore("scratch/")
        self.join("ann")
        self.sw("claim", "T1", "--as", "ann")
        tree = Path(self.task("T1")["worktree"])
        file = tree / "scratch" / "nested" / "unfinished.bin"
        file.parent.mkdir(parents=True)
        file.write_bytes(b"\x00unfinished ignored work\xff")
        self.assertEqual(self.git("status", "--porcelain", cwd=tree), "")
        before = dict(self.task("T1"))
        with self.board() as sw, sw.tx():
            self.assertFalse(core.return_if_untouched(sw, sw.task("T1"), "fixture holder exited"))
        self.assertEqual(file.read_bytes(), b"\x00unfinished ignored work\xff")
        self.assertEqual(dict(self.task("T1")), before)
        self.assertTrue(self.git("branch", "--list", "swarm/T1").strip())

    def test_ec251_cleanup_retains_ignored_content_until_the_operator_preserves_it(self):
        self.ignore("scratch/")
        self.config("[merge]\ncleanup = false")
        self.join("ann")
        tree = self.to_ready("ann", "T1")
        self.sw("merge", "--as", "ann")
        file = tree / "scratch" / "unfinished.txt"
        file.parent.mkdir()
        file.write_text("local work after merge", encoding="utf-8")
        self.assertEqual(self.git("status", "--porcelain", cwd=tree), "")
        with self.board() as sw, sw.tx():
            sw.update_task("T1", cleanup_pending=1)
        result = self.json_command("doctor", "--fix", "--json")["result"]
        self.assertIn("ignored", result["repairs"][0]["result"])
        self.assertEqual(file.read_text(encoding="utf-8"), "local work after merge")
        self.assertEqual(self.task("T1")["cleanup_pending"], 1)
        self.assertTrue(self.git("branch", "--list", "swarm/T1").strip())
        # The operator moves the valuable artifact outside the disposable worktree.
        file.rename(Path(self.tmp.name) / "preserved.txt")
        self.json_command("doctor", "--fix", "--json")
        self.assertFalse(tree.exists())
        self.assertEqual(self.task("T1")["cleanup_pending"], 0)

    def test_ec251_changed_generated_brief_is_preserved_but_an_exact_archived_copy_is_disposable(self):
        self.ignore("*swarm-brief.md")
        self.join("ann")
        self.sw("claim", "T1", "--as", "ann")
        tree = Path(self.task("T1")["worktree"])
        with self.board() as sw:
            core.write_brief(sw, "ann", "fake", sw.task("T1"))
            self.assertTrue(core.untouched(sw, sw.task("T1")))
            # Leading filename whitespace is data, not output decoration.
            extra = tree / (" " + core.BRIEF_NAME)
            extra.write_bytes(b"unique operator notes")
            with sw.tx():
                self.assertFalse(core.return_if_untouched(sw, sw.task("T1"), "fixture exited"))
            extra.rename(Path(self.tmp.name) / "preserved-brief-notes")
            brief = tree / core.BRIEF_NAME
            saved = brief.read_bytes()
            brief.write_bytes(saved + b"\nOperator recovery notes.\n")
            with sw.tx():
                self.assertFalse(core.return_if_untouched(sw, sw.task("T1"), "fixture exited"))
            self.assertIn(b"Operator recovery notes.", brief.read_bytes())
            brief.write_bytes(saved)
            # A missing archive cannot justify deleting a file just by its name.
            archive = sw.dir / "briefs" / "ann.md"
            archive.unlink()
            content_archive = sw.dir / "briefs" / "by-content" / f"{core.brief_digest(brief)}.md"
            content_archive.unlink()
            self.assertFalse(core.untouched(sw, sw.task("T1")))
            archive.write_bytes(saved)
            with sw.tx():
                self.assertTrue(core.return_if_untouched(sw, sw.task("T1"), "fixture exited"))
        self.assertFalse(tree.exists())
        self.assertEqual(self.task("T1")["state"], "open")

    def test_ec251_failed_worktree_removal_keeps_ownership_and_branch_references(self):
        self.join("ann")
        self.sw("claim", "T1", "--as", "ann")
        tree = Path(self.task("T1")["worktree"])
        self.git("worktree", "lock", "--reason", "preserve this fixture", str(tree))
        before = dict(self.task("T1"))
        with self.board() as sw, sw.tx():
            self.assertFalse(core.return_if_untouched(sw, sw.task("T1"), "fixture exited"))
        self.assertEqual(dict(self.task("T1")), before)
        self.assertTrue(tree.exists())
        self.assertTrue(self.git("branch", "--list", "swarm/T1").strip())


class ActiveWork(ProcessCase):
    def clean_worker(self, action="cooperative"):
        code = self.holding_program().replace("Path('unfinished.txt').write_text('preserve unfinished work'); ", "")
        self.configure_worker(action, code=code)

    def test_ec252_external_completion_defers_cleanup_until_the_owned_worker_exits(self):
        self.clean_worker()
        try:
            self.sw("spawn", "hold", "--name", "bob", "--task", "T1")
            self.wait_for(self.ready.exists)
            tree = Path(self.task("T1")["worktree"])
            row = self.execution()
            self.assertEqual(self.git("status", "--porcelain", cwd=tree), "")
            landed = self.git("rev-parse", "main").strip()
            output = self.sw("record", "T1", "--commit", landed, "--note", "Fixture landed externally")
            self.assertIn("execution", output)
            self.assertTrue(self.running(row["child_pid"]))
            self.assertTrue(tree.exists())
            self.assertEqual(self.task("T1")["cleanup_pending"], 1)
            # The same process takes another task but still has its old cwd.
            # Cleanup must also inspect the agent's executions, not only task IDs.
            self.sw("claim", "T2", "--as", "bob")
            self.wait_for(lambda: self.execution()["task"] == "T2")
            with self.board() as sw:
                core.write_brief(sw, "bob", "hold", sw.task("T2"))
            self.json_command("doctor", "--fix", "--json")
            self.assertTrue(tree.exists())
            self.release.write_text("exit", encoding="utf-8")
            self.wait_for(lambda: self.execution()["state"] == "exited")
            self.json_command("doctor", "--fix", "--json")
            self.assertFalse(tree.exists())
            self.assertEqual(self.task("T1")["cleanup_pending"], 0)
        finally:
            self.cleanup_worker()

    def test_ec252_unknown_supervision_retains_clean_work_until_labeled_human_reconciliation(self):
        self.clean_worker("terminate")
        self.sw(
            "spawn",
            "hold",
            "--name",
            "bob",
            "--task",
            "T1",
            ok=None,
            env={"SWARM_FAILPOINT": "after_execution_started"},
        )
        row = self.execution()
        self.wait_for(lambda: not self.running(row["supervisor_pid"]))
        self.wait_for(lambda: not self.running(row["child_pid"]))
        with self.board() as sw, sw.tx():
            sw.db.execute("UPDATE executions SET heartbeat_at='2020-01-01T00:00:00+00:00'")
        self.sw("status")
        self.assertEqual(self.execution()["state"], "unknown")
        tree = Path(self.task("T1")["worktree"])
        landed = self.git("rev-parse", "main").strip()
        self.assertIn("execution", self.sw("record", "T1", "--commit", landed))
        self.json_command("doctor", "--fix", "--json", ok=False)
        self.assertTrue(tree.exists())
        self.assertEqual(self.task("T1")["cleanup_pending"], 1)
        self.sw(
            "execution",
            "reconcile",
            row["id"],
            "--confirm-stopped",
            "--reason",
            "The fixture helper and its owned worker have stopped",
        )
        self.assertIn("Human attestation", self.execution()["detail"])
        self.json_command("doctor", "--fix", "--json")
        self.assertFalse(tree.exists())
        self.assertEqual(self.task("T1")["cleanup_pending"], 0)


class PolicySnapshot(HardeningCase):
    def test_ec253_a_transient_config_read_cannot_bind_different_policy_to_the_original_digest(self):
        config = self.root / ".agents" / "swarm" / "config.toml"
        config = config.resolve()
        original = config.read_bytes()
        transient = original.replace(b"max_agents = 5", b"max_agents = 4")
        self.assertNotEqual(transient, original)
        real_open = Path.open
        reads = []

        def opened(path, mode="r", *args, **kwargs):
            if mode in ("r", "rb") and path.resolve() == config:
                reads.append(mode)
                # The source changes A -> B -> A during parsing. Both outside
                # digests see A; the parse sees B. File statistics cannot prove
                # that the parsed bytes belonged to the captured policy.
                if len(reads) == 2:
                    return io.BytesIO(transient) if mode == "rb" else io.StringIO(transient.decode("utf-8"))
            return real_open(path, mode, *args, **kwargs)

        with mock.patch.object(Path, "open", opened):
            with self.assertRaisesRegex(core.SwarmError, "configuration changed while loading"):
                with self.board():
                    pass
        self.assertEqual(len(reads), 2)
        self.assertEqual(config.read_bytes(), original)
        self.assertFalse((self.root / ".git" / "swarm" / "config.cache.json").exists())
        with self.board() as sw:
            self.assertEqual(sw.config["swarm"]["max_agents"], 5)

    def test_ec253_invalid_toml_and_encoding_give_a_policy_repair_refusal(self):
        config = self.root / ".agents" / "swarm" / "config.toml"
        for content in (b"[spawn\nallow_unattended=true", b"[spawn]\n# invalid \xff\n"):
            config.write_bytes(content)
            data = self.json_command("status", "--json", ok=False)
            self.assertEqual(data["exit_code"], 2)
            self.assertIn("config.toml", data["error"]["message"])
            self.assertIn("Repair", data["error"]["message"])
