import json

from test_hardening import HardeningCase


class DispatchSnapshot(HardeningCase):
    def setUp(self):
        super().setUp()
        path = self.root / ".agents/swarm/config.toml"
        path.write_text(path.read_text(encoding="utf-8").replace(
            "[swarm]\n", '[swarm]\ncoordinator = "human"\n'), encoding="utf-8")
        self.git("commit", "-qam", "use independent human coordinator")
        self.join("ann")
        self.sw("claim", "T1", "--as", "ann")

    def snapshot(self, owner="ann", epoch=None, actor="human", ok=True):
        if epoch is None:
            epoch = self.task("T1")["epoch"]
        return self.json_command("dispatch-snapshot", "T1", "--owner", owner,
                                 "--expected-epoch", str(epoch), "--as", actor,
                                 "--json", ok=ok)

    def test_snapshot_is_current_non_authoritative_observation(self):
        before = self.task("T1")
        result = self.snapshot()["result"]
        self.assertEqual(result["authority"], "none")
        self.assertEqual(result["owner"], "ann")
        self.assertEqual(result["epoch"], before["epoch"])
        self.assertEqual(result["baseCommit"], before["base_sha"])
        self.assertEqual(result["headCommit"], self.git("rev-parse", "HEAD", cwd=before["worktree"]).strip())
        self.assertEqual(len(result["hivewireTermsSha256"]), 64)
        self.assertEqual(self.task("T1")["state"], before["state"])
        self.assertNotIn("token", json.dumps(result).lower())

    def test_wrong_owner_epoch_and_agent_caller_refused(self):
        self.snapshot(owner="bob", ok=False)
        self.snapshot(epoch=self.task("T1")["epoch"] + 1, ok=False)
        self.snapshot(actor="ann", ok=False)

    def test_paused_board_denied(self):
        self.sw("pause", "synthetic incident", "--as", "human")
        self.snapshot(ok=False)

    def test_inactive_contract_denied(self):
        with self.board() as sw, sw.tx():
            sw.db.execute("UPDATE task_contracts SET state='expired' WHERE task='T1'")
        self.snapshot(ok=False)

    def test_observer_in_review_cannot_get_new_assignment(self):
        with self.board() as sw, sw.tx():
            sw.update_agent("ann", reviewing=12)
        self.snapshot(ok=False)
