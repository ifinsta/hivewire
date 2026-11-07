"""Source-anchored code-intelligence gate tests, no third-party dependencies."""
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

TOOL = Path(__file__).resolve().parents[1] / 'skills' / 'agents-collaboration' / 'code_intelligence.py'
if not TOOL.is_file():  # local test fixture outside the repository
    TOOL = Path(__file__).with_name('hivewire_code_intelligence.py')
spec = importlib.util.spec_from_file_location('intel', TOOL)
intel = importlib.util.module_from_spec(spec)
spec.loader.exec_module(intel)


class IntelligenceGate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / 'repo'
        self.root.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.org')
        (self.root / 'payments.rs').write_text('pub fn charge() {}\n', encoding='utf-8')
        (self.root / 'client.rs').write_text('use crate::payments::charge;\n', encoding='utf-8')
        self.commit()
        self.base = self.git('rev-parse', 'HEAD').strip()

    def tearDown(self):
        self.tmp.cleanup()

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True)

    def commit(self):
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture')

    def run_tool(self, *args):
        completed = subprocess.run([sys.executable, str(TOOL), '--repo', str(self.root), '--base', self.base, *args], text=True, capture_output=True)
        return completed.returncode, json.loads(completed.stdout or completed.stderr)

    def test_removed_public_api_blocks_and_tracks_dependents(self):
        (self.root / 'payments.rs').write_text('fn charge() {}\n', encoding='utf-8')
        self.commit()
        code, report = self.run_tool()
        self.assertEqual(code, 2)
        self.assertEqual(report['blockingCount'], 1)
        self.assertEqual(report['findings'][0]['removedSymbols'], ['charge'])
        self.assertIn('client.rs', report['findings'][0]['affectedPaths'])
        self.assertFalse(report['independentApproval'])

    def test_additive_change_is_not_claimed_verified(self):
        (self.root / 'payments.rs').write_text('pub fn charge() {}\npub fn refund() {}\n', encoding='utf-8')
        self.commit()
        code, report = self.run_tool()
        self.assertEqual(code, 0)
        self.assertEqual(report['result'], 'no_blocking_static_findings')
        self.assertFalse(report['semanticCompleteness'])

    def test_declared_cross_repo_consumers_block_until_review(self):
        (self.root / 'payments.rs').write_text('pub fn charge() {}\n// stable\n', encoding='utf-8')
        self.commit()
        policy_path = Path(self.tmp.name) / 'policy.json'
        policy_path.write_text(json.dumps({'version': 1, 'contracts': [
            {'path': 'payments.rs', 'consumers': ['ifinsta/ifinpay-plugin']}]}))
        code, report = self.run_tool('--policy', str(policy_path))
        self.assertEqual(code, 2)
        self.assertEqual(report['findings'][0]['externalConsumers'], ['ifinsta/ifinpay-plugin'])
        self.assertIn('declared_external_consumers_not_tested', report['findings'][0]['reasons'])

    def test_policy_from_worktree_refused(self):
        (self.root / 'payments.rs').write_text('pub fn charge() {}\n// safe\n', encoding='utf-8')
        self.commit()
        p = self.root / 'policy.json'
        p.write_text('{}')
        code, report = self.run_tool('--policy', str(p))
        self.assertEqual(code, 3)
        self.assertEqual(report['result'], 'unknown')

    def test_changed_contract_blocks_even_if_symbols_not_detected(self):
        (self.root / 'contracts.proto').write_text('message Account { string id = 1; }\n')
        self.commit()
        code, report = self.run_tool()
        self.assertEqual(code, 2)
        self.assertEqual(report['findings'][0]['path'], 'contracts.proto')

    def test_stale_or_missing_revision_is_not_a_pass(self):
        code, report = self.run_tool('--head', 'does-not-exist')
        self.assertEqual(code, 3)
        self.assertEqual(report['result'], 'unknown')

    def test_dirty_worktree_cannot_alter_commit_pinned_findings(self):
        (self.root / 'payments.rs').write_text('pub fn charge() {}\n// retained\n', encoding='utf-8')
        self.commit()
        (self.root / 'payments.rs').write_text('no symbols at all\n', encoding='utf-8')
        code, report = self.run_tool()
        self.assertEqual(code, 0)
        self.assertEqual(report['blockingCount'], 0)

    def test_symlink_is_never_followed(self):
        try:
            (self.root / 'leak.rs').symlink_to('payments.rs')
        except (OSError, NotImplementedError):
            self.skipTest('symlink creation unavailable on this platform')
        self.commit()
        code, report = self.run_tool()
        self.assertEqual(code, 2)
        self.assertEqual(report['changedFiles'], 1)
        self.assertIn('unindexed_or_unsafe_change', report['findings'][0]['reasons'])


if __name__ == '__main__':
    unittest.main()
