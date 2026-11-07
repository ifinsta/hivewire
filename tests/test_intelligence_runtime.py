"""Boundary and integration tests for Hivewire's pinned intelligence runtime."""
from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

SKILLS = Path(__file__).resolve().parents[1] / "skills" / "agents-collaboration"
sys.path.insert(0, str(SKILLS))
import hivewire_intelligence as cli  # noqa: E402
import intelligence_execution as execution  # noqa: E402
import intelligence_graph as graph  # noqa: E402
import intelligence_mcp as mcp  # noqa: E402
import intelligence_metrics as metrics  # noqa: E402
import intelligence_verify as verify_runtime  # noqa: E402


class IntelligenceRuntime(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.home = Path(self.tmp.name)
        self.entries = []
        for slug in ("a", "b"):
            root = self.home / slug
            root.mkdir()
            self.git(root, "init", "-q")
            self.git(root, "config", "user.name", "Test")
            self.git(root, "config", "user.email", "test@example.invalid")
            text = ("pub fn charge() {}\n" if slug == "a" else
                    "use crate::payments::charge;\n")
            (root / ("payments.rs" if slug == "a" else "client.rs")).write_text(text)
            self.git(root, "add", ".")
            self.git(root, "commit", "-qm", "initial")
            commit = self.git(root, "rev-parse", "HEAD").decode().strip()
            entry = {"name": "ifinsta/" + slug, "root": str(root.resolve()), "commit": commit}
            if slug == "a":
                entry["contracts"] = [{"path": "payments.rs", "consumers": ["ifinsta/b"]}]
            self.entries.append(entry)
        self.inventory = {"version": 1, "repositories": self.entries}
        self.manifest = self.home / "inventory.json"
        self.manifest.write_text(json.dumps(self.inventory))
        self.catalog = graph.build(self.inventory, self.manifest)

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def git(root, *args):
        return subprocess.check_output(["git", *args], cwd=root)

    def test_two_exact_git_commits_are_indexed_and_linked_cross_repo(self):
        self.assertEqual(len(self.catalog["repositories"]), 2)
        self.assertEqual(len(self.catalog["files"]), 2)
        self.assertTrue(any(
            e["from"] == "ifinsta/b:client.rs" and e["to"] == "ifinsta/a:payments.rs"
            for e in self.catalog["edges"]
        ))
        impacted = graph.impact(self.catalog, "ifinsta/a", "payments.rs")
        self.assertTrue(impacted["requiresIndependentReview"])
        self.assertEqual(len(impacted["declaredConsumers"]), 1)
        self.assertFalse(impacted["independentVerification"])
        self.assertTrue(graph.check_catalog(self.catalog))

    def test_dirty_worktrees_cannot_affect_pinned_source_graph(self):
        (self.home / "a" / "payments.rs").write_text("malicious uncommitted content\n")
        updated = graph.build(self.inventory, self.manifest)
        self.assertEqual(self.catalog, updated)
        with tempfile.TemporaryDirectory() as tmp:
            snap = Path(tmp) / "source"
            snap.mkdir()
            result = execution.materialize(self.home / "a", self.entries[0]["commit"], snap)
            self.assertEqual(result["files"], 1)
            self.assertIn("pub fn charge()", (snap / "payments.rs").read_text())

    def test_tampering_and_manifest_path_bypass_are_rejected(self):
        forged = dict(self.catalog)
        forged["digest"] = "0" * 64
        with self.assertRaises(graph.IndexErrorSafe):
            graph.check_catalog(forged)
        wrong = {"version": 1, "repositories": [dict(self.entries[0])]}
        wrong["repositories"][0]["commit"] = "f" * 40
        with self.assertRaises(graph.IndexErrorSafe):
            graph.build(wrong, self.manifest)
        inside = self.home / "a" / "inventory.json"
        inside.write_text(json.dumps(self.inventory))
        with self.assertRaises(graph.IndexErrorSafe):
            graph.build(self.inventory, inside)

    def test_cargo_resolved_package_edges_require_matching_pinned_metadata(self):
        doc = {
            "version": 1, "workspace_root": str((self.home / "a").resolve()),
            "workspace_members": ["demo"], "packages": [
                {"id": "demo", "name": "demo", "version": "1.0.0"},
                {"id": "dep", "name": "dep", "version": "2.0.0"}],
            "resolve": {"nodes": [
                {"id": "demo", "deps": [{"name": "dep", "pkg": "dep",
                                        "dep_kinds": [{"kind": None, "target": None}]}]},
                {"id": "dep", "deps": []}]}
        }
        location = self.home / "metadata.json"
        blob = json.dumps(doc).encode()
        location.write_bytes(blob)
        new = json.loads(json.dumps(self.inventory))
        new["repositories"][0]["cargoMetadata"] = str(location)
        new["repositories"][0]["cargoMetadataSha256"] = hashlib.sha256(blob).hexdigest()
        result = graph.build(new, self.manifest)
        self.assertEqual(len(result["cargoResolvedEdges"]), 1)
        self.assertEqual(result["cargoResolvedEdges"][0]["kind"], "cargo-resolved")
        new["repositories"][0]["cargoMetadataSha256"] = "0" * 64
        with self.assertRaises(graph.IndexErrorSafe):
            graph.build(new, self.manifest)

    def test_isolation_docker_argv_has_no_host_fallback(self):
        image = "registry.invalid/test@sha256:" + "f" * 64
        argv = execution.docker_argv(image, ["cargo", "test", "--offline"],
                                     self.home / "snapshot", "hwi-" + "a" * 24)
        self.assertIn("--network", argv)
        self.assertIn("none", argv)
        self.assertIn("--read-only", argv)
        self.assertIn("--cap-drop", argv)
        self.assertIn("/work:rw,exec,nosuid,size=1g,uid=65534,gid=65534,mode=700", argv)
        self.assertIn("CARGO_TARGET_DIR=/work/target", argv)
        self.assertIn("cp -R /source/. /work/ && cd /work && exec \"$@\"", argv)
        self.assertIn("--pull", argv)
        self.assertIn("never", argv)
        self.assertEqual(argv[-3:], ["cargo", "test", "--offline"])
        with self.assertRaises(graph.IndexErrorSafe):
            execution.docker_argv("registry.invalid/test:latest", ["cargo", "test"],
                                  self.home, "hwi-" + "a" * 24)
        with self.assertRaises(graph.IndexErrorSafe):
            execution.validate_command(["python", "ok\x00unsafe"])

    def test_bounded_container_supervisor_rejects_unlimited_output(self):
        rc, out, err, timed_out = execution.bounded_process(
            [sys.executable, "-c", "print('bounded output')"], 5
        )
        self.assertEqual(rc, 0)
        self.assertIn(b"bounded output", out)
        self.assertEqual(err, b"")
        self.assertFalse(timed_out)
        with self.assertRaises(graph.IndexErrorSafe):
            execution.bounded_process(
                [sys.executable, "-c",
                 "import sys;sys.stdout.write('X'*2100000);sys.stdout.flush()"], 5
            )


    def test_review_agent_evidence_must_match_pinned_git_blob_and_identity(self):
        entry = self.entries[0]
        run = {"repo": entry["name"], "commit": entry["commit"], "passed": True,
               "reviewer": "agent-b", "author": "agent-a", "digest": "a" * 64}
        payload = {"headCommit": entry["commit"], "reviewer": "agent-b",
                   "decision": "concerns", "findings": [
                       {"path": "payments.rs", "line": 1, "quote": "pub fn charge()",
                        "reason": "Review whether this endpoint exposes charges."}]}
        result = execution.validate_agent_review(entry, "agent-a", "agent-b", run,
                                                  json.dumps(payload).encode())
        self.assertFalse(result["independentClearance"])
        self.assertTrue(result["needsHumanApproval"])
        payload["findings"][0]["quote"] = "false attributed quote"
        with self.assertRaises(graph.IndexErrorSafe):
            execution.validate_agent_review(entry, "agent-a", "agent-b", run,
                                            json.dumps(payload).encode())
        payload["findings"] = []
        payload["decision"] = "no_concerns"
        with self.assertRaises(graph.IndexErrorSafe):
            execution.validate_agent_review(entry, "agent-b", "agent-b", run,
                                            json.dumps(payload).encode())

    def sample(self, commit: str, event: str, escaped: int) -> dict:
        return {"tenant": "tenant-1", "repo": "ifinsta/a", "commit": commit,
                "observationId": event, "source": "production",
                "exposures": 1000, "checksRun": 100, "checksPassed": 100,
                "findings": 10, "falsePositives": 1, "escapedDefects": escaped}

    def policy(self):
        return {"minimumSamples": 1, "minimumExposures": 100,
                "maximumEscapeIncreasePpm": 100,
                "maximumFalsePositiveIncreasePpm": 100,
                "minimumCoveragePpm": 900000}

    def test_production_feedback_is_idempotent_and_regressions_fail_closed(self):
        store = self.home / "feedback.sqlite"
        base, after = "a" * 40, "b" * 40
        old = self.sample(base, "first", 1)
        current = self.sample(after, "second", 1)
        metrics.record(store, old)
        metrics.record(store, old)
        metrics.record(store, current)
        report = metrics.compare(store, "tenant-1", "ifinsta/a", base, after, self.policy())
        self.assertEqual(report["status"], "thresholds_met")
        self.assertEqual(report["baselineMetrics"]["samples"], 1)
        self.assertFalse(report["collectorSignatureVerified"])
        updated = dict(current)
        updated["escapedDefects"] = 5
        with self.assertRaises(graph.IndexErrorSafe):
            metrics.record(store, updated)
        updated["observationId"] = "third"
        metrics.record(store, updated)
        report = metrics.compare(store, "tenant-1", "ifinsta/a", base, after, self.policy())
        self.assertEqual(report["status"], "regression")
        self.assertIn("escaped_defects_regression", report["violations"])
        empty = metrics.compare(store, "tenant-2", "ifinsta/a", base, after, self.policy())
        self.assertEqual(empty["status"], "unknown_insufficient_samples")

    def test_mcp_is_read_only_and_tenant_bound(self):
        request = {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                   "params": {"name": "intelligence.impact",
                              "arguments": {"repo": "ifinsta/a", "path": "payments.rs"}}}
        answer = mcp.handle(request, self.catalog, "tenant-1", None, None)
        self.assertFalse(answer["result"]["isError"])
        evidence = json.loads(answer["result"]["content"][0]["text"])
        self.assertEqual(evidence["source"], "ifinsta/a:payments.rs")
        request["params"]["name"] = "intelligence.sign"
        self.assertEqual(mcp.handle(request, self.catalog, "tenant-1", None, None)["error"]["code"], -32602)
        notification = {"jsonrpc": "2.0", "method": "notifications/initialized"}
        self.assertIsNone(mcp.handle(notification, self.catalog, "tenant-1", None, None))
        input_stream = io.StringIO(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}) + "\n")
        output = io.StringIO()
        catalog_path = self.home / "catalog.json"
        catalog_path.write_text(json.dumps(self.catalog))
        self.assertEqual(mcp.serve(catalog_path, "tenant-1", None, None, input_stream, output), 0)
        self.assertEqual(len(json.loads(output.getvalue())["result"]["tools"]), 2)

    def consumer_policy(self):
        image = "registry.invalid/test@sha256:" + "f" * 64
        return {"version": 1, "jobs": [
            {"repo": name, "image": image,
             "argv": ["python", "-m", "unittest"], "timeoutSeconds": 90}
            for name in ("ifinsta/a", "ifinsta/b")
        ]}

    def test_cross_repository_plan_requires_owner_and_every_consumer(self):
        selected = verify_runtime.selection(self.inventory, self.manifest, self.catalog,
                                            "ifinsta/a", "payments.rs")
        self.assertEqual(selected["repositories"], ["ifinsta/a", "ifinsta/b"])
        self.assertFalse(selected["independentApproval"])
        forged = json.loads(json.dumps(self.catalog))
        forged["repositories"][0]["commit"] = "f" * 40
        with self.assertRaises(graph.IndexErrorSafe):
            verify_runtime.selection(self.inventory, self.manifest, forged,
                                     "ifinsta/a", "payments.rs")

    def test_cross_repo_consumer_jobs_are_all_required_and_fail_closed(self):
        policy = self.consumer_policy()
        policy_path = self.home / "verified-policy.json"
        policy_path.write_text(json.dumps(policy))
        expected = {
            "passed": True, "digest": "a" * 64,
            "exitCode": 0
        }
        with mock.patch.object(verify_runtime, "isolated", return_value=(expected, b"")) as execute:
            result = verify_runtime.verify(self.inventory, self.manifest, self.catalog,
                                           policy, policy_path, "ifinsta/a", "payments.rs")
        self.assertEqual(execute.call_count, 2)
        self.assertEqual(result["executions"], 2)
        self.assertTrue(result["allPassed"])
        self.assertFalse(result["clearanceGranted"])
        removed = {"version": 1, "jobs": policy["jobs"][:-1]}
        with self.assertRaises(graph.IndexErrorSafe):
            verify_runtime.verify(self.inventory, self.manifest, self.catalog,
                                  removed, policy_path, "ifinsta/a", "payments.rs")
        with mock.patch.object(verify_runtime, "isolated", side_effect=graph.IndexErrorSafe("no Docker")):
            unavailable = verify_runtime.verify(self.inventory, self.manifest, self.catalog,
                                                policy, policy_path, "ifinsta/a", "payments.rs")
        self.assertFalse(unavailable["allPassed"])
        self.assertTrue(all(x["status"] == "unknown" for x in unavailable["results"]))

    def test_verify_impact_plan_only_is_nonexecuting(self):
        destination = self.home / "consumer-plan.json"
        with mock.patch.object(verify_runtime, "isolated") as execute:
            status = cli.main(["verify-impact", "--inventory", str(self.manifest),
                               "--catalog", str(self._save_catalog()),
                               "--policy", str(self.manifest),
                               "--repo", "ifinsta/a", "--path", "payments.rs",
                               "--output", str(destination), "--plan-only"])
        self.assertEqual(status, 0)
        execute.assert_not_called()
        self.assertEqual(json.loads(destination.read_text())["repositories"],
                         ["ifinsta/a", "ifinsta/b"])

    def _save_catalog(self) -> Path:
        location = self.home / "consumer-catalog.json"
        location.write_text(json.dumps(self.catalog))
        return location


    def test_cli_real_git_snapshot_and_immutable_report(self):
        dest = self.home / "catalog2.json"
        self.assertEqual(cli.main(["index", "--inventory", str(self.manifest),
                                   "--output", str(dest)]), 0)
        self.assertTrue(graph.check_catalog(json.loads(dest.read_text())))
        self.assertEqual(cli.main(["index", "--inventory", str(self.manifest),
                                   "--output", str(dest)]), 3)


if __name__ == "__main__":
    unittest.main()
