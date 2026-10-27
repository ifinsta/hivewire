"""Negative, recovery and authorization tests for the HWP/1 fleet companion."""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from fleet.core import Refusal, Store, run_argv, verify_on_master
from fleet.dashboard import serve_console
from fleet.evidence import EvidenceRefusal, validate_remote_claim, verify_local_goal
from fleet.service import exclusive_state_lock, supervise
from fleet.workloads.browser import approve_origin


class FleetSafety(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state = str(self.root / "fleet.sqlite")
        self.store = Store(self.state)
        self.store.add_node("felix", "a" * 64)

    def test_node_revocation_and_certificate_rotation(self):
        self.assertEqual(self.store.identity("a" * 64), "felix")
        self.store.rotate_certificate("felix", "b" * 64)
        self.assertIsNone(self.store.identity("a" * 64))
        self.assertEqual(self.store.identity("b" * 64), "felix")
        self.store.revoke("felix", "operator disabled this device")
        self.assertIsNone(self.store.identity("b" * 64))
        self.assertEqual(self.store.nodes()[0]["health"], "revoked")
        with self.assertRaises(Refusal):
            self.store.assign("job-1", "felix", "frontend", "c" * 40, 60)
        self.store.rotate_certificate("felix", "d" * 64)
        self.assertEqual(self.store.identity("d" * 64), "felix")

    def test_epoch_fencing_and_expired_reassignment(self):
        first = self.store.assign("job", "felix", "frontend", "a" * 40, 60)
        self.assertEqual(first["epoch"], 1)
        self.store.heartbeat("felix", "boot", 1, [{"name": "browser", "state": "ready"}])
        self.assertEqual(self.store.nodes()[0]["components"][0]["state"], "ready")
        with self.assertRaises(Refusal):
            self.store.heartbeat("felix", "boot", 1)
        with self.store.db() as con:
            con.execute("UPDATE jobs SET lease_until=? WHERE id='job'", (0,))
        second = self.store.assign("job", "felix", "frontend", "a" * 40, 60)
        self.assertEqual(second["epoch"], 2)
        with self.assertRaises(Refusal):
            self.store.event("felix", "job", 1, "running")
        self.store.event("felix", "job", 2, "running")

    def test_local_execution_not_replayed(self):
        self.store.worker_claim("job", 1)
        with self.assertRaises(Refusal):
            self.store.worker_claim("job", 1)
        self.store.worker_finish("job", "unknown", "crashed after side effects")
        with self.assertRaises(Refusal):
            self.store.worker_claim("job", 1)

    def test_durable_remote_receipt_reconciliation(self):
        self.store.worker_claim("job", 1)
        self.store.worker_finish("job", "ready_to_submit")
        self.store.remote_ack("job", "1234", "received")
        self.assertEqual(len(self.store.remote_pending()), 1)
        again = Store(self.state)
        again.remote_ack("job", "1234", "merged")
        self.assertEqual(again.remote_pending(), [])
        self.assertEqual(again.worker_state("job")["state"], "submitted")
        with self.assertRaises(Refusal):
            again.remote_ack("job", "changed", "received")

    def test_cross_node_message_durable_and_idempotent(self):
        self.store.add_node("joseph", "b" * 64)
        result = self.store.send_message("felix", "joseph", "unique-key", "handoff", "Test complete")
        self.assertFalse(result["duplicate"])
        self.assertTrue(self.store.send_message("felix", "joseph", "unique-key",
                                                "handoff", "Test complete")["duplicate"])
        with self.assertRaises(Refusal):
            self.store.send_message("felix", "joseph", "unique-key",
                                    "handoff", "Changed content")
        received = self.store.inbox("joseph")
        self.assertEqual(received[0]["body"], "Test complete")
        local = Store(str(self.root / "joseph.sqlite"))
        local.receive_local(received[0])
        local.receive_local(received[0])
        self.assertEqual(len(local.local_inbox()), 1)
        self.store.ack_message("joseph", result["message_id"])
        self.store.ack_message("joseph", result["message_id"])
        self.assertEqual(self.store.inbox("joseph"), [])

    def test_uncertain_worker_release_requires_operator_attestation(self):
        self.store.worker_claim("job", 1)
        with self.assertRaises(Refusal):
            self.store.worker_reconcile("job", "machine restarted", False)
        with self.assertRaises(Refusal):
            self.store.worker_reconcile("job", "short", True)
        self.store.worker_reconcile("job", "Operator inspected machine and confirmed no process", True)
        self.assertIsNone(self.store.worker_state("job"))
        with self.store.db() as con:
            row = con.execute("SELECT * FROM worker_reconciliations").fetchone()
            self.assertEqual(row["old_epoch"], 1)

    def test_incident_deduplication_and_resolution(self):
        self.store.heartbeat("felix", "boot", 1)
        with self.store.db() as con:
            con.execute("UPDATE nodes SET last_seen=? WHERE id='felix'", (0,))
        first = self.store.sweep_liveness()[0]
        self.assertEqual(first["state"], "unreachable")
        second = self.store.sweep_liveness()[0]
        self.assertEqual(second["first_at"], first["first_at"])
        self.store.heartbeat("felix", "boot", 2)
        last = self.store.sweep_liveness()[0]
        self.assertIsNotNone(last["resolved_at"])

    def test_browser_requires_approved_staging_origin(self):
        with self.assertRaises(ValueError):
            approve_origin("https://staging.example.test", "https://staging.example.test", "no")
        with self.assertRaises(ValueError):
            approve_origin("https://other.example.test", "https://staging.example.test", "yes")
        with self.assertRaises(ValueError):
            approve_origin("http://other.example.test", "http://other.example.test", "yes")
        self.assertEqual(approve_origin("https://staging.example.test",
                                        "https://staging.example.test", "yes"),
                         "https://staging.example.test")

    def test_master_denies_unisolated_verification(self):
        with self.assertRaises(Refusal):
            verify_on_master({"gate_argv": []}, self.root, [sys.executable, "-c", "print(1)"])
        with self.assertRaises(Refusal):
            verify_on_master({"verifier": "isolated_docker", "docker_image": "python:latest"},
                             self.root, ["python", "-V"])
        ok = verify_on_master({"verifier": "lab_unsafe",
                               "allow_unsafe_local_verifier_for_tests": True},
                              self.root, [sys.executable, "-c", "print('checked')"])
        self.assertEqual(ok["exit_code"], 0)
        self.assertGreater(ok["output_bytes"], 0)

    def test_supervisor_timeout_is_bounded(self):
        report = run_argv([sys.executable, "-c", "import time; time.sleep(10)"], self.root, timeout=1)
        self.assertTrue(report["timed_out"])
        self.assertNotEqual(report["exit_code"], 0)

    def test_one_service_instance_and_resilient_loop(self):
        with exclusive_state_lock(self.state):
            with self.assertRaises(Refusal):
                with exclusive_state_lock(self.state):
                    pass
        stop = threading.Event()
        observed = []
        def cycle(_):
            stop.set()
            return []
        with mock.patch("fleet.service.run_worker_once", side_effect=cycle):
            self.assertEqual(supervise({"state": self.state, "node_id": "felix"},
                                       stop, interval=5, report=observed.append), 0)
        self.assertEqual(observed[0]["status"], "ok")

    def test_native_hivewire_receipt_required_and_bound_to_commit(self):
        repo = self.root / "repo"
        subprocess.run(["git", "init", str(repo)], check=True, capture_output=True)
        path = repo / ".git" / "swarm"
        path.mkdir()
        board = path / "swarm.db"
        with sqlite3.connect(board) as con:
            con.execute("CREATE TABLE goals(id TEXT, state TEXT, closure TEXT)")
            con.execute("CREATE TABLE verification_runs(id TEXT, state TEXT, commit_sha TEXT)")
            con.execute("INSERT INTO goals VALUES (?,?,?)",
                        ("GF", "closed", json.dumps({"receipt": "r1", "status": "verified_outcomes"})))
            con.execute("INSERT INTO verification_runs VALUES (?,?,?)", ("r1", "passed", "f" * 40))
        receipt = verify_local_goal(repo, "GF", "f" * 40)
        self.assertEqual(receipt["verification_id"], "r1")
        validate_remote_claim({"hivewire_goal": receipt}, "GF", "f" * 40)
        with self.assertRaises(EvidenceRefusal):
            validate_remote_claim({"hivewire_goal": receipt}, "GF", "b" * 40)
        with self.assertRaises(EvidenceRefusal):
            verify_local_goal(repo, "GF", "b" * 40)

    def test_console_refuses_unauthenticated_reads(self):
        import http.client
        import socket
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        server = serve_console(self.state, "test-only-strong-operator-token-1", port=port)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with http.client.HTTPConnection("127.0.0.1", port, timeout=3) as client:
                client.request("GET", "/api/state")
                self.assertEqual(client.getresponse().status, 403)
            with http.client.HTTPConnection("127.0.0.1", port, timeout=3) as client:
                client.request("GET", "/api/state",
                               headers={"X-Hivewire-Operator": "test-only-strong-operator-token-1"})
                response = client.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read())["nodes"][0]["id"], "felix")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
