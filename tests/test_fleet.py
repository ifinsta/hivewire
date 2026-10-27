"""End-to-end proof for HWP/1: real TLS, Git bundle handoff and Master integration."""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

from fleet.core import Client, Refusal, Store, cert_fingerprint, git, integrate, run_worker_once, serve


def cmd(*args: str, cwd: str | Path | None = None) -> None:
    subprocess.run(args, cwd=cwd, check=True, capture_output=True, timeout=40)


class StateTests(unittest.TestCase):
    def test_liveness_epoch_and_refusal(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            store = Store(str(Path(root) / "master.sqlite"))
            store.add_node("felix", "a" * 64)
            store.heartbeat("felix", "boot-1", 1)
            self.assertEqual(store.nodes()[0]["health"], "online")
            with self.assertRaises(Refusal):
                store.heartbeat("felix", "boot-1", 1)
            store.assign("job-1", "felix", "frontend", "a" * 40, 60)
            with self.assertRaises(Refusal):
                store.event("felix", "job-1", 99, "running")
            store.event("felix", "job-1", 1, "running")
            with store.db() as con:
                con.execute("UPDATE nodes SET last_seen=last_seen-120")
                con.execute("UPDATE jobs SET lease_until=lease_until-300")
            self.assertEqual(store.nodes()[0]["health"], "unreachable")
            with self.assertRaises(Refusal):
                store.event("felix", "job-1", 1, "running")


@unittest.skipUnless(shutil.which("git") and shutil.which("openssl"), "Git and OpenSSL required for TLS E2E")
class LiveTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.certs = self.root / "certs"
        self.certs.mkdir()
        self._certs()
        seed = self.root / "seed"
        cmd("git", "init", "-b", "main", str(seed))
        (seed / "app.txt").write_text("baseline\n")
        cmd("git", "add", "app.txt", cwd=seed)
        cmd("git", "-c", "user.name=Test", "-c", "user.email=test@localhost",
            "commit", "-m", "baseline", cwd=seed)
        self.master_repo = self.root / "master.git"
        cmd("git", "init", "--bare", str(self.master_repo))
        cmd("git", "remote", "add", "origin", str(self.master_repo), cwd=seed)
        cmd("git", "push", "origin", "main", cwd=seed)
        self.worker_repo = self.root / "worker"
        cmd("git", "clone", "-b", "main", str(self.master_repo), str(self.worker_repo))
        self.master_config = {
            "state": str(self.root / "master.sqlite"),
            "artifact_dir": str(self.root / "staged"),
            "repository": str(self.master_repo),
            "branch": "main",
            "bind": "127.0.0.1",
            "port": 0,
            "ca": str(self.certs / "ca.crt"),
            "cert": str(self.certs / "server.crt"),
            "key": str(self.certs / "server.key"),
            "profiles": {"frontend": {
                "allowed_paths": ["app.txt"],
                "verifier": "lab_unsafe",
                "allow_unsafe_local_verifier_for_tests": True,
                "gate_argv": [[sys.executable, "-c",
                               "from pathlib import Path; assert Path('app.txt').read_text() == 'optimized\\n'"]]
            }}
        }
        self.store = Store(self.master_config["state"])
        self.store.add_node("felix", cert_fingerprint(str(self.certs / "felix.crt")))
        self.service = serve(self.master_config, self.store)
        self.addCleanup(self.service.server_close)
        self.thread = threading.Thread(target=self.service.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.service.shutdown)
        self.worker_config = {
            "state": str(self.root / "worker.sqlite"),
            "node_id": "felix",
            "url": f"https://localhost:{self.service.server_address[1]}",
            "ca": str(self.certs / "ca.crt"),
            "cert": str(self.certs / "felix.crt"),
            "key": str(self.certs / "felix.key"),
            "profiles": {"frontend": {
                "repo": str(self.worker_repo),
                "run_argv": [sys.executable, "-c",
                             "from pathlib import Path; import subprocess; "
                             "Path('app.txt').write_text('optimized\\n'); "
                             "subprocess.run(['git','add','app.txt'], check=True); "
                             "subprocess.run(['git','-c','user.name=Felix',"
                             "'-c','user.email=felix@localhost','commit','-m','optimization'], check=True)"],
                "verify_argv": [sys.executable, "-c",
                                "from pathlib import Path; assert Path('app.txt').read_text() == 'optimized\\n'"]
            }}
        }

    def _certs(self) -> None:
        ca = self.certs
        cmd("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(ca / "ca.key"), "-out", str(ca / "ca.crt"),
            "-days", "1", "-subj", "/CN=HWP-Test-CA")
        for name in ("server", "felix", "unregistered"):
            cmd("openssl", "req", "-newkey", "rsa:2048", "-nodes",
                "-keyout", str(ca / f"{name}.key"), "-out", str(ca / f"{name}.csr"),
                "-subj", f"/CN={name}")
            ext = ca / f"{name}.ext"
            ext.write_text("subjectAltName=DNS:localhost\nextendedKeyUsage=" +
                           ("serverAuth" if name == "server" else "clientAuth") + "\n")
            cmd("openssl", "x509", "-req", "-in", str(ca / f"{name}.csr"),
                "-CA", str(ca / "ca.crt"), "-CAkey", str(ca / "ca.key"),
                "-CAcreateserial", "-days", "1", "-out", str(ca / f"{name}.crt"),
                "-extfile", str(ext))

    def test_node_completion_to_master_git_merge(self) -> None:
        base = git(self.master_repo, "rev-parse", "main")
        self.store.assign("task-frontend-1", "felix", "frontend", base, 3600)
        outcomes = run_worker_once(self.worker_config)
        self.assertEqual(outcomes[0]["state"], "ready_to_submit")
        with self.store.db() as con:
            row = con.execute("SELECT * FROM submissions").fetchone()
        self.assertEqual(row["state"], "received")
        sid = row["id"]
        ack = Client(self.worker_config).request("/v1/submissions/" + sid)
        self.assertEqual(ack["state"], "received")
        result = integrate(self.store, self.master_config, sid)
        self.assertEqual(result["state"], "merged")
        self.assertEqual(git(self.master_repo, "show", "main:app.txt"), "optimized")
        self.assertEqual(integrate(self.store, self.master_config, sid)["commit"], result["commit"])
        self.assertEqual(Client(self.worker_config).request("/v1/submissions/" + sid)["state"], "merged")
        with self.store.db() as con:
            self.assertEqual(con.execute("SELECT state FROM jobs WHERE id='task-frontend-1'")
                             .fetchone()["state"], "integrated")
        with Store(self.worker_config["state"]).db() as con:
            old = json.loads(con.execute("SELECT document FROM outbox").fetchone()["document"])
        self.assertEqual(Client(self.worker_config).request("/v1/submit", old)["state"], "merged")

    def test_unregistered_client_and_out_of_scope_refused(self) -> None:
        rogue = dict(self.worker_config, node_id="unregistered",
                     cert=str(self.certs / "unregistered.crt"),
                     key=str(self.certs / "unregistered.key"))
        with self.assertRaises(Refusal):
            Client(rogue).request("/v1/heartbeat", {"boot_id": "boot", "sequence": 1})
        with self.assertRaises(Refusal):
            self.store.assign("bad", "unknown", "frontend", "a" * 40, 3600)


if __name__ == "__main__":
    unittest.main()
