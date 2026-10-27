"""Experimental HWP/1 multi-node execution and verified Git handoff.

This module is an opt-in lab component, not production remote administration.
The Master alone advances the authoritative branch. TLS identities are pinned.
No network message contains or grants an arbitrary shell command.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import signal
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import RLock

PROTOCOL = "hwp/1"
MAX_BODY = 16 * 1024 * 1024
HEX = set("0123456789abcdef")


class Refusal(Exception):
    def __init__(self, message: str, code: int = 409):
        super().__init__(message)
        self.code = code


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha(value: str) -> str:
    if not isinstance(value, str) or len(value) != 40 or any(ch not in HEX for ch in value):
        raise Refusal("invalid Git SHA", 400)
    return value


def identifier(value: str) -> str:
    if not isinstance(value, str) or not (1 <= len(value) <= 80):
        raise Refusal("invalid identifier", 400)
    if any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for ch in value):
        raise Refusal("invalid identifier", 400)
    return value


def git(repo: str | Path, *args: str, check: bool = True) -> str:
    proc = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=False, timeout=90)
    if check and proc.returncode:
        raise Refusal("Git operation failed: " + proc.stderr.strip()[:450])
    return proc.stdout.strip()


def run_argv(argv: list[str], cwd: str | Path, timeout: int = 300) -> dict:
    """Owned local command with bounded capture and timed process cleanup.

    This is a supervisor, not an OS sandbox. Parent processes cannot prove their
    children have stopped on every platform; unknown descendants remain a risk.
    """
    if not isinstance(argv, list) or not argv or not all(isinstance(v, str) and v for v in argv):
        raise Refusal("missing local execution profile", 400)
    if not 1 <= timeout <= 900:
        raise Refusal("execution timeout outside policy", 400)
    with tempfile.TemporaryFile(mode="w+b") as output:
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        proc = subprocess.Popen(argv, cwd=cwd, stdout=output, stderr=subprocess.STDOUT,
                                start_new_session=(os.name != "nt"), creationflags=flags)
        timed_out = False
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            if os.name != "nt":
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            else:
                proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                if os.name != "nt":
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                else:
                    proc.kill()
                proc.wait(timeout=5)
        output.seek(0)
        total = 0
        sha256 = hashlib.sha256()
        tail = bytearray()
        while True:
            chunk = output.read(65536)
            if not chunk:
                break
            total += len(chunk)
            sha256.update(chunk)
            tail = (tail + chunk)[-1600:]
        return {"exit_code": -1 if timed_out else proc.returncode,
                "timed_out": timed_out, "output_bytes": total,
                "output_sha256": sha256.hexdigest(),
                "tail": tail.decode("utf-8", "replace")}


class Store:
    def __init__(self, path: str):
        self.path = str(Path(path).resolve())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = RLock()
        with self.db() as con:
            con.executescript("""
                CREATE TABLE IF NOT EXISTS nodes(
                    id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL UNIQUE,
                    last_seen REAL, boot_id TEXT, sequence INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS jobs(
                    id TEXT PRIMARY KEY, node TEXT NOT NULL, epoch INTEGER NOT NULL,
                    profile TEXT NOT NULL, base_sha TEXT NOT NULL, lease_until REAL NOT NULL,
                    state TEXT NOT NULL, updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS submissions(
                    id TEXT PRIMARY KEY, job TEXT NOT NULL UNIQUE, node TEXT NOT NULL,
                    epoch INTEGER NOT NULL, head_sha TEXT NOT NULL, base_sha TEXT NOT NULL,
                    bundle_sha TEXT NOT NULL, proof_json TEXT NOT NULL, state TEXT NOT NULL,
                    prepared_sha TEXT, integrated_sha TEXT);
                CREATE TABLE IF NOT EXISTS outbox(
                    job TEXT PRIMARY KEY, document TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS worker_runs(
                    job TEXT PRIMARY KEY, epoch INTEGER NOT NULL, state TEXT NOT NULL,
                    updated REAL NOT NULL, detail TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS node_certificates(
                    fingerprint TEXT PRIMARY KEY, node_id TEXT NOT NULL,
                    revoked INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS disabled_nodes(
                    node_id TEXT PRIMARY KEY, reason TEXT NOT NULL, disabled_at REAL NOT NULL);
            """)

    def db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def add_node(self, name: str, fingerprint: str) -> None:
        identifier(name)
        if len(fingerprint) != 64 or any(c not in HEX for c in fingerprint):
            raise Refusal("invalid certificate fingerprint", 400)
        with self.lock, self.db() as con:
            con.execute("INSERT INTO nodes(id,fingerprint) VALUES (?,?)", (name, fingerprint))
            con.execute("INSERT INTO node_certificates VALUES (?,?,0,?)", (fingerprint, name, time.time()))

    def identity(self, fingerprint: str) -> str | None:
        with self.db() as con:
            row = con.execute("""
                SELECT c.node_id AS id FROM node_certificates c
                LEFT JOIN disabled_nodes d ON d.node_id=c.node_id
                WHERE c.fingerprint=? AND c.revoked=0 AND d.node_id IS NULL
                """, (fingerprint,)).fetchone()
            return row["id"] if row else None

    def revoke(self, node: str, reason: str) -> None:
        identifier(node)
        if not reason.strip():
            raise Refusal("revocation reason required", 400)
        with self.lock, self.db() as con:
            if not con.execute("SELECT 1 FROM nodes WHERE id=?", (node,)).fetchone():
                raise Refusal("unknown node", 404)
            con.execute("INSERT OR REPLACE INTO disabled_nodes VALUES (?,?,?)", (node, reason[:240], time.time()))
            con.execute("UPDATE node_certificates SET revoked=1 WHERE node_id=?", (node,))
            con.execute("UPDATE jobs SET lease_until=?,state='expired' WHERE node=? AND state IN ('queued','running')",
                        (time.time(), node))

    def rotate_certificate(self, node: str, fingerprint: str) -> None:
        identifier(node)
        if len(fingerprint) != 64 or any(c not in HEX for c in fingerprint):
            raise Refusal("invalid certificate fingerprint", 400)
        with self.lock, self.db() as con:
            if not con.execute("SELECT 1 FROM nodes WHERE id=?", (node,)).fetchone():
                raise Refusal("unknown node", 404)
            con.execute("UPDATE node_certificates SET revoked=1 WHERE node_id=?", (node,))
            con.execute("INSERT INTO node_certificates VALUES (?,?,0,?)", (fingerprint, node, time.time()))
            con.execute("DELETE FROM disabled_nodes WHERE node_id=?", (node,))

    def worker_state(self, job: str) -> dict | None:
        with self.db() as con:
            row = con.execute("SELECT * FROM worker_runs WHERE job=?", (job,)).fetchone()
            return dict(row) if row else None

    def worker_claim(self, job: str, epoch: int) -> None:
        with self.lock, self.db() as con:
            row = con.execute("SELECT * FROM worker_runs WHERE job=?", (job,)).fetchone()
            if row:
                raise Refusal("local execution already exists; reconcile instead of restarting")
            con.execute("INSERT INTO worker_runs VALUES (?,?,?,?,?)",
                        (job, epoch, "running", time.time(), ""))

    def worker_finish(self, job: str, state: str, detail: str = "") -> None:
        if state not in ("failed", "ready_to_submit", "submitted", "unknown"):
            raise Refusal("invalid local worker completion state", 400)
        with self.lock, self.db() as con:
            con.execute("UPDATE worker_runs SET state=?,updated=?,detail=? WHERE job=?",
                        (state, time.time(), detail[:500], job))

    def heartbeat(self, node: str, boot: str, sequence: int) -> dict:
        identifier(boot)
        if type(sequence) is not int or sequence < 1:
            raise Refusal("invalid heartbeat sequence", 400)
        with self.lock, self.db() as con:
            row = con.execute("SELECT boot_id,sequence FROM nodes WHERE id=?", (node,)).fetchone()
            if row is None:
                raise Refusal("unregistered node", 403)
            if row["boot_id"] == boot and sequence <= row["sequence"]:
                raise Refusal("stale heartbeat sequence")
            con.execute("UPDATE nodes SET last_seen=?,boot_id=?,sequence=? WHERE id=?",
                        (time.time(), boot, sequence, node))
        return {"ok": True, "server_time": time.time()}

    def assign(self, job: str, node: str, profile: str, base: str, ttl: int) -> dict:
        identifier(job)
        identifier(node)
        identifier(profile)
        sha(base)
        if not 60 <= ttl <= 86400:
            raise Refusal("lease outside permitted bounds", 400)
        with self.lock, self.db() as con:
            if not con.execute("SELECT 1 FROM nodes WHERE id=?", (node,)).fetchone():
                raise Refusal("node not enrolled", 404)
            previous = con.execute("SELECT epoch,state FROM jobs WHERE id=?", (job,)).fetchone()
            if previous and previous["state"] not in ("expired", "failed"):
                raise Refusal("active assignment cannot be overwritten")
            epoch = previous["epoch"] + 1 if previous else 1
            con.execute("INSERT OR REPLACE INTO jobs VALUES (?,?,?,?,?,?,?,?)",
                        (job, node, epoch, profile, base, time.time() + ttl, "queued", time.time()))
        return {"job": job, "epoch": epoch}

    def work(self, node: str) -> list[dict]:
        with self.db() as con:
            return [dict(r) for r in con.execute(
                "SELECT * FROM jobs WHERE node=? AND state IN ('queued','running') AND lease_until>? ORDER BY updated",
                (node, time.time()))]

    def event(self, node: str, job: str, epoch: int, state: str) -> dict:
        if state not in ("running", "failed"):
            raise Refusal("invalid event", 400)
        with self.lock, self.db() as con:
            row = con.execute("SELECT * FROM jobs WHERE id=?", (job,)).fetchone()
            if not row or row["node"] != node or row["epoch"] != epoch:
                raise Refusal("stale or unauthorized assignment")
            if row["lease_until"] < time.time() or row["state"] in ("submitted", "integrated"):
                raise Refusal("assignment is no longer executable")
            con.execute("UPDATE jobs SET state=?,updated=? WHERE id=?", (state, time.time(), job))
        return {"ok": True}

    def save_submission(self, node: str, body: dict, artifact_dir: str) -> dict:
        job = identifier(body.get("job"))
        epoch = body.get("epoch")
        head = sha(body.get("head_sha"))
        base = sha(body.get("base_sha"))
        bundle = base64.b64decode(body.get("bundle", ""), validate=True)
        if not bundle or len(bundle) > 12 * 1024 * 1024:
            raise Refusal("bundle exceeds upload policy", 413)
        proof = body.get("proof")
        if not isinstance(proof, dict) or proof.get("verified") is not True or proof.get("commit") != head:
            raise Refusal("local verification evidence missing", 400)
        bsha = digest(bundle)
        submission = digest(f"{job}:{epoch}:{head}:{bsha}".encode())[:32]
        folder = Path(artifact_dir)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / (submission + ".bundle")
        with self.lock, self.db() as con:
            row = con.execute("SELECT * FROM jobs WHERE id=?", (job,)).fetchone()
            if not row or row["node"] != node or row["epoch"] != epoch or row["base_sha"] != base:
                raise Refusal("stale or unauthorized assignment")
            old = con.execute("SELECT * FROM submissions WHERE job=?", (job,)).fetchone()
            if old:
                if old["id"] != submission:
                    raise Refusal("submission payload changed")
                return {"submission_id": old["id"], "state": old["state"]}
            if row["lease_until"] <= time.time() or row["state"] not in ("running", "queued"):
                raise Refusal("assignment lease expired or invalid")
            # File creation is idempotent; the DB record is authoritative.
            if path.exists() and digest(path.read_bytes()) != bsha:
                raise Refusal("staged bundle digest conflict")
            temporary = path.with_suffix(".tmp")
            temporary.write_bytes(bundle)
            os.replace(temporary, path)
            con.execute("INSERT INTO submissions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (submission, job, node, epoch, head, base, bsha,
                         json.dumps(proof, sort_keys=True), "received", None, None))
            con.execute("UPDATE jobs SET state='submitted',updated=? WHERE id=?", (time.time(), job))
        return {"submission_id": submission, "state": "received"}

    def submission(self, node: str, sid: str) -> dict:
        identifier(sid)
        with self.db() as con:
            row = con.execute("SELECT * FROM submissions WHERE id=? AND node=?", (sid, node)).fetchone()
            if not row:
                raise Refusal("submission not found", 404)
            return {"submission_id": sid, "state": row["state"], "integrated_sha": row["integrated_sha"]}

    def jobs(self) -> list[dict]:
        with self.db() as con:
            return [dict(r) for r in con.execute("SELECT * FROM jobs ORDER BY updated DESC LIMIT 1000")]

    def submissions(self) -> list[dict]:
        with self.db() as con:
            return [dict(r) for r in con.execute(
                "SELECT id,job,node,epoch,head_sha,state,integrated_sha FROM submissions ORDER BY rowid DESC LIMIT 1000")]

    def nodes(self) -> list[dict]:
        with self.db() as con:
            data = [dict(row) for row in con.execute("SELECT id,last_seen,boot_id,sequence FROM nodes")]
        now = time.time()
        for row in data:
            age = now - row["last_seen"] if row["last_seen"] else None
            row["health"] = ("unknown" if age is None else "online" if age < 30 else
                             "suspect" if age < 90 else "unreachable")
            with self.db() as con:
                if con.execute("SELECT 1 FROM disabled_nodes WHERE node_id=?", (row["id"],)).fetchone():
                    row["health"] = "revoked"
        return data

    def spool(self, job: str, doc: dict) -> None:
        with self.lock, self.db() as con:
            old = con.execute("SELECT document FROM outbox WHERE job=?", (job,)).fetchone()
            serialized = json.dumps(doc, sort_keys=True)
            if old and old["document"] != serialized:
                raise Refusal("local outbox conflict")
            con.execute("INSERT OR IGNORE INTO outbox(job,document) VALUES (?,?)", (job, serialized))

    def pending(self) -> list[dict]:
        with self.db() as con:
            return [json.loads(r["document"]) for r in con.execute(
                "SELECT document FROM outbox WHERE delivered=0 ORDER BY job")]

    def delivered(self, job: str) -> None:
        with self.lock, self.db() as con:
            con.execute("UPDATE outbox SET delivered=1 WHERE job=?", (job,))


def serve(config: dict, store: Store) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: object) -> None:
            return

        def reply(self, status: int, obj: dict | list) -> None:
            payload = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def execute(self) -> None:
            try:
                cert = self.connection.getpeercert(binary_form=True)
                identity = store.identity(digest(cert)) if cert else None
                if identity is None:
                    raise Refusal("certificate is not enrolled", 403)
                if self.command == "GET":
                    path = self.path.partition("?")[0]
                    if path == "/v1/jobs":
                        self.reply(200, {"protocol": PROTOCOL, "jobs": store.work(identity)})
                    elif path.startswith("/v1/submissions/"):
                        self.reply(200, store.submission(identity, path.rsplit("/", 1)[-1]))
                    else:
                        raise Refusal("unknown route", 404)
                    return
                if self.command != "POST":
                    raise Refusal("method not allowed", 405)
                raw_length = self.headers.get("Content-Length", "")
                if not raw_length.isdigit() or not 0 < int(raw_length) <= MAX_BODY:
                    raise Refusal("request size invalid", 413)
                body = json.loads(self.rfile.read(int(raw_length)))
                if not isinstance(body, dict) or body.get("protocol") != PROTOCOL:
                    raise Refusal("unsupported protocol", 400)
                if body.get("node_id") != identity:
                    raise Refusal("device identity mismatch", 403)
                if self.path == "/v1/heartbeat":
                    output = store.heartbeat(identity, body["boot_id"], body["sequence"])
                elif self.path == "/v1/event":
                    output = store.event(identity, body["job"], body["epoch"], body["state"])
                elif self.path == "/v1/submit":
                    output = store.save_submission(identity, body, config["artifact_dir"])
                else:
                    raise Refusal("unknown route", 404)
                self.reply(200, {"protocol": PROTOCOL, **output})
            except Refusal as exc:
                self.reply(exc.code, {"error": str(exc)})
            except (KeyError, TypeError, ValueError, base64.binascii.Error, json.JSONDecodeError):
                self.reply(400, {"error": "invalid request"})

        def do_GET(self) -> None:
            self.execute()

        def do_POST(self) -> None:
            self.execute()

    server = ThreadingHTTPServer((config.get("bind", "127.0.0.1"), int(config.get("port", 8443))), Handler)
    sslctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    sslctx.minimum_version = ssl.TLSVersion.TLSv1_3
    sslctx.verify_mode = ssl.CERT_REQUIRED
    sslctx.load_cert_chain(config["cert"], config["key"])
    sslctx.load_verify_locations(config["ca"])
    server.socket = sslctx.wrap_socket(server.socket, server_side=True)
    return server


class Client:
    def __init__(self, config: dict):
        self.cfg = config
        self.node = identifier(config["node_id"])
        ctx = ssl.create_default_context(cafile=config["ca"])
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        ctx.load_cert_chain(config["cert"], config["key"])
        self.context = ctx
        if not config["url"].startswith("https://"):
            raise Refusal("HTTPS is required", 400)
        self.url = config["url"].rstrip("/")

    def request(self, path: str, data: dict | None = None) -> dict:
        payload = None
        if data is not None:
            payload = json.dumps({"protocol": PROTOCOL, "node_id": self.node, **data}).encode()
            if len(payload) > MAX_BODY:
                raise Refusal("outbound upload too large", 413)
        req = urllib.request.Request(self.url + path, data=payload,
                                     headers={"Content-Type": "application/json"} if payload else {},
                                     method="POST" if payload else "GET")
        try:
            with urllib.request.urlopen(req, timeout=30, context=self.context) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            raise Refusal(f"Master refused ({exc.code}): {exc.read(350).decode('utf-8', 'replace')}",
                          exc.code) from exc

    def flush(self, store: Store) -> list[dict]:
        results = []
        for entry in store.pending():
            result = self.request("/v1/submit", entry)
            store.delivered(entry["job"])
            results.append(result)
        return results


def execute_one(client: Client, store: Store, job: dict, profiles: dict) -> dict:
    profile = profiles.get(job["profile"])
    if not isinstance(profile, dict):
        raise Refusal("node has not authorized this execution profile", 403)
    repo = Path(profile["repo"]).resolve()
    if not (repo / ".git").exists():
        raise Refusal("configured worker Git checkout not found", 400)
    current_base = sha(git(repo, "rev-parse", "HEAD"))
    if current_base != job["base_sha"]:
        raise Refusal("worker checkout base differs from the assignment")
    if git(repo, "status", "--porcelain"):
        raise Refusal("worker checkout is dirty")
    store.worker_claim(job["id"], job["epoch"])
    client.request("/v1/event", {"job": job["id"], "epoch": job["epoch"], "state": "running"})
    remaining = int(job["lease_until"] - time.time() - 2)
    if remaining < 1:
        store.worker_finish(job["id"], "unknown", "lease expired before starting")
        raise Refusal("lease expired before execution")
    timeout = min(int(profile.get("timeout", 300)), remaining)
    result = run_argv(profile["run_argv"], repo, timeout)
    if result["exit_code"]:
        client.request("/v1/event", {"job": job["id"], "epoch": job["epoch"], "state": "failed"})
        store.worker_finish(job["id"], "failed", "execution returned nonzero")
        return {"job": job["id"], "state": "failed", "run": result}
    remaining = int(job["lease_until"] - time.time() - 2)
    if remaining < 1:
        store.worker_finish(job["id"], "unknown", "lease expired before verification")
        raise Refusal("lease expired before verification")
    check = run_argv(profile["verify_argv"], repo, min(int(profile.get("timeout", 300)), remaining))
    if check["exit_code"] or git(repo, "status", "--porcelain"):
        client.request("/v1/event", {"job": job["id"], "epoch": job["epoch"], "state": "failed"})
        store.worker_finish(job["id"], "failed", "local verification failed")
        return {"job": job["id"], "state": "verification_failed", "check": check}
    head = sha(git(repo, "rev-parse", "HEAD"))
    anc = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", current_base, head],
                         capture_output=True, check=False)
    if head == current_base or anc.returncode != 0:
        raise Refusal("candidate must be a descendant and differ from base")
    with tempfile.TemporaryDirectory() as temp:
        bundle = Path(temp) / "submission.bundle"
        git(repo, "bundle", "create", str(bundle), "HEAD")
        encoded = base64.b64encode(bundle.read_bytes()).decode()
    proof = {"verified": True, "commit": head, "run": result,
             "local_check": check, "profile": job["profile"],
             "note": "Local profile assertions; independently reverified by Master"}
    if profile.get("hivewire_goal"):
        from .evidence import EvidenceRefusal, verify_local_goal
        try:
            proof["hivewire_goal"] = verify_local_goal(repo, profile["hivewire_goal"], head)
        except EvidenceRefusal as exc:
            store.worker_finish(job["id"], "failed", "Hivewire local closure missing")
            raise Refusal(str(exc)) from exc
    doc = {"job": job["id"], "epoch": job["epoch"], "head_sha": head,
           "base_sha": current_base, "bundle": encoded, "proof": proof}
    store.spool(job["id"], doc)
    store.worker_finish(job["id"], "ready_to_submit", head)
    # Durable outbox is written before attempting the network send.
    return {"job": job["id"], "state": "ready_to_submit", "head": head}


def run_worker_once(config: dict) -> list[dict]:
    client = Client(config)
    store = Store(config["state"])
    boot_file = Path(config["state"] + ".boot")
    if boot_file.exists():
        boot = boot_file.read_text().strip()
    else:
        boot = os.urandom(16).hex()
        boot_file.write_text(boot)
    seq_file = Path(config["state"] + ".seq")
    seq = int(seq_file.read_text()) + 1 if seq_file.exists() else 1
    seq_file.write_text(str(seq))
    client.request("/v1/heartbeat", {"boot_id": boot, "sequence": seq})
    completed = client.flush(store)
    jobs = client.request("/v1/jobs")["jobs"]
    # A delivered outbox is still finished work. Never reexecute the same job on the next poll.
    already = {item["job"] for item in store.pending()}
    output = [{"delivered": item} for item in completed]
    for job in jobs:
        if job["id"] in already or store.worker_state(job["id"]):
            continue
        outcome = execute_one(client, store, job, config["profiles"])
        output.append(outcome)
        if outcome["state"] == "ready_to_submit":
            output.extend({"delivered": item} for item in client.flush(store))
    return output


def cert_fingerprint(path: str) -> str:
    pem = Path(path).read_text()
    return digest(ssl.PEM_cert_to_DER_cert(pem))


def verify_on_master(policy: dict, checkout: Path, gate: list[str]) -> dict:
    """Execute an approved verification gate. Default refuse unisolated source execution."""
    verifier = policy.get("verifier")
    if verifier == "isolated_docker":
        image = policy.get("docker_image", "")
        # Image digests must be pinned by the operator. Never trust tags or branch content.
        if "@sha256:" not in image or not image.split("@sha256:")[-1].isalnum():
            raise Refusal("verifier image must be pinned by digest")
        if not isinstance(gate, list) or not gate or any(not isinstance(v, str) for v in gate):
            raise Refusal("invalid Master gate")
        command = [
            "docker", "run", "--rm", "--network", "none", "--read-only",
            "--cpus", "1", "--memory", "1024m", "--pids-limit", "128",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            "--user", "65534:65534", "--tmpfs", "/tmp:rw,nosuid,nodev,size=64m",
            "--mount", f"type=bind,src={checkout},dst=/workspace,readonly",
            "--workdir", "/workspace", image, *gate
        ]
        return run_argv(command, checkout, timeout=min(int(policy.get("gate_timeout", 300)), 900))
    if verifier == "lab_unsafe" and policy.get("allow_unsafe_local_verifier_for_tests") is True:
        return run_argv(gate, checkout, timeout=min(int(policy.get("gate_timeout", 300)), 900))
    raise Refusal("Master verifier must be configured as isolated_docker; local mode is test-only")


def integrate(store: Store, config: dict, sid: str) -> dict:
    identifier(sid)
    repo = Path(config["repository"]).resolve()
    branch = identifier(config.get("branch", "main"))
    with store.lock, store.db() as con:
        sub = con.execute("SELECT * FROM submissions WHERE id=?", (sid,)).fetchone()
        if not sub:
            raise Refusal("unknown submission", 404)
        if sub["state"] == "merged":
            return {"state": "merged", "commit": sub["integrated_sha"]}
        job = con.execute("SELECT * FROM jobs WHERE id=?", (sub["job"],)).fetchone()
        if job["epoch"] != sub["epoch"] or job["node"] != sub["node"]:
            raise Refusal("superseded submission")
        policy = config["profiles"][job["profile"]]
        if policy.get("requires_hivewire_goal"):
            from .evidence import EvidenceRefusal, validate_remote_claim
            try:
                validate_remote_claim(json.loads(sub["proof_json"]), policy["requires_hivewire_goal"], sub["head_sha"])
            except EvidenceRefusal as exc:
                raise Refusal(str(exc)) from exc
        bundle = Path(config["artifact_dir"]) / (sid + ".bundle")
        if not bundle.is_file() or digest(bundle.read_bytes()) != sub["bundle_sha"]:
            raise Refusal("missing or corrupted staged bundle")
        previous_target = sha(git(repo, "rev-parse", "refs/heads/" + branch))
        if sub["prepared_sha"]:
            existing = git(repo, "cat-file", "-t", sub["prepared_sha"], check=False)
            if existing == "commit":
                test = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor",
                                       sub["prepared_sha"], "refs/heads/" + branch],
                                      capture_output=True, check=False)
                if test.returncode == 0:
                    con.execute("UPDATE submissions SET state='merged',integrated_sha=? WHERE id=?",
                                (sub["prepared_sha"], sid))
                    con.execute("UPDATE jobs SET state='integrated' WHERE id=?", (sub["job"],))
                    return {"state": "merged", "commit": sub["prepared_sha"]}
        with tempfile.TemporaryDirectory(prefix="hwp-master-") as temporary:
            git(temporary, "clone", "--quiet", "--no-local", "--branch", branch, str(repo), "checkout")
            checkout = Path(temporary) / "checkout"
            git(checkout, "fetch", "--quiet", str(bundle), "HEAD")
            candidate = sha(git(checkout, "rev-parse", "FETCH_HEAD"))
            if candidate != sub["head_sha"]:
                raise Refusal("bundle does not match claimed candidate commit")
            anc = subprocess.run(["git", "-C", str(checkout), "merge-base", "--is-ancestor",
                                  sub["base_sha"], candidate], capture_output=True, check=False)
            if anc.returncode:
                raise Refusal("candidate does not descend from assigned base")
            changed = git(checkout, "diff", "--name-only", sub["base_sha"], candidate).splitlines()
            scopes = policy.get("allowed_paths", [])
            if not scopes or any(not any(p == scope.rstrip("/") or p.startswith(scope.rstrip("/") + "/")
                                         for scope in scopes) for p in changed):
                raise Refusal("candidate changes files outside approved scope")
            merge = subprocess.run(["git", "-C", str(checkout), "-c", "user.name=Hivewire Master",
                                    "-c", "user.email=hwp@localhost", "merge", "--no-ff", "--no-edit", candidate],
                                   capture_output=True, text=True, check=False, timeout=90)
            if merge.returncode:
                raise Refusal("integration conflict: " + merge.stderr[-450:])
            merged = sha(git(checkout, "rev-parse", "HEAD"))
            # This pilot runner is for disposable trusted labs. Real use requires a sandboxed gate service.
            gates = policy.get("gate_argv", [])
            if not gates:
                raise Refusal("Master gate policy required")
            for gate in gates:
                result = verify_on_master(policy, checkout, gate)
                if result["exit_code"]:
                    raise Refusal("Master gate failed: " + result["tail"][-400:])
            if git(checkout, "status", "--porcelain"):
                raise Refusal("Master gate modified tracked files")
            # Persist the prepared integration intent before moving the target.
            con.execute("UPDATE submissions SET state='integrating',prepared_sha=? WHERE id=?", (merged, sid))
            con.commit()
            # Git push performs an atomic fast-forward update; concurrent advances reject this candidate.
            push = subprocess.run(["git", "-C", str(checkout), "push", "--quiet", "origin",
                                   "HEAD:refs/heads/" + branch], capture_output=True, text=True, check=False)
            if push.returncode:
                raise Refusal("target advanced or push rejected; re-evaluate against current main")
            con.execute("UPDATE submissions SET state='merged',integrated_sha=? WHERE id=?", (merged, sid))
            con.execute("UPDATE jobs SET state='integrated' WHERE id=?", (sub["job"],))
            return {"state": "merged", "commit": merged, "previous_target": previous_target}


def cli() -> int:
    parser = argparse.ArgumentParser(description="HWP/1 experimental distributed Hivewire companion")
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("serve", "nodes", "worker-once"):
        p = sub.add_parser(name)
        p.add_argument("config")
    p = sub.add_parser("enroll")
    p.add_argument("config")
    p.add_argument("node")
    p.add_argument("certificate")
    p = sub.add_parser("assign")
    p.add_argument("config")
    p.add_argument("job")
    p.add_argument("node")
    p.add_argument("profile")
    p.add_argument("--ttl", type=int, default=3600)
    p = sub.add_parser("revoke")
    p.add_argument("config")
    p.add_argument("node")
    p.add_argument("--reason", required=True)
    p = sub.add_parser("rotate")
    p.add_argument("config")
    p.add_argument("node")
    p.add_argument("certificate")
    p = sub.add_parser("integrate")
    p.add_argument("config")
    p.add_argument("submission")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    store = Store(config["state"])
    try:
        if args.action == "serve":
            service = serve(config, store)
            print(json.dumps({"listening": service.server_address}))
            service.serve_forever()
        elif args.action == "enroll":
            store.add_node(args.node, cert_fingerprint(args.certificate))
            print(json.dumps({"enrolled": args.node}))
        elif args.action == "assign":
            base = sha(git(config["repository"], "rev-parse", "refs/heads/" + config.get("branch", "main")))
            print(json.dumps(store.assign(args.job, args.node, args.profile, base, args.ttl)))
        elif args.action == "revoke":
            store.revoke(args.node, args.reason)
            print(json.dumps({"revoked": args.node}))
        elif args.action == "rotate":
            store.rotate_certificate(args.node, cert_fingerprint(args.certificate))
            print(json.dumps({"rotated": args.node}))
        elif args.action == "integrate":
            print(json.dumps(integrate(store, config, args.submission)))
        elif args.action == "nodes":
            print(json.dumps(store.nodes(), indent=2))
        elif args.action == "worker-once":
            print(json.dumps(run_worker_once(config), indent=2))
        return 0
    except (Refusal, OSError, KeyError, subprocess.TimeoutExpired) as exc:
        print("hivewire-fleet: " + str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(cli())
