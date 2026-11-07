"""Isolation and independent review adapters for Hivewire code intelligence.

Docker/Podman execution is mandatory; there is no host-process fallback. The
program is read-only relative to the source repository, and creates an exact
snapshot from Git objects instead of mounting a live/dirty checkout. Model
output is untrusted hypothesis; only separate Jani Ed25519 verification can
turn a human-signed clearance into an authoritative approval.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path

from intelligence_graph import IndexErrorSafe, SHA_RE, canonical, git, safe_repo, sha

IMAGE_RE = re.compile(r"^[a-zA-Z0-9._:/-]+@sha256:[a-f0-9]{64}$")
MAX_FILES = 20_000
MAX_BLOB = 6_000_000
MAX_TOTAL = 90_000_000
MAX_OUTPUT = 2_000_000


def materialize(root: Path, commit: str, destination: Path) -> dict:
    """Write only safe regular Git blobs. Never follow symlinks or .git."""
    raw = git(root, "ls-tree", "-rzl", commit, limit=12_000_000)
    count, total = 0, 0
    for entry in raw.split(b"\0"):
        if not entry:
            continue
        try:
            meta, path_bytes = entry.split(b"\t", 1)
            mode, kind, oid, size = meta.decode("ascii").split()
            relative = path_bytes.decode("utf-8")
            path = Path(relative)
            if not relative or len(relative) > 512 or path.is_absolute() or "\\" in relative or (
                not all(part not in ("", ".", "..", ".git") for part in path.parts)
            ):
                raise IndexErrorSafe("unsafe repository member")
            if mode not in ("100644", "100755") or kind != "blob" or not size.isdigit():
                raise IndexErrorSafe("symlinks, submodules and nonregular files are not sandbox inputs")
            if not SHA_RE.fullmatch(oid) or int(size) > MAX_BLOB:
                raise IndexErrorSafe("blob over sandbox size limit")
            count += 1
            total += int(size)
            if count > MAX_FILES or total > MAX_TOTAL:
                raise IndexErrorSafe("sandbox snapshot exceeds budget")
        except (UnicodeError, ValueError) as exc:
            raise IndexErrorSafe("invalid Git tree member") from exc
        dest = destination / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        body = git(root, "cat-file", "blob", oid, limit=MAX_BLOB)
        # Only Git-sourced bytes; no host caches, git credential files or live workspace.
        dest.write_bytes(body)
        dest.chmod(0o755 if mode == "100755" else 0o644)
    return {"files": count, "bytes": total, "sourceCommit": commit}


def validate_command(command: list[str]) -> list[str]:
    if not isinstance(command, list) or not 1 <= len(command) <= 80 or any(
        not isinstance(v, str) or not v or len(v) > 2000 or "\x00" in v for v in command
    ):
        raise IndexErrorSafe("command must be a bounded argv array")
    if command[0].startswith("-") or "/" in command[0] and ".." in Path(command[0]).parts:
        raise IndexErrorSafe("command executable invalid")
    return command


def docker_argv(image: str, command: list[str], snap: Path, name: str) -> list[str]:
    if not isinstance(image, str) or not IMAGE_RE.fullmatch(image):
        raise IndexErrorSafe("container image must be pinned to sha256 digest")
    validate_command(command)
    if not re.fullmatch(r"hwi-[a-f0-9]{24}", name):
        raise IndexErrorSafe("invalid isolated job identifier")
    return [
        "docker", "run", "--pull", "never", "--rm", "--name", name,
        "--network", "none", "--read-only",
        "--user", "65534:65534", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
        "--pids-limit", "64", "--memory", "512m", "--cpus", "1",
        "--tmpfs", "/tmp:rw,noexec,nosuid,size=32m",
        "--mount", "type=bind,src=" + str(snap) + ",dst=/source,readonly",
        "--workdir", "/source", image, *command,
    ]


def isolated(entry: dict, image: str, command: list[str], timeout: int = 90,
             reviewer: str | None = None, author: str | None = None) -> tuple[dict, bytes]:
    """Execute in a daemon-managed container, fail closed if cleanup is uncertain."""
    name, root, commit = safe_repo(entry)
    if type(timeout) is not int or not 1 <= timeout <= 900:
        raise IndexErrorSafe("isolation timeout outside policy")
    if (reviewer is None) != (author is None):
        raise IndexErrorSafe("both reviewer and author are required for agent review")
    if reviewer is not None and (
        not isinstance(reviewer, str) or not isinstance(author, str)
        or reviewer == author or len(reviewer) > 128 or len(author) > 128
    ):
        raise IndexErrorSafe("independent reviewer identity required")
    if shutil.which("docker") is None:
        raise IndexErrorSafe("Docker is required; host execution is prohibited")
    identifier = "hwi-" + uuid.uuid4().hex[:24]
    with tempfile.TemporaryDirectory(prefix="hivewire-snapshot-") as folder:
        snap = Path(folder) / "source"
        snap.mkdir(mode=0o755)
        provenance = materialize(root, commit, snap)
        argv = docker_argv(image, command, snap, identifier)
        try:
            done = subprocess.run(argv, capture_output=True, timeout=timeout, check=False)
            exit_code = done.returncode
            stdout, stderr = done.stdout, done.stderr
            timeout_hit = False
        except subprocess.TimeoutExpired as exc:
            exit_code = None
            stdout, stderr = exc.stdout or b"", exc.stderr or b""
            timeout_hit = True
        except OSError as exc:
            raise IndexErrorSafe("Docker invocation failed") from exc
        finally:
            # Even on timeout, never leave a privileged/unobserved process running.
            # A cleanup error makes the verification unknown, not passed.
            cleanup = subprocess.run(["docker", "rm", "-f", identifier],
                                     capture_output=True, timeout=15, check=False)
        if cleanup.returncode not in (0, 1):
            raise IndexErrorSafe("container cleanup uncertain; inspect the Docker daemon")
        if len(stdout) > MAX_OUTPUT or len(stderr) > MAX_OUTPUT:
            raise IndexErrorSafe("isolated command output exceeds retention bound")
        report = {
            "schemaVersion": 1, "repo": name, "commit": commit,
            "containerImage": image, "argv": command,
            "exitCode": exit_code, "timeout": timeout_hit,
            "passed": exit_code == 0 and not timeout_hit,
            "stdoutSha256": sha(stdout), "stderrSha256": sha(stderr),
            "source": provenance,
            "hostExecution": False, "network": "disabled",
            "readOnlySource": True, "reviewer": reviewer, "author": author,
        }
        report["digest"] = sha(canonical(report))
        return report, stdout


def validate_agent_review(entry: dict, author: str, reviewer: str,
                          report: dict, output: bytes) -> dict:
    """Validate an agent's source quotes; never turn its opinion into clearance."""
    name, root, commit = safe_repo(entry)
    if not isinstance(output, bytes) or len(output) > 800_000:
        raise IndexErrorSafe("agent review output exceeds limit")
    try:
        response = json.loads(output)
    except ValueError as exc:
        raise IndexErrorSafe("agent review must be JSON") from exc
    if not isinstance(response, dict) or set(response) != {"headCommit", "reviewer", "findings", "decision"}:
        raise IndexErrorSafe("agent review schema invalid")
    if response["headCommit"] != commit or reviewer == author or response["reviewer"] != reviewer:
        raise IndexErrorSafe("agent review context mismatch")
    if response["decision"] not in ("concerns", "no_concerns"):
        raise IndexErrorSafe("unknown agent review verdict")
    findings = response["findings"]
    if not isinstance(findings, list) or len(findings) > 100:
        raise IndexErrorSafe("too many agent findings")
    checked = []
    for f in findings:
        if not isinstance(f, dict) or set(f) != {"path", "line", "quote", "reason"}:
            raise IndexErrorSafe("agent finding schema invalid")
        path = f["path"]
        if not isinstance(path, str) or not path or path.startswith("/") or ".." in Path(path).parts:
            raise IndexErrorSafe("unsafe agent citation path")
        line = f["line"]
        quote = f["quote"]
        if type(line) is not int or not 1 <= line <= 100_000 or not isinstance(quote, str) or not 6 <= len(quote) <= 500:
            raise IndexErrorSafe("agent citation malformed")
        if not isinstance(f["reason"], str) or not 8 <= len(f["reason"]) <= 1200:
            raise IndexErrorSafe("agent rationale invalid")
        # Resolve the exact pinned Git blob; neither the review process nor a
        # dirty worktree can create an arbitrary source reference.
        blob = git(root, "show", commit + ":" + path, limit=MAX_BLOB)
        lines = blob.decode("utf-8", errors="replace").splitlines()
        if line > len(lines) or quote not in lines[line - 1]:
            raise IndexErrorSafe("agent source quote not in reviewed commit")
        checked.append(f)
    if (response["decision"] == "no_concerns") != (not findings):
        raise IndexErrorSafe("agent decision contradicts findings")
    if report.get("repo") != name or report.get("commit") != commit or not report.get("passed") or (
        report.get("reviewer") != reviewer or report.get("author") != author
    ):
        raise IndexErrorSafe("agent run was not independently isolated")
    result = {
        "schemaVersion": 1, "repo": name, "headCommit": commit,
        "reviewer": reviewer, "author": author,
        "candidateDecision": response["decision"], "findings": checked,
        "runtimeDigest": report["digest"],
        "independentClearance": False, "needsHumanApproval": True,
    }
    result["digest"] = sha(canonical(result))
    return result
