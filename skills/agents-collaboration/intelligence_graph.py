"""Pinned, multi-repository source graph for Hivewire.

All source comes from immutable Git blobs at explicit commit SHAs. No network,
checkout, model, compiler, or repository code is executed. Cargo-resolved edges
may be imported from an externally generated, SHA-256-pinned metadata report.
Lexical edges are advisory; they are not compiler-resolved symbols.
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections import defaultdict
from pathlib import Path

from code_intelligence import CONTRACT, SOURCE, dependencies, symbols

MAX_REPOS = 16
MAX_FILES = 35_000
MAX_BYTES = 1_000_000
MAX_EDGES = 60_000
MAX_METADATA_BYTES = 8_000_000
ID_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
SHA_RE = re.compile(r"^[a-f0-9]{40,64}$")


class IndexErrorSafe(ValueError):
    pass


def canonical(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git(root: Path, *argv: str, limit: int = 5_000_000) -> bytes:
    try:
        result = subprocess.run(["git", *argv], cwd=root, capture_output=True, check=False, timeout=35)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise IndexErrorSafe("Git failed or timed out") from exc
    if result.returncode != 0 or len(result.stdout) > limit:
        raise IndexErrorSafe("Git object lookup failed or exceeded bound")
    return result.stdout


def safe_repo(entry: dict) -> tuple[str, Path, str]:
    if not isinstance(entry, dict) or set(entry) - {"name", "root", "commit", "contracts", "cargoMetadata", "cargoMetadataSha256"}:
        raise IndexErrorSafe("unrecognized repository inventory fields")
    name, root_raw, commit = entry.get("name"), entry.get("root"), entry.get("commit")
    if not isinstance(name, str) or not ID_RE.fullmatch(name):
        raise IndexErrorSafe("invalid repository identity")
    if not isinstance(root_raw, str) or not isinstance(commit, str) or not SHA_RE.fullmatch(commit):
        raise IndexErrorSafe("repository root and exact Git commit are required")
    try:
        root = Path(root_raw).resolve(strict=True)
    except OSError as exc:
        raise IndexErrorSafe("repository unavailable") from exc
    if not root.is_dir() or git(root, "rev-parse", "--show-toplevel").decode().strip() != str(root):
        raise IndexErrorSafe("inventory root must be the Git repository top-level")
    resolved = git(root, "rev-parse", "--verify", commit + "^{commit}").decode().strip()
    if resolved != commit:
        raise IndexErrorSafe("inventory commit is not an exact local commit")
    return name, root, commit


def blobs(root: Path, commit: str):
    raw = git(root, "ls-tree", "-rzl", commit, limit=12_000_000)
    seen = set()
    for record in raw.split(b"\0"):
        if not record:
            continue
        try:
            header, path_raw = record.split(b"\t", 1)
            mode, kind, oid, size = header.decode("ascii").split()
            path = path_raw.decode("utf-8", "strict")
            suffix = Path(path).suffix.lower()
            if suffix not in (SOURCE | CONTRACT):
                continue
            if not path or len(path) > 512 or path.startswith("/") or ".." in Path(path).parts or "\\" in path:
                raise IndexErrorSafe("invalid indexed source path")
            if path in seen or kind != "blob" or mode in ("120000", "160000"):
                raise IndexErrorSafe("symlink, submodule or duplicate in indexable source")
            seen.add(path)
            if not size.isdigit() or int(size) > MAX_BYTES or not SHA_RE.fullmatch(oid):
                raise IndexErrorSafe("indexable source exceeds budget")
        except (ValueError, UnicodeError) as exc:
            raise IndexErrorSafe("malformed Git tree record") from exc
        body = git(root, "cat-file", "blob", oid, limit=MAX_BYTES)
        yield path, suffix, body


def cargo_edges(entry: dict, root: Path, repo: str) -> list[dict]:
    metadata = entry.get("cargoMetadata")
    if metadata is None:
        return []
    expected = entry.get("cargoMetadataSha256")
    if not isinstance(metadata, str) or not isinstance(expected, str) or not re.fullmatch(r"[a-f0-9]{64}", expected):
        raise IndexErrorSafe("Cargo metadata requires pinned SHA-256")
    try:
        path = Path(metadata).resolve(strict=True)
        if path.is_relative_to(root) or not path.is_file() or path.stat().st_size > MAX_METADATA_BYTES:
            raise IndexErrorSafe("Cargo metadata must be trusted, bounded, outside the source repository")
        raw = path.read_bytes()
    except OSError as exc:
        raise IndexErrorSafe("Cargo metadata unavailable") from exc
    if sha(raw) != expected:
        raise IndexErrorSafe("Cargo metadata checksum mismatch")
    try:
        doc = json.loads(raw)
    except ValueError as exc:
        raise IndexErrorSafe("Cargo metadata malformed") from exc
    if doc.get("version") != 1 or Path(doc.get("workspace_root", "")).resolve() != root:
        raise IndexErrorSafe("Cargo metadata workspace mismatch")
    packages = {p["id"] for p in doc.get("packages", [])}
    members = set(doc.get("workspace_members", []))
    nodes = (doc.get("resolve") or {}).get("nodes")
    if not packages or not members or not members <= packages or not isinstance(nodes, list):
        raise IndexErrorSafe("Cargo resolver metadata incomplete")
    seen = set()
    edges = []
    for n in nodes:
        source = n.get("id")
        if source not in packages or source in seen:
            raise IndexErrorSafe("invalid or duplicate Cargo resolved package")
        seen.add(source)
        for dep in n.get("deps", []):
            dest = dep.get("pkg")
            if dest not in packages:
                raise IndexErrorSafe("Cargo resolved target not declared")
            for item in dep.get("dep_kinds", []):
                kind = item.get("kind") or "normal"
                if kind not in ("normal", "dev", "build"):
                    raise IndexErrorSafe("invalid Cargo dependency kind")
                edges.append({"from": repo + "#" + source, "to": repo + "#" + dest,
                              "kind": "cargo-resolved", "depKind": kind,
                              "target": item.get("target")})
                if len(edges) > MAX_EDGES:
                    raise IndexErrorSafe("Cargo dependency graph exceeds budget")
    if not members <= seen:
        raise IndexErrorSafe("Cargo metadata resolver omitted workspace member")
    return sorted(edges, key=lambda e: canonical(e))


def build(inventory: dict, inventory_path: Path) -> dict:
    if not isinstance(inventory, dict) or set(inventory) != {"version", "repositories"} or inventory["version"] != 1:
        raise IndexErrorSafe("inventory schema unsupported")
    declared = inventory["repositories"]
    if not isinstance(declared, list) or not 1 <= len(declared) <= MAX_REPOS:
        raise IndexErrorSafe("repository count outside budget")
    roots = [safe_repo(item) for item in declared]
    if len({r[0] for r in roots}) != len(roots):
        raise IndexErrorSafe("duplicate repository name")
    if any(inventory_path.is_relative_to(root) for _, root, _ in roots):
        raise IndexErrorSafe("trusted inventory must be outside the indexed worktrees")
    files = []
    explicit = []
    compiled = []
    for entry, (name, root, commit) in zip(declared, roots):
        present = set()
        for path, suffix, body in blobs(root, commit):
            if len(files) >= MAX_FILES:
                raise IndexErrorSafe("cross-repository file budget exceeded")
            text = body.decode("utf-8", errors="replace")
            files.append({"repo": name, "commit": commit, "path": path, "sha256": sha(body),
                          "extension": suffix, "symbols": sorted(symbols(text, suffix))[:200],
                          "dependencies": sorted(dependencies(text))[:256]})
            present.add(path)
        contracts = entry.get("contracts", [])
        if not isinstance(contracts, list) or len(contracts) > 300:
            raise IndexErrorSafe("contract inventory malformed")
        for item in contracts:
            if not isinstance(item, dict) or set(item) != {"path", "consumers"}:
                raise IndexErrorSafe("contract mapping invalid")
            path, consumers = item["path"], item["consumers"]
            if path not in present or not isinstance(consumers, list) or len(consumers) > MAX_REPOS:
                raise IndexErrorSafe("contract must refer to an indexed source")
            for consumer in consumers:
                if consumer not in {r[0] for r in roots} or consumer == name:
                    raise IndexErrorSafe("declared consumer must exist and differ from owner")
                explicit.append({"from": consumer, "to": name + ":" + path, "kind": "declared-consumer"})
        compiled.extend(cargo_edges(entry, root, name))
    files.sort(key=lambda f: (f["repo"], f["path"]))
    paths = defaultdict(list)
    for file in files:
        paths[Path(file["path"]).stem.lower()].append(file)
    edges = []
    seen = set()
    for file in files:
        for token in file["dependencies"]:
            for target in paths.get(token, []):
                if file["repo"] == target["repo"] and file["path"] == target["path"]:
                    continue
                pair = (file["repo"] + ":" + file["path"], target["repo"] + ":" + target["path"])
                if pair in seen:
                    continue
                seen.add(pair)
                edges.append({"from": pair[0], "to": pair[1], "kind": "lexical-hint"})
                if len(edges) > MAX_EDGES:
                    raise IndexErrorSafe("cross-repository lexical graph exceeds budget")
    result = {"schemaVersion": 1, "repositories": [
        {"name": name, "commit": commit} for name, _, commit in roots
    ], "files": files,
        "edges": sorted(edges, key=lambda e: (e["from"], e["to"])),
        "contracts": sorted(explicit, key=lambda e: (e["from"], e["to"])),
        "cargoResolvedEdges": compiled, "sourceBodiesStored": False,
        "compilerExecutedByIndexer": False, "symbolResolution": False,
        "graphCompleteness": "bounded-advisory"}
    result["digest"] = sha(canonical(result))
    return result


def check_catalog(catalog: dict) -> dict:
    if not isinstance(catalog, dict) or catalog.get("schemaVersion") != 1:
        raise IndexErrorSafe("catalog version unsupported")
    body = dict(catalog)
    digest = body.pop("digest", None)
    if not isinstance(digest, str) or sha(canonical(body)) != digest:
        raise IndexErrorSafe("catalog content integrity mismatch")
    if not isinstance(body.get("files"), list) or len(body["files"]) > MAX_FILES:
        raise IndexErrorSafe("catalog file inventory invalid")
    return catalog


def impact(catalog: dict, repo: str, path: str) -> dict:
    check_catalog(catalog)
    sources = [f for f in catalog["files"] if f["repo"] == repo and f["path"] == path]
    if len(sources) != 1:
        raise IndexErrorSafe("target source not in pinned graph")
    target = repo + ":" + path
    affected = [e for e in catalog["edges"] if e["to"] == target]
    declared = [e for e in catalog["contracts"] if e["to"] == target]
    result = {"source": target, "sourceSha256": sources[0]["sha256"],
              "catalogDigest": catalog["digest"], "affected": affected,
              "declaredConsumers": declared, "independentVerification": False,
              "requiresIndependentReview": bool(declared or affected)}
    result["digest"] = sha(canonical(result))
    return result
