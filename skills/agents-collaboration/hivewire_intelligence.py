#!/usr/bin/env python3
"""Hivewire's governed code-intelligence helper (Python 3.11+, stdlib only).

Offline Git-pinned multi-repo indexing, independently isolated test/reviewer
jobs, measured feedback and local read-only MCP. No operation here signs or
grants a review clearance: Jani's independent Ed25519 authority validates it.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from intelligence_execution import isolated, validate_agent_review
from intelligence_graph import IndexErrorSafe, build, check_catalog, impact
from intelligence_mcp import serve
from intelligence_metrics import compare, record

MAX_JSON = 20_000_000


def load(path: str, limit: int = MAX_JSON) -> dict:
    source = Path(path).resolve(strict=True)
    if not source.is_file() or source.stat().st_size > limit:
        raise IndexErrorSafe("JSON input outside size limit")
    document = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise IndexErrorSafe("JSON input must be an object")
    return document


def write_once(path: str, body: dict) -> None:
    dest = Path(path).resolve()
    if not dest.parent.is_dir() or dest.exists() or dest.is_symlink():
        raise IndexErrorSafe("output must be a new file under an existing directory")
    with dest.open("x", encoding="utf-8", newline="\n") as f:
        json.dump(body, f, indent=2, sort_keys=True)
        f.write("\n")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="mode", required=True)
    indexer = sub.add_parser("index", help="index pinned Git repositories")
    indexer.add_argument("--inventory", required=True)
    indexer.add_argument("--output", required=True)
    impacted = sub.add_parser("impact", help="query impact of a pinned source path")
    impacted.add_argument("--catalog", required=True)
    impacted.add_argument("--repo", required=True)
    impacted.add_argument("--path", required=True)
    for name in ("test", "review-agent"):
        run = sub.add_parser(name, help="run in a pinned read-only Docker container")
        run.add_argument("--inventory", required=True)
        run.add_argument("--repo", required=True)
        run.add_argument("--image", required=True)
        run.add_argument("--argv", required=True, help="JSON argv array, no shell interpolation")
        run.add_argument("--timeout", type=int, default=120)
        run.add_argument("--output", required=True)
        if name == "review-agent":
            run.add_argument("--author", required=True)
            run.add_argument("--reviewer", required=True)
    record_p = sub.add_parser("feedback-record")
    record_p.add_argument("--store", required=True)
    record_p.add_argument("--observation", required=True)
    regression = sub.add_parser("regression")
    for key in ("store", "tenant", "repo", "baseline", "candidate", "policy"):
        regression.add_argument("--" + key, required=True)
    regression.add_argument("--source", default="production",
                            choices=["production", "staging", "synthetic"])
    mcp = sub.add_parser("mcp")
    mcp.add_argument("--catalog", required=True)
    mcp.add_argument("--tenant", required=True)
    mcp.add_argument("--store")
    mcp.add_argument("--policy")
    a = p.parse_args(argv)
    try:
        if a.mode == "index":
            source = Path(a.inventory).resolve(strict=True)
            report = build(load(a.inventory, 200_000), source)
            write_once(a.output, report)
            print(json.dumps({"catalogDigest": report["digest"], "indexedFiles": len(report["files"]),
                              "repositories": len(report["repositories"]), "written": True}))
            return 0
        if a.mode == "impact":
            result = impact(check_catalog(load(a.catalog)), a.repo, a.path)
            print(json.dumps(result, sort_keys=True))
            return 0
        if a.mode in ("test", "review-agent"):
            inventory = load(a.inventory, 200_000)
            entry = next((e for e in inventory.get("repositories", [])
                          if e.get("name") == a.repo), None)
            if entry is None:
                raise IndexErrorSafe("requested repository absent from inventory")
            command = json.loads(a.argv)
            if a.mode == "test":
                result, _ = isolated(entry, a.image, command, a.timeout)
                write_once(a.output, result)
                print(json.dumps({"runtimeDigest": result["digest"], "passed": result["passed"]}))
                return 0 if result["passed"] else 2
            result, output = isolated(entry, a.image, command, a.timeout,
                                      reviewer=a.reviewer, author=a.author)
            if not result["passed"]:
                write_once(a.output, result)
                return 2
            candidate = validate_agent_review(entry, a.author, a.reviewer, result, output)
            write_once(a.output, candidate)
            print(json.dumps({"reviewDigest": candidate["digest"],
                              "candidateDecision": candidate["candidateDecision"],
                              "clearanceGranted": False}))
            return 2 if candidate["findings"] else 0
        if a.mode == "feedback-record":
            result = record(Path(a.store), load(a.observation, 200_000))
            print(json.dumps(result, sort_keys=True))
            return 0
        if a.mode == "regression":
            result = compare(Path(a.store), a.tenant, a.repo, a.baseline, a.candidate,
                             load(a.policy, 50_000), a.source)
            print(json.dumps(result, sort_keys=True))
            return 0 if result["status"] == "thresholds_met" else 2
        if a.mode == "mcp":
            policy = load(a.policy, 50_000) if a.policy else None
            return serve(Path(a.catalog), a.tenant, Path(a.store) if a.store else None, policy)
        raise IndexErrorSafe("unsupported mode")
    except (IndexErrorSafe, ValueError, OSError, KeyError,
            subprocess.TimeoutExpired) as exc:
        print(json.dumps({"status": "unknown", "errorType": type(exc).__name__,
                          "clearanceGranted": False}), file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
