"""Fail-closed multi-repository consumer verification.

A trusted impact catalog identifies the owner and every directly affected
repository. Every such pinned repository must have an explicit, trusted,
digest-pinned container job. Missing work or unavailable runtimes never pass.
"""
from __future__ import annotations

from pathlib import Path

from intelligence_execution import isolated
from intelligence_graph import IndexErrorSafe, build, canonical, check_catalog, impact, sha

MAX_JOBS = 16


def selection(inventory: dict, inventory_path: Path, catalog: dict,
              repo: str, path: str) -> dict:
    current = build(inventory, inventory_path)
    check_catalog(catalog)
    if current["digest"] != catalog["digest"]:
        raise IndexErrorSafe("catalog no longer matches pinned trusted inventory")
    result = impact(catalog, repo, path)
    ids = {r["name"] for r in catalog["repositories"]}
    required = {repo}
    for reference in result["affected"]:
        source = reference["from"].split(":", 1)[0]
        if source not in ids:
            raise IndexErrorSafe("catalog contains an unknown source consumer")
        required.add(source)
    for reference in result["declaredConsumers"]:
        source = reference["from"]
        if source not in ids:
            raise IndexErrorSafe("catalog contains an unknown declared consumer")
        required.add(source)
    if not required <= ids or len(required) > MAX_JOBS:
        raise IndexErrorSafe("verification target list exceeds authorized repositories")
    plan = {"schemaVersion": 1, "catalogDigest": catalog["digest"],
            "source": repo + ":" + path, "sourceSha256": result["sourceSha256"],
            "repositories": sorted(required), "executionRequired": True,
            "independentApproval": False}
    plan["digest"] = sha(canonical(plan))
    return plan


def verify(inventory: dict, inventory_path: Path, catalog: dict, policy: dict,
           policy_path: Path, repo: str, path: str) -> dict:
    plan = selection(inventory, inventory_path, catalog, repo, path)
    if not isinstance(policy, dict) or set(policy) != {"version", "jobs"} or policy["version"] != 1:
        raise IndexErrorSafe("unsupported cross-repo verification policy")
    if not isinstance(policy["jobs"], list) or not 1 <= len(policy["jobs"]) <= MAX_JOBS:
        raise IndexErrorSafe("verification jobs count invalid")
    roots = {r["name"]: Path(r["root"]).resolve(strict=True)
             for r in inventory["repositories"]}
    policy_path = policy_path.resolve(strict=True)
    if any(policy_path.is_relative_to(root) for root in roots.values()):
        raise IndexErrorSafe("test policy cannot come from a reviewed repository")
    jobs = {}
    for job in policy["jobs"]:
        if not isinstance(job, dict) or set(job) != {"repo", "image", "argv", "timeoutSeconds"}:
            raise IndexErrorSafe("verification job schema invalid")
        key = job["repo"]
        if key not in roots or key in jobs:
            raise IndexErrorSafe("verification job unknown or duplicate")
        jobs[key] = job
    if set(jobs) != set(plan["repositories"]):
        raise IndexErrorSafe("verification policy must cover exactly every affected repository")
    entries = {e["name"]: e for e in inventory["repositories"]}
    receipts = []
    for name in plan["repositories"]:
        job = jobs[name]
        # Run all selected consumers; record false/unknown, never fabricate an
        # executable result if Docker or an image is missing.
        try:
            report, _ = isolated(entries[name], job["image"], job["argv"], job["timeoutSeconds"])
            receipts.append({"repo": name, "commit": entries[name]["commit"],
                             "status": "passed" if report["passed"] else "failed",
                             "receiptDigest": report["digest"],
                             "exitCode": report["exitCode"]})
        except (IndexErrorSafe, OSError, ValueError) as exc:
            receipts.append({"repo": name, "commit": entries[name]["commit"],
                             "status": "unknown", "errorType": type(exc).__name__,
                             "receiptDigest": None, "exitCode": None})
    result = {"schemaVersion": 1, "planDigest": plan["digest"],
              "catalogDigest": catalog["digest"], "source": plan["source"],
              "policyDigest": sha(canonical(policy)), "results": receipts,
              "allPassed": all(r["status"] == "passed" for r in receipts),
              "executions": len(receipts), "independentApproval": False,
              "clearanceGranted": False}
    result["digest"] = sha(canonical(result))
    return result
