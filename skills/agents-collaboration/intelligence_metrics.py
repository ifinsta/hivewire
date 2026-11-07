"""Bounded, append-only, tenant-scoped production regression measurements.

Observations are keyed/idempotent and store only counts and SHA-256 identifiers.
No customer message, prompt, token, source code or host metric is persisted.
A metric report is advisory until the upstream collector's provenance is verified.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from intelligence_graph import IndexErrorSafe, canonical, sha

MAX_COUNT = 1_000_000_000
FIELDS = ("exposures", "checksRun", "checksPassed", "findings", "falsePositives", "escapedDefects")
IDENTITY = ("tenant", "repo", "commit", "observationId", "source")


def validate(observation: dict) -> dict:
    if not isinstance(observation, dict) or set(observation) != set(IDENTITY + FIELDS):
        raise IndexErrorSafe("telemetry observation schema invalid")
    for field in IDENTITY:
        val = observation[field]
        if not isinstance(val, str) or not 1 <= len(val) <= 120 or any(x in val for x in ("\n", "\r", "\x00")):
            raise IndexErrorSafe("telemetry identifier invalid")
    if observation["source"] not in ("production", "staging", "synthetic"):
        raise IndexErrorSafe("unknown telemetry source")
    if len(observation["commit"]) not in (40, 64) or any(ch not in "0123456789abcdef" for ch in observation["commit"]):
        raise IndexErrorSafe("telemetry commit must be full SHA")
    for field in FIELDS:
        val = observation[field]
        if type(val) is not int or not 0 <= val <= MAX_COUNT:
            raise IndexErrorSafe("telemetry counter outside limits")
    if (observation["checksPassed"] > observation["checksRun"]
        or observation["falsePositives"] > observation["findings"]
        or observation["escapedDefects"] > observation["exposures"]):
        raise IndexErrorSafe("telemetry counters inconsistent")
    if sum(observation[f] for f in FIELDS) == 0:
        raise IndexErrorSafe("empty telemetry observation")
    return observation


def connect(store: Path) -> sqlite3.Connection:
    store.parent.mkdir(parents=True, exist_ok=True)
    if store.is_symlink():
        raise IndexErrorSafe("symlink telemetry database prohibited")
    db = sqlite3.connect(store, timeout=5)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("""CREATE TABLE IF NOT EXISTS observations (
        tenant TEXT NOT NULL,
        repo TEXT NOT NULL,
        commit_sha TEXT NOT NULL,
        observation_hash TEXT NOT NULL,
        source TEXT NOT NULL,
        exposures INTEGER NOT NULL,
        checks_run INTEGER NOT NULL,
        checks_passed INTEGER NOT NULL,
        findings INTEGER NOT NULL,
        false_positives INTEGER NOT NULL,
        escaped_defects INTEGER NOT NULL,
        payload_digest TEXT NOT NULL,
        PRIMARY KEY (tenant,repo,observation_hash)
    )""")
    return db


def record(store: Path, item: dict) -> dict:
    val = validate(item)
    identity_hash = hashlib.sha256((val["tenant"] + "\0" + val["repo"] + "\0" + val["observationId"]).encode()).hexdigest()
    digest = sha(canonical(val))
    with connect(store) as db:
        previous = db.execute("""SELECT payload_digest FROM observations
            WHERE tenant=? AND repo=? AND observation_hash=?""",
            (val["tenant"], val["repo"], identity_hash)).fetchone()
        if previous and previous[0] != digest:
            raise IndexErrorSafe("observation id reused for different metrics")
        db.execute("""INSERT OR IGNORE INTO observations
            (tenant,repo,commit_sha,observation_hash,source,exposures,
             checks_run,checks_passed,findings,false_positives,escaped_defects,payload_digest)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (val["tenant"], val["repo"], val["commit"], identity_hash, val["source"],
             val["exposures"], val["checksRun"], val["checksPassed"], val["findings"],
             val["falsePositives"], val["escapedDefects"], digest))
    return {"stored": True, "idempotencyHash": identity_hash, "observationDigest": digest,
            "rawDataStored": False, "collectorSignatureVerified": False}


def aggregate(db: sqlite3.Connection, tenant: str, repo: str, commit: str,
              source: str) -> dict:
    row = db.execute("""SELECT count(*),sum(exposures),sum(checks_run),sum(checks_passed),
        sum(findings),sum(false_positives),sum(escaped_defects)
        FROM observations WHERE tenant=? AND repo=? AND commit_sha=? AND source=?""",
        (tenant, repo, commit, source)).fetchone()
    return dict(zip(("samples",) + FIELDS, [x or 0 for x in row], strict=True))


def ppm(n: int, denominator: int) -> int | None:
    return (n * 1_000_000 // denominator) if denominator > 0 else None


def compare(store: Path, tenant: str, repo: str, before: str, after: str,
            policy: dict, source: str = "production") -> dict:
    if not isinstance(policy, dict) or set(policy) != {
        "minimumSamples", "minimumExposures", "maximumEscapeIncreasePpm",
        "maximumFalsePositiveIncreasePpm", "minimumCoveragePpm"
    }:
        raise IndexErrorSafe("regression policy requires all five limits")
    if any(type(v) is not int or not 0 <= v <= 1_000_000 for v in policy.values()):
        raise IndexErrorSafe("regression policy values outside limits")
    if policy["minimumSamples"] < 1 or policy["minimumExposures"] < 1:
        raise IndexErrorSafe("regression minima must be positive")
    if before == after:
        raise IndexErrorSafe("baseline must differ from candidate")
    if not store.is_file() or store.is_symlink():
        raise IndexErrorSafe("telemetry store does not exist or is unsafe")
    db = sqlite3.connect("file:" + store.resolve().as_posix() + "?mode=ro", uri=True, timeout=5)
    try:
        b = aggregate(db, tenant, repo, before, source)
        a = aggregate(db, tenant, repo, after, source)
    finally:
        db.close()
    coverage_b = ppm(b["checksPassed"], b["checksRun"])
    coverage_a = ppm(a["checksPassed"], a["checksRun"])
    escape_b = ppm(b["escapedDefects"], b["exposures"])
    escape_a = ppm(a["escapedDefects"], a["exposures"])
    false_b = ppm(b["falsePositives"], b["findings"])
    false_a = ppm(a["falsePositives"], a["findings"])
    adequate = (b["samples"] >= policy["minimumSamples"] and a["samples"] >= policy["minimumSamples"]
                and b["exposures"] >= policy["minimumExposures"]
                and a["exposures"] >= policy["minimumExposures"]
                and all(v is not None for v in (coverage_b, coverage_a, escape_b, escape_a, false_b, false_a)))
    violations = []
    if adequate:
        if escape_a - escape_b > policy["maximumEscapeIncreasePpm"]:
            violations.append("escaped_defects_regression")
        if false_a - false_b > policy["maximumFalsePositiveIncreasePpm"]:
            violations.append("false_positive_regression")
        if coverage_a < policy["minimumCoveragePpm"]:
            violations.append("coverage_below_floor")
    result = {
        "schemaVersion": 1, "tenant": tenant, "repo": repo,
        "baseline": before, "candidate": after, "source": source,
        "baselineMetrics": b, "candidateMetrics": a,
        "baselineRatesPpm": {"coverage": coverage_b, "escape": escape_b, "falsePositive": false_b},
        "candidateRatesPpm": {"coverage": coverage_a, "escape": escape_a, "falsePositive": false_a},
        "policy": policy, "sufficientEvidence": adequate, "violations": violations,
        "status": ("unknown_insufficient_samples" if not adequate else
                   "regression" if violations else "thresholds_met"),
        "collectorSignatureVerified": False,
        "independentApproval": False,
    }
    result["digest"] = sha(canonical(result))
    return result
