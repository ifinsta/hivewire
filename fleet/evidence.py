"""Read-only verification of the local Hivewire goal-closure receipt.

Native Hivewire's goals and verification_runs tables remain authoritative on the
worker. A remote copy of a receipt is an authenticated node claim, not a Master
verification. Master gates always execute independently.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path


class EvidenceRefusal(ValueError):
    """A required Hivewire completion receipt was absent or invalid."""


def _git_dir(repo: Path) -> Path:
    result = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--git-common-dir"],
        capture_output=True, text=True, check=False, timeout=10,
    )
    if result.returncode:
        raise EvidenceRefusal("not a Git worktree")
    path = Path(result.stdout.strip())
    return (repo / path).resolve() if not path.is_absolute() else path.resolve()


def verify_local_goal(repo: str | Path, goal_id: str, commit_sha: str) -> dict:
    """Confirm a locally *closed* Hivewire goal has a passed current-target receipt.

    Uses read-only SQLite mode and does not alter local Hivewire state. The caller
    must separately ensure that HEAD equals commit_sha and checks are enforced.
    """
    repo = Path(repo).resolve()
    db = _git_dir(repo) / "swarm" / "swarm.db"
    if not db.is_file():
        raise EvidenceRefusal("Hivewire board not found")
    if not isinstance(goal_id, str) or not goal_id or len(goal_id) > 120:
        raise EvidenceRefusal("invalid goal ID")
    if not isinstance(commit_sha, str) or len(commit_sha) != 40:
        raise EvidenceRefusal("invalid commit SHA")
    connection = sqlite3.connect(db.as_uri() + "?mode=ro", uri=True, timeout=8)
    connection.row_factory = sqlite3.Row
    try:
        goal = connection.execute(
            "SELECT state,closure FROM goals WHERE id=?", (goal_id,)
        ).fetchone()
        if not goal or goal["state"] != "closed" or not goal["closure"]:
            raise EvidenceRefusal("local Hivewire goal is not closed")
        closure = json.loads(goal["closure"])
        if not isinstance(closure, dict) or closure.get("status") != "verified_outcomes":
            raise EvidenceRefusal("local goal closure lacks verified outcomes")
        receipt_id = closure.get("receipt")
        if receipt_id is None:
            raise EvidenceRefusal("closure lacks verification receipt ID")
        receipt = connection.execute(
            "SELECT id,state,commit_sha FROM verification_runs WHERE id=?",
            (receipt_id,),
        ).fetchone()
        if not receipt or receipt["state"] != "passed" or receipt["commit_sha"] != commit_sha:
            raise EvidenceRefusal("verification receipt is not a passed receipt for HEAD")
        canonical = json.dumps(
            {"goal_id": goal_id, "receipt_id": receipt_id, "commit_sha": commit_sha,
             "status": "verified_outcomes"}, sort_keys=True, separators=(",", ":")
        )
        return {
            "goal_id": goal_id,
            "verification_id": str(receipt_id),
            "commit_sha": commit_sha,
            "status": "verified_outcomes",
            "local_receipt_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
        }
    except (sqlite3.DatabaseError, ValueError, KeyError, TypeError) as exc:
        raise EvidenceRefusal("local Hivewire verification data unavailable or invalid") from exc
    finally:
        connection.close()


def validate_remote_claim(proof: dict, required_goal: str, head_sha: str) -> None:
    """Check a declared node claim's structure, not its independent truth."""
    record = proof.get("hivewire_goal")
    if not isinstance(record, dict):
        raise EvidenceRefusal("native Hivewire goal completion evidence required")
    if record.get("goal_id") != required_goal or record.get("commit_sha") != head_sha:
        raise EvidenceRefusal("goal evidence does not match the submitted commit")
    if record.get("status") != "verified_outcomes":
        raise EvidenceRefusal("goal closure status is not verified_outcomes")
    if not isinstance(record.get("verification_id"), str) or not record["verification_id"]:
        raise EvidenceRefusal("goal verification receipt missing")
    if not isinstance(record.get("local_receipt_sha256"), str) or len(record["local_receipt_sha256"]) != 64:
        raise EvidenceRefusal("goal receipt digest missing")
