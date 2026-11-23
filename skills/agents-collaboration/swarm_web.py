"""swarm_web: the browser dashboard behind `hivewire dashboard --web`.

A small local web server for the person running the pool: standard library only, one machine. Every view reads
the board through read-only SQLite connections. The few actions a person takes from the page (answer or waive a
question, reply to or acknowledge a message, approve a brief, run the inbox steward, start a reviewer agent) run
the real `hivewire` command as the human, in that board's repository, so the engine's own checks and events apply.
`--read-only` turns actions off.

The inbox steward lives here too: a system participant that reviews every inbox, reminds late recipients and
escalates, and never acknowledges a message on anyone's behalf.

One dashboard serves every board on this machine, at a fixed address. The first `hivewire dashboard --web` starts it
(port 8787 when free); later runs, from any repository or session, add their board to it and print the same link.
The link's token is stored in ~/.agents/swarm-web/ and reused, so a bookmark keeps working after a restart.

Every data request must carry that token and a loopback Host header (this blocks other sites and DNS rebinding).
Board text is written by agents and is untrusted: the page only ever sets textContent, and its Content-Security-Policy
allows one nonce-protected script and no network but itself.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import secrets
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from socketserver import TCPServer
from urllib.parse import parse_qs, urlparse

DEFAULT_PORT = 8787
STEWARD = "steward"
AGENT_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{1,31}$")
GOAL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
REPLY_KINDS = ("decision", "status", "question", "warning")
REVIEWER_CLIS = ("claude", "codex", "opencode", "gemini", "qodercli", "kilocode", "deepseek")
REVIEWER_TITLE = "Review every inbox: answer or escalate what is waiting, acknowledge what is addressed to you"
TEXT_LIMIT = 4000
BOARD_ID = re.compile(r"^[0-9a-f]{8}$")
BODY_LIMIT = 8000
PREVIEW_LIMIT = 220
WORKING_STATES = (
    "claimed",
    "planning",
    "awaiting_answers",
    "in_progress",
    "needs_rebase",
    "gates_failed",
    "merge_blocked",
    "orphaned",
    "expired",
    "interrupted",
)
# Events that only record internal allocation bookkeeping; the terminal board shows them, a person does not need to.
NOISE = "kind NOT LIKE 'allocation!_%' ESCAPE '!' AND kind<>'contract_activated'"
TABS = {
    "question": ("question",),
    "decision": ("decision",),
    "warning": ("warning",),
    "review": ("review_request", "review_result"),
}


# ---------------------------------------------------------------------------------------------- small helpers


def _now() -> datetime:
    return datetime.now(UTC)


def _parse(ts):
    try:
        dt = datetime.fromisoformat(ts) if ts else None
    except (TypeError, ValueError):
        return None
    return dt if dt is None or dt.tzinfo else dt.replace(tzinfo=UTC)


def _age(ts, now) -> float | None:
    dt = _parse(ts)
    return max(0.0, (now - dt).total_seconds()) if dt else None


def _load(text, default):
    try:
        value = json.loads(text) if text else default
    except (TypeError, ValueError):
        return default
    return value if isinstance(value, type(default)) else default


def _clip(text, limit):
    text = "" if text is None else str(text)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _rows(db, sql, args=()):
    return db.execute(sql, args).fetchall()


def _meta(db, key):
    row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def open_readonly(path: Path) -> sqlite3.Connection:
    """A connection that cannot write, one per request (sqlite connections are not shared across threads)."""
    db = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


# ---------------------------------------------------------------------------------------------- payloads


def clock(contract, now) -> dict | None:
    """Accounted running time against the contract's terms (not instrumented tool time)."""
    if not contract:
        return None
    terms = _load(contract["terms"], {})
    elapsed = contract["active_seconds"] or 0.0
    if contract["active_since"]:
        elapsed += _age(contract["active_since"], now) or 0.0
    deadline = _parse(terms.get("deadline_at"))
    return {
        "state": contract["state"],
        "owner": contract["owner"],
        "epoch": contract["epoch"],
        "elapsed_s": round(elapsed),
        "active_min": terms.get("active_time_min"),
        "deadline_min": terms.get("deadline_min"),
        "deliberation_min": terms.get("debate_min"),
        "review_min": terms.get("review_reserve_min"),
        "checkpoint_min": terms.get("checkpoint_min"),
        "deadline_left_s": round((deadline - now).total_seconds()) if deadline else None,
        "checkpoint_due": bool(contract["checkpoint_due"]),
        "counter_count": contract["counter_count"],
        "extension_count": contract["extension_count"],
        "running": bool(contract["active_since"]),
    }


def task_card(t, now, contract=None) -> dict:
    steps = _load(t["steps"], [])
    return {
        "id": t["id"],
        "title": t["title"],
        "state": t["state"],
        "assignee": t["assignee"] or t["reserved_for"],
        "reserved": bool(t["reserved_for"] and not t["assignee"]),
        "goal": t["goal"],
        "step": [t["step_idx"] or 0, len(steps)],
        "needs": _load(t["needs"], []),
        "merged_sha": (t["merged_sha"] or "")[:8] or None,
        "clock": clock(contract, now),
    }


def directory(db, ctx) -> dict:
    """Name, kind and derived role for every agent the board has seen, so the page can write "Alex (claude) – Orchestrator"."""
    coordinator = "human" if ctx["config"]["swarm"]["coordinator"] == "human" else _meta(db, "coordinator")
    out = {
        "human": {"display": "You", "kind": "human", "role": "Owner"},
        "swarm": {"display": "System", "kind": "system", "role": ""},
        STEWARD: {"display": "Inbox steward", "kind": "steward", "role": "Steward"},
    }
    for a in _rows(db, "SELECT name, kind, state, reviewing FROM agents"):
        mine = {"reviewing": a["reviewing"], "coordinator": a["name"] == coordinator and a["state"] not in ("offline", "left")}
        out[a["name"]] = {"display": a["name"][:1].upper() + a["name"][1:], "kind": a["kind"] or "", "role": role_of(mine)}
    return out


def describer(db, ctx):
    """Turn a board event into a sentence for a person: no raw event ids, and agents by display name."""
    names = {n: v["display"] for n, v in directory(db, ctx).items() if n not in ("human", "swarm", STEWARD) and len(n) > 1}
    pattern = re.compile(r"(?<![\w-])(" + "|".join(re.escape(n) for n in sorted(names, key=len, reverse=True)) + r")(?![\w-])") if names else None

    def describe(e):
        color, text = ctx["describe"](e)
        if text.startswith(e["kind"]):  # an event the terminal board has no sentence for
            detail = (e["detail"] or "").strip()
            text = e["kind"].replace("_", " ").capitalize() + (f" on {e['task']}" if e["task"] else "") + (f": {detail}" if detail else "")
        if pattern:
            text = pattern.sub(lambda m: names[m.group(1)], text)
        text = re.sub(r"(?<![\w-])human(?![\w-])", "you", text)
        return color, text[:1].upper() + text[1:]

    return describe


def _table(db, name: str) -> bool:
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


ATTENTION_OPEN = ("new", "claimed", "investigating", "proposed_action", "escalated", "reopened")


def counts(db, now) -> dict:
    pending = db.execute("SELECT COUNT(*) FROM messages WHERE ack_state='pending'").fetchone()[0]
    dead = db.execute("SELECT COUNT(*) FROM messages WHERE ack_state='dead_letter'").fetchone()[0]
    questions = db.execute("SELECT COUNT(*) FROM questions WHERE answer IS NULL AND waived=0").fetchone()[0]
    contracts = db.execute("SELECT COUNT(*) FROM task_contracts WHERE state IN ('offered','countered')").fetchone()[0]
    incidents = db.execute("SELECT COUNT(*) FROM incidents WHERE resolved_at IS NULL").fetchone()[0]
    approvals = len(pending_approvals(db))
    merge = db.execute("SELECT COUNT(*) FROM tasks WHERE removed=0 AND state='ready_to_merge'").fetchone()[0]
    attention = 0
    if _table(db, "attention_items"):
        marks = ",".join("?" * len(ATTENTION_OPEN))
        attention = db.execute(
            f"SELECT COUNT(*) FROM attention_items WHERE status IN ({marks})", ATTENTION_OPEN
        ).fetchone()[0]
    return {
        "inbox": pending + dead,
        "dead_letters": dead,
        "questions": questions,
        "decisions": questions + contracts + incidents + approvals + dead,
        "merge": merge,
        "attention": attention,
    }


def attention_panel(db, now) -> dict:
    """Open signals with severity, SLA, owner and evidence; no inference of misconduct, no actions."""
    out = {"open": 0, "queue": {"P0": 0, "P1": 0, "P2": 0, "P3": 0}, "oldest_s": None, "overdue": 0, "items": [], "history": []}
    if not _table(db, "attention_items"):
        return out
    marks = ",".join("?" * len(ATTENTION_OPEN))
    rows = _rows(
        db,
        f"SELECT * FROM attention_items WHERE status IN ({marks}) ORDER BY "
        "CASE severity WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END, "
        "COALESCE(due_at,'9999'), id LIMIT 60",
        ATTENTION_OPEN,
    )
    for r in rows:
        due = _parse(r["due_at"])
        out["items"].append(
            {
                "id": r["id"],
                "kind": r["kind"],
                "severity": r["severity"],
                "status": r["status"],
                "subject": r["subject_ref"],
                "evidence": r["evidence_ref"],
                "summary": _clip(r["summary"], 260),
                "owner": r["owner"],
                "due_at": r["due_at"],
                "overdue": bool(due and due < now),
                "age_s": round(_age(r["detected_at"], now) or 0),
                "finding": _clip(r["finding"], 320) if r["finding"] else None,
                "recommendation": _clip(r["recommendation"], 320) if r["recommendation"] else None,
                "decision_owner": r["decision_owner"],
            }
        )
    out["open"] = len(out["items"])
    out["queue"] = {sev: sum(1 for i in out["items"] if i["severity"] == sev) for sev in ("P0", "P1", "P2", "P3")}
    out["oldest_s"] = max((i["age_s"] for i in out["items"]), default=None)
    out["overdue"] = sum(1 for i in out["items"] if i["overdue"])
    if _table(db, "attention_history"):
        for h in _rows(db, "SELECT * FROM attention_history ORDER BY id DESC LIMIT 40"):
            out["history"].append(
                {
                    "at": h["at"],
                    "item": h["attention"],
                    "actor": h["actor"],
                    "from": h["old_status"],
                    "to": h["new_status"],
                    "note": _clip(h["note"], 240),
                }
            )
    return out


def deputy_panel(db, now) -> dict:
    """The live lease, its fence, and the latest revoked/expired leases for audit."""
    out = {
        "active": None,
        "nominee": _meta(db, "deputy_nominee"),
        "cooldown_until": _meta(db, "deputy_cooldown_until"),
        "recent": [],
    }
    if not _table(db, "deputy_delegations"):
        return out
    row = db.execute(
        "SELECT d.*, a.state AS agent_state FROM deputy_delegations d "
        "LEFT JOIN agents a ON a.name=d.deputy WHERE d.state='active' ORDER BY d.id DESC LIMIT 1"
    ).fetchone()
    if row:
        end = _parse(row["expires_at"])
        out["active"] = {
            "id": row["id"],
            "deputy": row["deputy"],
            "issuer": row["issuer"],
            "capabilities": _load(row["capabilities"], []),
            "scope": _load(row["subject_scope"], []),
            "granted_at": row["granted_at"],
            "expires_at": row["expires_at"],
            "remaining_s": max(0, round((end - now).total_seconds())) if end else None,
            "live": row["agent_state"] not in (None, "offline", "left"),
        }
    for r in _rows(db, "SELECT * FROM deputy_delegations ORDER BY id DESC LIMIT 6"):
        out["recent"].append(
            {
                "id": r["id"],
                "deputy": r["deputy"],
                "issuer": r["issuer"],
                "state": r["state"],
                "reason": _clip(r["reason"], 200),
                "at": r["revoked_at"] or r["granted_at"],
            }
        )
    return out


def pending_approvals(db) -> list:
    out = []
    for g in _rows(db, "SELECT * FROM goals WHERE state IN ('interrogating','drafting','active')"):
        arch = _load(g["architecture"], {})
        if arch and not _load(g["architecture_approval"], {}):
            out.append(
                {
                    "goal": g["id"],
                    "title": g["title"],
                    "risk": arch.get("risk"),
                    "revision": arch.get("revision"),
                    "needs_human": arch.get("risk") == "high",
                }
            )
    return out


TASK_STATUS = {
    "claimed": ("Working", "good"),
    "planning": ("Working", "good"),
    "in_progress": ("Working", "good"),
    "awaiting_answers": ("Needs an answer", "warn"),
    "needs_rebase": ("Needs rebase", "warn"),
    "ready_to_merge": ("In merge queue", "warn"),
    "interrupted": ("Interrupted", "warn"),
    "gates_failed": ("Gates failed", "bad"),
    "merge_blocked": ("Merge blocked", "bad"),
    "orphaned": ("Orphaned", "bad"),
    "expired": ("Contract expired", "bad"),
}


def role_of(agent: dict) -> str:
    """The tool records no job titles. A role is what the board already says the agent is doing."""
    if agent.get("reviewing"):
        return "Independent reviewer"
    return "Orchestrator" if agent["coordinator"] else "Implementer"


def status_of(agent: dict) -> dict:
    """One honest label and a tone (good, warn, bad, info) for an agent; the page adds a shape so colour is never alone."""
    if agent["state"] == "offline":
        minutes = (agent["seen_s"] or 0) // 60
        silent = agent["offline_reason"] == "silent"
        return {"label": (f"Silent for {minutes} min" if minutes else "Silent") if silent else "Offline", "tone": "bad"}
    if agent.get("reviewing"):
        return {"label": "Reviewing", "tone": "good"}
    if agent["coordinator"]:
        return {"label": "Coordinating", "tone": "good"}
    task = agent["task"]
    if task:
        label, tone = TASK_STATUS.get(task["state"], ("Working", "good"))
        return {"label": label, "tone": tone}
    return {"label": "Ready", "tone": "info"}


def overview(db, ctx, now) -> dict:
    cfg = ctx["config"]
    tasks = _rows(db, "SELECT * FROM tasks WHERE removed=0 ORDER BY ord, created_at")
    contracts = {}
    for c in _rows(db, "SELECT * FROM task_contracts ORDER BY id"):
        contracts[c["task"]] = c  # the latest per task wins
    describe = describer(db, ctx)
    cards = {t["id"]: task_card(t, now, contracts.get(t["id"])) for t in tasks}
    done = {t["id"] for t in tasks if t["state"] == "done"}

    agents = []
    coordinator = _meta(db, "coordinator")
    if cfg["swarm"]["coordinator"] == "human":
        coordinator = "human"
    running = {r["agent"] for r in _rows(db, "SELECT agent FROM executions WHERE state NOT IN ('exited','terminated','failed','reconciled')")}
    hidden = 0
    for a in _rows(db, "SELECT * FROM agents WHERE state<>'left' ORDER BY joined_at"):
        # An offline agent holding no task, review or live execution is history, not part of the team.
        if a["state"] == "offline" and not a["task"] and not (a["reviewing"] if "reviewing" in a.keys() else None) and a["name"] not in running:
            hidden += 1
            continue
        env = _load(a["env"], {})
        card = cards.get(a["task"]) if a["task"] else None
        agents.append(
            {
                "name": a["name"],
                "kind": a["kind"],
                "model": a["model"],
                "state": a["state"],
                "offline_reason": a["offline_reason"] if a["state"] == "offline" else None,
                "coordinator": a["name"] == coordinator and a["state"] != "offline",
                "slot": a["slot"],
                "os": env.get("os"),
                "seen_s": None if a["heartbeat_at"] is None else round(_age(a["heartbeat_at"], now) or 0),
                "reviewing": a["reviewing"] if "reviewing" in a.keys() else None,
                "task": card,
            }
        )
    for a in agents:
        a["display"] = a["name"][:1].upper() + a["name"][1:]
        a["role"] = role_of(a)
        a["status"] = status_of(a)
    live = [a for a in agents if a["state"] not in ("offline", "left")]

    board = {"queued": [], "working": [], "merge": [], "done": []}
    for t in tasks:
        card = cards[t["id"]]
        if t["state"] == "done":
            board["done"].append(card)
        elif t["state"] == "ready_to_merge":
            board["merge"].append(card)
        elif t["state"] in WORKING_STATES:
            board["working"].append(card)
        elif t["state"] == "open":
            card["waits"] = [n for n in card["needs"] if n not in done]
            board["queued"].append(card)
    totals = {
        "tasks": len(tasks),
        "done": len(board["done"]),
        "working": len(board["working"]),
        "merge": len(board["merge"]),
        "queued": len(board["queued"]),
    }
    board["done"] = board["done"][-8:]

    goals = []
    for g in _rows(db, "SELECT * FROM goals WHERE state IN ('interrogating','drafting','active') ORDER BY created_at"):
        mine = [cards[t["id"]] for t in tasks if t["goal"] == g["id"]]
        arch = _load(g["architecture"], {})
        goals.append(
            {
                "id": g["id"],
                "title": g["title"],
                "state": g["state"],
                "total": len(mine),
                "done": sum(c["state"] == "done" for c in mine),
                "queue": sum(c["state"] == "ready_to_merge" for c in mine),
                "working": sum(c["state"] in WORKING_STATES for c in mine),
                "outcomes": _load(g["outcomes"], []),
                "architecture": {
                    "present": bool(arch),
                    "approved": bool(_load(g["architecture_approval"], {})),
                    "risk": arch.get("risk"),
                    "revision": arch.get("revision"),
                }
                if arch
                else None,
            }
        )

    needs = []
    for q in _rows(db, "SELECT * FROM questions WHERE answer IS NULL AND waived=0 ORDER BY critical DESC, id"):
        needs.append(
            {
                "type": "question",
                "id": q["id"],
                "text": _clip(q["text"], 300),
                "from": q["asker"],
                "to": q["to_whom"],
                "task": q["task"],
                "critical": bool(q["critical"]),
                "age_s": round(_age(q["created_at"], now) or 0),
            }
        )
    for a in pending_approvals(db):
        needs.append({"type": "approval", **a})
    for m in _rows(db, "SELECT * FROM messages WHERE ack_state='dead_letter' ORDER BY id DESC LIMIT 5"):
        needs.append(
            {
                "type": "dead_letter",
                "id": m["id"],
                "text": _clip(m["body"], 200),
                "from": m["sender"],
                "to": m["recipient"],
                "task": m["task"],
                "age_s": round(_age(m["created_at"], now) or 0),
            }
        )

    events = []
    for e in reversed(_rows(db, f"SELECT * FROM events WHERE {NOISE} ORDER BY id DESC LIMIT 40")):
        color, text = describe(e)
        events.append(
            {"id": e["id"], "at": e["at"], "kind": e["kind"], "agent": e["agent"], "task": e["task"], "color": color, "text": text}
        )

    lock = _load(_meta(db, "merge_lock"), {})
    return {
        "now": now.isoformat(),
        "pool": {
            "project": ctx["project"],
            "target": cfg["project"]["target"],
            "max_agents": cfg["swarm"]["max_agents"],
            "live": len(live),
            "retired": hidden,
            "solo": len(live) == 1,
            "coordinator": coordinator,
            "paused": _meta(db, "paused"),
            "merging": lock or None,
            "gates": [" ".join(c) if isinstance(c, list) else str(c) for c in cfg.get("gates", {}).get("commands", [])],
            "command": ctx["command"],
        },
        "goals": goals,
        "agents": agents,
        "needs": needs,
        "board": board,
        "totals": totals,
        "tasks": [cards[t["id"]] for t in tasks][:300],
        "events": events,
        "attention": attention_panel(db, now),
        "deputy": deputy_panel(db, now),
    }


def inbox(db, ctx, now, query) -> dict:
    tab = (query.get("tab") or ["all"])[0]
    kind = "COALESCE(envelope_kind, kind)"
    where, args = "1=1", []
    if tab == "dead_letter":
        where = "ack_state='dead_letter'"
    elif tab == "unread":
        where = (
            "EXISTS (SELECT 1 FROM message_recipients r WHERE r.msg=messages.id "
            "AND r.delivered_at IS NULL AND r.acked_at IS NULL)"
        )
    elif tab in TABS:
        where = f"{kind} IN ({','.join('?' * len(TABS[tab]))})"
        args = list(TABS[tab])
    rows = _rows(db, f"SELECT *, {kind} AS k FROM messages WHERE {where} ORDER BY id DESC LIMIT 150", args)
    stats = {}
    ids = [m["id"] for m in rows]
    if ids:
        marks = ",".join("?" * len(ids))
        for row in db.execute(
            "SELECT msg, COUNT(*), SUM(CASE WHEN acked_at IS NOT NULL THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN delivered_at IS NOT NULL OR acked_at IS NOT NULL THEN 1 ELSE 0 END) "
            f"FROM message_recipients WHERE msg IN ({marks}) GROUP BY msg",
            ids,
        ):
            total, acked, read = row[1], row[2] or 0, row[3] or 0
            stats[row[0]] = {"total": total, "read": read, "acked": acked, "unread": max(0, total - read)}
    messages = [
        {
            "id": m["id"],
            "sender": m["sender"],
            "recipient": m["recipient"],
            "kind": m["k"],
            "task": m["task"],
            "priority": m["priority"] or "normal",
            "ack_state": m["ack_state"] or "pending",
            "created_at": m["created_at"],
            "age_s": round(_age(m["created_at"], now) or 0),
            "preview": _clip(" ".join((m["body"] or "").split()), PREVIEW_LIMIT),
            "recipients": stats.get(m["id"], {"total": 0, "read": 0, "acked": 0, "unread": 0}),
        }
        for m in rows
    ]
    tabs = {"all": db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]}
    for name, kinds in TABS.items():
        tabs[name] = db.execute(
            f"SELECT COUNT(*) FROM messages WHERE {kind} IN ({','.join('?' * len(kinds))})", kinds
        ).fetchone()[0]
    tabs["unread"] = db.execute(
        "SELECT COUNT(*) FROM messages m WHERE EXISTS (SELECT 1 FROM message_recipients r "
        "WHERE r.msg=m.id AND r.delivered_at IS NULL AND r.acked_at IS NULL)"
    ).fetchone()[0]
    tabs["dead_letter"] = db.execute("SELECT COUNT(*) FROM messages WHERE ack_state='dead_letter'").fetchone()[0]
    return {"tab": tab, "tabs": tabs, "messages": messages, "command": ctx["command"]}


def message(db, ctx, now, msg_id) -> dict | None:
    m = db.execute("SELECT *, COALESCE(envelope_kind, kind) AS k FROM messages WHERE id=?", (msg_id,)).fetchone()
    if not m:
        return None

    def view(r):
        return {
            "id": r["id"],
            "sender": r["sender"],
            "recipient": r["recipient"],
            "kind": r["k"],
            "task": r["task"],
            "epoch": r["epoch"],
            "priority": r["priority"] or "normal",
            "ack_state": r["ack_state"] or "pending",
            "created_at": r["created_at"],
            "expires_at": r["expires_at"],
            "thread_id": r["thread_id"],
            "reply_to": r["reply_to"],
            "correlation_id": r["correlation_id"],
            "requires_ack": bool(r["requires_ack"]),
            "pruned": bool(r["body_pruned_at"]),
            "body": _clip(r["body"], BODY_LIMIT),
        }

    thread = [m]
    if m["thread_id"]:
        thread = _rows(
            db,
            "SELECT *, COALESCE(envelope_kind, kind) AS k FROM messages WHERE thread_id=? ORDER BY id LIMIT 50",
            (m["thread_id"],),
        )
    recipients = []
    for r in _rows(db, "SELECT * FROM message_recipients WHERE msg=? ORDER BY agent", (msg_id,)):
        claim = db.execute("SELECT * FROM message_claims WHERE msg=? AND agent=?", (msg_id, r["agent"])).fetchone()
        recipients.append(
            {
                "agent": r["agent"],
                "delivered_at": r["delivered_at"],
                "acked_at": r["acked_at"],
                "read_at": r["delivered_at"] or r["acked_at"],
                "claim": None
                if not claim
                else {
                    "consumer": claim["consumer"],
                    "attempts": claim["attempts"],
                    "outcome": claim["outcome"],
                    "lease_left_s": None
                    if claim["finished_at"]
                    else round((_parse(claim["lease_until"]) - now).total_seconds())
                    if _parse(claim["lease_until"])
                    else None,
                },
            }
        )
    return {"message": view(m), "thread": [view(r) for r in thread], "recipients": recipients, "command": ctx["command"]}


SHORT_STATE = {"ready_to_merge": "submitted", "awaiting_answers": "needs an answer"}
BAD_TASK_STATES = ("gates_failed", "merge_blocked", "orphaned", "expired")


def team(db, ctx, now) -> dict:
    """The team as a graph: who sits in which lane and which lines connect them, all read from the board.

    Lines are facts, not decoration: assignments come from task holders, submissions from the merge queue, reviews
    from open incidents, and dotted lines from messages exchanged in the last 24 hours between peers.
    """
    ov = overview(db, ctx, now)
    agents = ov["agents"]
    names = {a["name"] for a in agents}
    coordinator = next((a["name"] for a in agents if a["coordinator"]), None)
    waiting = len(ov["needs"])
    working = ov["board"]["working"] + ov["board"]["merge"]
    blocked = sum(1 for c in working if c["state"] in BAD_TASK_STATES or any(a["name"] == c["assignee"] and a["state"] == "offline" for a in agents))
    total = db.execute("SELECT COUNT(*) FROM tasks WHERE removed=0").fetchone()[0]

    # who messaged whom in the last 24 hours, peers only (a message to the coordinator is the normal chain of command)
    cutoff = (now - timedelta(hours=24)).isoformat()
    traffic: dict[tuple, dict] = {}
    per_agent: dict[str, dict] = {n: {} for n in names}
    for m in _rows(
        db,
        "SELECT sender, recipient, COALESCE(envelope_kind, kind) AS k, COUNT(*) AS n FROM messages "
        "WHERE created_at>=? GROUP BY sender, recipient, k",
        (cutoff,),
    ):
        a, b = m["sender"], m["recipient"]
        if a not in names or b not in names or a == b:
            continue
        per_agent[a][b] = per_agent[a].get(b, 0) + m["n"]
        per_agent[b][a] = per_agent[b].get(a, 0) + m["n"]
        if coordinator in (a, b):
            continue
        pair = traffic.setdefault(tuple(sorted((a, b))), {"count": 0, "kinds": {}})
        pair["count"] += m["n"]
        pair["kinds"][m["k"]] = pair["kinds"].get(m["k"], 0) + m["n"]

    incidents = {}
    for a in agents:
        if a["reviewing"]:
            row = db.execute("SELECT * FROM incidents WHERE id=?", (a["reviewing"],)).fetchone()
            if row:
                incidents[a["name"]] = {"id": row["id"], "subject": row["subject"], "task": row["task"]}

    describe = describer(db, ctx)

    def activity(name):
        out = []
        for e in _rows(db, f"SELECT * FROM events WHERE agent=? AND {NOISE} ORDER BY id DESC LIMIT 4", (name,)):
            out.append({"at": e["at"], "text": describe(e)[1]})
        return out

    nodes = [
        {
            "id": "human",
            "type": "human",
            "name": "You",
            "kind": "human",
            "role": "Owner",
            "lane": 0,
            "status": {"label": f"{waiting} waiting" if waiting else "Nothing waiting", "tone": "warn" if waiting else "good"},
            "line": "Approves, answers",
            "detail": {"task": None, "warn": None, "relations": [], "activity": []},
        }
    ]
    for a in agents:
        review = incidents.get(a["name"])
        task = a["task"]
        if review:
            line = f"Incident I{review['id']} · {review['task']}"
        elif a["coordinator"]:
            line = f"{total} tasks · {blocked} blocked" if blocked else f"{total} tasks"
        elif task:
            line = f"{task['id']} · step {min(task['step'][0] + 1, task['step'][1])} of {task['step'][1]}" if task["step"][1] else f"{task['id']} · {SHORT_STATE.get(task['state'], task['state'].replace('_', ' '))}"
        else:
            line = "No task"
        relations = []
        if coordinator and not a["coordinator"] and not review:
            relations.append({"label": "Assigned by", "who": coordinator})
        for other, inc in incidents.items():
            if inc["subject"] == a["name"]:
                relations.append({"label": "Reviewed by", "who": other, "note": f"incident I{inc['id']}"})
        if review:
            relations.append({"label": "Reviewing", "who": review["subject"], "note": f"incident I{review['id']}"})
        peers = sorted(per_agent[a["name"]].items(), key=lambda kv: -kv[1])
        warn = None
        if a["state"] == "offline" and task:
            warn = "Its branch and worktree are kept. Another agent can take the task over once it is orphaned."
        nodes.append(
            {
                "id": a["name"],
                "type": "agent",
                "name": a["display"],
                "kind": a["kind"],
                "role": a["role"],
                "lane": 1 if a["coordinator"] else 3 if review else 2,
                "status": a["status"],
                "line": line,
                "detail": {
                    "task": task,
                    "seen_s": a["seen_s"],
                    "model": a["model"],
                    "slot": a["slot"],
                    "warn": warn,
                    "relations": relations,
                    "messages": [{"peer": p, "count": n} for p, n in peers[:4]],
                    "activity": activity(a["name"]),
                },
            }
        )
    seen = _meta(db, "steward_seen")
    seen_s = _age(seen, now)
    sent = _rows(
        db,
        "SELECT recipient, COALESCE(envelope_kind, kind) AS k, COUNT(*) AS n FROM messages WHERE sender=? AND created_at>=? GROUP BY recipient, k",
        (STEWARD, cutoff),
    )
    reminders = sum(r["n"] for r in sent if r["k"] != "warning")
    escalations = sum(r["n"] for r in sent if r["k"] == "warning")
    if seen_s is not None and seen_s < 180:
        steward_status = {"label": "Active", "tone": "good"}
    elif seen_s is not None:
        steward_status = {"label": f"Last review {int(seen_s // 60)} min ago", "tone": "info"}
    else:
        steward_status = {"label": "Not started", "tone": "info"}
    nudged = sorted({r["recipient"] for r in sent if r["recipient"] in names or r["recipient"] == "human"})
    nodes.append(
        {
            "id": STEWARD,
            "type": "steward",
            "name": "Inbox steward",
            "kind": "steward",
            "role": "Steward",
            "lane": 1,
            "status": steward_status,
            "line": f"{reminders} reminders, {escalations} escalations, 24 h" if sent else "Reviews every inbox",
            "detail": {
                "task": None,
                "warn": None,
                "seen_s": None if seen_s is None else round(seen_s),
                "relations": [{"label": "Reminded", "who": n} for n in nudged],
                "activity": activity(STEWARD),
                "reminders": reminders,
                "escalations": escalations,
            },
        }
    )
    merging = ov["pool"]["merging"]
    queued = len(ov["board"]["merge"])
    nodes.append(
        {
            "id": "gate",
            "type": "gate",
            "name": "Integration gate",
            "kind": "system",
            "role": "Merge queue",
            "lane": 4,
            "status": {"label": f"Merging {merging['task']}", "tone": "warn"}
            if merging
            else {"label": f"{queued} waiting" if queued else "Idle", "tone": "warn" if queued else "info"},
            "line": "rebase, run gates, move the target by compare-and-swap",
            "detail": {"task": None, "warn": None, "relations": [], "activity": []},
        }
    )

    edges = []
    head = coordinator or "human"
    if coordinator:
        edges.append({"kind": "approves", "from": "human", "to": coordinator, "label": "approves, answers"})
    for a in agents:
        if a["coordinator"] or a["name"] in incidents:
            continue
        edges.append({"kind": "assigns", "from": head, "to": a["name"], "label": "assigns tasks"})
        if a["task"] and a["task"]["state"] == "ready_to_merge":
            edges.append({"kind": "submits", "from": a["name"], "to": "gate", "label": f"submits {a['task']['id']}"})
    for reviewer, inc in incidents.items():
        if inc["subject"] in names:
            edges.append({"kind": "review", "from": reviewer, "to": inc["subject"], "label": f"reviews incident I{inc['id']}"})
    for r in sorted({r["recipient"] for r in sent if r["recipient"] in names}):
        edges.append({"kind": "steward", "from": STEWARD, "to": r, "label": "reminders"})
    for (a, b), t in traffic.items():
        kinds = t["kinds"]
        label = "review" if "review_request" in kinds else "lesson" if "experience" in kinds else f"{t['count']} message{'s' if t['count'] != 1 else ''}"
        edges.append({"kind": "collab", "from": a, "to": b, "label": label})
    return {"nodes": nodes, "edges": edges, "pool": ov["pool"], "window_hours": 24, "command": ctx["command"]}


def decisions(db, ctx, now) -> dict:
    questions = []
    for q in _rows(db, "SELECT * FROM questions ORDER BY (answer IS NULL AND waived=0) DESC, id DESC LIMIT 25"):
        questions.append(
            {
                "id": q["id"],
                "text": _clip(q["text"], 400),
                "from": q["asker"],
                "to": q["to_whom"],
                "task": q["task"],
                "goal": q["goal"],
                "critical": bool(q["critical"]),
                "open": q["answer"] is None and not q["waived"],
                "waived": bool(q["waived"]),
                "answer": _clip(q["answer"], 400) if q["answer"] else None,
                "answered_by": q["answered_by"],
                "age_s": round(_age(q["created_at"], now) or 0),
            }
        )
    negotiations = []
    for c in _rows(
        db, "SELECT * FROM task_contracts WHERE state IN ('offered','countered','declined') ORDER BY id DESC LIMIT 12"
    ):
        t = db.execute("SELECT title FROM tasks WHERE id=?", (c["task"],)).fetchone()
        terms = _load(c["terms"], {})
        left = (_parse(c["decision_at"]) - now).total_seconds() if _parse(c["decision_at"]) else None
        negotiations.append(
            {
                "task": c["task"],
                "title": t["title"] if t else None,
                "owner": c["owner"],
                "state": c["state"],
                "counters": c["counter_count"],
                "decision_left_s": None if left is None else round(left),
                "active_min": terms.get("active_time_min"),
                "deadline_min": terms.get("deadline_min"),
                "risk": terms.get("risk_level"),
                "approval": terms.get("approval_requirements"),
            }
        )
    incidents = []
    for i in _rows(db, "SELECT * FROM incidents ORDER BY (resolved_at IS NULL) DESC, id DESC LIMIT 10"):
        review = db.execute(
            "SELECT * FROM incident_reviews WHERE incident=? ORDER BY revision DESC LIMIT 1", (i["id"],)
        ).fetchone()
        incidents.append(
            {
                "id": i["id"],
                "task": i["task"],
                "subject": i["subject"],
                "signal": _clip(i["signal"], 300),
                "state": i["state"],
                "revision": i["revision"],
                "outcome": i["outcome"],
                "resolved": bool(i["resolved_at"]),
                "reviewer": review["reviewer"] if review else None,
                "verdict": review["verdict"] if review else None,
                "deadline_left_s": None
                if not _parse(i["deadline_at"]) or i["resolved_at"]
                else round((_parse(i["deadline_at"]) - now).total_seconds()),
            }
        )
    interrupts = []
    for r in _rows(db, "SELECT * FROM interrupts ORDER BY id DESC LIMIT 10"):
        interrupts.append(
            {
                "id": r["id"],
                "target": r["target"],
                "by": r["requester"],
                "task": r["task"],
                "mode": r["mode"],
                "state": r["state"],
                "reason": _clip(r["reason"], 200),
                "age_s": round(_age(r["requested_at"], now) or 0),
            }
        )
    recent = []
    for m in _rows(db, "SELECT * FROM messages WHERE COALESCE(envelope_kind, kind)='decision' ORDER BY id DESC LIMIT 10"):
        recent.append(
            {
                "id": m["id"],
                "sender": m["sender"],
                "recipient": m["recipient"],
                "task": m["task"],
                "ack_state": m["ack_state"],
                "text": _clip(" ".join((m["body"] or "").split()), 300),
                "age_s": round(_age(m["created_at"], now) or 0),
            }
        )
    return {
        "approvals": pending_approvals(db),
        "questions": questions,
        "negotiations": negotiations,
        "incidents": incidents,
        "interrupts": interrupts,
        "decisions": recent,
        "command": ctx["command"],
    }


def task_detail(db, ctx, now, task_id) -> dict | None:
    t = db.execute("SELECT * FROM tasks WHERE id=? AND removed=0", (task_id,)).fetchone()
    if not t:
        return None
    contract = db.execute("SELECT * FROM task_contracts WHERE task=? ORDER BY id DESC LIMIT 1", (task_id,)).fetchone()
    card = task_card(t, now, contract)
    steps = []
    for i, s in enumerate(_load(t["steps"], [])):
        done = bool(s.get("done"))
        steps.append(
            {
                "n": i + 1,
                "text": _clip(s.get("text"), 300),
                "done": done,
                "current": not done and i == (t["step_idx"] or 0),
                "evidence": _clip(s.get("evidence"), 600) if done else None,
                "at": s.get("at"),
            }
        )
    checkpoints = []
    for c in _rows(db, "SELECT * FROM checkpoints WHERE task=? ORDER BY id DESC LIMIT 8", (task_id,)):
        snap = _load(c["snapshot"], {})
        checkpoints.append(
            {
                "id": c["id"],
                "epoch": c["epoch"],
                "at": c["created_at"],
                "age_s": round(_age(c["created_at"], now) or 0),
                "note": _clip(snap.get("note"), 300),
                "next": _clip(snap.get("next_step"), 300),
                "blockers": _clip(snap.get("blockers"), 300),
            }
        )
    holder = db.execute("SELECT * FROM agents WHERE name=?", (t["assignee"],)).fetchone() if t["assignee"] else None
    agent = None
    if holder:
        agent = {
            "name": holder["name"],
            "kind": holder["kind"],
            "model": holder["model"],
            "state": holder["state"],
            "slot": holder["slot"],
            "seen_s": None if holder["heartbeat_at"] is None else round(_age(holder["heartbeat_at"], now) or 0),
            "coordinator": holder["name"] == _meta(db, "coordinator"),
        }
    events = []
    for e in reversed(_rows(db, f"SELECT * FROM events WHERE task=? AND {NOISE} ORDER BY id DESC LIMIT 30", (task_id,))):
        color, text = describer(db, ctx)(e)
        events.append({"id": e["id"], "at": e["at"], "agent": e["agent"], "color": color, "text": text})
    submit = _load(t["submit"], {})
    questions = [
        {"id": q["id"], "text": _clip(q["text"], 300), "open": q["answer"] is None and not q["waived"], "critical": bool(q["critical"])}
        for q in _rows(db, "SELECT * FROM questions WHERE task=? ORDER BY id DESC LIMIT 10", (task_id,))
    ]
    return {
        "task": {
            **card,
            "scope": t["scope_hint"],
            "done_when": t["done_when"],
            "branch": t["branch"],
            "worktree": t["worktree"],
            "epoch": t["epoch"],
            "failure": _clip(t["failure"], 400) if t["failure"] else None,
            "owed": _clip(submit.get("owed"), 300) if submit.get("owed") else None,
        },
        "steps": steps,
        "locks": [{"name": r["name"], "agent": r["agent"]} for r in _rows(db, "SELECT * FROM locks WHERE task=?", (task_id,))],
        "checkpoints": checkpoints,
        "agent": agent,
        "events": events,
        "questions": questions,
        "command": ctx["command"],
    }


# ---------------------------------------------------------------------------------------------- the server


class Handler(BaseHTTPRequestHandler):
    server_version = "swarm-web"
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args) -> None:  # the console is the board's, not an access log
        pass

    # ---- plumbing
    def send_bytes(self, status: int, body: bytes, content_type: str, extra: dict | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def send_json(self, status: int, payload: dict) -> None:
        self.send_bytes(status, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

    def refuse(self, status: int, why: str) -> None:
        self.send_json(status, {"error": why})

    def origin_ok(self) -> bool:
        host = (self.headers.get("Host") or "").lower()
        if host not in self.server.hosts:
            self.refuse(403, "unexpected Host header")
            return False
        site = self.headers.get("Sec-Fetch-Site")
        if site not in (None, "same-origin", "none"):
            self.refuse(403, "cross-site request")
            return False
        return True

    def token_ok(self) -> bool:
        given = self.headers.get("X-Swarm-Token") or ""
        if not given or not secrets.compare_digest(given.encode("utf-8"), self.server.token.encode("utf-8")):
            self.refuse(403, "missing or wrong token: run `hivewire dashboard --web` in your repository to print the current link")
            return False
        return True

    # ---- routes
    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/":
            # The page holds no board data, so it loads without the token and explains itself when the token is missing.
            if not self.origin_ok():
                return
            nonce = secrets.token_urlsafe(16)
            csp = (
                f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; font-src data:; connect-src 'self'; "
                "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
            )
            import swarm_sessions

            fonts = Path(__file__).with_name("swarm_fonts.css").read_text(encoding="utf-8")
            page = swarm_sessions.dashboard_page(PAGE).replace("/*__FONT_FACES__*/", fonts).replace("__NONCE__", nonce).encode("utf-8")
            self.send_bytes(200, page, "text/html; charset=utf-8", {"Content-Security-Policy": csp})
        elif url.path.startswith("/api/"):
            if self.origin_ok() and self.token_ok():
                self.api(url)
        else:
            self.refuse(404, "not found")

    def api(self, url) -> None:
        parts = url.path[5:].strip("/").split("/")
        query = parse_qs(url.query)
        now = _now()
        name, arg = parts[0], parts[1] if len(parts) > 1 else None
        boards = known_boards()
        if name == "ping":
            return self.send_json(200, {"swarm": "dashboard", "version": 1})
        if name == "boards":
            return self.send_json(200, {"boards": board_list(boards), "default": self.server.default_board})
        wanted = (query.get("b") or [""])[0] or self.server.default_board
        entry = boards.get(wanted) if BOARD_ID.match(wanted or "") else None
        if not entry:
            return self.refuse(404, "that board is not registered with this dashboard: run `hivewire dashboard --web` in its repository")
        if not Path(entry["path"]).is_file():
            return self.refuse(404, "that board's database is no longer there")
        ctx = board_ctx(entry, self.server.describe)
        try:
            db = open_readonly(ctx["db_path"])
            try:
                if name == "overview":
                    data = overview(db, ctx, now)
                elif name == "inbox":
                    data = inbox(db, ctx, now, query)
                elif name == "message" and arg and arg.isdigit():
                    data = message(db, ctx, now, int(arg))
                elif name == "team":
                    data = team(db, ctx, now)
                elif name == "decisions":
                    data = decisions(db, ctx, now)
                elif name == "task" and arg:
                    data = task_detail(db, ctx, now, arg)
                elif name == "sessions":
                    import swarm_sessions

                    data = swarm_sessions.detail(db, ctx["db_path"].parent, arg) if arg else swarm_sessions.listing(db)
                    if data is not None:
                        data["command"] = ctx["command"]
                else:
                    return self.refuse(404, "unknown endpoint")
                if data is None:
                    return self.refuse(404, "not found")
                data["counts"] = counts(db, now)
                data["directory"] = directory(db, ctx)
                data.setdefault("pool", {"project": ctx["project"], "target": ctx["config"]["project"]["target"]})
                data["active"] = {"id": entry["id"], "name": entry["name"]}
                data["actions"] = bool(self.server.actions and launcher_ok(entry))
                data["boards"] = board_list(boards)
                data["now"] = now.isoformat()
            finally:
                db.close()
        except sqlite3.Error as exc:
            return self.refuse(503, f"the board is busy or unreadable: {exc.__class__.__name__}")
        self.send_json(200, data)

    def _no(self) -> None:
        self.send_response(405)
        self.send_header("Allow", "GET")
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_PUT = do_DELETE = do_PATCH = do_OPTIONS = _no

    def do_POST(self) -> None:
        """Actions, as the human. Same-origin page, token header, JSON body, a fixed list of commands."""
        self.close_connection = True
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = -1
        # Read what was sent before any refusal: a socket closed with unread data resets the client's connection.
        raw = self.rfile.read(length) if 0 < length <= 1_048_576 else b""
        url = urlparse(self.path)
        if url.path != "/api/act":
            return self._no()
        if not (self.origin_ok() and self.token_ok()):
            return None
        if self.headers.get("Origin") not in {f"http://{h}" for h in self.server.hosts}:
            return self.refuse(403, "actions come only from the dashboard's own page")
        if not self.server.actions:
            return self.refuse(403, "this dashboard was started with --read-only")
        if (self.headers.get("Content-Type") or "").split(";")[0].strip().lower() != "application/json":
            return self.refuse(415, "send JSON")
        if not 0 < length <= 16384:
            return self.refuse(413, "the request is empty or too large")
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return self.refuse(400, "the request is not valid JSON")
        if not isinstance(body, dict):
            return self.refuse(400, "the request must be a JSON object")
        wanted = (parse_qs(url.query).get("b") or [""])[0] or self.server.default_board
        entry = known_boards().get(wanted) if BOARD_ID.match(wanted or "") else None
        if not entry:
            return self.refuse(404, "that board is not registered with this dashboard: run `hivewire dashboard --web` in its repository")
        if not launcher_ok(entry):
            return self.refuse(409, "this board was listed by a hivewire without actions: run `hivewire dashboard --web` in its repository again")
        try:
            result = perform(entry, body)
        except ActionError as exc:
            return self.refuse(400, str(exc))
        print(f"Hivewire dashboard: {entry['name']}: you ran {body.get('action')}: {'done' if result.get('ok') else 'refused'}", flush=True)
        self.send_json(200, result)


class Server(ThreadingHTTPServer):
    """Loopback only. Windows lets two sockets share a port under SO_REUSEADDR, so it gets exclusive use instead."""

    daemon_threads = True
    allow_reuse_address = os.name != "nt"

    def server_bind(self) -> None:
        if os.name == "nt":
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        # HTTPServer.server_bind resolves the address via reverse DNS. The
        # dashboard only binds to a numeric loopback address.
        TCPServer.server_bind(self)
        self.server_name = str(self.server_address[0])
        self.server_port = self.server_address[1]


# ---------------------------------------------------------------------------------------------- shared state
# One dashboard per user: ~/.agents/swarm-web/server.json holds its port and token, boards/<id>.json one file per
# board. Each board is its own file, so two repositories registering at once never overwrite each other.


def state_dir() -> Path:
    return Path(os.environ.get("SWARM_WEB_HOME") or Path.home() / ".agents" / "swarm-web")


def _read(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
    except OSError:  # Windows has no POSIX modes
        pass
    os.replace(tmp, path)


def board_id(db_path: Path) -> str:
    return hashlib.sha256(str(db_path.resolve()).encode("utf-8")).hexdigest()[:8]


def register(sw, command: str, launcher: str | None = None) -> dict:
    """List this repository's board in the shared dashboard (idempotent; refreshes its name, gates and command)."""
    cfg = sw.config
    db_path = sw.dir / "swarm.db"
    entry = {
        "id": board_id(db_path),
        "path": str(db_path),
        "name": cfg["project"].get("name") or sw.main_root.name,
        "target": cfg["project"]["target"],
        "max_agents": cfg["swarm"]["max_agents"],
        "coordinator": cfg["swarm"]["coordinator"],
        "gates": [" ".join(c) if isinstance(c, list) else str(c) for c in cfg.get("gates", {}).get("commands", [])],
        "command": command,
        "root": str(sw.main_root),
        "launcher": launcher,
        "registered_at": _now().isoformat(),
    }
    _write(state_dir() / "boards" / f"{entry['id']}.json", entry)
    return entry


def known_boards() -> dict:
    out = {}
    for f in sorted((state_dir() / "boards").glob("*.json")):
        e = _read(f)
        path = Path(str(e.get("path", "")))
        # only a real hivewire board database: the registry is a file a person can edit, so it never names arbitrary files
        if BOARD_ID.match(str(e.get("id", ""))) and path.name == "swarm.db" and path.parent.name == "swarm":
            out[e["id"]] = e
    return out


def board_list(boards: dict) -> list:
    return [{"id": b["id"], "name": b["name"]} for b in sorted(boards.values(), key=lambda b: (b["name"].lower(), b["id"]))]


def board_ctx(entry: dict, describe) -> dict:
    return {
        "db_path": Path(entry["path"]),
        "config": {
            "project": {"target": entry.get("target", "main")},
            "swarm": {"coordinator": entry.get("coordinator", "auto"), "max_agents": entry.get("max_agents", 5)},
            "gates": {"commands": entry.get("gates", [])},
        },
        "project": entry["name"],
        "describe": describe,
        "command": entry.get("command", "hivewire"),
    }


def link(port: int, token: str, board: str) -> str:
    return f"http://127.0.0.1:{port}/?t={token}&b={board}"


def ping(port, token) -> bool:
    """Is a dashboard of ours answering on this port with this token?"""
    if not isinstance(port, int) or not isinstance(token, str):
        return False
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1.5)
        try:
            conn.request("GET", "/api/ping", headers={"Host": f"127.0.0.1:{port}", "X-Swarm-Token": token})
            response = conn.getresponse()
            return response.status == 200 and json.loads(response.read()).get("swarm") == "dashboard"
        finally:
            conn.close()
    except (OSError, ValueError, http.client.HTTPException):
        return False


def running() -> dict | None:
    """The dashboard already serving this user's boards, as {"port", "token"}, or None."""
    state = _read(state_dir() / "server.json")
    return {"port": state["port"], "token": state["token"]} if ping(state.get("port"), state.get("token")) else None


def make_server(
    sw, *, port: int | None = None, describe, command: str, reset: bool = False, launcher: str | None = None, read_only: bool = False
) -> Server:
    """A bound, not yet running server on loopback only, with this board registered and the link's token kept stable.

    port None means the saved port, then 8787 upwards; 0 means any free port. It reads sw's config once and never
    uses sw's connection.
    """
    entry = register(sw, command, launcher)
    state = _read(state_dir() / "server.json")
    token = state["token"] if isinstance(state.get("token"), str) and state["token"] and not reset else secrets.token_urlsafe(24)
    tries = [port] if port is not None else [state.get("port"), *range(DEFAULT_PORT, DEFAULT_PORT + 20), 0]
    server = None
    for candidate in dict.fromkeys(t for t in tries if isinstance(t, int)):
        try:
            server = Server(("127.0.0.1", candidate), Handler)
            break
        except OSError:
            if port is not None:
                raise
    if server is None:
        raise OSError("no free port for the dashboard")
    server.token = token
    server.describe = describe
    server.actions = not read_only
    server.default_board = entry["id"]
    bound = server.server_address[1]
    server.hosts = {f"127.0.0.1:{bound}", f"localhost:{bound}"}
    _write(state_dir() / "server.json", {"port": bound, "token": token, "pid": os.getpid(), "started_at": _now().isoformat()})
    return server


def _open(url: str, open_browser: bool) -> None:
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:  # a headless box has no browser; the URL printed above is enough
            pass


def serve(
    sw,
    *,
    port: int | None = None,
    open_browser: bool = True,
    sweep: bool = True,
    interval: int = 5,
    describe,
    command: str,
    reset: bool = False,
    launcher: str | None = None,
    read_only: bool = False,
):
    """Serve every registered board until Ctrl+C, or, if a dashboard already runs, add this board to it and return.

    `sw` is the caller's board: used for its config and, when sweeping, to sweep it. Other boards are only read.
    """
    entry = register(sw, command, launcher)
    there = running()
    if there:
        if reset:
            raise ValueError("A dashboard is already running. Stop it (Ctrl+C in its terminal), then run this again to make a new link.")
        if port is not None and port != there["port"]:
            raise ValueError(f"A dashboard is already running on port {there['port']}. Stop it first to use another port.")
        url = link(there["port"], there["token"], entry["id"])
        print(f"Hivewire dashboard: already running; {entry['name']} is now listed in it.\n  {url}", flush=True)
        print("Run `hivewire monitor` for this board's sweeps: only the terminal that started the dashboard sweeps its own board.", flush=True)
        _open(url, open_browser)
        return
    try:
        server = make_server(sw, port=port, describe=describe, command=command, reset=reset, launcher=launcher, read_only=read_only)
    except OSError:
        there = running()  # another run started it a moment ago
        if there and port is None:
            url = link(there["port"], there["token"], entry["id"])
            print(f"Hivewire dashboard: already running.\n  {url}", flush=True)
            _open(url, open_browser)
            return
        raise
    url = link(server.server_address[1], server.token, entry["id"])
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"Hivewire dashboard: {url}", flush=True)
    print(
        "One dashboard serves every board on this machine, and this link stays the same each time you start it. "
        + ("Read-only. " if read_only else "Anyone with the link can act as you on the boards it lists: keep it private. ")
        + "Ctrl+C stops it.",
        flush=True,
    )
    _open(url, open_browser)
    warned = False
    try:
        while True:
            if sweep:
                try:
                    with sw.tx():
                        sw.sweep()
                except Exception as exc:  # keep serving; say so once
                    if not warned:
                        print(f"Hivewire dashboard: the sweep failed ({exc.__class__.__name__}); still serving.", flush=True)
                        warned = True
            time.sleep(max(1, interval))
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()



# ---------------------------------------------------------------------------------------------- actions


class ActionError(ValueError):
    """A request the page should not have sent: refused before any command runs."""


def launcher_ok(entry: dict) -> bool:
    launcher, root = entry.get("launcher"), entry.get("root")
    if not isinstance(launcher, str) or not isinstance(root, str):
        return False
    path = Path(launcher)
    return path.name == "hivewire.py" and path.is_file() and (path.parent / "swarm_core.py").is_file() and Path(root).is_dir()


def _text(value, what: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > TEXT_LIMIT or "\x00" in value:
        raise ActionError(f"{what} must be 1 to {TEXT_LIMIT} characters.")
    return value.strip()


def _id(value, what: str) -> str:
    if type(value) is not int or not 0 < value < 10**12:
        raise ActionError(f"{what} must be a positive whole number.")
    return str(value)


def run_cli(entry: dict, argv: list, timeout: int = 90) -> dict:
    """Run `hivewire --json ...` as the human in the board's repository. Argument lists only: nothing goes through a shell."""
    env = {k: v for k, v in os.environ.items() if k not in ("SWARM_AS", "SWARM_EPOCH")}
    env["PYTHONUTF8"] = "1"
    try:
        done = subprocess.run(
            [sys.executable, entry["launcher"], "--json", *argv],
            cwd=entry["root"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "The command took too long. Check the board before trying again."}
    except OSError as exc:
        return {"ok": False, "error": f"Could not run Hivewire: {exc.strerror or exc}"}
    try:
        data = json.loads(done.stdout)
    except ValueError:
        return {"ok": False, "error": _clip((done.stderr or done.stdout).strip() or "hivewire returned no result.", 600)}
    ok = bool(data.get("ok"))
    err = data.get("error") or {}
    return {
        "ok": ok,
        "error": None if ok else _clip(err.get("message") if isinstance(err, dict) else str(err), 600),
        "output": _clip(data.get("output") or "", 1200),
        "result": data.get("result"),
    }


def acknowledge(entry: dict, message_id: int) -> dict:
    """Acknowledge one message addressed to the human through the bus receipt protocol, like any recipient."""
    got = run_cli(entry, ["bus", "receive", "--consumer", "web:human", "--lease-sec", "15", "--limit", "1000", "--as", "human"])
    if not got["ok"]:
        return got
    for delivery in (got.get("result") or {}).get("deliveries", []):
        receipt = delivery.get("receipt") or {}
        if receipt.get("message_id") == message_id:
            return run_cli(
                entry,
                ["bus", "ack", str(message_id), "--consumer", str(receipt["consumer"]), "--token", str(receipt["token"]), "--as", "human"],
            )
    return {
        "ok": False,
        "error": "That message is not waiting for your acknowledgement. It may be acknowledged already, held by another reader for a moment, or not addressed to you.",
    }


def next_review_task(entry: dict) -> str:
    db = open_readonly(Path(entry["path"]))
    try:
        taken = [r[0] for r in db.execute("SELECT id FROM tasks WHERE id LIKE 'INBOX-%'")]
    finally:
        db.close()
    numbers = [int(t[6:]) for t in taken if t[6:].isdigit()]
    return f"INBOX-{max(numbers, default=0) + 1}"


def perform(entry: dict, body: dict) -> dict:
    action = body.get("action")
    if action == "answer":
        return run_cli(entry, ["answer", "--as", "human", "--", _id(body.get("question"), "The question"), _text(body.get("text"), "The answer")])
    if action == "waive":
        return run_cli(entry, ["waive", "--as", "human", "--", _id(body.get("question"), "The question"), _text(body.get("text"), "The reason")])
    if action == "send":
        to, kind = body.get("to"), body.get("kind") or "decision"
        if not isinstance(to, str) or not AGENT_NAME.match(to):
            raise ActionError("Choose an agent to send the message to.")
        if kind not in REPLY_KINDS:
            raise ActionError(f"The message kind is one of {', '.join(REPLY_KINDS)}.")
        argv = ["send", "--kind", kind, "--as", "human"]
        if body.get("reply_to") is not None:
            argv += ["--reply-to", _id(body.get("reply_to"), "The message replied to")]
        return run_cli(entry, [*argv, "--", to, _text(body.get("text"), "The message")])
    if action == "read":
        return run_cli(entry, ["inbox", "--id", _id(body.get("message"), "The message"), "--as", "human"])
    if action == "ack":
        return acknowledge(entry, int(_id(body.get("message"), "The message")))
    if action == "approve":
        goal = body.get("goal")
        if not isinstance(goal, str) or not GOAL_ID.match(goal):
            raise ActionError("Name the goal whose brief to approve.")
        return run_cli(entry, ["goal", "architecture", "--approve", "--as", "human", "--", goal])
    if action == "steward":
        return run_cli(entry, ["steward", "--once"])
    if action == "start_reviewer":
        cli, name = body.get("cli"), body.get("name")
        if cli not in REVIEWER_CLIS:
            raise ActionError(f"The reviewer runs on one of {', '.join(REVIEWER_CLIS)}.")
        if not isinstance(name, str) or not AGENT_NAME.match(name):
            raise ActionError("A reviewer's name is 2 to 32 characters of a-z, 0-9 and '-'.")
        task = next_review_task(entry)
        added = run_cli(entry, ["add", "--as", "human", "--", task, REVIEWER_TITLE])
        if not added["ok"]:
            return added
        started = run_cli(entry, ["spawn", "--name", name, "--task", task, "--as", "human", "--", cli], timeout=120)
        started["task"] = task
        if not started["ok"]:
            started["error"] = f"Task {task} was created, but the agent did not start: {started['error']}"
        return started
    raise ActionError("Unknown action.")


# ---------------------------------------------------------------------------------------------- the inbox steward


def _send_once(sw, to: str, body: str, kind: str, priority: str, key: str, expires: str) -> bool:
    if sw.db.execute("SELECT 1 FROM messages WHERE sender=? AND idempotency_key=?", (STEWARD, key)).fetchone():
        return False
    sw.message(STEWARD, to, body, kind=kind, priority=priority, idempotency_key=key, expires_at=expires)
    return True


def steward_pass(sw, *, remind_after_min: int = 10) -> dict:
    """One review of every inbox. Reminds recipients of messages left unacknowledged, escalates what is stuck.

    It never acknowledges a message for anyone: a receipt must come from the recipient. Each reminder is sent once per
    batch of waiting messages (idempotency keys), repeats only after three reminder periods, and expires by itself,
    so reminders do not pile up in the inboxes they are about.
    """
    now = _now().replace(microsecond=0)
    period = max(remind_after_min, 1)
    cutoff = (now - timedelta(minutes=period)).isoformat()
    expires = (now + timedelta(minutes=max(period * 3, 30))).isoformat()
    done = {"reminded": [], "escalated": [], "dead_letters": 0}
    with sw.tx():
        coordinator = sw.coordinator()
        waiting: dict[str, list] = {}
        for r in sw.db.execute(
            "SELECT m.id, m.priority, m.created_at, r.agent FROM messages m JOIN message_recipients r ON r.msg=m.id "
            "WHERE r.acked_at IS NULL AND m.ack_state='pending' AND m.created_at<=? AND m.sender<>? AND r.agent NOT IN ('human', ?)",
            (cutoff, STEWARD, STEWARD),
        ):
            waiting.setdefault(r["agent"], []).append(r)
        states = {a["name"]: a["state"] for a in sw.db.execute("SELECT name, state FROM agents")}
        for name, rows in sorted(waiting.items()):
            if states.get(name) in (None, "left"):
                continue
            ids = sorted(r["id"] for r in rows)
            minutes = int((now - min(_parse(r["created_at"]) for r in rows)).total_seconds() // 60)
            listing = ", ".join(f"#{i}" for i in ids[:8]) + (" and more" if len(ids) > 8 else "")
            if states[name] == "offline":
                target = coordinator if coordinator and coordinator != name else "human"
                body = (
                    f"{name} is offline with {len(ids)} unacknowledged message(s) ({listing}); the oldest is {minutes} min old. "
                    f"Follow up or reassign the work. The messages stay pending until {name} acknowledges them."
                )
                if _send_once(sw, target, body, "warning", "high", f"steward:offline:{name}:{ids[-1]}", expires):
                    done["escalated"].append(name)
                continue
            critical = any(r["priority"] == "critical" for r in rows)
            body = (
                f"{len(ids)} message(s) to you are still unacknowledged ({listing}); the oldest is {minutes} min old. "
                "Read them with `hivewire bus receive`, act on them, then acknowledge each one with `hivewire bus ack`. "
                "Nobody else can acknowledge them for you."
            )
            if _send_once(sw, name, body, "status", "high" if critical else "normal", f"steward:remind:{name}:{ids[-1]}:{minutes // (period * 3)}", expires):
                done["reminded"].append(name)
            if critical and coordinator and coordinator != name:
                body = f"{name} has left a critical message unacknowledged for {minutes} min ({listing}). It becomes a dead letter when it expires."
                if _send_once(sw, coordinator, body, "warning", "high", f"steward:critical:{name}:{ids[-1]}", expires):
                    done["escalated"].append(name)
        dead = [r[0] for r in sw.db.execute("SELECT id FROM messages WHERE ack_state='dead_letter' ORDER BY id")]
        if dead:
            done["dead_letters"] = len(dead)
            listing = ", ".join(f"#{i}" for i in dead[:8])
            _send_once(sw, "human", f"{len(dead)} critical message(s) expired unacknowledged ({listing}). Review and resolve them.", "warning", "high", f"steward:dead:{dead[-1]}", expires)
        sw.set_meta("steward_seen", now.isoformat())
        if done["reminded"] or done["escalated"]:
            sw.event(
                "steward_review",
                STEWARD,
                None,
                f"reminded {', '.join(done['reminded']) or 'nobody'}; escalated {', '.join(done['escalated']) or 'nothing'}",
            )
    return done

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light">
<title>Hivewire</title>
<style nonce="__NONCE__">
/*__FONT_FACES__*/
:root{color-scheme:light;--font-body:"Archivo","Helvetica Neue",Arial,sans-serif;--font-mono:"IBM Plex Mono",ui-monospace,Menlo,monospace;--bg:#F4F1E9;--surface:#FBF9F3;--surface-soft:#EBE6DA;--grid-ink:rgba(21,33,31,.055);--workspace-grid:linear-gradient(var(--grid-ink) 1px,transparent 1px),linear-gradient(90deg,var(--grid-ink) 1px,transparent 1px);--line:#D8D2C4;--line2:#E4DFD3;--line-strong:#B9B1A0;--text:#15211F;--ink-soft:#1F302C;--mute:#4C5A55;--dim:#5F6B64;--accent:#0D6B5E;--accentbg:#DFECE7;--accentink:#084C43;--signal:#B93D0A;--signalbg:#F7E4D8;--good:#0D6B5E;--warn:#B93D0A;--warnbg:#F7E4D8;--warnline:#D6A48A;--bad:#B93D0A;--badbg:#F7E4D8;--info:#1F302C;--tile:#EBE6DA}
*{box-sizing:border-box}
html,body{margin:0;background:var(--bg);color:var(--text)}
body{font:400 13px/18px var(--font-body);min-height:100vh;min-height:100dvh;background-image:var(--workspace-grid);background-size:32px 32px}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
:focus-visible{outline:1px solid var(--accent);outline-offset:1px}
h1{margin:0;font-size:14px;line-height:20px;font-weight:700;letter-spacing:-.02em}
h2{margin:0;font-size:12px;line-height:16px;font-weight:700;color:var(--text);letter-spacing:-.01em}
.mono{font-family:var(--font-mono);font-size:12px}
.muted{color:var(--mute)}.dim{color:var(--dim)}.small{font-size:12px;line-height:16px}.b{font-weight:600}.m{font-weight:500}.err{color:var(--bad)}.ok{color:var(--good)}
.top{background:var(--surface);border-bottom:1px solid var(--line);display:flex;align-items:center;gap:12px;padding:0 12px;flex-wrap:wrap;position:sticky;top:0;z-index:5;min-height:33px}
.brand{display:flex;align-items:center;gap:6px}
.brand b{font-size:13px;font-weight:700;letter-spacing:-.02em}
.tabs{display:flex;gap:2px;height:32px;overflow-x:auto;scrollbar-width:none}
.tabs::-webkit-scrollbar{display:none}
.tabs a{display:flex;align-items:center;gap:4px;padding:0 8px;color:var(--mute);font-weight:400;white-space:nowrap;font-family:var(--font-mono);font-size:11.5px;letter-spacing:.04em;text-transform:uppercase}
.tabs a:hover{color:var(--text);text-decoration:none}
.tabs a.on{color:var(--text);box-shadow:inset 0 -1px 0 var(--signal)}
.grow{flex:1}
.meta{display:flex;align-items:center;gap:8px}
.tag{border:1px solid var(--line);border-radius:2px;padding:0 6px;font-size:10px;line-height:18px;color:var(--mute);font-family:var(--font-mono);font-weight:400}
.tag.live{border-color:var(--signal);color:var(--signal);background:var(--signalbg)}
.switch{height:26px;max-width:180px;border:1px solid var(--line-strong);border-radius:2px;background:var(--surface);color:var(--text);font:inherit;padding:0 8px}
.page p{margin:0}
.page{max-width:1440px;margin:0 auto;padding:8px 12px 16px;display:flex;flex-direction:column;gap:8px}
.panel{background:var(--surface);border:1px solid var(--line);border-radius:4px}
.row{display:flex;align-items:center;gap:6px}
.wrap{flex-wrap:wrap}
.col{display:flex;flex-direction:column;gap:4px}
.between{justify-content:space-between}
.cols{display:flex;flex-wrap:wrap;gap:8px;align-items:flex-start}
.cols>.c2{flex:2 1 680px;min-width:0}
.cols>.c1{flex:1 1 320px;min-width:0}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:4px;height:26px;padding:0 8px;border:1px solid var(--line-strong);background:var(--surface);border-radius:2px;font:inherit;font-size:12.5px;font-weight:500;color:var(--text);cursor:pointer;text-decoration:none;white-space:nowrap}
.btn:hover{background:var(--surface-soft);text-decoration:none}
.btn:disabled{opacity:.55;cursor:default}
.btn.on{background:var(--accentbg);border-color:var(--accent);color:var(--accentink)}
.btn.pri{background:var(--text);border-color:var(--text);color:var(--bg)}
.btn.pri:hover{background:var(--accentink)}
.btn.sm{height:24px;padding:0 8px;font-size:12px}
.field{width:100%;font:inherit;color:var(--text);border:1px solid var(--line-strong);border-radius:2px;padding:4px 8px;background:var(--surface);resize:vertical;min-height:28px}
.field:focus,.canvas-find input:focus{outline:1px solid var(--accent);outline-offset:-1px;border-color:var(--accent);box-shadow:none}
select.field{height:24px;padding:0 4px;width:auto;resize:none;min-height:0;font-size:12px}
.stats{display:flex;flex-wrap:wrap}
.stat{flex:1 1 200px;display:flex;align-items:baseline;gap:6px;padding:6px 10px;border-right:1px solid var(--line2);min-width:0}
.stat:last-child{border-right:0}
.stat .v{font-size:16px;font-weight:600;font-variant-numeric:tabular-nums}
.stat .n{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tbl{overflow-x:auto}
.tbl>div{min-width:760px}
.tr{display:grid;gap:8px;align-items:center;padding:4px 10px;border-top:1px solid var(--line2)}
.tr.head{border-top:0;padding:4px 10px;color:var(--accentink);font-size:10.5px;font-weight:600;font-family:var(--font-mono);letter-spacing:.04em;text-transform:uppercase;background:var(--accentbg)}
.tr.alert{background:var(--badbg)}
.tile{position:relative;flex:none;border-radius:3px;background:var(--tile);color:var(--ink-soft);display:flex;align-items:center;justify-content:center}
.tile.bad{background:var(--badbg);color:var(--bad)}
.tile .badge{position:absolute;right:-4px;bottom:-4px;background:var(--surface);border:1px solid var(--line-strong);border-radius:50%;display:flex;align-items:center;justify-content:center;color:var(--ink-soft)}
.shape{display:inline-block;width:7px;height:7px;margin-right:6px;flex:none;border-radius:50%}
.shape.sq{border-radius:2px}.shape.di{border-radius:1px;transform:rotate(45deg)}.shape.ring{border:1.5px solid}
.bar{height:3px;border-radius:1px;background:var(--surface-soft);overflow:hidden;width:72px;display:inline-block;vertical-align:middle;margin-left:6px}
.bar>i{display:block;height:100%;background:var(--accent)}
.item{padding:6px 10px;border-top:1px solid var(--line2);display:flex;flex-direction:column;gap:4px}
.item:first-child{border-top:0}
.cmd{display:flex;align-items:center;gap:6px;background:var(--surface-soft);border:1px solid var(--line2);border-radius:2px;padding:3px 3px 3px 8px}
.cmd code{flex:1;min-width:0;overflow-wrap:anywhere;color:var(--ink-soft)}
.note{border:1px solid var(--warnline);background:var(--warnbg);border-radius:4px;padding:6px 8px;display:flex;flex-direction:column;gap:4px;color:var(--warn)}
.note.bad{border-color:var(--warnline);background:var(--badbg);color:var(--bad)}
.filters{display:flex;gap:2px;flex-wrap:wrap}
.filters a{display:inline-flex;align-items:center;gap:5px;height:26px;padding:0 8px;border-radius:2px;color:var(--mute);font-weight:500;border:1px solid transparent;font-size:12.5px}
.filters a:hover{text-decoration:none;background:var(--surface-soft)}
.filters a.on{background:var(--accentbg);border-color:var(--line-strong);color:var(--accentink)}
.msg{display:block;padding:6px 10px;border-top:1px solid var(--line2);color:inherit}
.msg:first-child{border-top:0}
.msg:hover{background:var(--surface-soft);text-decoration:none}
.msg.sel{background:var(--accentbg)}
.msg.unread{box-shadow:inset 2px 0 0 var(--accent)}
.unread-dot{width:5px;height:5px;border-radius:50%;background:var(--accent);display:inline-block;flex:none;margin-right:5px;vertical-align:middle}
.drow{display:grid;grid-template-columns:minmax(150px,1.4fr) minmax(110px,1fr) minmax(110px,1fr);gap:8px;padding:4px 0;border-top:1px solid var(--line2)}
.step{display:flex;gap:8px}
.step .rail{display:flex;flex-direction:column;align-items:center}
.step .rail i{flex:1;width:1.5px;background:var(--line);margin:3px 0}
.step .no{width:20px;height:20px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:600;border:1.5px solid var(--line-strong);color:var(--dim);background:var(--surface)}
.step.done .no{background:var(--good);border-color:var(--good);color:var(--bg)}
.step.cur .no{border-color:var(--accent);color:var(--accent)}
.empty{padding:10px;text-align:center;color:var(--dim)}
.kv{display:grid;grid-template-columns:92px 1fr;gap:3px 10px}
.kv>span:nth-child(odd){color:var(--mute)}
.break{overflow-wrap:anywhere}.pre{white-space:pre-wrap;overflow-wrap:anywhere}
.md{display:flex;flex-direction:column;gap:7px;max-width:720px;min-width:0;line-height:1.55;overflow-wrap:anywhere}
.md>*{margin:0}
.md-p{white-space:normal}
.md-h{font-weight:600;line-height:1.3}
.md-h1{font-size:17px}.md-h2{font-size:15px}.md-h3,.md-h4,.md-h5,.md-h6{font-size:13.5px}
.md-list{margin:0;padding-left:22px;display:flex;flex-direction:column;gap:3px}
.md-list .md-list{margin-top:3px;padding-left:20px}
.md-list li{padding-left:1px}
.md-li-body{min-width:0}
.md-quote{border-left:2px solid var(--line-strong);padding:2px 0 2px 10px;color:var(--mute)}
.md-quote .md{max-width:none}
.md-pre{background:var(--surface-soft);border:1px solid var(--line2);border-radius:3px;padding:7px 9px;overflow-x:auto;max-width:100%}
.md-pre code{display:block;white-space:pre;font-family:var(--font-mono);font-size:11.5px;line-height:1.5;color:var(--ink-soft)}
.md-code{font-family:var(--font-mono);font-size:.92em;background:var(--surface-soft);border:1px solid var(--line2);border-radius:2px;padding:0 3px;overflow-wrap:anywhere}
.md-hr{border:0;border-top:1px solid var(--line2);margin:2px 0}
.md-tablewrap{overflow-x:auto;max-width:100%}
.md-table{border-collapse:collapse;width:100%;font-size:12px;line-height:1.45;font-variant-numeric:tabular-nums}
.md-table th,.md-table td{border:1px solid var(--line2);padding:4px 7px;vertical-align:top;text-align:left;overflow-wrap:anywhere}
.md-table th{background:var(--surface-soft);font-weight:600;font-family:var(--font-mono);font-size:10.5px;letter-spacing:.03em;text-transform:uppercase;color:var(--mute)}
.md-link{color:var(--accentink);text-decoration:underline;text-underline-offset:2px}
.board{position:relative;overflow:auto;border:1px solid var(--line);border-radius:4px;background-color:var(--bg);background-image:radial-gradient(circle,var(--line-strong) .8px,transparent 1px);background-size:20px 20px;overscroll-behavior:contain;cursor:grab;touch-action:pan-x pan-y}
.board.panning{cursor:grabbing;user-select:none}.board:focus-visible{outline:1px solid var(--accent);outline-offset:-1px}
.canvas-plane{position:absolute;left:0;top:0;transform-origin:0 0}
.canvas-plane>svg{position:absolute;left:0;top:0;pointer-events:none}
.canvas-stage{position:relative;min-width:100%;min-height:100%}
.lane{position:absolute;font-size:10.5px;line-height:16px;font-weight:500;color:var(--dim);font-family:var(--font-mono);letter-spacing:.06em;text-transform:uppercase}
.node{position:absolute;display:flex;flex-direction:column;gap:4px;padding:8px 10px;background:var(--surface);border:1px solid var(--line);border-radius:4px;color:var(--text);box-shadow:0 1px 3px rgba(21,33,31,.04);cursor:pointer}
.node:hover{text-decoration:none;border-color:var(--accent);box-shadow:0 2px 5px rgba(21,33,31,.06)}
.node:focus-visible{outline:1px solid var(--accent);outline-offset:1px}
.node.quiet{opacity:.28}.node.match{border-color:var(--accent);background:var(--accentbg);box-shadow:none}
.node-head{display:flex;align-items:center;gap:6px;width:100%;min-width:0}
.node-progress{height:4px;background:var(--surface-soft);border-radius:1px;overflow:hidden;width:100%}
.node-progress>span{display:block;height:100%;background:var(--accent);border-radius:1px}
.node.sel{border-color:var(--accent);box-shadow:none}
.node.alert{border-color:var(--warnline)}
.node .nm{display:block;font-size:13px;line-height:18px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.node .ln{display:block;font-size:12px;line-height:16px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.node.flow{position:static;height:auto}
.node.flow .ln{white-space:normal}
.elabel{position:absolute;font-size:11.5px;line-height:14px;color:var(--mute);white-space:nowrap;background:var(--bg);padding:2px 4px;border-radius:2px;pointer-events:none}
.canvas-head{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;padding:3px 12px;background:var(--surface);border-bottom:1px solid var(--line);flex-shrink:0}
.canvas-head h1{font-size:14px;line-height:20px}.canvas-find{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.canvas-find input{appearance:none;width:210px;max-width:100%;height:26px;padding:0 8px;border:1px solid var(--line);border-radius:2px;font:inherit;font-size:12px;background:var(--surface)}
.canvas-count{font-size:10.5px;color:var(--mute);min-width:60px;font-family:var(--font-mono)}
.canvas-controls{position:absolute;left:50%;bottom:10px;transform:translateX(-50%);z-index:3;display:flex;align-items:center;gap:3px;padding:4px 5px;background:var(--surface);border:1px solid var(--line);border-radius:4px;box-shadow:0 2px 8px rgba(21,33,31,.06);white-space:nowrap}
.canvas-controls button{height:28px;min-width:28px;border:0;border-radius:2px;background:var(--surface);color:var(--text);font:inherit;font-size:12px;font-weight:500;cursor:pointer;padding:0 7px}
.canvas-controls button:hover{background:var(--accentbg)}.canvas-controls button:disabled{opacity:.35;cursor:default}
.canvas-controls output{font-size:12px;color:var(--mute);width:44px;text-align:center}
.canvas-controls .sep{height:20px;width:1px;background:var(--line);margin:0 4px}
.canvas-hint{position:absolute;top:12px;left:14px;font-size:11px;color:var(--dim);pointer-events:none}
.canvas-controls button[aria-pressed="true"]{background:var(--accentbg);color:var(--accentink)}
.legend{display:flex;gap:18px;flex-wrap:wrap;align-items:center;color:var(--mute);font-size:12px}
.page.fill{height:calc(100vh - 33px);overflow:hidden;padding-bottom:8px;max-width:none}
.page.fill.work{max-width:1440px}
.workfill{flex:1;min-height:0;align-items:stretch !important;flex-wrap:nowrap !important}
.workfill>.c2,.workfill>.c1{overflow:auto;max-height:100%;overscroll-behavior:contain;padding-right:2px}
.workfill>.c2{flex:2 1 0}.workfill>.c1{flex:1 1 320px}
.teamfill{flex:1;min-height:0;min-width:0;display:flex}
.canvas-section{display:flex;flex-direction:column;flex:1;min-width:0;min-height:0;overflow:hidden}
.boardwrap{position:relative;flex:1 1 auto;min-width:0;min-height:0;display:flex}
.boardwrap>.board{flex:1;min-width:0;min-height:0;border:0;border-radius:0}
.teamfill>aside{overflow:auto;min-height:0;max-height:100%;border:0;border-left:1px solid var(--line);border-radius:0;overscroll-behavior:contain}
.teamfill>aside>*{flex-shrink:0}
#app.team-shell{height:100vh;height:100dvh;display:flex;flex-direction:column;overflow:hidden}
.team-shell>.top{height:auto;min-height:33px;flex-shrink:0}
#app.team-shell>.page{flex:1;min-height:0;width:100%;height:auto;margin:0;padding:0;overflow:hidden}
@media (min-width:761px){#app.inbox-shell{height:100vh;height:100dvh;display:flex;flex-direction:column;overflow:hidden}.inbox-shell>.top{flex-shrink:0}.inbox-shell>.page.fill{height:auto;min-height:0;flex:1;width:100%;margin:0}}
.toolrail{position:absolute;right:10px;top:50%;transform:translateY(-50%);z-index:3;display:flex;flex-direction:column;align-items:center;gap:4px;padding:4px;background:var(--surface);border:1px solid var(--line);border-radius:4px;box-shadow:0 2px 6px rgba(21,33,31,.05)}
.rbtn{width:32px;height:32px;border-radius:2px;border:0;background:transparent;color:var(--ink-soft);display:flex;align-items:center;justify-content:center;cursor:pointer;padding:0}
.rbtn:hover{background:var(--surface-soft)}
.rbtn.filter[aria-pressed="false"]{color:var(--line-strong)}
.rbtn.filter[aria-pressed="true"]{color:var(--accentink)}
.inboxfill{flex:1;min-height:0;display:flex;gap:8px}
.inboxfill>.list{flex:0 1 420px !important;align-self:stretch;overflow:auto !important}
.inboxfill>.thread{flex:1 1 auto !important;align-self:stretch;display:flex;flex-direction:column;min-height:0;overflow:hidden}
.thread>.tbody{flex:1;min-height:0;overflow:auto}
.boardlegend{position:absolute;right:10px;top:10px;z-index:2;gap:6px;background:var(--surface);border:1px solid var(--line);border-radius:4px;padding:8px 10px;box-shadow:0 1px 4px rgba(21,33,31,.03);flex-direction:column;align-items:flex-start}
.legend span{display:inline-flex;align-items:center;gap:6px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:8px}
.seg{display:inline-flex;border:1px solid var(--line-strong);border-radius:2px;overflow:hidden;background:var(--surface)}
.seg button{border:0;background:var(--surface);height:26px;padding:0 10px;font:inherit;font-size:12.5px;font-weight:500;color:var(--mute);cursor:pointer}
.seg button.on{background:var(--accentbg);color:var(--accentink)}
.conn{display:flex;flex-direction:column;gap:1px;margin-top:6px;padding-top:6px;border-top:1px solid var(--line2);color:var(--mute);font-size:12px;line-height:16px}
.toast{position:fixed;right:16px;bottom:16px;z-index:9;background:var(--text);color:var(--bg);border-radius:4px;padding:8px 12px;font-size:12.5px;box-shadow:0 4px 16px rgba(0,0,0,.18);opacity:0;transform:translateY(6px);transition:opacity .15s,transform .15s;pointer-events:none;max-width:420px}
.toast.show{opacity:1;transform:none}
.toast.bad{background:var(--bad)}
@media (prefers-reduced-motion:reduce){.toast{transition:none}}
@media (max-width:1000px){.page.fill.work{height:auto;overflow:visible}.workfill{flex:none;flex-wrap:wrap !important;align-items:flex-start !important}.workfill>.c2,.workfill>.c1{flex:1 1 100% !important;overflow:visible;max-height:none}.cols>.thread{order:-1}.cols>.list{max-width:none !important}.inboxfill{flex-direction:column}.inboxfill>.thread{order:-1;flex:1 1 60% !important}.inboxfill>.list{flex:1 1 40% !important;max-width:none !important}}
@media (max-width:860px){
.tr{grid-template-columns:1fr 1fr !important;gap:4px 12px;padding:8px 12px}
.tr>:first-child,.tr>.wide{grid-column:1/-1}
.tr.head{display:none}
.tr>[data-label]::before{content:attr(data-label);display:block;color:var(--mute);font-size:11px;line-height:14px}
.tr>:empty{display:none}
.tbl>div{min-width:0}
}
@media (max-width:760px){
.top{height:auto;padding:4px 12px;gap:2px 10px}
.brand{order:1}.top .grow{display:none}.meta{order:2;margin-left:auto}
.tabs{order:3;flex:1 1 100%;height:40px;margin:0 -12px;padding:0 2px;border-top:1px solid var(--line2);justify-content:space-between}
.tabs a{padding:0 5px;gap:3px;font-size:10px;letter-spacing:.02em}
.page{padding:8px 10px 16px;gap:8px}
.stat{flex:1 1 45%;border-bottom:1px solid var(--line2);flex-wrap:wrap;gap:0 8px}.stat .n{white-space:normal;flex-basis:100%}
.btn{min-height:44px}.btn.sm{min-height:40px}
.rbtn{width:44px;height:44px}
.seg button,.switch,select.field{height:36px}
.cmd{flex-wrap:wrap}
.drow{grid-template-columns:minmax(0,1.2fr) minmax(0,1fr) minmax(0,1fr)}
.filters{flex-wrap:nowrap;overflow-x:auto;max-width:100%}
.filters a{min-height:36px;white-space:nowrap}
.grid{grid-template-columns:1fr}
.meta .small{display:none}
.page.fill{height:auto;overflow:visible}
.inboxfill>.list,.inboxfill>.thread{flex:none !important}
.thread>.tbody{overflow:visible}
.teamfill{flex-direction:column}
.canvas-section{flex:1 1 60%}
.teamfill>aside{flex:1 1 40% !important;max-height:40%;width:100%;border-left:0;border-top:1px solid var(--line)}
.boardlegend{display:none}
.canvas-head{gap:4px;padding:4px 10px}.canvas-find{width:100%}.canvas-find input{flex:1;min-width:0;width:160px;height:40px}
.canvas-controls{bottom:10px;padding:4px;gap:0}.canvas-controls button{min-width:38px;height:44px;padding:0 7px}
.canvas-controls output{width:38px}.canvas-controls .sep{margin:0 2px}.canvas-hint{display:none}
}
</style>
</head>
<body>
<div id="app"></div>
<noscript>The Hivewire dashboard needs JavaScript.</noscript>
<script nonce="__NONCE__">
(() => {
'use strict';
const app = document.getElementById('app');
const params = new URLSearchParams(location.search);
let token = params.get('t') || '';
try { if (token) sessionStorage.setItem('swarm-token', token); else token = sessionStorage.getItem('swarm-token') || ''; } catch (e) { /* storage may be blocked */ }
let boardId = params.get('b') || '';
try { if (boardId) sessionStorage.setItem('swarm-board', boardId); else boardId = sessionStorage.getItem('swarm-board') || ''; } catch (e) { /* storage may be blocked */ }
function rememberBoard(id) {
  boardId = id || '';
  try { if (boardId) sessionStorage.setItem('swarm-board', boardId); else sessionStorage.removeItem('swarm-board'); } catch (e) { /* storage may be blocked */ }
  history.replaceState(null, '', location.pathname + (boardId ? '?b=' + encodeURIComponent(boardId) : '') + location.hash);
}
if (params.has('t') || params.has('b')) rememberBoard(boardId);

// ---- a tiny DOM helper: board text only ever becomes a text node, never markup
function h(tag, props, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(props || {})) {
    if (v == null || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k === 'style') for (const [p, val] of Object.entries(v)) el.style.setProperty(p, val);
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v === true ? '' : v);
  }
  for (const kid of kids.flat(3)) {
    if (kid == null || kid === false) continue;
    el.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
  return el;
}
const link = (href, props, ...kids) => h('a', { href, ...props }, ...kids);
const taskLink = (id) => link('#/task/' + encodeURIComponent(id), { class: 'mono' }, id);
const SVG = 'http://www.w3.org/2000/svg';
function svgEl(tag, attrs, ...kids) {
  const el = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs || {})) el.setAttribute(k, v);
  for (const kid of kids.flat()) if (kid) el.append(kid);
  return el;
}

// ---- who is who: the board stores a name and a kind; roles are derived on the server
let DIR = { human: { display: 'You', kind: 'human', role: 'Owner' } };
const SYSTEM_KINDS = ['human', 'system', 'steward'];
const who = (n) => DIR[n] || { display: n, kind: '', role: '' };
const nameText = (n) => { const w = who(n); return w.kind && !SYSTEM_KINDS.includes(w.kind) ? w.display + ' (' + w.kind + ')' : w.display; };
const nameRole = (n) => { const w = who(n); return w.role && w.role !== 'Steward' ? nameText(n) + ' – ' + w.role : nameText(n); };

// ---- agent icons: original line drawings, one body per kind of CLI, plus a small role badge
const BODIES = {
  orb: [['rect', { x: 5, y: 8, width: 14, height: 11, rx: 3.5 }], ['path', { d: 'M12 8V5' }], ['circle', { cx: 12, cy: 4, r: 1.1 }], ['circle', { cx: 9.5, cy: 13.5, r: 1, fill: 'currentColor' }], ['circle', { cx: 14.5, cy: 13.5, r: 1, fill: 'currentColor' }]],
  visor: [['rect', { x: 4, y: 7, width: 16, height: 12, rx: 4 }], ['path', { d: 'M7.5 12h9', 'stroke-width': 2.6 }], ['path', { d: 'M2.5 11v4M21.5 11v4M9.5 16.2h5' }]],
  twin: [['circle', { cx: 12, cy: 13.5, r: 7 }], ['path', { d: 'M9 7L8 3.5M15 7l1-3.5' }], ['circle', { cx: 9.5, cy: 12.5, r: 1, fill: 'currentColor' }], ['circle', { cx: 14.5, cy: 12.5, r: 1, fill: 'currentColor' }], ['path', { d: 'M9.5 16q2.5 2 5 0' }]],
  prompt: [['rect', { x: 4, y: 6, width: 16, height: 13, rx: 2.5 }], ['path', { d: 'M8 10.5l3 2.5-3 2.5M13 16h3.5' }]],
  eye: [['rect', { x: 5, y: 7, width: 14, height: 12, rx: 5 }], ['circle', { cx: 12, cy: 13, r: 2.8 }], ['path', { d: 'M12 7V4M10 4h4' }]],
  hex: [['path', { d: 'M12 3.5l7.5 4.3v8.4L12 20.5l-7.5-4.3V7.8z' }], ['circle', { cx: 9.7, cy: 11.5, r: 1, fill: 'currentColor' }], ['circle', { cx: 14.3, cy: 11.5, r: 1, fill: 'currentColor' }], ['path', { d: 'M9.8 15h4.4' }]],
  wave: [['rect', { x: 4, y: 8, width: 16, height: 11, rx: 3 }], ['path', { d: 'M12 8V5M8 14q1.5-2 3 0t3 0 3 0' }], ['circle', { cx: 12, cy: 4, r: 1.1 }]],
  person: [['circle', { cx: 12, cy: 9, r: 3.4 }], ['path', { d: 'M5.5 19.5c.9-3.7 3.4-5.2 6.5-5.2s5.6 1.5 6.5 5.2' }]],
  merge: [['circle', { cx: 7, cy: 6, r: 2 }], ['circle', { cx: 7, cy: 18, r: 2 }], ['circle', { cx: 17, cy: 12, r: 2 }], ['path', { d: 'M7 8v8M7 12h8' }]],
  envelope: [['rect', { x: 3.5, y: 6, width: 17, height: 12, rx: 2 }], ['path', { d: 'M4 7l8 6 8-6' }], ['path', { d: 'M14.5 16.5l1.6 1.6 3.4-3.6', 'stroke-width': 1.8 }]],
};
const TOOL_ICONS = {
  accountability: [['path', { d: 'M12 4v15M7 14l5 5 5-5' }]],
  collaboration: [['path', { d: 'M4 12h16', 'stroke-dasharray': '0.5 3.6', 'stroke-width': 2.6 }]],
  reviews: [['circle', { cx: 10.5, cy: 10.5, r: 5.5 }], ['path', { d: 'M15 15l5 5' }]],
};
const KIND_BODY = { claude: 'orb', opencode: 'prompt', qodercli: 'visor', qoder: 'visor', codex: 'eye', gemini: 'twin', kilocode: 'hex', deepseek: 'wave', human: 'person', system: 'merge', steward: 'envelope' };
const BODY_KEYS = ['orb', 'visor', 'twin', 'prompt', 'eye', 'hex', 'wave'];
const BADGES = {
  Orchestrator: [['path', { d: 'M12 5v4M12 15v4M5 12h4M15 12h4' }], ['circle', { cx: 12, cy: 12, r: 2 }]],
  Implementer: [['path', { d: 'M9 8l-4 4 4 4M15 8l4 4-4 4' }]],
  'Independent reviewer': [['circle', { cx: 10.5, cy: 10.5, r: 4.5 }], ['path', { d: 'M14 14l5 5' }]],
};
function bodyFor(kind) {
  if (KIND_BODY[kind]) return KIND_BODY[kind];
  let n = 0; for (const ch of String(kind || '?')) n = (n * 31 + ch.charCodeAt(0)) >>> 0;
  return BODY_KEYS[n % BODY_KEYS.length];
}
function glyph(parts, px, width) {
  return svgEl('svg', { width: px, height: px, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', 'stroke-width': width || 1.6, 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-hidden': 'true' },
    parts.map(([tag, attrs]) => svgEl(tag, attrs)));
}
function tile(kind, role, size, bad) {
  const badge = BADGES[role];
  const t = h('div', { class: 'tile' + (bad ? ' bad' : ''), style: { width: size + 'px', height: size + 'px' }, 'aria-hidden': 'true' }, glyph(BODIES[bodyFor(kind)], Math.round(size * 0.68)));
  if (badge && size >= 24) { const b = Math.round(size * 0.48); t.append(h('span', { class: 'badge', style: { width: b + 'px', height: b + 'px' } }, glyph(badge, Math.round(b * 0.74), 2.2))); }
  return t;
}

// ---- formatting
function ago(s) {
  if (s == null) return '–';
  if (s < 5) return 'now';
  if (s < 60) return Math.round(s) + 's ago';
  if (s < 3600) return Math.round(s / 60) + 'm ago';
  if (s < 86400) return Math.round(s / 3600) + 'h ago';
  return Math.round(s / 86400) + 'd ago';
}
function mmss(s) { s = Math.max(0, Math.round(s)); return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0'); }
function clockTime(iso) { const d = new Date(iso); return isNaN(d) ? '--:--' : d.toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit' }); }

// ---- a small Markdown reader for message bodies: every span becomes a text node, never markup from the board
function mdInline(text, links) {
  const out = [];
  const re = /(`+)([^`\n]*?)\1|\*\*\*(?=\S)([^*\n]*?\S)\*\*\*|\*\*(?=\S)([^*\n]*?\S)\*\*|~~(?=\S)([^~\n]*?\S)~~|\*(?=\S)([^*\n]*?\S)\*|\[([^\]\n]+)\]\(([^)\s]+)\)/g;
  let last = 0, m;
  while ((m = re.exec(text))) {
    if (m.index > last) out.push(text.slice(last, m.index));
    if (m[1]) out.push(h('code', { class: 'md-code' }, m[2].trim() || ' '));
    else if (m[3]) out.push(h('strong', null, h('em', null, ...mdInline(m[3], links))));
    else if (m[4]) out.push(h('strong', null, ...mdInline(m[4], links)));
    else if (m[5]) out.push(h('del', null, ...mdInline(m[5], links)));
    else if (m[6]) out.push(h('em', null, ...mdInline(m[6], links)));
    else if (m[7] && links !== false) {
      out.push(/^(?:https?|mailto):/i.test(m[8])
        ? h('a', { class: 'md-link', href: m[8], target: '_blank', rel: 'noreferrer noopener' }, ...mdInline(m[7], false))
        : h('span', { class: 'md-link', title: m[8] }, ...mdInline(m[7], false)));
    } else if (m[7]) out.push(m[0]);
    last = re.lastIndex;
  }
  if (last < text.length) out.push(text.slice(last));
  return out;
}
function mdPlain(text) {
  return String(text == null ? '' : text)
    .replace(/^ {0,3}(?:#{1,6}|>|[-*+]|\d+[.)])\s+/, '')
    .replace(/`([^`]*)`/g, '$1')
    .replace(/\*\*\*([^*]+)\*\*\*/g, '$1')
    .replace(/\*\*([^*]+)\*\*/g, '$1')
    .replace(/~~([^~]+)~~/g, '$1')
    .replace(/\*([^*]+)\*/g, '$1')
    .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
    .replace(/\s+/g, ' ')
    .trim();
}
function mdIndent(line) { return /^ */.exec(line)[0].length; }
function mdHr(line) { return /^ {0,3}(?:(?:\* *){3,}|(?:- *){3,}|(?:_ *){3,})$/.test(line); }
function mdFence(line) { const m = /^ {0,3}(`{3,}|~{3,})(.*)$/.exec(line); return m ? { ch: m[1][0], len: m[1].length } : null; }
function mdFenceClose(line, f) { const m = /^ {0,3}(`{3,}|~{3,})\s*$/.exec(line); return !!m && m[1][0] === f.ch && m[1].length >= f.len; }
function mdCells(line) {
  let s = line.trim();
  if (s.startsWith('|')) s = s.slice(1);
  if (s.endsWith('|')) s = s.slice(0, -1);
  const cells = [];
  let cur = '', escaped = false;
  for (const ch of s) {
    if (escaped) { cur += ch; escaped = false; }
    else if (ch === '\\') escaped = true;
    else if (ch === '|') { cells.push(cur.trim()); cur = ''; }
    else cur += ch;
  }
  cells.push(cur.trim());
  return cells;
}
function mdTableAt(lines, i) {
  if (i + 1 >= lines.length || !lines[i].includes('|')) return false;
  const head = mdCells(lines[i]), sep = mdCells(lines[i + 1]);
  return head.length > 1 && head.length === sep.length && sep.every((c) => /^:?-+:?$/.test(c));
}
function mdList(lines) {
  const marks = lines.filter((l) => /^ *(?:[-*+]|\d+[.)]) +/.test(l));
  const base = Math.min(...marks.map(mdIndent));
  const rows = lines.map((l) => l.slice(Math.min(base, mdIndent(l))));
  let root = null;
  const stack = [];
  for (const raw of rows) {
    if (!raw.trim()) continue;
    const m = /^( *)([-*+]|\d+[.)]) +(.*)$/.exec(raw);
    if (m) {
      const indent = m[1].length, ordered = /^\d/.test(m[2]), num = parseInt(m[2], 10);
      while (stack.length > 1 && indent < stack[stack.length - 1].indent) stack.pop();
      let top = stack[stack.length - 1];
      if (!top) {
        root = h(ordered ? 'ol' : 'ul', { class: 'md-list' });
        if (ordered && num !== 1) root.setAttribute('start', String(num));
        stack.push({ indent, list: root, item: null });
        top = stack[stack.length - 1];
      } else if (indent > top.indent && top.item) {
        const list = h(ordered ? 'ol' : 'ul', { class: 'md-list' });
        if (ordered && num !== 1) list.setAttribute('start', String(num));
        top.item.li.append(list);
        stack.push({ indent, list, item: null });
        top = stack[stack.length - 1];
      }
      const li = h('li'), body = h('div', { class: 'md-li-body' }, ...mdInline(m[3]));
      li.append(body);
      top.list.append(li);
      top.item = { li, body };
    } else if (stack.length) {
      const indent = mdIndent(raw);
      while (stack.length > 1 && indent < stack[stack.length - 1].indent) stack.pop();
      const top = stack[stack.length - 1];
      if (top.item && raw.trim()) top.item.body.append(' ', ...mdInline(raw.trim()));
    }
  }
  return root || h('div');
}
function markdown(text) {
  const root = h('div', { class: 'md' });
  const lines = String(text == null ? '' : text).replace(/\r\n?/g, '\n').replace(/\t/g, '    ').split('\n');
  const blank = (s) => !s.trim();
  const start = (i) => {
    const l = lines[i];
    return !!mdFence(l) || /^ {0,3}#{1,6} +/.test(l) || mdHr(l) || /^ {0,3}>/.test(l) || /^ *(?:[-*+]|\d+[.)]) +/.test(l) || mdTableAt(lines, i);
  };
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (blank(line)) { i++; continue; }
    const fence = mdFence(line);
    if (fence) {
      const body = [];
      i++;
      while (i < lines.length && !mdFenceClose(lines[i], fence)) { body.push(lines[i]); i++; }
      if (i < lines.length) i++;
      root.append(h('div', { class: 'md-pre' }, h('code', null, body.join('\n'))));
      continue;
    }
    const head = /^ {0,3}(#{1,6}) +(.*?)\s*#*\s*$/.exec(line);
    if (head) { root.append(h('h' + head[1].length, { class: 'md-h md-h' + head[1].length }, ...mdInline(head[2]))); i++; continue; }
    if (mdHr(line)) { root.append(h('hr', { class: 'md-hr' })); i++; continue; }
    if (/^ {0,3}>/.test(line)) {
      const quote = [];
      while (i < lines.length) {
        if (/^ {0,3}>/.test(lines[i])) { quote.push(lines[i].replace(/^ {0,3}> ?/, '')); i++; }
        else if (blank(lines[i]) && /^ {0,3}>/.test(lines[i + 1] || '')) { quote.push(''); i++; }
        else break;
      }
      root.append(h('blockquote', { class: 'md-quote' }, markdown(quote.join('\n'))));
      continue;
    }
    if (mdTableAt(lines, i)) {
      const headCells = mdCells(lines[i]);
      const align = mdCells(lines[i + 1]).map((c) => c.startsWith(':') && c.endsWith(':') ? 'center' : c.endsWith(':') ? 'right' : c.startsWith(':') ? 'left' : '');
      const table = h('table', { class: 'md-table' });
      const header = h('tr');
      headCells.forEach((c, k) => header.append(h('th', align[k] ? { style: { 'text-align': align[k] } } : null, ...mdInline(c))));
      table.append(h('thead', null, header));
      const tbody = h('tbody');
      i += 2;
      while (i < lines.length && lines[i].includes('|') && !blank(lines[i])) {
        const cells = mdCells(lines[i]), row = h('tr');
        headCells.forEach((_, k) => row.append(h('td', align[k] ? { style: { 'text-align': align[k] } } : null, ...mdInline(cells[k] || ''))));
        tbody.append(row);
        i++;
      }
      table.append(tbody);
      root.append(h('div', { class: 'md-tablewrap' }, table));
      continue;
    }
    if (/^ *(?:[-*+]|\d+[.)]) +/.test(line)) {
      const base = mdIndent(line), ordered = /^ *\d/.test(line), block = [line];
      i++;
      while (i < lines.length) {
        const l = lines[i];
        if (/^ *(?:[-*+]|\d+[.)]) +/.test(l)) {
          if (mdIndent(l) <= base && /^ *\d/.test(l) !== ordered) break;
          block.push(l); i++; continue;
        }
        if (blank(l)) {
          let k = i + 1;
          while (k < lines.length && blank(lines[k])) k++;
          if (k < lines.length && (mdIndent(lines[k]) > base || /^ *(?:[-*+]|\d+[.)]) +/.test(lines[k]))) {
            if (/^ *(?:[-*+]|\d+[.)]) +/.test(lines[k]) && mdIndent(lines[k]) <= base && /^ *\d/.test(lines[k]) !== ordered) break;
            block.push(''); i++; continue;
          }
          break;
        }
        if (mdIndent(l) > base) { block.push(l); i++; continue; }
        break;
      }
      root.append(mdList(block));
      continue;
    }
    const para = [line.trim()];
    i++;
    while (i < lines.length && !blank(lines[i]) && !start(i)) { para.push(lines[i].trim()); i++; }
    const p = h('p', { class: 'md-p' });
    para.forEach((ln, k) => { if (k) p.append(h('br')); p.append(...mdInline(ln)); });
    root.append(p);
  }
  return root;
}
const COLOR = { good: 'var(--good)', warn: 'var(--warn)', bad: 'var(--bad)', info: 'var(--info)', mute: 'var(--mute)' };
const LABEL = { open: 'Queued', draft: 'Proposed', claimed: 'Working', planning: 'Working', awaiting_answers: 'Needs an answer', in_progress: 'Working',
  needs_rebase: 'Needs rebase', gates_failed: 'Gates failed', ready_to_merge: 'In merge queue', merge_blocked: 'Merge blocked', orphaned: 'Orphaned',
  expired: 'Contract expired', interrupted: 'Interrupted', done: 'Merged' };
const TASK_TONE = { claimed: 'good', planning: 'good', in_progress: 'good', awaiting_answers: 'warn', needs_rebase: 'warn', ready_to_merge: 'warn', interrupted: 'warn',
  gates_failed: 'bad', merge_blocked: 'bad', orphaned: 'bad', expired: 'bad', open: 'mute', draft: 'mute', done: 'mute' };
function shape(tone) {
  const kind = { warn: 'sq', bad: 'di', info: 'ring', mute: 'ring' }[tone] || '';
  const c = COLOR[tone] || COLOR.mute;
  return h('span', { class: 'shape ' + kind, 'aria-hidden': 'true', style: { background: kind === 'ring' ? 'transparent' : c, 'border-color': c } });
}
function status(s) {
  return h('span', { style: { color: COLOR[s.tone], 'font-weight': '500', display: 'inline-flex', 'align-items': 'center' } }, shape(s.tone), s.label);
}
let hivewireCmd = 'hivewire', canAct = false;
function cmd(rest, note) {
  const btn = h('button', { class: 'btn sm', type: 'button' }, 'Copy');
  btn.addEventListener('click', async () => {
    try { await navigator.clipboard.writeText(hivewireCmd + ' ' + rest); btn.textContent = 'Copied'; }
    catch (e) { btn.textContent = 'Select it'; }
    setTimeout(() => { btn.textContent = 'Copy'; }, 1600);
  });
  return h('div', { class: 'col', style: { gap: '2px' } }, h('div', { class: 'cmd' }, h('code', { class: 'mono' }, 'hivewire ' + rest), btn), note ? h('span', { class: 'small dim' }, note) : null);
}
const panel = (...kids) => h('div', { class: 'panel' }, ...kids);
const section = (title, right, body) => h('section', { class: 'col' }, h('div', { class: 'row between' }, h('h2', null, title), right || null), body);
const empty = (t) => h('div', { class: 'empty' }, t);
function personCell(name, extra) {
  const w = who(name);
  return h('span', { class: 'row', style: { gap: '8px', 'min-width': 0 } }, tile(w.kind, w.role, 26),
    h('span', { style: { 'min-width': 0, 'line-height': '16px' } }, h('b', { class: 'b' }, w.display), w.kind && !SYSTEM_KINDS.includes(w.kind) ? h('span', { class: 'muted' }, ' (' + w.kind + ')') : null, h('br'), h('span', { class: 'muted small' }, extra || w.role)));
}

// ---- acting as the human: drafts survive the live refresh, and the page does not redraw while you type
const drafts = {};
function field(key, placeholder, rows) {
  const ta = h('textarea', { class: 'field', rows: rows || 2, placeholder, 'data-draft': key, 'aria-label': placeholder });
  ta.value = drafts[key] || '';
  ta.addEventListener('input', () => { drafts[key] = ta.value; });
  return ta;
}
const toast = h('div', { class: 'toast', role: 'status', 'aria-live': 'polite' });
document.body.append(toast);
let toastTimer = null;
function flash(text, bad) {
  toast.textContent = text; toast.className = 'toast show' + (bad ? ' bad' : '');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => { toast.className = 'toast'; }, 3200);
}
async function post(body) {
  const r = await fetch('/api/act' + (boardId ? '?b=' + encodeURIComponent(boardId) : ''), { method: 'POST', headers: { 'X-Swarm-Token': token, 'Content-Type': 'application/json' }, body: JSON.stringify(body), cache: 'no-store' });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) return { ok: false, error: data.error || 'HTTP ' + r.status };
  return data;
}
async function act(body, btn, out, draftKey, done) {
  btn.disabled = true; out.className = 'small muted'; out.textContent = 'Working…';
  let r;
  try { r = await post(body); } catch (e) { r = { ok: false, error: 'Could not reach the dashboard.' }; }
  btn.disabled = false;
  if (r.ok) {
    if (draftKey) delete drafts[draftKey];
    out.textContent = '';
    flash(done || 'Done.');
    lastSig = ''; tick();
  } else { out.className = 'small err'; out.textContent = r.error || 'Refused.'; }
}
const readMarks = new Set();
function markRead(id) {
  if (!canAct || readMarks.has(id)) return;
  readMarks.add(id);
  post({ action: 'read', message: id }).then((r) => {
    if (r.ok) { lastSig = ''; tick(); }
  }).catch(() => readMarks.delete(id));
}
function button(label, primary, onClick) {
  const b = h('button', { class: 'btn sm' + (primary ? ' pri' : ''), type: 'button' }, label);
  b.addEventListener('click', () => onClick(b));
  return b;
}
function questionForm(n) {
  const key = 'q:' + n.id, ta = field(key, 'Your answer, or the reason to waive it', 2), out = h('span', { class: 'small' });
  return h('div', { class: 'col', style: { gap: '4px', 'margin-top': '2px' } }, ta, h('div', { class: 'row wrap', style: { gap: '6px' } },
    button('Answer', true, (b) => act({ action: 'answer', question: n.id, text: ta.value }, b, out, key, 'Answer recorded. The plan can continue.')),
    button('Waive', false, (b) => { if (!ta.value.trim()) { out.className = 'small err'; out.textContent = 'Write the reason first.'; return; } act({ action: 'waive', question: n.id, text: ta.value }, b, out, key, 'Question waived.'); }),
    out));
}
function approveForm(n) {
  const out = h('span', { class: 'small' });
  return h('div', { class: 'row wrap', style: { gap: '6px' } },
    button('Approve brief', true, (b) => { if (window.confirm('Approve the architecture brief for goal ' + n.goal + ' (revision ' + (n.revision || '?') + ')? Its tasks can then be opened and claimed.')) act({ action: 'approve', goal: n.goal }, b, out, null, 'Brief approved.'); }),
    link('#/decisions', { class: 'small' }, 'Read it first in Decisions'), out);
}

// ---- shell
const TABS = [['', 'Overview', null], ['attention', 'Attention', 'attention'], ['team', 'Team', null], ['inbox', 'Inbox', 'inbox'], ['decisions', 'Decisions', 'decisions'], ['tasks', 'Tasks', null]];
const BADGE_COLOR = { inbox: 'var(--bad)', decisions: 'var(--warn)', attention: 'var(--warn)' };
function boardSwitch(data) {
  const sel = h('select', { class: 'switch', 'aria-label': 'Board' }, data.boards.map((b) => h('option', { value: b.id, selected: b.id === (data.active && data.active.id) ? true : null }, b.name)));
  sel.addEventListener('change', () => { rememberBoard(sel.value); lastSig = ''; poolCache = null; tick(); });
  return sel;
}
const keptScroll = {};
function saveScroll() { app.querySelectorAll('[data-keep]').forEach((el) => { keptScroll[el.getAttribute('data-keep')] = [el.scrollTop, el.scrollLeft]; }); }
function restoreScroll(root) { root.querySelectorAll('[data-keep]').forEach((el) => { const s = keptScroll[el.getAttribute('data-keep')]; if (s) { el.scrollTop = s[0]; el.scrollLeft = s[1]; } }); }
function topBar(active, data) {
  const counts = data.counts || {}, pool = data.pool || {};
  const tabs = h('nav', { class: 'tabs', 'aria-label': 'Primary' }, TABS.map(([route, label, key]) => link('#/' + route, { class: route === active ? 'on' : '', 'aria-current': route === active ? 'page' : null },
    label, key && counts[key] ? h('span', { class: 'b small', style: { color: BADGE_COLOR[key] } }, counts[key]) : null)));
  return h('header', { class: 'top' }, h('div', { class: 'brand' }, h('b', null, 'Hivewire'), data.boards && data.boards.length > 1 ? boardSwitch(data) : h('span', { class: 'muted' }, pool.project || ''), pool.target ? h('span', { class: 'mono dim' }, pool.target) : null),
      tabs, h('span', { class: 'grow' }), h('span', { class: 'meta' }, h('span', { class: 'small dim' }, data.now ? 'Updated ' + new Date(data.now).toLocaleTimeString([], { hour12: false }) : ''),
        canAct ? h('span', { class: 'tag live', title: 'Actions on this page run hivewire commands as you' }, 'Acting as you') : h('span', { class: 'tag' }, 'Read-only')));
}
function shell(active, data, content) {
  saveScroll();
  app.classList.toggle('team-shell', active === 'team');
  app.classList.toggle('inbox-shell', active === 'inbox');
  if (active !== 'team' && boardResize) boardResize.disconnect();
  app.replaceChildren(
    topBar(active, data),
    h('main', { class: 'page' + (() => { const has = (n) => [].concat(content).some((c) => c && c.classList && c.classList.contains(n)); return has('teamfill') || has('inboxfill') ? ' fill' : has('workfill') ? ' fill work' : ''; })() }, content));
  restoreScroll(app);
}

// ---- overview
function needItem(n) {
  let kind = 'Question', color = 'var(--warn)', text = n.text, meta = n.from ? nameRole(n.from) + (n.task ? ' · ' : '') : '', command, note, form = null;
  if (n.type === 'question') {
    if (n.critical) { kind = 'Critical question'; color = 'var(--bad)'; }
    command = 'answer ' + n.id + ' "your answer"'; note = 'Answering unblocks the plan.';
    if (canAct) form = questionForm(n);
  } else if (n.type === 'approval') {
    kind = 'Approval'; color = 'var(--accentink)'; text = 'Architecture brief for “' + n.title + '”, revision ' + (n.revision || '?') + ', risk ' + (n.risk || '?'); meta = 'Goal ' + n.goal;
    command = 'goal architecture ' + n.goal + ' --approve';
    if (canAct) form = approveForm(n);
  } else { kind = 'Dead letter'; color = 'var(--bad)'; meta = nameRole(n.from) + ' to ' + nameText(n.to); command = 'messages resolve ' + n.id + ' --reason "reviewed outcome"'; note = 'A critical message expired unacknowledged.'; }
  return h('div', { class: 'item' },
    h('div', { class: 'row between' }, h('span', { class: 'b small', style: { color } }, kind + (n.type === 'question' ? ' · Q' + n.id : '')), h('span', { class: 'dim small' }, n.age_s != null ? ago(n.age_s) : '')),
    h('div', { class: 'm break' }, text), h('div', { class: 'muted small' }, meta, n.task ? taskLink(n.task) : null), form || cmd(command, note));
}
function taskTone(t, agentsByName) {
  const owner = agentsByName[t.assignee];
  if (owner && owner.state === 'offline' && t.state !== 'done') return ['Blocked, owner silent', 'bad'];
  return [LABEL[t.state] || t.state, TASK_TONE[t.state] || 'mute'];
}
const TEAM_COLS = 'minmax(190px,2fr) minmax(120px,1.1fr) minmax(170px,2fr) minmax(130px,1.1fr) minmax(70px,.6fr) minmax(64px,.6fr)';
function teamTable(d) {
  const grid = { 'grid-template-columns': TEAM_COLS };
  const rows = d.agents.map((a) => {
    const t = a.task, k = t && t.clock, off = a.state === 'offline';
    const pct = t && t.step[1] ? Math.round(100 * t.step[0] / t.step[1]) : t && t.state === 'ready_to_merge' ? 100 : 0;
    return h('div', { class: 'tr' + (off ? ' alert' : ''), style: grid },
      personCell(a.name), h('span', { 'data-label': 'Status' }, status(a.status)),
      t ? h('span', { class: 'wide break', 'data-label': 'Work' }, taskLink(t.id), ' ', h('span', { class: 'muted' }, t.title)) : h('span', { class: 'muted' }, a.coordinator ? d.totals.tasks + ' tasks on this board' : a.reviewing ? 'Incident I' + a.reviewing : 'No task'),
      t ? h('span', { class: 'wide', 'data-label': 'Progress' }, h('span', { class: 'muted small' }, t.step[1] ? 'Step ' + Math.min(t.step[0] + 1, t.step[1]) + '/' + t.step[1] : (LABEL[t.state] || t.state)), h('span', { class: 'bar' }, h('i', { style: { width: pct + '%' } }))) : h('span', { class: 'dim' }, '–'),
      h('span', { class: 'mono', 'data-label': 'Contract' }, k && k.active_min && (k.running || k.elapsed_s >= 60) ? Math.floor(k.elapsed_s / 60) + '/' + k.active_min + 'm' : ''),
      h('span', { class: 'muted small', 'data-label': 'Last seen' }, ago(a.seen_s)));
  });
  return h('div', { class: 'panel tbl' }, h('div', null,
    h('div', { class: 'tr head', style: grid }, h('span', null, 'Agent'), h('span', null, 'Status'), h('span', null, 'Work'), h('span', null, 'Progress'), h('span', null, 'Contract'), h('span', null, 'Seen')),
    rows.length ? rows : empty('No agents have joined. Start one with `hivewire join`.')));
}
const WORK_COLS = '92px minmax(170px,2fr) minmax(150px,1.4fr) minmax(130px,1fr) minmax(90px,.8fr)';
function workTable(d, limit) {
  const byName = Object.fromEntries(d.agents.map((a) => [a.name, a]));
  const grid = { 'grid-template-columns': WORK_COLS };
  const tasks = limit ? d.tasks.slice(0, limit) : d.tasks;
  return h('div', { class: 'panel tbl' }, h('div', null,
    h('div', { class: 'tr head', style: grid }, h('span', null, 'Task'), h('span', null, 'Title'), h('span', null, 'Owner'), h('span', null, 'State'), h('span', null, 'Needs')),
    tasks.length ? tasks.map((t) => { const [label, tone] = taskTone(t, byName); return h('div', { class: 'tr', style: grid },
      taskLink(t.id), h('span', { class: 'wide', 'data-label': 'Title' }, t.title), h('span', { 'data-label': 'Owner' }, t.assignee ? nameText(t.assignee) : h('span', { class: 'dim' }, 'Unassigned')),
      h('span', { class: 'm', 'data-label': 'State', style: { color: COLOR[tone] } }, label), h('span', { class: 'mono muted', 'data-label': 'Needs' }, t.needs.join(', '))); }) : empty('No tasks yet.')));
}
function activityList(events, limit) {
  const rows = [];
  for (const e of events.slice().reverse()) {
    const last = rows[rows.length - 1];
    if (last && last.text === e.text) last.n += 1; else rows.push({ at: e.at, text: e.text, n: 1 });
  }
  return panel(rows.length ? h('div', { style: { padding: '4px 0' } }, rows.slice(0, limit).map((e) => h('div', { class: 'row', style: { padding: '3px 12px', 'align-items': 'flex-start', gap: '10px' } },
    h('span', { class: 'mono dim', style: { flex: '0 0 38px' } }, clockTime(e.at)), h('span', { class: 'break' }, e.text, e.n > 1 ? h('span', { class: 'dim' }, ' \u00d7' + e.n) : null)))) : empty('No events yet.'));
}
const SEV_TONE = { P0: 'bad', P1: 'warn', P2: 'info', P3: 'mute' };
function attentionItem(a, withFinding) {
  const tone = SEV_TONE[a.severity] || 'mute';
  return h('div', { class: 'item' },
    h('div', { class: 'row between' }, h('span', { class: 'b small', style: { color: COLOR[tone] } },
      a.severity + ' · ' + a.kind + ' · ' + a.subject), h('span', { class: 'dim small' }, ago(a.age_s) + (a.overdue ? ' · overdue' : ''))),
    h('div', { class: 'm break' }, a.summary),
    h('div', { class: 'muted small' }, a.status + (a.owner ? ' · owner ' + nameText(a.owner) : ' · unclaimed') + (a.evidence ? ' · evidence ' + a.evidence : '')),
    withFinding && a.finding ? h('div', { class: 'small break', style: { 'margin-top': '3px' } }, h('b', null, 'Finding: '), a.finding) : null,
    withFinding && a.recommendation ? h('div', { class: 'small break' }, h('b', null, 'Recommended: '), a.recommendation,
      a.decision_owner ? h('span', { class: 'muted' }, ' · decides: ' + nameText(a.decision_owner)) : null) : null);
}
function deputyNote(d) {
  const dep = d.deputy || {}, lease = dep.active, a = d.attention || { queue: {} };
  const state = lease
    ? h('span', { class: 'small' }, h('b', null, 'Deputy ' + nameText(lease.deputy)), ' · lease ' + lease.id + ' until ' + clockTime(lease.expires_at) +
      ' · scope ' + (lease.scope.length ? lease.scope.join(', ') : 'none') + (lease.live ? '' : ' · agent not live'),
      h('span', { class: 'muted' }, ' · triage only'))
    : h('span', { class: 'small muted' }, 'No deputy lease; the deterministic scout keeps watching.');
  const counts = 'Attention: ' + (d.attention ? d.attention.open : 0) + ' open · P0 ' + (a.queue.P0 || 0) + ' · P1 ' + (a.queue.P1 || 0);
  return h('div', { class: 'note row between wrap' }, state, h('span', { class: 'small dim' }, counts));
}
const ATT_COLS = '58px minmax(200px,2.2fr) 112px minmax(110px,1fr) minmax(150px,1.4fr) 86px';
function attentionTable(d) {
  const a = d.attention || { items: [] }, grid = { 'grid-template-columns': ATT_COLS };
  return h('div', { class: 'panel tbl' }, h('div', null,
    h('div', { class: 'tr head', style: grid }, h('span', null, 'Sev'), h('span', null, 'Signal'), h('span', null, 'State'), h('span', null, 'Owner'), h('span', null, 'Evidence'), h('span', null, 'Age')),
    a.items.length ? a.items.map((i) => h('div', { class: 'tr' + (i.overdue ? ' alert' : ''), style: grid },
      h('span', { class: 'b', style: { color: COLOR[SEV_TONE[i.severity] || 'mute'] } }, i.severity),
      h('span', { class: 'wide break', 'data-label': 'Signal' }, h('b', null, i.kind + ' ' + i.subject), h('br'), h('span', { class: 'muted' }, i.summary),
        i.finding ? h('div', { class: 'small break', style: { 'margin-top': '2px' } }, h('b', { class: 'b' }, 'Finding: '), i.finding) : null,
        i.recommendation ? h('div', { class: 'small break' }, h('b', { class: 'b' }, 'Recommended: '), i.recommendation) : null),
      h('span', { class: 'm small', 'data-label': 'State' }, i.status),
      h('span', { class: 'small', 'data-label': 'Owner' }, i.owner ? nameText(i.owner) : h('span', { class: 'dim' }, 'Unclaimed')),
      h('span', { class: 'mono small break', 'data-label': 'Evidence' }, i.evidence || '—'),
      h('span', { class: 'small muted', 'data-label': 'Age' }, ago(i.age_s) + (i.overdue ? ' · due' : '')))) : empty('No open attention items. The scout found nothing that needs a person.')));
}
function historyTable(d) {
  const rows = ((d.attention || {}).history) || [];
  return panel(rows.length ? rows.map((r) => h('div', { class: 'item' }, h('div', { class: 'row between' },
    h('span', { class: 'b small' }, 'A' + r.item + ' · ' + (r.actor || 'system')), h('span', { class: 'dim small' }, clockTime(r.at))),
    h('div', { class: 'small' }, (r.from || '—') + ' → ' + (r.to || '—'), r.note ? h('span', { class: 'muted break' }, ' · ' + r.note) : null))) : empty('No attention history yet.'));
}
function leaseHistory(d) {
  const dep = d.deputy || {}, active = dep.active, recent = dep.recent || [];
  const body = [];
  if (active) body.push(h('div', { class: 'item' }, h('div', { class: 'row between' }, h('span', { class: 'b small' }, 'Active: ' + nameText(active.deputy)), h('span', { class: 'dim small' }, 'expires ' + clockTime(active.expires_at))),
    h('div', { class: 'small muted break' }, 'scope ' + active.scope.join(', ') + ' · caps ' + active.capabilities.join(', ')),
    h('div', { class: 'small muted' }, 'proposed actions only; the coordinator or human decides. ' + (active.live ? '' : 'Agent is not live.'))));
  for (const r of recent) body.push(h('div', { class: 'item' }, h('div', { class: 'row between' }, h('span', { class: 'b small' }, 'Lease ' + r.id + ' · ' + nameText(r.deputy)), h('span', { class: 'dim small' }, r.state + ' · ' + clockTime(r.at))),
    h('div', { class: 'small muted break' }, r.reason || 'no reason recorded')));
  return panel(body.length ? body : empty('No deputy leases yet.'));
}
const VIEWS = {};
VIEWS[''] = {
  load: () => get('/api/overview'),
  render(d) {
    const byName = Object.fromEntries(d.agents.map((a) => [a.name, a]));
    const blocked = d.tasks.filter((t) => taskTone(t, byName)[1] === 'bad');
    const silent = d.agents.filter((a) => a.state === 'offline');
    const critical = d.needs.filter((n) => n.critical || n.type === 'dead_letter').length;
    const attention = d.attention || { open: 0, queue: {}, items: [] };
    const urgent = (attention.queue.P0 || 0) + (attention.queue.P1 || 0);
    const goal = d.goals[0];
    const stat = (label, value, note, color) => h('div', { class: 'stat' }, h('span', { class: 'muted' }, label), h('span', { class: 'v', style: { color: color || 'var(--text)' } }, value), h('span', { class: 'n small muted' }, note));
    const open = d.tasks.filter((t) => t.state !== 'done').length;
    return [
      h('div', { class: 'row between wrap' },
        h('div', { class: 'row wrap', style: { gap: '10px', 'align-items': 'baseline' } }, h('h1', null, goal ? goal.title : 'No active goal'),
          h('span', { class: 'muted small' }, goal ? 'Goal ' + goal.id + (goal.architecture ? (goal.architecture.approved ? ' · architecture approved' : ' · architecture awaiting approval') + (goal.architecture.risk ? ' · risk ' + goal.architecture.risk : '') : '') : d.pool.project)),
        link('#/team', { class: 'btn' }, 'Team canvas')),
      d.pool.paused ? h('div', { class: 'note' }, 'Paused by ' + d.pool.paused + ': no claims or merges until `resume`.') : null,
      deputyNote(d),
      goal ? null : panel(h('div', { class: 'item' }, h('span', { class: 'm' }, 'No goal yet. Start one from a terminal:'), cmd('goal new ID "what you want done"'))),
      h('section', { class: 'panel stats', 'aria-label': 'Summary' },
        stat('Agents', d.pool.live + '/' + d.pool.max_agents, (silent.length ? silent.length + ' silent or offline' : 'all responding') + (d.pool.retired ? ' · ' + d.pool.retired + ' earlier hidden' : ''), silent.length ? 'var(--bad)' : ''),
        stat('Merged', d.totals.done + '/' + d.totals.tasks, d.totals.working + ' in progress · ' + d.totals.merge + ' queued'),
        stat('Waiting on you', String(d.needs.length), critical ? critical + ' critical' : 'nothing urgent', d.needs.length ? 'var(--warn)' : ''),
        stat('Attention', String(attention.open), urgent ? urgent + ' P0/P1 · ' + (attention.overdue || 0) + ' overdue' : 'nothing urgent', urgent ? 'var(--warn)' : ''),
        stat('Blocked', String(blocked.length), blocked.length ? blocked.map((t) => t.id).join(', ') : 'nothing blocked', blocked.length ? 'var(--bad)' : '')),
      h('div', { class: 'cols workfill' },
        h('div', { class: 'c2 col', 'data-keep': 'ov-c2', style: { gap: '8px' } },
          section('Team', link('#/team', { class: 'small' }, 'Canvas'), teamTable(d)),
          section('Work', link('#/tasks', { class: 'small' }, 'All tasks (' + open + ' open)'), workTable(d, 8))),
        h('div', { class: 'c1 col', 'data-keep': 'ov-c1', style: { gap: '8px' } },
          section('Waiting on you', d.needs.length ? h('span', { class: 'b small', style: { color: 'var(--warn)' } }, d.needs.length) : null, panel(d.needs.length ? d.needs.map(needItem) : empty('Nothing is waiting for you.'))),
          section('Attention', link('#/attention', { class: 'small' }, attention.open + ' open'), panel(attention.items.length ? attention.items.slice(0, 5).map((i) => attentionItem(i, false)) : empty('The scout has no open signals.'))),
          section('Activity', null, activityList(d.events, 12))))];
  },
};
VIEWS.tasks = {
  load: () => get('/api/overview'),
  render(d) { return [h('h1', null, 'Tasks'), workTable(d, 0)]; },
};
VIEWS.attention = {
  load: () => get('/api/overview'),
  render(d) {
    const a = d.attention || { open: 0, queue: {}, items: [], history: [] };
    const box = (label, value, note, color) => h('div', { class: 'stat' }, h('span', { class: 'muted' }, label), h('span', { class: 'v', style: { color: color || 'var(--text)' } }, value), h('span', { class: 'n small muted' }, note));
    return [
      h('div', { class: 'row between wrap' }, h('h1', null, 'Attention'), deputyNote(d)),
      h('p', { class: 'muted small', style: { 'margin-top': '0' } }, 'The deterministic scout queues signals. A deputy investigates only inside its scoped lease; findings are evidence-backed recommendations, never sanctions or merge authority.'),
      h('section', { class: 'panel stats', 'aria-label': 'Attention summary' },
        box('Open', String(a.open), (a.oldest_s != null ? 'oldest ' + ago(a.oldest_s) : 'nothing queued')),
        box('P0', String(a.queue.P0 || 0), 'needs a decision now', a.queue.P0 ? 'var(--bad)' : ''),
        box('P1', String(a.queue.P1 || 0), 'urgent but bounded', a.queue.P1 ? 'var(--warn)' : ''),
        box('Overdue', String(a.overdue || 0), 'past the response SLA', a.overdue ? 'var(--bad)' : '')),
      attentionTable(d),
      h('div', { class: 'cols' },
        h('div', { class: 'c1 col' }, section('Leases', null, leaseHistory(d))),
        h('div', { class: 'c1 col' }, section('Action history', null, historyTable(d))))];
  },
};

// ---- team canvas: lanes without backgrounds, lines that are facts
const filterState = { accountability: true, collaboration: true, reviews: true };
const canvasStates = new Map();
function canvasState(d) {
  const key = d.active.id;
  if (!canvasStates.has(key)) canvasStates.set(key, { zoom: 1, x: 0, y: 0, fit: window.innerWidth > 760, query: '', focus: false });
  return canvasStates.get(key);
}
const LANE_TEXT = [['Governance', 'Decides and approves'], ['Orchestration', 'Plans, assigns, keeps inboxes moving'], ['Execution', 'One task each, in its own worktree'], ['Assurance', 'Independent review'], ['Integration', 'Rebase, run gates, merge']];
function layout(nodes, edges, avail) {
  const exec = nodes.filter((n) => n.lane === 2), rest = (l) => nodes.filter((n) => n.lane === l);
  const lanes = [...new Set(nodes.map((n) => n.lane))].sort((a, b) => a - b);
  const laneRow = (l) => lanes.indexOf(l);
  const cols = Math.max(exec.length, 2);
  const narrow = avail < 900, labelW = narrow ? 96 : 132;
  const cardH = 104, gap = 56, step = cardH + gap;
  const G = narrow ? 28 : 44;
  const W = Math.max(narrow ? 208 : 224, Math.min(260, Math.floor((avail - (labelW + 24) - (cols - 1) * G) / cols)));
  const content = cols * W + (cols - 1) * G;
  const totalW = Math.max(avail, labelW + content + 24);
  const x0 = Math.round(labelW + Math.max(0, (totalW - labelW - 24 - content) / 2));
  const colX = (i) => x0 + i * (W + G);
  const execW = Math.max(exec.length, 1) * W + (Math.max(exec.length, 1) - 1) * G;
  const cx = x0 + (exec.length >= 2 ? execW : content) / 2;
  const TOP = 56;
  const top = (l) => TOP + laneRow(l) * step;
  const box = {};
  for (const n of nodes) {
    if (n.lane === 0) box[n.id] = { x: cx - W / 2, y: top(0), w: W, h: cardH };
    else if (n.lane === 1) box[n.id] = n.type === 'steward' ? { x: cx + W / 2 + G, y: top(1), w: W, h: cardH } : { x: cx - W / 2, y: top(1), w: W, h: cardH };
    else if (n.lane === 4) box[n.id] = { x: x0, y: top(4), w: Math.max(execW, W), h: cardH };
  }
  exec.forEach((n, i) => { box[n.id] = { x: colX(i), y: top(2), w: W, h: cardH }; });
  for (const n of rest(3)) {
    const e = edges.find((q) => q.kind === 'review' && q.from === n.id);
    const idx = e ? exec.findIndex((q) => q.id === e.to) : -1;
    box[n.id] = { x: colX(idx >= 0 ? idx : cols - 1), y: top(3), w: W, h: cardH };
  }
  const right = Math.max(...Object.values(box).map((b) => b.x + b.w)) + 24;
  const mid = (b) => ({ x: b.x + b.w / 2, y: b.y + b.h / 2 });
  const lines = [], labels = [], fanLabelled = new Set(); // a fan-out (assignments, reminders) is labelled once, not once per connector
  const reviewers = new Set(rest(3).map((n) => box[n.id].x));
  for (const e of edges) {
    const a = box[e.from], b = box[e.to];
    if (!a || !b) continue;
    const key = JSON.stringify([e.kind, e.from, e.to]);
    const g = e.kind === 'review' ? 'reviews' : ['collab', 'steward'].includes(e.kind) ? 'collaboration' : 'accountability';
    const style = g === 'reviews' ? 'review' : g === 'collaboration' ? 'dots' : 'solid';
    let path, labelX, labelY;
    if (e.kind === 'collab' && a.y === b.y) {
      const [l, r] = a.x < b.x ? [a, b] : [b, a], y = mid(l).y;
      if (r.x - l.x < W + G + 1) {
        const sx = l.x + l.w, tx = r.x, bend = (tx - sx) / 2;
        path = `M ${sx} ${y} C ${sx + bend} ${y - 10}, ${tx - bend} ${y - 10}, ${tx} ${y}`;
        labelX = (sx + tx) / 2; labelY = y - 25;
      } else {
        const sx = l.x + l.w * .7, tx = r.x + r.w * .3, bottom = l.y + l.h;
        path = roundedConnector([[sx, bottom], [sx, bottom + 24], [tx, bottom + 24], [tx, bottom]]);
        labelX = (sx + tx) / 2; labelY = bottom + 27;
      }
    } else if (e.kind === 'submits' && reviewers.has(a.x)) {
      const sx = mid(a).x, sy = a.y + a.h, corridor = a.x + a.w + G / 2;
      const target = Math.max(b.x + 16, Math.min(corridor, b.x + b.w - 16));
      path = roundedConnector([[sx, sy], [sx, sy + 22], [corridor, sy + 22], [corridor, b.y - 24], [target, b.y - 24], [target, b.y - 2]]);
      labelX = corridor + 8; labelY = b.y - 28;
    } else if (e.kind === 'review' && Math.abs(a.x - b.x) > 1) {
      const sx = mid(a).x, sy = a.y, corridor = right - 8;
      path = roundedConnector([[sx, sy], [sx, sy - 22], [corridor, sy - 22], [corridor, mid(b).y], [b.x + b.w + 2, mid(b).y]]);
      labelX = corridor - 8; labelY = sy - 40;
    } else {
      const down = b.y > a.y;
      const fraction = e.kind === 'steward' ? .8 : .5;
      const sx = a.x + a.w * fraction, sy = down ? a.y + a.h : a.y;
      const tx = e.kind === 'submits' ? Math.max(b.x + 16, Math.min(mid(a).x, b.x + b.w - 16)) : b.x + b.w * fraction;
      const ty = down ? b.y - 2 : b.y + b.h + 2;
      const bend = Math.max(24, Math.abs(ty - sy) * .5) * (down ? 1 : -1);
      path = `M ${sx} ${sy} C ${sx} ${sy + bend}, ${tx} ${ty - bend}, ${tx} ${ty}`;
      labelX = (sx + tx) / 2 + 8; labelY = (sy + ty) / 2 - 9;
    }
    lines.push({ key, g, style, path, arrow: e.kind !== 'collab' });
    if (e.kind !== 'assigns' && e.kind !== 'steward') labels.push({ key, g, text: e.label, x: labelX, y: labelY, color: g === 'reviews' ? 'var(--accentink)' : '' });
    else if (!fanLabelled.has(e.kind)) { fanLabelled.add(e.kind); labels.push({ key, g, text: e.label, x: labelX, y: labelY, color: '' }); }
  }
  const height = TOP + lanes.length * step - gap + 44;
  return { box, lines, labels, lanes, totalW: Math.max(totalW, right), height, top, narrow, step, TOP, cx };
}
function nodeCard(n, b, selected) {
  const alert = n.status.tone === 'bad';
  const suffix = n.kind && !SYSTEM_KINDS.includes(n.kind) ? ' (' + n.kind + ')' : n.type === 'gate' ? ' (system)' : '';
  const t = n.detail.task, steps = t && t.step[1], done = steps ? Math.min(t.step[0], steps) : 0;
  return link('#/team/' + encodeURIComponent(n.id), { class: 'node' + (selected ? ' sel' : '') + (alert ? ' alert' : ''), 'data-node': n.id, 'aria-current': selected ? 'true' : null, style: { left: b.x + 'px', top: b.y + 'px', width: b.w + 'px', height: b.h + 'px' }, 'aria-label': (n.type === 'agent' ? nameRole(n.id) : n.name + ', ' + n.role) + ', ' + n.status.label, title: n.type === 'agent' ? nameRole(n.id) : n.name + ' – ' + n.role },
    h('span', { class: 'node-head' }, tile(n.kind, n.role, 30, alert),
    h('span', { style: { display: 'block', 'min-width': 0, flex: 1 } },
      h('span', { class: 'nm' }, n.name, suffix ? h('span', { class: 'muted', style: { 'font-weight': '400' } }, suffix) : null),
      h('span', { class: 'ln muted' }, n.role))),
    h('span', { class: 'ln' }, status(n.status)),
    steps ? h('span', { class: 'node-progress', role: 'progressbar', 'aria-label': 'Completed task steps', 'aria-valuemin': 0, 'aria-valuemax': steps, 'aria-valuenow': done }, h('span', { style: { width: (done / steps * 100) + '%' } })) : null,
    h('span', { class: 'ln muted', style: { width: '100%' }, title: n.line }, n.line));
}
function roundedConnector(points, radius = 14) {
  let path = 'M ' + points[0].join(' ');
  for (let i = 1; i < points.length - 1; i++) {
    const a = points[i - 1], b = points[i], c = points[i + 1];
    const before = Math.hypot(b[0] - a[0], b[1] - a[1]), after = Math.hypot(c[0] - b[0], c[1] - b[1]);
    if (!before || !after) continue;
    const r = Math.min(radius, before / 2, after / 2);
    const p = b.map((v, j) => v + (a[j] - v) * r / before), q = b.map((v, j) => v + (c[j] - v) * r / after);
    path += ` L ${p.join(' ')} Q ${b.join(' ')} ${q.join(' ')}`;
  }
  return path + ' L ' + points[points.length - 1].join(' ');
}
function patchCanvasNode(old, next) {
  if (old.nodeType !== next.nodeType || old.nodeName !== next.nodeName) { old.replaceWith(next); return next; }
  if (old.nodeType === Node.TEXT_NODE) { if (old.data !== next.data) old.data = next.data; return old; }
  for (const a of [...old.attributes]) if (!next.hasAttribute(a.name)) old.removeAttribute(a.name);
  for (const a of [...next.attributes]) {
    if (old.getAttribute(a.name) === a.value) continue;
    // Trusted CSS is assigned through CSSOM, keeping the nonce-only CSP intact.
    if (a.name === 'style') old.style.cssText = next.style.cssText;
    else old.setAttribute(a.name, a.value);
  }
  const oldKids = [...old.childNodes], nextKids = [...next.childNodes];
  nextKids.forEach((child, i) => { if (oldKids[i]) patchCanvasNode(oldKids[i], child); else old.append(child); });
  oldKids.slice(nextKids.length).forEach((child) => child.remove());
  return old;
}
function patchCanvasChildren(parent, nodes, key) {
  const old = new Map([...parent.children].map((el) => [el.getAttribute(key), el]));
  let cursor = parent.firstElementChild;
  for (const next of nodes) {
    const id = next.getAttribute(key), found = old.get(id);
    if (found && key === 'data-node') for (const name of ['match', 'quiet']) next.classList.toggle(name, found.classList.contains(name));
    const el = found ? patchCanvasNode(found, next) : next;
    old.delete(id);
    if (el !== cursor) parent.insertBefore(el, cursor);
    cursor = el.nextElementSibling;
  }
  old.forEach((el) => el.remove());
}
function drawBoard(boardEl, d, selected) {
  const state = canvasState(d), L = layout(d.nodes, d.edges, Math.max(640, (boardEl.clientWidth || 1000) - 64));
  const dash = { dots: '1 5', review: '5 4' }, color = { solid: 'var(--mute)', dots: 'var(--dim)', review: 'var(--accent)' }, width = { solid: 1.5, dots: 2, review: 1.5 };
  const defs = svgEl('defs', null, ...[['h0', 'var(--mute)'], ['h1', 'var(--accent)'], ['h2', 'var(--dim)']].map(([id, c]) => svgEl('marker', { id, markerWidth: 8, markerHeight: 8, refX: 7, refY: 4, orient: 'auto', markerUnits: 'userSpaceOnUse' }, svgEl('path', { d: 'M0 0L8 4L0 8z', fill: c }))));
  const svg = svgEl('svg', { width: L.totalW, height: L.height, 'aria-hidden': 'true' }, defs,
    L.lines.filter((l) => filterState[l.g]).map((l) => svgEl('path', { 'data-edge': l.key, d: l.path, fill: 'none', stroke: color[l.style], 'stroke-width': width[l.style], 'stroke-linecap': 'round', 'stroke-linejoin': 'round',
      'stroke-dasharray': dash[l.style] || '', 'marker-end': l.arrow ? 'url(#' + ({ solid: 'h0', review: 'h1', dots: 'h2' })[l.style] + ')' : '' })));
  const paths = [...svg.children].filter((el) => el.hasAttribute('data-edge'));
  const laneLabels = L.lanes.map((lane, i) => h('div', { class: 'lane', 'data-lane': lane, style: { left: '12px', top: (L.TOP + i * L.step + 6) + 'px', width: (L.narrow ? 80 : 112) + 'px' }, title: LANE_TEXT[lane][1] }, LANE_TEXT[lane][0]));
  const labels = L.labels.filter((t) => filterState[t.g]).map((t) => h('span', { class: 'elabel', 'data-edge': t.key, style: { left: t.x + 'px', top: t.y + 'px', color: t.color || '', transform: t.align === 'center' ? 'translateX(-50%)' : t.align === 'right' ? 'translateX(-100%)' : '' } }, t.text));
  const cards = d.nodes.map((n) => L.box[n.id] ? nodeCard(n, L.box[n.id], n.id === selected) : null);
  let camera = boardEl._canvas;
  if (!camera) {
    const plane = h('div', { class: 'canvas-plane' });
    const stage = h('div', { class: 'canvas-stage' }, plane);
    boardEl.append(stage);
    camera = boardEl._canvas = { state, L, plane, stage, offset: 0 };
    camera.lanes = h('div', { class: 'canvas-lanes' });
    camera.cards = h('div', { class: 'canvas-cards' });
    camera.labels = h('div', { class: 'canvas-labels' });
    camera.svg = svgEl('svg', { 'aria-hidden': 'true' }, defs);
    camera.edges = svgEl('g', { class: 'canvas-edges' }); camera.svg.append(camera.edges);
    plane.append(camera.lanes, camera.svg, camera.cards, camera.labels);
  }
  camera.L = L;
  if (camera.plane.style.width !== L.totalW + 'px') camera.plane.style.width = L.totalW + 'px';
  if (camera.plane.style.height !== L.height + 'px') camera.plane.style.height = L.height + 'px';
  if (camera.svg.getAttribute('width') !== String(L.totalW)) camera.svg.setAttribute('width', L.totalW);
  if (camera.svg.getAttribute('height') !== String(L.height)) camera.svg.setAttribute('height', L.height);
  patchCanvasChildren(camera.edges, paths, 'data-edge');
  patchCanvasChildren(camera.lanes, laneLabels, 'data-lane');
  patchCanvasChildren(camera.cards, cards.filter(Boolean), 'data-node');
  patchCanvasChildren(camera.labels, labels, 'data-edge');
  if (state.fit) state.zoom = Math.max(.25, Math.min(1, (boardEl.clientWidth - 24) / L.totalW, (boardEl.clientHeight - 76) / L.height));
  paintCamera(boardEl);
  boardEl.scrollLeft = state.x; boardEl.scrollTop = state.y;
  if (state.fit) { boardEl.scrollLeft = 0; boardEl.scrollTop = 0; }
  state.x = boardEl.scrollLeft; state.y = boardEl.scrollTop;
  applyCanvasFocus(boardEl, d, selected);
  if (state.center) { centerCanvasNode(boardEl, state.center); state.center = null; }
  return camera;
}
function paintCamera(el) {
  const c = el._canvas, z = c.state.zoom;
  c.offset = Math.max(0, (el.clientWidth - c.L.totalW * z) / 2);
  const style = (el, name, value) => { if (el.style[name] !== value) el.style[name] = value; };
  style(c.stage, 'width', Math.ceil(c.L.totalW * z) + 'px');
  style(c.stage, 'height', Math.ceil(c.L.height * z + 76) + 'px');
  style(c.plane, 'left', c.offset + 'px'); style(c.plane, 'transform', 'scale(' + z + ')');
  const wrap = el.closest('.boardwrap');
  if (!wrap) return;
  const output = wrap.querySelector('output'), text = Math.round(z * 100) + '%';
  if (output.textContent !== text) output.textContent = text;
  wrap.querySelector('[aria-label="Zoom out"]').disabled = z <= .25;
  wrap.querySelector('[aria-label="Zoom in"]').disabled = z >= 2;
}
function zoomCanvas(el, zoom, px = el.clientWidth / 2, py = el.clientHeight / 2) {
  const c = el._canvas; if (!c) return;
  const wx = (el.scrollLeft + px - c.offset) / c.state.zoom, wy = (el.scrollTop + py) / c.state.zoom;
  c.state.zoom = Math.max(.25, Math.min(2, zoom)); c.state.fit = false;
  paintCamera(el);
  el.scrollLeft = wx * c.state.zoom + c.offset - px; el.scrollTop = wy * c.state.zoom - py;
  c.state.x = el.scrollLeft; c.state.y = el.scrollTop;
}
function applyCanvasFocus(el, d, selected) {
  const s = canvasState(d), query = s.query.trim().toLocaleLowerCase(), near = new Set([selected]);
  const groups = { approves: 'accountability', assigns: 'accountability', submits: 'accountability', collab: 'collaboration', steward: 'collaboration', review: 'reviews' };
  d.edges.filter((e) => filterState[groups[e.kind]] && (e.from === selected || e.to === selected)).forEach((e) => { near.add(e.from); near.add(e.to); });
  const matches = [];
  el.querySelectorAll('[data-node]').forEach((card) => {
    const n = d.nodes.find((n) => n.id === card.dataset.node);
    const match = !query || [n.name, n.id, n.kind, n.role, n.line, n.status.label].join(' ').toLocaleLowerCase().includes(query);
    if (match) matches.push(n.id);
    const flags = { match: !!query && match, quiet: !match || (s.focus && !near.has(n.id)) };
    for (const [key, on] of Object.entries(flags)) if (card.classList.contains(key) !== on) card.classList.toggle(key, on);
  });
  const count = el.closest('.teamfill').parentElement.querySelector('.canvas-count');
  const countText = query ? matches.length + (matches.length === 1 ? ' match' : ' matches') : d.nodes.filter((n) => n.type === 'agent').length + ' agents';
  if (count && count.textContent !== countText) count.textContent = countText;
  return matches;
}
function centerCanvasNode(el, id) {
  const c = el._canvas, b = c && c.L.box[id]; if (!b) return;
  c.state.fit = false;
  el.scrollLeft = (b.x + b.w / 2) * c.state.zoom + c.offset - el.clientWidth / 2;
  el.scrollTop = (b.y + b.h / 2) * c.state.zoom - el.clientHeight / 2;
  c.state.x = el.scrollLeft; c.state.y = el.scrollTop;
}
function canvasNavigation(el, d, selected) {
  const s = canvasState(d); let drag = null, dragged = false;
  el.addEventListener('scroll', () => { s.x = el.scrollLeft; s.y = el.scrollTop; });
  el.addEventListener('pointerdown', (e) => {
    if (e.pointerType === 'touch' || e.button !== 0 || e.target.closest('a,button')) return;
    drag = { id: e.pointerId, x: e.clientX, y: e.clientY, left: el.scrollLeft, top: el.scrollTop };
    dragged = false; el.setPointerCapture(e.pointerId); el.classList.add('panning'); el.focus({ preventScroll: true });
  });
  el.addEventListener('pointermove', (e) => {
    if (!drag) return;
    const dx = e.clientX - drag.x, dy = e.clientY - drag.y;
    if (Math.abs(dx) + Math.abs(dy) > 4) { dragged = true; s.fit = false; }
    el.scrollLeft = drag.left - dx; el.scrollTop = drag.top - dy;
  });
  const end = () => { drag = null; el.classList.remove('panning'); };
  el.addEventListener('pointerup', end); el.addEventListener('pointercancel', end); el.addEventListener('lostpointercapture', end);
  el.addEventListener('click', (e) => { if (dragged) { e.preventDefault(); dragged = false; } }, true);
  el.addEventListener('wheel', (e) => {
    if (!e.ctrlKey && !e.metaKey) { s.fit = false; return; }
    e.preventDefault(); const r = el.getBoundingClientRect();
    zoomCanvas(el, s.zoom * Math.exp(-e.deltaY * .004), e.clientX - r.left, e.clientY - r.top);
  }, { passive: false });
  el.addEventListener('keydown', (e) => {
    if (e.target !== el || e.ctrlKey || e.metaKey || e.altKey) return;
    const key = e.key.toLowerCase();
    if (['+', '=', '-', '0', 'f', 'escape', 'arrowleft', 'arrowright', 'arrowup', 'arrowdown'].includes(key)) e.preventDefault();
    if (key === '+' || key === '=') zoomCanvas(el, s.zoom * 1.2);
    else if (key === '-') zoomCanvas(el, s.zoom / 1.2);
    else if (key === 'f') { s.fit = true; drawBoard(el, d, selected()); }
    else if (key === '0') zoomCanvas(el, 1);
    else if (key === 'escape') el.closest('.teamfill').parentElement.querySelector('input').focus();
    else if (key.startsWith('arrow')) { s.fit = false; el.scrollLeft += key === 'arrowleft' ? -80 : key === 'arrowright' ? 80 : 0; el.scrollTop += key === 'arrowup' ? -80 : key === 'arrowdown' ? 80 : 0; }
  });
}
function reviewerForm() {
  const out = h('span', { class: 'small' });
  const cli = h('select', { class: 'field', 'aria-label': 'Agent CLI' }, ['claude', 'codex', 'opencode', 'gemini', 'qodercli', 'kilocode', 'deepseek'].map((c) => h('option', { value: c }, c)));
  const name = h('input', { class: 'field', value: drafts['rev:name'] || 'reviewer', 'aria-label': 'Agent name', 'data-draft': 'rev:name', style: { width: '120px', height: '24px', padding: '0 6px' } });
  name.addEventListener('input', () => { drafts['rev:name'] = name.value; });
  return h('div', { class: 'col', style: { gap: '4px' } },
    h('span', { class: 'small muted' }, 'For judgement calls, start an LLM agent on its own task (INBOX-n) to review the inboxes. It uses that CLI’s account and tokens.'),
    h('div', { class: 'row wrap', style: { gap: '6px' } }, cli, name,
      button('Start reviewer', false, (b) => { if (window.confirm('Start ' + cli.value + ' as “' + name.value + '” on a new inbox review task? It runs headless and uses that CLI’s account.')) act({ action: 'start_reviewer', cli: cli.value, name: name.value }, b, out, 'rev:name', 'Reviewer started.'); }), out));
}
function inspector(d, node) {
  const det = node.detail, t = det.task, bad = node.status.tone === 'bad';
  const kv = [];
  if (t) {
    kv.push(['Task', h('span', null, taskLink(t.id), ' ', t.title)]);
    if (t.step[1]) kv.push(['Progress', 'Step ' + Math.min(t.step[0] + 1, t.step[1]) + ' of ' + t.step[1]]);
    if (t.clock && t.clock.active_min) kv.push(['Contract', Math.floor(t.clock.elapsed_s / 60) + ' of ' + t.clock.active_min + ' min active']);
  }
  if (node.type === 'steward') { kv.push(['Last review', det.seen_s == null ? 'never' : ago(det.seen_s)]); kv.push(['24 hours', det.reminders + ' reminders, ' + det.escalations + ' escalations']); }
  else if (det.seen_s != null) kv.push(['Last seen', ago(det.seen_s)]);
  if (det.model) kv.push(['Model', det.model]);
  if (det.slot != null) kv.push(['Slot', String(det.slot)]);
  const kvEl = (rows) => h('div', { class: 'kv' }, rows.map(([k, v]) => [h('span', null, k), h('span', { class: 'break' }, v)]));
  const rel = det.relations.map((r) => [r.label, h('span', null, d.nodes.some((n) => n.id === r.who) ? link('#/team/' + encodeURIComponent(r.who), { onclick: () => { canvasState(d).center = r.who; } }, nameRole(r.who)) : nameRole(r.who), r.note ? h('span', { class: 'muted' }, ' · ' + r.note) : null)]);
  if (det.messages && det.messages.length) rel.push(['Messages, 24h', det.messages.map((m) => m.count + ' with ' + who(m.peer).display).join(', ')]);
  const title = node.type === 'agent' ? nameRole(node.id) : node.name + ' – ' + node.role;
  let actions = null;
  if (node.type === 'steward') {
    const out = h('span', { class: 'small' });
    actions = h('div', { class: 'col', style: { gap: '8px' } },
      h('span', { class: 'small muted' }, 'Reviews every inbox, reminds recipients of messages they have not acknowledged, and escalates what is stuck. It never acknowledges a message for anyone.'),
      canAct ? h('div', { class: 'row wrap', style: { gap: '6px' } }, button('Review inboxes now', true, (b) => act({ action: 'steward' }, b, out, null, 'Inboxes reviewed.')), out) : null,
      cmd('steward', 'Keeps reviewing every minute in a terminal or console pane.'),
      canAct ? reviewerForm() : null);
  }
  return h('aside', { class: 'panel', 'data-selected': node.id, 'aria-label': 'Selected participant', style: { flex: '0 0 300px', 'max-width': '100%', padding: '10px', display: 'flex', 'flex-direction': 'column', gap: '12px' } },
    inspectorPart('identity', [node.name, node.kind, node.role, node.status],
      h('div', { class: 'row', style: { gap: '10px' } }, tile(node.kind, node.role, 32, bad), h('div', { style: { 'min-width': 0 } }, h('div', { class: 'b break' }, title), h('div', { class: 'small' }, status(node.status))))),
    inspectorPart('work', [t, det.seen_s, det.model, det.slot, det.reminders, det.escalations],
      kv.length ? h('div', { class: 'col' }, h('h2', null, node.type === 'steward' ? 'Work' : 'Current work'), kvEl(kv)) : null),
    inspectorPart('warning', [det.warn, t && t.id],
      det.warn ? h('div', { class: 'note' }, h('b', { class: 'b' }, 'Needs attention'), h('span', null, det.warn), t ? cmd('claim ' + t.id + ' --take-over --as NAME') : null) : null),
    inspectorPart('actions', [node.id, canAct], actions),
    inspectorPart('relations', [det.relations.map((r) => [r, who(r.who)]), det.messages],
      rel.length ? h('div', { class: 'col' }, h('h2', null, 'Relationships'), kvEl(rel)) : null),
    inspectorPart('activity', det.activity,
      det.activity.length ? h('div', { class: 'col' }, h('h2', null, 'Recent activity'), h('div', { class: 'col', style: { gap: '3px' } }, det.activity.map((e) => h('div', { class: 'row', style: { 'align-items': 'flex-start', gap: '8px' } }, h('span', { class: 'mono dim', style: { flex: '0 0 38px' } }, clockTime(e.at)), h('span', { class: 'break small' }, e.text))))) : null),
    inspectorPart('decisions', node.type, node.type === 'human' ? link('#/decisions', { class: 'btn' }, 'Open decisions') : null),
    inspectorPart('task', t && t.id, node.type === 'agent' && t ? link('#/task/' + encodeURIComponent(t.id), { class: 'btn' }, 'Open task') : null));
}
function inspectorPart(key, value, element) {
  if (!element) return null;
  element.setAttribute('data-inspector-part', key); element._signature = JSON.stringify(value);
  return element;
}
function updateInspector(current, d, node) {
  const next = inspector(d, node), same = current.dataset.selected === node.id;
  const old = new Map([...current.children].map((el) => [el.dataset.inspectorPart, el]));
  let cursor = current.firstElementChild;
  for (const fresh of [...next.children]) {
    const key = fresh.dataset.inspectorPart, found = old.get(key);
    let el = fresh;
    if (same && found) {
      if (found._signature === fresh._signature) el = found;
      else if (['identity', 'work', 'activity', 'task', 'decisions'].includes(key)) {
        el = patchCanvasNode(found, fresh); el._signature = fresh._signature;
      }
    }
    old.delete(key);
    if (el !== cursor) current.insertBefore(el, cursor);
    if (found && found !== el) found.remove();
    cursor = el.nextElementSibling;
  }
  old.forEach((el) => el.remove());
  current.dataset.selected = node.id;
  if (!same) current.scrollTop = 0;
}

function railButton(icon, label, on, filter, onClick) {
  const b = h('button', { class: 'rbtn' + (filter ? ' filter' : on ? ' tool-on' : ''), type: 'button', title: label, 'aria-label': label, 'aria-pressed': String(!!on) }, glyph(TOOL_ICONS[icon], 20, 1.8));
  b.addEventListener('click', onClick);
  return b;
}
let boardRef = null, boardData = null, boardSel = null, teamMount = null, boardResize = null;
function selectedTeamNode(d) {
  return d.nodes.find((n) => n.id === d.pick) || d.nodes.find((n) => n.status.tone === 'bad') || d.nodes.find((n) => n.lane === 1 && n.type === 'agent') || d.nodes[1] || d.nodes[0];
}
VIEWS.team = {
  load: async (p) => { const d = await get('/api/team'); d.pick = p[1] || null; return d; },
  update(next) {
    if (!teamMount || !teamMount.board.isConnected || teamMount.data.active.id !== next.active.id) return false;
    const m = teamMount;
    Object.assign(m.data, next);
    for (const n of m.data.nodes) if (n.type === 'agent') DIR[n.id] = { display: n.name, kind: n.kind, role: n.role };
    const node = selectedTeamNode(m.data);
    m.selection.id = node.id; boardData = m.data; boardSel = node.id;
    drawBoard(m.board, m.data, node.id);
    updateInspector(m.inspector, m.data, node);
    patchCanvasNode(app.querySelector('header.top'), topBar('team', next));
    return true;
  },
  render(d) {
    for (const n of d.nodes) if (n.type === 'agent') DIR[n.id] = { display: n.name, kind: n.kind, role: n.role };
    const sel = { id: selectedTeamNode(d).id };
    const picked = selectedTeamNode(d);
    const state = canvasState(d);
    if (!state.initialized) { state.initialized = true; if (d.pick && !state.fit) state.center = sel.id; }
    const boardEl = h('div', { class: 'board', tabindex: 0, role: 'region', 'aria-label': 'Team canvas', 'aria-describedby': 'canvas-help' });
    canvasNavigation(boardEl, d, () => sel.id);
    boardRef = boardEl; boardData = d; boardSel = sel.id;
    if (boardResize) boardResize.disconnect();
    boardResize = new ResizeObserver(() => { if (boardEl.isConnected) { saveScroll(); drawBoard(boardEl, d, sel.id); } });
    boardResize.observe(boardEl);
    setTimeout(() => { if (boardEl.isConnected) drawBoard(boardEl, d, sel.id); }, 0);
    const insp = inspector(d, picked);
    teamMount = { data: d, selection: sel, inspector: insp, board: boardEl };
    insp.setAttribute('data-keep', 'team-insp:' + d.active.id + ':' + sel.id);
    const search = h('input', { type: 'search', value: state.query, placeholder: 'Find agent, role or task…', 'aria-label': 'Find on canvas', 'aria-controls': 'team-workspace' });
    search.addEventListener('input', () => { state.query = search.value; applyCanvasFocus(boardEl, d, sel.id); });
    search.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') { search.value = ''; state.query = ''; applyCanvasFocus(boardEl, d, sel.id); boardEl.focus(); }
      if (e.key !== 'Enter') return;
      e.preventDefault(); const matches = applyCanvasFocus(boardEl, d, sel.id);
      if (!matches.length) return;
      const id = matches[(matches.indexOf(sel.id) + 1) % matches.length];
      state.center = id; centerCanvasNode(boardEl, id);
      if (id === sel.id) state.center = null;
      location.hash = '#/team/' + encodeURIComponent(id);
      boardEl.focus({ preventScroll: true });
    });
    const control = (text, label, fn) => h('button', { type: 'button', 'aria-label': label, title: label, onclick: fn }, text);
    const focus = control('Focus', 'Focus on selected participant and neighbours', () => {
      state.focus = !state.focus; focus.setAttribute('aria-pressed', String(state.focus)); applyCanvasFocus(boardEl, d, sel.id);
    });
    focus.setAttribute('aria-pressed', String(state.focus));
    const controls = h('div', { class: 'canvas-controls', role: 'toolbar', 'aria-label': 'Canvas navigation' },
      control('−', 'Zoom out', () => zoomCanvas(boardEl, state.zoom / 1.2)),
      h('output', { 'aria-label': 'Canvas zoom' }, Math.round(state.zoom * 100) + '%'),
      control('+', 'Zoom in', () => zoomCanvas(boardEl, state.zoom * 1.2)), h('span', { class: 'sep', 'aria-hidden': 'true' }),
      control('Fit', 'Fit team to screen (F)', () => { state.fit = true; drawBoard(boardEl, d, sel.id); }),
      control('Reset', 'Reset zoom to 100% (0)', () => { zoomCanvas(boardEl, 1); centerCanvasNode(boardEl, sel.id); }),
      h('span', { class: 'sep', 'aria-hidden': 'true' }), focus);
    const legendLine = (style) => svgEl('svg', { width: 28, height: 8, 'aria-hidden': 'true' }, svgEl('line', { x1: 1, y1: 4, x2: 27, y2: 4, stroke: { solid: 'var(--mute)', dots: 'var(--dim)', review: 'var(--accent)' }[style], 'stroke-width': style === 'dots' ? 2 : 1.5, 'stroke-linecap': 'round', 'stroke-dasharray': { dots: '1 5', review: '5 4' }[style] || '' }));
    return [h('div', { class: 'teamfill', id: 'team-workspace' },
      h('section', { class: 'canvas-section', 'aria-label': 'Team workspace' },
      h('div', { class: 'canvas-head' },
      h('div', { class: 'row' }, h('h1', null, 'Team canvas'), h('span', { class: 'tag' }, 'Live board')),
      h('div', { class: 'canvas-find' }, search, h('span', { class: 'canvas-count', role: 'status', 'aria-live': 'polite' }))),
      h('div', { class: 'boardwrap' }, boardEl,
        h('span', { class: 'canvas-hint', id: 'canvas-help' }, 'Drag to pan · Ctrl/⌘ + scroll to zoom · + / − · F to fit'), controls,
        h('div', { class: 'toolrail', role: 'toolbar', 'aria-label': 'Canvas tools', 'aria-orientation': 'vertical' },
          [['accountability', 'accountability lines'], ['collaboration', 'collaboration and reminder lines'], ['reviews', 'review lines']].map(([key, label]) =>
            railButton(key, (filterState[key] ? 'Hide ' : 'Show ') + label, filterState[key], true, (e) => {
              filterState[key] = !filterState[key]; e.currentTarget.setAttribute('aria-pressed', String(filterState[key]));
              e.currentTarget.setAttribute('aria-label', (filterState[key] ? 'Hide ' : 'Show ') + label);
              e.currentTarget.title = e.currentTarget.getAttribute('aria-label'); drawBoard(boardEl, d, sel.id);
            }))),
        h('div', { class: 'legend boardlegend' }, h('span', null, legendLine('solid'), 'Accountability'), h('span', null, legendLine('dots'), 'Collaboration, reminders (' + d.window_hours + 'h)'), h('span', null, legendLine('review'), 'Independent review')))),
      insp)];
  },
};

// ---- inbox
const FILTERS = [['all', 'All'], ['unread', 'Unread'], ['question', 'Questions'], ['decision', 'Decisions'], ['warning', 'Warnings'], ['review', 'Reviews'], ['dead_letter', 'Dead letters']];
const KIND_TEXT = { question: 'Question', decision: 'Decision', warning: 'Warning', experience: 'Lesson', review_request: 'Review request', review_result: 'Review result', status: 'Status', handoff: 'Handoff', interrupt: 'Interrupt', ack: 'Acknowledgement' };
const ACK = { pending: ['pending', 'var(--warn)'], acked: ['acknowledged', 'var(--mute)'], dead_letter: ['dead letter', 'var(--bad)'], expired: ['expired', 'var(--mute)'], resolved: ['resolved', 'var(--mute)'] };
function readCounts(rs) {
  const acked = rs.filter((r) => r.acked_at).length, read = rs.filter((r) => r.read_at).length;
  return { total: rs.length, read, acked, unread: Math.max(0, rs.length - read) };
}
function readPhrase(c) {
  if (!c.total) return 'no receipts';
  return c.unread ? c.unread + (c.total > 1 ? ' of ' + c.total : '') + ' unread' : 'read by all';
}
function composer(det) {
  const m = det.message, other = m.sender === 'human' ? m.recipient : m.sender;
  if (['swarm', 'steward', '*'].includes(other)) return null;
  if (!canAct) return h('div', { class: 'col', style: { padding: '6px 10px', 'border-top': '1px solid var(--line2)' } }, h('h2', null, 'Reply'), cmd('send ' + other + ' "your reply" --kind decision --reply-to ' + m.id));
  const key = 'r:' + m.id, ta = field(key, 'Reply to ' + who(other).display, 3), out = h('span', { class: 'small' });
  const kind = h('select', { class: 'field', 'aria-label': 'Message kind' }, ['decision', 'status', 'question', 'warning'].map((k) => h('option', { value: k, selected: (drafts[key + ':kind'] || 'decision') === k ? true : null }, KIND_TEXT[k])));
  kind.addEventListener('change', () => { drafts[key + ':kind'] = kind.value; });
  return h('div', { class: 'col', style: { padding: '6px 10px', 'border-top': '1px solid var(--line2)', gap: '6px' } }, ta,
    h('div', { class: 'row wrap', style: { gap: '6px' } }, kind, button('Send reply', true, (b) => act({ action: 'send', to: other, kind: kind.value, reply_to: m.id, text: ta.value }, b, out, key, 'Reply sent to ' + who(other).display + '.')), out,
      h('span', { class: 'grow' }), h('span', { class: 'small dim' }, 'A reply does not acknowledge the message.')));
}
VIEWS.inbox = {
  async load(p) {
    const tab = p[1] || 'all', chosen = /^\d+$/.test(p[2] || '');
    const list = await get('/api/inbox?tab=' + encodeURIComponent(tab));
    const id = chosen ? p[2] : list.messages.length ? String(list.messages[0].id) : null;
    list.detail = id ? await get('/api/message/' + id).catch(() => null) : null;
    list.selected = id;
    const mine = list.detail && list.detail.recipients.find((r) => r.agent === 'human');
    if (chosen && id && canAct && mine && !mine.read_at) markRead(Number(id));
    return list;
  },
  render(d) {
    const list = h('section', { class: 'panel list', 'data-keep': 'inbox-list:' + d.tab, style: { flex: '1 1 360px', 'max-width': '440px', 'min-width': 0, overflow: 'hidden' } },
      d.messages.length ? d.messages.map((m) => {
        const a = ACK[m.ack_state] || [m.ack_state, 'var(--mute)'];
        const rc = m.recipients || { total: 0, read: 0, acked: 0, unread: 0 }, unread = rc.unread > 0;
        return link('#/inbox/' + d.tab + '/' + m.id, { class: 'msg' + (String(m.id) === d.selected ? ' sel' : '') + (unread ? ' unread' : '') },
          h('div', { class: 'row between', style: { 'align-items': 'baseline', gap: '8px' } },
            h('span', { class: 'break', style: { 'font-weight': unread ? 600 : 400 } }, unread ? h('span', { class: 'unread-dot', 'aria-hidden': 'true' }) : null, nameText(m.sender), h('span', { class: 'dim' }, ' → ' + nameText(m.recipient))),
            h('span', { class: 'dim small', style: { 'white-space': 'nowrap' } }, ago(m.age_s))),
          h('div', { style: { 'white-space': 'nowrap', overflow: 'hidden', 'text-overflow': 'ellipsis', color: unread ? 'var(--text)' : 'var(--mute)' } }, mdPlain(m.preview) || '(empty)'),
          h('div', { class: 'small m row wrap', style: { color: m.priority === 'critical' ? 'var(--bad)' : a[1] } },
            (m.priority === 'critical' ? 'Critical ' : '') + (KIND_TEXT[m.kind] || m.kind).toLowerCase() + ' · ',
            h('span', { style: { color: unread ? 'var(--warn)' : 'var(--mute)' } }, readPhrase(rc)),
            ' · ' + a[0] + (m.task ? ' · ' + m.task : '')));
      }) : [empty('No messages here.')]);
    const det = d.detail;
    let thread;
    if (det) {
      const m = det.message, mine = det.recipients.find((r) => r.agent === 'human'), rc = readCounts(det.recipients);
      const ackable = canAct && mine && !mine.acked_at && m.ack_state === 'pending';
      const out = h('span', { class: 'small' });
      thread = h('section', { class: 'panel thread', style: { flex: '2 1 540px', 'min-width': 0 } },
        h('div', { class: 'row between wrap', style: { padding: '6px 10px', 'border-bottom': '1px solid var(--line2)', gap: '8px' } },
          h('div', { style: { 'min-width': 0 } }, h('div', { class: 'small b', style: { color: m.priority === 'critical' ? 'var(--bad)' : 'var(--mute)' } }, (m.priority === 'critical' ? 'Critical ' : '') + (KIND_TEXT[m.kind] || m.kind).toLowerCase() + (m.task ? ' · ' + m.task : '') + ' · #' + m.id + (m.epoch != null ? ' · epoch ' + m.epoch : '')),
            h('div', { class: 'muted small' }, nameRole(m.sender) + ' to ' + nameText(m.recipient) + ' · ' + new Date(m.created_at).toLocaleString()),
            h('div', { class: 'row wrap small', style: { gap: '5px', 'margin-top': '3px' } },
              h('span', { class: 'tag', style: { color: rc.unread ? 'var(--warn)' : 'var(--mute)' } }, readPhrase(rc)),
              h('span', { class: 'tag' }, rc.acked + ' acknowledged'),
              mine && !mine.read_at ? h('span', { class: 'tag', style: { color: 'var(--warn)' } }, 'Your copy: unread') : null)),
          ackable ? h('div', { class: 'row', style: { gap: '6px' } }, out, button('Acknowledge', false, (b) => act({ action: 'ack', message: m.id }, b, out, null, 'Acknowledged.'))) : null),
        h('div', { class: 'tbody', 'data-keep': 'inbox-body:' + m.id }, h('div', { class: 'col', style: { padding: '8px 10px', gap: '8px' } },
          det.thread.map((t) => h('div', { class: 'row', style: { 'align-items': 'flex-start', gap: '10px' } }, tile(who(t.sender).kind, who(t.sender).role, 26),
            h('div', { style: { flex: 1, 'min-width': 0 } }, h('div', { class: 'row wrap', style: { gap: '6px', 'align-items': 'baseline' } }, h('b', { class: 'b' }, nameText(t.sender)), h('span', { class: 'muted small' }, who(t.sender).role), h('span', { class: 'mono dim', style: { 'margin-left': 'auto' } }, clockTime(t.created_at))),
              t.pruned ? h('div', { class: 'muted small', style: { margin: '2px 0 0' } }, '[payload pruned; envelope retained]') : markdown(t.body)))),
          expiry(m)),
        h('div', { style: { padding: '4px 10px 6px', 'border-top': '1px solid var(--line2)' } },
          h('div', { class: 'drow small', style: { 'border-top': 0, color: 'var(--mute)', 'font-weight': 600 } }, h('span', null, 'Recipient'), h('span', null, 'Read'), h('span', null, 'Acknowledged')),
          det.recipients.length ? det.recipients.map((r) => h('div', { class: 'drow small' },
            h('span', null, nameRole(r.agent)),
            h('span', { class: 'm', style: { color: r.read_at ? 'var(--info)' : 'var(--warn)' } }, r.read_at ? h('span', null, 'Read ', h('span', { class: 'mono dim' }, clockTime(r.read_at))) : 'Unread'),
            h('span', { class: 'm', style: { color: r.acked_at ? 'var(--good)' : 'var(--mute)' } }, r.acked_at ? clockTime(r.acked_at) : r.claim && r.claim.lease_left_s > 0 ? 'leased' : '–'))) : [h('div', { class: 'small dim' }, 'No recipient rows.')])),
        composer(det));
    } else thread = h('section', { class: 'panel thread', style: { flex: '2 1 540px' } }, empty('Select a message.'));
    const out = h('span', { class: 'small' });
    return [h('div', { class: 'row between wrap' }, h('div', { class: 'row wrap', style: { gap: '10px', 'align-items': 'baseline' } }, h('h1', null, 'Inbox'), canAct ? h('span', { class: 'row', style: { gap: '6px' } }, button('Review inboxes now', false, (b) => act({ action: 'steward' }, b, out, null, 'Inboxes reviewed.')), out) : null),
      h('div', { class: 'filters' }, FILTERS.map(([key, label]) => link('#/inbox/' + key, { class: d.tab === key ? 'on' : '' }, label, h('span', { class: 'b' }, d.tabs[key] || 0))))),
      h('div', { class: 'inboxfill' }, list, thread)];
  },
};
function expiry(m) {
  if (m.ack_state === 'dead_letter') return h('div', { class: 'note bad small' }, 'This critical message expired unacknowledged and escalated once to the coordinator. Resolve it after reviewing the evidence.');
  if (m.ack_state === 'pending' && m.priority === 'critical' && m.expires_at) {
    const left = (new Date(m.expires_at) - Date.now()) / 1000;
    if (left > 0) return h('div', { class: 'note bad small' }, 'Unacknowledged. If nobody acknowledges it within ' + (left > 3600 ? Math.round(left / 3600) + 'h' : mmss(left)) + ' it becomes a dead letter and escalates once to the coordinator.');
  }
  return null;
}

// ---- decisions
VIEWS.decisions = {
  load: () => get('/api/decisions'),
  render(d) {
    const openQ = d.questions.filter((q) => q.open);
    const awaiting = [
      ...d.approvals.map((a) => needItem({ type: 'approval', ...a })),
      ...openQ.map((q) => needItem({ type: 'question', id: q.id, text: q.text, from: q.from, to: q.to, task: q.task, critical: q.critical, age_s: q.age_s })),
      ...d.incidents.filter((i) => !i.resolved).map(incidentItem)];
    const settled = [
      ...d.questions.filter((q) => !q.open).slice(0, 6).map((q) => h('div', { class: 'item' }, h('div', { class: 'row between' }, h('span', { class: 'b small muted' }, (q.waived ? 'Waived' : 'Answered') + ' · Q' + q.id), h('span', { class: 'dim small' }, ago(q.age_s))),
        h('div', { class: 'break' }, q.text), q.answer ? h('div', { class: 'muted break small' }, nameText(q.answered_by || 'human') + ': ' + q.answer) : null)),
      ...d.decisions.map((m) => h('div', { class: 'item' }, h('div', { class: 'row between' }, h('span', { class: 'b small', style: { color: 'var(--accentink)' } }, 'Decision'), h('span', { class: 'dim small' }, ago(m.age_s))), h('div', { class: 'break' }, m.text), h('div', { class: 'muted small' }, nameText(m.sender) + ' to ' + nameText(m.recipient)))),
      ...d.incidents.filter((i) => i.resolved).map(incidentItem)];
    return [h('div', { class: 'row wrap', style: { gap: '10px', 'align-items': 'baseline' } }, h('h1', null, 'Decisions'), h('span', { class: 'muted small' }, 'Bounded: one counter per contract, timed decision windows, reviews that cite evidence.')),
      h('div', { class: 'cols workfill' },
        h('div', { class: 'c2 col', 'data-keep': 'dc-c2', style: { gap: '8px' } },
          section('Awaiting a decision', awaiting.length ? h('span', { class: 'b small', style: { color: 'var(--warn)' } }, awaiting.length + ' open') : null, panel(awaiting.length ? awaiting : empty('No open questions, approvals or reviews.'))),
          section('Contract negotiations', null, d.negotiations.length ? panel(d.negotiations.map(contractItem)) : panel(empty('No contract is being negotiated.')))),
        h('div', { class: 'c1 col', 'data-keep': 'dc-c1', style: { gap: '8px' } },
          section('Recently settled', null, panel(settled.length ? settled : empty('Nothing settled yet.'))),
          d.interrupts.length ? section('Interrupts', null, panel(d.interrupts.map((r) => h('div', { class: 'item' }, h('div', { class: 'row between' }, h('span', { class: 'b small', style: { color: r.mode === 'cancel' ? 'var(--bad)' : 'var(--warn)' } }, r.mode + ' · ' + r.state), h('span', { class: 'dim small' }, ago(r.age_s))),
            h('div', { class: 'break' }, nameText(r.by) + ' to ' + nameText(r.target) + (r.task ? ' on ' + r.task : '') + ': ' + r.reason))))) : null))];
  },
};
function incidentItem(i) {
  return h('div', { class: 'item' }, h('div', { class: 'row between' }, h('span', { class: 'b small', style: { color: i.resolved ? 'var(--mute)' : 'var(--bad)' } }, (i.resolved ? 'Incident ' + (i.outcome || 'resolved') : 'Incident open') + ' · I' + i.id), h('span', { class: 'mono dim' }, 'rev ' + i.revision)),
    h('div', { class: 'break' }, i.signal), h('div', { class: 'muted small' }, 'Subject ' + (i.subject ? nameText(i.subject) : '–') + ' · reviewer ' + (i.reviewer ? nameText(i.reviewer) : 'not yet assigned') + (i.verdict ? ' · verdict ' + i.verdict : ''), ' · ', taskLink(i.task)),
    i.resolved ? null : cmd('incident resolve ' + i.id + ' --revision ' + i.revision + ' --outcome cleared|upheld|escalated --reason "why"'));
}
function contractItem(c) {
  return h('div', { class: 'item' }, h('div', { class: 'row between' }, h('span', null, taskLink(c.task), ' ', h('span', { class: 'm' }, c.title || '')), h('span', { class: 'b small', style: { color: c.state === 'countered' ? 'var(--warn)' : c.state === 'declined' ? 'var(--bad)' : 'var(--info)' } }, c.state)),
    h('div', { class: 'muted small' }, 'Offered to ' + nameText(c.owner) + ' · ' + c.active_min + ' min active, ' + c.deadline_min + ' min deadline · risk ' + (c.risk || '?') + ' · counters ' + c.counters + '/1'),
    c.decision_left_s != null ? h('div', { class: 'mono small', style: { color: c.decision_left_s < 60 ? 'var(--bad)' : 'var(--warn)' } }, c.decision_left_s > 0 ? 'decision window ' + mmss(c.decision_left_s) : 'decision window closed') : null,
    c.state === 'countered' ? cmd('contract decide ' + c.task + ' --reason "why"') : null);
}

// ---- one task
VIEWS.task = {
  load: (p) => get('/api/task/' + encodeURIComponent(p[1] || '')),
  render(d) {
    const t = d.task, k = t.clock, a = d.agent;
    const [label, tone] = [LABEL[t.state] || t.state, TASK_TONE[t.state] || 'mute'];
    const steps = d.steps.length ? d.steps.map((s) => h('div', { class: 'step ' + (s.done ? 'done' : s.current ? 'cur' : '') },
      h('div', { class: 'rail' }, h('div', { class: 'no' }, s.done ? '✓' : s.n), h('i')),
      h('div', { style: { flex: 1, 'min-width': 0, 'padding-bottom': '6px' } }, h('div', { class: 'row wrap', style: { gap: '6px' } }, h('span', { class: s.done || s.current ? 'm' : 'm muted' }, s.text), h('span', { class: 'grow' }), s.at ? h('span', { class: 'mono dim' }, clockTime(s.at)) : s.current ? h('span', { class: 'small m', style: { color: 'var(--accent)' } }, 'In progress') : null),
        s.evidence ? h('div', { class: 'muted small break', style: { 'margin-top': '2px' } }, h('span', { class: 'b' }, 'Evidence: '), s.evidence) : null))) : [h('div', { class: 'small dim' }, 'No plan yet. The holder runs `plan` first.')];
    let contract = null;
    if (k && k.active_min) {
      const used = Math.min(1, k.elapsed_s / (k.active_min * 60)), col = used >= 1 ? 'var(--bad)' : used >= 0.8 ? 'var(--warn)' : 'var(--accent)';
      contract = panel(h('div', { class: 'item' }, h('div', { class: 'row between' }, h('h2', null, 'Contract · ' + k.state), k.deadline_left_s != null ? h('span', { class: 'mono dim' }, 'deadline ' + mmss(k.deadline_left_s)) : null),
        h('div', { class: 'row', style: { 'align-items': 'baseline', gap: '6px' } }, h('span', { class: 'b', style: { 'font-size': '16px' } }, mmss(k.elapsed_s)), h('span', { class: 'muted small' }, 'of ' + k.active_min + ':00 active' + (k.running ? '' : k.elapsed_s ? ' (paused)' : ' (not started)')),
          h('span', { class: 'bar', style: { width: '100px' } }, h('i', { style: { width: used * 100 + '%', background: col } }))),
        h('div', { class: 'small muted' }, 'Deliberation ' + k.deliberation_min + 'm · review reserve ' + k.review_min + 'm · checkpoint every ' + k.checkpoint_min + 'm' + (d.checkpoints.length ? ' · last ' + ago(d.checkpoints[0].age_s) : ' · none yet'))));
    }
    return [
      h('div', { class: 'col', style: { gap: '4px' } }, h('div', { class: 'small dim' }, link('#/tasks', null, 'Tasks'), ' / ', h('span', { class: 'mono' }, t.id)),
        h('div', { class: 'row wrap', style: { gap: '12px', 'align-items': 'baseline' } }, h('h1', null, t.title), status({ label, tone: tone === 'mute' ? 'info' : tone }), h('span', { class: 'dim mono' }, 'epoch ' + t.epoch), t.goal ? h('span', { class: 'muted small' }, 'Goal ' + t.goal) : null, t.merged_sha ? h('span', { class: 'mono dim' }, 'merged ' + t.merged_sha) : null)),
      t.failure ? h('div', { class: 'note bad' }, t.failure) : null, t.owed ? h('div', { class: 'note small' }, 'Owed checks: ' + t.owed) : null,
      h('div', { class: 'cols workfill' },
        h('div', { class: 'c2 col', 'data-keep': 'tk-c2', style: { gap: '8px' } },
          section('Plan, in order, with evidence', h('span', { class: 'mono dim' }, t.step[0] + '/' + t.step[1]), panel(h('div', { style: { padding: '6px 10px 2px' } }, steps))),
          h('div', { class: 'cols', style: { gap: '8px' } },
            h('div', { class: 'c1' }, section('Scope and locks', null, panel(h('div', { class: 'item' }, t.scope ? h('div', { class: 'mono break' }, t.scope) : h('div', { class: 'small dim' }, 'No scope recorded.'),
              d.locks.length ? d.locks.map((l) => h('div', { class: 'row small' }, h('span', { class: 'mono break', style: { flex: 1 } }, l.name), h('span', { class: 'muted' }, 'locked by ' + nameText(l.agent)))) : h('div', { class: 'small dim' }, 'No locks held.'), t.done_when ? h('div', { class: 'small muted' }, 'Done when: ' + t.done_when) : null)))),
            h('div', { class: 'c1' }, section('Recent events', null, activityList(d.events, 10))))),
        h('aside', { class: 'c1 col', 'data-keep': 'tk-c1', style: { gap: '8px' } },
          a ? panel(h('div', { class: 'item' }, h('div', { class: 'row', style: { gap: '10px' } }, tile(a.kind, who(a.name).role, 30, a.state === 'offline'), h('div', { style: { flex: 1, 'min-width': 0 } }, h('div', { class: 'b break' }, nameRole(a.name)), h('div', { class: 'small m', style: { color: a.state === 'offline' ? 'var(--bad)' : 'var(--good)' } }, a.state === 'offline' ? 'Offline' : a.state === 'busy' ? 'Working' : 'Ready'))),
            h('div', { class: 'kv small' }, h('span', null, 'Last seen'), h('span', null, ago(a.seen_s)), h('span', null, 'Branch'), h('span', { class: 'mono break' }, t.branch || '–'), h('span', null, 'Worktree'), h('span', { class: 'mono break' }, t.worktree || '–'), h('span', null, 'Slot'), h('span', null, a.slot == null ? '–' : String(a.slot)))))
            : panel(h('div', { class: 'item muted' }, 'No agent holds this task.')),
          contract,
          section('Checkpoints', null, panel(d.checkpoints.length ? d.checkpoints.map((c) => h('div', { class: 'item' }, h('div', { class: 'row between' }, h('span', { class: 'mono b' }, '#' + c.id), h('span', { class: 'dim small' }, ago(c.age_s))), h('div', { class: 'break' }, c.note), h('div', { class: 'muted small break' }, 'Next: ' + c.next + (c.blockers ? ' · blocked: ' + c.blockers : '')))) : empty('No checkpoints yet.'))),
          d.questions.length ? section('Questions', null, panel(d.questions.map((q) => h('div', { class: 'item' }, h('span', { class: 'b small', style: { color: q.open ? (q.critical ? 'var(--bad)' : 'var(--warn)') : 'var(--good)' } }, (q.open ? (q.critical ? 'Critical, open' : 'Open') : 'Answered') + ' · Q' + q.id), h('span', { class: 'break' }, q.text))))) : null,
          section('Safe next action', null, cmd('explain ' + t.id))))];
  },
};

// ---- loop
let lastSig = '', failures = 0, poolCache = null;
async function get(path, retry = true) {
  const r = await fetch(path + (boardId ? (path.includes('?') ? '&' : '?') + 'b=' + encodeURIComponent(boardId) : ''), { headers: { 'X-Swarm-Token': token }, cache: 'no-store' });
  if (r.status === 403) throw new Error('forbidden');
  if (r.status === 404) {
    const why = await r.json().catch(() => ({}));
    if (boardId && retry && /not registered/.test(why.error || '')) { rememberBoard(''); return get(path, false); } // a stale board in this tab: use the default
    throw new Error(/not registered/.test(why.error || '') ? 'no board' : 'not found');
  }
  if (!r.ok) throw new Error('HTTP ' + r.status);
  const data = await r.json();
  if (data.active && data.active.id !== boardId) rememberBoard(data.active.id);
  if (data.directory) DIR = { human: DIR.human, ...data.directory };
  if (data.command) hivewireCmd = data.command;
  if ('actions' in data) canAct = !!data.actions;
  return data;
}
function parts() { return location.hash.replace(/^#\/?/, '').split('/').map(decodeURIComponent); }
function typing() {
  const el = document.activeElement;
  return !!(el && app.contains(el) && ['TEXTAREA', 'INPUT', 'SELECT'].includes(el.tagName));
}
let pageLeaving = false;
window.addEventListener('beforeunload', () => { pageLeaving = true; });
window.addEventListener('pageshow', () => { pageLeaving = false; });
async function tick() {
  if (pageLeaving) return;
  const p = parts(), view = VIEWS[p[0]];
  const requestedBoard = boardId;
  if (!view) { location.hash = '#/'; return; }
  try {
    const data = await view.load(p);
    if (requestedBoard !== boardId || JSON.stringify(p) !== JSON.stringify(parts())) return;
    failures = 0;
    const sig = JSON.stringify([p, data], (k, v) => (k === 'now' ? undefined : v));
    if (sig === lastSig && !document.getElementById('problem')) return;
    if (lastSig && typing()) return; // never redraw under someone's cursor; the next refresh after they leave the field will
    if (p[0] === 'team' && boardRef && boardRef.classList.contains('panning')) return;
    if (data.pool) poolCache = data.pool;
    const focused = p[0] === 'team' && document.activeElement.closest('.teamfill');
    const focusKey = focused && (document.activeElement.getAttribute('data-node') || document.activeElement.getAttribute('aria-label'));
    if (!view.update || !view.update(data)) shell(p[0] === 'task' ? 'tasks' : p[0], { ...data, pool: data.pool || poolCache }, view.render(data));
    if (focusKey) app.querySelectorAll('.teamfill [data-node], .teamfill [aria-label]').forEach((el) => {
      if ((el.getAttribute('data-node') || el.getAttribute('aria-label')) === focusKey) el.focus({ preventScroll: true });
    });
    lastSig = sig;
  } catch (e) {
    if (pageLeaving || requestedBoard !== boardId || JSON.stringify(p) !== JSON.stringify(parts())) return;
    failures++;
    if (!['forbidden', 'no board', 'not found'].includes(e.message)) console.error('Hivewire dashboard:', e);
    const msg = e.message === 'forbidden' ? 'This page needs its link. Run `hivewire dashboard --web` in your repository to print it. The link stays the same each time, so a bookmark of it keeps working.'
      : e.message === 'no board' ? 'This board is not listed in the dashboard. Run `hivewire dashboard --web` in its repository to add it.'
      : e.message === 'not found' ? 'Not found on this board.' : /^HTTP |fetch|network/i.test(e.message) ? 'Lost contact with the dashboard server. Retrying…' : 'This page could not be drawn. Details are in the browser console; it retries every few seconds.';
    if (failures > 1 || ['forbidden', 'no board', 'not found'].includes(e.message)) {
      lastSig = '';
      app.replaceChildren(h('main', { class: 'page' }, h('div', { id: 'problem', class: 'note bad' }, msg), link('#/', null, 'Back to the overview')));
    }
  }
}
window.addEventListener('hashchange', () => { lastSig = ''; tick(); });
document.addEventListener('focusout', () => setTimeout(() => { if (!typing()) tick(); }, 150));
tick();
setInterval(tick, 3000);
})();
</script>
</body>
</html>
"""
