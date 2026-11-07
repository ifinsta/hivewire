"""Local read-only MCP stdio bridge for Hivewire intelligence.

No network listener, credentials or mutation tools. Launch through Jani's
governed external-tool broker for tenancy and delegated permissions; this
process itself is not an IAM service. Never expose the local stdio process
directly to untrusted multi-tenant clients.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from intelligence_graph import IndexErrorSafe, check_catalog, impact
from intelligence_metrics import compare

MAX_MESSAGE = 500_000
MAX_REQUESTS = 1000
VERSION = "2025-11-25"


def tools_list() -> list[dict]:
    return [
        {
            "name": "intelligence.impact",
            "description": "Inspect pinned cross-repository change impact (heuristic)",
            "inputSchema": {
                "type": "object", "additionalProperties": False,
                "properties": {"repo": {"type": "string"}, "path": {"type": "string"}},
                "required": ["repo", "path"]
            }
        },
        {
            "name": "intelligence.regression",
            "description": "Read tenant-scoped measured regression evidence; not a clearance",
            "inputSchema": {
                "type": "object", "additionalProperties": False,
                "properties": {
                    "repo": {"type": "string"}, "baseline": {"type": "string"},
                    "candidate": {"type": "string"}
                }, "required": ["repo", "baseline", "candidate"]
            }
        },
    ]


def handle(request: dict, catalog: dict, tenant: str, store: Path | None,
           policy: dict | None) -> dict | None:
    if not isinstance(request, dict) or request.get("jsonrpc") != "2.0":
        return {"jsonrpc": "2.0", "id": None,
                "error": {"code": -32600, "message": "Invalid Request"}}
    method, identifier = request.get("method"), request.get("id")
    if identifier is not None and (not isinstance(identifier, (str, int)) or type(identifier) is bool):
        return {"jsonrpc": "2.0", "id": None,
                "error": {"code": -32600, "message": "Invalid request ID"}}
    if method == "notifications/initialized" and identifier is None:
        return None
    if method == "initialize":
        result = {"protocolVersion": VERSION, "capabilities": {"tools": {"listChanged": False}},
                  "serverInfo": {"name": "hivewire-intelligence", "version": "0.1.0"}}
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        result = {"tools": tools_list()}
    elif method == "tools/call":
        params = request.get("params")
        if not isinstance(params, dict) or set(params) != {"name", "arguments"}:
            return {"jsonrpc": "2.0", "id": identifier,
                    "error": {"code": -32602, "message": "Invalid parameters"}}
        name, args = params["name"], params["arguments"]
        if not isinstance(args, dict) or any(not isinstance(v, str) or len(v) > 256 for v in args.values()):
            return {"jsonrpc": "2.0", "id": identifier,
                    "error": {"code": -32602, "message": "Invalid tool arguments"}}
        try:
            if name == "intelligence.impact" and set(args) == {"repo", "path"}:
                evidence = impact(catalog, args["repo"], args["path"])
            elif name == "intelligence.regression" and set(args) == {"repo", "baseline", "candidate"} and store and policy:
                evidence = compare(store, tenant, args["repo"], args["baseline"], args["candidate"], policy)
            else:
                return {"jsonrpc": "2.0", "id": identifier,
                        "error": {"code": -32602, "message": "Unsupported or unauthorized tool"}}
        except (IndexErrorSafe, OSError, ValueError):
            # Do not leak host paths, source bytes or tenant internals through errors.
            return {"jsonrpc": "2.0", "id": identifier,
                    "result": {"content": [{"type": "text", "text": "Intelligence evidence unavailable"}],
                               "isError": True}}
        result = {"content": [{"type": "text", "text": json.dumps(evidence, sort_keys=True)}],
                  "isError": False}
    else:
        return {"jsonrpc": "2.0", "id": identifier,
                "error": {"code": -32601, "message": "Method not found"}}
    if identifier is None:
        return None
    return {"jsonrpc": "2.0", "id": identifier, "result": result}


def serve(catalog_path: Path, tenant: str, store: Path | None, policy: dict | None,
          stdin=None, stdout=None) -> int:
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout
    if not tenant or len(tenant) > 128:
        raise IndexErrorSafe("explicit scoped tenant is required")
    if catalog_path.stat().st_size > 20_000_000:
        raise IndexErrorSafe("MCP graph catalog too large")
    catalog = check_catalog(json.loads(catalog_path.read_text(encoding="utf-8")))
    for _ in range(MAX_REQUESTS):
        raw = stdin.readline(MAX_MESSAGE + 1)
        if not raw:
            return 0
        if len(raw) > MAX_MESSAGE or not raw.endswith("\n"):
            return 3
        try:
            request = json.loads(raw)
        except ValueError:
            response = {"jsonrpc": "2.0", "id": None,
                        "error": {"code": -32700, "message": "Parse error"}}
        else:
            response = handle(request, catalog, tenant, store, policy)
        if response is not None:
            stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
            stdout.flush()
    return 3
