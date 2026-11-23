"""Loopback-only, read-only fleet console.

The operator console never reuses the worker's mTLS listener or treats a
dashboard request as a privileged Hivewire action.
"""
from __future__ import annotations

import argparse
import hmac
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .core import Store

HTML = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hivewire Fleet</title><link rel="stylesheet" href="/app.css"></head>
<body><main><header><h1>Hivewire Fleet</h1><small>Local Master Console — read only</small></header>
<section><label>Operator token <input type="password" id="token" autocomplete="off" placeholder="Locally configured token"></label>
<button id="refresh" type="button">Refresh state</button><span id="state" role="status"></span></section>
<section><h2>Registered machines</h2><div id="nodes"></div></section>
<section><h2>Assignments</h2><div id="jobs"></div></section>
<section><h2>Submissions</h2><div id="submissions"></div></section>\n<section><h2>Node incidents</h2><div id="incidents"></div></section>
<footer>Missing telemetry is not proof of execution failure. Accepted source requires an independent merge receipt.</footer>
</main><script src="/app.js"></script></body></html>"""

CSS = """*{box-sizing:border-box}body{margin:0;background:#f5f7fb;color:#1c293b;font:14px system-ui,-apple-system,Segoe UI,sans-serif}main{max-width:1020px;margin:auto;padding:32px 18px}header{padding:22px 24px;background:#1a56db;color:#fff;border-radius:12px}h1{margin:0;font-size:24px}small{opacity:.85}section{background:white;border:1px solid #dce3ee;border-radius:12px;padding:18px;margin-top:16px}h2{font-size:17px}label{display:inline-flex;align-items:center;gap:8px;margin-right:12px}input{padding:9px;border:1px solid #bbc8d9;border-radius:6px}button{padding:10px 14px;background:#1a56db;border:0;border-radius:7px;color:white;cursor:pointer}.entry{display:grid;grid-template-columns:1.3fr 1fr 2fr;gap:8px;padding:12px 5px;border-bottom:1px solid #e8edf3;word-break:break-word}.entry:last-child{border:0}.entry:first-child{font-weight:600}footer{margin-top:26px;color:#586477}@media(max-width:580px){.entry{grid-template-columns:1fr 1fr}.entry span:last-child{grid-column:1 / -1}}"""

JS = """'use strict';
const list = (id, rows, fields) => {
  const target = document.getElementById(id); target.replaceChildren();
  for (const row of rows) {
    const div = document.createElement('div'); div.className = 'entry';
    for (const field of fields) {
      const span = document.createElement('span');
      span.textContent = String(row[field] ?? '—');
      div.appendChild(span);
    }
    target.appendChild(div);
  }
  if (!rows.length) target.textContent = 'No records.';
};
document.getElementById('refresh').addEventListener('click', async () => {
  const token = document.getElementById('token').value;
  const status = document.getElementById('state');
  status.textContent = 'Loading…';
  try {
    const response = await fetch('/api/state', {
      headers: {'X-Hivewire-Operator': token}, cache: 'no-store'
    });
    if (!response.ok) throw new Error('Access refused (' + response.status + ')');
    const data = await response.json();
    list('nodes', data.nodes, ['id','health','last_seen']);
    list('jobs', data.jobs, ['id','state','node']);
    list('submissions', data.submissions, ['id','state','node']);
    list('incidents', data.incidents, ['node','state','resolved_at']);
    status.textContent = 'Current snapshot';
  } catch (err) { status.textContent = err.message; }
});"""


def serve_console(state_path: str, operator_token: str, port: int = 8765) -> ThreadingHTTPServer:
    if len(operator_token) < 24:
        raise ValueError("set a strong token with HWP_OPERATOR_TOKEN (24+ characters)")
    if not 1 <= port <= 65535:
        raise ValueError("invalid local console port")
    store = Store(state_path)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args) -> None:
            return

        def do_GET(self) -> None:
            host = self.headers.get("Host", "")
            if host not in (f"127.0.0.1:{port}", f"localhost:{port}"):
                self.reply(403, b"Access refused", "text/plain")
                return
            if self.path == "/api/state":
                supplied = self.headers.get("X-Hivewire-Operator", "")
                if not hmac.compare_digest(supplied, operator_token):
                    self.reply(403, b'{"error":"access refused"}', "application/json")
                    return
                payload = json.dumps({"nodes": store.nodes(), "jobs": store.jobs(),
                                      "submissions": store.submissions(),
                                      "incidents": store.incidents()}, sort_keys=True).encode()
                self.reply(200, payload, "application/json")
            elif self.path == "/":
                self.reply(200, HTML.encode(), "text/html; charset=utf-8")
            elif self.path == "/app.js":
                self.reply(200, JS.encode(), "application/javascript; charset=utf-8")
            elif self.path == "/app.css":
                self.reply(200, CSS.encode(), "text/css; charset=utf-8")
            else:
                self.reply(404, b"Not found", "text/plain")

        def reply(self, status: int, content: bytes, mime: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Security-Policy",
                             "default-src 'none'; script-src 'self'; style-src 'self'; "
                             "connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(content)
    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def main() -> int:
    parser = argparse.ArgumentParser(description="Hivewire local read-only fleet console")
    parser.add_argument("master_config")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    config = json.loads(Path(args.master_config).read_text(encoding="utf-8"))
    try:
        server = serve_console(config["state"], os.environ.get("HWP_OPERATOR_TOKEN", ""), args.port)
    except (ValueError, KeyError) as exc:
        print("fleet-dashboard: " + str(exc), file=sys.stderr)
        return 2
    print(f"Fleet dashboard: http://127.0.0.1:{args.port} (read-only)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
