"""Write (or, with --check, verify) the skill's MANIFEST.json: version and SHA-256 per file.

The installer refuses any downloaded file whose hash differs from the manifest of
the same release, so a stale manifest makes every install fail. CI runs --check.
Hashes are taken over LF line endings: a Windows checkout with core.autocrlf=true
must produce the same manifest as Linux.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "agents-collaboration"
FILES = ("SKILL.md", "hivewire.py", "swarm_core.py", "swarm_deputy.py", "swarm_deadhand.py", "EDGE-CASES.md", "BUS.md", "BUS.schema.json", "swarm_web.py", "swarm_sessions.py", "swarm_workspace.py", "swarm_fonts.css", "code_intelligence.py", "intelligence_graph.py", "intelligence_execution.py", "intelligence_metrics.py", "intelligence_mcp.py", "hivewire_intelligence.py")


def version() -> str:
    for line in (ROOT / "pyproject.toml").read_text(encoding="utf-8").splitlines():
        if line.startswith("version"):
            return line.split("=", 1)[1].strip().strip('"')
    raise SystemExit("no version in pyproject.toml")


def build() -> str:
    files = {name: hashlib.sha256((SKILL / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for name in FILES}
    return json.dumps({"version": version(), "files": files}, indent=2) + "\n"


def main() -> int:
    wanted = build()
    path = SKILL / "MANIFEST.json"
    if "--check" in sys.argv:
        current = path.read_text(encoding="utf-8") if path.exists() else ""
        if current != wanted:
            print("MANIFEST.json is stale: run `python scripts/make_manifest.py`", file=sys.stderr)
            return 1
        print("MANIFEST.json is current")
        return 0
    path.write_text(wanted, encoding="utf-8", newline="\n")
    print(f"wrote {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
