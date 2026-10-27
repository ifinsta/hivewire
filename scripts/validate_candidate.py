"""Run one complete discovery at a fixed source hash, retaining per-case evidence."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]


def hashes():
    paths = [ROOT / name for name in ("install.py", "pyproject.toml", "docs/simulation.md")]
    for name in ("tests", "scripts", "skills", "examples", ".github/workflows"):
        paths.extend(p for p in (ROOT / name).rglob("*")
                     if p.is_file() and "__pycache__" not in p.parts)
    return {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(paths) if p.is_file()}


def normalized_fingerprint(inputs):
    normalized = {name: hashlib.sha256((ROOT / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                  for name in inputs}
    return hashlib.sha256(json.dumps(normalized, sort_keys=True).encode("utf-8")).hexdigest()


def flatten(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from flatten(item)
        else:
            yield item


class EvidenceResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cases = []

    def startTest(self, test):
        self.started = time.monotonic()
        super().startTest(test)

    def stopTest(self, test):
        failed = {t.id() for t, _ in self.failures + self.errors
                  if t is test or getattr(t, "test_case", None) is test}
        skipped = {t.id(): reason for t, reason in self.skipped}
        self.cases.append({"id": test.id(), "seconds": round(time.monotonic() - self.started, 3),
                           "outcome": "failed" if failed or test in self.unexpectedSuccesses else "skipped" if test.id() in skipped else "passed",
                           "skip_reason": skipped.get(test.id())})
        super().stopTest(test)


def worker(spec, destination):
    sys.path.insert(0, str(ROOT / "tests"))
    suite = unittest.defaultTestLoader.loadTestsFromNames(json.loads(Path(spec).read_text(encoding="utf-8")))
    result = unittest.TextTestRunner(verbosity=2, resultclass=EvidenceResult).run(suite)
    Path(destination).write_text(json.dumps({"ok": result.wasSuccessful(), "cases": result.cases}, indent=2), encoding="utf-8")
    return 0 if result.wasSuccessful() else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--output", type=Path, default=ROOT / "candidate-validation")
    parser.add_argument("--worker", nargs=2, metavar=("SPEC", "RESULT"))
    args = parser.parse_args()
    if args.worker:
        return worker(*args.worker)
    if not 1 <= args.workers <= 32:
        parser.error("--workers must be 1..32")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        parser.error("--output must be a fresh empty directory; preserve prior evidence")
    output.mkdir(parents=True, exist_ok=True)
    before = hashes()
    fingerprint = normalized_fingerprint(before)
    started = time.time()
    sys.path.insert(0, str(ROOT / "tests"))
    ids = sorted(test.id() for test in flatten(unittest.defaultTestLoader.discover(str(ROOT / "tests"))))
    if len(ids) != len(set(ids)) or not ids:
        repeated = sorted({value for value in ids if ids.count(value) > 1})
        raise RuntimeError(f"Discovery must produce unique, nonempty test IDs: duplicates={repeated[:10]}, total={len(ids)}")
    (output / "source-before.json").write_text(json.dumps(before, indent=2), encoding="utf-8")

    def run(index):
        spec = output / f"shard-{index}.tests.json"
        destination = output / f"shard-{index}.result.json"
        if destination.exists():
            raise RuntimeError("Use a fresh --output directory; earlier evidence must not be reused")
        spec.write_text(json.dumps(ids[index::args.workers]), encoding="utf-8")
        command = [sys.executable, str(Path(__file__).resolve()), "--worker", str(spec), str(destination)]
        with (output / f"shard-{index}.log").open("wb") as log:
            completed = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=False)
        try:
            result = json.loads(destination.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            result = None
        return {"index": index, "exit_code": completed.returncode, "result": result,
                "log_sha256": hashlib.sha256((output / f"shard-{index}.log").read_bytes()).hexdigest()}

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        shards = list(pool.map(run, range(args.workers)))
    after = hashes()
    cases = [case for shard in shards if shard["result"] for case in shard["result"]["cases"]]
    exact = sorted(c["id"] for c in cases) == ids
    ok = before == after and exact and all(s["exit_code"] == 0 and s["result"] and s["result"]["ok"] for s in shards)
    report = {"ok": ok, "platform": platform.platform(), "python": sys.version,
              "system": platform.system(), "python_version": platform.python_version(),
              "source_fingerprint": fingerprint,
              "core_sha256": before["skills/agents-collaboration/swarm_core.py"],
              "source_unchanged": before == after, "exact_discovery_coverage": exact,
              "changed_inputs": sorted(k for k in before.keys() | after.keys() if before.get(k) != after.get(k)),
              "discovered": len(ids), "workers": args.workers,
              "elapsed_seconds": round(time.time() - started, 3),
              "counts": {state: sum(c["outcome"] == state for c in cases) for state in ("passed", "skipped", "failed")},
              "cases": sorted(cases, key=lambda c: c["id"]), "shards": shards,
              "environment": {"os_name": os.name}}
    (output / "source-after.json").write_text(json.dumps(after, indent=2), encoding="utf-8")
    (output / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k not in ("cases", "shards")}))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
