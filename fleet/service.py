"""Continuously supervise one Hivewire HWP/1 node.

The one-process file lock prevents two service instances from polling/executing
the same assignments. Work is profile-authorized locally, and the durable
worker_runs ledger prevents replay after restart. Unexpected termination during
a job remains uncertain and requires operator reconciliation.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from contextlib import contextmanager
from pathlib import Path

from .core import Refusal, run_worker_once


@contextmanager
def exclusive_state_lock(state_file: str):
    lock_path = Path(state_file + ".service.lock").resolve()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        try:
            # Do not rewrite a byte another Windows instance has locked.
            if lock.seek(0, os.SEEK_END) == 0:
                lock.write(b"0")
                lock.flush()
            lock.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise Refusal("another HWP/1 worker service is using the same state") from exc
        try:
            yield lock
        finally:
            if os.name == "nt":
                import msvcrt
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def supervise(config: dict, stop: threading.Event, interval: float = 15,
              report=None) -> int:
    """Restart *polling* after errors, but never retry uncertain agent execution.

    Returns after a requested stop. Network failures are logged in bounded form.
    """
    if not 5 <= interval <= 300:
        raise Refusal("worker polling interval must be between 5 and 300 seconds", 400)
    if report is None:
        def report(item):
            print(json.dumps(item, sort_keys=True), flush=True)
    with exclusive_state_lock(config["state"]):
        failures = 0
        while not stop.is_set():
            try:
                results = run_worker_once(config)
                failures = 0
                report({"event": "worker_cycle", "node": config["node_id"],
                        "results": len(results), "status": "ok"})
            except (Refusal, OSError, TimeoutError, ValueError) as exc:
                failures = min(failures + 1, 6)
                report({"event": "worker_cycle", "node": config["node_id"],
                        "status": "error", "kind": type(exc).__name__,
                        "message": str(exc)[:200]})
            delay = min(300, max(interval, interval * (2 ** min(failures, 4))))
            stop.wait(delay)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Supervised HWP/1 worker daemon")
    parser.add_argument("config", help="Local node JSON profile")
    parser.add_argument("--interval", type=float, default=15)
    args = parser.parse_args()
    conf = json.loads(Path(args.config).read_text(encoding="utf-8"))
    stopped = threading.Event()

    def shutdown(signum, frame):
        stopped.set()

    signal.signal(signal.SIGINT, shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shutdown)
    try:
        return supervise(conf, stopped, args.interval)
    except (Refusal, OSError, KeyError, ValueError) as exc:
        print("hivewire-service: " + str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
