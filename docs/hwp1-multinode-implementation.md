# Hivewire HWP/1 Multi-Node Workforce — architecture, proposal and implementation plan

**Status:** Experimental implementation pilot. **Branch:** `feature/hwp1-multinode-foundation`. **Scope:** four machines, one Master, local Hivewire collaborators, secure task handoff and Git integration. This is NOT a production-approved remote-management product.

## 1. Problem, outcomes and invariants

Douglas Machine (Master) coordinates Felix, Joseph and Alex over independently routed internet connections. Each worker can have a local Hivewire team and Jani instances. Master owns global assignment epochs, integration decisions and final goal closure. Workers own local execution and local SQLite/Git coordination.

The central invariants are:

1. Offline does not mean failed, completed, or terminated. Master preserves uncertainty.
2. A node's local agent exit, local review, final verification, network receipt and Master merge are separate events.
3. One authoritative Git branch is advanced only by Master after re-running configured gates on the final integrated source.
4. Node and Master communicate using mutually authenticated TLS; node IDs are bound to registered certificate fingerprints.
5. No incoming message provides an arbitrary shell command. Nodes select *locally configured* execution profiles.
6. Global assignment epochs, expiration, per-node identities and idempotent submissions fence duplicated/stale completion.
7. A worker may operate within its preapproved lease while offline, but work may not automatically acquire more authority.
8. Node data and stdout remain untrusted; all code execution on the Master must eventually use a sandboxed verification service.
9. Never run target load tests without explicit authorization, bounded rates, emergency stop and an approved window.
10. Existing single-machine Hivewire commands and manifest remain unchanged by this optional pilot.

## 2. Existing code and compatibility

Hivewire 0.4.4 is a local standard-library Python coordination engine. Its `.git/swarm` SQLite board, local inbox, local Git worktrees and process supervision assume one machine as the trust boundary. `skills/agents-collaboration/swarm_core.py` remains unchanged. Local `merge`, `verification`, task epochs, contracts and local bus receipts provide useful composition points, but do **not** authenticate off-host agents.

The experimental companion lives in `fleet/core.py` and is launched with `python -m fleet` from a source checkout. It is not added to the skill installer or asserted to be a mature distributed Hivewire feature. Worker profiles can call existing installed Hivewire CLI commands locally. The pilot verifies the final candidate commit with its configured local verification command; full machine-readable import of all Hivewire local merge/goal closure receipts is a follow-on integration gate.

Jani's JSMT/1 probe protocol is read-only telemetry with TLS authentication, not a remote-command channel. HWP/1 is distinct and requires independent authorization.

## 3. Target architecture

    +------------------------------------------------------------+
    | Douglas Master                                             |
    | HWP/1 mTLS server; node registry; durable assignment ledger|
    | submission object staging; Master gate and Git integration |
    +------------------------------+-----------------------------+
                                   |
                       Encrypted HTTPS, direct or relay
                   +---------------+--------------+
                   |               |              |
             +-----v-----+   +-----v------+  +----v------+
             | Felix     |   | Joseph     |  | Alex      |
             | Local     |   | Local      |  | Local     |
             | Hivewire  |   | Hivewire   |  | Hivewire  |
             | + Jani    |   | + Jani     |  | + Jani    |
             | + profiles|   | + profiles |  | + profiles|
             +-----------+   +------------+  +-----------+

The Master is not an HTTP traffic proxy. Distributed load generators send test traffic directly to authorized test systems; only assignment and status/evidence traverse HWP/1.

## 4. Protocol and trust boundaries

### HWP/1 pilot endpoints

| Method | Path | Intended effect |
|---|---|---|
| POST | `/v1/heartbeat` | Record boot ID, monotonic-per-boot heartbeat sequence and last-seen timestamp |
| GET | `/v1/jobs` | Return assignments visible to authenticated node |
| POST | `/v1/event` | Record `running` or `failed` for the currently leased assignment |
| POST | `/v1/submit` | Idempotently stage an immutable Git bundle and local gate evidence |
| GET | `/v1/submissions/{id}` | Reconcile receipt, including post-integration result |
| POST | `/v1/message` | Publish a bounded, typed informational cross-node message |
| GET | `/v1/inbox` | Fetch only messages addressed to authenticated node |
| POST | `/v1/ack` | Record durable receiver acknowledgement |

Transport is HTTPS over TLS 1.3 using client certificates and an independently configured CA. The server validates both chain and SHA-256 DER fingerprint against its registered nodes. Node ID in body must match the authenticated identity. Hostname identity is checked by standard TLS verification on the client.

Important: the current implementation assumes manual CA management and certificate provisioning. It lacks cert expiry policy/rotation automation, revocation distribution, fine-grained node enrollment approval and scalable relay discovery. **Do not expose to untrusted internet networks.** Use an approved private overlay and firewall allowlists for a controlled pilot.

### Assignment state and epoch

`queued -> running -> submitted -> integrated`; `failed` can be reassigned only by Master under a new epoch. Every command compares node identity and epoch. A queued/running assignment has a fixed lease. A stale node cannot generate a valid new completion against a reassigned epoch.

Master liveness observations are separate from task state: fresh <30 s = online; 30–90 s = suspect; >90 s = unreachable. Absence of heartbeat is only a suspicion of lost connectivity, not proof the remote process has stopped. Lease expiry prevents later HWP/1 effects but cannot terminate an isolated running OS process. A future local watchdog must enforce bounded execution and ensure external side effects are idempotent.

### Submission and reconciliation

1. Local worker invokes an allowlisted local profile; a profile may launch the local Hivewire collaboration CLI.
2. Local profile produces committed source; the locally configured verification command passes on this checkout.
3. Worker checks a clean Git worktree, verifies assigned base ancestry, creates a Git bundle and durable SQLite outbox record.
4. Worker sends `work.submission` (currently POST `/v1/submit`) with exact base/head hashes, proof and bundle.
5. Master binds the submission to node identity, job and epoch; stages artifact under a content-derived ID; acknowledges durable receipt.
6. If an acknowledgment is lost, worker resends the identical outbox document. An existing submission ID yields the original state. Mutating the payload under the same job is refused.
7. Master operator invokes `integrate`. It clones the authoritative Git repository into a temporary checkout, fetches the exact candidate and verifies base ancestry and allowed changed paths.
8. Master merges against the current head and runs its own configured gates on the final tree; if anything fails, global main is unchanged.
9. Master records integration intent, then pushes the verified merge into the authoritative bare repository. A concurrent ref advance is rejected by Git's fast-forward checks.
10. Integration acknowledgement is a separate `merged` state, queryable by the node; retry returns the same receipt.

Crash windows: if Git push succeeded but DB acknowledgement did not, a persisted `prepared_sha` allows reconciliation when retrying `integrate`. Uploaded artifacts remain in staging. Another Master process should not independently advance the same repository without a dedicated global merge lock and CAS fencing; the pilot uses a local lock only.

### Verification evidence model

Pilot proof includes exact commit, local run exit code, SHA-256 of combined stdout/stderr, verification command result, profile and bounded tail. If the local profile specifies `hivewire_goal`, the worker now opens the native Hivewire SQLite board read-only and refuses submission unless the named goal is closed with a passed verification receipt for the exact submitted commit. Master policy can require that claim with `requires_hivewire_goal`. A remote receipt remains an authenticated **node claim**, not Master proof; Master independently re-runs isolated gates. A comprehensive signed/native task-level proof bundle and reviewer policy digest remain follow-on release gates.

## 5. Operator setup (lab only)

Prerequisites: Python 3.11+, Git, OpenSSL or equivalent PKI tooling. Four authorized machines and a secure private network/overlay; all certificates must be provisioned locally without committing private keys. A dedicated bare Git repository on Master, not a live working tree.

### Master config example

```json
{
  "bind": "127.0.0.1",
  "port": 8443,
  "state": "/srv/hwp/master.sqlite",
  "artifact_dir": "/srv/hwp/staged",
  "repository": "/srv/hwp/project.git",
  "branch": "main",
  "ca": "/srv/hwp/pki/ca.crt",
  "cert": "/srv/hwp/pki/master.crt",
  "key": "/srv/hwp/pki/master.key",
  "profiles": {
    "browser-tests": {
      "allowed_paths": ["tests", "src"],
      "verifier": "isolated_docker",
      "docker_image": "python@sha256:<approved-64-hex-image-digest>",
      "gate_argv": [["python", "-m", "unittest", "discover", "-s", "tests"]]
    }
  }
}
```

### Worker config example

```json
{
  "node_id": "felix",
  "url": "https://master.example.internal:8443",
  "state": "/var/lib/hwp/worker.sqlite",
  "ca": "/opt/hwp/pki/ca.crt",
  "cert": "/opt/hwp/pki/felix.crt",
  "key": "/opt/hwp/pki/felix.key",
  "profiles": {
    "browser-tests": {
      "repo": "/home/felix/repositories/example",
      "run_argv": ["python", "scripts/approved-hivewire-workflow.py"],
      "verify_argv": ["python", "-m", "unittest", "discover", "-s", "tests"],
      "timeout": 300
    }
  }
}
```

The profile script is locally installed and explicitly approved; Master cannot interpolate arbitrary executable strings into it. A production worker should isolate the profile process and credentials in an OS sandbox/container with network and filesystem limits. The present pilot does not satisfy that condition.

### Commands

```sh
# On Master, from the Hivewire source checkout
python -m fleet enroll master.json felix /srv/hwp/pki/felix.crt
python -m fleet enroll master.json joseph /srv/hwp/pki/joseph.crt
python -m fleet enroll master.json alex /srv/hwp/pki/alex.crt

python -m fleet serve master.json
python -m fleet assign master.json browser-024 felix browser-tests --ttl 3600
python -m fleet nodes master.json

# On Felix, run an approved bounded iteration
python -m fleet worker-once felix.json

# On Master, after receiving the submission, record and verify exact submission
python -m fleet integrate master.json <submission-id>
```

The `worker-once` command runs one poll/execution/flush cycle. `python -m fleet.service worker.json --interval 15` provides a continuously polled worker with an exclusive lock and bounded reconnection backoff. The durable worker_runs ledger prevents a previously claimed assignment from being silently rerun after a crash. A lost process may remain uncertain; `reconcile-worker` requires explicit operator cessation attestation and retains a reconciliation record. A worker process runner bounds output capture and attempts to terminate its owned process group on timeout; it is **not** full OS isolation. Never point the pilot at a production repository or real customer data.

## 6. High-concurrency traffic testing

For a staged authorized website, Felix can run browser profiles, Joseph a protocol-level load profile and Alex diagnostic jobs. Each local Hivewire board coordinates its agents. Douglas coordinates campaign goals, workers, approved target boundaries and final code integration.

Telemetry should be handled through Jani Probe/OpenTelemetry, not by sending high-cardinality raw load events over HWP/1. The load controller must measure actual delivered iterations, target latency, transport errors, application correctness and generator resource saturation. It must enforce approved target host allowlists, rate/concurrency caps and an emergency stop independently of Master connectivity.

Real-browser capture should be scoped to dedicated test sessions. Desktop capture of a person's machine requires explicit visible consent; HWP/1 completion machinery never grants screen access.

## 7. Failure catalogue and acceptance gates

| Scenario | Required decision |
|---|---|
| Node loses network while running | Mark suspect/unreachable; never assert process stopped |
| Worker loses POST response | Retain outbox and retry identical payload |
| Duplicate upload | Return same submission ID/state |
| Modified retry payload | Refuse conflict |
| Worker sends obsolete epoch | Refuse |
| Node certificate unknown | Refuse connection/application identity |
| Submission lacks local proof | Refuse |
| Bundle SHA/candidate mismatch | Refuse |
| Changed paths outside scope | Refuse |
| Gate fails or merge conflicts | Preserve Master main |
| Master loses acknowledgement after push | Reconcile prepared commit ancestry |
| Master and worker both restart | Recover durable state; do not duplicate integration |
| Quarantine/revocation | Deny new assignments and uploads; preserve evidence |

Test source: `tests/test_fleet.py`. It covers HWP/1 liveness and expiry plus a real mTLS server/client exchange, real local Git commit/bundle, durable submission, Master gate/merge and replay idempotency when Git/OpenSSL exist. Continuous six-platform acceptance is subject to repository CI and operating-system tool availability; tests skipped by lack of prerequisites must not count as equivalent proof.

## 8. Phased implementation and exit criteria

- **Phase A — Working lab vertical slice (this branch):** registry, TLS identity, heartbeat, epoch/lease, local approved runner, durable outbox, Git bundle transfer, Master path scope/gates, Git integration and tests. **Pilot only.**
- **Phase B — Local Hivewire receipts:** import typed local bus, task merge, verification and goal receipts with exact commit/assignment/policy binding. Prove every local reviewer/gate requirement completed before submission.
- **Phase C — Security and availability:** signed enrollment and revocation, certificate rotation, OS sandbox, unprivileged service account, tenant policy, retention, quota/backpressure, reliable boot and heartbeat storage, hardened upload ingestion, rate limits, remote relay handling and long-running node watchdog.
- **Phase D — Resilient integration:** durable distributed merge lock, worker-independent staging storage, full crash-point injection, fencing and stale process recovery, master HA leader election only with tested consensus, no automatic unsafe role promotion.
- **Phase E — High-scale browser/load tests:** reproducible Playwright and k6 profiles; static allowlisted test targets, telemetry correlation, emergency stop, controlled 1k/10k/100k sessions only after proving worker capacity.
- **Phase F — UX and release:** fleet dashboard, signed binaries, installers, update/rollback, cross-platform end-to-end OS acceptance, security review and conformance documentation.

### Honest release blockers

The current implementation is not safe for arbitrary untrusted code on Douglas Machine. Master gates now default to **refusal** unless a verifier profile is configured. The `isolated_docker` backend uses a pinned image digest, no networking, a read-only checkout mount, dropped capabilities, a non-root identity and resource limits. Its security is not independently certified. The `lab_unsafe` mode is deliberately gated by `allow_unsafe_local_verifier_for_tests` and must not be used for untrusted source. Additional isolation, Docker daemon permissions and host escape testing remain necessary for production. TLS and identity provisioning remain manual. The current Master serves as a single point of failure; its SQLite connection handling, concurrency and bundle-transfer approach require hardening and load testing. Work is profile-driven rather than fully orchestrated from Hivewire's native global goal engine. Before declaring the feature completely integrated, implement and verify Phases B–F.

## 9. Product positioning

HWP/1 should become a native, evidence-centered Hivewire fleet protocol: agents coordinate locally; nodes are independently authenticated; the global Master schedules work and merges only independently verified results. Jani remains an optional agent and telemetry source, never an implicit authority grant. Once hardened, the same foundation supports distributed engineering, regression testing, traffic simulation and remote diagnostics.

## 10. Hardening progress and remaining gates (updated 2026-10-27)

### Implemented on the feature branch

- Worker `worker_runs` ledger: a restart or duplicate poll never blindly reruns a previously claimed job. Explicit recovery logs human cessation attestation; unattended replays remain refused.
- Master retains node components, individual source/status receipts, and deduplicated suspect/unreachable incidents. An offline node is *not* presumed to have stopped its OS processes.
- Certificate rotation, node revocation and active-lease expiry; credential issuance/revocation remains operator-managed, not enterprise PKI.
- Reliable typed informational messages, durable local inbox and ACK after persistence. Network messages never authorize arbitrary shell commands and are **not** yet injected into existing local Hivewire inboxes.
- Local native Hivewire goal closure verification can be enabled per worker profile; Master separately requires the claim if configured.
- Master verification defaults to refusal unless Docker isolation is configured, with a restricted lab mode for synthetic CI fixtures.
- Loopback-only operator console `python -m fleet.dashboard master.json` requires the `HWP_OPERATOR_TOKEN` environment variable (minimum 24 characters). Do not publish it on the internet.
- Linux service templates at `fleet/deploy/hivewire-master.service` and `fleet/deploy/hivewire-worker.service` are examples only; adjust users, repository mounts and ACLs before installing.
- Explicitly authorized Playwright and k6 workload samples in `fleet/workloads/`. They are opt-in test profiles, **not** proof of a 100,000-session fleet or production-browser acceptance.

### Additional operator commands

```sh
python -m fleet revoke master.json felix --reason "Revoke lost device"
python -m fleet rotate master.json felix /srv/hwp/pki/felix-new.crt
python -m fleet message master.json felix "Please inspect local verification" --kind question
python -m fleet sweep master.json

# On Felix after independently confirming uncertain execution has stopped:
python -m fleet reconcile-worker felix.json job-123 \
  --confirm-stopped --reason "Operator confirmed process tree stopped and checked preserved work"

# Run node supervision as a service; avoid overlapping invocations:
python -m fleet.service felix.json --interval 15

# Loopback viewer — token must be set securely in environment:
python -m fleet.dashboard master.json --port 8765
```

### Additional production blockers

1. **Unattended end-to-end real-machine proof** on Douglas, Felix, Joseph and Alex across distinct networks, including outage/reconnect and OS-specific process/permission behavior.
2. **Security**: PKI enrollment automation, scoped revocation distribution, certificate-expiry/rotation drills, file and network sandbox validation, strong hardware/device identity and remote audit integrity.
3. **Distributed consensus and HA**: one Master is currently authoritative; no automatic election or HA takeover. Cross-process Master ref update and SQLite durability require failure-injection under concurrent writers.
4. **Native integration**: full round-trip of per-agent Hivewire task ownership, reviewer clearance, cross-node inbox, merge event lineage and local goal closure into Master acceptance. The pilot's optional native closed-goal check is only a subset.
5. **High-volume transports**: protocol currently uses HTTPS/JSON with base64 Git bundles; future HWP/1 revisions should use chunked artifact exchange, backpressure, quotas and measurements under network loss.
6. **Media**: live screen sharing requires a separately authorized WebRTC capture pipeline and OS-native user consent; no unattended desktop capture is implemented.
7. **Traffic generation**: Playwright/k6 workload samples require an authorized staging target, synthetic test users, resource calibration and explicit campaign limits; no 100k benchmark has been run.

**Do not merge the draft PR into main solely because focused synthetic tests pass.** Keep the implementation opt-in until the relevant multi-platform CI, security gates and operator-reviewed deployment evidence are complete.
