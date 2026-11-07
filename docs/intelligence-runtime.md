# Hivewire code intelligence — operational contract

This source introduces offline Git-pinned multi-repository indexing, Cargo-resolved package dependency imports, independently isolated agent review and tests, Ed25519 review verification in Jani, a local read-only MCP bridge, and tenant-scoped regression measurements. These are components requiring trusted operational configuration; they are not yet evidence of a fully deployed production pipeline.

## Trusted inventory

Store the following configuration OUTSIDE all source worktrees; commit strings below are examples that must be replaced by real full Git SHAs.

~~~json
{
  "version": 1,
  "repositories": [
    {
      "name": "ifinsta/jani",
      "root": "/srv/source/jani",
      "commit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "contracts": [
        {"path": "backend/mcp/src/lib.rs", "consumers": ["ifinsta/hivewire"]}
      ]
    },
    {
      "name": "ifinsta/hivewire",
      "root": "/srv/source/hivewire",
      "commit": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    }
  ]
}
~~~

Read actual source with Git object IDs, not a dirty working tree:

~~~bash
python .agents/skills/agents-collaboration/hivewire_intelligence.py index --inventory /var/lib/hivewire/trust/inventory.json --output /var/lib/hivewire/evidence/catalog.json
python .agents/skills/agents-collaboration/hivewire_intelligence.py impact --catalog /var/lib/hivewire/evidence/catalog.json --repo ifinsta/jani --path backend/mcp/src/lib.rs
~~~

The catalog contains commit and file-content hashes, symbols, lexical dependency tokens, declared external consumers, optional Cargo-resolved edges, and a content integrity digest. It never stores source bodies. This truly reads multiple locally checked-out repositories, but it does not perform network cloning or fully resolve symbols across language runtimes.

Cargo-resolved edges require trusted offline Cargo metadata, generated against the correct workspace and pinned by its SHA-256, supplied in the repository inventory as cargoMetadata and cargoMetadataSha256. Metadata source revocation and proof of generation against the exact commit require an independent governed command receipt. Cargo package resolution is not function-level compiler resolution.

## Graph-directed consumer verification

The verify-impact command takes the integrity-checked catalog, selects the source owner and every direct lexical/declared consumer repository, and requires an administrator-owned verification policy covering exactly those repositories. No consumer may silently lack a test.

Example trusted policy (replace both placeholder image digests with real pinned images):

~~~json
{
  "version": 1,
  "jobs": [
    {
      "repo": "ifinsta/jani",
      "image": "registry.example/build@sha256:REPLACE_WITH_REAL_SHA256",
      "argv": ["cargo", "test", "--offline", "--workspace"],
      "timeoutSeconds": 600
    },
    {
      "repo": "ifinsta/hivewire",
      "image": "registry.example/python@sha256:REPLACE_WITH_REAL_SHA256",
      "argv": ["python", "-m", "unittest", "discover", "-s", "tests"],
      "timeoutSeconds": 600
    }
  ]
}
~~~

The plan is nonexecuting, whereas the next command performs actual Docker tests of every selected consumer repository pinned to its exact commit:

~~~bash
python .agents/skills/agents-collaboration/hivewire_intelligence.py verify-impact --inventory /var/lib/hivewire/trust/inventory.json --catalog /var/lib/hivewire/evidence/catalog.json --policy /var/lib/hivewire/trust/verification.json --repo ifinsta/jani --path backend/mcp/src/lib.rs --plan-only --output /var/lib/hivewire/evidence/consumer-plan.json

python .agents/skills/agents-collaboration/hivewire_intelligence.py verify-impact --inventory /var/lib/hivewire/trust/inventory.json --catalog /var/lib/hivewire/evidence/catalog.json --policy /var/lib/hivewire/trust/verification.json --repo ifinsta/jani --path backend/mcp/src/lib.rs --output /var/lib/hivewire/evidence/consumer-tests.json
~~~

The verifier rebuilds the graph from the trusted inventory and demands an exact catalog digest match. Missing policy jobs, outdated catalogs, unknown Docker execution, or failed test jobs cannot pass. The result includes a digest and per-repository receipts. It never grants independent human clearance, issues signatures, or performs a final merge.


## Isolated tests and independent reviewer agents

~~~bash
python .agents/skills/agents-collaboration/hivewire_intelligence.py test --inventory /var/lib/hivewire/trust/inventory.json --repo ifinsta/jani --image registry.example/build@sha256:REPLACE_WITH_REAL_SHA256 --argv '["cargo","test","--offline","--workspace"]' --output /var/lib/hivewire/evidence/test.json
~~~

The Docker image must be locally available, pinned to an actual SHA-256 digest and independently trusted. It must provide `/bin/sh` and `cp`. The runner materializes a commit-pinned Git snapshot, rejects unsafe entries, mounts the original source read-only, and copies it into a separate non-root writable **ephemeral `/work` tmpfs** so that builds and test fixtures can produce temporary artifacts without modifying the pinned snapshot. `CARGO_TARGET_DIR` is `/work/target`. The container disables networking, drops capabilities, uses non-root UID, and applies CPU/memory/process/time budgets. It requires a Linux-container-capable Docker host. There is no host execution fallback or fake success if Docker is missing or cleanup is uncertain.

The review-agent command takes the same options plus --author and --reviewer, which must differ. The reviewer image must emit JSON with headCommit, reviewer, decision (concerns or no_concerns) and findings. Each finding has path, line, quote and reason. Hivewire validates every cited quote against the exact Git blob and stores only an untrusted independent-review candidate. Even no_concerns has independentClearance=false; a model cannot approve its own change.

## Signed review clearance

Jani provides jani_mcp::review::verify, and a human-authorized offline jani mcp review-verify entrypoint. The Ed25519 signature binds tenant, task, author, separate reviewer, exact base/head commits, policy digest, multi-repository catalog digest, review digest, isolated runtime digest, regression digest, nonce and expiry. Its public-key trust comes from the operator-managed --mcp-receipt-key configuration. The command requires signed human identity, policy authorization, expected-context JSON via --mcp-arguments and receipt JSON via --mcp-response.

Jani NEVER holds production signing keys, performs the user's final commit, or grants clearance based merely on model output. Verification does not consume the nonce: deployments must maintain a durable replay register and independent evidence checks at the merge or approval boundary. A valid signature proves origin and byte integrity, not that all observations are true.

## Measured feedback

Each observation has tenant, repo, commit, observationId, source (production, staging or synthetic), and six counters: exposures, checksRun, checksPassed, findings, falsePositives and escapedDefects. Records are idempotent, tenant-scoped and stored as counts and hashed IDs, without customer content.

~~~bash
python .agents/skills/agents-collaboration/hivewire_intelligence.py feedback-record --store /var/lib/hivewire/feedback.sqlite --observation /var/lib/hivewire/evidence/observation.json
python .agents/skills/agents-collaboration/hivewire_intelligence.py regression --store /var/lib/hivewire/feedback.sqlite --tenant tenant-1 --repo ifinsta/jani --baseline BASE_COMMIT --candidate CANDIDATE_COMMIT --policy /var/lib/hivewire/trust/regression-policy.json
~~~

Trusted regression policy has minimumSamples, minimumExposures, maximumEscapeIncreasePpm, maximumFalsePositiveIncreasePpm and minimumCoveragePpm, all nonnegative integers. Unknown samples and zero denominators are not passes. Provenance from production collectors is explicitly NOT cryptographically verified by this metrics store; protect its ingestion channel.

## Local read-only MCP

~~~bash
python .agents/skills/agents-collaboration/hivewire_intelligence.py mcp --catalog /var/lib/hivewire/evidence/catalog.json --tenant tenant-1 --store /var/lib/hivewire/feedback.sqlite --policy /var/lib/hivewire/trust/regression-policy.json
~~~

The stdio-only JSON-RPC server supports initialize, ping, tools/list, tools/call and initialized notifications. It exposes only intelligence.impact and intelligence.regression. There are no mutation, execution, sign or merge MCP tools. Launch it only through Jani's existing governed broker; the local process does not authenticate arbitrary remote users by itself.

## Verification commands

~~~bash
python -m ruff check .
python scripts/make_manifest.py --check
python -m unittest discover -s tests -p test_intelligence_runtime.py -v
python scripts/validate_candidate.py --workers 2 --output candidate-validation
~~~

Focused tests use real Git fixtures for multiple repositories and check source provenance, metadata tampering, path rejection, isolated command construction, review citation attribution, idempotency, thresholds and MCP mutation denial.

## Operational work not falsely represented as complete

A trusted repository sync service, independent Cargo metadata signing, reviewed agent container images, a production image registry, actual Docker fleet tests, KMS-based signature issuance, durable nonce consumption, Jani broker registration for this stdio service, authenticated live production telemetry collectors and cross-repository end-to-end merge trials still require deployment and verification.
