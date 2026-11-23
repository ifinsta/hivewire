# Commit-pinned code intelligence gate (experimental)

**Status:** implemented opt-in deterministic gate, not compiler-resolved, not a Greptile replacement, and not independent approval. The existing Hivewire integration and goal-outcome checks remain authoritative.

## What is actually enforced

`code_intelligence.py` reads **Git object contents** at a selected base and head commit (never uncommitted working-tree changes), uses bounded lexical import/module analysis, and produces a JSON finding list with both commit SHAs and an evidence SHA-256.

- Removing detected public Rust, Python, Go, JavaScript or TypeScript symbols is blocking.
- Changing tracked `.proto`, `.graphql`, `.gql` or `.sql` contracts is blocking for independent review, even if lexical extraction finds no public symbols.
- A trusted external policy can declare files with consumers in other repositories. Changes to these files block until those consumers have been independently assessed; the scanner **does not** contact, scan or test remote repositories.
- Changed source that cannot be safely indexed (for example a symlink, oversized blob or malformed path) blocks instead of silently passing.
- Invalid revision, non-ancestor comparison, malformed policy or unsupported scan produces exit **3** (unknown), never success.
- Exit **0** means *no blocking static findings*, not that the code works or was independently reviewed. Exit **2** means blocking findings.

## Enable in a Hivewire-managed project

Install Hivewire from a reviewed version, then configure:

```toml
[gates]
intelligence = true
commands = [["python", "-m", "unittest", "discover", "-s", "tests"]]
```

The coordinator appends the **installed** `code_intelligence.py` to the configured checks. It is run through the existing owned-process gate and verification receipt machinery. During task integration, Hivewire supplies `SWARM_INTELLIGENCE_BASE` from the **actual target commit** before rebasing and checks the rebased branch and final integration commit. For later standalone `verify` operations, the scanner defaults to `HEAD^` unless an explicit reviewed baseline is supplied.

The opt-in flag is initially **false** to preserve existing integrations and fixture repositories; enable it per project with reviewed policy. A user who wishes to require independent sign-off must also use Hivewire's existing reviewer/incident clearance and goal outcome checks.

## Trusted external consumer policy

Only a policy outside the reviewed checkout may be loaded using `--policy /trusted/policy.json`:

```json
{
  "version": 1,
  "contracts": [
    {
      "path": "backend/api/payments.proto",
      "consumers": ["ifinsta/ifinpay-plugin", "ifinsta/academy"]
    }
  ]
}
```

Provision that file under administrative control. A change author must not be able to edit the scanner, installed skill, trusted policy or gate configuration as a way to self-approve. The currently exposed Hivewire integration is an opt-in gate without an administrative policy-store attachment: consumers requiring this policy must configure an external, trusted gate invocation explicitly in addition to normal tests.

Example manual invocation from a Git checkout:

```bash
python /reviewed/install/code_intelligence.py --repo . --base BASE_SHA --head HEAD_SHA --policy /trusted/policy.json
```

Use actual Git commit SHA values. The command is cross-platform because it uses Python argv, not shell expansion.

## What this cannot yet guarantee

This is **not** a compiler-resolved interprocedural graph, semantic embeddings, AI code review, autonomous runtime/sandbox testing, signed external-review clearance, or multi-repository test execution. Its detection can miss aliases, dynamic imports, generated code, macros and language-specific export forms. It reports named consumer repositories but does not inspect their contents. Cross-repository policy rules are intentionally blocking rather than pretending remote tests passed.

The user or coordinator should resolve a blocking finding through independent review and corrections, then rerun checks on the exact final commit. Overrides, if absolutely necessary, belong in a separately authorized and auditable policy process, not in the change author's code.

## Verification

```bash
python -m unittest discover -s tests -p test_code_intelligence.py -v
python scripts/make_manifest.py --check
python -m ruff check .
python scripts/validate_candidate.py --workers 2 --output candidate-validation
```

The test module uses actual temporary Git repositories to exercise API deletion, declared consumers, changed contracts, dirty worktrees, unknown revisions, unsafe symlinks and multi-commit merge baselines. This document is a contract description, not evidence that the full test matrix has passed on a specific commit.

## Next independent phases

1. Compiler/AST-grade dependency resolution and true cross-repository versioned graph (Rust first).
2. Typed, independently signed review receipts bound to head SHA, task epoch and policy revision.
3. Sandboxed runtime tests with negative oracles and dependency-service fixtures.
4. Multi-language language-server providers, incremental indexing and schema evolution.
5. Production telemetry feedback under approved data governance and measurable false-positive/escaped-defect rates.
