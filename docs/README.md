# Hivewire documentation

Current source: **0.4.4, unreleased**; CLI protocol **2**, database schema **13**,
bus/message/event versions **1**. These guides target the current Hivewire source.
The [migration guide](legacy-migration.md) describes the breaking cleanup.

## Start here

| Need | Document |
|---|---|
| Purpose, installation and workflow overview | [Repository README](../README.md) |
| Select a release or install local source | [Installation](../INSTALL.md) |
| Agent instructions and command reference | [Installed skill](../skills/agents-collaboration/SKILL.md) |
| Concrete collaboration workflows | [Use cases](../usecases/README.md) |
| Local trust boundary and sensitive data | [Security policy](../SECURITY.md) |
| Architecture, enforcement and limits | [Design](design.md) |

## Contracts and operation

| Topic | Document |
|---|---|
| Goals, architecture, assignments, review and memory | [Workforce implementation](resilient-workforce.md) |
| Message envelopes, leases, acknowledgements and hooks | [Bus contract](../skills/agents-collaboration/BUS.md), [JSON Schema](../skills/agents-collaboration/BUS.schema.json) |
| Owned processes, stop policy and uncertain recovery | [Process supervision](process-supervision.md) |
| One terminal, headless workers and retained audit tabs | [Managed sessions](managed-sessions.md) |
| Integration, outcome checks and receipts | [Completion evidence](completion-evidence.md) |
| Opt-in commit-pinned source impact gate | [Code intelligence](code-intelligence.md) |
| Recorded time, tokens/cost and missing accounting | [Usage allowances](usage-budgets.md) |
| Browser dashboard, demo board, actions, access token and observation mode | [Web dashboard](web-dashboard.md) |
| Regenerating the dashboard pictures | [dashboard_demo.py](../examples/dashboard_demo.py), [dashboard_screenshots.py](../scripts/dashboard_screenshots.py), [images](images/) |
| Adapter command snapshots and readiness checks | [Agent CLIs](agents.md) |
| Reviewed liveness and identity proposals, with implementation status | [Merge liveness](proposals/merge-liveness.md), [identity, scope and offers](proposals/identity-scope-offers.md) |
| Implemented behavior and known gaps | [Edge-case register](../skills/agents-collaboration/EDGE-CASES.md) |

## Evidence and release readiness

The latest recorded broad runtime snapshot passed **425 Linux tests** and **94 selected
Windows cases** (91 passed, 3 skipped). See its [source hashes, scopes, browser
and real-terminal checks](validation/managed-sessions/report.md). These results describe that
snapshot. They do not validate subsequent code, documentation or packaging edits.
Hosted Windows/Linux/macOS acceptance and macOS execution remain unestablished
by the retained local results.

| Evidence | Scope |
|---|---|
| [Platform validation](platform-validation.md) | Reproduction commands and source-specific runtime reports |
| [Candidate status](release-candidate.md) | Publication requirements and current acceptance limits |
| [Proposal conformance](workforce-conformance.md) | Original requirements mapped to code/tests and remaining work |
| [Transition audit](transition-crash-audit.md) | Selected race/crash evidence and historical counterexamples |
| [Workforce trials](workforce-benchmark.md) | Scripted mechanics and mixed live-agent results; no productivity claim |
| [Deputy coordinator benchmark](deputy-benchmark.md) | Scripted solo/coordinator/deputy comparison, p95 triage and four fault checks; no productivity claim |
| [Managed-session validation](validation/managed-sessions/report.md) | Complete 425-case Linux run, 94 selected Windows cases and browser/terminal checks |
| [Assignment workspace startup](validation/assignment-workspace/report.md) | Live Windows viewer startup/reuse, hidden-server integration, 94 selected cases with one skip |
| [Pre-push validation](validation/github-push/report.md) | Terminal-output fix, synchronized liveness fixture, retained full-run failures and final CI acceptance scope |
| [Documentation alignment](validation/markdown-alignment/report.md) | File inventory, link/command checks and focused installer/simulation evidence |
| [Generated simulation](simulation.md) | Scripted real-Git workflow, not a live-provider benchmark |

The browser dashboard is an **unreleased source feature** included in the
manifest and newer local validation. See its [scope](design.md#optional-browser-preview),
[operation](web-dashboard.md) and [managed session checks](validation/managed-sessions/report.md).
Local checks do not establish the full required platform matrix.

## Historical material

The original proposal (removed from the tree; kept in git history)
records the starting design and acceptance targets. [CHANGELOG.md](../CHANGELOG.md)
records development and release history. Reports under `validation/` apply to
their own source hashes and runtimes. The reports for the managed-session run, the pre-push run, the
documentation alignment and the canvas check are kept with every file they cite. Raw logs, result files,
intermediate runs and the older reports that other documents still describe were removed to keep the
repository small; the prose in those documents stands as the record, and the files themselves remain in
git history (`git log --stat -- docs/validation`).
The implementation guide's verification history describes earlier stages, not
the current suite or a hosted CI pass.
