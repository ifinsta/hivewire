# Agent CLIs

Which coding agents can join a pool, how readiness is checked, and how they are
started headless. Any agent that can run a shell command can join by hand
(`hivewire join`). Adapters define CLI-specific probing, headless/interactive startup and optional
native notification hooks.

| Kind | Headless command | Sign-in check | Unattended flag | Recorded command-line check |
|---|---|---|---|---|
| `claude` (Claude Code) | `claude -p "<prompt>"` | `claude auth status` | `--dangerously-skip-permissions` | help checked at 2.1.280 |
| `opencode` | `opencode run "<prompt>"` | `opencode auth list` | `--auto` | help checked at 2.0.32 |
| `qodercli` (Qoder) | `qodercli -p "<prompt>" -w <dir>` (interactive: `-i "<prompt>"`) | `qodercli status` | `--dangerously-skip-permissions` | help checked at 1.1.73 |
| `codex` | `codex exec "<prompt>" -C <dir> --add-dir <git dir>` | `codex login status` | `--full-auto` | help checked at 0.104.0 |
| `gemini` | `gemini -p "<prompt>"` | none (reported as unknown) | `--approval-mode yolo` | help checked at 0.53.1 |
| `kilocode` (Kilo Code) | `kilocode --auto --nosplash --workspace <dir> "<prompt>"` | none (`kilocode auth` starts a login, so it is never run; reported as unknown) | `--yolo` | help checked at 0.18.0 |
| `jani` ([Jani](https://github.com/ifinsta/jani)) | `jani --print "<prompt>" --repo <worktree> --max-steps 8` | none (unknown) | refused | **experimental, unverified** |
| `deepseek` (deepseek-tui) | `deepseek exec "<prompt>"` | `deepseek auth status` | none | **unverified**: `exec` does not document its arguments |

These are recorded help/option checks for the listed versions, not claims about
today's latest CLI or successful authenticated task execution. A successful live
probe and supervised trial are separate evidence. Keep unavailable accounts,
timeouts and missing usage visible; see the recorded workforce trials.

CLIs update themselves. Qoder went from 0.1.46 to 1.1.73 during one session and
dropped two options its adapter passed. Before `spawn`, `launch` or `probe` uses
a CLI, the tool checks every option it is about to pass against that CLI's own
help. A missing option stops the launch and is named; `probe` reports it as
`CHANGED`. Re-verify with `hivewire probe --live` before relying on an adapter.

**DeepSeek models through opencode** are a configurable adapter route; actual
availability requires the chosen provider/model and account. opencode accepts
a DeepSeek API key, so:

```bash
hivewire spawn opencode --name ds1 --task T1 --model deepseek/deepseek-chat
```

**Kilo Code** has its own parallel mode (`--parallel`), which creates a git branch
of its own. The adapter never uses it: the claim has already made the task's
worktree and branch. A model is chosen with `-mo`, and a provider with `kilocode
config` beforehand.

## Experimental Jani observer

The built-in `jani` adapter is an opt-in launch experiment, not a full
collaboration integration. It uses the assigned Git worktree and a bounded
print-mode invocation. It does not grant write or command authority, change
Jani's policy, auto-approve operations, or turn model output into completion
evidence. There is no interactive adapter or native inbox/interrupt bridge.

```bash
hivewire probe jani
hivewire spawn jani --name jani-observer --task T1 --force --dry-run
```

Without `--force`, the unverified adapter refuses to spawn; `--unattended`
is refused even if the repository allows unattended agents elsewhere. Use only
a disposable synthetic test repository with an operator-reviewed provider and
settings. `--force` permits testing the adapter, not bypassing Jani's policy.
A real spawn may make model requests and claims a Hivewire task; do not expect
the observer to finish the task alone. An operator must check the evidence,
handle any retained assignment, and explicitly decide its disposition.

This slice does not authenticate agents, inject messages into a running Jani
session, translate a Hivewire task into a signed Jani work contract, or attest
provider cost. The local machine and configuration remain trusted. Keep
`verified = false` until a real binary/provider trial and independent gate
have been recorded.

## Readiness

```bash
hivewire probe                 # installed? version? signed in? (no requests sent)
hivewire probe --live          # also asks each installed CLI to reply READY (one request each)
hivewire agents                # agents already in the pool: state, OS, task, last heartbeat
hivewire send ds1 "free for the API task?" --kind question --as human # acknowledge via bus; reply with --reply-to ID
```

`probe` runs each CLI's own sign-in status command and reports only its exit
code. The output usually contains an account name or email, so it is never
printed or stored.

## Spawning

At assignment startup, the coordinator runs `hivewire workspace` to open or reuse
the managed terminal and browser dashboard. Spawned executions appear as tabs
automatically; opening the workspace does not start any provider agents.

Prefer headless `spawn` plus `hivewire sessions` for workers whose output must stay
auditable. The manager uses one existing terminal with per-execution tabs, leaves
workers running when detached, and retains failed/exited sessions. The browser
Sessions page shows the same captures. See [managed sessions](managed-sessions.md).

```bash
hivewire spawn <kind> --name <name> --task <task> [--model <model>] [--unattended] [--dry-run]
```

1. The service joins `<name>` and claims `<task>` (or the best task) for it, which
   creates the worktree.
2. It writes `.swarm-brief.md` in the worktree: identity, task, the absolute tool
   command, the skill path, and the detected environment with its rules.
3. It creates an owned supervisor and launches the CLI in the worktree. Source
   versions described here retain
   bounded worker output in `.git/swarm/logs/<execution-id>.log` and engine
   startup diagnostics in `<execution-id>.supervisor.log`. `execution show ID`
   exposes observed byte counts, capture status and hash; the process id is
   status information, never termination authority.
4. Observed group exit settles the owner's state. Untouched work may return to
   the pool; started work is retained for recovery. Unknown supervision blocks
   blind restart/cleanup and can require human reconciliation before takeover.

The prompt on the command line is always one safe sentence that points at the
brief. Windows `.cmd` shims pass arguments through `cmd.exe`, and anything more
could be reinterpreted.

**Unattended mode** adds each CLI's permission-bypass flag. A headless agent
without it often stops at the first tool use, because there is nobody to approve
it. With it, the agent can run any command its CLI allows. That is why
`--unattended` also needs `[spawn] allow_unattended = true` in the project
config: a person decides, once, per repository.

## Automatic inbox relay and native hooks (unreleased)

With `[bus] enabled = true`, headless `spawn` and foreground `launch` start
an automatic standard-bus relay in their owning supervisor. The worker inherits `SWARM_BUS_INBOX`, pointing to
an atomic JSON snapshot of leased deliveries and current stop controls. Committed
messages/control changes wake the relay over loopback; a 15-second fallback
recovers missed hints. The relay does the local reads, so the model need not run
an inbox command continuously. Handle deliveries and acknowledge their receipts
after durable processing. Plain `send` now defaults to reliable typed status.
Historical read-once rows remain inspectable without fabricated receipts.

Configure a native bridge as an argv array, globally under `[bus]` or for one
adapter in its existing table:

```toml
[adapters.mycli]
message_hook = ["python", "/absolute/path/to/native_bridge.py"]
interrupt_hook = ["python", "/absolute/path/to/native_bridge.py"]
```

The bridge reads the file named by `SWARM_BUS_EVENT_PATH`. Its JSON includes
deliveries with receipts, `blocked`, `reason`, current task/epoch and controls.
The same interrupt hook receives permission/control transitions, including resume.
Use the CLI's supported channel to forward context; hook exit 0 means notification
only. Hooks run as bounded owned commands, without shell interpolation. Defaults
and failure handling are in the [bus contract](../skills/agents-collaboration/BUS.md#automatic-adapter-relay-and-hooks).

At native tool boundaries, a wrapper can invoke
`hivewire bus hook --stage before-tool --enforce --json`. It returns current messages
and control state and exits 2 when blocked. Map its result to that CLI's native
hook protocol; allow recovery/diagnostic coordination commands so a paused worker
can checkpoint, acknowledge and hand off. The engine does not modify vendor-global
hook settings. A native bridge is needed to inject context into a CLI's active
session; the common relay cannot preempt an unsupported model request.

Externally started CLIs can run `hivewire bus listen --as NAME --json`
as their relay. Its optional `--once` is an inspection/notification pass that
releases receipts before returning. Supervised launch paths manage lifetime and
lease release automatically. No relay signal authorizes killing a registry PID.

## Adding a CLI

```toml
[adapters.mycli]
version = ["mycli", "--version"]
auth = ["mycli", "whoami"]                 # [] if there is no status command
run = ["mycli", "--print", "{prompt}"]     # placeholders: {prompt} {cwd} {git_common} {max_turns} {model}
model = ["--model", "{model}"]
unattended = ["--yes"]
verified = false                           # true only after a real probe --live and spawn
```

If the CLI sandboxes file access to its working directory, give it the git common
directory too (as the `codex` adapter does). Commits in a worktree write objects
there.
