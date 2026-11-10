# spark-connect.el — Emacs client for a Spark Connect companion server (with optional Databricks)

- Status: **Proposed** (draft 2; supersedes draft 1, kept as
  `spark-connect-emacs.draft1.md`)
- Date: 2026-11-09
- Author: architect agent, revised after review with the operator

**Changes since draft 1.** The design is now client–server. A Python
companion process (the _server_) runs the user's code, owns the Spark
session and all race-sensitive state. Emacs (the _client_) only sends
requests and renders notifications. They talk JSON-RPC 2.0 over the
server's stdio, using the built-in `jsonrpc.el`. Effects:

- The python.el comint REPL is no longer in the execution path. There is
  one input path into the Python process, so every submission is tracked
  and the SIGINT/SIGUSR1 split, "untracked" input and prompt-scraping
  bootstrap disappear.
- The local socket, one-time tokens, the 0700 directory and the Windows
  TCP fallback are gone. The transport is the child's stdio.
- Explicit policies (section 2.2) replace the multi-executor registry.
- A JSON performance budget (section 4.7) is based on measurements of
  Emacs 30.2 and a read of `jsonrpc.el` 1.0.25.
- Emacs 30.1 is the minimum (C6, section 2.3).
- Section 1.3, the compatibility rules and the parity matrix are now
  based on the source of the official VS Code extension v2.23.1.

## 1. Context and problem

### 1.1 Current state of the repository

The project is **greenfield**. The repository holds only multi-agent
tooling state (`.mas/`, `.agent-shell/`, `.claude-container/`,
`.codex-container/`) and this document. Section 6 lists the files this
design creates.

Local tooling observed in the dev container: GNU Emacs 30.2 (built-in JSON
parser, `jsonrpc.el` 1.0.25) and Python 3 without `pyspark`. Python
integration tests need a venv with `pyspark` (section 8).

### 1.2 What the operator wants

> An Emacs plugin for Spark Connect with optional Databricks Connect
> support, inspired by the Databricks VS Code extension, aiming at parity
> with it. Databricks features (and the SDK) stay optional; the core
> works with _any_ Spark Connect server.

User stories:

1. **Try PySpark code on a real cluster from a buffer**: send a region,
   function, cell or buffer, or run a file. Get the output back in Emacs.
2. **See what is going on**: connection state and running work in the
   mode line, stage/task progress, logs, cancel.
3. **Databricks extras (optional)**: pick an auth profile and a cluster
   (or serverless), start the cluster, list job runs and watch their
   status, deploy and run Asset Bundles, open the Spark UI or run page.
4. **Work with any Spark Connect server**: a local
   `start-connect-server.sh`, Spark on Kubernetes, EMR, or another vendor.
   The Databricks pieces must not be installed or loaded for this case.

### 1.3 Reference: the official Databricks VS Code extension

Read from the source of `databricks/databricks-vscode` v2.23.1 (shallow
clone, 2026-11-09). Paths are relative to `packages/databricks-vscode/`.
It depends on `ms-python.python`, `ms-python.debugpy`,
`ms-toolsai.jupyter` and `redhat.vscode-yaml`, and leaves Python
execution, debugging and Jupyter to them.

| Area | What it does | How |
| --- | --- | --- |
| Run with DB Connect | Runs the file in the active terminal | Sends one line, `<python> resources/python/dbconnect-bootstrap.py <file>`. The bootstrap loads `.databricks/.databricks.env`, creates `DatabricksSession`, injects `spark`, `sql`, `udf`, `dbutils` (from `databricks.sdk.runtime`), `getArgument`, fixes `sys.path`, then `runpy.run_path(..., "__main__")` (`src/run/RunCommands.ts`) |
| Debug with DB Connect | debugpy launch | A debug-configuration provider swaps `program` for the bootstrap and merges the env file (`src/run/DatabricksDebugConfigurationProvider.ts`) |
| Connection env | `.databricks/.databricks.env`, regenerated on every state change | `DATABRICKS_HOST`, `DATABRICKS_AUTH_TYPE=metadata-service` plus `DATABRICKS_METADATA_SERVICE_URL` pointing at an **in-process HTTP token server** (15 s tokens; `src/configuration/auth/MetadataService.ts`), `DATABRICKS_CLUSTER_ID` or `DATABRICKS_SERVERLESS_COMPUTE_ID=auto`, `SPARK_CONNECT_USER_AGENT`, `SPARK_CONNECT_PROGRESS_BAR_ENABLED`, proxies, plus the user's `python.envFile`. A raw token (`SPARK_REMOTE` with `token=`) is written only for PAT auth |
| Notebooks | Jupyter via ms-toolsai.jupyter | Installs `00-databricks-init-<hash>.py` into the IPython startup dir. It creates `spark`/`dbutils`/`displayHTML`, adds `%fs`, translates Databricks `.py` notebook syntax (`# COMMAND ----------`, `# MAGIC`, `%sql` → `spark.sql`, `%md`, `%sh`, `%run`), renders DataFrames as HTML (`limit 20`), and registers an ipywidgets **progress bar** |
| Progress / cancel | Minimal | `registerProgressHandler` is used **only** in the Jupyter init script. Terminal runs rely on PySpark's own console progress bar. **No** `addTag`/`interruptTag`/`interruptAll` anywhere; cancel = kill the terminal or debug session |
| Env setup | Two flows | (a) uv-native: the CLI command `databricks environments setup-local --cluster-id X \| --serverless-version N --output json` creates `.venv` with uv, merges runtime constraints into `pyproject.toml` and installs DB Connect; drift detection when compute changes. (b) Legacy checklist plus `pip`/`uv pip` install of `databricks-connect==<DBR major.minor>.*` after uninstalling `pyspark` |
| Compatibility rules | Enforced in `src/language/EnvironmentDependenciesVerifier.ts` | Cluster DBR ≥ 13 with UC access mode (`SINGLE_USER`, `SHARED`, `USER_ISOLATION`); workspace has UC; Python ≥ 3.10 (serverless: ≥ 3.12 for DB Connect 16+, else ≥ 3.11); DB Connect ≥ 13 (serverless ≥ 15.4), not newer than DBR; Python/DBR mismatch is a warning (DBR 13–14 → 3.10, 15 → 3.11, 16+ → 3.12) |
| Databricks CLI | **Bundled** (pinned version, sha256-checked) | Used for `bundle validate/summary/deploy/destroy/run/sync/init`, `bundle sync --watch --output json`, `auth profiles`, `auth login`, `environments setup-local`, `ssh connect`, `aitools` (`src/cli/CliWrapper.ts`) |
| JS SDK | In-process | Clusters (polled every 10 s), command execution, jobs submit/getRun/export/cancel, pipelines, Unity Catalog, workspace FS, current user |
| Auth | Profiles, OAuth U2M via `databricks-cli` auth type, Azure CLI, PAT | Login wizard; `databricks auth login` on failure; `az login` for Azure |
| Upload and run | Classic clusters only, dev targets | `bundle sync`, then the 1.2 command-execution API with a `bootstrap.py` wrapper; output in the Debug Console, remote traceback paths mapped to local files (`src/run/DatabricksRuntime.ts`) |
| Run as workflow | One-off `jobs.submit` | Wrapper file or notebook cell written to the workspace; `spark_python_task` or `notebook_task`; serverless via `environments`; polls `getRun` every 3 s; result webview |
| Bundles | Resource explorer from `bundle summary`; validate on YAML change | `bundle run` in a pseudo-terminal; run id parsed from stdout; jobs polled every 3 s, pipelines every 5 s with incremental events; pipeline ERROR/WARN events with source locations become **diagnostics** on local files; table-selective refresh |
| Sync | VS Code task | `bundle sync --watch --output json`, parsed into sync states |
| Explorers | Unity Catalog tree (catalogs → schemas → tables/volumes/functions/models), Workspace FS provider | SDK `list`/`get` calls |
| Autocomplete | Typing stubs | Appends `__builtins__.pyi` with typed `spark`, `dbutils`, `display`, … |
| Newer | SSH tunnel to clusters/serverless (`databricks ssh connect --ide=vscode`), AI-agent skills installer (`databricks aitools`), experimental Unity AI Gateway chat model | CLI wrappers; chat provider streams SSE |
| UI state | `CONNECTED \| CONNECTING \| DISCONNECTED`; one attached cluster or serverless per bundle target | One status-bar item ("Databricks Connect enabled/disabled"); views: Configuration, Compute, Bundle Resources, Bundle Variables, Workspace FS, Unity Catalog, SSH |

**Lessons taken into this design:**

1. **Execution is delegated, and status is thin.** VS Code has no
   out-of-band progress channel and no tag-based cancel for script runs.
   Section 4.6 therefore goes _beyond_ parity. That justifies it as a
   differentiator, but it also means a simpler cancel would not be a
   parity gap (Q10).
2. **The CLI is the control plane, including env provisioning.**
   `databricks environments setup-local` does the venv and DB Connect
   install with uv. Section 4.12 uses it instead of printing `pip`
   commands.
3. **Auth without tokens in the env.** The metadata-service indirection
   keeps long-lived tokens out of the Python process. Section 4.8 keeps
   profile-based auth (`DATABRICKS_CONFIG_PROFILE`, the Python SDK reads
   the profile and calls the CLI for U2M), which also avoids tokens in the
   env. A metadata-service server in Emacs is an option, not a P1 need.
4. **Databricks notebook source format matters.** `# COMMAND ----------`
   cells and `# MAGIC %sql` are handled in the IPython layer. Our server
   applies the same translation for `send-cell` and `ob-spark-connect`.
5. **Globals users expect:** `spark`, `sql`, `udf`, `dbutils`,
   `getArgument`, `displayHTML`. The server injects the same set
   (`dbutils` only for Databricks profiles).
6. **Pipeline diagnostics and source-mapped tracebacks** map well onto
   Flymake and `compilation-mode` in Emacs.


## 2. Constraints, forces and policies

### 2.1 Constraints

- **C1. No gRPC in Emacs.** Every Spark Connect call goes through a real
  client library. PySpark is the one the target users write.
- **C2. `databricks-connect` and `pyspark` cannot share an
  environment** (`pyspark-client` also conflicts with both: all three
  provide the `pyspark` module). The plugin never changes an
  **existing** venv on its own and works with whichever flavour is there.
  It may **create** a new project venv for a profile after explicit
  confirmation (section 4.14).
- **C3. Client/server compatibility is a rule set** (section 4.9). The
  plugin probes and reports errors and warnings separately.
- **C4. Progress is version-dependent.** PySpark ≥ 4.0 has
  `SparkSession.registerProgressHandler`. Older clients and some
  Databricks Connect versions may not. The UI degrades to
  submission-level status.
- **C5. Databricks optional, for real.** The core does not `require`
  Databricks code, does not need the `databricks` CLI and does not import
  `databricks.*` unless a Databricks profile is in use.
- **C6. Emacs ≥ 30.1 only** (`Package-Requires: ((emacs "30.1"))`).
  Emacs 30 always has the built-in JSON parser and serializer (no
  libjansson, no `json.el` fallback) and bundles a `jsonrpc.el` with
  `:events-buffer-config`. Emacs 29 is not supported: on 29 the JSON speed
  depends on how Emacs was built, and an older `jsonrpc.el` would need a
  GNU ELPA pin. No other external Elisp packages in the core. Section
  2.3 lists the Emacs 30 features the design relies on.
- **C7. Async everywhere.** No synchronous `jsonrpc-request`, no
  `call-process` on hot paths.
- **C8. Secrets** never go in customization files, command lines or
  JSON-RPC traffic. They travel only in the server's environment.

### 2.2 Policies

These are deliberate product restrictions. Each one removes a class of
problems found in review of draft 1.

| #   | Policy                                                                                                                                               | Removes                                                                                         |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| P-1 | **At most one interactive server per Emacs.** Run-file processes (section 4.10) are separate, short-lived and each is one tracked job.               | Executor registry, fingerprints, generations, naming collisions, "which target am I on"         |
| P-2 | **One Spark operation stream per server.** Catalog and preview requests are jobs in the same queue as code and run only between submissions.          | Concurrent client use from helper threads (draft-1 R2), catalog progress polluting submissions  |
| P-3 | **Profiles are defined only in the user's init.** `.dir-locals.el` may only _name_ a profile, and the name variable is safe only for known profiles. | Repository-controlled interpreter paths, env and endpoints                                      |
| P-4 | **Local servers only (P0–P2).** A remote `default-directory` is refused with a clear message.                                                        | TRAMP process semantics                                                                         |
| P-5 | **Any profile change restarts the server**, after confirmation if a job is running. No hot re-targeting.                                             | Stale credentials after rotation, mixed-target state                                            |
| P-6 | **Only the attached session is tracked.** The server detects other sessions and reports them; it does not chase them.                                | Silent "cancel did nothing" when user code builds its own session                               |

Users who need two targets at once run two Emacs instances.

### 2.3 Emacs 30 baseline

The core uses only what Emacs 30.1 ships:

| Emacs 30 feature                                            | Used for                                                                                     |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Built-in `json-parse-buffer` / `json-serialize` (no jansson) | All protocol traffic and Databricks CLI output (section 4.7)                                 |
| Bundled `jsonrpc.el` with `:events-buffer-config`           | Transport (section 3.2); events buffer off by default                                        |
| `jsonrpc-default-request-timeout` as a defcustom            | Users can tune request timeouts; our long work never uses request timeouts (jobs are notifications) |
| `read-process-output-max` default 65536                     | Large reads per filter call without user tuning                                              |
| `tabulated-list-groups`                                     | Jobs buffer: Spark operations grouped under their job                                        |
| `mode-line-format-right-align`                              | Right-aligned `SC:` lighter and progress                                                     |
| `completion-preview-mode`                                   | Optional inline completion from the `complete` request in source and REPL buffers            |
| `python-ts-mode`, `project.el`, `transient`, `comint`       | Source-buffer mode, project root, dispatch menu, REPL buffer                                 |
| Native compilation on by default                            | No design dependency; byte-compiled code must also be fast enough                            |

_To verify:_ that Emacs 30.1 (not only 30.2) bundles `jsonrpc.el` 1.0.25
or another release with `:events-buffer-config`.

## 3. Options considered

### 3.1 Where user code executes

| Option                                      | Description                                                                  | Verdict                                                                                                                                                                                                    |
| ------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A. EIN as the core                          | Notebook UI, Jupyter kernel                                                  | Rejected: notebook-centric, uncertain maintenance, needs a Jupyter server, status must be scraped.                                                                                                         |
| B. emacs-jupyter as the core                | ZMQ kernels                                                                  | Rejected as core: native `emacs-zmq` module, needs `ipykernel` in the user's venv. Possible optional adapter later.                                                                                        |
| C. python.el comint + side channel (draft 1)| `run-python` with a helper that opens a socket back to Emacs                 | Rejected: two input paths into one process (our envelope and anything typed at the prompt) force a SIGINT/SIGUSR1 protocol, "untracked" work, prompt-detection bootstrap and a socket with tokens.        |
| **D. Companion server is the executor**     | Our Python process `exec`s code on its main thread; Emacs speaks JSON-RPC    | **Chosen.** One input path, one state machine in one language, testable with pytest alone.                                                                                                                 |
| E. Native Elisp Spark Connect client        | gRPC from Emacs                                                              | Infeasible (C1).                                                                                                                                                                                           |

Draft 1 rejected D as "a worse Jupyter kernel". That overstated the
cost. The server does not reimplement Jupyter's display protocol, only
`execute`, output, `input()`, interrupt and completion. That is a small
surface, and in return the race-sensitive logic lives in one place.

What D gives up, and how each loss is handled:

| Lost                                   | Replacement                                                                                                    |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Stock python.el REPL and its `C-c C-*` | `spark-connect-repl-mode` (comint-derived) whose input goes through `execute`; `spark-connect-python-mode` keys |
| python.el REPL completion              | `complete` request backed by `rlcompleter` (stdlib); `completion-at-point` in source and REPL buffers          |
| `ob-python :session`                   | `ob-spark-connect` (P0): one `execute` per block                                                               |
| Direct stdin for `input()` / `pdb`     | stdin proxy: server→client `input` request, answered from the REPL buffer (section 4.5)                       |

### 3.2 Transport

| Option                                       | Pros                                                                                       | Cons                                                                                                     |
| -------------------------------------------- | ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| Local socket + NDJSON (draft 1)              | Separate from user stdio                                                                   | Tokens, socket permissions, Windows fallback, reconnect semantics                                        |
| **stdio + JSON-RPC 2.0 (`jsonrpc.el`)**      | Built-in client with framing, request ids, timeouts; Emacs owns the process; EOF = dead    | User code must not write to the protocol fds: handled by fd redirection (section 4.4)                   |
| stdio + custom NDJSON filter in Elisp        | Avoids `jsonrpc.el`'s per-message timer and logging overhead                               | Reimplements request/response bookkeeping                                                                |

**Chosen:** `jsonrpc.el`, behind a thin `spark-connect-rpc.el` wrapper so
the transport can be swapped for a custom NDJSON filter if section 4.7
budgets are not met in practice.

### 3.3 Databricks control plane

Unchanged from draft 1: the **Databricks CLI** (`databricks … -o json`) as
an async subprocess. It implements unified auth and all REST commands
plus `bundle`, as the VS Code extension does. Rejected: the Python
`databricks-sdk` (needs a second venv under C2) and raw REST from Elisp
(reimplements unified auth). A REST fast path via `databricks auth token`
is a deferred optimisation.

## 4. Decision

### 4.1 Architecture

```
┌─────────────────────────────── Emacs (client) ───────────────────────────────┐
│ spark-connect (core, no Databricks deps)                                     │
│  profiles ─ env resolver ─ rpc wrapper ─ state model ─ UI                     │
│     UI: REPL buffer, mode line, *spark-connect-jobs*, log, transient,         │
│         catalog browser, DataFrame preview, ob-spark-connect                 │
│                                                                              │
│ spark-connect-databricks (optional, loaded only when enabled)                │
│  auth ─ clusters ─ jobs/runs ─ bundles ──────────── `databricks` CLI (async)  │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │ JSON-RPC 2.0, Content-Length framing, over the child's stdin/stdout
               ▼
┌────────── python -m spark_connect_emacs serve  (user's venv, stdlib-only pkg) ┐
│ main thread   : job loop — exec user code, catalog, previews (one at a time)  │
│ reader thread : parses requests, enqueues jobs, handles interrupt/ping       │
│ writer thread : the only writer of the protocol fd; batching, flow control   │
│ pump threads  : drain redirected fd 1/2 → `output` notifications              │
│ connect thread: creates/recreates the SparkSession off the job loop          │
└──────────────┬────────────────────────────────────────────────────────────────┘
               │ gRPC (pyspark / pyspark-client / databricks-connect)
               ▼
   Spark Connect server (OSS / any vendor)  or  Databricks cluster / serverless
```

### 4.2 Client (Emacs) components

**Profiles** (`spark-connect-profile.el`). A profile is a plist in the
defcustom `spark-connect-profiles`: `:name`, `:kind`
(`spark-connect` | `databricks`), `:python` (interpreter or venv dir;
default: `.venv` under the `project.el` root, then
`python-shell-interpreter`), `:env` (extra non-connection variables),
`:cwd` (`buffer` | `project`), `:confirm` (ask on every submission),
`:spark-ui-url`. OSS: `:remote` (`sc://host:port/;use_ssl=true;…`), token
from `auth-source` by host. Databricks: `:databricks-profile` and exactly
one of `:cluster-id` / `:serverless t`. Per P-3, a project selects a
profile with the dir-local `spark-connect-profile-name`.

**Server lifecycle** (`spark-connect-server.el`). One server per Emacs
(P-1). `spark-connect-start` resolves the env (section 4.8), starts
`python -m spark_connect_emacs serve` with `make-process`
(`:connection-type 'pipe`, `:coding 'binary`, a separate `:stderr`
buffer) and wraps it in a `jsonrpc-process-connection`. Selecting another
profile restarts it (P-5). Process exit marks every open job `unknown`.
`kill-emacs-hook` sends `shutdown`, then kills after 2 s.

**Source-buffer commands** (`spark-connect-python-mode`, a minor mode
for `python-mode`/`python-ts-mode`): `spark-connect-send-{region,defun,
cell,buffer}` and `spark-connect-run-file`, on the familiar keys
(`C-c C-r`, `C-M-x`, `C-c C-c`, …), plus `completion-at-point` through
`complete`. Region extraction reuses `python-shell-buffer-substring`
(dedent, `if __name__` handling). The request carries `origin` (file,
first line) for tracebacks.

**REPL buffer** (`spark-connect-repl-mode`, derived from `comint-mode`,
with no process of its own). `comint-input-sender` turns each input into
an `execute` request with `mode: "interactive"`. The server answers
`incomplete` for unfinished statements (via `codeop`), and the buffer
shows a continuation prompt. `C-c C-c` sends `interrupt` for the
running job. Output notifications are inserted before the prompt. While
the server waits on `input()`, the next line the user enters answers
that request.

**State model** (`spark-connect-state.el`). A single struct updated
only by notifications: server state, session state and target (from the
server, never from the selected profile), job table, per-op progress,
counters. All UI reads this model on a throttled timer (≤ 5 Hz). No
notification handler touches the UI directly.

### 4.3 Server (Python) components

Package `spark_connect_emacs` shipped under `python/` in the Elisp
package. It is **stdlib-only** and added via `PYTHONPATH`, never
pip-installed into the user's venv by the plugin. An optional
`pip install spark-connect-emacs` is harmless because it has no
dependencies. Supported Python: the versions supported by the oldest
client (PySpark 3.5 → 3.8+).

Entry points:

- `python -m spark_connect_emacs probe`: JSON report on stdout and exit.
  Python version, installed `pyspark` / `pyspark-client` /
  `databricks-connect` versions via `importlib.metadata` (no imports),
  conflicts, capability flags. Used by `spark-connect-doctor`.
- `python -m spark_connect_emacs serve`: the JSON-RPC server (sections
  4.4–4.6).
- `python -m spark_connect_emacs run [--module] [--] TARGET ARGS…`: the
  run-file mode, the same server protocol with one implicit job
  (section 4.10).
- `python -m spark_connect_emacs interrupt --tag … [--session-id …]`:
  best-effort remote interrupt used after a hard kill (Q2a).

### 4.4 Stdio discipline

At startup, before anything else:

1. `proto_in = os.dup(0)` and `proto_out = os.dup(1)`. These descriptors
   are non-inheritable by default since Python 3.4, so child processes
   started by user code never see them.
2. fd 0 is replaced by `/dev/null` (`os.devnull`). `sys.stdin` becomes a
   proxy whose `readline` sends an `input` request to Emacs and blocks
   for the reply. `input()`, `pdb` and `breakpoint()` go through
   `sys.stdin`, so they work from the REPL buffer.
3. fd 1 and fd 2 are each replaced with a pipe drained by a pump thread.
   C extensions, gRPC core logging and subprocesses all write to these
   descriptors, so their output is captured too. `sys.stdout` and
   `sys.stderr` are rebuilt on the new fds with line buffering and
   `errors="backslashreplace"`.
4. Only the writer thread writes to `proto_out`.

If the server crashes before step 3, the traceback reaches Emacs's
`:stderr` buffer, which the client shows on abnormal exit.

### 4.5 Protocol (JSON-RPC 2.0, `protocol: 1`)

Field names are camelCase. Every notification is a batch where noted.

**Client → server requests**

| Method               | Params                                                       | Result                                                                                  |
| -------------------- | ------------------------------------------------------------ | --------------------------------------------------------------------------------------- |
| `initialize`         | `protocol`, `clientVersion`, `kind`, `limits`                 | `serverVersion`, `protocol`, `python`, `client{flavour, version}`, `caps`; returns at once, the connect runs in the background |
| `execute`            | `code` or `path`, `origin{file, line}`, `mode` (`interactive`\|`exec`\|`cell`), `confirmToken?` | `{req, position}` on acceptance, or `{incomplete: true}`; completion arrives as `req/end` |
| `interrupt`          | `req`                                                        | `ack` \| `stale` \| `queued-removed`                                                    |
| `interruptAll`       | —                                                            | `{ok, error?}` (user-confirmed in Emacs)                                                |
| `catalog`            | `query` (`catalogs` \| `schemas` \| `tables` \| `columns`, args) | `{req}`; data arrives in `req/end.result`                                          |
| `preview`            | `expr`, `limit`                                              | `{req}`; table arrives in `req/end.result`                                              |
| `complete`           | `text`, `cursor`                                             | `{candidates}`; answered by the reader thread with `rlcompleter` on a namespace snapshot, never blocks on the job loop |
| `session/reconnect`  | —                                                            | `{ok}`; state changes follow as notifications                                           |
| `ping`               | —                                                            | `{time}`                                                                                |
| `shutdown`           | —                                                            | `{ok}`, then the server exits                                                           |

**Server → client request**

| Method  | Params             | Result                    |
| ------- | ------------------ | ------------------------- |
| `input` | `req`, `prompt`    | `{line}` or `{eof: true}` |

**Server → client notifications**

| Method            | Params                                                                                       |
| ----------------- | -------------------------------------------------------------------------------------------- |
| `session/state`   | `state` (`connecting`\|`connected`\|`failed`\|`stopped`), `sessionId?`, `target{host, compute}?`, `error?` |
| `session/changed` | `req`, `kind` (`stopped`\|`foreign`), `detail`                                              |
| `req/start`       | `req`, `kind` (`code`\|`catalog`\|`preview`), `origin`                                      |
| `req/end`         | `req`, `status` (`ok`\|`error`\|`interrupted`\|`attach-failed`), `error?`, `result?`, `untrackedSession?` |
| `cancel/remote`   | `req`, `outcome` (`ok`\|`failed`\|`timeout`\|`skipped`)                                      |
| `progress`        | batch: `[{req, opId, stages[{id, done, total}], inflight, done}]`, latest per op             |
| `output`          | batch: `[{req?, stream (stdout\|stderr), text}]`                                             |
| `log`             | batch: `[{level, logger, msg}]`                                                              |
| `dropped`         | `{logs, output, progress}` counters since the last report                                    |
| `warning`         | `{code, msg}` (e.g. `cancel.local-unavailable`, `protocol.mismatch`)                         |

`initialize` checks `protocol` and both versions. On a mismatch the
client shows `protocol.mismatch` and offers a restart. This covers the
case where a package upgrade replaces the helper on disk while an older
server keeps running.

### 4.6 Jobs, sessions and cancellation (server)

**Jobs.** Every `execute`, `catalog` and `preview` is a job in one FIFO
queue (bound 16; overflow returns error `busy`). The main thread runs
one job at a time (P-2). `interrupt` on a queued job removes it and
emits `req/end{interrupted}`. Because ordering lives in the server,
Emacs needs no queue of its own.

**Session lifecycle.** `initialize` returns immediately. The connect
thread then builds the session (`SparkSession.builder.getOrCreate()`
under `SPARK_REMOTE`, or `DatabricksSession.builder.getOrCreate()`),
registers the progress handler when supported, binds `spark`, `sql`
(`spark.sql`) and `udf` (`pyspark.sql.functions`) in the user namespace
(plus the Databricks globals of section 4.12), and emits
`session/state`. `SPARK_CONNECT_USER_AGENT` gets `spark-connect-emacs/<v>`
appended, and PySpark's own console progress bar is disabled
(`SPARK_CONNECT_PROGRESS_BAR_ENABLED=0`) because it would write
carriage-return frames into the output stream. Jobs submitted meanwhile wait
in the queue. A cold Databricks cluster no longer freezes anything:
the REPL accepts input and the mode line shows `connecting`. If the
connect fails, queued code jobs end with `attach-failed`, and
`session/reconnect` retries. A reconnect builds a new session object,
rebinds `spark` and emits `session/changed{kind: stopped}`. Existing
DataFrame variables still point at the old session, and the UI says so.

_To verify (R3):_ creating the session on the connect thread and using it
on the main thread, for OSS PySpark 3.5/4.x and Databricks Connect.

**Session-change detection (P-6).** After each code job the server
compares the active and default sessions and `__main__.spark` against the
attached session. If user code stopped it, `session/changed{stopped}`.
If user code created another one, `session/changed{foreign}`, and that
job's `req/end` carries `untrackedSession: true`, because its Spark work
was not tagged. Spark work started from user-created threads is also not
tagged (tags are thread-local). This is documented, and the progress
handler attributes it to `opId` only.

**Progress attribution.** The handler runs on the thread that iterates
the execution. On the main thread it is attributed to the current job.
On any other thread it is reported with `req: null`. The handler never
blocks and never raises: it updates a per-op slot and the writer thread
emits the latest values (section 4.7).

**Job execution (main thread).**

```python
def run_job(job):
    with state_lock:                          # in-memory only
        st.current, st.pending_cancel, st.ki_raised = job.req, None, False
        st.phase = STARTING
    status, err, result, tag = "interrupted", None, None, None
    try:
        emit("req/start", req=job.req, kind=job.kind, origin=job.origin)
        try:
            if job.needs_session:
                sess = session.require()      # raises AttachFailed
                tag = f"sce-{job.req}"
                sess.addTag(tag)              # client-side, thread-local, no RPC
            st.phase = EXECUTING
            try:
                result = job.run()            # exec / catalog / preview
                status = "ok"
            finally:
                st.phase = FINALIZING         # first statement: closes the window
        except KeyboardInterrupt:
            status = "interrupted"
        except AttachFailed as e:
            status, err = "attach-failed", e.summary
        except BaseException as e:            # incl. SystemExit: server survives
            status, err = "error", safe_summary(e)
            safe_print_traceback(e)           # never raises
    except KeyboardInterrupt:                 # landed between the inner raise and the store
        status = "interrupted"
    finally:
        st.phase = FINALIZING
        if tag: safe(lambda: sess.removeTag(tag))
        with state_lock:
            st.current = st.pending_cancel = None
            st.phase = IDLE
        emit("req/end", req=job.req, status=status, error=err, result=result,
             untrackedSession=detect_foreign_session())
        # emit() never raises: on a dead channel it is a no-op and the
        # server shuts down after the job loop sees the dead flag.
```

At most one `KeyboardInterrupt` per job (`st.ki_raised`, see below), and
only while `EXECUTING`. Every path reaches `req/end` exactly once, which
the client relies on. `cell` mode first applies the Databricks notebook-source translation
(strip `# MAGIC` prefixes, `%sql` → `spark.sql(...)` per statement with the
last result shown, `%md` → text, `%fs` → `dbutils.fs`, `%run` → execute
the referenced file in the same namespace; `%scala`/`%r` are errors),
modelled on the VS Code IPython init script. Interactive mode compiles
the input with `ast`:
all statements but the last run in `exec` mode, and a trailing expression
is evaluated and shown through `sys.displayhook`, like a normal REPL.
Code is compiled with the original filename and line offset
(`ast.increment_lineno`), so tracebacks point into the source buffer.

**Cancellation.** There is one source of interrupts: the client's
`interrupt{req}`. The server never relies on a terminal SIGINT
(`:connection-type 'pipe` means Emacs never sends one).

1. The reader thread, under `state_lock`: if `req` is queued, remove it
   and reply `queued-removed`. If `st.current != req` or
   `st.phase != EXECUTING`, reply `stale`. If `st.pending_cancel == req`,
   reply `ack` (duplicate). Otherwise set `st.pending_cancel = req` and
   reply `ack`. Release the lock.
2. Wake the main thread. On POSIX this is
   `signal.pthread_kill(main_ident, SIGINT)`, which interrupts blocking
   syscalls. On Windows it is `_thread.interrupt_main()`, which takes
   effect at the next bytecode boundary (Q7).
3. The server's SIGINT handler, installed for the server's lifetime,
   raises `KeyboardInterrupt` **only if** `st.phase == EXECUTING`,
   `st.pending_cancel == st.current` and not `st.ki_raised`. It then sets
   `ki_raised` and clears `pending_cancel`. Otherwise it does nothing.
   So a wakeup that arrives after the job finished, or during the next
   job, is dropped. Any other SIGINT, for example from a `kill -INT`, is
   also dropped unless a cancel is pending.
4. `interruptTag("sce-"+req)` runs on a short-lived remote-interrupt
   thread (at most 2 outstanding, 10 s watchdog) and reports
   `cancel/remote`. A late `interruptTag` matches nothing, because the
   next job has a different tag.
5. At each job start the server checks that its SIGINT handler is still
   installed. If user code replaced it and it cannot be restored, it
   emits `warning{cancel.local-unavailable}`, and cancel relies on step 4
   plus escalation.

**Limits, stated plainly.** Local cancel is cooperative. User code can
catch `KeyboardInterrupt`, and C extensions can delay it. If no
`req/end` arrives within `spark-connect-cancel-timeout` (10 s), or the
user cancels again, Emacs offers **kill and restart**. The job becomes
`unknown`, all Python state is lost, and server-side work is interrupted
best-effort with `python -m spark_connect_emacs interrupt` (Q2a).

### 4.7 JSON performance and flow control

**Findings.** Measured in this container (Emacs 30.2, `emacs -Q --batch`):

| Operation                                                       | Native (`json-parse-string` / `json-serialize`) | `json.el` | Ratio |
| --------------------------------------------------------------- | ----------------------------------------------- | --------- | ----- |
| Parse a 200-byte progress notification                          | ~1.5 µs                                         | ~21 µs    | ~14×  |
| Parse a 750 KB result (20 000 rows × 5 columns)                 | ~5.4 ms                                         | ~64 ms    | ~12×  |
| Serialize the same 750 KB object                                | ~5.5 ms                                         | —         | —     |
| `pp-to-string` of one 200-byte message (jsonrpc events buffer)   | ~80 µs                                          | —         | —     |

From the Emacs 30 NEWS and `jsonrpc.el` 1.0.25 source:

- Emacs 30 dropped libjansson for a built-in JSON parser and serializer
  (NEWS: "Native JSON support is now always available"). Its author
  reported 8–9× faster parsing than jansson on clangd traffic. The
  `json.el` column above is therefore not a risk on the supported
  baseline (C6); it is shown only to explain why Emacs 29 is excluded.
- `read-process-output-max` went from 4096 to 65536 in Emacs 30. On
  Linux, values above 64 KiB do not help without enlarging the pipe, so
  the default is right and the plugin does not touch it.
- Emacs 30 also reimplemented the _default_ process filter in C
  (`fast-read-process-output`). `jsonrpc.el` installs its own Lisp filter,
  so this does not apply to our connection.
- `jsonrpc.el`'s events buffer defaults to `:size nil :format full`,
  which means unbounded, with every message logged. Pretty-printing
  costs far more than parsing (about 50× above), and the events buffer
  has been the main cause of large-message slowness in Eglot (bug#65760).
- `jsonrpc.el` dispatches **each** message through its own timer, and it
  keeps a copy of the raw JSON string on each message (`:jsonrpc-json`).
  Per-message overhead therefore matters more than parse speed.
- Third-party `emacs-lsp-booster` speeds up LSP clients by converting
  JSON to Elisp bytecode in a Rust wrapper process. Its maintainers
  claim about 4× on large messages against older parsers. On Emacs 30
  the native parser closes most of that gap. We own both ends of the
  wire, so it is cheaper to send less than to parse faster. We do not
  use or recommend a booster.

**Rules derived from them.**

1. Emacs 30.1+ only (C6), so built-in JSON is always there.
2. The connection is created with
   `:events-buffer-config '(:size 0)`. `spark-connect-rpc-debug` turns
   on `(:size 2000 :format short)`. The bundled `jsonrpc.el` is used; no
   GNU ELPA pin.
3. **Message-rate budget: ≤ 20 notifications/s steady state** per
   server, whatever the Spark or log activity. The writer thread builds
   it:
   - `progress`: per-op latest slot, flushed at most every 200 ms as one
     batch.
   - `output`: concatenated per stream, flushed every 50 ms or at
     32 KiB, whichever comes first.
   - `log`: batched with output, bounded queue of 256 records, overflow
     counted in `dropped`.
   - Reliable messages (`req/*`, `session/*`, replies) are sent at once,
     bounded queue 1024. A full reliable queue marks the channel dead.
4. **Size budget.** Notifications are capped at 256 KiB (output split
   across batches). `catalog` and `preview` results are capped at 4 MiB
   and `limit` defaults to 1 000 rows (`preview` caps at 10 000). At
   about 7 ms/MB parse, a full preview costs ~30 ms to parse; rendering
   costs more, so tables are inserted page by page.
5. **Backpressure.** The writer has a 5 s send timeout. If the client
   stops reading, telemetry is dropped and counted, user code keeps
   running, and on timeout the channel is declared dead. Output
   producers never block user code: pump threads drop and count output
   when the output queue is full. The only exception is the `input`
   request, which blocks by design.
6. **Emacs side.** Notification handlers only update the state model
   (O(1) per batch). Rendering runs on a ≤ 5 Hz timer. The REPL and log
   buffers are capped by line count. Output is inserted with
   `inhibit-modification-hooks` and without font-lock on the inserted
   range. No synchronous `jsonrpc-request` anywhere except `shutdown`
   from `kill-emacs-hook`.
7. The doctor reports `read-process-output-max` below the Emacs 30
   default (65536) as info, since a user setting may have lowered it. The
   plugin does not change this global option itself.

Benchmarks to keep in CI (ERT, `emacs --batch`): 10⁴ progress batches,
a 4 MiB preview, a 10⁶-line output flood through a stub server. Pass
criteria: no single command-loop stall over 50 ms, and memory bounded.

### 4.8 Environment resolution

`spark-connect-resolve-env (profile)` returns the full
`process-environment` for the server **and** every Databricks CLI call
for that profile.

1. **Scrub** inherited connection/auth variables matching
   `spark-connect-env-scrub-regexps`. Default: `^DATABRICKS_`,
   `^SPARK_REMOTE$`, `^SPARK_CONNECT_MODE_ENABLED$`,
   `^ARM_(CLIENT_ID|CLIENT_SECRET|TENANT_ID)$`. The resolver starts from
   the `process-environment` of the buffer that issued the command, so
   envrc/direnv values are seen. **Everything scrubbed is listed** in the
   start message and in the doctor, so a variable from `.envrc` that
   gets dropped is visible.
2. **Set** what the profile defines. OSS: `SPARK_REMOTE` (token from
   auth-source, inserted only into the environment). Databricks:
   `DATABRICKS_CONFIG_PROFILE`, optional `DATABRICKS_CONFIG_FILE`, and
   exactly one compute selector (`DATABRICKS_CLUSTER_ID` or
   `DATABRICKS_SERVERLESS_COMPUTE_ID=auto`). Also
   `PYTHONPATH=<helper dir>:<inherited>` and `PYTHONUNBUFFERED=1`.
3. **Validate**, failing loudly. `:env` may not set scrubbed keys.
   `:cluster-id` / `:serverless` must be exactly one. An OSS profile
   needs `:remote`. **The Databricks config profile itself is also
   read**: if it already sets `cluster_id` or `serverless_compute_id`
   and the Emacs profile selects the other one, that is an error
   (behaviour of DB Connect with both set: Q8).
4. **Show** the target reported by the server (`session/state.target`)
   as the only truth in the UI. For Databricks, `databricks auth describe`
   with the same env is a pre-flight hint, not proof: the CLI (Go SDK)
   and DB Connect (Python SDK) can resolve unified auth differently in
   edge cases.

Secrets live only in the server's environment. User code and any
process it spawns inherit them, as when running `python` from a shell
with the same variables set. The fingerprint used to decide whether a
restart is needed includes a hash of the token, so rotation restarts the
server (P-5).

### 4.9 Compatibility checks (doctor and pre-flight)

`spark-connect-doctor` combines `probe` with target facts (cluster
`spark_version`, `data_security_mode`, serverless flag via the CLI) and
Emacs facts. Rules live in the data table `spark-connect-compat-rules`.
**To be re-verified against the Databricks Connect requirements page
when implemented.**

The Databricks rows match what the VS Code extension enforces in v2.23.1
(section 1.3) unless marked "ours".

| Rule                                                                                                   | Level                   |
| ------------------------------------------------------------------------------------------------------ | ----------------------- |
| More than one of `pyspark`, `pyspark-client`, `databricks-connect` installed (ours)                    | error                   |
| Databricks profile without `databricks-connect` / OSS profile without `pyspark` or `pyspark-client`   | error                   |
| Cluster DBR < 13                                                                                       | error                   |
| Cluster access mode not UC-capable (allowed: `SINGLE_USER`, `SHARED`, `USER_ISOLATION`)                | error                   |
| Workspace without Unity Catalog (`catalogs list` empty)                                                | error                   |
| DB Connect < 13, or newer than the cluster DBR major.minor                                            | error                   |
| DBR newer than the client (supported; matching recommended)                                            | warning                 |
| Serverless with DB Connect < 15.4                                                                      | error                   |
| Local Python < 3.10; serverless: < 3.12 for DB Connect 16+, else < 3.11                                | error                   |
| Local Python minor ≠ DBR Python (13–14 → 3.10, 15 → 3.11, 16+ → 3.12); Python UDFs fail               | warning                 |
| OSS client minor newer than the server minor (ours)                                                    | warning                 |
| No progress API (client < 4.0) (ours)                                                                  | info: degraded progress |
| `read-process-output-max` < 65536, lowered by user config (ours)                                       | info                    |
| Running server's `protocol`/version ≠ installed helper (ours)                                          | warning: restart        |

### 4.10 Run-file mode

`python -m spark_connect_emacs run [--module] [--] TARGET [ARGS…]` is a
server with one implicit job, and it behaves like `python TARGET ARGS…`
(or `python -m TARGET`):

- `sys.argv = [TARGET, *ARGS]`. `sys.path[0]` = directory of
  `abspath(TARGET)` (cwd for `--module`). Execution goes through
  `runpy.run_path`/`run_module` with `run_name="__main__"`, so `__file__`,
  the main guard and pickling of `__main__` functions (UDFs) work.
- Cwd comes from the profile's `:cwd` (buffer directory by default).
- Exit status: `SystemExit` propagates its code. An uncaught exception
  prints a traceback with runner frames removed and exits 1. If the
  connect fails before the script starts, it exits 3 with
  `req/end{attach-failed}`. `--no-attach` runs without a session.
- Emacs renders `output` into a `compilation-mode` buffer (clickable
  tracebacks) and the job into the jobs list. Interrupt, progress and
  `input` work as in the interactive server.
- Several run-file processes may run next to the interactive server
  (P-1). Each has its own Spark session.

Tests: a differential harness that compares with plain `python` (sibling
imports, positional and flag arguments, relative reads, `SystemExit(0/2)`,
uncaught exception, `--module` on a package).

### 4.11 UI

- **Mode line** (`spark-connect-mode`): `SC:<server target>` plus a
  state glyph (stopped / connecting / idle / N queued / running /
  `!` session changed), stage progress while a job runs
  (`[st 3/10 ▓▓░ 45%]`), and the cluster state for Databricks. Click opens
  the transient.
- **`*spark-connect-jobs*`** (`tabulated-list-mode`): jobs with kind,
  origin, start, duration, status
  (`queued|running|cancel-pending|ok|error|interrupted|attach-failed|unknown`),
  and on 4.x clients their operations as child rows. Keys: `k` cancel,
  `RET` details, `g` refresh. `cancel-pending` is shown after `ack`, and
  `interrupted` only after `req/end`.
- **REPL buffer** (section 4.2), **log buffer** (line-capped),
  **transient** `spark-connect-dispatch`, **catalog browser** (P1;
  queued jobs, so it waits while code runs and says so), **DataFrame
  preview** (P1; paged `tabulated-list` or org table), **Spark UI link**,
  completion notifications via `notifications-notify` / `alert`.

### 4.12 Databricks module

Optional (`spark-connect-databricks*.el`). It needs `databricks` CLI ≥ a
pinned minimum (`databricks version`). All calls are async `make-process`
with `-o json`, parsed with `json-parse-buffer`, and the env from
section 4.8.

- **auth**: list profiles (`auth profiles --skip-validate`),
  `databricks auth login --host … --profile …` in a terminal buffer
  (OAuth U2M), `auth describe`. Azure CLI auth works through the profile.
- **environment** (P1): `databricks environments setup-local
  --cluster-id X | --serverless-version N --output json`, the same CLI
  command the VS Code extension uses. It creates `.venv` with uv, merges
  runtime constraints into `pyproject.toml` and installs DB Connect. Runs
  only on explicit confirmation, shows the `--dry-run` plan first, and
  reports the CLI's phases and `E_*` codes. After it succeeds, the
  profile's `:python` points at the new venv. When the compute changes,
  the doctor warns about drift. Projects using poetry, conda or
  `requirements.txt` get the printed `pip install` command instead.
- **clusters**: list, select, start, state polling with exponential
  backoff only while a Databricks profile is active and the cluster is
  not in a stable state. Feeds section 4.9.
- **jobs/runs**: `*databricks-runs*` (`jobs list-runs` with `--limit`,
  `--active-only` toggle), `get-run`, `get-run-output`, cancel/repair,
  refresh only while visible.
- **bundles**: `bundle validate/deploy/destroy/run/summary -o json`, a
  target selector, a resource tree from `summary --include-locations`,
  validate after saving bundle YAML. `bundle run` streams into a
  `compilation-mode` buffer; the run id is parsed from its output and
  then polled (jobs every 3 s, pipelines every 5 s with incremental
  events, as in VS Code). Pipeline ERROR/WARN events that carry source
  locations become **Flymake** diagnostics (P2). Selective table refresh
  (`--refresh` / `--full-refresh`, confirmed) is P2.
- **sync** (P2): `bundle sync --watch --output json` as a long-lived
  process; its state shows in the mode line.
- **Run file as workflow**: `jobs submit` with a `spark_python_task`,
  through a bundle (P1) or `workspace import` (P2).
- **Unity Catalog browser** for Databricks profiles goes through the CLI
  (`catalogs/schemas/tables/volumes/functions list`), not the Spark
  session, like VS Code. It works without a running cluster and shows
  volumes, functions and models. OSS profiles use the Spark catalog
  (section 4.11).
- **Databricks globals** in the server: `dbutils` (from
  `databricks.sdk.runtime`), `getArgument`, `displayHTML` (renders as
  text, or opens in a browser), in addition to the core `spark`, `sql`,
  `udf`. This is the set the VS Code bootstrap injects.
- At most 2 concurrent CLI processes. OAuth token refreshes by
  concurrent CLI calls write the same token cache, so they are
  serialized.

### 4.13 LLM chat through gptel (optional, P2)

VS Code registers a chat model provider that streams from the Unity AI
Gateway Responses API (`<host>/ai-gateway/mlflow/v1/responses`, models
from `system.ai` that support `mlflow/v1/responses`;
`src/lm-chat/UnityGatewayResponsesClient.ts`). That endpoint is
OpenAI Responses-compatible: the operator already uses it this way with
an OpenAI-compatible code-review tool. In Emacs, `gptel` provides the
chat UI, including chat in Org buffers. We only supply a backend.

- **gptel supports the Responses API.** Read from gptel master
  (2026-11-04): `gptel-openai-responses.el` defines the backend struct
  `gptel-openai-responses` (streaming, reasoning, tools, JSON schema) and
  the autoloaded constructor `gptel-make-openai-responses`
  (`:host`, `:endpoint` default `/v1/responses`, `:key`, `:models`,
  `:stream`, `:header`, `:request-params`).
- **Do not use `gptel-make-openai` for this.** It picks the Responses
  backend only when `:host` matches `api.openai.com` (a TODO in its source
  admits this is unreliable). For a Databricks host it silently builds a
  Chat Completions backend.
- **Backend.** `spark-connect-databricks-gptel-setup` (in
  `spark-connect-databricks-gptel.el`, loaded only if `gptel` is
  installed) defines both backends (see "Which route by default" below).
  The Responses one uses `gptel-make-openai-responses` with `:host` from
  `databricks auth describe` (no scheme), `:endpoint
  "/ai-gateway/mlflow/v1/responses"`, `:stream t`, and `:models` listed the
  way VS Code does it: `system.ai` models whose supported API types
  include `mlflow/v1/responses` (`src/lm-chat/unityGatewayModels.ts`).
  VS Code calls the listing through the separate npm package
  `@databricks/sdk-aigateway` 0.23.0, not the classic Databricks SDK. We
  need no SDK for it: the listing is a Unity Catalog REST resource
  (`/api/2.1/unity-catalog/model-services`, per the Databricks docs on
  model services), so `databricks api get
  "/api/2.1/unity-catalog/model-services?parent=schemas/system.ai" -o json`
  uses the CLI's own auth. _To verify:_ the exact list path, query
  parameters and response field names (`supported_api_types`?). A
  user-set model list overrides discovery.
- **Request shape** (verified by the operator with curl on 2026-11-10):
  `POST https://<workspace-host>/ai-gateway/mlflow/v1/responses` with
  `Authorization: Bearer <token>`. The model is **not** in the URL but in
  the body, as the full Unity Catalog name (e.g.
  `"model": "system.ai.kimi-k3-1-pro"`). The gateway accepts the exact
  body gptel builds: `model`, `input` with plain-string `content`,
  `stream`, `store: false`, `instructions`. gptel models are therefore
  declared as symbols such as `system.ai.kimi-k3-1-pro`.
- **Known gap: reasoning is not shown.** The gateway returns reasoning as
  an output item `{type: "reasoning", content: [{type: "reasoning_text",
  …}], summary: []}`. gptel reads reasoning only from `summary` (non-
  streaming) and from `response.reasoning_summary_text.delta` /
  `response.reasoning.delta` events (streaming). So reasoning text is
  dropped, but the answer (`output_text`) is displayed correctly. Fix
  upstream in gptel (handle `reasoning_text` content and the matching
  stream delta event), not with local advice. _To verify:_ the event
  name the gateway uses when streaming reasoning.
- **Auth.** `:key` is a function returning a bearer token from
  `databricks auth token --profile <p> -o json` for the active profile,
  cached until shortly before `expiry`. Tokens are never stored in
  customization files (C8). The CLI call is synchronous because gptel
  asks for the key synchronously, but it runs at most about once per
  token lifetime.
- **Chat Completions on the same gateway** (verified by the operator,
  2026-11-10): `POST <host>/ai-gateway/mlflow/v1/chat/completions` with
  `messages` (system prompt as a `system` message), `model` =
  `system.ai.<name>`, `max_tokens`. Reasoning comes back as
  `message.reasoning_content` (DeepSeek style), which gptel's OpenAI
  backend already parses (`gptel-openai.el` checks `:reasoning_content`
  in stream deltas).
- **Which route by default.** VS Code speaks only the Responses route.
  Its source calls it "Unity Gateway's unified Responses route, the only
  API the provider speaks", and lists models by
  `supportedApiTypes` containing `mlflow/v1/responses`
  (`src/lm-chat/unityGatewayModels.ts`). Likely reasons (inferred, not
  stated in the code): Responses is the route that covers every model the
  gateway serves, including Responses-only OpenAI models; it has typed
  tool-call items and stream events, which VS Code's agent-mode chat
  depends on; and it is OpenAI's primary API going forward. None of that
  forces our choice. **Default: the Chat Completions backend**
  (`gptel-make-openai`, `:endpoint "/ai-gateway/mlflow/v1/chat/completions"`),
  because gptel shows reasoning there and it is gptel's most used path.
  The Responses backend is offered as a second backend and becomes the
  default once gptel handles `reasoning_text`. Models that list only
  `mlflow/v1/responses` go only to the Responses backend.
- **Fallback for workspaces without Unity AI Gateway.** Classic serving
  endpoints through Chat Completions (`:endpoint
  "/serving-endpoints/chat/completions"`, `model` = endpoint name).
- **Chat buffer.** Plain gptel: `M-x gptel` with `gptel-default-mode`
  set to `org-mode`. The plugin adds a `spark-connect-dispatch` entry
  that opens such a buffer with the Databricks backend selected.
- **Minimum gptel version.** The first release that ships
  `gptel-make-openai-responses`. gptel's NEWS mentions Responses support
  in the 0.9.9.5 section; whether that release already has the public
  constructor is _to verify_. If it does not, require the first release
  that does, rather than calling the internal `gptel--make-openai-responses`.
- Not planned: tool use against the live Spark session, or sending query
  results as context. Possible later through gptel's tools and context
  features.

### 4.14 Project environment provisioning (first run, P1)

When a profile has no usable interpreter (no `:python`, no `.venv` under
the project root, or the doctor reports a blocking error), the plugin
offers to **create** a project venv. It never modifies an existing one
(C2). Creation always shows the plan first and needs confirmation.

| Profile kind | Created with | Contents |
| --- | --- | --- |
| `spark-connect` (OSS) | `uv venv` + `uv pip install` (fallback `python -m venv` + `pip`) | Spark 4.x server: `pyspark-client==<server major.minor>.*` (thin pure-Python client). Spark 3.5: `pyspark[connect]==3.5.*`. The server version comes from the profile (`:spark-version`) or is asked; a probe RPC to learn it would need a client first |
| `databricks` | `databricks environments setup-local --cluster-id X \| --serverless-version N --output json` (section 4.12) | `databricks-connect` matching the DBR or serverless version, which also brings `databricks-sdk`, so `dbutils` works |

Rules:

- **Where.** `<project root>/.venv`, the same place auto-detection looks
  and where VS Code's uv flow puts it. If `pyproject.toml` exists, the
  Databricks path lets the CLI merge constraints into it; the OSS path
  only installs into the venv and prints the `uv add` line for the user
  to adopt.
- **Only what the runtime needs.** The plugin's own helper is
  stdlib-only and stays on `PYTHONPATH`; nothing of ours is installed.
  User dependencies are the user's business.
- **No plugin-owned "tools venv".** Control-plane calls (clusters, jobs,
  bundles, AI Gateway model listing) go through the `databricks` CLI,
  including generic `databricks api get|post` for REST resources without a
  dedicated command. A second Python environment just for
  `databricks-sdk` would add a runtime to manage without giving us
  anything the CLI lacks. Revisit only if a needed API is not reachable
  through `databricks api`.
- **uv.** Preferred and detected on `PATH`. If missing, the plugin
  prints the official install command instead of running it.
- After creation the doctor runs, and the profile's interpreter is
  recorded in `spark-connect-profiles` via Customize, so the choice is
  visible and editable.

## 5. Parity matrix and phasing

Rows follow the VS Code extension as read in section 1.3.

| VS Code feature (v2.23.1)                        | Emacs counterpart                                                                                    | Phase                         |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------- | ----------------------------- |
| Configure/select connection, target, compute    | `spark-connect-select-profile` (+ dir-local profile name); bundle target selector                   | P0 / P1 (target)              |
| Run with Databricks Connect                      | run-file mode (section 4.10)                                                                         | P0                            |
| Interactive Window / `.py` cells                 | REPL buffer; send region/defun/cell/buffer; `# %%` and `# COMMAND ----------` cells                 | P0                            |
| Databricks notebook source (`# MAGIC %sql`, `%md`, `%fs`, `%run`) | Same translation in the server for `execute` (cell mode) and `ob-spark-connect`     | P1                            |
| Progress (ipywidgets bar in Jupyter; console bar in terminal) | Mode line + jobs buffer with stage progress — beyond parity                            | P0 (degrades per C4)          |
| Cancel (kill terminal / debug session)           | Per-job cancel with `interruptTag` (section 4.6) — beyond parity                                     | P0                            |
| Output channels / logs                           | log buffer, `*spark-connect-jobs*`                                                                   | P0                            |
| Typing stubs for `spark`/`dbutils` (autocomplete) | `complete` against the live namespace; optional stub file for LSP users (Eglot + pyright/basedpyright) | P0 / P2 (stub)             |
| Env check                                        | `spark-connect-doctor` (section 4.9 rules)                                                           | P0                            |
| Env setup (uv, `environments setup-local`)       | first-run venv creation: CLI for Databricks, uv + `pyspark-client` for OSS (section 4.14)            | P1                            |
| Auth (profiles, OAuth U2M, Azure CLI, PAT)       | Databricks CLI unified auth                                                                          | P1                            |
| Compute view: clusters, start/stop, serverless   | clusters list/start/stop, `:serverless t`                                                            | P1                            |
| Bundles: deploy/destroy/run, resource explorer, variables | bundle commands + summary tree; variables via the CLI                                        | P1                            |
| Job / pipeline run status                        | `*databricks-runs*` + run polling                                                                    | P1                            |
| Unity Catalog explorer                           | catalog browser (CLI for Databricks, Spark catalog for OSS)                                          | P1                            |
| DataFrame HTML display                           | `preview` (paged table)                                                                              | P1                            |
| Debug with Databricks Connect                    | dape + debugpy on run-file mode                                                                      | P2                            |
| Run file as workflow (`jobs submit`)             | `jobs submit` with a wrapper, result in a buffer                                                     | P2                            |
| Sync (`bundle sync --watch`)                     | long-lived sync process + mode-line state                                                            | P2                            |
| Pipeline diagnostics, selective table refresh    | Flymake diagnostics; refresh commands                                                                | P2                            |
| Jupyter notebooks (`.ipynb`)                     | optional emacs-jupyter adapter, if demanded (Q3)                                                     | P2                            |
| Workspace FS explorer                            | read-only `workspace list/export` browser                                                            | P3                            |
| SSH tunnel / remote IDE (`databricks ssh connect`) | **Out of scope** (operator decision 2026-11-10)                                                    | —                             |
| Unity AI Gateway chat                            | gptel backends for the gateway (Chat Completions default, Responses optional); gptel Org buffer (section 4.13) | P2                |
| Upload and run (command execution API, classic only) | **Won't do**: covered by Connect and workflow runs                                               | —                             |
| AI tools installer (`databricks aitools`)        | **Out of scope**                                                                                     | —                             |

**P0 exit criterion.** With only Emacs and a venv containing
`pyspark[connect]` ≥ 3.5, against a local Spark Connect server, a user
can select a profile, start the server, use the REPL and send commands,
see job status (and stage progress on 4.x), cancel a long query _and_ a
local Python loop, answer an `input()` prompt, and run a multi-file
script with arguments. The Databricks module is not on `load-path`.
CI passes against **real Spark 3.5 and 4.x Connect servers** with the
test lists of sections 4.4–4.10, and the section 4.7 benchmarks are
within budget.

**P1 Databricks acceptance gate** (credential-gated: manual, or a CI job
that runs only when `SCE_DATABRICKS_PROFILE` is set): auth via the CLI;
server-reported target equals the resolved target; R3 session identity;
a simple action; cancel of a running job; the same on serverless if
claimed. Databricks features are not announced before this gate passes.

## 6. Repository layout

```
lisp/
  spark-connect.el             ; entry, defgroup, global minor mode, autoloads
  spark-connect-profile.el     ; profiles, dir-local name, fingerprint
  spark-connect-env.el         ; env resolver (section 4.8)
  spark-connect-rpc.el         ; jsonrpc.el wrapper, transport swap point
  spark-connect-server.el      ; server lifecycle, initialize, restart
  spark-connect-state.el       ; state model fed by notifications
  spark-connect-python.el      ; spark-connect-python-mode (send, completion)
  spark-connect-repl.el        ; spark-connect-repl-mode
  spark-connect-run.el         ; run-file mode buffers
  spark-connect-ui.el          ; mode line, jobs, log, transient
  spark-connect-catalog.el
  spark-connect-preview.el
  ob-spark-connect.el
  spark-connect-doctor.el
  spark-connect-databricks.el  ; optional entry
  spark-connect-databricks-cli.el
  spark-connect-databricks-clusters.el
  spark-connect-databricks-jobs.el
  spark-connect-databricks-bundle.el
  spark-connect-databricks-gptel.el ; optional (soft-depends on `gptel`)
python/spark_connect_emacs/
  __init__.py  __main__.py  probe.py  server.py  rpc.py  stdio.py
  jobs.py  session.py  cancel.py  progress.py  run.py  interrupt_cli.py
test/
  elisp/   (ERT; stub server speaking the protocol; fake `databricks` on PATH; JSON benchmarks)
  python/  (pytest; fake session unit tests; integration against local Spark Connect 3.5 / 4.x)
docs/design/spark-connect-emacs.md
```

Distribution: one MELPA package whose recipe `:files` includes
`python/`. Elisp locates the helper with
`(file-name-directory (locate-library "spark-connect"))`. This must be
tested with package.el, straight.el and elpaca build layouts.

## 7. Consequences

**Positive**

- Works with any Spark Connect server. Databricks is an add-on (C5).
- One input path and one state machine, in Python, unit-testable without
  Emacs. The client is a renderer with a request sender.
- No socket, tokens or port handling. Emacs owns the process lifetime.
- Built-in `jsonrpc.el` and native JSON keep the client free of native
  modules and third-party transport packages.
- No silent installs into the user's venv (C2). Env provisioning is the
  CLI's job, as in VS Code.

**Negative / costs**

- Users lose the stock python.el REPL and `ob-python :session` for
  Spark work. They get our REPL, `ob-spark-connect` and `rlcompleter`
  completion instead: a smaller feature set than IPython or Jedi.
- A home-grown protocol to version (`protocol: 1` in `initialize`).
- Text-only output in the REPL. Tables go through `preview`.
- One interactive target per Emacs (P-1).
- `jsonrpc.el`'s per-message timer means throughput depends on our
  batching discipline (section 4.7).
- Databricks users must install the `databricks` CLI.

## 8. Risks and mitigations

| #   | Risk                                                                                                         | Mitigation                                                                                                                              |
| --- | ------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| R1  | Progress API missing or different across PySpark / DB Connect versions (C4)                                  | Feature-detect at connect time; job-level status needs no Spark hook; integration matrix 3.5 / 4.x                                     |
| R2  | Spark work outside the attached session or the main thread is invisible to tags                              | P-6 detection and `untrackedSession`; thread-originated progress shown unattributed; documented; `interruptAll` as explicit fallback   |
| R3  | Session created on the connect thread misbehaves when used on the main thread, or `getOrCreate()` in user code does not return it | Integration tests for OSS and DB Connect; fallback: create the session on the main thread as the first queued job                    |
| R4  | JSON or message volume stalls Emacs                                                                          | Section 4.7 budgets, batching in the writer, events buffer off, throttled rendering, CI benchmarks; transport swap point in `spark-connect-rpc.el` |
| R5  | User code writes to the protocol fds or replaces `sys.stdout` early                                          | Protocol on private dup'ed, non-inheritable fds; fd-level redirection; test with C-level writes and subprocesses                       |
| R6  | `input()` / `pdb` deadlock if the client never answers                                                       | `input` request cancelled by `interrupt`; `eof` reply on REPL kill; test `pdb` session end-to-end                                     |
| R7  | Databricks CLI JSON shapes drift                                                                             | Pinned minimum version, defensive parsing, contract tests on recorded fixtures                                                         |
| R8  | Wrong target from inherited env or misread Databricks config                                                 | Single resolver with scrubbing, listing and validation, including the config file (4.8); UI shows only the server-reported target     |
| R9  | Helper on disk replaced by a package upgrade under a running server                                          | `initialize` version check, `protocol.mismatch` warning, restart offer                                                                 |
| R10 | Scope creep toward "full parity"                                                                             | Phase gates; P1 starts only after the P0 exit criterion                                                                                |

**Testing strategy**

- ERT unit tests: profiles, env resolver, state model, mode-line
  rendering, CLI JSON parsing (fixtures).
- ERT integration with a stub Python server that replays protocol
  scripts, including floods and partial frames, plus the section 4.7
  benchmarks.
- pytest for the server: job envelope outcomes; deterministic injection
  of a cancel wakeup at each phase boundary (before `exec`, after it
  returns, inside each `except` before the phase store, during
  `removeTag`, during `req/end` emit); delayed wakeups across a job
  boundary (next job unaffected); queued-job cancel; user code that
  catches `KeyboardInterrupt` (escalation); user code that replaces the
  SIGINT handler; stalled and failing `interruptTag`; fd-level output
  capture (C extension, subprocess); `input()` and `pdb`; a non-reading
  client (user code continues, memory bounded); channel loss mid-job.
- Integration against real local Spark Connect **3.5 and 4.x** (one CI
  venv each). PySpark 4.x can start a local server with
  `remote("local")`. 3.5 needs the Connect server package, so CI uses a
  pre-fetched distribution.
- A `databricks-connect` venv for probe and doctor-rule tests. Workspace
  behaviour goes through the P1 gate.

## 9. Open questions (for the operator)

- **Q1. Minimum versions.** Emacs 30.1 (decided). PySpark 3.5 (degraded
  progress), 4.0+ recommended. OK?
- **Q2. DB Connect progress.** Which Databricks Connect versions expose
  the progress handler? Needs a real workspace.
- **Q2a. Session rejoin for hard cancel.** Can a fresh client
  `interruptTag` in an existing session by session id, on OSS and on
  DB Connect? This decides whether kill-and-restart also stops server
  work.
- **Q3. Notebooks.** Is a `.ipynb` workflow needed before P2, or are the
  REPL plus `ob-spark-connect` enough?
- **Q4. CLI as the Databricks dependency.** Acceptable (as in VS Code)?
- **Q5. Non-Python clients** (Scala, Go) stay out of scope. The protocol
  is language-neutral. Confirm.
- **Q6. Naming/distribution.** `spark-connect` prefix, one MELPA package
  with the Databricks module inside?
- **Q7. Windows cancel.** `_thread.interrupt_main()` does not interrupt
  blocking calls. Is Windows support best-effort (remote `interruptTag`
  plus escalation)?
- **Q10. Cancel scope.** VS Code has no tag-based cancel or progress
  channel for script runs (section 1.3). Keep section 4.6's precise cancel
  in P0 as a differentiator, or ship P0 with `interruptTag` plus
  kill-and-restart and add local-loop cancel in P0.5?
- **Q8. DB Connect with both compute selectors.** What does
  `DatabricksSession` do when the config profile has `cluster_id` and the
  env sets `DATABRICKS_SERVERLESS_COMPUTE_ID`? The resolver currently
  treats it as an error.

## 10. References

- Emacs 30 `etc/NEWS`: native JSON without libjansson;
  `read-process-output-max` default 65536; `tabulated-list-groups`;
  `mode-line-format-right-align`; `completion-preview-mode`;
  `fast-read-process-output`.
- `jsonrpc.el` 1.0.25 (Emacs 30.2): process filter, per-message timers,
  `:events-buffer-config`.
- Géza Herman, new JSON parser, emacs-devel 2024-03:
  <https://lists.gnu.org/archive/html/emacs-devel/2024-03/msg00351.html>
- Native JSON slower than json.el with jansson (historic), bug#31138:
  <https://debbugs.gnu.org/db/31/31138.html>
- Eglot slowness from `jsonrpc--log-event`, bug#65760:
  <https://yhetil.org/emacs-bugs/875y4o5yfh.fsf@ya.ru/>
- `read-process-output-max` discussion, bug#66020; 64 KiB pipe limit,
  bug#55737: <https://yhetil.org/emacs/87zgivz9oj.fsf@gnus.org/T>
- Databricks VS Code extension source, v2.23.1:
  <https://github.com/databricks/databricks-vscode> (`packages/databricks-vscode`)
- emacs-lsp-booster: <https://github.com/blahgeek/emacs-lsp-booster>
- "The new JSON parser is fast", r/emacs:
  <https://redlib.hyperreal.coffee/r/emacs/comments/1jsxamc/the_new_json_parser_is_fast>
- Mastering Emacs, What's new in Emacs 30.1:
  <https://www.masteringemacs.org/article/whats-new-in-emacs-301>
