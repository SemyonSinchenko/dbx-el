# spark-connect.el — Emacs as a UI/orchestration front-end for Spark Connect (with optional Databricks)

- Status: **Proposed** (draft 1, run `20261109-113012-architecture`)
- Date: 2026-11-09
- Author: architect agent; to be reviewed by the critic and the operator

## 1. Context and problem

### 1.1 Current state of the repository

The project is **greenfield**. At the time of writing the repository has no
Emacs Lisp, Python, build files or README; it holds only multi-agent
tooling state (`.mas/`, `.agent-shell/`, `.claude-container/`,
`.codex-container/`). Nothing in this design is constrained by existing
code, so the decisions below set the initial layout. Section 6 lists the
files this design would create.

Local tooling observed in the dev container: GNU Emacs 30.2 and Python 3
without `pyspark`. That is enough for Elisp development. Python integration
tests will need a venv with `pyspark` (see 8.3).

### 1.2 What the operator wants

> An Emacs plugin for Spark Connect with optional Databricks Connect
> support, inspired by the Databricks VS Code extension, aiming at parity
> with it. Databricks features (and the SDK) stay optional; the core
> works with _any_ Spark Connect server. Proposed shape: "EIN (or friends)
>
> - PySpark Connect thin client + Emacs as UI with status bars, logs and
>   all the fancy things", to be confirmed.

Main user stories:

1. **Try PySpark code on a real cluster from a buffer**: send a region,
   function or buffer, or run a file, through Spark Connect. Get the
   output back in Emacs.
2. **See what is going on**: connection state and running operations in
   the mode line, stage/task progress, logs, the option to cancel.
3. **Databricks extras (optional)**: pick an auth profile and a cluster
   (or serverless), start the cluster, list job runs and watch their
   status, deploy and run Asset Bundles, open the Spark UI or run page.
4. **Work with any Spark Connect server**: a local
   `start-connect-server.sh`, Spark on Kubernetes, EMR, or another vendor.
   The Databricks pieces must not be installed or loaded for this case.

### 1.3 Reference: what the Databricks VS Code extension does

This is a rough feature inventory as of 2025–2026. It has to be checked
against the current extension release before the parity matrix (section 5)
is frozen.

| Area           | VS Code extension feature                                                                                                    |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Auth           | Profiles from `~/.databrickscfg`, OAuth U2M, PAT, Azure CLI; sign-in flow                                                    |
| Compute        | Select cluster, start/stop, serverless option                                                                                |
| Env setup      | Check/create Python venv, install `databricks-connect` matching the cluster DBR version                                      |
| Run            | "Run with Databricks Connect", debug with Databricks Connect, Jupyter notebooks backed by Databricks Connect, `dbutils` shim |
| Remote run     | "Upload and run file" (command execution API), "Run file as workflow" (one-off job run)                                      |
| Bundles        | Bundle init/validate/deploy/run, target selection, resource explorer (jobs, pipelines), run status, bundle variables         |
| Sync           | Sync local project to the workspace (now done through bundles)                                                               |
| Explorers      | Unity Catalog explorer, pipeline/dataset views (newer releases)                                                              |
| Under the hood | It **bundles the Databricks CLI** and calls it for bundle/sync operations                                                    |

The last row matters: the reference implementation already treats the
Databricks CLI as its control-plane engine. This design does the same.

## 2. Constraints and forces

- **C1. No gRPC in Emacs.** Spark Connect is gRPC plus protobuf logical
  plans. Emacs has no maintained gRPC/HTTP2 client, and building plans in
  Elisp would mean re-implementing a DataFrame client. Every Spark Connect
  call has to go through a real client library. PySpark is the obvious one,
  since the target users write PySpark.
- **C2. `databricks-connect` and `pyspark` cannot live in the same
  environment.** `databricks-connect` ships its own `pyspark` package and
  requires that the OSS `pyspark` is not installed. So the plugin must
  never install its own Python dependencies into the user's venv. It also
  has to work with whatever flavour that venv contains (OSS `pyspark`,
  `pyspark-client`, or `databricks-connect`).
- **C3. Client/server compatibility is a rule set, not an equality.**
  Databricks Connect supports a compute DBR greater than or equal to the
  client version, and matching versions are recommended. There are further
  constraints: a serverless client minimum, Python minor matching for
  UDFs, and supported access modes. OSS Spark Connect clients should not
  be newer than the server. The plugin must _probe and report_, and it
  separates errors from warnings (section 4.7).
- **C4. Progress information is version-dependent.** PySpark 4.0+
  provides a client-side progress hook for Spark Connect
  (`SparkSession.registerProgressHandler`, fed by `ExecutionProgress`
  messages in the `ExecutePlan` response stream). Older clients and some
  Databricks Connect versions may not have it. _To verify per version;
  see Q2._ The design has to degrade to "operation started/finished".
- **C5. Databricks optional, for real.** The core package must not
  `require` Databricks code, must not need the `databricks` CLI, and must
  not import `databricks.*` in Python unless a Databricks profile is in
  use.
- **C6. Emacs ≥ 29.1.** Lets us use built-in `jsonrpc`/`json-parse-*`,
  `transient`, `project.el`, `tabulated-list`, `compile`/`comint`, and
  `python.el`. No native modules in the core (rules out a _hard_
  dependency on emacs-jupyter/ZMQ).
- **C7. Async everywhere.** Polling clusters and runs, probing envs, and
  running code must never block the UI. Only `make-process` /
  `make-network-process` with filters/sentinels; no `call-process` on hot
  paths.
- **C8. Secrets.** Tokens never go in Emacs customization files or command
  lines (they would show in `ps`). Sources: `auth-source` for OSS tokens;
  the Databricks CLI / unified auth for Databricks.

## 3. Options considered

### 3.1 Where user code executes (the "EIN?" question)

| Option                                                 | Description                                                                          | Pros                                                                                                                    | Cons                                                                                                                                                                                                          |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A. EIN as the core**                                 | Notebook UI via EIN, Jupyter kernel with a Spark session                             | Rich output, notebook workflow familiar to VS Code users                                                                | EIN is notebook-centric and its maintenance is low/uncertain. Needs a Jupyter server. Progress/status would have to be scraped from cell output. Weak fit for "send region from a `.py` file".                |
| **B. emacs-jupyter as the core**                       | `jupyter-python` REPL / org-babel through ZMQ kernels                                | Actively used; rich output; org-babel integration                                                                       | Needs the `emacs-zmq` **native module** (a known install pain), a Jupyter kernel in the venv (`ipykernel` → clashes with C2's "never install into the user's venv"). Kernel protocol hides our control plane. |
| **C. `python.el` inferior Python (comint)**            | Standard `run-python` process in the project venv, bootstrapped with a Spark session | Zero extra deps; users already know `C-c C-r`/`C-c C-c`; `ob-python :session` works out of the box; works with any venv | Plain-text output only; there is no structured channel in comint itself                                                                                                                                       |
| **D. Custom Python sidecar that is also the executor** | Our own long-lived Python process doing `exec` in a namespace, JSON-RPC to Emacs     | One structured channel for everything                                                                                   | Re-invents a (worse) Jupyter kernel; loses `python.el` integration; still needs a REPL UX                                                                                                                     |
| **E. Native Elisp Spark Connect client**               | Talk gRPC from Emacs                                                                 | No Python                                                                                                               | Infeasible (C1)                                                                                                                                                                                               |

**Assessment.** Executing code is a commodity that Emacs already has in
two forms (C and B). The missing piece is the **out-of-band status
channel**: progress, operation list, interrupt, logs, catalog queries.
These are needed whatever executes the code. So we split _execution_ from
_observation/control_.

### 3.2 How status reaches Emacs

| Option                                                                                                                                                                | Pros                                                                                                                                                           | Cons                                                                                                                                                                                                            |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Parse REPL text output (markers in stdout)                                                                                                                            | Trivial                                                                                                                                                        | Fragile; interleaves with user output; no way to send commands while the REPL is busy                                                                                                                           |
| **Side channel socket**: Emacs listens on a local socket, the Python helper inside the executor connects and streams NDJSON events, Emacs sends control messages back | Works for _every_ executor (comint, Jupyter kernel, `python script.py` subprocess); interrupt works while the main thread is busy (handled on a helper thread) | Needs a small Python helper on `PYTHONPATH`; local-only (remote/TRAMP executors need port forwarding: deferred)                                                                                                 |
| Separate sidecar process with its own SparkSession                                                                                                                    | Isolated                                                                                                                                                       | A **different** Spark Connect session cannot see the user's operations or tags, so `interruptAll` and progress would not cover them. Rejected for progress; acceptable for a read-only catalog browser at most. |

### 3.3 Databricks control plane (clusters, jobs, runs, bundles)

| Option                                                                                         | Pros                                                                                                                                                                                            | Cons                                                                                                                                                    |
| ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Python `databricks-sdk` in a sidecar                                                           | Typed, complete                                                                                                                                                                                 | Needs a Python env with the SDK. Must not be the user's env when it holds OSS pyspark (C2), so a second venv to manage. Heavy for "show me run status". |
| Raw REST from Elisp (`url.el`/`plz`)                                                           | No external process                                                                                                                                                                             | We would re-implement unified auth (OAuth U2M refresh, Azure CLI, M2M), pagination, retries                                                             |
| **Databricks CLI (`databricks …  -o json`) as a subprocess**                                   | Single static Go binary. Implements unified auth and **all** REST commands (generated from the same API spec as the SDK) plus `bundle`. Same engine the VS Code extension bundles. JSON output. | External binary. Its JSON shapes follow API versions. Slower per call (process spawn)                                                                   |
| Hybrid: CLI for auth + bundles, REST from Elisp for hot polling, using `databricks auth token` | Fast polling                                                                                                                                                                                    | Two code paths; token refresh handling in Elisp                                                                                                         |

## 4. Decision

**Confirmed in part, with one change.** "PySpark Connect thin client +
Emacs as UI" is confirmed. **EIN is _not_ the core.** Execution is
pluggable. The default executor is the built-in `python.el` REPL, and
emacs-jupyter (or EIN) is an _optional adapter_. They all share one
out-of-band **status/control channel** that we own.

### 4.1 Architecture

```
┌──────────────────────────── Emacs ─────────────────────────────────────┐
│ spark-connect (core, no Databricks deps)                                │
│  profiles ─ env probe ─ executors ─ channel server ─ UI                  │
│   │                       │            ▲  NDJSON over local socket       │
│   │        ┌──────────────┼────────────┼───────────────┐                 │
│   │        │ comint REPL  │ run-file   │ jupyter (opt) │  executors      │
│   │        └──────┬───────┴─────┬──────┴──────┬────────┘                 │
│ UI: mode-line lighter, *spark-connect-ops*, progress, log buffer,       │
│     transient menu, catalog browser, DataFrame viewer                    │
│                                                                          │
│ spark-connect-databricks (optional, autoloaded only when enabled)        │
│  auth profiles ─ clusters ─ jobs/runs ─ bundles   ── `databricks` CLI ── │
└───────────────┼─────────────┼─────────────┼──────────────────────────────┘
                ▼             ▼             ▼        (user's project venv)
        python + spark_connect_emacs helper (stdlib only, via PYTHONPATH)
                │  SparkSession.builder.remote(...)  or DatabricksSession
                ▼
        Spark Connect server (OSS / any vendor)  or  Databricks cluster/serverless
```

### 4.2 Components

**(1) Profiles and executor identity** (`spark-connect-profile.el`,
`spark-connect-exec.el`). A profile is a plist: `:name`, `:kind`
(`spark-connect` | `databricks`), `:python` (interpreter or venv dir;
default: auto-detect `.venv` from the `project.el` root, then
`python-shell-interpreter`), `:env` (extra non-connection env vars),
and `:confirm` (ask before every submission; for production targets).
For OSS: `:remote` (`sc://host:port/;use_ssl=true;user_id=…`), with the
token looked up via `auth-source` by host. For Databricks:
`:databricks-profile`, and **exactly one** of `:cluster-id` or
`:serverless t`. Profiles come from `spark-connect-profiles`
(defcustom) and `.dir-locals.el`. Databricks auth profiles are listed
with `databricks auth profiles -o json`.

_Executors are owned objects, not "the Python shell"_ (F2):

- The registry key is `(canonical project root via file-truename,
profile fingerprint)`. The fingerprint is a hash over the resolved
  profile, the absolute interpreter path and the resolved env (section
  4.4) minus secrets. Editing a profile therefore yields a _new_
  executor; it never mutates a running one.
- Process and buffer names are **unique per executor instance**:
  `*sc:<profile>@<project-basename>-<8-hex root hash>-<6-hex
fingerprint>#<generation>*`. `generation` is a counter that increases
  every time an executor starts in this Emacs session. So two projects
  with the same basename, two revisions of a same-named profile, or a
  restart after a crash never share a name.
- **Creation is verified, not assumed.** `python-shell-make-comint`
  silently reuses a buffer that already has a live process
  (`comint-check-proc`). Before calling it the plugin asserts that no
  buffer with the new name exists. After the call it checks that the
  returned buffer's process is a _new_ process object that is not in
  the registry yet. If not, it signals an error instead of using that
  process. The registry stores the process object, and every send goes
  to that object, never to a name lookup.
- The plugin **never adopts** an existing `run-python` shell
  (`*Python*` or project-dedicated).
- In buffers of a project whose active executor is bound,
  `spark-connect-python-mode` (a buffer-local minor mode) holds a
  reference to that **executor object** and sets
  `python-shell-buffer-name` buffer-locally to its exact unique name. So
  both our commands and stock `python.el` commands reach that
  generation, and not the global `*Python*` or an older generation.
  Rebinding the project (profile switch, profile edit, restart) updates
  this variable in every buffer of the project.
- Tests: edit a same-named profile (change host, then interpreter) while
  the previous process stays alive. Both generations must exist side by
  side, and sends from project buffers must reach only the newly
  selected one. Also: two projects with the same basename, and an
  existing plain `*Python*` shell left untouched.
- **Switching profile** = point the project at another executor. If one
  with that key already exists, reuse it; otherwise start one. The old
  executor keeps running (it is listed in `spark-connect-executors`);
  it is never silently re-targeted. If it has a running submission, the
  switch asks whether to cancel it, leave it running, or abort the
  switch.
- **Status shows the executor's actual target**: the host/compute
  reported in its `hello`, not the selected profile. Until the new
  executor sends `hello`, the mode line shows `SC:<old>→<new>…`.
- **Lifecycle**: process exit or channel EOF marks the executor `dead`
  and all its open submissions `unknown`. A user-level `spark.stop()` or
  a user-created replacement session is detected by the helper (the
  session id in the next `req.start` differs from `hello`) and reported
  as `session-changed`; the UI then warns that tags and progress may no
  longer apply. `kill-emacs-hook` stops all owned executors.

**(2) Connection by environment variables, not code.** The plugin
configures the executor's process environment the way the client
libraries already expect: `SPARK_REMOTE` for OSS PySpark, and
`DATABRICKS_CONFIG_PROFILE` plus `DATABRICKS_CLUSTER_ID` /
`DATABRICKS_SERVERLESS_COMPUTE_ID=auto` for Databricks Connect. So a
user's unmodified `SparkSession.builder.getOrCreate()` /
`DatabricksSession.builder.getOrCreate()` connects to the selected
target, and the same script runs the same way outside Emacs. Secrets
travel only through the environment (C8). The env is built by **one
resolver** that is shared with the Databricks CLI calls (section 4.4).

**(3) Python helper `spark_connect_emacs`** (shipped inside the Elisp
package under `python/`, **stdlib-only**, prepended to `PYTHONPATH` by
Emacs; never pip-installed into the user's venv, C2):

- `python -m spark_connect_emacs probe`: prints JSON with the Python
  version, which of `pyspark` / `pyspark-client` / `databricks-connect` is
  installed, their versions, a conflict check (both installed), and
  capability flags (`progress_handler`, `tags`). It does not connect.
  Emacs uses it for `spark-connect-doctor` and pre-flight checks
  (section 4.7).
- `spark_connect_emacs.attach()`: run once by the executor bootstrap.
  It gets or creates the session (`DatabricksSession` if the profile kind
  is `databricks`, else `SparkSession`), sets it as the default/active
  session, opens the channel (address and one-time token from
  `SPARK_CONNECT_EMACS_CHANNEL` / `…_TOKEN`), registers the progress
  handler when available, installs the log handler, starts the reader,
  writer and worker threads (section 4.6) and sends `hello`. A failure
  is reported as `attach.error` and does **not** kill the REPL. The user
  can retry with `spark-connect-reattach`.
- `spark_connect_emacs.submit(...)`: the execution envelope (section
  4.5).
- `python -m spark_connect_emacs.run`: the run-file runner (section
  4.8).

**(4) Channel** (`spark-connect-channel.el`). Emacs runs one
`make-network-process :server t` per Emacs session. It uses a Unix domain
socket in a private directory (0700) under `temporary-file-directory`.
Where local sockets are not available (Windows) it falls back to TCP on
`127.0.0.1` with a random port. Every connection must present its
one-time token first; the token also tells Emacs which executor the
connection belongs to. Wire format: newline-delimited JSON with
`protocol: 1` in `hello`. Messages, Python → Emacs:
`hello{protocol, session_id, target{host, compute}, versions, caps}`,
`req.start{req, origin, source}`, `req.end{req, status, error?}`,
`op.start{req?, op_id}`, `op.progress{req?, op_id, stages, inflight}`,
`op.end{req?, op_id}`, `log{level, logger, msg}`, `dropped{logs,
progress}`, `attach.error`, `session-changed`, and replies.
Emacs → Python: `interrupt{req}`, `ping`, `catalog{id, query}`,
`cancel-catalog{id}`. Flow control and limits: section 4.6.

**(5) Executors** (`spark-connect-exec-*.el`), behind a small generic
interface (`cl-defgeneric`: `start`, `submit`, `interrupt`, `stop`,
`buffer`, `target`):

- **comint (default)**: an owned `python.el` comint process (see (1))
  with our env and the bootstrap `import spark_connect_emacs as _sce;
spark = _sce.attach()`. Code is sent through the submission envelope
  (section 4.5).
- **run-file**: a `compilation-mode` buffer running the runner (section
  4.8), with clickable tracebacks and progress on the same channel. This
  matches "Run with Databricks Connect".
- **jupyter (optional, P2)**: `spark-connect-jupyter.el`, loaded only
  if `jupyter` (emacs-jupyter) is installed. It runs the same bootstrap
  in the kernel and wraps cells in `_sce.submit`. It is the path for
  rich/notebook output, but _requires_ `ipykernel` in the user's venv
  (the user installs it; the plugin checks it). An EIN adapter is not
  planned but is possible on the same interface.
- **debug (P2)**: run-file under `debugpy` driven by `dape`, if
  installed.

**(6) UI** (core):

- **Mode-line lighter** (global minor mode `spark-connect-mode`):
  `SC:<executor target>` (from `hello`, see (1)) plus a state glyph
  (dead / connecting / idle / N running / `!` session-changed). While an op runs it shows the stage progress `[st 3/10 ▓▓░
45%]`. For Databricks profiles it adds the cluster state
  (`RUNNING`/`PENDING`/`TERMINATED`). Clicking it opens the transient
  menu.
- **`*spark-connect-ops*`**: `tabulated-list-mode` with _submissions_
  (origin, start, duration, status `running|ok|error|interrupted|unknown`)
  and, on clients with the progress API, their Spark operations as child
  rows (op id, stages, progress). Keys: `k` cancel (section 4.5), `RET`
  details, `g` refresh. Untracked progress (from input that bypassed the
  envelope) is listed under an `untracked` row.
- **`*spark-connect-log*`**: forwarded log records with level faces,
  plus executor stderr. Size-capped.
- **Transient menu** `spark-connect-dispatch`: profile, connect, REPL,
  send, run file, interrupt, doctor, ops, catalog, plus a Databricks
  sub-menu when that module is loaded.
- **Catalog browser** (P1): catalogs → schemas → tables → columns,
  queried through the catalog worker (section 4.6); `RET` inserts the name or shows
  `df.limit(n)`.
- **DataFrame viewer** (P2): `_sce.view(df, n=1000)` sends an Arrow-free
  JSON preview over the channel into a `tabulated-list`/`org` table
  buffer.
- **Spark UI link**: for OSS, an optional `:spark-ui-url` profile key
  (opened with `browse-url`). For Databricks, the cluster or run URL.
- Notifications on long-op completion via `notifications-notify` /
  `alert` if available.

**(7) Databricks module** (`spark-connect-databricks*.el`, optional; it
requires the `databricks` CLI ≥ a pinned minimum, checked with
`databricks version`). All calls are async `make-process` with `-o json`
and the env from the shared resolver (section 4.4):

- auth: list profiles, `databricks auth login` in a terminal buffer for
  OAuth U2M, `auth describe` for status.
- clusters: list, select, start (`clusters start`), status polling with
  exponential backoff (only while a Databricks profile is active and
  the cluster is not terminal-stable); feeds the compatibility check
  (section 4.7).
- jobs/runs: `*databricks-runs*` tabulated list (`jobs list-runs`,
  `--active-only` toggle), run details (`jobs get-run`), task output
  (`jobs get-run-output`), cancel/repair, auto-refresh timer while visible,
  notification on state change. This covers the "get the job status"
  story.
- bundles: `bundle validate/deploy/run/summary -o json` with a target
  selector. `bundle summary` provides the resource tree. Runs stream in
  a `compilation-mode` buffer.
- "Run file as workflow": `jobs submit` of a one-off run with a
  `spark_python_task`. This needs the file in the workspace: done through
  a bundle when the project has one (P1), else `workspace import` (P2).
- **Polling stays on the CLI** (first iteration). A REST fast path
  (`databricks auth token` + `url.el`) is a deferred optimisation, to be
  taken only if measured spawn latency hurts.

### 4.3 What "parity" means here

Parity means the _workflows_ match, not the UI. Each VS Code feature gets
an Emacs-native counterpart or an explicit "won't do". See the matrix in
section 5.

### 4.4 Environment resolution (one resolver for executor and CLI)

`spark-connect-resolve-env (profile)` returns the full
`process-environment` for **both** the executor and every Databricks CLI
call made for that profile. So the work and the status always resolve
the same host, credentials and compute (F4). The Databricks unified
config loads env values _before_ profile values and does not override
fields that are already set. An inherited `DATABRICKS_HOST`/`TOKEN`
would therefore beat the selected profile. Hence:

1. **Scrub** inherited connection/auth variables matching
   `spark-connect-env-scrub-regexps` (defcustom). Default: `^DATABRICKS_`,
   `^SPARK_REMOTE$`, `^SPARK_CONNECT_MODE_ENABLED$`,
   `^ARM_(CLIENT_ID|CLIENT_SECRET|TENANT_ID)$`. `PATH`, `HOME`, proxies and
   `az` CLI state are kept, so Azure CLI auth still works.
2. **Set** only what the profile defines: OSS → `SPARK_REMOTE` (token
   from auth-source); Databricks → `DATABRICKS_CONFIG_PROFILE`, optional
   `DATABRICKS_CONFIG_FILE` (from `spark-connect-databricks-config-file`),
   and exactly one compute selector.
3. **Validate**, failing loudly instead of guessing: the profile `:env`
   may not contain scrubbed keys (it would bypass the profile; error).
   `:cluster-id` and `:serverless` are mutually exclusive, and exactly one
   is required. An OSS profile must have `:remote`.
4. **Show** the resolved non-secret target before the first execution:
   host (for Databricks, from `databricks auth describe -o json` run with
   the same env) and compute. Profiles with `:confirm t` ask on every
   submission.

Tests: profile B selected while Emacs inherited host/token for A;
inherited `SPARK_REMOTE`; both compute selectors set; `:env` trying to
set `DATABRICKS_HOST`.

### 4.5 Submission envelope and cancellation (execution contract)

`attach()` alone cannot see where a REPL submission begins and ends, so
tracking is defined per **submission** (F1). A submission is one block
of code sent by a plugin command. It is separate from the **Spark
operations** that block may trigger: zero, one or many.

**Sending.** `spark-connect-send-{region,defun,buffer,cell,file}` (bound
in `spark-connect-python-mode` over the `python.el` keys `C-c C-r`,
`C-c C-c`, `C-M-x`…) write the code to a temp file, the same way
`python.el` already does for large sends, and send the REPL **one line**:
`_sce.submit("<req-id>", "<tmpfile>", origin="<buffer>:<line>")`.
`submit` runs on the REPL's main thread:

```python
def submit(req, path, origin):
    with _user_sigint_handler():               # see "SIGINT handler (user only)" below
        with state_lock:                       # short, no I/O
            st.current, st.pending_cancel, st.ki_raised = req, None, False
            st.phase = STARTING
        emit("req.start", req=req, origin=origin)   # put_nowait, never blocks
        spark.addTag(tag := f"sce-{req}")      # client-side, thread-local, no RPC
        status, err = "interrupted", None      # default if a KI escapes classification
        try:
            try:
                st.phase = EXECUTING
                exec(compile(src_with_offset(path, origin), origin_file, "exec"),
                     __main__.__dict__)
                st.phase = FINALIZING; status = "ok"
            except KeyboardInterrupt:
                st.phase = FINALIZING; status = "interrupted"; print("Interrupted")
            except BaseException as e:
                st.phase = FINALIZING; status, err = "error", summarize(e)
                print_traceback(e)
        except KeyboardInterrupt:              # the single KI landed inside a handler above
            st.phase = FINALIZING; status = "interrupted"
        # --- FINALIZING: the handler never raises from here on ---
        spark.removeTag(tag)                   # client-side, no RPC
        with state_lock:
            st.current = st.pending_cancel = None
            st.phase = IDLE
        emit("req.end", req=req, status=status, error=err)
    # a user C-c that arrived outside EXECUTING is re-raised here, after req.end
```

Line offsets and the original filename go into `compile`, so tracebacks
point into the source buffer. **Success or failure comes from `submit`,
never from the progress callback**, whose finish fires on exceptional
exits too. The plugin removes only its own tag; user tags stay
untouched.

**What is observable, by client:**

|                                       | PySpark ≥ 4.0 (progress API)      | PySpark 3.5 / no progress API                  |
| ------------------------------------- | --------------------------------- | ---------------------------------------------- |
| Submission start/end/status           | yes                               | yes                                            |
| Per-operation rows and stage progress | yes, attributed to `_current.req` | **no**: only the submission row with a spinner |
| Cancel the submission's Spark work    | `interruptTag`                    | `interruptTag` (3.5 has tags)                  |

**Coverage, stated explicitly.** Code typed directly at the REPL prompt,
stock `python.el` sends from buffers without `spark-connect-python-mode`,
and `ob-python :session` blocks are **untracked**. They work, and on
4.0+ their progress appears under `untracked`, but they have no
submission status. They can only be cancelled by an explicit,
user-confirmed `spark-connect-interrupt-all` (`interruptAll`, plus the
ordinary comint `C-c C-c`, which the user sends themselves). An `ob-spark-connect` wrapper that routes org-babel blocks
through `submit` is P1.

**Submission states and dispatch.** Each executor has a submission
queue in Emacs, and **at most one submission in flight**. States:
`queued` (in Emacs only, not sent) → `running` (sent; `req.start`
seen) → `cancel-pending` → terminal `ok | error | interrupted |
unknown`. Emacs sends the next queued submission only after the
current one is terminal (`req.end`, or `unknown` on channel loss). So
while a cancellation is unsettled, nothing new can start in the
executor through the plugin. Input typed directly at the REPL bypasses
this queue and is untracked (see above).

**Cancellation semantics** (`k` in ops, `C-c C-k`): cancel targets
**the whole submission**, not just the current Spark action:

- **queued**: removed from the Emacs queue and marked `interrupted`.
  It never reaches Python.
- **terminal**: ignored with a message. Stale ops rows cannot cancel
  anything.
- **running**: Emacs sends only `interrupt{req}` and marks the row
  `cancel-pending`. **Emacs never signals the process itself.**

**Phases.** Inside the helper a submission moves through `STARTING →
EXECUTING → FINALIZING → IDLE` (`st.phase`). An interrupt may take
effect **only in `EXECUTING`**. Teardown (tag removal, state clearing,
`req.end`) runs in `FINALIZING`, where no handler raises. The phase is
a single attribute store, read by the handlers on the main thread
between bytecodes, so it needs no lock.

**Two signals, two meanings.** Checking the request id before a signal
is _sent_ says nothing about what that signal means when it is
_handled_. A wakeup claimed for A can be delivered after A has finished
and B has started. So helper wakeups **never use SIGINT**, and a SIGINT
always means "the user". The helper uses a separate **cancel signal**:

- POSIX: `SIGUSR1`, sent to the main thread with
  `signal.pthread_kill(main_ident, SIGUSR1)`. It is a real signal, so it
  also interrupts blocking syscalls.
- Windows: `_thread.interrupt_main(signal.SIGBREAK)` (Python ≥ 3.10
  allows a signum), which simulates the signal at the next bytecode
  boundary. _To verify on Windows (Q7)._ If it does not work, Windows
  local cancel is unavailable, and cancel falls back to `interruptTag`
  plus the escalation.

**The cancel handler validates; it never trusts origin.** It is
installed by `attach()` **for the whole executor lifetime**, never
restored after a submission. So a late cancel signal at the idle prompt
reaches our handler, and not the default `SIGUSR1` action (which would
kill the process). The handler raises `SceCancelled` (a subclass of
`KeyboardInterrupt`) **only if** `st.phase == EXECUTING`,
`st.pending_cancel == st.current`, and no interrupt has been raised yet
in this submission (`st.ki_raised`). Before raising it sets
`st.ki_raised = True` and clears `pending_cancel`. In every other case
it does nothing. The consequences:

- A wakeup for A delivered while B executes: `pending_cancel` is `None`
  (cleared in A's end transition and reset at B's start), so it is
  dropped and B is unaffected.
- A wakeup for A delivered while B executes _and B has its own claimed
  cancel_: raising cancels B, which is exactly what B's claim asked for.
  B's own wakeup then finds `ki_raised` set and is dropped.
- A wakeup delivered at the idle prompt or in `FINALIZING`: dropped.
- An external `SIGUSR1` (or a real Ctrl-Break on Windows): only acts if
  a cancel has actually been claimed for the running submission.
  Misattribution can only ever lead to a drop, never to a wrong
  interrupt.

**SIGINT handler (user only)**. It is installed for the duration of
`submit` and the previous handler is restored afterwards, so the normal
REPL Ctrl-C works at the prompt. In `EXECUTING` with `st.ki_raised`
unset, it sets `ki_raised` and raises a plain `KeyboardInterrupt`
(one-shot, shared with the cancel handler). In `EXECUTING` after an
interrupt was already raised, it is dropped with a hint to use the
escalation. Outside `EXECUTING` it is remembered and re-raised by
`submit` after `req.end`, so the user's interrupt is never lost and
never breaks teardown.

**Handler hijack check.** At each `submit` start the helper checks that
`signal.getsignal(SIGUSR1)` is still its handler. User code or a
library may have replaced it. If so, the helper reinstalls it if it can
and emits `cancel.local-unavailable` once if it cannot. Cancel then
relies on `interruptTag` plus the escalation.

**Interrupt worker** (section 4.6). The state lock is held only for
in-memory checks, never across I/O or RPC:

1. Under `state_lock`: if `st.current != req` or `st.phase !=
EXECUTING`, reply `cancel.stale` and stop. If `st.pending_cancel ==
req` already, stop (duplicate). Otherwise set `st.pending_cancel =
req`. Release the lock.
2. Send the cancel signal (outside the lock). A delay here is harmless,
   because the handler validates when the signal is handled.
3. Reply `cancel.ack{req}`.
4. Remote cancellation, `interruptTag("sce-"+req)`, runs on a separate
   short-lived **remote-interrupt thread**. At most 2 can be
   outstanding, each with a 10 s watchdog. The result is reported as
   `cancel.remote{req, ok|failed|timeout|skipped}`. A stalled or failing
   RPC never holds `state_lock`, never delays the local wakeup, and never
   blocks the main thread's end transition. A late `interruptTag` for
   A's tag after A finished matches no operation (B carries a different
   tag), so it is harmless.

**Why exactly one terminal event.** At most one `KeyboardInterrupt`
(`SceCancelled` or a user one) is raised per submission, and only in
`EXECUTING`. Every point in that phase is covered by one of the two
nested `except KeyboardInterrupt` clauses: the inner one during user
code, the outer one if the interrupt lands in the window between an
inner handler starting and its `st.phase = FINALIZING` store. After
that store neither handler can raise, so `removeTag`, state clearing and
`req.end` always run. `addTag`/`removeTag` are client-side thread-local
operations with no RPC, which a test verifies.

- **Cooperative, not guaranteed.** `KeyboardInterrupt` can be caught
  (and swallowed) by user code or libraries, and C extensions may delay
  it. So the local part of the cancel is best effort. Unwinding `submit`
  normally means no further action starts from that submission, but
  only **terminating the process** is definitive.
- The UI shows `cancel-pending` after `cancel.ack`, and `interrupted`
  only when `req.end` arrives. The two are kept distinct.
- **Escalation**: if no `req.end` arrives within
  `spark-connect-cancel-timeout` (10 s), or on a repeated cancel, the UI
  offers **kill and restart the executor**. Open submissions become `unknown`,
  and the server-side work is interrupted by tag from a short-lived CLI
  call (`python -m spark_connect_emacs interrupt --session-id <id from
 hello> --tag …`, same env). Rejoining a session by id via the
  connection string has to be verified per client (OSS / DB Connect).
  Where it does not work, this step is reported as "best effort: server
  work may continue".

Tests (real Python, against Spark 3.5 and 4.x servers): success; Python
exception; Spark exception; two actions in one submission; no action;
consecutive submissions (no tag leak); cancel during a Spark action;
**deterministic injection** of a cancel (and of a user SIGINT) at
these test hooks: before `exec`, right after `exec` returns, inside
each inner `except` before the phase store, during `removeTag`, while
waiting for the end `state_lock`, and during the `req.end` emit. Each
case must produce exactly one terminal event, no leaked tag, and a
usable next submission. **Delayed-wakeup tests:** pause the worker
_after_ a successful validate-and-claim of A, and release the cancel
signal (a) after A finished while B executes (B unaffected, B's status
`ok`), (b) after A finished at the idle prompt (process alive, prompt
usable), (c) while B executes with its own claimed cancel (B
`interrupted`, exactly once). In every case a genuine user `C-c C-c`
must still interrupt a running tracked submission and the idle prompt.
Also: user code that replaces the `SIGUSR1` handler
(`cancel.local-unavailable`, and escalation still works). Also: an
`interruptTag` that stalls (local
cancel still lands, `cancel.remote{timeout}`) or raises
(`cancel.remote{failed}`);
cancel during a local `time.sleep` loop; cancel while a catalog request
is stalled; a delayed cancel of A injected across the A→B boundary (B
must be unaffected, with `cancel.stale` returned); cancel of a queued
request; repeated cancel; user code that catches `KeyboardInterrupt`
(escalation must be offered); plain `C-c C-c` in the REPL with no
pending cancel.

### 4.6 Helper threading, flow control and limits

Threads in the helper (F3, F5):

- **reader**: reads control frames and only dispatches them. It never
  calls Spark itself.
- **interrupt worker**: a dedicated thread that only validates and
  claims cancellations under a short `state_lock` and wakes the main
  thread (section 4.5). It never performs RPCs. `interruptTag` /
  `interruptAll` run on short-lived remote-interrupt threads (at most 2
  outstanding, 10 s watchdog). It is never blocked behind catalog work
  or a stalled interrupt RPC.
- The SIGINT and cancel-signal handlers only _read_ `st` (apart from
  the one-shot fields) and never take `state_lock`. So
  it cannot deadlock with the main thread holding the lock during a
  transition.
- **catalog worker**: a single thread with a bounded queue (depth 4;
  extra requests are rejected with `busy`). Each request carries a
  deadline (default 15 s) that Emacs enforces. When it expires Emacs
  shows a timeout, and the late reply is discarded. A stalled catalog
  call can therefore never block cancellation.
- **writer**: the only thread that touches the socket. It has a send
  timeout and is fed by two queues:
  - _reliable_ (`hello`, `req.*`, `op.start/end`, replies; bounded at
    1024). These are low-rate. If it is ever full, the helper treats the
    channel as broken (below).
  - _telemetry_ (progress, logs; bounded at 256). Progress is
    **coalesced per op** (latest wins). Logs are dropped when the queue
    is full and counted in a periodic `dropped{…}` frame. Producers use
    `put_nowait`.

Guarantees: progress callbacks and log handlers never block and never
raise into user code; every telemetry exception is swallowed and
counted. Frames are capped at 64 KiB, and log messages are truncated at
8 KiB. When the writer fails (timeout or EPIPE), the helper marks the
channel dead, disables telemetry, and lets user code continue. Emacs
sees EOF and marks the executor's open submissions `unknown`.

Emacs side: the process filter appends to a per-connection buffer and
parses complete lines only. A partial frame over 1 MiB, or an invalid
token, drops the connection. UI refresh (mode line, ops buffer) is
throttled by a timer (≤ 5 Hz) that reads the latest coalesced state, so
a progress flood costs O(1) redraws. The log buffer is capped by line
count.

Tests: a peer that stops reading (user code must keep running, memory
stays bounded, cancel still works via the cancel signal); a log flood of 10⁶ records
(bounded memory, `dropped` reported); a channel killed mid-submission
(`unknown`).

### 4.7 Compatibility checks (doctor and pre-flight)

`spark-connect-doctor` combines `probe` (local) with target facts
(cluster `spark_version`, `data_security_mode`, serverless flag from the
CLI). It reports each rule as **error** (block execution), **warning**,
or **info**. The rules live in one data table
(`spark-connect-compat-rules`) so they can be updated without code
changes. Initial content, **to be re-verified against the Databricks
Connect requirements page when it is implemented**:

| Rule                                                                                           | Level                   |
| ---------------------------------------------------------------------------------------------- | ----------------------- |
| `pyspark` and `databricks-connect` both installed                                              | error                   |
| Databricks profile but no `databricks-connect` / OSS profile but no `pyspark`/`pyspark-client` | error                   |
| DB Connect client newer than the cluster DBR                                                   | error                   |
| DBR newer than the client (supported per the current docs; matching versions recommended)      | warning                 |
| Serverless with DB Connect older than the documented minimum (15.4.10 per the current docs)    | error                   |
| Local Python minor ≠ compute Python minor (Python UDFs fail)                                   | warning                 |
| Cluster access mode not supported by Databricks Connect (e.g. no-isolation shared)             | error                   |
| OSS client minor newer than the server minor                                                   | warning                 |
| No progress API (client < 4.0)                                                                 | info: degraded progress |

### 4.8 Run-file runner semantics

`python -m spark_connect_emacs.run [--module] [--] TARGET [ARGS…]` must
behave like `python TARGET ARGS…` (or `python -m TARGET`) (F6):

- `sys.argv = [TARGET, *ARGS]`: the helper's own arguments are
  stripped.
- `sys.path[0]` = directory of `abspath(TARGET)`, replacing the cwd
  entry that `-m` inserted. For `--module` it is the cwd, as with
  `python -m`. The helper directory reaches `sys.path` only through
  `PYTHONPATH`. That entry is documented, and it is harmless because the
  package name is unique.
- The working directory is the process cwd chosen by Emacs. The default
  is the buffer's `default-directory`, with a per-profile option for the
  project root, exactly as if the user had run `python` from there.
- `__main__`: `runpy.run_path(TARGET, run_name="__main__")` or
  `runpy.run_module(TARGET, run_name="__main__", alter_sys=True)`, so
  `__file__`, `if __name__ == "__main__"` and pickling of `__main__`
  functions (UDFs) work.
- Exit status: `SystemExit` propagates unchanged. Uncaught exceptions
  print a traceback with the runner frames removed, and exit 1.
- The whole script is one submission (section 4.5), with the same tag
  and cancel behaviour.
- **Attach failure policy**: the session is attached eagerly before the
  script starts. A connection failure exits with code **3** and an
  `attach.error` frame, and the compilation buffer says "connection failed
  before the script started". That keeps it distinguishable from script
  errors. `--no-attach` runs the script plain (no progress or cancel by
  tag).

Tests: a differential subprocess harness comparing the runner with plain
`python` on a script importing a sibling module, positional and flag
arguments, relative file reads, `SystemExit(0/2)`, an uncaught exception,
and `--module` on a package.

## 5. Parity matrix and phasing

| VS Code feature                                | Emacs counterpart                                                                                                                                                 | Phase                         |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| Select connection/profile                      | `spark-connect-select-profile` (+ dir-locals)                                                                                                                     | P0                            |
| Env check / install DB Connect                 | `spark-connect-doctor` (probe + version check). Prints the exact `pip install databricks-connect==X.Y.*` command; installs only on an explicit `C-u` confirmation | P0 doctor / P1 install        |
| Run file with DB Connect                       | run-file executor                                                                                                                                                 | P0                            |
| Interactive cells                              | comint REPL + send region/defun/buffer; `# %%` cell send (`spark-connect-send-cell`)                                                                              | P0                            |
| Progress / cancel                              | mode-line, ops buffer, cancel of the whole submission (section 4.5)                                                                                               | P0 (progress degrades per C4) |
| Logs                                           | log buffer                                                                                                                                                        | P0                            |
| Auth (OAuth/PAT/Azure)                         | Databricks CLI unified auth                                                                                                                                       | P1                            |
| Cluster select/start/state                     | databricks clusters                                                                                                                                               | P1                            |
| Serverless                                     | `:serverless t`                                                                                                                                                   | P1                            |
| Job/run status                                 | `*databricks-runs*`                                                                                                                                               | P1                            |
| Bundles validate/deploy/run, resource explorer | bundle commands + summary tree                                                                                                                                    | P1                            |
| Catalog explorer                               | catalog browser (works on OSS too)                                                                                                                                | P1                            |
| Notebooks with DB Connect                      | jupyter adapter (optional); org-babel via `ob-python` session today                                                                                               | P2                            |
| Debug with DB Connect                          | dape + debugpy run-file                                                                                                                                           | P2                            |
| Run file as workflow                           | `jobs submit`                                                                                                                                                     | P2                            |
| Upload-and-run via command execution API       | **Won't do initially**: covered by Connect and workflow runs                                                                                                      | —                             |
| Workspace sync (legacy)                        | `bundle sync` passthrough                                                                                                                                         | P2                            |
| Pipelines (DLT) views                          | deferred                                                                                                                                                          | P3                            |
| Remote executors over TRAMP                    | deferred (needs channel port forwarding)                                                                                                                          | P3                            |

**P0 exit criterion:** with only Emacs and a venv containing
`pyspark[connect]` ≥ 3.5 (progress needs ≥ 4.0) against a local
`start-connect-server.sh`, a user can select a profile, start the REPL,
send code, see submission status (and stage progress on 4.x), cancel a
long query _and_ a local Python loop, and run a multi-file script with
arguments. All of this works with the Databricks module not even on
`load-path`. It must pass against **real Spark 3.5 and 4.x Connect
servers** in CI, with the test lists of sections 4.4–4.8.

**P1 Databricks acceptance gate** (credential-gated: a manual checklist,
or a CI job that runs only when `SCE_DATABRICKS_PROFILE` is set; OSS-only
CI stays green without credentials): auth through the CLI; `hello`
target equal to the resolved target (section 4.4); session identity
(R3); a simple action; cancel of a running submission; the same on
serverless if serverless is claimed. Databricks features are not
announced until this gate passes on the versions listed in the
compatibility table.

## 6. Proposed repository layout

```
lisp/
  spark-connect.el            ; entry, defgroup, minor mode, autoloads
  spark-connect-profile.el
  spark-connect-channel.el
  spark-connect-exec.el       ; generic executor interface
  spark-connect-exec-comint.el
  spark-connect-exec-run.el
  spark-connect-ui.el         ; mode-line, ops, log, transient
  spark-connect-catalog.el
  spark-connect-jupyter.el    ; optional (soft-depends on `jupyter`)
  spark-connect-databricks.el ; optional entry
  spark-connect-databricks-cli.el
  spark-connect-databricks-clusters.el
  spark-connect-databricks-jobs.el
  spark-connect-databricks-bundle.el
python/spark_connect_emacs/
  __init__.py  __main__.py (probe)  run.py  _channel.py  _progress.py  _session.py  _control.py
test/
  elisp/  (ERT; fake channel peer; fake `databricks` CLI script on PATH)
  python/ (pytest; unit tests with a fake session; integration against local Spark Connect)
docs/design/spark-connect-emacs.md
```

Distribution: a single MELPA package whose recipe `:files` includes
`python/`. Elisp finds the helper via
`(file-name-directory (locate-library "spark-connect"))`. A second
MELPA package for Databricks is possible later. Optionality comes from
the separate feature and lazy loading, not from packaging.

## 7. Consequences

**Positive**

- Works with any Spark Connect server; Databricks is an add-on (C5).
- No pip installs into the user's venv, which sidesteps the
  pyspark/databricks-connect conflict (C2).
- The default path has no native modules and no Jupyter.
  `python.el`/`ob-python` users get Spark features in tools they already
  use.
- The same status channel serves the REPL, run-file and Jupyter, so the
  UI is written once.
- The Databricks control plane reuses the CLI's auth and API coverage,
  like the reference extension.

**Negative / costs**

- Two languages (Elisp plus a small Python helper) and a home-grown wire
  protocol to version (`hello` carries `protocol: 1`).
- The comint default gives text-only output. Rich output depends on the
  optional Jupyter adapter or our DataFrame viewer.
- Spawning a CLI process per poll is heavier than REST. It is acceptable
  at the intended polling rates (≥ 5 s, only while visible/active).
- Users of the Databricks module must install the `databricks` CLI.

## 8. Risks and mitigations

| #   | Risk                                                                                                                      | Mitigation                                                                                                                                                                                                               |
| --- | ------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| R1  | Progress API missing or different across PySpark / DB Connect versions (C4)                                               | Feature-detect in `probe`. Without it, only submission-level status is shown, which needs no Spark hook (section 4.5 table). Integration matrix with real 3.5 and 4.x servers.                                           |
| R2  | Calling the Spark Connect client from helper threads while the main thread executes (thread safety)                       | Separate interrupt and catalog workers (section 4.6); interrupt APIs are designed for concurrent use. If catalog calls on the shared session cause contention or bugs, move the catalog to a separate read-only session. |
| R8  | Wrong target: stale executor, adopted shell, or inherited auth env                                                        | Owned executors keyed by root and profile fingerprint (4.2(1)); a single env resolver with scrubbing and validation (4.4); status shows the `hello` target.                                                              |
| R9  | Slow or non-reading peer stalls user code                                                                                 | Bounded queues, a non-blocking producer, and writer timeouts (4.6).                                                                                                                                                      |
| R3  | `getOrCreate()` in user scripts not returning the session that `attach()` created (losing tags/progress)                  | `attach()` sets the default/active session. Integration test asserting identity for OSS and DB Connect. If it fails, attach the handler lazily through a builder hook.                                                   |
| R4  | Databricks CLI JSON shapes drift                                                                                          | Pin a minimum CLI version. Parse defensively (`alist-get` with defaults). Contract tests on recorded fixtures.                                                                                                           |
| R5  | `python.el` REPL quirks (output filtering, `python-shell-send-*` temp files, prompt detection) interfering with bootstrap | Bootstrap via `python-shell-first-prompt-hook` / `python-shell-setup-codes` style injection. ERT tests with a real Python.                                                                                               |
| R6  | Local socket security on multi-user hosts                                                                                 | 0600 socket in a private dir, plus a one-time token per executor, plus refusing non-local TCP.                                                                                                                           |
| R7  | Scope creep toward "full parity"                                                                                          | Phase gates (section 5). P1 starts only after the P0 exit criterion is met.                                                                                                                                              |

### 8.3 Testing strategy

- ERT unit tests for profiles, the channel parser, mode-line rendering, and
  CLI JSON parsing (fixtures).
- ERT integration with a stub Python peer speaking the NDJSON protocol.
- pytest for the helper, against real local Spark Connect servers
  **3.5 and 4.x** (one CI venv per version), covering the test lists in
  sections 4.4–4.8: envelope outcomes, cancellation, flow control, and
  differential runner tests.
- A separate venv with `databricks-connect` for import/probe and
  doctor-rule tests. Workspace behaviour goes through the credential-gated
  P1 gate (section 5).
- A fake `databricks` executable on `PATH` replaying fixtures for the
  Databricks module.

## 9. Open questions (for the operator)

- **Q1. Minimum versions.** Proposal: Emacs 29.1, PySpark client 3.5
  (degraded progress), with 4.0+ recommended. OK?
- **Q2a. Session rejoin for hard-cancel.** Can a fresh client
  `interruptTag` in an existing session by session id, on OSS and on
  DB Connect? This decides whether the kill-and-restart fallback in 4.5
  also stops server work.
- **Q7. Windows local cancel.** Does
  `_thread.interrupt_main(signal.SIGBREAK)` reliably deliver to our
  handler in a comint-hosted Python on Windows? If not, Windows gets
  remote-only cancel plus escalation.
- **Q2. Databricks Connect progress.** Which DB Connect versions expose
  the progress handler? This needs a check against a real workspace. It
  decides how good the Databricks status UX is in P0.
- **Q3. Jupyter.** Is emacs-jupyter as an _optional_ P2 adapter enough,
  or is a notebook (`.ipynb`) workflow a must-have earlier? EIN is not
  proposed as a dependency.
- **Q4. CLI as the Databricks dependency.** Is requiring the
  `databricks` CLI (rather than the Python `databricks-sdk`) acceptable?
  It is what VS Code does, and it keeps Python out of the control plane.
- **Q5. Non-Python clients.** Scala/Go Spark Connect clients are out of
  scope. The channel protocol is language-neutral, so other helpers can
  be added later. Confirm out of scope.
- **Q6. Naming/distribution.** Use `spark-connect` as the package prefix
  and ship one MELPA package with the Databricks module inside?
