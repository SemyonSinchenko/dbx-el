# databricks.el — a Databricks workbench for Emacs

- Status: **Proposed** (draft 3; supersedes draft 2, kept as
  `spark-connect-emacs.draft2.md`; draft 1 as `spark-connect-emacs.draft1.md`)
- Date: 2026-11-10
- Author: architect agent, revised with the operator

**Changes since draft 2.**

- **Databricks only.** OSS Spark Connect (`pyspark`, `pyspark-client`,
  `SPARK_REMOTE`, any vendor server) is dropped. Code runs through
  Databricks Connect only. The goal is now "as many Databricks features
  as possible", using the official VS Code extension as the floor, not
  the ceiling.
- **New feature areas:**
  - a SQL editor on SQL warehouses or the selected cluster/serverless;
  - a jobs and runs browser with fuzzy search through `completing-read`
    (Vertico, Helm, Ivy, Consult);
  - a bundle minor mode with resource graphs;
  - Lakeflow Declarative Pipelines (formerly DLT) with dataset graphs and
    diagnostics;
  - workspace notebooks, including a Jupyter/EIN path;
  - Unity Catalog and workspace browsers, AI Gateway chat via gptel.
- **Kept from draft 2:** the client–server execution design (Python
  companion over JSON-RPC on stdio), the policies, the Emacs 30 baseline
  and the JSON performance budget. Those sections are condensed here;
  draft 2 has the full pseudocode and test lists.
- **Verified against Databricks CLI v1.25.0** (downloaded 2026-11-10):
  the command groups this design relies on exist (section 3.3).

## 1. Context

### 1.1 Repository

Greenfield. The repository holds multi-agent tooling state and this
document. Section 6 lists the files this design creates. Dev container:
GNU Emacs 30.2 (built-in JSON, `jsonrpc.el` 1.0.25), Python 3.

### 1.2 What the operator wants

An Emacs workbench for Databricks, inspired by and broader than the
official VS Code extension.

User stories:

1. **Run PySpark code on Databricks from a buffer** (cluster or
   serverless): send a region, cell or file; see output, progress and
   cancel.
2. **Write and run SQL** against a SQL warehouse or the selected
   cluster/serverless; see results in a table; export them.
3. **Find and watch jobs**: fuzzy-search jobs and runs, see status, task
   graph, output and logs, cancel or repair, get notified.
4. **Work with Asset Bundles**: validate, plan, deploy, run, and
   **visualize** the bundle's resources and job task graphs.
5. **Work with Lakeflow Declarative Pipelines**: run, dry-run, watch
   events, see the **dataset graph** with status, jump from errors to
   source.
6. **Work with notebooks**: open workspace notebooks, edit them as
   source or `.ipynb` (EIN), run cells, run a notebook as a job, see its
   run status.
7. **Browse** Unity Catalog, the workspace, clusters and warehouses.
8. **Chat** with AI Gateway models (gptel).

### 1.3 Reference: the official VS Code extension

From the source of `databricks/databricks-vscode` v2.23.1 (read on
2026-11-09; full notes in draft 2, section 1.3). Paths are relative to
`packages/databricks-vscode/`.

| Area | What VS Code does |
| --- | --- |
| Run / debug | Sends `python dbconnect-bootstrap.py <file>` to a terminal; debug via debugpy. The bootstrap loads `.databricks/.databricks.env`, builds `DatabricksSession`, injects `spark`, `sql`, `udf`, `dbutils`, `getArgument`, runs the file with `runpy` |
| Auth into Python | In-process HTTP token server; Python gets `DATABRICKS_AUTH_TYPE=metadata-service` and a localhost URL, never a long-lived token (except PAT) |
| Notebooks | Jupyter extension plus an IPython startup script: `spark`/`dbutils`/`displayHTML`, `%fs`, Databricks source-format translation (`# COMMAND ----------`, `# MAGIC`, `%sql`, `%md`, `%sh`, `%run`), HTML DataFrames, an ipywidgets progress bar |
| Progress / cancel | Progress only in Jupyter. No `interruptTag`/`interruptAll`; cancel = kill the terminal |
| Env setup | `databricks environments setup-local` (uv) or a legacy pip flow; compatibility checklist |
| CLI | Bundled. Bundles, sync, `auth login`, `environments setup-local`, `ssh connect`, `aitools` |
| JS SDK | Clusters (10 s polling), command execution, jobs, pipelines, Unity Catalog, workspace FS |
| Upload and run | Command Execution API 1.2, classic clusters only |
| Run as workflow | One-off `jobs submit` with wrapper; `getRun` every 3 s; HTML result webview |
| Bundles | Resource explorer from `bundle summary`; validate on YAML change; `bundle run` in a pseudo-terminal; jobs polled every 3 s, pipelines every 5 s with incremental events; pipeline ERROR/WARN events with source locations become editor diagnostics; table-selective refresh |
| Explorers | Unity Catalog tree, workspace FS |
| AI | Unity AI Gateway chat provider (Responses route), AI-agent skills installer |
| Not present | SQL editor, jobs browser outside bundles, bundle or pipeline **graphs**, query history |

What we take from it:

1. The CLI is the control plane; the SDK is used only where the CLI is
   awkward. We use the CLI everywhere (no JS or Python SDK of our own).
2. Notebook source format support matters as much as `.ipynb`.
3. Pipeline diagnostics mapped to source locations are high value.
4. There is room to go beyond it: SQL, jobs outside bundles, graphs,
   progress and cancel.

## 2. Constraints and policies

### 2.1 Constraints

- **C1. No gRPC in Emacs.** Spark work goes through Databricks Connect
  in Python.
- **C2. Databricks Connect only.** The project venv must contain
  `databricks-connect` and must not contain `pyspark` or `pyspark-client`
  (they all provide the `pyspark` module). The plugin never modifies an
  existing venv. It may create one after confirmation (section 4.8).
- **C3. Compatibility is a rule set** (DBR, serverless version, Python
  minor, access mode), checked by the doctor (section 4.8).
- **C4. Progress is version-dependent.** The progress handler exists in
  recent PySpark-based clients. Which Databricks Connect versions expose
  it is open (Q2). Without it, job-level status only.
- **C5. The `databricks` CLI is required**, at a pinned minimum version
  (initially 1.25.0, the version this design was checked against). Users
  install it; MELPA cannot ship binaries. The doctor offers the official
  install command.
- **C6. Emacs ≥ 30.1.** Built-in JSON, bundled `jsonrpc.el` with
  `:events-buffer-config`, `vtable`, `tabulated-list-groups`,
  `mode-line-format-right-align`, `completion-preview-mode`.
- **C7. Async everywhere.** CLI calls and the companion server use
  `make-process`; no `call-process` on interactive paths except one
  documented case (the gptel key function, section 4.15).
- **C8. Secrets** never go into customization files, command lines or
  JSON-RPC traffic. Auth is by Databricks config profile; the CLI and the
  Python SDK resolve credentials themselves.
- **C9. Completion-framework neutral.** All pickers use
  `completing-read` with completion metadata (`category`,
  `annotation-function`/`affixation-function`, `group-function`). That
  makes them work with Vertico, Helm, Ivy, Icomplete, Consult, Marginalia
  and Embark without any hard dependency on them.

### 2.2 Policies

| #   | Policy | Why |
| --- | --- | --- |
| P-1 | **One interactive Python server per Emacs.** Run-file, SQL-on-warehouse and CLI calls are separate. | No executor registry; one obvious target |
| P-2 | **One Spark operation stream per server.** Catalog/preview/SQL-on-cluster requests queue behind code. | No concurrent use of the Connect client |
| P-3 | **Contexts are defined in the user's init.** `.dir-locals.el` may only name a context. | Repositories cannot choose interpreters or hosts |
| P-4 | **Local processes only.** Remote (`TRAMP`) `default-directory` is refused. SSH tunnels / remote IDE are out of scope. | Operator decision 2026-11-10 |
| P-5 | **Changing the context's profile or compute restarts the server** (confirmed if busy). | No mixed-target state |
| P-6 | **Only the attached session is tracked.** Sessions created by user code are detected and reported. | Honest status |
| P-7 | **Read before write.** Destructive actions (deploy, destroy, cancel, full refresh, overwrite a workspace notebook) always confirm and show what will change (`bundle plan`, diff). | Production workspaces |

## 3. Options considered

### 3.1 Code execution

Unchanged from draft 2: a **Python companion server** in the project
venv runs user code on its main thread; Emacs talks JSON-RPC 2.0 over its
stdio with `jsonrpc.el`. Rejected: python.el comint plus side channel
(two input paths, signal ambiguity), EIN or emacs-jupyter as the core
(Jupyter server or native ZMQ module; status scraping), Command
Execution API as the core (classic clusters only, no serverless).

### 3.2 Control plane

**Databricks CLI** with `-o json` as an async subprocess, for everything
outside Spark execution. Commands without a dedicated CLI verb go
through `databricks api get|post <path>`, which still uses the CLI's
auth. A REST fast path from Elisp (token from `databricks auth token`,
then `url.el`) is reserved for measured hot spots, starting with SQL
statement polling (section 4.10). Rejected: a plugin-owned Python venv
with `databricks-sdk` (a second runtime with no capability the CLI
lacks).

### 3.3 CLI coverage (checked against v1.25.0)

| Need | CLI |
| --- | --- |
| Auth | `auth profiles`, `auth login`, `auth describe`, `auth token` |
| Compute | `clusters list/get/start/…`, `warehouses list/get/start/stop` |
| Env | `environments setup-local` |
| Jobs | `jobs list/get/list-runs/get-run/get-run-output/export-run/cancel-run/repair-run/run-now/submit` |
| Bundles | `bundle validate/plan/deploy/destroy/run/summary/open/generate/deployment/sync/init/schema` |
| Pipelines | `pipelines list-pipelines/get/list-updates/get-update/list-pipeline-events/start-update (--validate-only, --full-refresh)/stop`, plus project verbs `pipelines dry-run/logs/history/show` |
| SQL | no verb; `databricks api post /api/2.0/sql/statements` (Statement Execution API) and `query-history list` |
| Notebooks | `workspace list/get-status/export (SOURCE, JUPYTER, …)/import/export-dir/import-dir` |
| Unity Catalog | `catalogs/schemas/tables/volumes/functions/registered-models list/get` |
| AI Gateway | `ai-gateway list-model-services --parent schemas/system.ai --view FULL` |

### 3.4 Graph rendering (bundles, job tasks, pipelines)

| Option | Pros | Cons |
| --- | --- | --- |
| **Graphviz `dot` → SVG in an image buffer** | One small, common binary; good DAG layout; Emacs 30 renders SVG; **image maps** make nodes clickable | External binary |
| Mermaid (`mmdc`) | Popular text format, renders in GitHub/Org | `mmdc` needs Node and headless Chromium: heavy, slow |
| Pure-Elisp text tree / outline | No dependencies | Not a graph for DAGs with joins |

**Chosen:** one internal graph model rendered three ways: Graphviz SVG
(interactive, default when `dot` exists), Mermaid **text** export
(copy into README, Org with `ob-mermaid`, or render if `mmdc` exists),
and a text outline fallback.

### 3.5 Notebooks

| Option | Verdict |
| --- | --- |
| Databricks **source format** (`.py`/`.sql` with `# COMMAND ----------`, `# MAGIC`) edited in `python-mode`/`sql-mode` with a cell minor mode, executed by our server | **Primary.** No extra dependencies; diff-friendly; the format bundles and Git folders use |
| `.ipynb` through a local Jupyter server with Databricks Connect, front-end EIN or emacs-jupyter | **Optional adapter.** Rich output; needs `jupyter_server` + `ipykernel` in the venv |
| Command Execution API 1.2 as a remote REPL | **Optional, classic clusters only** (no serverless). Gives cluster-side Python/SQL/Scala/R like a real notebook |
| Attach to a notebook open in the browser, see its live cell state | **Impossible**: no public API exposes an interactive notebook's execution state |

EIN status (GitHub, 2026-11-10): not archived, last push 2025-12-12. It
talks to a Jupyter server over REST and websockets and needs no native
module. emacs-jupyter: last push 2026-10-14, needs the `emacs-zmq`
module. The adapter therefore owns only the server side (launch plus
IPython startup) and supports both front ends.

## 4. Decision

### 4.1 Architecture

```
┌──────────────────────────────── Emacs ──────────────────────────────────┐
│ context (profile + compute + warehouse + bundle target)                │
│                                                                         │
│ databricks-cli    : async runner, JSON, pagination, cache, error map    │
│ databricks-server : JSON-RPC client of the Python companion             │
│                                                                         │
│ UI: code buffers + REPL │ SQL editor │ jobs/runs │ bundle mode + graph  │
│     pipelines + graph   │ notebooks  │ catalog/workspace │ compute      │
│     mode line, transient dispatch, completing-read pickers, gptel       │
└───────┬───────────────────────────────┬─────────────────────────────────┘
        │ JSON-RPC over stdio            │ `databricks … -o json` / `databricks api`
        ▼                                ▼
 python -m databricks_emacs serve     Databricks CLI ──► workspace REST APIs
 (project venv, databricks-connect)        (jobs, bundles, pipelines, SQL warehouses,
        │ gRPC (Spark Connect)              UC, workspace, AI Gateway)
        ▼
 cluster or serverless compute
```

Optional side processes: `dot` (graphs), a Jupyter server (notebook
adapter), `gptel` HTTP requests (AI Gateway).

### 4.2 Context

A **context** is the unit the user selects (`databricks-select-context`,
picker with category `databricks-context`). Defined in
`databricks-contexts` (defcustom, P-3):

| Key | Meaning |
| --- | --- |
| `:name` | Display name |
| `:profile` | `~/.databrickscfg` profile |
| `:compute` | `(:cluster "<id>")` or `(:serverless t)` or `nil` (CLI-only work) |
| `:warehouse` | SQL warehouse id for the SQL editor (optional; picker if missing) |
| `:target` | Bundle target (optional; default from `databricks.yml`) |
| `:python` | Interpreter or venv (default `<project>/.venv`) |
| `:confirm` | Confirm every execution and destructive action (production) |

A project with `databricks.yml` can derive a context from the bundle
target (workspace host, `cluster_id`, serverless) so that no defcustom
entry is needed. The mode line shows `DBX:<context>` with the compute
state and, while code runs, stage progress.

### 4.3 CLI layer (`databricks-cli.el`)

- One async runner: `make-process` with the context's env (section 4.7),
  `-o json`, `--profile`, output parsed with `json-parse-buffer`, errors
  mapped to a condition with the CLI's message.
- At most 4 concurrent CLI processes; a queue beyond that. Auth-sensitive
  calls (token refresh) are serialized.
- Pagination: `--limit` / page tokens wrapped into a stream of pages so
  lists render progressively.
- Caches per context with TTLs (jobs list 5 min, clusters 30 s,
  warehouses 30 s, catalog trees 10 min), invalidated by our own write
  actions and by `g`.
- Polling helpers with exponential backoff that stop when the target is
  terminal or the buffer is no longer visible.
- Contract tests on recorded JSON fixtures; a fake `databricks`
  executable replays them in ERT.

### 4.4 Compute

- **Clusters** (`databricks-clusters`, `tabulated-list`): state, DBR,
  access mode, owner; start, stop, restart; set as the context's compute.
- **Serverless**: a context flag; the serverless environment version is
  part of the context's doctor check.
- **SQL warehouses** (`databricks-warehouses`): state, size, type; start,
  stop; set as the context's warehouse.
- The mode line shows the compute state (`RUNNING`, `PENDING`,
  `TERMINATED`), polled only while a context with that compute is active.

### 4.5 Code execution (companion server)

Condensed from draft 2 (sections 4.3–4.7, unchanged in substance):

- `python -m databricks_emacs serve` in the project venv. The package is
  stdlib-only, shipped under `python/` and put on `PYTHONPATH`.
- Transport: JSON-RPC 2.0 over the child's stdio via `jsonrpc.el`. At
  startup the server moves the protocol to private, non-inheritable fds,
  redirects fd 1/2 to pipes drained into `output` notifications, and
  replaces stdin with a proxy that asks Emacs for input (`input()`,
  `pdb`).
- Requests: `initialize`, `execute` (`interactive` | `exec` | `cell`),
  `interrupt`, `interruptAll`, `catalog`, `preview`, `sql`, `complete`,
  `session/reconnect`, `ping`, `shutdown`. Notifications: `session/state`,
  `session/changed`, `req/start`, `req/end`, `cancel/remote`, `progress`,
  `output`, `log`, `dropped`, `warning`.
- Session: created in the background after `initialize`
  (`DatabricksSession.builder.getOrCreate()` from the context's env), so
  a cold cluster never blocks Emacs. Injected globals: `spark`, `sql`,
  `udf`, `dbutils` (from `databricks.sdk.runtime`), `getArgument`,
  `displayHTML`, `display` (renders DataFrames through `preview`).
  `SPARK_CONNECT_USER_AGENT` gets `databricks-emacs/<v>`.
- Jobs queue (P-2); every job is tagged `dbx-emacs-<req>`; cancel =
  validated local interrupt of the main thread plus `interruptTag` on a
  short-lived thread; kill-and-restart escalation. Exactly one `req/end`
  per job.
- `cell` mode translates the Databricks notebook source format (`# MAGIC`
  prefixes, `%sql` → `spark.sql` with the last result displayed, `%md` →
  rendered text, `%fs` → `dbutils.fs`, `%run` → execute the referenced
  notebook file in the same namespace; `%scala`/`%r` are errors).
- Run-file mode: `python -m databricks_emacs run [--module] FILE ARGS…`
  behaves like `python FILE ARGS…` (argv, `sys.path[0]`, `__main__`,
  exit codes); output in `compilation-mode`.
- REPL buffer (`databricks-repl-mode`, comint-derived, no process of its
  own), source-buffer minor mode `databricks-python-mode`, `ob-databricks`
  for Org Babel (Python and SQL blocks).

### 4.6 JSON performance (unchanged from draft 2, section 4.7)

Measured on Emacs 30.2: native parse ~1.5 µs per 200-byte message, ~5 ms
per 750 KB; `jsonrpc.el`'s pretty-printed event log costs ~50× the parse,
and it dispatches each message through its own timer. Rules: events
buffer off (`:events-buffer-config '(:size 0)`); the server batches
notifications to ≤ 20/s (progress every 200 ms, output every 50 ms or
32 KiB); results capped (4 MiB per message, paged); UI redraw on a ≤ 5 Hz
timer. The same caps apply to large CLI outputs (SQL results, run lists),
which are parsed once and rendered page by page.

### 4.7 Environment and auth

One resolver builds the environment for the server **and** every CLI
call of a context:

1. Scrub inherited `^DATABRICKS_`, `^SPARK_REMOTE$`,
   `^SPARK_CONNECT_MODE_ENABLED$`, `^ARM_(CLIENT_ID|CLIENT_SECRET|TENANT_ID)$`;
   list what was scrubbed.
2. Set `DATABRICKS_CONFIG_PROFILE`, optional `DATABRICKS_CONFIG_FILE`,
   and exactly one compute selector (`DATABRICKS_CLUSTER_ID` or
   `DATABRICKS_SERVERLESS_COMPUTE_ID=auto`) for the server.
3. Validate: compute selectors exclusive, including what the config
   profile itself sets (Q8).
4. The UI shows the target reported by the server and by `auth describe`.

Auth flows: `databricks auth login --profile …` in a terminal buffer for
OAuth U2M; Azure CLI and PAT work through the profile. No token is ever
stored by the plugin. (VS Code's metadata-service token server is an
alternative for the future; profile-based auth already keeps tokens out
of the process environment.)

### 4.8 Environment provisioning and doctor

- **Provisioning** (first run, confirmed): `databricks environments
  setup-local --cluster-id X | --serverless-version N --output json
  [--dry-run]` creates `<project>/.venv` with uv and a matching
  `databricks-connect`. Optional extras for the notebook adapter
  (`jupyter_server`, `ipykernel`) are offered separately. Existing venvs
  are never modified (C2).
- **Doctor** (`databricks-doctor`): CLI presence and version, auth,
  `probe` of the venv, compute facts, Emacs facts. Rules (the Databricks
  rows match VS Code v2.23.1):

| Rule | Level |
| --- | --- |
| `databricks` CLI missing or older than the pinned minimum | error |
| `pyspark` or `pyspark-client` installed next to / instead of `databricks-connect` | error |
| Cluster DBR < 13; access mode not `SINGLE_USER`/`SHARED`/`USER_ISOLATION` | error |
| Workspace without Unity Catalog | error |
| DB Connect < 13, or newer than the cluster DBR major.minor; serverless with DB Connect < 15.4 | error |
| Python < 3.10; serverless: < 3.12 for DB Connect 16+, else < 3.11 | error |
| Local Python minor ≠ DBR Python (13–14 → 3.10, 15 → 3.11, 16+ → 3.12) | warning |
| No progress API in the installed client | info |
| `dot` missing (graphs fall back to text) | info |
| Running server's protocol ≠ installed helper | warning: restart |

### 4.9 Notebooks

**Source-format notebooks (P1).** `databricks-notebook-mode`, a minor
mode for `python-mode`/`python-ts-mode`/`sql-mode` buffers whose first
line is `# Databricks notebook source` (or `-- Databricks notebook
source`):

- Cell navigation and marking by `# COMMAND ----------`; `# MAGIC`
  lines shown with a face and optionally hidden; `%md` cells rendered via
  `markdown-mode` if installed.
- `C-c C-c` runs the cell through the server's `cell` mode; `C-c C-a`
  runs all above; results appear in the REPL buffer and, optionally,
  inline below the cell as an overlay (text and tables).
- **Workspace notebooks**: `databricks-workspace-open` (picker over
  `workspace list`, category `databricks-workspace-object`) exports with
  `--format SOURCE` into a local mirror under
  `.databricks/emacs/workspace/` and opens it; saving imports with
  `--format SOURCE --overwrite` after a conflict check
  (`workspace get-status` modification time vs. the time of export;
  _field to verify_). Inside a bundle project, notebook files are
  edited locally and pushed by `bundle deploy`/`bundle sync`.
- **Run as job**: `databricks-notebook-run-as-job` submits a one-off run
  (`jobs submit` with `notebook_task` on the context's cluster, or
  serverless environments), then follows it in the runs UI (section
  4.11): status, task state, and the exported result (`jobs export-run`
  HTML rendered with `shr`, or opened in the browser).

**Jupyter adapter (P2), front end EIN or emacs-jupyter.**
`databricks-jupyter-start` runs `jupyter server --no-browser` from the
project venv with our IPython startup file (spark, `dbutils`, `%sql`,
`%fs`, source-format translation, HTML/text DataFrame display; modelled
on VS Code's `00-databricks-init.py`). EIN then logs into that server
(`ein:notebooklist-login`), or emacs-jupyter connects to the kernel.
Workspace `.ipynb`: export with `--format JUPYTER`, edit, import back
with the same conflict check. Requires `jupyter_server` and `ipykernel`
in the venv (offered by provisioning, never installed silently).

**Remote REPL (P3, classic clusters only).** A Command Execution API
1.2 context on the selected cluster (`databricks api post
/api/1.2/contexts/create`, `/commands/execute`, status polling, cancel):
cluster-side Python, SQL, Scala or R in a comint buffer, like a notebook
attached to the cluster. Not available on serverless (documented by
Databricks).

**Not possible:** attaching to a notebook that is open in the browser,
seeing its running cells, or sharing its state. "Notebook status" exists
only for notebook **runs** (jobs), which section 4.11 covers.

### 4.10 SQL editor (P0)

`databricks-sql-mode`, a minor mode for `sql-mode` buffers (and `.sql`
files in bundles). The header line shows the **target**: a SQL
warehouse, or the context's cluster/serverless. `C-c C-t` switches it
(picker over warehouses and the context's compute).

- **Execute**: statement at point (`C-c C-c`), region, or buffer
  (statements split by a quote- and comment-aware splitter). Named
  parameters `:name` are prompted for and sent as typed parameters.
- **On a warehouse**: Statement Execution API via
  `databricks api post /api/2.0/sql/statements` with `warehouse_id`,
  `catalog`, `schema`, `wait_timeout: "0s"`, `disposition: INLINE`,
  `format: JSON_ARRAY`, `row_limit`. Then poll
  `GET /api/2.0/sql/statements/{id}` with backoff (0.5 s → 2 s), fetch
  further chunks with `…/result/chunks/{n}`, cancel with
  `POST …/cancel`. A stopped warehouse is started by the query; the
  header line shows `STARTING`. HTTP 200 with `status.state = FAILED` is
  an error (per the API docs). Polling is the first candidate for the
  REST fast path (section 3.2) if CLI spawn latency shows.
- **On a cluster or serverless**: the server's `sql` request
  (`spark.sql(...)`, JSON rows capped like `preview`), queued per P-2.
- **Results buffer** (`databricks-sql-result-mode`, built on `vtable`):
  typed columns from the result manifest, paging, sort, column width,
  copy cell/row, export to CSV / TSV / Org table / JSON, "open as
  DataFrame" (re-runs on the server for further work). A result history
  ring per editor buffer.
- **Completion**: catalogs, schemas, tables and columns from the cached
  catalog (section 4.14), offered through `completion-at-point`.
- **History**: `query-history list` in a picker (category
  `databricks-query`) to reopen past statements, with duration and
  status annotations.
- **Org Babel**: `ob-databricks` SQL blocks with `:warehouse` or
  `:compute` header arguments; results as Org tables.

### 4.11 Jobs and runs (P0)

- **Pickers** (C9): `databricks-find-job` (category `databricks-job`;
  annotations: owner, schedule, tags, last run state and time; grouped
  by bundle or tag), `databricks-find-run` (category `databricks-run`;
  active first). An Embark keymap per category (if Embark is installed)
  offers run now, open, watch, cancel, repair, copy id/URL. Large
  workspaces: the job list is cached and refreshed asynchronously; with
  Consult installed, an async Consult source is used instead.
- **Jobs buffer** (`tabulated-list`, `tabulated-list-groups` by tag or
  bundle): `RET` opens the job, `r` run now (with parameter prompts), `l`
  runs.
- **Runs buffer**: `jobs list-runs` with active-only toggle, filters by
  job, state and time; auto-refresh while visible.
- **Run buffer**: run state, timing, parameters, **task graph** (section
  4.12 renderer, nodes coloured by task state), per-task output and logs
  (`get-run-output`), error traces with clickable source locations when
  they map to local files, export (`export-run`). Actions: cancel,
  cancel all, repair (choose failed tasks), rerun.
- **Watch**: `databricks-watch-run` keeps polling (3 s while running,
  like VS Code) and sends a desktop notification
  (`notifications-notify`) on a terminal state; watched runs appear in
  the mode line.

### 4.12 Bundles (P0 basics, P1 graphs)

`databricks-bundle-mode`, a minor mode enabled in buffers of a project
that contains `databricks.yml`:

- Mode line: bundle name and target; target picker.
- Commands: `validate` (also after saving bundle YAML; results as a
  **Flymake** backend on the YAML files), `plan` (diff-like buffer,
  required before `deploy` when the context has `:confirm`), `deploy`,
  `destroy` (P-7), `run <resource>` (picker over resources; output in
  `compilation-mode`; run id parsed and handed to the runs UI), `summary`
  (resource tree with `RET` jumping to the YAML location, using
  `--include-locations` as VS Code does), `open` (browser), `generate`
  (adopt an existing job or pipeline), `deployment bind/unbind`,
  variables (view and override per target), `sync` (P2).
- **Resource graph** (`databricks-bundle-graph`): built from the
  resolved configuration (`bundle validate -o json`; _to verify that it
  includes the fully resolved resources_, Q12):
  - nodes: jobs, tasks, pipelines, notebooks/files, other resources
    (dashboards, apps, …) as plain nodes;
  - edges: task `depends_on`, `pipeline_task`/`run_job_task` references,
    task → source file, `${resources.…}` references between resources;
  - overlay: latest run state per job/task and latest update state per
    pipeline (from the runs and pipelines modules), when available.
- **Rendering** (section 3.4): Graphviz SVG in an image buffer with an
  **image map**, so clicking a node (or `RET` on it via a node picker)
  opens its YAML definition, its latest run, or its source file; Mermaid
  text export (`flowchart LR`) to a buffer or the kill ring; text outline
  fallback without `dot`.

### 4.13 Lakeflow Declarative Pipelines (P1)

- **Pipelines buffer**: `pipelines list-pipelines`, state, last update;
  inside bundles, the bundle's pipelines first.
- **Run**: start update, `--full-refresh`, selective refresh of chosen
  tables (picker over datasets), `--validate-only`, `pipelines dry-run`
  (validates the graph without materializing), stop. Full refresh
  confirms (P-7).
- **Events buffer**: `pipelines logs` / `list-pipeline-events` with
  filters (level, event type, update, time); incremental polling every
  5 s while an update runs, like VS Code.
- **Diagnostics**: ERROR/WARN events that carry source locations become
  Flymake diagnostics on the matching local files (VS Code does the same
  using `origin.source_code_location`).
- **Dataset graph** (`databricks-pipeline-graph`): edges from
  `flow_definition` events (`input_datasets` → `output_dataset`, plus
  `output_sink`) of the latest update or a dry run; node types (streaming
  table, materialized view, view, sink) from `dataset_definition` events;
  overlay from `flow_progress` events (state, rows written, expectation
  pass/fail). Same renderer as 4.12: SVG with clickable nodes (open the
  flow's source, show its `explain_text`, preview the table with
  `pipelines show TABLE` on the context's warehouse), Mermaid export,
  text fallback.
- **History**: `pipelines history` with update durations and states.

### 4.14 Browsers

- **Unity Catalog** (P1): catalogs → schemas → tables / volumes /
  functions / registered models, via CLI list commands (works without a
  running cluster). `RET` shows details (columns, owner, comment,
  properties); actions: insert name, copy, preview rows (`pipelines show`
  on the warehouse, or `preview` on the server), open in browser. Cached
  for SQL completion.
- **Workspace** (P1): `workspace list` tree; open notebooks (4.9) and
  files; import/export; copy path; open in browser.
- **Compute** (P0): section 4.4.

### 4.15 AI Gateway chat via gptel (optional, P2)

Unchanged from draft 2 section 4.13, with verified facts:

- Base URL `https://<workspace-host>/ai-gateway/mlflow/v1`; model in the
  body as `system.ai.<name>` (verified with curl by the operator,
  2026-11-10).
- **Default backend: Chat Completions** (`gptel-make-openai`,
  `:endpoint "/ai-gateway/mlflow/v1/chat/completions"`). Verified; gptel
  shows `reasoning_content`.
- **Second backend: Responses** (`gptel-make-openai-responses`,
  `:endpoint "/ai-gateway/mlflow/v1/responses"`). Verified to work;
  gptel currently drops the gateway's `reasoning_text` (upstream fix).
  Not `gptel-make-openai`, which picks Responses only for
  `api.openai.com`.
- Models: `databricks ai-gateway list-model-services --parent
  schemas/system.ai --view FULL -o json`, filtered by supported API type
  per backend (_field name to verify_).
- Key: `databricks auth token --profile <p>`, cached until expiry. This
  is the one synchronous CLI call (gptel asks for the key
  synchronously), about once per token lifetime.
- Chat buffers: plain gptel with `gptel-default-mode` = `org-mode`.

### 4.16 UI conventions

- **Transient** `databricks-dispatch` (`C-c d` suggested, not bound by
  default): context, compute, REPL, SQL, jobs, runs, bundles, pipelines,
  notebooks, catalog, workspace, doctor, chat.
- **Pickers**: `completing-read` with metadata (C9). Each object type
  has a category, so Marginalia/Embark integrations are plain keymaps
  and annotators keyed by category.
- **Lists**: `tabulated-list-mode` (with `tabulated-list-groups`) for
  resource lists; `vtable` for SQL results.
- **Links**: every object has "copy URL" and "open in browser".
- **Mode line**: `mode-line-format-right-align`; `DBX:<context>`, compute
  state, running job progress, watched runs.
- **Notifications**: `notifications-notify` when available, else the
  echo area.

## 5. Parity matrix and phasing

| Feature | VS Code v2.23.1 | Emacs | Phase |
| --- | --- | --- | --- |
| Contexts: profile, compute, warehouse, target | Configuration view | `databricks-select-context`, dir-local name | P0 |
| Auth (OAuth U2M, Azure CLI, PAT) | yes | CLI | P0 |
| Doctor / compatibility checks | yes | `databricks-doctor` | P0 |
| Run file with DB Connect | yes | run-file mode | P0 |
| Interactive cells, REPL | Interactive Window | REPL, send region/cell/buffer | P0 |
| Progress and per-job cancel | Jupyter only / kill terminal | mode line, jobs, `interruptTag` | P0 (beyond) |
| Clusters, serverless | yes | compute buffer, context | P0 |
| SQL warehouses | — | compute buffer, SQL target | P0 (beyond) |
| **SQL editor + results** | — | `databricks-sql-mode`, `vtable` results | P0 (beyond) |
| **Jobs/runs browser, fuzzy search, watch** | bundle runs only | pickers, buffers, notifications | P0 (beyond) |
| Bundles: validate/deploy/run/destroy, explorer, variables | yes | `databricks-bundle-mode` | P0 |
| Bundle `plan`, `generate`, bind | — | yes | P1 (beyond) |
| **Bundle and job task graphs** | — | Graphviz SVG, Mermaid export | P1 (beyond) |
| Pipelines: run, selective refresh, validate, events, diagnostics | yes | pipelines module, Flymake | P1 |
| **Pipeline dataset graph with status** | — | Graphviz SVG, Mermaid export | P1 (beyond) |
| Env setup (uv, `setup-local`) | yes | same command | P1 |
| Notebook source format, cells | via Jupyter init | `databricks-notebook-mode` | P1 |
| Workspace notebooks open/save | WSFS explorer | export/import with conflict check | P1 |
| Run notebook/file as job | yes | `jobs submit` + runs UI | P1 |
| Unity Catalog explorer | yes | catalog browser | P1 |
| Workspace browser | yes | workspace tree | P1 |
| Query history | — | picker | P1 (beyond) |
| Org Babel (Python, SQL) | — | `ob-databricks` | P1 (beyond) |
| `.ipynb` notebooks | Jupyter extension | Jupyter server adapter + EIN or emacs-jupyter | P2 |
| Debug | debugpy | dape + debugpy | P2 |
| AI Gateway chat | VS Code chat provider | gptel backends | P2 |
| Sync | yes | `bundle sync --watch` | P2 |
| Remote REPL on classic clusters | upload-and-run | Command Execution contexts | P3 |
| SSH tunnel / remote IDE, AI-tools installer | yes | **out of scope** | — |
| Attach to a browser notebook session | no | **impossible** (no API) | — |
| Dashboards, Apps, Genie, vector search | partial / no | out of scope for now (`bundle open` only) | — |

**P0 exit criterion.** On a real workspace (credential-gated CI job, or a
manual checklist): select a context; doctor passes; start the server on a
cluster and on serverless; send code, see progress (where supported),
cancel a query and a local loop; run a file; run SQL on a warehouse and
on the cluster, page and export results; find a job by fuzzy search, run
it, watch it to completion with a notification; validate, deploy and run
a bundle resource. Offline CI: ERT against the fake CLI and a stub
server; pytest for the server with a fake session; the JSON benchmarks.

**P1 gate**: graphs for a sample bundle and pipeline match the workspace
UI; pipeline diagnostics land on the right lines; notebook export/import
round-trips without changes.

## 6. Repository layout

```
lisp/
  databricks.el                 ; entry, defgroup, autoloads, dispatch
  databricks-context.el         ; contexts, dir-local name, mode line
  databricks-env.el             ; env resolver
  databricks-cli.el             ; async CLI runner, cache, pagination, polling
  databricks-rest.el            ; optional REST fast path (token + url.el)
  databricks-pickers.el         ; completing-read sources, categories, annotators
  databricks-compute.el         ; clusters, warehouses
  databricks-doctor.el          ; doctor, provisioning
  databricks-rpc.el             ; jsonrpc wrapper
  databricks-server.el          ; companion server lifecycle
  databricks-state.el           ; state model fed by notifications
  databricks-python.el          ; databricks-python-mode (send, completion)
  databricks-repl.el            ; databricks-repl-mode
  databricks-run.el             ; run-file mode
  databricks-notebook.el        ; source-format notebooks, workspace open/save
  databricks-jupyter.el         ; Jupyter server adapter (EIN / emacs-jupyter)
  databricks-sql.el             ; databricks-sql-mode, statement execution
  databricks-sql-result.el      ; vtable result buffers, export
  databricks-jobs.el            ; jobs, runs, watch
  databricks-bundle.el          ; databricks-bundle-mode, Flymake backend
  databricks-pipelines.el       ; pipelines, events, diagnostics
  databricks-graph.el           ; graph model; Graphviz, Mermaid, text renderers
  databricks-catalog.el         ; Unity Catalog browser
  databricks-workspace.el       ; workspace browser
  databricks-gptel.el           ; optional, soft-depends on gptel
  databricks-embark.el          ; optional, soft-depends on embark
  ob-databricks.el
python/databricks_emacs/
  __init__.py  __main__.py  probe.py  server.py  rpc.py  stdio.py  jobs.py
  session.py  cancel.py  progress.py  cells.py  run.py  interrupt_cli.py
  ipython_startup.py
test/
  elisp/    (ERT; fake `databricks` replaying fixtures; stub server; graph golden files)
  python/   (pytest; fake session; cell translation; optional workspace integration)
  fixtures/ (recorded CLI v1.25.0 JSON)
docs/design/spark-connect-emacs.md
```

Distribution: one MELPA package with `python/` in `:files`. Optional
integrations (gptel, Embark, Consult, EIN, emacs-jupyter, markdown-mode)
are soft dependencies loaded with `with-eval-after-load`.

## 7. Consequences

**Positive**

- Broader than the VS Code extension where Emacs is strong: SQL in
  buffers, keyboard-driven fuzzy navigation of jobs and runs, Org Babel,
  graphs as text-exportable artifacts, precise cancel.
- One control-plane path (the CLI) and one execution path (the companion
  server). No SDK of our own.
- Completion-framework neutral; works for Vertico and Helm users alike.

**Negative / costs**

- Databricks only; OSS Spark Connect users are not served.
- Large surface; phases and gates are essential (R10).
- Hard runtime dependency on the CLI and its JSON shapes.
- Graphs need Graphviz for the good view; Mermaid rendering needs Node.
- `.ipynb` editing depends on EIN or emacs-jupyter and a Jupyter server.
- The stock python.el REPL is not used for Databricks work.

## 8. Risks and mitigations

| # | Risk | Mitigation |
| --- | --- | --- |
| R1 | Progress API absent in some DB Connect versions | Feature detection; job-level status always works |
| R2 | Spark work outside the attached session or main thread not tagged | Detection and `untrackedSession`; documented |
| R3 | Session built on a background thread misbehaves | Integration test; fallback to building it as the first queued job |
| R4 | JSON or message volume stalls Emacs | Section 4.6 budgets; paged rendering; CI benchmarks |
| R5 | CLI JSON shapes drift; commands renamed | Pinned minimum version; defensive parsing; fixtures re-recorded per CLI release; doctor warns on unknown versions |
| R6 | CLI spawn latency hurts SQL polling or large pickers | Caches; async pages; REST fast path for SQL polling |
| R7 | `bundle validate -o json` lacks resolved task graphs | Fall back to `bundle summary`; parse YAML only as last resort (Q12) |
| R8 | Pipeline graph depends on events from a recent update | Offer `dry-run` to produce them; show "no graph yet" honestly |
| R9 | Overwriting workspace notebooks loses remote edits | Conflict check before import; P-7 confirmation; keep the exported copy |
| R10 | Scope creep | Phases P0–P3 with gates; out-of-scope list |
| R11 | Destructive actions on production | P-7; `:confirm` contexts; `bundle plan` before deploy |
| R12 | EIN maintenance slows | Adapter owns only the server side; emacs-jupyter or the browser as alternative front ends |

## 9. Open questions (for the operator)

- **Q1. Naming.** Package `databricks` with prefix `databricks-`
  (clear, long names), or a short prefix? `dbx` collides with Databricks
  Labs' retired `dbx` tool. The doc file name stays until decided.
- **Q2. DB Connect progress.** Which versions expose the progress handler?
- **Q2a. Hard cancel.** Can a fresh client `interruptTag` an existing
  session by id on Databricks Connect?
- **Q3. Notebook front end.** EIN (as requested), emacs-jupyter, or both
  behind the adapter?
- **Q7. Windows.** Best effort only?
- **Q8. Compute selectors.** DB Connect behaviour when the config profile
  sets `cluster_id` and the env selects serverless.
- **Q10. Cancel scope in P0.** Precise local cancel, or `interruptTag`
  plus kill-and-restart first?
- **Q11. SQL editor scope in P0.** Warehouses only first, cluster SQL in
  P1?
- **Q12. Bundle graph source.** Confirm `bundle validate -o json` output
  is fully resolved; otherwise accept `bundle summary` as the source.
- **Q13. Out of scope list.** Dashboards (Lakeview), Apps, Genie, vector
  search: confirm out for now.

## 10. References

- Databricks VS Code extension v2.23.1 source:
  <https://github.com/databricks/databricks-vscode>
- Databricks CLI v1.25.0 (command groups checked locally, 2026-11-10):
  <https://github.com/databricks/cli>
- Statement Execution API tutorial (CLI `api post` usage):
  <https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial>
- Statement Execution API reference:
  <https://docs.databricks.com/api/statement-execution/v1>
- Command Execution API (classic clusters only; serverless not supported):
  <https://databricks-sdk-py.readthedocs.io/en/latest/workspace/compute/command_execution.html>
- Pipeline event log schema (`flow_definition`, `dataset_definition`):
  <https://docs.databricks.com/aws/en/dlt/monitor-event-log-schema>
- AI Gateway model services:
  <https://docs.databricks.com/aws/en/ai-gateway/model-services>,
  <https://docs.databricks.com/aws/en/ai-gateway/query-model-services>
- EIN: <https://github.com/millejoh/emacs-ipython-notebook>;
  emacs-jupyter: <https://github.com/emacs-jupyter/jupyter>
- gptel: <https://github.com/karthink/gptel>
- Emacs 30 NEWS; `jsonrpc.el` 1.0.25; JSON measurements in draft 2,
  section 4.7.
