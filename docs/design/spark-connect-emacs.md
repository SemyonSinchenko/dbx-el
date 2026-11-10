# databricks.el — a Databricks workbench for Emacs

- Status: **Proposed** (draft 3.1; earlier drafts kept as
  `spark-connect-emacs.draft{1,2,3}.md`)
- Date: 2026-11-10
- Author: architect agent, revised with the operator

**Implementation (2026-11-10).** Implemented as the package `dbx-el`
(main file `dbx-el.el`, symbol prefix `dbx-`, files at the repository root, Eask, WTFPL); see
`README.org`. P0 (SQL, jobs/runs, clusters, warehouses, bundles,
pipelines, graphs, doctor) and the P1 companion server (uv-managed or
user venv via `dbx-connect-venv`, REPL, send code, run file) are in
place. The catalog browser and gptel integration are not yet
implemented.

**Changes in draft 3.1 (operator, 2026-11-10).** Notebooks are removed
from scope (source format, workspace notebooks, Jupyter/EIN, remote
REPL). Focus: the SQL editor with results as Org tables (up to 2000 rows),
Graphviz graphs for bundles and Lakeflow Declarative Pipelines, and the
jobs, runs, clusters and warehouses APIs, all with fuzzy pickers. A new
section 4.9 specifies the **text UI**: object pages built from glyphs,
text bars and sparklines, modelled on the Databricks web UI. P0 is now
pure Elisp plus the CLI; the Python companion server moves to P1.

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
6. **Browse** Unity Catalog, the workspace, clusters and warehouses.
7. **Chat** with AI Gateway models (gptel).

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
2. Pipeline diagnostics mapped to source locations are high value.
3. There is room to go beyond it: SQL, jobs outside bundles, graphs,
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
| P-7 | **Read before write.** Destructive actions (deploy, destroy, cancel, full refresh) always confirm and show what will change (`bundle plan`, diff). | Production workspaces |

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
| Compute | `clusters list/get/start/events/…`, `warehouses list/get/start/stop` |
| Env | `environments setup-local` |
| Jobs | `jobs list/get/list-runs/get-run/get-run-output/export-run/cancel-run/repair-run/run-now/submit` |
| Bundles | `bundle validate/plan/deploy/destroy/run/summary/open/generate/deployment/sync/init/schema` |
| Pipelines | `pipelines list-pipelines/get/list-updates/get-update/list-pipeline-events/start-update (--validate-only, --full-refresh)/stop`, plus project verbs `pipelines dry-run/logs/history/show` |
| SQL | no verb; `databricks api post /api/2.0/sql/statements` (Statement Execution API) and `query-history list` |
| Workspace | `workspace list/get-status/export/import` |
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

**Out of scope** (operator decision, 2026-11-10): Databricks source-format
notebooks, workspace notebook editing, `.ipynb` via EIN or emacs-jupyter,
and the Command Execution API REPL. Notebook **job runs** remain visible
in the jobs UI like any other task. (The analysis is kept in
`spark-connect-emacs.draft3.md`, section 3.5 and 4.9.)

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
│     pipelines + graph   │ catalog/workspace browsers │ compute          │
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

Optional side processes: `dot` (graphs), `gptel` HTTP requests (AI
Gateway).

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

### 4.5 Code execution (companion server, P1)

P0 ships without Python: every P0 feature (SQL on warehouses, jobs,
runs, clusters, warehouses, bundles) is Elisp plus the CLI. The server
arrives in P1 for PySpark from buffers, run-file and SQL on a cluster or
serverless.

Condensed from draft 2 (sections 4.3–4.7, unchanged in substance):

- `python -m databricks_emacs serve` in the project venv. The package is
  stdlib-only, shipped under `python/` and put on `PYTHONPATH`.
- Transport: JSON-RPC 2.0 over the child's stdio via `jsonrpc.el`. At
  startup the server moves the protocol to private, non-inheritable fds,
  redirects fd 1/2 to pipes drained into `output` notifications, and
  replaces stdin with a proxy that asks Emacs for input (`input()`,
  `pdb`).
- Requests: `initialize`, `execute` (`interactive` | `exec`),
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
  `databricks-connect`. Existing venvs are never modified (C2).
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

### 4.9 Text UI: object pages, glyphs and bars

Every object (job, run, cluster, warehouse, pipeline, bundle) has a
**page**: a read-only buffer that mirrors the corresponding Databricks
web page in text. Pickers (C9) are the entry point: choosing a job in
Vertico or Helm opens its page. Pages are plain text with faces, so they
work in terminal Emacs too.

**Building blocks**

| Element | Implementation |
| --- | --- |
| Status glyph | One **single-width** symbol plus a face (mapping below). Emoji (`✅`, `❌`, `⏳`) are **not** used: Emacs counts them as width 2 and fonts differ, which breaks alignment |
| Glyph sets | `databricks-glyphs` defcustom: `unicode` (default), `ascii` (`+ x > o - #`), or user alist. Chosen automatically with `char-displayable-p` at first use |
| Vertical bar chart | Eighth-blocks `▁▂▃▄▅▆▇█` stacked over N rows (default 4, so 32 levels), one column per run, a y-axis with two labels; the bar's face is its run's state |
| Horizontal bars | `█` plus an eighth-block remainder, for durations, progress and cluster utilisation |
| Sparkline | One row of `▁…█`, used inside picker annotations and list columns |
| Run matrix | Rows = tasks, columns = runs aligned under the bar chart; each cell is a status glyph |
| Sections | Foldable headings (`TAB`), built on `outline-minor-mode` with our heading prefix; no extra dependency |
| Actions | Text buttons (`button.el`) for "Run now", "Cancel", "Open in browser", plus single-key bindings on the page and an Embark map per category |
| Tables | `vtable` where pixel alignment matters (mixed fonts), `tabulated-list` for sortable lists |
| Time | Relative ("3m ago") and absolute in the local zone; durations as `1h 37m 59s` |
| Alignment | Column widths computed with `string-width`; `string-pixel-width` when `vtable` is used with proportional fonts |

**Status mapping** (from the verified API shapes, section 4.11):

| `status.state` | `termination_details.code` (examples) | Glyph | Face |
| --- | --- | --- | --- |
| `RUNNING` | — | `⟳` | running (blue) |
| `PENDING`, `QUEUED` | — | `○` | waiting (grey) |
| `BLOCKED` | — | `■` | blocked (orange) |
| `TERMINATING` | — | `⟳` | running, dimmed |
| `TERMINATED` | `SUCCESS` | `✔` | success (green); "retried" variant (green on yellow underline) when earlier attempts failed |
| `TERMINATED` | `SKIPPED` | `⊘` | skipped (grey) |
| `TERMINATED` | `CANCELED`, `USER_CANCELED` | `⊘` | cancelled (dark grey) |
| `TERMINATED` | anything else (`RUN_EXECUTION_ERROR`, `INTERNAL_ERROR`, …) | `✘` | failed (red); `type` (`CLIENT_ERROR`, `INTERNAL_ERROR`, `CLOUD_FAILURE`) in `help-echo` |
| no task entry in that run | — | `·` | absent |

Unknown values map to `?` with a warning face and are logged, so new API
states are visible instead of misreported.

**Job page** (`databricks-job-page`), the text version of the web UI's
job page (mock with 22 runs; actual widths adapt to the window):

```
 soe-etl-job  ★                               [Run now] [Run with params] [Browser]
 Managed by bundle soe (target prod)  ·  Job ID 276415565620551
 Creator ua-conversion-models-stage  ·  Run as ua-conversion-models-stage
 Schedule  At 55 minutes past the hour (UTC)  ·  Performance optimized: on
 Description  SoE data generation job

▼ Runs  (Nov 09 07:55 → Nov 10 06:55)                  [p] older  [n] newer
  3h 15m ┤                    █
         │                    █               ▃
  1h 37m ┤                    █           ▆   █
         │▃ ▂ ▂ ▂ ▂ ▃ ▂ ▂ ▂ ▂ █ ▂ ▂ ▃ ▂ ▄ █ ▃ █ ▂ ▂ ▂ ▁
         └──────────────────────────────────────────────
          Nov 09                  Nov 10
  soe_etl_preflight     ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ⟳
  soe_etl_hour_ge_23    ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ○
  soe_etl_hour_le_5     ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ○
  soe_etl_job_offpeak   ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ○
  soe_etl_job_peak_e…   ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ○

▼ Run list                                       [c] cancel  [R] repair  [RET] open
  Start            Run ID         Launched     Duration  Status        Parameters
  Nov 10 06:55     810416184…     scheduler     2m 57s   ⟳ Running     country_filt…
  Nov 10 05:55     810398812…     scheduler    52m 10s   ✔ Succeeded   country_filt…
  …

▶ Tasks (5)
▶ Job parameters (3)
▶ Schedules & triggers
▶ Lineage  24 upstream tables, 3 downstream tables
```

The bar column of a run and its matrix column share the same x-position;
`RET` on either opens the run page. Point motion over a column echoes
that run's start, duration and state. The page refreshes while visible
(3 s when a run is active, 60 s otherwise) and only redraws the parts
that changed.

**Other pages**, same blocks:

- **Run page**: header (state glyph, duration bar, trigger, parameters),
  task list as a tree with per-task glyph, duration bar and attempt,
  the task graph (Graphviz, section 4.12), output and error trace per
  task, actions cancel / repair / rerun.
- **Cluster page**: state glyph, DBR, node type, workers with a
  utilisation bar, autotermination countdown, events list
  (`clusters events`), libraries; actions start / restart / terminate.
- **Warehouse page**: state, size, running and queued queries, recent
  query history with duration sparkline.
- **Pipeline page**: latest update state, per-dataset glyphs and row
  counts, the dataset graph, events.

**Picker annotations** carry the same vocabulary, so the fuzzy list is
informative before anything is opened:

```
soe-etl-job              ✔✔✔✔✔✔✔✘✔⟳  ▂▂▃█▂▂▄█▂▁  hourly  ua-conversion…  2m ago
soe-backfill             ✔✔✔⊘✔        ▃▃▄▂▃      manual  sem            1d ago
```

(last runs as glyphs, their durations as a sparkline, trigger, creator,
last start). With Marginalia the annotation aligns automatically; without
it the annotation function pads itself.

**Optional graphics.** In a graphical Emacs, `databricks-pages-use-svg`
renders the bar chart and run matrix with `svg.el` (built in) instead of
text, with the same layout and click targets. Text stays the default and
the only mode in terminals.

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
- **On a cluster or serverless** (P1, needs the companion server): the
  server's `sql` request (`spark.sql(...)`, same row limit), queued per
  P-2. In P0 the SQL target is a warehouse.
- **Results as Org tables** (`databricks-sql-result-mode`, derived from
  `org-mode`): each execution appends a section to a per-editor results
  buffer: a heading with the statement, target, row count, duration and
  a status glyph, then the result as an **Org table**. Limit:
  `databricks-sql-max-rows`, default **2000**, sent as `row_limit` so the
  warehouse stops early; a truncation note says when more rows exist.
  The table text is generated **pre-aligned** in Elisp (column widths
  from the data, `|` separators, a header rule), so no
  `org-table-align` pass runs over 2000 rows (measured on Emacs 30.2,
  2000 × 10 cells: 0.34 s to generate, insert, enable `org-mode` and
  fontify, against 0.97 s for `org-table-align` alone); Org then treats it as a
  normal table (sort, column math, `org-table-export` to CSV/TSV,
  export to HTML/Markdown). Cells are truncated to
  `databricks-sql-max-cell-width` (default 60) with the full value in a
  `help-echo`; `NULL` is shown as `∅`; nested types as compact JSON.
  Numeric columns are right-aligned. Typed columns come from the result
  manifest and are noted in a `#+COLUMNS`-style comment line above the
  table. Old sections fold; the buffer is capped by a section count.
- **Completion**: catalogs, schemas, tables and columns from the cached
  catalog (section 4.14), offered through `completion-at-point`.
- **History**: `query-history list` in a picker (category
  `databricks-query`) to reopen past statements, with duration and
  status annotations.
- **Org Babel**: `ob-databricks` SQL blocks with `:warehouse` or
  `:compute` header arguments; results as Org tables, same renderer and
  limit.

### 4.11 Jobs and runs (P0)

- **Pickers** (C9): `databricks-find-job` (category `databricks-job`;
  annotations: owner, schedule, tags, last run state and time; grouped
  by bundle or tag), `databricks-find-run` (category `databricks-run`;
  active first). An Embark keymap per category (if Embark is installed)
  offers run now, open, watch, cancel, repair, copy id/URL. Large
  workspaces: the job list is cached and refreshed asynchronously; with
  Consult installed, an async Consult source is used instead.
- **Jobs buffer** (`tabulated-list`, `tabulated-list-groups` by tag or
  bundle, a last-runs glyph column): `RET` opens the job page, `r` run
  now, `l` runs.
- **Runs buffer**: `jobs list-runs` with active-only toggle, filters by
  job, state and time; auto-refresh while visible.
- **Job page and run page**: section 4.9. Run data: `jobs get-run`
  (tasks, states, timings), `get-run-output` (logs, error trace with
  clickable local source locations), `export-run` (HTML, opened in the
  browser). Actions: cancel, cancel all, repair (choose failed tasks),
  rerun, run now with parameter prompts.
- **Run history for the page**: `jobs list-runs --job-id … --expand-tasks
  --limit N`, paged with `--page-token` (`p`/`n` like the web UI). One
  call per page gives the bar chart **and** the task matrix; no per-run
  `get-run` is needed.

**Jobs API facts** (verified by the operator against a workspace,
2026-11-10, API 2.2 and CLI `jobs list-runs --help`):

| Fact | Consequence |
| --- | --- |
| `expand_tasks=true` returns each run's `tasks[]` with `task_key`, `start_time`, `end_time`, `state`, `status` | Matrix from one paged call |
| Two state models per run and task: legacy `state` (`life_cycle_state`, `result_state`, `state_message`) and `status` (`state`, `termination_details{code, type, message}`) | Map from `status` first, fall back to `state`; table below |
| A **skipped** task has `status.state = TERMINATED`, `termination_details.code = SKIPPED`, `type = SUCCESS` (legacy: `SKIPPED` / `EXCLUDED`) | Map by `code`, never by `type` alone |
| A **retried** task appears **several times** in `tasks[]` with the same `task_key` (e.g. a `FAILED` attempt, then `SUCCESS`) | The cell shows the latest attempt (by `start_time`) with a "retried" face; `help-echo` and the run page list all attempts |
| Task `run_duration` is `null`; run `run_duration` is set (also while running) | Task duration = `end_time - start_time`; `end_time = 0` means still running |
| `start_time = 0` for tasks not started (`BLOCKED`) | Shown as not started, not as 1970 |
| Times are epoch milliseconds | Converted once on parse |
| `has_more` is `null` while `next_page_token` is present | Paging relies on the token only |
| `tasks[]` is not in dependency order | Topological sort for matrix rows and graphs |
| `depends_on[]` items may carry `outcome: "true"/"false"` (condition tasks) | Graph edges labelled with the outcome; tasks skipped by conditions are normal, shown grey, not as warnings |
| `creator_user_name` / `run_as_user_name` can be a service principal **application id** (UUID) | Resolve the display name via `service-principals list` (SCIM filter on `applicationId`), cached per workspace |
| `settings.deployment = {kind: BUNDLE, metadata_file_path: …/.bundle/<bundle>/<target>/…}` and `edit_mode = UI_LOCKED` | "Managed by bundle `<bundle>` (target `<target>`)" header line; parse the path, read `metadata.json` only on demand |
| `schedule.quartz_cron_expression` + `timezone_id` + `pause_status` | Header shows a human description (small Quartz-cron describer for common patterns, raw expression otherwise) and a paused marker |
| `settings.parameters[]` holds definitions; values per run are on the run | Job page lists names and defaults; run page shows values |
| `performance_target` was `null` while the web UI showed "Performance optimized: On" | Do not derive that line from this field; omit it until its source is known (Q14) |
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
- **Workspace** (P2): `workspace list` tree; export files; copy path;
  open in browser. No notebook editing (section 3.5).
- **Compute** (P0): section 4.4; cluster and warehouse pages in 4.9.

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
  catalog, workspace, doctor, chat.
- **Pickers**: `completing-read` with metadata (C9). Each object type
  has a category, so Marginalia/Embark integrations are plain keymaps
  and annotators keyed by category.
- **Lists**: `tabulated-list-mode` (with `tabulated-list-groups`) for
  resource lists; Org tables for SQL results (section 4.10).
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
| Run file with DB Connect | yes | run-file mode | P1 |
| Interactive REPL, send region/buffer | Interactive Window | REPL (companion server) | P1 |
| Progress and per-job cancel | Jupyter only / kill terminal | mode line, jobs, `interruptTag` | P1 (beyond) |
| Clusters, serverless | yes | compute buffer, context | P0 |
| SQL warehouses | — | compute buffer, SQL target | P0 (beyond) |
| **SQL editor + Org-table results (≤ 2000 rows), warehouses** | — | `databricks-sql-mode` | P0 (beyond) |
| SQL on cluster / serverless | — | via companion server | P1 |
| **Jobs/runs pickers, job and run pages, watch** | bundle runs only | section 4.9 text UI, notifications | P0 (beyond) |
| **Cluster and warehouse pages** | compute view | section 4.9 | P0 (beyond) |
| Bundles: validate/deploy/run/destroy, explorer, variables | yes | `databricks-bundle-mode` | P0 |
| Bundle `plan`, `generate`, bind | — | yes | P1 (beyond) |
| **Bundle and job task graphs** | — | Graphviz SVG, Mermaid export | P1 (beyond) |
| Pipelines: run, selective refresh, validate, events, diagnostics | yes | pipelines module, Flymake | P1 |
| **Pipeline dataset graph with status** | — | Graphviz SVG, Mermaid export | P1 (beyond) |
| Env setup (uv, `setup-local`) | yes | same command | P1 |
| Run file as job (`jobs submit`) | yes | `jobs submit` + run page | P1 |
| Unity Catalog explorer | yes | catalog browser | P1 |
| Workspace browser | yes | workspace tree (no notebook editing) | P2 |
| Query history | — | picker | P1 (beyond) |
| Org Babel (Python, SQL) | — | `ob-databricks` | P1 (beyond) |
| Debug | debugpy | dape + debugpy | P2 |
| AI Gateway chat | VS Code chat provider | gptel backends | P2 |
| Sync | yes | `bundle sync --watch` | P2 |
| SSH tunnel / remote IDE, AI-tools installer | yes | **out of scope** | — |
| Notebooks (source format, `.ipynb`, EIN), upload-and-run REPL | yes | **out of scope** (operator, 2026-11-10) | — |
| Dashboards, Apps, Genie, vector search | partial / no | out of scope for now (`bundle open` only) | — |

**P0 exit criterion** (no Python required). On a real workspace
(credential-gated CI job, or a manual checklist): select a context;
doctor passes; run SQL on a warehouse and get a 2000-row Org table in
under 2 s after the result arrives; cancel a running statement; find a
job by fuzzy search in Vertico and in Helm, open its page, run it, watch
it to completion with a notification; open a cluster and a warehouse
page and start/stop them; validate, plan, deploy and run a bundle
resource. Offline CI: ERT against the fake CLI, page rendering golden
files (unicode and ascii glyph sets), the Org-table renderer benchmark.

**P1 exit criterion**: the companion server (PySpark from buffers,
run-file, SQL on cluster/serverless, cancel) on a cluster and on
serverless; bundle and pipeline graphs match the workspace UI; pipeline
diagnostics land on the right lines.

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
  databricks-ui.el              ; glyphs, bars, sparklines, sections, buttons
  databricks-pages.el           ; job, run, cluster, warehouse, pipeline pages
  databricks-sql.el             ; databricks-sql-mode, statement execution
  databricks-sql-result.el      ; pre-aligned Org-table results, export
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
  session.py  cancel.py  progress.py  run.py  interrupt_cli.py
test/
  elisp/    (ERT; fake `databricks` replaying fixtures; stub server; graph golden files)
  python/   (pytest; fake session; optional workspace integration)
  fixtures/ (recorded CLI v1.25.0 JSON)
docs/design/spark-connect-emacs.md
```

Distribution: one MELPA package with `python/` in `:files`. Optional
integrations (gptel, Embark, Consult, Marginalia)
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
- No notebook support; notebook users keep the browser for editing.
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
| R9 | Text pages misalign (fonts, ambiguous-width glyphs, emoji) | Single-width glyphs only; `char-displayable-p` fallback to ascii; golden-file tests per glyph set |
| R10 | Scope creep | Phases P0–P3 with gates; out-of-scope list |
| R11 | Destructive actions on production | P-7; `:confirm` contexts; `bundle plan` before deploy |
| R12 | Large Org tables are slow to open or edit | Pre-aligned generation, 2000-row default cap, `org-table` features used on demand only; benchmark in CI |

## 9. Open questions (for the operator)

- **Q1. Naming.** Package `databricks` with prefix `databricks-`
  (clear, long names), or a short prefix? `dbx` collides with Databricks
  Labs' retired `dbx` tool. The doc file name stays until decided.
- **Q2. DB Connect progress.** Which versions expose the progress handler?
- **Q2a. Hard cancel.** Can a fresh client `interruptTag` an existing
  session by id on Databricks Connect?
- **Q3. Page refresh budget.** 3 s while a run is active and 60 s
  otherwise, only while visible: acceptable CLI load?
- **Q7. Windows.** Best effort only?
- **Q8. Compute selectors.** DB Connect behaviour when the config profile
  sets `cluster_id` and the env selects serverless.
- **Q10. Cancel scope in P0.** Precise local cancel, or `interruptTag`
  plus kill-and-restart first?
- **Q11. SQL on clusters.** Decided: warehouses in P0, cluster and
  serverless in P1 with the companion server.
- **Q12. Bundle graph source.** Confirm `bundle validate -o json` output
  is fully resolved; otherwise accept `bundle summary` as the source.
- **Q14. "Performance optimized".** Which field backs the web UI's
  toggle (`performance_target` was `null` on a job showing "On")?
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
- gptel: <https://github.com/karthink/gptel>
- Emacs 30 NEWS; `jsonrpc.el` 1.0.25; JSON measurements in draft 2,
  section 4.7.

## Appendix A. UI catalogue and keymap

All mock-ups use mock data. Glyphs are the `unicode` set (section 4.9);
the `ascii` set replaces them one-for-one. Warnings use `▲` (not `⚠`,
which many fonts draw as an emoji).

### A.1 Global elements

**Mode line** (right-aligned):

```
 DBX:prod-etl ● cluster RUNNING · ◆ wh-small · ⟳2 watched · soe/etl_prod
```

Context name, compute state, SQL target, watched runs (click lists them),
bundle and target when in a bundle project. Click opens the dispatch.

**Dispatch** (`databricks-dispatch`, transient):

```
Databricks · prod-etl (profile prod · cluster etl-shared ● · warehouse wh-small ◆)

Context          Jobs & runs         Compute             SQL
 x select         j find job          c find cluster      s SQL editor
 D doctor         J jobs list         C clusters list     h query history
                  r find run          w find warehouse    t SQL target
                  R active runs       W warehouses list
                  ! watched runs
Pipelines        Bundle              Browse              Other
 p find pipeline  b bundle menu…      u Unity Catalog     g graph (here)
 P pipelines list                     f workspace files   a AI chat
                                                          ? help
```

**Bundle menu** (`b` in the dispatch, or `C-c C-b` in bundle files):

```
Bundle soe · target etl_prod
 v validate   l plan   d deploy   D destroy   r run resource   s summary
 g graph      t target  o open in browser    G generate from existing
```

### A.2 Pickers (Vertico, Helm, Ivy, … via `completing-read`)

Each picker has a category; annotations use the page vocabulary.

```
Job: soe█
  soe-etl-job        ✔✔✔✔✔✔✔✘✔⟳  ▂▂▃█▂▂▄█▂▁  hourly   ua-conversion…  2m ago
  soe-backfill       ✔✔✔⊘✘        ▃▃▄▂▃      manual   sem             2d ago
  soe-dq-checks      ✔✔✔✔✔✔✔✔✔✔  ▁▁▁▁▂▁▁▁▁▁  daily    ua-team         9h ago

Run: █
  ⟳ soe-etl-job      810416184528902   PERIODIC   started 11m ago   running 11m
  ✔ soe-etl-job      215034528049341   PERIODIC   06:15             40m 14s
  ✘ soe-backfill     733120044511002   ONE_TIME   2d ago            3m 02s  RUN_EXECUTION_ERROR

Cluster: █
  ● etl-shared       16.4  SHARED       4/8 workers   sem   autoterm in 42m
  ○ ml-gpu           15.4  SINGLE_USER  terminated    ana
  ⟳ adhoc-sem        17.3  SINGLE_USER  starting      sem

Warehouse: █
  ◆ wh-small         Serverless  Small   RUNNING   2 running · 0 queued
  ◇ wh-reporting     Pro         Medium  STOPPED

Table: main.analytics.█
  main.analytics.daily_users       MV      12 cols   ua-team
  main.analytics.events_clean      ST      31 cols   ua-team
  main.analytics.v_country_rev     VIEW     5 cols   sem
```

Other pickers: context, pipeline, bundle resource, bundle target,
workspace path, query history (statement, warehouse, duration glyph).

### A.3 Job page

```
 soe-etl-job  ★                               [Run now] [Run with params] [Browser]
 Managed by bundle soe (target etl_prod)  ·  UI locked  ·  Job ID 276415565620551
 Creator ua-conversion-models-stage  ·  Run as ua-conversion-models-stage
 Schedule  At 55 minutes past the hour (GMT)  ·  active
 Tags  BU=ua  Component=SoE  Env=dev  JobType=etl
 Description  SoE data generation job

▼ Runs  (Nov 09 07:55 → Nov 10 06:55)                     [ older  ] newer
  3h 15m ┤                    █
         │                    █               ▃
  1h 37m ┤                    █           ▆   █
         │▃ ▂ ▂ ▂ ▂ ▃ ▂ ▂ ▂ ▂ █ ▂ ▂ ▃ ▂ ▄ █ ▃ █ ▂ ▂ ▂ ▁
         └──────────────────────────────────────────────
          Nov 09                  Nov 10
  soe_etl_preflight     ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔
  soe_etl_hour_le_5     ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔
  soe_etl_hour_ge_23    ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔
  soe_etl_job_offpeak   ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ⟳
  soe_etl_optimize_off… ✔ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ■
  soe_etl_job_peak_ear… ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ✔ ✔ ✔ ✔ ✔ ✔ ✔ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘ ⊘

▼ Run list
  Start          Run ID            Trigger    Duration   Status
  Nov 10 06:55   810416184528902   PERIODIC     11m 05s  ⟳ Running
  Nov 10 05:55   215034528049341   PERIODIC     40m 14s  ✔ Succeeded
  Nov 10 04:55   553993077518776   PERIODIC     50m 29s  ✔ Succeeded (retried)

▶ Tasks (9)   ▶ Parameters (11)   ▶ Schedule & triggers   ▶ Deployment
```

### A.4 Run page

```
 Run 553993077518776  ✔ Succeeded  ·  soe-etl-job  ·  PERIODIC
 Nov 10 04:55 → 05:45  ·  50m 29s  ·  1 retried task     [Cancel] [Repair] [Rerun] [Browser]
 Parameters  country_filter=US  os_filter=android  partition_date=2026-11-10  …(8 more)

▼ Timeline                 04:55       05:10       05:25       05:40
  ✔ soe_etl_preflight      ✘██▏········█▌
  ✔ soe_etl_hour_le_5                   ▏
  ✔ soe_etl_hour_ge_23                  ▏
  ✔ soe_etl_job_offpeak                 ████████████▌
  ✔ soe_etl_optimize_offpeak                         ████████▊
  ⊘ soe_etl_job_peak_early  condition soe_etl_hour_le_5 → true not met
  ⊘ soe_etl_job_peak_late   condition soe_etl_hour_ge_23 → true not met

▼ Attempts of soe_etl_preflight
  1 ✘ RUN_EXECUTION_ERROR (CLIENT_ERROR)  1m 54s  "Workload failed, see run output"  [L] log
  2 ✔ SUCCESS                              1m 29s

▶ Task graph   [G] Graphviz view
▶ Output: soe_etl_job_offpeak
```

### A.5 Lists

**Jobs** (`tabulated-list`, grouped by bundle or tag):

```
 ▼ bundle soe (etl_prod)
   soe-etl-job      ✔✔✔✔✔✔✔✘✔⟳  hourly  ua-conversion…  ⟳ running 11m
   soe-dq-checks    ✔✔✔✔✔✔✔✔✔✔  daily   ua-team        ✔ 9h ago
 ▼ untagged
   adhoc-export     ✘✔           manual  sem            ✘ 3d ago
```

**Clusters**:

```
  State      Name         DBR   Workers         Access        Owner  Autoterm
  ● RUNNING  etl-shared   16.4  4/8 ████▌       SHARED        sem    in 42m
  ⟳ PENDING  adhoc-sem    17.3  starting        SINGLE_USER   sem
  ○ TERM     ml-gpu       15.4  —               SINGLE_USER   ana
```

**Active runs**, **warehouses**, **pipelines** follow the same pattern.

### A.6 Cluster and warehouse pages

```
 etl-shared  ● RUNNING  ·  DBR 16.4 LTS  ·  SHARED  ·  owner sem   [Restart] [Terminate] [Use]
 Workers 4/8 ████▌    autoscale 2–8  ·  i3.xlarge  ·  autoterminates in 42m
▼ Events
  10:42  RESIZING        4 → 6 workers (autoscale)
  10:30  RUNNING
  10:21  STARTING        by sem
▶ Libraries (3)   ▶ Spark config   ▶ Tags
```

```
 wh-small  ◆ RUNNING  ·  Serverless Small  ·  2 running · 0 queued     [Stop] [Use for SQL]
▼ Recent queries               duration
  ✔ SELECT country, … 10:41    ▃ 1.8s   sem
  ✘ MERGE INTO …      10:30    █ 12.0s  etl-sp   [RET] statement  [e] error
```

### A.7 SQL editor and results

Editor header line (`databricks-sql-mode`):

```
 SQL ▸ ◆ wh-small (Serverless Small, RUNNING) · main.analytics · limit 2000
```

Results buffer (Org, one section per execution):

```
* ✔ 2000 rows (more available) · 1.8 s · wh-small · 10:42:13
  SELECT country, os, count(*) AS users, sum(revenue) AS revenue …
  # types: country STRING | os STRING | users BIGINT | revenue DECIMAL(18,2)
| country | os      |  users |     revenue |
|---------+---------+--------+-------------|
| US      | android | 912334 | 1203345.20  |
| US      | ios     | 840122 | 1530011.75  |
| DE      | android | 120443 |  98003.10   |
| …       | ∅       |        |             |
* ✘ failed · 0.4 s · wh-small · 10:40:02
  [UNRESOLVED_COLUMN] A column `reveneu` cannot be resolved …   (line 3)
```

### A.8 Bundles

**Summary**:

```
 Bundle soe · target etl_prod · /Workspace/Shared/.bundle/soe/etl_prod
▼ Jobs (2)
  ✔ soe-etl-job     9 tasks · hourly · last ✔ 52m ago      databricks.yml:42
  ✘ soe-backfill    3 tasks · manual · last ✘ 2d ago       resources/backfill.yml:3
▼ Pipelines (1)
  ⟳ soe_dlt         6 datasets · update running 3m         resources/dlt.yml:1
▶ Schemas (2)   ▶ Volumes (1)
```

**Plan** (illustrative; the exact `bundle plan` output format is to be
checked):

```
 bundle plan · soe · etl_prod
  ~ update  jobs.soe-etl-job      tasks.soe_etl_job_offpeak.timeout_seconds
  + create  pipelines.soe_dlt_v2
  - delete  jobs.soe-legacy       ▲ destructive
 [d] deploy   [q] quit
```

**Validate**: errors and warnings appear as Flymake diagnostics in the
YAML buffers, plus a summary in the echo area.

### A.9 Graphs

Graphviz SVG buffer (GUI): nodes coloured by state, condition edges
labelled `true`/`false`, legend in the corner; click or `RET` on a node.
Text fallback (and terminal Emacs):

```
 Job soe-etl-job · run 553993077518776
 ✔ soe_etl_preflight ─┬─▶ ✔ soe_etl_hour_le_5 ──false─┬─▶ ✔ soe_etl_job_offpeak ─▶ ✔ soe_etl_optimize_offpeak
                      │                     └──true──▶ ⊘ soe_etl_job_peak_early ─▶ ⊘ soe_etl_optimize_peak_early
                      └─▶ ✔ soe_etl_hour_ge_23 ─false─┘
                                            └──true──▶ ⊘ soe_etl_job_peak_late ──▶ ⊘ soe_etl_optimize_peak_late
```

```
 Pipeline soe_dlt · update 3f2a… ⟳ running · 4/6 flows done
 ✔ raw_events (ST) 1.2M ──┐
                          ├─▶ ✔ events_clean (ST) 1.1M ─┬─▶ ⟳ daily_users (MV)
 ✔ users_dim (MV) 40k ────┘                             └─▶ ○ revenue_by_country (MV)
 Expectations  events_clean: valid_ts ✔ 99.8%  non_null_user ▲ 97.1% (warn)
```

### A.10 Pipelines

```
 soe_dlt  ⟳ update 3f2a… running 3m  ·  serverless  ·  bundle soe   [Stop] [Full refresh] [Browser]
▼ Datasets
  ✔ raw_events          ST   1,204,233 rows   12s
  ✔ events_clean        ST   1,104,233 rows   41s   ▲ 1 expectation warning
  ⟳ daily_users         MV   running
  ○ revenue_by_country  MV   queued
▼ Events  [level ERROR,WARN,INFO]  following
  10:40:02 INFO   flow_progress  events_clean  COMPLETED 1,104,233 rows
  10:40:05 WARN   expectation    events_clean  non_null_user 2.9% failed   src/events.sql:14
  10:41:10 ERROR  flow_progress  revenue…      AnalysisException …          src/revenue.py:22
▶ History (20 updates)   ▶ Graph
```

### A.11 Unity Catalog and doctor

```
▼ main
  ▼ analytics
      MV   daily_users          12 cols   ua-team
      ST   events_clean         31 cols   ua-team
      VIEW v_country_rev         5 cols   sem
      VOL  raw_files
      FN   normalize_os
  ▶ staging
▶ samples
```

```
 Databricks doctor · prod-etl
 ✔ databricks CLI 1.25.0 (minimum 1.25.0)
 ✔ auth: profile prod → https://dbc-….cloud.databricks.com (OAuth U2M)
 ✔ warehouse wh-small: RUNNING
 ✔ cluster etl-shared: DBR 16.4, SHARED
 ▲ graphviz `dot` not found: graphs use text      [i] install hint
 ✘ .venv: pyspark installed next to databricks-connect (P1 features)   [f] fix hint
```

### A.12 Keymap

Nothing is bound globally by default (`C-c <letter>` belongs to the
user). The package provides `databricks-command-map`; suggested:

```elisp
(keymap-global-set "C-c d" databricks-command-map)
```

**Global prefix** (`C-c d …`, mirrors the dispatch):

| Key | Command | Key | Command |
| --- | --- | --- | --- |
| `C-c d d` | dispatch | `C-c d x` | select context |
| `C-c d j` | find job | `C-c d J` | jobs list |
| `C-c d r` | find run | `C-c d R` | active runs |
| `C-c d !` | watched runs | `C-c d D` | doctor |
| `C-c d c` | find cluster | `C-c d C` | clusters list |
| `C-c d w` | find warehouse | `C-c d W` | warehouses list |
| `C-c d s` | SQL editor | `C-c d h` | query history |
| `C-c d p` | find pipeline | `C-c d P` | pipelines list |
| `C-c d b` | bundle menu | `C-c d g` | graph for thing at point |
| `C-c d u` | Unity Catalog | `C-c d f` | workspace files |
| `C-c d a` | AI chat | | |

**All pages and lists** (read-only buffers, single keys):

| Key | Action | Key | Action |
| --- | --- | --- | --- |
| `RET` | open thing at point | `g` | refresh |
| `TAB` / `S-TAB` | fold section / all | `q` | quit window |
| `n` / `p` | next / previous item | `M-n` / `M-p` | next / previous section |
| `]` / `[` | newer / older page | `/` | filter (narrow) |
| `w` | copy id | `W` | copy URL |
| `o` | open in browser | `G` | graph view |
| `f` | watch (follow) toggle | `?` | actions menu (transient) |

**Object actions** (destructive ones confirm, P-7):

| Buffer | Keys |
| --- | --- |
| Job page / jobs list | `r` run now, `R` run with parameters, `l` runs, `C` cancel all active runs |
| Run page / runs list | `c` cancel, `R` repair (choose tasks), `x` rerun, `L` task output/log at point, `E` error trace |
| Cluster page / list | `s` start, `S` terminate, `x` restart, `e` events, `U` use as context compute |
| Warehouse page / list | `s` start, `S` stop, `U` use as SQL target, `h` its query history |
| Pipeline page / list | `s` start update, `F` full refresh, `T` refresh selected tables, `v` validate only, `d` dry run, `S` stop, `e` events, `H` history |
| Bundle summary | `RET` jump to YAML, `r` run resource, `v` validate, `l` plan, `d` deploy |
| Catalog | `RET` details, `i` insert name, `v` preview rows, `s` new SQL editor on it |

**SQL editor** (`databricks-sql-mode`, standard `C-c C-…` mode keys):

| Key | Action | Key | Action |
| --- | --- | --- | --- |
| `C-c C-c` | run statement at point | `C-c C-r` | run region |
| `C-c C-b` | run buffer | `C-c C-k` | cancel running statement |
| `C-c C-t` | choose target | `C-c C-l` | set row limit |
| `C-c C-h` | query history | `C-c C-o` | show results buffer |

**SQL results** (read-only Org, our minor-mode keys win):

| Key | Action | Key | Action |
| --- | --- | --- | --- |
| `n` / `p` | next / previous result | `TAB` | fold result |
| `r` | re-run this statement | `e` | export table (CSV/TSV/Markdown) |
| `c` / `C` | copy cell / row | `y` | copy table as TSV |
| `s` | sort by column at point | `E` | make editable (plain Org) |

**Embark** (optional, per category: job, run, cluster, warehouse,
pipeline, table): `o` open page, `r` run/start, `c` cancel/stop, `w`
copy id, `W` copy URL, `b` browser, `g` graph, `f` watch.
