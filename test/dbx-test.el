;;; dbx-test.el --- Tests for dbx  -*- lexical-binding: t; -*-

;;; Commentary:

;; Offline tests.  A fake `databricks' executable (test/bin/databricks)
;; replays recorded, anonymized API responses from test/fixtures.

;;; Code:

(require 'ert)
(require 'org)
(require 'dbx-el)

(defconst dbx-test-dir (file-name-directory (or load-file-name buffer-file-name)))

(defun dbx-test-fixture (name)
  (with-temp-buffer
    (insert-file-contents (expand-file-name (concat "fixtures/" name) dbx-test-dir))
    (if (string-suffix-p ".json" name)
        (dbx-json-parse-string (buffer-string))
      (buffer-string))))

(defmacro dbx-test-with-fake-cli (&rest body)
  "Run BODY with the fake CLI, a fresh cache and a temporary log."
  (declare (indent 0))
  `(let* ((dbx-cli-program (expand-file-name "bin/databricks" dbx-test-dir))
          (log (make-temp-file "dbx-fake-log"))
          (process-environment (cons (concat "DBX_FAKE_LOG=" log) process-environment))
          (dbx-contexts '((:name "test" :profile "TEST" :warehouse "wh-1")))
          (dbx-current-context-name "test")
          (dbx-glyph-style 'unicode))
     (unwind-protect
         (progn (clrhash dbx--cache) (clrhash dbx--principal-names) ,@body)
       (delete-file log))))

(defun dbx-test-wait (pred &optional timeout)
  "Wait until PRED returns non-nil or TIMEOUT seconds pass."
  (let ((end (+ (float-time) (or timeout 10))))
    (while (and (not (funcall pred)) (< (float-time) end))
      (accept-process-output nil 0.05))
    (funcall pred)))

;;;; Status mapping (verified API shapes)

(ert-deftest dbx-test-run-status-mapping ()
  (let* ((runs (alist-get 'runs (dbx-test-fixture "runs_list.json")))
         (running (nth 0 runs)) (retried (nth 2 runs)))
    (should (eq (dbx-run-status running) 'running))
    (should (eq (dbx-run-status (nth 1 runs)) 'success))
    (should (eq (dbx-run-task-status running "optimize_offpeak") 'blocked))
    (should (eq (dbx-run-task-status running "job_offpeak") 'running))
    ;; skipped: code SKIPPED with type SUCCESS must not be success
    (should (eq (dbx-run-task-status running "job_peak_early") 'skipped))
    ;; retried: same task_key twice, FAILED then SUCCESS
    (should (= (length (dbx-run-task-attempts retried "preflight")) 2))
    (should (eq (dbx-run-task-status retried "preflight") 'retried))
    (should (eq (dbx-run-status (car (dbx-run-task-attempts retried "preflight"))) 'failed))
    (should (eq (dbx-run-task-status retried "missing") 'absent))))

(ert-deftest dbx-test-run-status-legacy-and-unknown ()
  (should (eq (dbx-run-status '((state (life_cycle_state . "TERMINATED") (result_state . "CANCELED"))))
              'cancelled))
  (should (eq (dbx-run-status '((state (life_cycle_state . "SKIPPED")))) 'skipped))
  (should (eq (dbx-run-status '((status (state . "WEIRD")))) 'unknown))
  (should (eq (dbx-run-status '((status (state . "TERMINATED")
                                        (termination_details (code . "INTERNAL_ERROR")))))
              'failed))
  (should (eq (dbx-run-status nil) 'absent)))

(ert-deftest dbx-test-durations-and-times ()
  (should (equal (dbx-format-duration 664555) "11m 05s"))
  (should (equal (dbx-format-duration 11758000) "3h 15m 58s"))
  (should (equal (dbx-format-duration 450) "450ms"))
  (should (equal (dbx-format-duration nil) "—"))
  (should (equal (dbx-format-time 0) "—"))
  ;; task run_duration is null: computed from times; end 0 = still running
  (should (= (dbx-task-duration '((start_time . 1000) (end_time . 5000))) 4000))
  (should (null (dbx-task-duration '((start_time . 0) (end_time . 0)))))
  (should (> (dbx-task-duration `((start_time . ,(- (dbx-now-ms) 2000)) (end_time . 0))) 1000)))

(ert-deftest dbx-test-quartz ()
  (should (equal (dbx-describe-quartz "0 55 * * * ?" "GMT") "At 55 minutes past the hour (GMT)"))
  (should (equal (dbx-describe-quartz "0 30 3 * * ?") "Daily at 03:30"))
  (should (equal (dbx-describe-quartz "0 0 6 ? * MON") "Weekly on MON at 06:00"))
  (should (equal (dbx-describe-quartz "0 0/5 * * * ?") "0 0/5 * * * ?")))

(ert-deftest dbx-test-task-order ()
  (let* ((job (dbx-test-fixture "job_get.json"))
         (keys (dbx-task-keys (dbx-get job 'settings 'tasks))))
    (should (equal (car keys) "preflight"))
    (should (< (cl-position "hour_le_5" keys :test #'equal)
               (cl-position "job_offpeak" keys :test #'equal)))
    (should (< (cl-position "job_offpeak" keys :test #'equal)
               (cl-position "optimize_offpeak" keys :test #'equal)))
    (should (= (length keys) 6))))

(ert-deftest dbx-test-bars ()
  (let ((dbx-glyph-style 'unicode))
    (should (equal (dbx-hbar 4 8 4) "██"))
    (should (equal (dbx-hbar 0 8 4) ""))
    (should (equal (length (dbx-sparkline '(1 2 3 nil))) 4))
    (should (equal (dbx-vbars '(8 4) 2) '("█ " "██")))
    (let ((rows (dbx-vbars '(1 100) 4)))
      (should (= (length rows) 4))
      (should (equal (substring (car (last rows)) 0 1) "▁"))))
  (let ((dbx-glyph-style 'ascii))
    (should (equal (dbx-glyph 'success) "+"))
    (should (string-match-p "\\`=+\\'" (dbx-hbar 4 8 4)))))

(ert-deftest dbx-test-pad-and-widths ()
  (should (equal (dbx-pad "abc" 5) "abc  "))
  (should (equal (dbx-pad "abc" 5 t) "  abc"))
  (should (= (string-width (dbx-pad "abcdefgh" 5)) 5))
  ;; every unicode glyph is single width (emoji would break alignment)
  (dolist (g dbx--glyphs-unicode)
    (should (= (string-width (cdr g)) 1))))

;;;; SQL

(ert-deftest dbx-test-sql-statement-splitting ()
  (let ((text "select 1;\n-- comment; not a split\nselect 'a;b', \"c;d\", `e;f`;\n/* block; */ select 2 ;;  \n select 'it''s; fine'; -- trailing\n"))
    (should (equal (mapcar (lambda (b) (substring text (car b) (cdr b)))
                           (dbx-sql-statement-bounds text))
                   '("select 1"
                     "-- comment; not a split\nselect 'a;b', \"c;d\", `e;f`"
                     "/* block; */ select 2"
                     "select 'it''s; fine'"))))
  (should (equal (dbx-sql-statement-bounds "  \n -- only a comment\n") nil))
  (should (equal (length (dbx-sql-statement-bounds "select '\\';' as x; select 2")) 2)))

(ert-deftest dbx-test-sql-statement-at-point ()
  (with-temp-buffer
    (insert "select 1;\nselect 2;\n\nselect 3")
    (goto-char (point-min)) (search-forward "2")
    (let ((b (dbx-sql-statement-at-point)))
      (should (equal (buffer-substring (car b) (cdr b)) "select 2")))
    (goto-char (point-max))
    (let ((b (dbx-sql-statement-at-point)))
      (should (equal (buffer-substring (car b) (cdr b)) "select 3")))))

(ert-deftest dbx-test-sql-parameters ()
  (should (equal (dbx-sql-parameter-names
                  "select * from t where d = :day and c = :country and x::int > 1 and j:field = ':nope' -- :nope2\n and y = :day")
                 '("day" "country")))
  (should (equal (dbx-sql-parameter-names "select a[:x] from t") '("x")))
  (should (null (dbx-sql-parameter-names "select '::' || ':a'"))))

(ert-deftest dbx-test-sql-org-table ()
  (let* ((dbx-glyph-style 'unicode)
         (d (dbx-test-fixture "statement_done.json"))
         (cols (dbx-get d 'manifest 'schema 'columns))
         (rows (dbx-get d 'result 'data_array))
         (tbl (dbx-sql-org-table cols rows))
         (lines (split-string tbl "\n" t)))
    (should (= (length lines) 5))
    ;; all lines have the same display width (pre-aligned)
    (should (= 1 (length (delete-dups (mapcar #'string-width lines)))))
    ;; numeric column right-aligned, NULL glyph, pipes and newlines escaped
    (should (string-match-p "| 912334 |" tbl))
    (should (string-match-p "|      5 |" tbl))
    (should (string-match-p "∅" tbl))
    (should (string-match-p "a¦b" tbl))
    (should (string-match-p "multi line" tbl))
    ;; Org parses it to the same shape
    (with-temp-buffer
      (insert tbl) (org-mode) (goto-char (point-min))
      (let ((parsed (org-table-to-lisp)))
        (should (equal (car parsed) '("country" "users" "note")))
        (should (eq (cadr parsed) 'hline))
        (should (= (length (cddr parsed)) 3))))))

(ert-deftest dbx-test-sql-org-table-cell-truncation ()
  (let* ((dbx-sql-max-cell-width 10)
         (tbl (dbx-sql-org-table '(((name . "s") (type_name . "STRING")))
                                 (list (list (make-string 50 ?x))))))
    (should (string-match-p "x\\{9\\}…" tbl))))

(ert-deftest dbx-test-sql-execute-end-to-end ()
  (dbx-test-with-fake-cli
    (let ((buf (generate-new-buffer "q.sql")))
      (unwind-protect
          (with-current-buffer buf
            (sql-mode) (dbx-sql-mode 1)
            (insert "select country, users from t where d = :day")
            (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "2026-11-10")))
              (dbx-sql-execute-statement))
            (should (dbx-test-wait (lambda () (null dbx-sql--running)) 15))
            (let ((res (get-buffer (format "*dbx sql results: %s*" (buffer-name buf)))))
              (should res)
              (with-current-buffer res
                (should (string-match-p "✔ 3 rows" (buffer-string)))
                (should (string-match-p "| US      | 912334 |" (buffer-string)))
                (should buffer-read-only)
                (kill-buffer res)))
            ;; the request body carried the warehouse, row limit and typed parameter
            (let ((log (with-temp-buffer (insert-file-contents (getenv "DBX_FAKE_LOG")) (buffer-string))))
              (should (string-match-p "\"warehouse_id\":\"wh-1\"" log))
              (should (string-match-p "\"row_limit\":2000" log))
              (should (string-match-p "\"name\":\"day\",\"value\":\"2026-11-10\"" log))
              (should (string-match-p "--profile TEST" log))))
        (kill-buffer buf)))))

;;;; CLI layer

(ert-deftest dbx-test-cli-runner ()
  (dbx-test-with-fake-cli
    (let ((r (dbx-cli-sync '("api" "get" "/api/2.2/jobs/get?job_id=42"))))
      (should (equal (dbx-get r 'settings 'name) "etl-job")))
    (should-error (dbx-cli-sync '("api" "get" "/api/2.0/fail")) :type 'dbx-cli-error)
    (condition-case e (dbx-cli-sync '("api" "get" "/api/2.0/fail"))
      (dbx-cli-error (should (equal (cadr e) "boom from fake"))))))

(ert-deftest dbx-test-api-query-encoding ()
  (should (equal (dbx-url-encode-params '((a . 1) (b . "x y") (c . nil))) "a=1&b=x%20y")))

(ert-deftest dbx-test-context-env-scrubs ()
  (let* ((process-environment (list "DATABRICKS_HOST=https://other" "DATABRICKS_TOKEN=secret"
                                    "SPARK_REMOTE=sc://x" "PATH=/bin" "HOME=/home/u"))
         (dbx-contexts '((:name "a" :profile "PROD" :cluster "c-1")))
         (dbx-current-context-name "a")
         (env (dbx-context-env nil t)))
    (should-not (cl-find-if (lambda (s) (string-prefix-p "DATABRICKS_TOKEN" s)) env))
    (should-not (cl-find-if (lambda (s) (string-prefix-p "SPARK_REMOTE" s)) env))
    (should (member "DATABRICKS_CONFIG_PROFILE=PROD" env))
    (should (member "DATABRICKS_CLUSTER_ID=c-1" env))
    (should (member "PATH=/bin" env))
    (should (equal (sort (dbx-context-scrubbed-vars))
                   '("DATABRICKS_HOST" "DATABRICKS_TOKEN" "SPARK_REMOTE")))))

(ert-deftest dbx-test-config-profiles-drop-secrets ()
  (let ((file (make-temp-file "dbcfg")))
    (unwind-protect
        (progn
          (with-temp-file file
            (insert "[DEFAULT]\nhost = https://a.cloud.databricks.com\ntoken = dapiSECRET\n\n[prod]\nhost=b.cloud.databricks.com\n"))
          (let* ((dbx-config-file file) (profiles (dbx-config-profiles)))
            (should (equal (mapcar #'car profiles) '("DEFAULT" "prod")))
            (should-not (alist-get 'token (cdr (assoc "DEFAULT" profiles))))
            (let ((dbx-contexts '((:name "p" :profile "prod"))) (dbx-current-context-name "p"))
              (should (equal (dbx-context-host) "https://b.cloud.databricks.com")))))
      (delete-file file))))

;;;; Pages

(ert-deftest dbx-test-job-page-renders ()
  (dbx-test-with-fake-cli
    (let ((dbx-page-refresh-idle 3600) (dbx-page-refresh-active 3600))
      (save-window-excursion
        (dbx-job-page 42)
        (let ((buf (current-buffer)))
          (unwind-protect
              (progn
                (should (dbx-test-wait (lambda () (with-current-buffer buf
                                                    (string-match-p "Run list" (buffer-string))))))
                (let ((text (with-current-buffer buf (buffer-string))))
                  (should (string-match-p "etl-job" text))
                  (should (string-match-p "Managed by bundle soe (target etl_prod)" text))
                  (should (string-match-p "UI locked" text))
                  (should (string-match-p "At 55 minutes past the hour (GMT)" text))
                  ;; matrix: oldest run first; preflight retried then success, running last
                  (should (string-match-p "preflight +✔ ✔ ✔" text))
                  (should (string-match-p "job_peak_early +⊘ ⊘ ⊘" text))
                  (should (string-match-p "optimize_offpeak +✔ ✔ ■" text))
                  (should (string-match-p "1001 .*Succeeded" text)))
                (with-current-buffer buf
                  ;; the retried cell uses the retried face
                  (goto-char (point-min))
                  (re-search-forward "^  preflight +")
                  (should (eq (get-text-property (point) 'face) 'dbx-retried))
                  ;; RET target and id on cells
                  (should (equal (get-text-property (point) 'dbx-id) 1001))
                  (should (functionp (get-text-property (point) 'dbx-open)))))
            (kill-buffer buf)))))))

(ert-deftest dbx-test-run-page-renders ()
  (dbx-test-with-fake-cli
    (let ((dbx-page-refresh-idle 3600))
      (save-window-excursion
        (dbx-run-page 1001)
        (let ((buf (current-buffer)))
          (unwind-protect
              (progn
                (should (dbx-test-wait (lambda () (with-current-buffer buf
                                                    (string-match-p "Timeline" (buffer-string))))))
                (let ((text (with-current-buffer buf (buffer-string))))
                  (should (string-match-p "Run 1001 +✔ Succeeded" text))
                  (should (string-match-p "country_filter=US" text))
                  (should (string-match-p "2 attempts" text))
                  (should (string-match-p "RUN_EXECUTION_ERROR" text))
                  (should (string-match-p "Excluded because its conditional dependency" text))))
            (kill-buffer buf)))))))

(ert-deftest dbx-test-principal-name-resolves ()
  (dbx-test-with-fake-cli
    (let ((uuid "ebc9c1e3-74bf-4acf-8bf2-0f1bb14773c9"))
      (should (equal (dbx-principal-name uuid) uuid))
      (should (dbx-test-wait (lambda () (stringp (gethash uuid dbx--principal-names)))))
      (should (equal (dbx-principal-name uuid) "etl-service-principal"))
      (should (equal (dbx-principal-name "me@example.com") "me@example.com")))))

(ert-deftest dbx-test-job-bundle-and-schedule ()
  (let ((job (dbx-test-fixture "job_get.json")))
    (should (equal (dbx-job-bundle job) '("soe" . "etl_prod")))
    (should (equal (dbx-job-schedule-short job) "hourly"))))

;;;; Graphs

(defun dbx-test-job-graph ()
  (let ((job (dbx-test-fixture "job_get.json"))
        (run (dbx-test-fixture "run_get.json")))
    (dbx-job--task-graph "etl-job" (dbx-get job 'settings 'tasks) run)))

(ert-deftest dbx-test-graph-dot-and-mermaid ()
  (let* ((g (dbx-test-job-graph))
         (dot (dbx-graph-dot g))
         (mm (dbx-graph-mermaid g)))
    (should (string-match-p "\"preflight\" -> \"hour_le_5\"" dot))
    (should (string-match-p "\"hour_le_5\" -> \"job_offpeak\" \\[label=\"false\"\\]" dot))
    (should (string-match-p "URL=\"dbx:preflight\"" dot))
    (should (string-match-p "\\`flowchart LR" mm))
    (should (string-match-p "-->|true|" mm))
    (when (executable-find "dot")
      (with-temp-buffer
        (insert dot)
        (should (zerop (call-process-region (point-min) (point-max) "dot" t t nil "-Tsvg")))
        (should (string-match-p "<svg" (buffer-string)))))))

(ert-deftest dbx-test-graph-text-tree ()
  (let ((dbx-glyph-style 'unicode))
    (with-temp-buffer
      (dbx-graph-text (dbx-test-job-graph))
      (let ((text (buffer-string)))
        (should (string-match-p "\\`  ✔ preflight" text))
        (should (string-match-p "false─▶ ✔ job_offpeak" text))
        ;; job_offpeak has two parents: second time it is a back reference
        (should (string-match-p "↑ job_offpeak" text))))))

(ert-deftest dbx-test-graph-cmapx-parse ()
  (skip-unless (executable-find "dot"))
  (let ((dot (make-temp-file "g" nil ".dot" (dbx-graph-dot (dbx-test-job-graph))))
        (svg (make-temp-file "g" nil ".svg"))
        (map (make-temp-file "g" nil ".map")))
    (unwind-protect
        (progn
          (should (zerop (call-process "dot" nil nil nil "-Tsvg" "-o" svg "-Tcmapx" "-o" map dot)))
          (let ((areas (dbx-graph--parse-cmapx map)))
            (should (= (length areas) 6))
            (should (member "preflight" (mapcar #'car areas)))
            (dolist (a areas) (should (< (nth 1 a) (nth 3 a))))))
      (mapc #'delete-file (list dot svg map)))))

;;;; Pipelines

(ert-deftest dbx-test-pipeline-analysis-and-graph ()
  (let* ((events (alist-get 'events (dbx-test-fixture "pipeline_events.json")))
         (a (dbx-pipeline-analyze events))
         (ds (plist-get a :datasets))
         (g (dbx-pipeline-graph-from-events "p" events)))
    (should (= (hash-table-count (plist-get a :flows)) 3))
    (should (equal (plist-get (gethash "main.app.events_clean" ds) :rows) 1104233))
    (should (equal (plist-get (gethash "main.app.events_clean" ds) :expect-failed) 4233))
    ;; short (origin.dataset_name) and qualified (flow_definition) names are merged
    (should (equal (plist-get (gethash "main.app.events_clean" ds) :type) "STREAMING_TABLE"))
    (should-not (gethash "events_clean" ds))
    (should (= (hash-table-count ds) 4))
    (should (= (length (plist-get g :edges)) 3))
    (should (cl-find "main.app.users_dim" (plist-get g :nodes)
                     :key (lambda (n) (plist-get n :id)) :test #'equal))
    (should (eq (plist-get (cl-find "main.app.daily_users" (plist-get g :nodes)
                                    :key (lambda (n) (plist-get n :id)) :test #'equal)
                           :status)
                'running))))

;;;; Bundles

(ert-deftest dbx-test-bundle-diagnostics ()
  (let ((d (dbx-bundle-parse-diagnostics (dbx-test-fixture "bundle_validate.txt"))))
    (should (= (length d) 3))
    (should (equal (car d) '(:warning "unknown field: retries" "databricks.yml" 12 7)))
    (should (equal (nth 2 d) '(:error "cannot resolve variable ${var.missing}" "resources/other.yml" 8 1)))))

(ert-deftest dbx-test-bundle-graph-from-config ()
  (let* ((config (dbx-json-parse-string
                  "{\"bundle\":{\"name\":\"soe\",\"target\":\"dev\"},
                    \"resources\":{\"jobs\":{\"etl\":{\"name\":\"etl-job\",\"tasks\":[
                       {\"task_key\":\"a\",\"spark_python_task\":{}},
                       {\"task_key\":\"b\",\"depends_on\":[{\"task_key\":\"a\",\"outcome\":\"true\"}],
                        \"pipeline_task\":{\"pipeline_id\":\"${resources.pipelines.dlt.id}\"}}]}},
                     \"pipelines\":{\"dlt\":{\"name\":\"soe_dlt\"}}}}"))
         (g (dbx-bundle-graph-from-config config)))
    (should (equal (plist-get g :title) "Bundle soe"))
    (should (= (length (plist-get g :nodes)) 3))
    (should (cl-find "pipeline:dlt" (plist-get g :edges) :key (lambda (e) (plist-get e :to)) :test #'equal))
    (should (equal (plist-get (car (plist-get g :edges)) :label) "true"))))

(ert-deftest dbx-test-bundle-targets ()
  (let ((dir (make-temp-file "bundle" t)))
    (unwind-protect
        (progn
          (with-temp-file (expand-file-name "databricks.yml" dir)
            (insert "bundle:\n  name: x\ntargets:\n  dev:\n    mode: development\n    default: true\n\n  prod:\n    mode: production\nresources:\n  jobs: {}\n"))
          (should (equal (dbx-bundle-targets dir) '("dev" "prod")))
          (should (equal (file-truename (dbx-bundle-root (file-name-as-directory dir)))
                         (file-truename (file-name-as-directory dir)))))
      (delete-directory dir t))))

(defvar dbx-test--venv nil)

(defun dbx-test-venv ()
  "A uv venv without databricks-connect (the server runs in fake mode)."
  (or dbx-test--venv
      (let ((dir (expand-file-name "dbx-test-venv" temporary-file-directory)))
        (unless (file-executable-p (dbx-venv-python dir))
          (call-process "uv" nil nil nil "venv" "--python" "3.12" dir))
        (setq dbx-test--venv dir))))

(ert-deftest dbx-test-server-repl-roundtrip ()
  (skip-unless (executable-find "uv"))
  (let* ((dbx-connect-venv (dbx-test-venv))
         (process-environment (cons "DBX_EMACS_FAKE_SESSION=1" process-environment))
         (dbx-contexts '((:name "t" :profile "T" :cluster "c-1")))
         (dbx-current-context-name "t")
         (repl (lambda () (with-current-buffer (dbx-repl--ensure-buffer)
                            (buffer-substring-no-properties (point-min) (point-max))))))
    (unwind-protect
        (progn
          (dbx-server-start)
          (should (dbx-test-wait (lambda () (equal (plist-get dbx-server--session :state) "connected")) 20))
          (dbx-server-execute "print(6 * 7)")
          (should (dbx-test-wait (lambda () (string-match-p "^42$" (funcall repl)))))
          (should (dbx-test-wait (lambda () (string-match-p "✔ \\[1\\] ok" (funcall repl)))))
          ;; interactive expression value and error status
          (dbx-server-execute "40 + 2" nil "interactive")
          (should (dbx-test-wait (lambda () (string-match-p "✔ \\[2\\] ok" (funcall repl)))))
          (dbx-server-execute "1/0")
          (should (dbx-test-wait (lambda () (string-match-p "ZeroDivisionError" (funcall repl)))))
          (should (dbx-test-wait (lambda () (string-match-p "✘ \\[3\\] error" (funcall repl)))))
          ;; interrupt a long loop
          (dbx-server-execute "import time\nfor _ in range(400): time.sleep(0.05)")
          (should (dbx-test-wait (lambda () (equal (plist-get (gethash 4 dbx-server--jobs) :status) "running"))))
          (dbx-server-interrupt 4)
          (should (dbx-test-wait (lambda () (string-match-p "\\[4\\] interrupted" (funcall repl))) 5))
          ;; the mode line shows the session
          (should (string-match-p "py" (or (dbx-server--mode-line) ""))))
      (dbx-server-stop)
      (when (get-buffer dbx-repl-buffer-name)
        (let ((kill-buffer-query-functions nil)) (kill-buffer dbx-repl-buffer-name))))))

;;;; Python package (needs uv; skipped otherwise)

(ert-deftest dbx-test-python-suite ()
  (skip-unless (executable-find "uv"))
  (let* ((root (expand-file-name ".." dbx-test-dir))
         (default-directory root)
         (out (with-temp-buffer
                (list (call-process "uv" nil t nil "run" "--no-project" "--python" "3.12"
                                    "--with" "pytest" "pytest" "-q" "test/python")
                      (buffer-string)))))
    (unless (zerop (car out)) (message "%s" (cadr out)))
    (should (zerop (car out)))))

(provide 'dbx-test)
;;; dbx-test.el ends here
