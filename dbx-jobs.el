;;; dbx-jobs.el --- Jobs, runs, pages, pickers and watching  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; Jobs API 2.2 through `databricks api'.  Verified response shapes:
;; - runs/list with expand_tasks returns tasks[] per run;
;; - retried tasks repeat their task_key; skipped tasks have
;;   status.termination_details.code = SKIPPED (type SUCCESS);
;; - task run_duration is null; has_more may be null while
;;   next_page_token is set; times are epoch milliseconds.

;;; Code:

(require 'dbx-util)
(require 'dbx-cli)
(require 'dbx-page)
(require 'dbx-graph)

(defcustom dbx-job-page-runs 25
  "Maximum number of runs shown in the job page chart and matrix."
  :type 'integer :group 'dbx)

(defcustom dbx-jobs-max 2000
  "Maximum number of jobs fetched for pickers and lists."
  :type 'integer :group 'dbx)

(defcustom dbx-watch-interval 10
  "Seconds between polls of watched runs."
  :type 'number :group 'dbx)

(defcustom dbx-watch-started-runs t
  "Non-nil to watch runs started from Emacs automatically."
  :type 'boolean :group 'dbx)

(defvar dbx-run-finished-functions nil
  "Abnormal hook run when a watched run reaches a terminal state.
Each function is called with the run object (as returned by
runs/get) and the name of the context it ran in.")

;;;; API

(defun dbx-jobs-fetch (callback &optional error-callback force)
  "Fetch all jobs (cached 5 minutes) and call CALLBACK."
  (dbx-cached 'jobs 300
              (lambda (k)
                (dbx-api-pages "/api/2.2/jobs/list" :items-key 'jobs
                               :params '((limit . 100)) :max-items dbx-jobs-max
                               :callback k :error-callback error-callback))
              callback force))

(defun dbx-recent-runs-fetch (callback &optional error-callback force)
  "Fetch the most recent runs across jobs (cached 60 s)."
  (dbx-cached 'recent-runs 60
              (lambda (k)
                (dbx-api-pages "/api/2.2/jobs/runs/list" :items-key 'runs
                               :params '((limit . 25)) :max-items 100
                               :callback k :error-callback error-callback))
              callback force))

(defun dbx-job-runs-page (job-id token callback &optional error-callback)
  "Fetch one page of runs of JOB-ID with tasks, starting at TOKEN."
  (dbx-api "get" "/api/2.2/jobs/runs/list"
           :params `((job_id . ,job-id) (expand_tasks . "true")
                     (limit . ,(min 25 dbx-job-page-runs))
                     ,@(and token `((page_token . ,token))))
           :callback callback :error-callback error-callback))

(cl-defun dbx-job-latest-runs (job-id n callback &key context error-callback)
  "Call CALLBACK with the N latest runs of JOB-ID, newest first.
Runs are fetched without tasks, which keeps this cheap enough for a
dashboard of many jobs.  CONTEXT defaults to the active context."
  (dbx-api-pages "/api/2.2/jobs/runs/list" :items-key 'runs :max-items n
                 :params `((job_id . ,job-id) (limit . ,(max 1 (min 25 n))))
                 :context context :callback callback :error-callback error-callback))

(defun dbx--principal-uuid-p (s)
  (and (stringp s)
       (string-match-p "\\`[0-9a-f]\\{8\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{12\\}\\'" s)))

(defvar dbx--principal-names (make-hash-table :test #'equal))

(defvar dbx-principal-resolved-hook nil
  "Hook run after a service principal name was resolved.")

(defun dbx-principal-name (name)
  "Display name for NAME; service principal UUIDs are resolved lazily.
Return NAME until the lookup finishes, then run
`dbx-principal-resolved-hook'."
  (if (not (dbx--principal-uuid-p name)) (or name "—")
    (let ((hit (gethash name dbx--principal-names)))
      (cond ((stringp hit) hit)
            ((eq hit 'pending) name)
            (t (puthash name 'pending dbx--principal-names)
               (dbx-api "get" "/api/2.0/preview/scim/v2/ServicePrincipals"
                        :params `((filter . ,(format "applicationId eq \"%s\"" name)))
                        :callback (lambda (r)
                                    (puthash name (or (dbx-get r 'Resources 0 'displayName) name)
                                             dbx--principal-names)
                                    (run-hooks 'dbx-principal-resolved-hook))
                        :error-callback (lambda (_) (puthash name name dbx--principal-names)))
               name)))))

;;;; Job helpers

(defun dbx-job-name (job)
  (or (dbx-get job 'settings 'name) (format "job %s" (alist-get 'job_id job))))

(defun dbx-job-bundle (job)
  "Return (BUNDLE . TARGET) if JOB is managed by a bundle."
  (let ((dep (dbx-get job 'settings 'deployment)))
    (when (equal (alist-get 'kind dep) "BUNDLE")
      (let ((path (or (alist-get 'metadata_file_path dep) "")))
        (if (string-match "/\\.bundle/\\([^/]+\\)/\\([^/]+\\)/" path)
            (cons (match-string 1 path) (match-string 2 path))
          (cons "bundle" nil))))))

(defun dbx-job-schedule-short (job)
  (let ((sched (dbx-get job 'settings 'schedule))
        (trig (dbx-get job 'settings 'trigger)))
    (cond (sched
           (let ((expr (alist-get 'quartz_cron_expression sched)))
             (concat
              (pcase (split-string (or expr "") " +" t)
                (`(,_ ,_ "*" "*" "*" . ,_) "hourly")
                (`(,_ ,_ ,_ "*" "*" ,(or "?" "*") . ,_) "daily")
                (`(,_ ,_ ,_ "?" "*" ,_ . ,_) "weekly")
                (_ "cron"))
              (if (equal (alist-get 'pause_status sched) "PAUSED") " (paused)" ""))))
          ((dbx-get trig 'file_arrival) "file arrival")
          ((dbx-get trig 'table_update) "table update")
          ((dbx-get trig 'periodic) "periodic")
          ((dbx-get job 'settings 'continuous) "continuous")
          (t "manual"))))

(defun dbx-run-task-attempts (run key)
  "Attempts of task KEY in RUN, oldest first."
  (sort (cl-remove-if-not (lambda (tk) (equal (alist-get 'task_key tk) key))
                          (alist-get 'tasks run))
        :key (lambda (tk) (or (alist-get 'start_time tk) 0))))

(defun dbx-run-task-status (run key)
  "Normalized status of task KEY in RUN, with `retried' for retries."
  (let* ((attempts (dbx-run-task-attempts run key))
         (last (car (last attempts))))
    (cond ((null last) 'absent)
          ((and (> (length attempts) 1) (eq (dbx-run-status last) 'success)) 'retried)
          (t (dbx-run-status last)))))

(defun dbx-task-keys (tasks)
  "Task keys of TASKS (with depends_on) in dependency order."
  (let* ((keys (delete-dups (mapcar (lambda (tk) (alist-get 'task_key tk)) tasks)))
         (edges (cl-loop for tk in tasks
                         append (mapcar (lambda (d) (cons (alist-get 'task_key d)
                                                          (alist-get 'task_key tk)))
                                        (alist-get 'depends_on tk)))))
    (dbx-topo-sort keys edges)))

(defun dbx-run-active-p (run)
  (memq (dbx-run-status run) '(running waiting blocked)))

(defun dbx-runs-glyph-strip (runs)
  "Status glyphs of RUNS (in any order) as one string, oldest first."
  (mapconcat (lambda (r) (dbx-status-glyph (dbx-run-status r)))
             (sort (copy-sequence runs) :key (lambda (r) (or (alist-get 'start_time r) 0)))
             ""))

;;;; Pickers

(defun dbx--jobs-candidates (jobs runs)
  "Candidate table for JOBS: alist of (STRING . (JOB . RUNS))."
  (let ((by-job (make-hash-table :test #'equal))
        (names (make-hash-table :test #'equal)))
    (dolist (r runs) (push r (gethash (alist-get 'job_id r) by-job)))
    (dolist (j jobs) (cl-incf (gethash (dbx-job-name j) names 0)))
    (mapcar (lambda (j)
              (let ((name (dbx-job-name j)))
                (cons (if (> (gethash name names) 1)
                          (format "%s #%s" name (alist-get 'job_id j))
                        name)
                      (cons j (sort (gethash (alist-get 'job_id j) by-job)
                                    :key (lambda (r) (or (alist-get 'start_time r) 0)))))))
            jobs)))

(defun dbx--job-annotation (cands)
  (lambda (cand)
    (let* ((entry (cdr (assoc cand cands)))
           (job (car entry)) (runs (last (cdr entry) 10))
           (last-run (car (last runs))))
      (concat
       (propertize " " 'display '(space :align-to 32))
       ;; glyphs may be wider than one column: realign after each
       (dbx-runs-glyph-strip runs) (dbx-align-to 44)
       (propertize (dbx-sparkline (mapcar #'dbx-run-duration runs)) 'face 'dbx-dim)
       (dbx-align-to 56) (dbx-pad (dbx-job-schedule-short job) 9)
       "  " (propertize (dbx-pad (dbx-principal-name (alist-get 'creator_user_name job)) 22)
                        'face 'dbx-dim)
       "  " (if last-run (dbx-format-ago (alist-get 'start_time last-run)) "")))))

(defun dbx--job-group (cand transform)
  (if transform cand
    (let ((b (get-text-property 0 'dbx-bundle cand)))
      (if b (format "Bundle %s" b) "Jobs"))))

;;;###autoload
(defun dbx-find-job (&optional force)
  "Fuzzy-find a job and open its page.  With prefix FORCE, refetch."
  (interactive "P")
  (let* ((jobs (dbx-await (lambda (k e) (dbx-jobs-fetch k e force)) "jobs"))
         (runs (condition-case nil
                   (dbx-await (lambda (k e) (dbx-recent-runs-fetch k e force)))
                 (error nil)))
         (cands (mapcar (lambda (c)
                          (let ((b (dbx-job-bundle (cadr c))))
                            (cons (propertize (car c) 'dbx-bundle (car b)) (cdr c))))
                        (dbx--jobs-candidates jobs runs)))
         (pick (completing-read
                "Job: "
                (lambda (str pred action)
                  (if (eq action 'metadata)
                      `(metadata (category . dbx-job)
                                 (annotation-function . ,(dbx--job-annotation cands))
                                 (group-function . dbx--job-group))
                    (complete-with-action action cands str pred)))
                nil t)))
    (dbx-job-page (alist-get 'job_id (cadr (assoc pick cands))))))

(defun dbx--run-candidates (runs)
  (mapcar (lambda (r)
            (cons (format "%s %s  %s"
                          (dbx-status-glyph (dbx-run-status r))
                          (or (alist-get 'run_name r) "run")
                          (propertize (format "%s" (alist-get 'run_id r)) 'face 'dbx-dim))
                  r))
          runs))

;;;###autoload
(defun dbx-find-run (&optional force)
  "Fuzzy-find a recent run (active first) and open its page."
  (interactive "P")
  (let* ((runs (dbx-await (lambda (k e) (dbx-recent-runs-fetch k e force)) "runs"))
         (runs (sort (copy-sequence runs)
                     :key (lambda (r) (list (if (dbx-run-active-p r) 0 1)
                                            (- (or (alist-get 'start_time r) 0))))
                     :lessp (lambda (a b) (or (< (car a) (car b))
                                              (and (= (car a) (car b)) (< (cadr a) (cadr b)))))))
         (cands (dbx--run-candidates runs))
         (pick (completing-read
                "Run: "
                (lambda (str pred action)
                  (if (eq action 'metadata)
                      `(metadata (category . dbx-run)
                                 (display-sort-function . identity)
                                 (annotation-function
                                  . ,(lambda (c)
                                       (let ((r (cdr (assoc c cands))))
                                         (concat (propertize " " 'display '(space :align-to 50))
                                                 (dbx-pad (or (alist-get 'trigger r) "") 10)
                                                 "  " (dbx-pad (dbx-format-time (alist-get 'start_time r)) 13)
                                                 "  " (dbx-format-duration (dbx-run-duration r)))))))
                    (complete-with-action action cands str pred)))
                nil t)))
    (dbx-run-page (alist-get 'run_id (cdr (assoc pick cands))))))

;;;; Job page

(defvar-local dbx-job--id nil)
(defvar-local dbx-job--job nil)
(defvar-local dbx-job--runs nil)
(defvar-local dbx-job--tokens nil "Stack of page tokens for older pages.")
(defvar-local dbx-job--next-token nil)

(defvar-keymap dbx-job-page-mode-map
  :parent dbx-page-mode-map
  "r" #'dbx-job-run-now
  "R" #'dbx-job-run-with-params
  "l" #'dbx-job-runs-list
  "C" #'dbx-job-cancel-all
  "G" #'dbx-job-graph)

(define-derived-mode dbx-job-page-mode dbx-page-mode "DBX-Job"
  "Mode for a job page.

\\{dbx-job-page-mode-map}"
  (setq-local dbx-page-actions
              '(("r" "Run now" dbx-job-run-now)
                ("R" "Run with parameters" dbx-job-run-with-params)
                ("l" "Runs list" dbx-job-runs-list)
                ("C" "Cancel all active runs" dbx-job-cancel-all)
                ("G" "Task graph" dbx-job-graph)
                ("o" "Open in browser" dbx-page-browse)))
  (setq-local dbx-page-older-function #'dbx-job-older)
  (setq-local dbx-page-newer-function #'dbx-job-newer))

(defun dbx-job--rerender-all ()
  "Re-render job pages, e.g. after principal names were resolved."
  (dolist (buf (buffer-list))
    (when (and (eq (buffer-local-value 'major-mode buf) 'dbx-job-page-mode)
               (buffer-local-value 'dbx-job--job buf))
      (dbx--in-buffer buf (dbx-job--render)))))

(add-hook 'dbx-principal-resolved-hook #'dbx-job--rerender-all)

;;;###autoload
(defun dbx-job-page (job-id &optional context no-select)
  "Open the page of JOB-ID in CONTEXT and return its buffer.
CONTEXT (a name or plist) defaults to the active context.  With
NO-SELECT, prepare the page without displaying it.  Interactively,
read a job ID or a job or run URL; a URL of a known workspace opens
in that workspace's context."
  (interactive
   (let ((ref (dbx-read-object-ref "Job ID or URL" '(job run))))
     (list (or (plist-get ref :job-id) (plist-get ref :id)) (plist-get ref :context))))
  (let ((buf (dbx-page-buffer "job" job-id #'dbx-job-page-mode context)))
    (dbx--in-buffer buf
      (setq dbx-job--id job-id)
      (setq-local dbx-page-refresh-function #'dbx-job--refresh)
      (dbx-page-loading buf (format "job %s" job-id))
      (dbx-job--refresh))
    (dbx-page-display buf no-select)))

(defun dbx-job--refresh ()
  (let ((buf (current-buffer)) (id dbx-job--id)
        (token (car dbx-job--tokens)))
    (dbx-api "get" "/api/2.2/jobs/get" :params `((job_id . ,id)) :context (dbx-context)
             :error-callback (lambda (m) (dbx-page-error buf m))
             :callback
             (lambda (job)
               (dbx-job-runs-page
                id token
                (lambda (page)
                  (when (buffer-live-p buf)
                    (dbx--in-buffer buf
                      (setq dbx-job--job job
                            dbx-job--runs (alist-get 'runs page)
                            dbx-job--next-token (alist-get 'next_page_token page))
                      (dbx-page-rename (dbx-job-name job))
                      (setq dbx-page-active-p (cl-some #'dbx-run-active-p dbx-job--runs))
                      (dbx-job--render)
                      (dbx-page-schedule-refresh))))
                (lambda (m) (dbx-page-error buf m)))))))

(defun dbx-job-older ()
  "Show older runs."
  (interactive)
  (if (not dbx-job--next-token) (message "No older runs")
    (push dbx-job--next-token dbx-job--tokens)
    (dbx-job--refresh)))

(defun dbx-job-newer ()
  "Show newer runs."
  (interactive)
  (if (null dbx-job--tokens) (message "Already at the newest runs")
    (pop dbx-job--tokens)
    (dbx-job--refresh)))

(defconst dbx--gutter 24
  "Column of the first run in the job page chart and matrix.
Run I starts at column (+ dbx--gutter (* 2 I)) in both.")

(defun dbx-job--cell-end (i)
  "Spacer after the cell of run I in the chart and matrix.
It ends the cell at an absolute column, so a glyph drawn wider than
one column (fallback font, ambiguous width) does not shift the
cells after it."
  (dbx-align-to (+ dbx--gutter (* 2 i) 2)))

(defun dbx-job--render ()
  (let* ((job dbx-job--job)
         (settings (alist-get 'settings job))
         (width (max 40 (- (window-width (get-buffer-window (current-buffer))) 2)))
         (max-cols (max 5 (/ (- width dbx--gutter 4) 2)))
         (runs (reverse (seq-take dbx-job--runs (min dbx-job-page-runs max-cols))))
         (keys (dbx-task-keys (append (alist-get 'tasks settings)
                                      (cl-loop for r in runs append (alist-get 'tasks r)))))
         (bundle (dbx-job-bundle job))
         (id (alist-get 'job_id job)))
    (dbx-page-with-render
      ;; Header
      (insert " " (propertize (dbx-job-name job) 'face 'dbx-title) "   ")
      (dbx-page-button "Run now" #'dbx-job-run-now)
      (dbx-page-button "Run with params" #'dbx-job-run-with-params)
      (dbx-page-button "Graph" #'dbx-job-graph)
      (dbx-page-button "Browser" #'dbx-job-browse)
      (insert "\n")
      (let ((start (point-min)))
        (put-text-property start (point) 'dbx-url (dbx-url (format "/jobs/%s" id)))
        (put-text-property start (point) 'dbx-id id))
      (dbx-page-kv (and bundle "Managed by bundle")
                   (and bundle (format "%s%s" (car bundle)
                                       (if (cdr bundle) (format " (target %s)" (cdr bundle)) "")))
                   nil (and (equal (alist-get 'edit_mode settings) "UI_LOCKED") "UI locked")
                   "Job ID" id)
      (dbx-page-kv "Creator" (dbx-principal-name (alist-get 'creator_user_name job))
                   "Run as" (dbx-principal-name (alist-get 'run_as_user_name job)))
      (let ((sched (alist-get 'schedule settings)))
        (dbx-page-kv "Schedule"
                     (if sched
                         (dbx-describe-quartz (alist-get 'quartz_cron_expression sched)
                                              (alist-get 'timezone_id sched))
                       (dbx-job-schedule-short job))
                     nil (and sched (if (equal (alist-get 'pause_status sched) "PAUSED")
                                        (propertize "paused" 'face 'dbx-blocked)
                                      "active"))))
      (when-let* ((tags (alist-get 'tags settings)))
        (dbx-page-kv "Tags" (mapconcat (lambda (kv) (format "%s=%s" (car kv) (cdr kv))) tags "  ")))
      (when-let* ((d (alist-get 'description settings)))
        (dbx-page-kv "Description" d))
      (insert "\n")
      ;; Runs chart and matrix
      (dbx-page-heading "Runs"
                        (if runs
                            (format "(%s → %s)   [ older  ] newer"
                                    (dbx-format-time (alist-get 'start_time (car runs)))
                                    (dbx-format-time (alist-get 'start_time (car (last runs)))))
                          "no runs"))
      (when runs
        (dbx-job--insert-chart runs)
        (dbx-job--insert-matrix runs keys))
      (insert "\n")
      ;; Run list
      (dbx-page-heading "Run list" "RET open · c cancel (on run page)")
      (insert (propertize (format "  %-14s %-18s %-10s %10s   %s\n"
                                  "Start" "Run ID" "Trigger" "Duration" "Status")
                          'face 'dbx-dim))
      (dolist (r (reverse runs))
        (let ((start (point)) (rid (alist-get 'run_id r)))
          (insert (format "  %-14s %-18s %-10s %10s   %s %s\n"
                          (dbx-format-time (alist-get 'start_time r))
                          rid (or (alist-get 'trigger r) "")
                          (dbx-format-duration (dbx-run-duration r))
                          (dbx-status-glyph (dbx-run-status r))
                          (dbx-run-status-label r)))
          (dbx-page-item start (1- (point))
                         'dbx-open (lambda () (dbx-run-page rid))
                         'dbx-id rid 'dbx-run-id rid
                         'dbx-url (dbx-url (format "/jobs/%s/runs/%s" id rid)))))
      (insert "\n")
      ;; Tasks
      (let ((tasks (alist-get 'tasks settings)))
        (dbx-page-heading (format "Tasks (%d)" (length tasks)) nil t)
        (dolist (key (dbx-task-keys tasks))
          (let ((tk (cl-find key tasks :key (lambda (x) (alist-get 'task_key x)) :test #'equal)))
            (insert "  " (dbx-pad key 30) " "
                    (propertize (dbx--task-type tk) 'face 'dbx-dim)
                    (if-let* ((deps (alist-get 'depends_on tk)))
                        (concat "  ← "
                                (mapconcat (lambda (d) (concat (alist-get 'task_key d)
                                                               (if (alist-get 'outcome d)
                                                                   (format " (%s)" (alist-get 'outcome d))
                                                                 "")))
                                           deps ", "))
                      "")
                    "\n"))))
      (insert "\n")
      ;; Parameters
      (let ((params (alist-get 'parameters settings)))
        (dbx-page-heading (format "Parameters (%d)" (length params)) nil t)
        (dolist (p params)
          (insert "  " (dbx-pad (alist-get 'name p) 30) " "
                  (propertize (format "%s" (or (alist-get 'default p) "")) 'face 'dbx-dim) "\n")))
      (insert "\n"))))

(defun dbx--task-type (task)
  (let ((type (cl-find-if (lambda (kv) (string-suffix-p "_task" (symbol-name (car kv)))) task)))
    (if type (string-remove-suffix "_task" (symbol-name (car type))) "")))

(defun dbx-job--insert-chart (runs)
  (let* ((durs (mapcar #'dbx-run-duration runs))
         (mx (apply #'max 1 (delq nil durs)))
         (rows (dbx-vbars durs 4))
         (n (length runs)) (jid dbx-job--id))
    (cl-loop for row in rows for i from 0 do
             (insert "  " (dbx-pad (pcase i
                                     (0 (dbx-format-duration mx))
                                     (2 (dbx-format-duration (/ mx 2)))
                                     (_ ""))
                                   (- dbx--gutter 5) t)
                     (if (memq i '(0 2)) " ┤" " │")
                     (dbx-align-to dbx--gutter))
             (cl-loop for r in runs for c from 0 do
                      (let ((rid (alist-get 'run_id r))
                            (start (point)))
                        (insert (propertize (substring row c (1+ c))
                                            'face (dbx-status-face (dbx-run-status r))))
                        (dbx-page-item start (point)
                                       'dbx-open (lambda () (dbx-run-page rid))
                                       'dbx-id rid 'dbx-run-id rid
                                       'dbx-url (dbx-url (format "/jobs/%s/runs/%s" jid rid))
                                       'help-echo (format "%s · %s · %s"
                                                          (dbx-format-time (alist-get 'start_time r))
                                                          (dbx-format-duration (dbx-run-duration r))
                                                          (dbx-run-status-label r)))
                        (remove-text-properties start (1+ start) '(dbx-item-start nil))
                        (insert (dbx-job--cell-end c))))
             (insert "\n"))
    (insert "  " (make-string (- dbx--gutter 5) ?\s) " └" (make-string (1+ (* 2 n)) ?─) "\n")
    ;; date labels where the day changes
    (let ((line (make-string (* 2 n) ?\s)) (prev nil))
      (cl-loop for r in runs for c from 0 do
               (let ((day (dbx-format-time (alist-get 'start_time r) "%b %d")))
                 (when (and (not (equal day prev)) (<= (+ (* 2 c) 6) (length line))
                            (string-blank-p (substring line (* 2 c) (min (length line) (+ (* 2 c) 7)))))
                   (setq line (concat (substring line 0 (* 2 c)) day
                                      (substring line (+ (* 2 c) (length day))))))
                 (setq prev day)))
      (insert "  " (make-string (- dbx--gutter 2) ?\s) (propertize line 'face 'dbx-dim) "\n"))))

(defun dbx-job--insert-matrix (runs keys)
  (let ((jid dbx-job--id))
    (dolist (key keys)
      ;; one blank column at least between the label and the first cell
      (insert "  " (dbx-pad key (- dbx--gutter 3)) (dbx-align-to dbx--gutter))
      (cl-loop for r in runs for i from 0 do
               (let* ((st (dbx-run-task-status r key))
                      (attempts (dbx-run-task-attempts r key))
                      (rid (alist-get 'run_id r))
                      (start (point)))
                 (insert (dbx-status-glyph st))
                 (dbx-page-item start (point)
                                'dbx-open (lambda () (dbx-run-page rid))
                                'dbx-id rid 'dbx-run-id rid
                                'dbx-url (dbx-url (format "/jobs/%s/runs/%s" jid rid))
                                'help-echo (concat key " · " (dbx-format-time (alist-get 'start_time r))
                                                   (mapconcat (lambda (a)
                                                                (format "\n  %s %s %s"
                                                                        (dbx-glyph (dbx-run-status a))
                                                                        (dbx-run-status-label a)
                                                                        (or (dbx-get a 'status 'termination_details 'message) "")))
                                                              attempts "")))
                 (remove-text-properties start (1+ start) '(dbx-item-start nil))
                 (insert (dbx-job--cell-end i))))
      (insert "\n"))))

(defun dbx-job-browse ()
  "Open the job in the browser."
  (interactive)
  (browse-url (dbx-url (format "/jobs/%s" dbx-job--id))))

;;;; Job actions

(defvar dbx-jobs--jobs)

(defun dbx--job-id-at-point ()
  (or (plist-get (dbx-job-at-point) :id)
      (read-string "Job ID: ")))

(defun dbx-job-at-point ()
  "The job at point as a plist (:id ID :name NAME :context CONTEXT-NAME).
Works in job and run pages and in the jobs and runs lists; return
nil elsewhere.  Never prompts."
  (cond
   ((derived-mode-p 'dbx-job-page-mode)
    (and dbx-job--id
         (list :id dbx-job--id :name (and dbx-job--job (dbx-job-name dbx-job--job))
               :context (dbx-context-name))))
   ((derived-mode-p 'dbx-run-page-mode)
    (when-let* ((jid (alist-get 'job_id dbx-run--run)))
      (list :id jid :name (alist-get 'run_name dbx-run--run) :context (dbx-context-name))))
   ((derived-mode-p 'dbx-jobs-list-mode)
    (when-let* ((id (dbx-list-row-id)))
      (list :id id
            :name (let ((job (cl-find id dbx-jobs--jobs :key (lambda (j) (alist-get 'job_id j))
                                      :test #'equal)))
                    (if job (dbx-job-name job) (aref (tabulated-list-get-entry) 1)))
            :context (dbx-context-name (dbx-list-row-context)))))
   ((derived-mode-p 'dbx-runs-list-mode)
    (when-let* ((run (dbx-runs--run-at-point))
                (jid (alist-get 'job_id run)))
      (list :id jid :name (alist-get 'run_name run)
            :context (dbx-context-name (dbx-list-row-context)))))))

(defun dbx-run-at-point ()
  "The run at point as a plist (:id :job-id :name :context), or nil.
:context is the context name.  Works in run pages, on runs in job
pages and in the runs list.  Never prompts."
  (let ((mk (lambda (rid run ctx)
              (and rid (list :id rid :job-id (alist-get 'job_id run)
                             :name (alist-get 'run_name run)
                             :context (dbx-context-name ctx))))))
    (cond
     ((derived-mode-p 'dbx-run-page-mode)
      (funcall mk dbx-run--id dbx-run--run nil))
     ((derived-mode-p 'dbx-job-page-mode)
      (when-let* ((rid (or (get-text-property (point) 'dbx-run-id)
                           (and (> (point) (point-min))
                                (get-text-property (1- (point)) 'dbx-run-id)))))
        (funcall mk rid (or (cl-find rid dbx-job--runs :key (lambda (r) (alist-get 'run_id r)))
                            `((job_id . ,dbx-job--id)))
                 nil)))
     ((derived-mode-p 'dbx-runs-list-mode)
      (when-let* ((run (dbx-runs--run-at-point)))
        (funcall mk (alist-get 'run_id run) run (dbx-list-row-context)))))))

(defun dbx--confirm-context-p ()
  (dbx-context-get :confirm))

(defun dbx-job-run-now (&optional job-id params)
  "Trigger JOB-ID now with optional job PARAMS alist."
  (interactive)
  (let ((job-id (or job-id (dbx--job-id-at-point))))
    (when (or (not (dbx--confirm-context-p))
              (dbx-confirm (format "Run job %s in context %s?" job-id
                                   (dbx-context-get :name))))
      (dbx-api "post" "/api/2.2/jobs/run-now"
               :body `((job_id . ,(if (stringp job-id) (string-to-number job-id) job-id))
                       ,@(and params `((job_parameters . ,params))))
               :callback (lambda (r)
                           (let ((rid (alist-get 'run_id r)))
                             (message "dbx: started run %s" rid)
                             (when dbx-watch-started-runs (dbx-watch-run rid (dbx-context)))
                             (dbx-cache-clear)
                             (dbx-run-page rid)))))))

(defun dbx-job-run-with-params ()
  "Trigger the job, prompting for each job parameter."
  (interactive)
  (let* ((job-id (dbx--job-id-at-point))
         (job (or (and (derived-mode-p 'dbx-job-page-mode) dbx-job--job)
                  (dbx-await (lambda (k e) (dbx-api "get" "/api/2.2/jobs/get"
                                                    :params `((job_id . ,job-id))
                                                    :callback k :error-callback e))
                             "job")))
         (params (cl-loop for p in (dbx-get job 'settings 'parameters)
                          collect (cons (intern (alist-get 'name p))
                                        (read-string (format "%s: " (alist-get 'name p))
                                                     (format "%s" (or (alist-get 'default p) "")))))))
    (dbx-job-run-now job-id params)))

(defun dbx-job-cancel-all ()
  "Cancel all active runs of the job."
  (interactive)
  (let ((job-id (dbx--job-id-at-point)))
    (when (dbx-confirm (format "Cancel ALL active runs of job %s?" job-id))
      (dbx-api "post" "/api/2.2/jobs/runs/cancel-all"
               :body `((job_id . ,(if (stringp job-id) (string-to-number job-id) job-id)))
               :callback (lambda (_) (message "dbx: cancel requested"))))))

(defun dbx-job-graph ()
  "Show the task graph of the job, coloured by its latest run."
  (interactive)
  (let* ((job dbx-job--job)
         (latest (car dbx-job--runs)))
    (dbx-graph-show (dbx-job--task-graph (dbx-job-name job)
                                         (dbx-get job 'settings 'tasks) latest)
                    "job-graph" dbx-job--id)))

(defun dbx-job--task-graph (title tasks run)
  "Graph of TASKS (with depends_on), statuses from RUN if given."
  (let* ((keys (dbx-task-keys tasks))
         (rid (alist-get 'run_id run)))
    (list :title title
          :subtitle (if run (format "Latest run %s · %s" rid (dbx-run-status-label run)) "")
          :nodes (mapcar (lambda (k)
                           (let ((attempts (and run (dbx-run-task-attempts run k))))
                             (list :id k :label k
                                   :status (and run (dbx-run-task-status run k))
                                   :info (and attempts
                                              (not (memq (dbx-run-task-status run k) '(skipped absent)))
                                              (let ((d (dbx-task-duration (car (last attempts)))))
                                                (and d (dbx-format-duration d))))
                                   :action (and rid (lambda () (dbx-run-page rid))))))
                         keys)
          :edges (cl-loop for tk in tasks
                          append (mapcar (lambda (d)
                                           (list :from (alist-get 'task_key d)
                                                 :to (alist-get 'task_key tk)
                                                 :label (alist-get 'outcome d)))
                                         (alist-get 'depends_on tk))))))

;;;; Run page

(defvar-local dbx-run--id nil)
(defvar-local dbx-run--run nil)

(defvar-keymap dbx-run-page-mode-map
  :parent dbx-page-mode-map
  "c" #'dbx-run-cancel
  "R" #'dbx-run-repair
  "x" #'dbx-run-rerun
  "L" #'dbx-run-task-output
  "G" #'dbx-run-graph
  "j" #'dbx-run-job-page)

(define-derived-mode dbx-run-page-mode dbx-page-mode "DBX-Run"
  "Mode for a job run page.

\\{dbx-run-page-mode-map}"
  (setq-local dbx-page-actions
              '(("c" "Cancel run" dbx-run-cancel)
                ("R" "Repair (rerun failed tasks)" dbx-run-repair)
                ("x" "Rerun (run job now)" dbx-run-rerun)
                ("L" "Task output at point" dbx-run-task-output)
                ("G" "Task graph" dbx-run-graph)
                ("j" "Job page" dbx-run-job-page)
                ("f" "Watch" dbx-page-watch)))
  (setq-local dbx-page-watch-function (lambda () (dbx-watch-run dbx-run--id (dbx-context)))))

;;;###autoload
(defun dbx-run-page (run-id &optional context no-select)
  "Open the page of job run RUN-ID in CONTEXT and return its buffer.
CONTEXT (a name or plist) defaults to the active context.  With
NO-SELECT, prepare the page without displaying it.  Interactively,
read a run ID or a run URL."
  (interactive
   (let ((ref (dbx-read-object-ref "Run ID or URL" '(run) 'run)))
     (list (plist-get ref :id) (plist-get ref :context))))
  (let ((buf (dbx-page-buffer "run" run-id #'dbx-run-page-mode context)))
    (dbx--in-buffer buf
      (setq dbx-run--id run-id)
      (setq-local dbx-page-refresh-function #'dbx-run--refresh)
      (dbx-page-loading buf (format "run %s" run-id))
      (dbx-run--refresh))
    (dbx-page-display buf no-select)))

(defun dbx-run--refresh ()
  (let ((buf (current-buffer)))
    (dbx-api "get" "/api/2.2/jobs/runs/get" :params `((run_id . ,dbx-run--id))
             :context (dbx-context)
             :error-callback (lambda (m) (dbx-page-error buf m))
             :callback (lambda (run)
                         (when (buffer-live-p buf)
                           (dbx--in-buffer buf
                             (setq dbx-run--run run)
                             (setq dbx-page-active-p (dbx-run-active-p run))
                             (dbx-run--render)
                             (dbx-page-schedule-refresh)))))))

(defun dbx-run--render ()
  (let* ((run dbx-run--run)
         (rid (alist-get 'run_id run)) (jid (alist-get 'job_id run))
         (tasks (alist-get 'tasks run))
         (keys (dbx-task-keys tasks))
         (start (alist-get 'start_time run))
         (end (let ((e (alist-get 'end_time run))) (if (and e (> e 0)) e (dbx-now-ms))))
         (span (max 1 (- end (or start end))))
         (width (max 20 (min 60 (- (window-width (get-buffer-window (current-buffer))) 50))))
         (url (dbx-url (format "/jobs/%s/runs/%s" jid rid))))
    (dbx-page-with-render
      (insert " " (propertize (format "Run %s" rid) 'face 'dbx-title) "  "
              (dbx-status-glyph (dbx-run-status run)) " " (dbx-run-status-label run)
              "   ")
      (when (dbx-run-active-p run) (dbx-page-button "Cancel" #'dbx-run-cancel))
      (dbx-page-button "Repair" #'dbx-run-repair)
      (dbx-page-button "Job" #'dbx-run-job-page)
      (dbx-page-button "Browser" (lambda () (interactive) (browse-url url)))
      (insert "\n")
      (put-text-property (point-min) (point) 'dbx-url url)
      (put-text-property (point-min) (point) 'dbx-id rid)
      (dbx-page-kv nil (alist-get 'run_name run) "Trigger" (alist-get 'trigger run)
                   "Type" (alist-get 'run_type run))
      (dbx-page-kv "Start" (dbx-format-time start "%b %d %H:%M:%S")
                   "End" (if (dbx-run-active-p run) "running"
                           (dbx-format-time (alist-get 'end_time run) "%H:%M:%S"))
                   "Duration" (dbx-format-duration (dbx-run-duration run)))
      (when-let* ((msg (or (dbx-get run 'status 'termination_details 'message)
                           (dbx-get run 'state 'state_message))))
        (unless (string-empty-p msg) (dbx-page-kv "Message" msg)))
      (when-let* ((params (alist-get 'job_parameters run)))
        (dbx-page-kv "Parameters"
                     (mapconcat (lambda (p) (format "%s=%s" (alist-get 'name p)
                                                    (or (alist-get 'value p) (alist-get 'default p) "")))
                                params "  ")))
      (insert "\n")
      ;; Timeline
      (dbx-page-heading "Timeline"
                        (format "%s → %s" (dbx-format-time start "%H:%M")
                                (dbx-format-time end "%H:%M")))
      (dolist (key keys)
        (let* ((attempts (dbx-run-task-attempts run key))
               (last (car (last attempts)))
               (st (dbx-run-task-status run key))
               (line-start (point))
               (bar (make-string width ?\s)))
          (insert "  " (dbx-status-glyph st) (dbx-align-to 4) (dbx-pad key 30) " ")
          (if (memq (dbx-run-status last) '(skipped))
              (insert (propertize (or (dbx-get last 'state 'state_message) "skipped") 'face 'dbx-dim))
            (let ((bar-start (point)))
              (insert bar)
              (dolist (a attempts)
                (let* ((s (alist-get 'start_time a)) (d (dbx-task-duration a)))
                  (when (and s (> s 0) d start)
                    (let* ((off (floor (* width (/ (float (- s start)) span))))
                           (len (max 1 (round (* width (/ (float d) span)))))
                           (off (min off (1- width)))
                           (len (min len (- width off)))
                           (seg (if (dbx--ascii-p) (make-string len ?=)
                                  (make-string len ?█))))
                      (save-excursion
                        (goto-char (+ bar-start off))
                        (delete-char len)
                        (insert (propertize seg 'face (dbx-status-face (dbx-run-status a)))))))))
              (goto-char (point-max))
              (insert " " (propertize (dbx-format-duration (and last (dbx-task-duration last)))
                                      'face 'dbx-dim)
                      (if (> (length attempts) 1)
                          (propertize (format "  %d attempts" (length attempts)) 'face 'dbx-blocked)
                        ""))))
          (insert "\n")
          (let ((task-run-id (alist-get 'run_id last)))
            (dbx-page-item line-start (1- (point))
                           'dbx-open (lambda () (dbx-run-show-output task-run-id key))
                           'dbx-id task-run-id
                           'dbx-task key))))
      (insert "\n")
      ;; Attempts and errors
      (let ((multi (cl-remove-if-not (lambda (k) (> (length (dbx-run-task-attempts run k)) 1)) keys))
            (failed (cl-remove-if-not (lambda (k) (eq (dbx-run-task-status run k) 'failed)) keys)))
        (when (or multi failed)
          (dbx-page-heading "Attempts and errors")
          (dolist (k (delete-dups (append failed multi)))
            (insert "  " (propertize k 'face 'bold) "\n")
            (cl-loop for a in (dbx-run-task-attempts run k) for i from 1 do
                     (let ((s (point)) (trid (alist-get 'run_id a)))
                       (insert (format "    %d %s %-22s %10s  %s\n" i
                                       (dbx-status-glyph (dbx-run-status a))
                                       (or (dbx-get a 'status 'termination_details 'code)
                                           (dbx-get a 'state 'result_state) "")
                                       (dbx-format-duration (dbx-task-duration a))
                                       (or (dbx-get a 'status 'termination_details 'message)
                                           (dbx-get a 'state 'state_message) "")))
                       (dbx-page-item s (1- (point))
                                      'dbx-open (lambda () (dbx-run-show-output trid k))
                                      'dbx-id trid))))
          (insert "\n")))
      (insert (propertize "  RET on a task: output · G graph · R repair · c cancel · f watch\n"
                          'face 'dbx-dim)))))

(defun dbx-run-job-page ()
  "Open the job of this run."
  (interactive)
  (dbx-job-page (alist-get 'job_id dbx-run--run)))

(defun dbx-run-graph ()
  "Show the task graph of this run."
  (interactive)
  (let ((run dbx-run--run))
    (dbx-graph-show (dbx-job--task-graph (format "Run %s" (alist-get 'run_id run))
                                         (cl-remove-duplicates (alist-get 'tasks run)
                                                               :key (lambda (x) (alist-get 'task_key x))
                                                               :test #'equal :from-end t)
                                         run)
                    "run-graph" (alist-get 'run_id run))))

(defun dbx-run-cancel ()
  "Cancel this run."
  (interactive)
  (let ((rid (or (get-text-property (point) 'dbx-id) dbx-run--id)))
    (setq rid (if (and (derived-mode-p 'dbx-run-page-mode) (not (get-text-property (point) 'dbx-task)))
                  dbx-run--id rid))
    (when (dbx-confirm (format "Cancel run %s?" rid))
      (dbx-api "post" "/api/2.2/jobs/runs/cancel"
               :body `((run_id . ,(if (stringp rid) (string-to-number rid) rid)))
               :callback (lambda (_) (message "dbx: cancel requested for %s" rid))))))

(defun dbx-run-repair ()
  "Repair this run: rerun chosen failed tasks."
  (interactive)
  (let* ((run dbx-run--run) (buf (current-buffer))
         (keys (dbx-task-keys (alist-get 'tasks run)))
         (failed (cl-remove-if-not
                  (lambda (k) (memq (dbx-run-task-status run k) '(failed cancelled)))
                  keys))
         (chosen (completing-read-multiple
                  "Rerun tasks (empty = all failed): " keys nil t
                  (string-join failed ","))))
    (when (dbx-confirm (format "Repair run %s rerunning %s?" (alist-get 'run_id run)
                               (if chosen (string-join chosen ", ") "all failed tasks")))
      (dbx-api "post" "/api/2.2/jobs/runs/repair"
               :body `((run_id . ,(alist-get 'run_id run))
                       ,@(if chosen `((rerun_tasks . ,(vconcat chosen)))
                           '((rerun_all_failed_tasks . t))))
               :callback (lambda (_)
                           (message "dbx: repair started")
                           (when dbx-watch-started-runs
                             (dbx-watch-run (alist-get 'run_id run) (dbx-context)))
                           (when (buffer-live-p buf)
                             (dbx--in-buffer buf (dbx-page-refresh))))))))

(defun dbx-run-rerun ()
  "Run the job of this run again with the same job parameters."
  (interactive)
  (let ((run dbx-run--run))
    (dbx-job-run-now (alist-get 'job_id run)
                     (mapcar (lambda (p) (cons (intern (alist-get 'name p))
                                               (or (alist-get 'value p) (alist-get 'default p) "")))
                             (alist-get 'job_parameters run)))))

(defun dbx-run-task-output ()
  "Show the output of the task at point."
  (interactive)
  (let ((open (get-text-property (point) 'dbx-open)))
    (if open (funcall open) (message "No task at point"))))

(defun dbx-run-show-output (task-run-id key)
  "Show output and errors of task run TASK-RUN-ID (task KEY)."
  (if (not task-run-id) (message "Task %s has not run" key)
    (dbx-api "get" "/api/2.2/jobs/runs/get-output" :params `((run_id . ,task-run-id))
             :callback
             (lambda (out)
               (with-current-buffer (get-buffer-create (format "*dbx output: %s*" key))
                 (let ((inhibit-read-only t))
                   (erase-buffer)
                   (insert (propertize (format "Task %s · run %s\n\n" key task-run-id) 'face 'dbx-title))
                   (when-let* ((e (alist-get 'error out)))
                     (insert (propertize "Error\n" 'face 'dbx-heading) e "\n\n"))
                   (when-let* ((tr (alist-get 'error_trace out)))
                     (insert (propertize "Trace\n" 'face 'dbx-heading) tr "\n\n"))
                   (when-let* ((r (dbx-get out 'notebook_output 'result)))
                     (insert (propertize "Notebook result\n" 'face 'dbx-heading) r "\n\n"))
                   (when-let* ((l (alist-get 'logs out)))
                     (insert (propertize "Logs" 'face 'dbx-heading)
                             (if (dbx-truthy (alist-get 'logs_truncated out)) " (truncated)" "")
                             "\n" l "\n"))
                   (when (= (point) (point-min)) (insert "No output.\n"))
                   (goto-char (point-min))
                   (special-mode))
                 (pop-to-buffer (current-buffer)))))))

;;;; Runs list

(defvar-local dbx-runs--active-only t)
(defvar-local dbx-runs--job-id nil)
(defvar-local dbx-runs--runs nil "Runs shown in the list.")

(defun dbx-runs--run-at-point ()
  (when-let* ((id (dbx-list-row-id)))
    (cl-find id dbx-runs--runs :key (lambda (r) (alist-get 'run_id r)) :test #'equal)))

(define-derived-mode dbx-runs-list-mode dbx-list-mode "DBX-Runs"
  "List of job runs."
  (setq tabulated-list-format [("" 2 nil) ("Job" 34 t) ("Run ID" 18 t) ("Trigger" 10 t)
                               ("Start" 14 t) ("Duration" 11 nil :right-align t) ("Status" 12 t)])
  (setq-local dbx-list-open-function #'dbx-run-page)
  (setq-local dbx-page-refresh-function #'dbx-runs--refresh)
  (setq-local dbx-page-watch-function
              (lambda () (dbx-watch-run (dbx-list-row-id) (dbx-list-row-context))))
  (setq-local dbx-page-actions '(("RET" "Open run" dbx-page-open)
                                 ("a" "Toggle active only" dbx-runs-toggle-active)
                                 ("f" "Watch" dbx-page-watch)))
  (keymap-set dbx-runs-list-mode-map "a" #'dbx-runs-toggle-active)
  (tabulated-list-init-header))

;;;###autoload
(defun dbx-runs (&optional all job-id context)
  "List active runs; with prefix ALL, recent runs.  JOB-ID filters.
CONTEXT (a name or plist) defaults to the active context."
  (interactive "P")
  (let ((buf (dbx-page-buffer "runs" job-id #'dbx-runs-list-mode context)))
    (dbx--in-buffer buf
      (setq dbx-runs--active-only (not all) dbx-runs--job-id job-id)
      (dbx-runs--refresh))
    (pop-to-buffer buf)))

(defun dbx-job-runs-list ()
  "List all recent runs of this job."
  (interactive)
  (dbx-runs t (dbx--job-id-at-point)))

(defun dbx-runs-toggle-active ()
  "Toggle between active and recent runs."
  (interactive)
  (setq dbx-runs--active-only (not dbx-runs--active-only))
  (dbx-runs--refresh))

(defun dbx-runs--refresh ()
  (let ((buf (current-buffer)))
    (setq mode-name (if dbx-runs--active-only "DBX-Runs[active]" "DBX-Runs"))
    (dbx-api-pages "/api/2.2/jobs/runs/list" :items-key 'runs :max-items 100
                   :params `((limit . 25)
                             ,@(and dbx-runs--active-only '((active_only . "true")))
                             ,@(and dbx-runs--job-id `((job_id . ,dbx-runs--job-id))))
                   :error-callback (lambda (m) (message "dbx: %s" m))
                   :callback
                   (lambda (runs)
                     (when (buffer-live-p buf)
                       (dbx--in-buffer buf
                         (setq dbx-runs--runs runs)
                         (setq dbx-page-active-p (cl-some #'dbx-run-active-p runs))
                         (dbx-list-set-entries
                          (mapcar (lambda (r)
                                    (list (alist-get 'run_id r)
                                          (vector (dbx-status-glyph (dbx-run-status r))
                                                  (or (alist-get 'run_name r) "")
                                                  (format "%s" (alist-get 'run_id r))
                                                  (or (alist-get 'trigger r) "")
                                                  (dbx-format-time (alist-get 'start_time r))
                                                  (dbx-format-duration (dbx-run-duration r))
                                                  (dbx-run-status-label r))))
                                  runs))
                         (dbx-page-schedule-refresh)))))))

;;;; Jobs list

(defvar-local dbx-jobs--jobs nil "Jobs shown in the list.")

(define-derived-mode dbx-jobs-list-mode dbx-list-mode "DBX-Jobs"
  "List of jobs."
  (setq tabulated-list-format [("Last runs" 11 nil) ("Name" 36 t) ("Bundle" 16 t)
                               ("Schedule" 10 t) ("Creator" 26 t) ("Last run" 10 nil)])
  (setq tabulated-list-sort-key '("Name"))
  (setq-local dbx-list-open-function #'dbx-job-page)
  (setq-local dbx-list-url-function (lambda (id) (dbx-url (format "/jobs/%s" id))))
  (setq-local dbx-page-refresh-function #'dbx-jobs--refresh)
  (setq-local dbx-page-actions '(("RET" "Open job" dbx-page-open)
                                 ("r" "Run now" dbx-job-run-now)
                                 ("l" "Runs" dbx-job-runs-list)
                                 ("/" "Filter" dbx-list-filter)))
  (keymap-set dbx-jobs-list-mode-map "r" #'dbx-job-run-now)
  (keymap-set dbx-jobs-list-mode-map "l" #'dbx-job-runs-list)
  (tabulated-list-init-header))

;;;###autoload
(defun dbx-jobs (&optional context)
  "List jobs in CONTEXT (default: the active context)."
  (interactive)
  (let ((buf (dbx-page-buffer "jobs" nil #'dbx-jobs-list-mode context)))
    (dbx--in-buffer buf (dbx-jobs--refresh t))
    (pop-to-buffer buf)))

(defun dbx-jobs--refresh (&optional cached)
  (let ((buf (current-buffer)))
    (dbx-jobs-fetch
     (lambda (jobs)
       (dbx-recent-runs-fetch
        (lambda (runs)
          (when (buffer-live-p buf)
            (dbx--in-buffer buf
              (setq dbx-jobs--jobs jobs)
              (dbx-list-set-entries
               (mapcar (lambda (c)
                         (let* ((job (cadr c)) (rs (last (cddr c) 10))
                                (b (dbx-job-bundle job)))
                           (list (alist-get 'job_id job)
                                 (vector (dbx-runs-glyph-strip rs)
                                         (car c)
                                         (if b (format "%s/%s" (car b) (or (cdr b) "")) "")
                                         (dbx-job-schedule-short job)
                                         (dbx-principal-name (alist-get 'creator_user_name job))
                                         (if rs (dbx-format-ago (alist-get 'start_time (car (last rs)))) "")))))
                       (dbx--jobs-candidates jobs runs))))))
        (lambda (m) (message "dbx: %s" m)) (not cached)))
     (lambda (m) (message "dbx: %s" m))
     (not cached))))

;;;; Watching

(defvar dbx--watched nil
  "Runs being watched: list of (RUN-ID CONTEXT . NAME).
CONTEXT is a context name (or plist, for ad-hoc contexts).")
(defvar dbx--watch-timer nil)

(defun dbx--watch-find (run-id context)
  (let ((cname (dbx-context-name context)))
    (cl-find-if (lambda (w) (and (equal (format "%s" (car w)) (format "%s" run-id))
                                 (equal (dbx-context-name (cadr w)) cname)))
                dbx--watched)))

(defun dbx-watch-run (run-id &optional context)
  "Watch RUN-ID in CONTEXT and notify when it reaches a terminal state.
CONTEXT (a name or plist) defaults to the active context.  When the
run ends, `dbx-run-finished-functions' run."
  (interactive (list (read-string "Run ID: ")))
  (let ((ctx (if context (dbx--resolve-context context) (dbx-context))))
    (unless (dbx--watch-find run-id ctx)
      (push (cons run-id (cons (dbx--context-ref ctx) nil)) dbx--watched)
      (message "dbx: watching run %s [%s]" run-id (dbx-context-name ctx))))
  (unless (timerp dbx--watch-timer)
    (setq dbx--watch-timer (run-at-time dbx-watch-interval dbx-watch-interval #'dbx--watch-poll)))
  (force-mode-line-update t))

(defun dbx--watch-poll ()
  (if (null dbx--watched)
      (progn (cancel-timer dbx--watch-timer) (setq dbx--watch-timer nil))
    (dolist (w dbx--watched)
      (let ((rid (car w)) (ctx (cadr w)))
        (dbx-api "get" "/api/2.2/jobs/runs/get" :params `((run_id . ,rid))
                 :context ctx
                 :error-callback #'ignore
                 :callback
                 (lambda (run)
                   (when (and (memq w dbx--watched) (not (dbx-run-active-p run)))
                     (setq dbx--watched (delq w dbx--watched))
                     (force-mode-line-update t)
                     (dbx-notify (format "%s %s" (or (alist-get 'run_name run) "Run")
                                         (dbx-run-status-label run))
                                 (format "Run %s finished in %s" rid
                                         (dbx-format-duration (dbx-run-duration run))))
                     (run-hook-with-args 'dbx-run-finished-functions
                                         run (dbx-context-name ctx)))))))))

(defun dbx-notify (title body)
  "Show a desktop notification with TITLE and BODY, or a message."
  (if (and (require 'notifications nil t) (fboundp 'notifications-notify)
           (ignore-errors (notifications-notify :title title :body body :app-name "Emacs dbx")))
      nil
    (message "dbx: %s — %s" title body)))

(defun dbx-watched-runs ()
  "Pick a watched run and open it."
  (interactive)
  (if (null dbx--watched) (message "dbx: no watched runs")
    (let* ((cands (mapcar (lambda (w) (cons (format "%s [%s]" (car w) (dbx-context-name (cadr w))) w))
                          dbx--watched))
           (w (cdr (assoc (completing-read "Watched run: " cands nil t) cands))))
      (dbx-run-page (car w) (cadr w)))))

(defun dbx--watch-mode-line ()
  (when dbx--watched
    (propertize (format "%s%d watched" (dbx-glyph 'running) (length dbx--watched))
                'face 'dbx-running)))

(add-hook 'dbx-mode-line-extra-functions #'dbx--watch-mode-line)

(provide 'dbx-jobs)
;;; dbx-jobs.el ends here
