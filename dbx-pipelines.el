;;; dbx-pipelines.el --- Lakeflow Declarative Pipelines  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; Pipelines API 2.0 through `databricks api'.  The dataset graph comes
;; from the event log: `flow_definition' events give the edges
;; (input_datasets -> output_dataset), `dataset_definition' the node
;; types, `flow_progress' the state, rows and expectations.

;;; Code:

(require 'dbx-util)
(require 'dbx-cli)
(require 'dbx-page)
(require 'dbx-graph)

(defcustom dbx-pipeline-max-events 1000
  "Maximum number of events fetched for a pipeline page or graph."
  :type 'integer :group 'dbx)

;;;; API

(defun dbx-pipelines-fetch (callback &optional error-callback force)
  "Fetch pipelines (cached 60 s)."
  (dbx-cached 'pipelines 60
              (lambda (k)
                (dbx-api-pages "/api/2.0/pipelines" :items-key 'statuses
                               :params '((max_results . 100)) :max-items 1000
                               :callback k :error-callback error-callback))
              callback force))

(defun dbx-pipeline-events (id callback &optional error-callback)
  "Fetch events of pipeline ID for its latest update (newest first)."
  (let ((acc nil) (latest nil))
    (cl-labels
        ((fetch (token)
           (dbx-api "get" (format "/api/2.0/pipelines/%s/events" id)
                    :params `((max_results . 250) (order_by . "timestamp desc")
                              ,@(and token `((page_token . ,token))))
                    :error-callback error-callback
                    :callback
                    (lambda (r)
                      (let* ((evs (alist-get 'events r))
                             (older nil))
                        (dolist (e evs)
                          (let ((u (dbx-get e 'origin 'update_id)))
                            (when (and u (not latest)) (setq latest u))
                            (if (and latest u (not (equal u latest)))
                                (setq older t)
                              (push e acc))))
                        (let ((next (alist-get 'next_page_token r)))
                          (if (and next (not older) (< (length acc) dbx-pipeline-max-events))
                              (fetch next)
                            (funcall callback (nreverse acc) latest))))))))
      (fetch nil))))

;;;; Event analysis

(defun dbx--dataset-name (d)
  (cond ((stringp d) d)
        ((listp d) (or (alist-get 'name d)
                       (string-join (delq nil (list (alist-get 'catalog d) (alist-get 'schema d)
                                                    (alist-get 'table d)))
                                    ".")))
        (t (format "%s" d))))

(defun dbx-pipeline-analyze (events)
  "Summarize EVENTS (newest first) into a plist.
Keys: :flows (FLOW -> (:output :inputs :explain)), :datasets
(NAME -> (:type :status :rows :expect-failed :expect-total))."
  (let ((flows (make-hash-table :test #'equal))
        (datasets (make-hash-table :test #'equal)))
    (cl-flet ((ds (name)
                (or (gethash name datasets)
                    ;; events mix qualified (main.s.t) and short (t) names
                    (let ((short (dbx--short-dataset name)) hit)
                      (maphash (lambda (k v) (when (and (not hit) (equal (dbx--short-dataset k) short))
                                               (setq hit v)))
                               datasets)
                      (when (and hit (string-search "." name)
                                 (not (string-search "." (plist-get hit :name))))
                        ;; upgrade a short entry to the qualified name
                        (remhash (plist-get hit :name) datasets)
                        (plist-put hit :name name)
                        (puthash name hit datasets))
                      hit)
                    (puthash name (list :name name) datasets))))
      ;; oldest first so later events win
      (dolist (e (reverse events))
        (let ((type (alist-get 'event_type e))
              (flow (dbx-get e 'origin 'flow_name))
              (dname (dbx-get e 'origin 'dataset_name)))
          (pcase type
            ("flow_definition"
             (let* ((fd (dbx-get e 'details 'flow_definition))
                    (out (dbx--dataset-name (or (alist-get 'output_dataset fd) dname flow)))
                    (ins (mapcar #'dbx--dataset-name (alist-get 'input_datasets fd))))
               (puthash (or flow out) (list :output out :inputs ins
                                            :explain (alist-get 'explain_text fd)
                                            :sink (alist-get 'output_sink fd))
                        flows)
               (ds out)
               (dolist (i ins) (ds i))))
            ("dataset_definition"
             (let ((d (ds (or dname flow))))
               (plist-put d :type (dbx-get e 'details 'dataset_definition 'dataset_type))))
            ("flow_progress"
             (let* ((fp (dbx-get e 'details 'flow_progress))
                    (f (gethash flow flows))
                    (d (ds (or (plist-get f :output) dname flow)))
                    (exps (dbx-get fp 'data_quality 'expectations)))
               (plist-put d :status (alist-get 'status fp))
               (when-let* ((rows (dbx-get fp 'metrics 'num_output_rows)))
                 (plist-put d :rows rows))
               (when exps
                 (plist-put d :expect-failed
                            (cl-loop for x in exps sum (or (alist-get 'failed_records x) 0)))
                 (plist-put d :expect-total
                            (cl-loop for x in exps sum (+ (or (alist-get 'failed_records x) 0)
                                                          (or (alist-get 'passed_records x) 0))))
                 (plist-put d :expectations exps))))))))
    (list :flows flows :datasets datasets)))

(defun dbx--short-dataset (name)
  (car (last (split-string (replace-regexp-in-string "`" "" name) "\\."))))

(defun dbx--dataset-type-short (type)
  (pcase type
    ("MATERIALIZED_VIEW" "MV") ("STREAMING_TABLE" "ST") ("VIEW" "VIEW")
    ('nil "") (_ type)))

(defun dbx--format-count (n)
  (cond ((not (numberp n)) "")
        ((>= n 1000000) (format "%.1fM" (/ n 1e6)))
        ((>= n 1000) (format "%.1fk" (/ n 1e3)))
        (t (format "%d" n))))

(defun dbx-pipeline-graph-from-events (title events &optional open-table)
  "Graph of datasets from pipeline EVENTS.  OPEN-TABLE is called with a name."
  (let* ((a (dbx-pipeline-analyze events))
         (flows (plist-get a :flows)) (datasets (plist-get a :datasets))
         nodes edges)
    (maphash (lambda (_key d)
               (push (list :id (plist-get d :name)
                           :label (concat (dbx--short-dataset (plist-get d :name))
                                          (let ((ty (dbx--dataset-type-short (plist-get d :type))))
                                            (if (string-empty-p ty) "" (format " (%s)" ty))))
                           :status (if (plist-get d :status)
                                       (dbx-pipeline-status (plist-get d :status))
                                     'absent)
                           :kind 'table
                           :info (string-join
                                  (delq nil (list (and (plist-get d :rows)
                                                       (concat (dbx--format-count (plist-get d :rows)) " rows"))
                                                  (and (plist-get d :expect-failed)
                                                       (> (plist-get d :expect-failed) 0)
                                                       (format "%s %s failed expectations"
                                                               (dbx-glyph 'warning)
                                                               (dbx--format-count (plist-get d :expect-failed))))))
                                  " · ")
                           :action (and open-table (let ((name (plist-get d :name)))
                                                     (lambda () (funcall open-table name)))))
                     nodes))
             datasets)
    (maphash (lambda (_flow f)
               (dolist (i (plist-get f :inputs))
                 (push (list :from i :to (plist-get f :output)) edges)))
             flows)
    (list :title title :nodes (sort nodes :key (lambda (n) (plist-get n :id)))
          :edges (delete-dups edges))))

;;;; Pickers and list

;;;###autoload
(defun dbx-find-pipeline (&optional force)
  "Fuzzy-find a pipeline and open its page."
  (interactive "P")
  (let* ((ps (dbx-await (lambda (k e) (dbx-pipelines-fetch k e force)) "pipelines"))
         (cands (mapcar (lambda (p)
                          (cons (concat (dbx-status-glyph
                                         (dbx-pipeline-status (dbx-get p 'latest_updates 0 'state)))
                                        " " (alist-get 'name p))
                                p))
                        ps))
         (pick (completing-read
                "Pipeline: "
                (lambda (str pred action)
                  (if (eq action 'metadata)
                      `(metadata (category . dbx-pipeline)
                                 (annotation-function
                                  . ,(lambda (c)
                                       (let ((p (cdr (assoc c cands))))
                                         (concat (propertize " " 'display '(space :align-to 40))
                                                 (dbx-pad (or (alist-get 'state p) "") 10)
                                                 (dbx-pad (or (dbx-get p 'latest_updates 0 'state) "") 12)
                                                 (propertize (or (alist-get 'creator_user_name p) "")
                                                             'face 'dbx-dim))))))
                    (complete-with-action action cands str pred)))
                nil t)))
    (dbx-pipeline-page (alist-get 'pipeline_id (cdr (assoc pick cands))))))

(define-derived-mode dbx-pipelines-list-mode dbx-list-mode "DBX-Pipelines"
  "List of pipelines."
  (setq tabulated-list-format [("Last" 12 t) ("Name" 40 t) ("State" 10 t) ("Creator" 30 t)])
  (setq tabulated-list-sort-key '("Name"))
  (setq-local dbx-list-open-function #'dbx-pipeline-page)
  (setq-local dbx-list-url-function (lambda (id) (dbx-url (format "/pipelines/%s" id))))
  (setq-local dbx-page-refresh-function #'dbx-pipelines--refresh)
  (tabulated-list-init-header))

;;;###autoload
(defun dbx-pipelines (&optional context)
  "List pipelines in CONTEXT (default: the active context)."
  (interactive)
  (let ((buf (dbx-page-buffer "pipelines" nil #'dbx-pipelines-list-mode context)))
    (dbx--in-buffer buf (dbx-pipelines--refresh))
    (pop-to-buffer buf)))

(defun dbx-pipelines--refresh ()
  (let ((buf (current-buffer)))
    (dbx-pipelines-fetch
     (lambda (ps)
       (when (buffer-live-p buf)
         (dbx--in-buffer buf
           (dbx-list-set-entries
            (mapcar (lambda (p)
                      (let ((u (dbx-get p 'latest_updates 0 'state)))
                        (list (alist-get 'pipeline_id p)
                              (vector (concat (dbx-status-glyph (dbx-pipeline-status u)) " " (or u ""))
                                      (alist-get 'name p) (or (alist-get 'state p) "")
                                      (or (alist-get 'creator_user_name p) "")))))
                    ps)))))
     (lambda (m) (message "dbx: %s" m)) t)))

;;;; Page

(defvar-local dbx-pipeline--id nil)
(defvar-local dbx-pipeline--data nil)
(defvar-local dbx-pipeline--events nil)

(defvar-keymap dbx-pipeline-page-mode-map
  :parent dbx-page-mode-map
  "s" #'dbx-pipeline-start
  "F" #'dbx-pipeline-full-refresh
  "T" #'dbx-pipeline-refresh-tables
  "v" #'dbx-pipeline-validate
  "S" #'dbx-pipeline-stop
  "H" #'dbx-pipeline-history
  "G" #'dbx-pipeline-graph)

(define-derived-mode dbx-pipeline-page-mode dbx-page-mode "DBX-Pipeline"
  "Mode for a pipeline page.

\\{dbx-pipeline-page-mode-map}"
  (setq-local dbx-page-actions
              '(("s" "Start update" dbx-pipeline-start)
                ("F" "Full refresh" dbx-pipeline-full-refresh)
                ("T" "Refresh selected tables" dbx-pipeline-refresh-tables)
                ("v" "Validate only" dbx-pipeline-validate)
                ("S" "Stop" dbx-pipeline-stop)
                ("H" "Update history" dbx-pipeline-history)
                ("G" "Dataset graph" dbx-pipeline-graph)
                ("o" "Open in browser" dbx-page-browse))))

;;;###autoload
(defun dbx-pipeline-page (id &optional context no-select)
  "Open the page of pipeline ID in CONTEXT and return its buffer.
CONTEXT (a name or plist) defaults to the active context.  With
NO-SELECT, prepare the page without displaying it.  Interactively,
read a pipeline ID or URL."
  (interactive
   (let ((ref (dbx-read-object-ref "Pipeline ID or URL" '(pipeline) 'pipeline)))
     (list (plist-get ref :id) (plist-get ref :context))))
  (let ((buf (dbx-page-buffer "pipeline" id #'dbx-pipeline-page-mode context)))
    (dbx--in-buffer buf
      (setq dbx-pipeline--id id)
      (setq-local dbx-page-refresh-function #'dbx-pipeline--refresh)
      (dbx-page-loading buf "pipeline")
      (dbx-pipeline--refresh))
    (dbx-page-display buf no-select)))

(defun dbx-pipeline--refresh ()
  (let ((buf (current-buffer)) (id dbx-pipeline--id))
    (dbx-api "get" (format "/api/2.0/pipelines/%s" id)
             :error-callback (lambda (m) (dbx-page-error buf m))
             :callback
             (lambda (p)
               (dbx-pipeline-events
                id
                (lambda (events _latest)
                  (when (buffer-live-p buf)
                    (dbx--in-buffer buf
                      (setq dbx-pipeline--data p dbx-pipeline--events events)
                      (dbx-page-rename (alist-get 'name p))
                      (setq dbx-page-active-p
                            (eq (dbx-pipeline-status (dbx-get p 'latest_updates 0 'state)) 'running))
                      (dbx-pipeline--render)
                      (dbx-page-schedule-refresh))))
                (lambda (m) (dbx-page-error buf m)))))))

(defun dbx-pipeline--render ()
  (let* ((p dbx-pipeline--data) (spec (alist-get 'spec p))
         (upd (dbx-get p 'latest_updates 0))
         (a (dbx-pipeline-analyze dbx-pipeline--events))
         (datasets (let (l) (maphash (lambda (_ d) (push d l)) (plist-get a :datasets))
                        (sort l :key (lambda (d) (plist-get d :name)))))
         (url (dbx-url (format "/pipelines/%s" (alist-get 'pipeline_id p)))))
    (dbx-page-with-render
      (insert " " (propertize (alist-get 'name p) 'face 'dbx-title) "  "
              (dbx-status-glyph (dbx-pipeline-status (alist-get 'state upd))) " "
              (format "update %s" (or (alist-get 'state upd) "—")) "   ")
      (dbx-page-button "Start" #'dbx-pipeline-start)
      (dbx-page-button "Full refresh" #'dbx-pipeline-full-refresh)
      (dbx-page-button "Stop" #'dbx-pipeline-stop)
      (dbx-page-button "Graph" #'dbx-pipeline-graph)
      (insert "\n")
      (put-text-property (point-min) (point) 'dbx-url url)
      (put-text-property (point-min) (point) 'dbx-id (alist-get 'pipeline_id p))
      (dbx-page-kv "Target" (string-join (delq nil (list (alist-get 'catalog spec)
                                                         (or (alist-get 'schema spec) (alist-get 'target spec))))
                                         ".")
                   nil (if (dbx-truthy (alist-get 'serverless spec)) "serverless" "classic compute")
                   nil (if (dbx-truthy (alist-get 'continuous spec)) "continuous" "triggered")
                   nil (and (dbx-truthy (alist-get 'development spec)) "development"))
      (dbx-page-kv "State" (alist-get 'state p) "Creator" (alist-get 'creator_user_name p)
                   "Last update" (and upd (format "%s · %s" (alist-get 'update_id upd)
                                                  (or (alist-get 'creation_time upd) ""))))
      (insert "\n")
      (dbx-page-heading (format "Datasets (%d)" (length datasets)) "latest update")
      (let ((mx (apply #'max 1 (delq nil (mapcar (lambda (d) (plist-get d :rows)) datasets)))))
        (dolist (d datasets)
          (let ((s (point)) (name (plist-get d :name)))
            (insert "  " (dbx-status-glyph (if (plist-get d :status)
                                                (dbx-pipeline-status (plist-get d :status))
                                              'absent))
                    " " (dbx-pad (dbx--short-dataset name) 32) " "
                    (dbx-pad (dbx--dataset-type-short (plist-get d :type)) 5) " "
                    (dbx-pad (dbx--format-count (plist-get d :rows)) 8 t) " "
                    (dbx-pad (dbx-hbar (plist-get d :rows) mx 10) 10) " "
                    (propertize (or (plist-get d :status) "source") 'face 'dbx-dim)
                    (if (and (plist-get d :expect-failed) (> (plist-get d :expect-failed) 0))
                        (propertize (format "  %s %s failed expectations" (dbx-glyph 'warning)
                                            (dbx--format-count (plist-get d :expect-failed)))
                                    'face 'dbx-unknown)
                      "")
                    "\n")
            (dbx-page-item s (1- (point)) 'dbx-id name
                           'dbx-open (lambda () (dbx-pipeline--show-dataset name a))))))
      (insert "\n")
      (dbx-page-heading "Events" "latest update, newest first")
      (dolist (e (seq-take dbx-pipeline--events 60))
        (let ((lvl (alist-get 'level e)))
          (insert "  " (dbx-pad (let ((ts (alist-get 'timestamp e)))
                                  (if (and ts (string-match "T\\([0-9:]+\\)" ts)) (match-string 1 ts) ""))
                                8)
                  " " (propertize (dbx-pad lvl 7) 'face (pcase lvl ("ERROR" 'dbx-failed)
                                                          ("WARN" 'dbx-unknown) (_ 'dbx-dim)))
                  " " (dbx-pad (alist-get 'event_type e) 18)
                  " " (dbx-pad (or (dbx-get e 'origin 'flow_name) "") 22)
                  " " (replace-regexp-in-string "\n" " " (or (alist-get 'message e) "")) "\n")))
      (insert "\n"))))

(defun dbx-pipeline--show-dataset (name analysis)
  "Show details of dataset NAME from ANALYSIS."
  (let* ((d (gethash name (plist-get analysis :datasets)))
         (flow (cl-loop for f being the hash-values of (plist-get analysis :flows)
                        when (equal (plist-get f :output) name) return f)))
    (with-current-buffer (get-buffer-create (format "*dbx dataset: %s*" (dbx--short-dataset name)))
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize name 'face 'dbx-title) "\n\n"
                (format "Type: %s\nStatus: %s\nRows written: %s\n"
                        (or (plist-get d :type) "—") (or (plist-get d :status) "—")
                        (or (plist-get d :rows) "—")))
        (when flow
          (insert (format "Inputs: %s\n" (string-join (plist-get flow :inputs) ", "))))
        (dolist (x (plist-get d :expectations))
          (insert (format "Expectation %s: %s passed, %s failed\n" (alist-get 'name x)
                          (or (alist-get 'passed_records x) 0) (or (alist-get 'failed_records x) 0))))
        (when (plist-get flow :explain)
          (insert "\nPlan:\n" (format "%s" (plist-get flow :explain)) "\n"))
        (goto-char (point-min))
        (special-mode))
      (pop-to-buffer (current-buffer)))))

(defun dbx-pipeline-graph ()
  "Show the dataset graph of the pipeline's latest update."
  (interactive)
  (let ((events dbx-pipeline--events) (name (alist-get 'name dbx-pipeline--data))
        (a (dbx-pipeline-analyze dbx-pipeline--events)))
    (if (zerop (hash-table-count (plist-get a :flows)))
        (message "dbx: no flow definitions in the latest update; run `v' (validate) to produce them")
      (dbx-graph-show (dbx-pipeline-graph-from-events
                       (format "Pipeline %s" name) events
                       (lambda (n) (dbx-pipeline--show-dataset n a)))))))

;;;; Actions

(defun dbx--pipeline-id-here ()
  (or dbx-pipeline--id
      (and (derived-mode-p 'tabulated-list-mode) (tabulated-list-get-id))
      (user-error "No pipeline here")))

(defun dbx--pipeline-update (body what &optional confirm)
  (let ((id (dbx--pipeline-id-here)))
    (when (or (not (or confirm (dbx-context-get :confirm)))
              (dbx-confirm (format "%s pipeline %s?" what id)))
      (dbx-api "post" (format "/api/2.0/pipelines/%s/updates" id) :body body
               :callback (lambda (r) (message "dbx: %s started (update %s)" what (alist-get 'update_id r))
                           (when (derived-mode-p 'dbx-page-mode) (dbx-page-refresh)))))))

(defun dbx-pipeline-start () "Start an update." (interactive)
       (dbx--pipeline-update '() "Start"))
(defun dbx-pipeline-full-refresh () "Start a full refresh (resets tables)." (interactive)
       (dbx--pipeline-update '((full_refresh . t)) "FULL REFRESH" t))
(defun dbx-pipeline-validate () "Validate without materializing." (interactive)
       (dbx--pipeline-update '((validate_only . t)) "Validate"))

(defun dbx-pipeline-refresh-tables ()
  "Refresh chosen tables."
  (interactive)
  (let* ((a (dbx-pipeline-analyze dbx-pipeline--events))
         (names (let (l) (maphash (lambda (_ f) (push (plist-get f :output) l)) (plist-get a :flows)) l))
         (chosen (completing-read-multiple "Tables to refresh: " names nil t)))
    (when chosen
      (dbx--pipeline-update `((refresh_selection . ,(vconcat chosen)))
                            (format "Refresh %s in" (string-join chosen ", "))))))

(defun dbx-pipeline-stop ()
  "Stop the running update."
  (interactive)
  (let ((id (dbx--pipeline-id-here)))
    (when (dbx-confirm (format "Stop pipeline %s?" id))
      (dbx-api "post" (format "/api/2.0/pipelines/%s/stop" id) :body '()
               :callback (lambda (_) (message "dbx: stop requested"))))))

(defun dbx-pipeline-history ()
  "Show recent updates of the pipeline."
  (interactive)
  (let ((id (dbx--pipeline-id-here)))
    (dbx-api "get" (format "/api/2.0/pipelines/%s/updates" id) :params '((max_results . 25))
             :callback
             (lambda (r)
               (with-current-buffer (get-buffer-create (format "*dbx pipeline history %s*" id))
                 (let ((inhibit-read-only t))
                   (erase-buffer)
                   (insert (propertize "Updates\n\n" 'face 'dbx-title))
                   (dolist (u (alist-get 'updates r))
                     (insert "  " (dbx-status-glyph (dbx-pipeline-status (alist-get 'state u))) " "
                             (dbx-pad (alist-get 'state u) 12) " "
                             (dbx-pad (dbx-format-time (alist-get 'creation_time u)) 14) " "
                             (dbx-pad (or (alist-get 'cause u) "") 16) " "
                             (if (dbx-truthy (alist-get 'full_refresh u)) "full refresh " "")
                             (if (dbx-truthy (alist-get 'validate_only u)) "validate only " "")
                             (propertize (or (alist-get 'update_id u) "") 'face 'dbx-dim) "\n"))
                   (goto-char (point-min))
                   (special-mode))
                 (pop-to-buffer (current-buffer)))))))

(provide 'dbx-pipelines)
;;; dbx-pipelines.el ends here
