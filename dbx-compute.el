;;; dbx-compute.el --- Clusters and SQL warehouses  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; Clusters API 2.1 and SQL Warehouses API 2.0 through `databricks api':
;; pickers, lists, pages and start/stop actions.

;;; Code:

(require 'dbx-util)
(require 'dbx-cli)
(require 'dbx-page)

(defvar dbx-page-actions)

(defcustom dbx-clusters-include-job-clusters nil
  "Non-nil to list job clusters too (usually many and short-lived)."
  :type 'boolean :group 'dbx)

;;;; Clusters

(defun dbx-clusters-fetch (callback &optional error-callback force)
  "Fetch clusters (cached 30 s) and call CALLBACK."
  (dbx-cached 'clusters 30
              (lambda (k)
                (dbx-api-pages "/api/2.1/clusters/list" :items-key 'clusters
                               :params '((page_size . 100)) :max-items 500
                               :error-callback error-callback
                               :callback
                               (lambda (cs)
                                 (funcall k (if dbx-clusters-include-job-clusters cs
                                              (cl-remove-if (lambda (c) (equal (alist-get 'cluster_source c) "JOB"))
                                                            cs))))))
              callback force))

(defun dbx--cluster-workers (c)
  (let ((auto (alist-get 'autoscale c)))
    (cons (or (and (alist-get 'executors c) (length (alist-get 'executors c)))
              (alist-get 'num_workers c) 0)
          (if auto (alist-get 'max_workers auto) (or (alist-get 'num_workers c) 0)))))

(defun dbx--cluster-dbr (c)
  (let ((v (or (alist-get 'spark_version c) "")))
    (if (string-match "\\`\\([0-9]+\\.[0-9]+\\)" v) (match-string 1 v) v)))

(defun dbx--cluster-annotation (cands)
  (lambda (cand)
    (let* ((c (cdr (assoc cand cands))) (w (dbx--cluster-workers c)))
      (concat (propertize " " 'display '(space :align-to 34))
              (dbx-pad (dbx--cluster-dbr c) 6) " "
              (dbx-pad (or (alist-get 'data_security_mode c) "") 14) " "
              (dbx-pad (if (equal (alist-get 'state c) "RUNNING")
                           (format "%d/%d workers" (car w) (cdr w))
                         (downcase (or (alist-get 'state c) "")))
                       14)
              "  " (propertize (or (alist-get 'creator_user_name c) "") 'face 'dbx-dim)))))

(defun dbx--read-cluster (prompt &optional force)
  (let* ((clusters (dbx-await (lambda (k e) (dbx-clusters-fetch k e force)) "clusters"))
         (cands (mapcar (lambda (c)
                          (cons (concat (dbx-status-glyph (dbx-cluster-status (alist-get 'state c)))
                                        " " (alist-get 'cluster_name c))
                                c))
                        clusters))
         (pick (completing-read
                prompt
                (lambda (str pred action)
                  (if (eq action 'metadata)
                      `(metadata (category . dbx-cluster)
                                 (annotation-function . ,(dbx--cluster-annotation cands)))
                    (complete-with-action action cands str pred)))
                nil t)))
    (cdr (assoc pick cands))))

;;;###autoload
(defun dbx-find-cluster (&optional force)
  "Fuzzy-find a cluster and open its page."
  (interactive "P")
  (dbx-cluster-page (alist-get 'cluster_id (dbx--read-cluster "Cluster: " force))))

(defvar-local dbx-cluster--id nil)
(defvar-local dbx-cluster--data nil)

(defvar-keymap dbx-cluster-page-mode-map
  :parent dbx-page-mode-map
  "s" #'dbx-cluster-start
  "S" #'dbx-cluster-terminate
  "x" #'dbx-cluster-restart
  "U" #'dbx-cluster-use)

(define-derived-mode dbx-cluster-page-mode dbx-page-mode "DBX-Cluster"
  "Mode for a cluster page.

\\{dbx-cluster-page-mode-map}"
  (setq-local dbx-page-actions
              '(("s" "Start" dbx-cluster-start) ("S" "Terminate" dbx-cluster-terminate)
                ("x" "Restart" dbx-cluster-restart) ("U" "Use as context compute" dbx-cluster-use)
                ("o" "Open in browser" dbx-page-browse))))

;;;###autoload
(defun dbx-cluster-page (cluster-id &optional context no-select)
  "Open the page of CLUSTER-ID in CONTEXT and return its buffer.
CONTEXT (a name or plist) defaults to the active context.  With
NO-SELECT, prepare the page without displaying it."
  (interactive (list (alist-get 'cluster_id (dbx--read-cluster "Cluster: "))))
  (let ((buf (dbx-page-buffer "cluster" cluster-id #'dbx-cluster-page-mode context)))
    (dbx--in-buffer buf
      (setq dbx-cluster--id cluster-id)
      (setq-local dbx-page-refresh-function #'dbx-cluster--refresh)
      (dbx-page-loading buf "cluster")
      (dbx-cluster--refresh))
    (dbx-page-display buf no-select)))

(defun dbx-cluster--refresh ()
  (let ((buf (current-buffer)) (id dbx-cluster--id))
    (dbx-api "get" "/api/2.1/clusters/get" :params `((cluster_id . ,id))
             :error-callback (lambda (m) (dbx-page-error buf m))
             :callback
             (lambda (c)
               (dbx-api "post" "/api/2.1/clusters/events"
                        :body `((cluster_id . ,id) (limit . 20) (order . "DESC"))
                        :error-callback (lambda (_) (dbx-cluster--show buf c nil))
                        :callback (lambda (ev) (dbx-cluster--show buf c (alist-get 'events ev))))))))

(defun dbx-cluster--show (buf c events)
  (when (buffer-live-p buf)
    (dbx--in-buffer buf
      (setq dbx-cluster--data c)
      (dbx-page-rename (alist-get 'cluster_name c))
      (setq dbx-page-active-p (eq (dbx-cluster-status (alist-get 'state c)) 'running))
      (let* ((st (alist-get 'state c)) (w (dbx--cluster-workers c))
             (url (dbx-url (format "/compute/clusters/%s" (alist-get 'cluster_id c)))))
        (dbx-page-with-render
          (insert " " (propertize (alist-get 'cluster_name c) 'face 'dbx-title) "  "
                  (dbx-status-glyph (dbx-cluster-status st)) " " st "   ")
          (dbx-page-button "Start" #'dbx-cluster-start)
          (dbx-page-button "Restart" #'dbx-cluster-restart)
          (dbx-page-button "Terminate" #'dbx-cluster-terminate)
          (dbx-page-button "Use" #'dbx-cluster-use)
          (insert "\n")
          (put-text-property (point-min) (point) 'dbx-url url)
          (put-text-property (point-min) (point) 'dbx-id (alist-get 'cluster_id c))
          (dbx-page-kv "DBR" (alist-get 'spark_version c)
                       "Access" (alist-get 'data_security_mode c)
                       "Owner" (alist-get 'creator_user_name c))
          (dbx-page-kv "Workers" (concat (format "%d/%d " (car w) (cdr w))
                                         (dbx-hbar (car w) (max 1 (cdr w)) 8))
                       "Node" (alist-get 'node_type_id c)
                       "Autoterminate" (let ((m (alist-get 'autotermination_minutes c)))
                                         (if (and m (> m 0)) (format "after %d min idle" m) "off")))
          (when-let* ((msg (alist-get 'state_message c)))
            (unless (string-empty-p msg) (dbx-page-kv "Message" msg)))
          (dbx-page-kv "ID" (alist-get 'cluster_id c) "Source" (alist-get 'cluster_source c))
          (insert "\n")
          (dbx-page-heading "Events" (format "%d latest" (length events)))
          (dolist (e events)
            (insert "  " (dbx-pad (dbx-format-time (alist-get 'timestamp e) "%b %d %H:%M") 13)
                    " " (dbx-pad (alist-get 'type e) 22) " "
                    (propertize (dbx--cluster-event-detail e) 'face 'dbx-dim) "\n"))
          (insert "\n")
          (let ((tags (alist-get 'custom_tags c)))
            (when tags
              (dbx-page-heading "Tags" nil t)
              (dolist (kv tags) (insert (format "  %s = %s\n" (car kv) (cdr kv))))
              (insert "\n")))
          (when-let* ((conf (alist-get 'spark_conf c)))
            (dbx-page-heading "Spark config" nil t)
            (dolist (kv conf) (insert (format "  %s = %s\n" (car kv) (cdr kv))))
            (insert "\n"))))
      (dbx-page-schedule-refresh))))

(defun dbx--cluster-event-detail (e)
  (let ((d (alist-get 'details e)))
    (string-join
     (delq nil (list (and (alist-get 'current_num_workers d)
                          (format "%s → %s workers" (or (alist-get 'previous_num_workers d) "?")
                                  (alist-get 'current_num_workers d)))
                     (and (alist-get 'user d) (format "by %s" (alist-get 'user d)))
                     (dbx-get d 'reason 'code)))
     "  ")))

(defun dbx--cluster-id-here ()
  (or dbx-cluster--id
      (and (derived-mode-p 'tabulated-list-mode) (tabulated-list-get-id))
      (alist-get 'cluster_id (dbx--read-cluster "Cluster: "))))

(defun dbx--cluster-action (verb path &optional confirm)
  (let ((id (dbx--cluster-id-here)))
    (when (or (not confirm) (dbx-confirm (format "%s cluster %s?" verb id)))
      (dbx-api "post" path :body `((cluster_id . ,id))
               :callback (lambda (_) (message "dbx: %s requested" (downcase verb))
                           (dbx-cache-clear)
                           (when (derived-mode-p 'dbx-page-mode 'dbx-list-mode) (dbx-page-refresh)))))))

(defun dbx-cluster-start () "Start the cluster." (interactive)
       (dbx--cluster-action "Start" "/api/2.1/clusters/start"))
(defun dbx-cluster-restart () "Restart the cluster." (interactive)
       (dbx--cluster-action "Restart" "/api/2.1/clusters/restart" t))
(defun dbx-cluster-terminate () "Terminate the cluster." (interactive)
       (dbx--cluster-action "Terminate" "/api/2.1/clusters/delete" t))

(defun dbx-cluster-use ()
  "Use the cluster as the context's compute for this session."
  (interactive)
  (let ((id (dbx--cluster-id-here)))
    (dbx-context-set :cluster id)
    (dbx-context-set :serverless nil)
    (message "dbx: context %s now uses cluster %s" (dbx-context-get :name) id)))

(define-derived-mode dbx-clusters-list-mode dbx-list-mode "DBX-Clusters"
  "List of clusters."
  (setq tabulated-list-format [("State" 14 t) ("Name" 32 t) ("DBR" 6 t) ("Workers" 14 nil)
                               ("Access" 14 t) ("Owner" 26 t)])
  (setq tabulated-list-sort-key '("Name"))
  (setq-local dbx-list-open-function #'dbx-cluster-page)
  (setq-local dbx-list-url-function (lambda (id) (dbx-url (format "/compute/clusters/%s" id))))
  (setq-local dbx-page-refresh-function #'dbx-clusters--refresh)
  (setq-local dbx-page-actions '(("RET" "Open" dbx-page-open) ("s" "Start" dbx-cluster-start)
                                 ("S" "Terminate" dbx-cluster-terminate)
                                 ("U" "Use as compute" dbx-cluster-use)))
  (keymap-set dbx-clusters-list-mode-map "s" #'dbx-cluster-start)
  (keymap-set dbx-clusters-list-mode-map "S" #'dbx-cluster-terminate)
  (keymap-set dbx-clusters-list-mode-map "U" #'dbx-cluster-use)
  (tabulated-list-init-header))

;;;###autoload
(defun dbx-clusters (&optional context)
  "List clusters in CONTEXT (default: the active context)."
  (interactive)
  (let ((buf (dbx-page-buffer "clusters" nil #'dbx-clusters-list-mode context)))
    (dbx--in-buffer buf (dbx-clusters--refresh))
    (pop-to-buffer buf)))

(defun dbx-clusters--refresh ()
  (let ((buf (current-buffer)))
    (dbx-clusters-fetch
     (lambda (cs)
       (when (buffer-live-p buf)
         (dbx--in-buffer buf
           (dbx-list-set-entries
            (mapcar (lambda (c)
                      (let ((w (dbx--cluster-workers c)) (st (alist-get 'state c)))
                        (list (alist-get 'cluster_id c)
                              (vector (concat (dbx-status-glyph (dbx-cluster-status st)) " " st)
                                      (alist-get 'cluster_name c) (dbx--cluster-dbr c)
                                      (if (equal st "RUNNING")
                                          (concat (format "%d/%d " (car w) (cdr w))
                                                  (dbx-hbar (car w) (max 1 (cdr w)) 5))
                                        "")
                                      (or (alist-get 'data_security_mode c) "")
                                      (or (alist-get 'creator_user_name c) "")))))
                    cs)))))
     (lambda (m) (message "dbx: %s" m)) t)))

;;;; Warehouses

(defun dbx-warehouses-fetch (callback &optional error-callback force)
  "Fetch SQL warehouses (cached 30 s) and call CALLBACK."
  (dbx-cached 'warehouses 30
              (lambda (k)
                (dbx-api "get" "/api/2.0/sql/warehouses"
                         :error-callback error-callback
                         :callback (lambda (r) (funcall k (alist-get 'warehouses r)))))
              callback force))

(defun dbx--warehouse-kind (w)
  (cond ((dbx-truthy (alist-get 'enable_serverless_compute w)) "Serverless")
        ((equal (alist-get 'warehouse_type w) "PRO") "Pro")
        (t "Classic")))

(defun dbx--read-warehouse (prompt &optional force)
  "Read a SQL warehouse with PROMPT; return its alist."
  (let* ((whs (dbx-await (lambda (k e) (dbx-warehouses-fetch k e force)) "warehouses"))
         (cands (mapcar (lambda (w)
                          (cons (concat (dbx-status-glyph (dbx-warehouse-status (alist-get 'state w)))
                                        " " (alist-get 'name w))
                                w))
                        whs))
         (pick (completing-read
                prompt
                (lambda (str pred action)
                  (if (eq action 'metadata)
                      `(metadata (category . dbx-warehouse)
                                 (annotation-function
                                  . ,(lambda (c)
                                       (let ((w (cdr (assoc c cands))))
                                         (concat (propertize " " 'display '(space :align-to 34))
                                                 (dbx-pad (dbx--warehouse-kind w) 11)
                                                 (dbx-pad (or (alist-get 'cluster_size w) "") 10)
                                                 (dbx-pad (or (alist-get 'state w) "") 10)
                                                 (format "%s sessions" (or (alist-get 'num_active_sessions w) 0)))))))
                    (complete-with-action action cands str pred)))
                nil t)))
    (cdr (assoc pick cands))))

;;;###autoload
(defun dbx-find-warehouse (&optional force)
  "Fuzzy-find a SQL warehouse and open its page."
  (interactive "P")
  (dbx-warehouse-page (alist-get 'id (dbx--read-warehouse "Warehouse: " force))))

(defvar-local dbx-warehouse--id nil)

(defvar-keymap dbx-warehouse-page-mode-map
  :parent dbx-page-mode-map
  "s" #'dbx-warehouse-start
  "S" #'dbx-warehouse-stop
  "U" #'dbx-warehouse-use)

(define-derived-mode dbx-warehouse-page-mode dbx-page-mode "DBX-Warehouse"
  "Mode for a SQL warehouse page.

\\{dbx-warehouse-page-mode-map}"
  (setq-local dbx-page-actions
              '(("s" "Start" dbx-warehouse-start) ("S" "Stop" dbx-warehouse-stop)
                ("U" "Use as SQL target" dbx-warehouse-use) ("o" "Open in browser" dbx-page-browse))))

;;;###autoload
(defun dbx-warehouse-page (id &optional context no-select)
  "Open the page of SQL warehouse ID in CONTEXT and return its buffer.
CONTEXT (a name or plist) defaults to the active context.  With
NO-SELECT, prepare the page without displaying it."
  (interactive (list (alist-get 'id (dbx--read-warehouse "Warehouse: "))))
  (let ((buf (dbx-page-buffer "warehouse" id #'dbx-warehouse-page-mode context)))
    (dbx--in-buffer buf
      (setq dbx-warehouse--id id)
      (setq-local dbx-page-refresh-function #'dbx-warehouse--refresh)
      (dbx-page-loading buf "warehouse")
      (dbx-warehouse--refresh))
    (dbx-page-display buf no-select)))

(defun dbx-warehouse--refresh ()
  (let ((buf (current-buffer)) (id dbx-warehouse--id))
    (dbx-api "get" (format "/api/2.0/sql/warehouses/%s" id)
             :error-callback (lambda (m) (dbx-page-error buf m))
             :callback
             (lambda (w)
               (dbx-api "get" "/api/2.0/sql/history/queries"
                        :params `((max_results . 50) (filter_by.warehouse_ids . ,id))
                        :error-callback (lambda (_) (dbx-warehouse--show buf w nil))
                        :callback (lambda (h)
                                    (dbx-warehouse--show
                                     buf w (cl-remove-if-not
                                            (lambda (q) (equal (alist-get 'warehouse_id q) id))
                                            (alist-get 'res h)))))))))

(defun dbx--query-status (s)
  (pcase s
    ("FINISHED" 'success) ("FAILED" 'failed) ((or "CANCELED" "CANCELLED") 'cancelled)
    ("RUNNING" 'running) ("QUEUED" 'waiting) (_ 'unknown)))

(defun dbx-warehouse--show (buf w queries)
  (when (buffer-live-p buf)
    (dbx--in-buffer buf
      (dbx-page-rename (alist-get 'name w))
      (setq dbx-page-active-p (memq (dbx-warehouse-status (alist-get 'state w)) '(running)))
      (let ((url (dbx-url (format "/sql/warehouses/%s" (alist-get 'id w))))
            (mx (apply #'max 1 (delq nil (mapcar (lambda (q) (alist-get 'duration q)) queries)))))
        (dbx-page-with-render
          (insert " " (propertize (alist-get 'name w) 'face 'dbx-title) "  "
                  (dbx-status-glyph (dbx-warehouse-status (alist-get 'state w))) " "
                  (alist-get 'state w) "   ")
          (dbx-page-button "Start" #'dbx-warehouse-start)
          (dbx-page-button "Stop" #'dbx-warehouse-stop)
          (dbx-page-button "Use for SQL" #'dbx-warehouse-use)
          (insert "\n")
          (put-text-property (point-min) (point) 'dbx-url url)
          (put-text-property (point-min) (point) 'dbx-id (alist-get 'id w))
          (dbx-page-kv "Type" (dbx--warehouse-kind w) "Size" (alist-get 'cluster_size w)
                       "Clusters" (format "%s–%s" (or (alist-get 'min_num_clusters w) 1)
                                          (or (alist-get 'max_num_clusters w) 1))
                       "Auto stop" (let ((m (alist-get 'auto_stop_mins w)))
                                     (if (and m (> m 0)) (format "%d min" m) "off")))
          (dbx-page-kv "Sessions" (or (alist-get 'num_active_sessions w) 0)
                       "Owner" (alist-get 'creator_name w) "ID" (alist-get 'id w))
          (insert "\n")
          (dbx-page-heading "Recent queries" (format "%d" (length queries)))
          (dolist (q queries)
            (let ((s (point)) (d (alist-get 'duration q)))
              (insert "  " (dbx-status-glyph (dbx--query-status (alist-get 'status q))) (dbx-align-to 4)
                      (dbx-pad (replace-regexp-in-string "[ \t\n]+" " " (or (alist-get 'query_text q) ""))
                               48)
                      " " (dbx-pad (dbx-format-time (alist-get 'query_start_time_ms q) "%H:%M") 6)
                      " " (dbx-pad (dbx-hbar d mx 8) 8) " "
                      (dbx-pad (dbx-format-duration d) 9 t) "  "
                      (propertize (or (alist-get 'user_name q) "") 'face 'dbx-dim) "\n")
              (let ((text (alist-get 'query_text q)) (err (alist-get 'error_message q)))
                (dbx-page-item s (1- (point))
                               'dbx-id (alist-get 'query_id q)
                               'help-echo err
                               'dbx-open (lambda () (dbx-sql-new-buffer text))))))))
      (dbx-page-schedule-refresh))))

(declare-function dbx-sql-new-buffer "dbx-sql")

(defun dbx--warehouse-id-here ()
  (or dbx-warehouse--id
      (and (derived-mode-p 'tabulated-list-mode) (tabulated-list-get-id))
      (alist-get 'id (dbx--read-warehouse "Warehouse: "))))

(defun dbx-warehouse-start () "Start the warehouse." (interactive)
       (let ((id (dbx--warehouse-id-here)))
         (dbx-api "post" (format "/api/2.0/sql/warehouses/%s/start" id) :body '()
                  :callback (lambda (_) (message "dbx: start requested") (dbx-cache-clear)))))

(defun dbx-warehouse-stop () "Stop the warehouse." (interactive)
       (let ((id (dbx--warehouse-id-here)))
         (when (dbx-confirm (format "Stop warehouse %s?" id))
           (dbx-api "post" (format "/api/2.0/sql/warehouses/%s/stop" id) :body '()
                    :callback (lambda (_) (message "dbx: stop requested") (dbx-cache-clear))))))

(defun dbx-warehouse-use ()
  "Use the warehouse as the context's SQL target."
  (interactive)
  (let ((id (dbx--warehouse-id-here)))
    (dbx-context-set :warehouse id)
    (message "dbx: SQL target is warehouse %s" id)))

(define-derived-mode dbx-warehouses-list-mode dbx-list-mode "DBX-Warehouses"
  "List of SQL warehouses."
  (setq tabulated-list-format [("State" 12 t) ("Name" 32 t) ("Type" 11 t) ("Size" 10 t)
                               ("Sessions" 8 nil :right-align t) ("Owner" 26 t)])
  (setq tabulated-list-sort-key '("Name"))
  (setq-local dbx-list-open-function #'dbx-warehouse-page)
  (setq-local dbx-list-url-function (lambda (id) (dbx-url (format "/sql/warehouses/%s" id))))
  (setq-local dbx-page-refresh-function #'dbx-warehouses--refresh)
  (setq-local dbx-page-actions '(("RET" "Open" dbx-page-open) ("s" "Start" dbx-warehouse-start)
                                 ("S" "Stop" dbx-warehouse-stop) ("U" "Use for SQL" dbx-warehouse-use)))
  (keymap-set dbx-warehouses-list-mode-map "s" #'dbx-warehouse-start)
  (keymap-set dbx-warehouses-list-mode-map "S" #'dbx-warehouse-stop)
  (keymap-set dbx-warehouses-list-mode-map "U" #'dbx-warehouse-use)
  (tabulated-list-init-header))

;;;###autoload
(defun dbx-warehouses (&optional context)
  "List SQL warehouses in CONTEXT (default: the active context)."
  (interactive)
  (let ((buf (dbx-page-buffer "warehouses" nil #'dbx-warehouses-list-mode context)))
    (dbx--in-buffer buf (dbx-warehouses--refresh))
    (pop-to-buffer buf)))

(defun dbx-warehouses--refresh ()
  (let ((buf (current-buffer)))
    (dbx-warehouses-fetch
     (lambda (ws)
       (when (buffer-live-p buf)
         (dbx--in-buffer buf
           (dbx-list-set-entries
            (mapcar (lambda (w)
                      (list (alist-get 'id w)
                            (vector (concat (dbx-status-glyph (dbx-warehouse-status (alist-get 'state w)))
                                            " " (alist-get 'state w))
                                    (alist-get 'name w) (dbx--warehouse-kind w)
                                    (or (alist-get 'cluster_size w) "")
                                    (format "%s" (or (alist-get 'num_active_sessions w) 0))
                                    (or (alist-get 'creator_name w) ""))))
                    ws)))))
     (lambda (m) (message "dbx: %s" m)) t)))

(defun dbx--compute-mode-line ()
  (let ((ctx (dbx-context)))
    (string-join
     (delq nil (list (cond ((plist-get ctx :serverless) "serverless")
                           ((plist-get ctx :cluster) (concat (dbx-glyph 'on) " cluster")))
                     (and (plist-get ctx :warehouse) (concat (dbx-glyph 'wh-on) " sql"))))
     " · ")))

(add-hook 'dbx-mode-line-extra-functions
          (lambda () (let ((s (dbx--compute-mode-line))) (and (not (string-empty-p s)) s))))

(provide 'dbx-compute)
;;; dbx-compute.el ends here
