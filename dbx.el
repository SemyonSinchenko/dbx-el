;;; dbx.el --- Core of the dbx-el Databricks workbench  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; Menus, the command map and module loading for dbx-el, a Databricks
;; workbench for Emacs 30.  The package entry point is dbx-el.el.  It needs the
;; `databricks' CLI for the control plane and uv for the optional
;; Python side (Databricks Connect).
;;
;; - SQL editor on SQL warehouses with results as Org tables
;; - jobs, runs, clusters, warehouses and pipelines as fuzzy pickers,
;;   text pages and lists
;; - bundle minor mode with validation, deployment and graphs
;; - pipeline dataset graphs from the event log
;; - a Python REPL running on Databricks Connect
;;
;; Suggested setup:
;;
;;   (keymap-global-set "C-c d" dbx-command-map)
;;   (dbx-mode-line-mode 1)

;;; Code:

(require 'transient)
(require 'dbx-util)
(require 'dbx-context)
(require 'dbx-cli)
(require 'dbx-page)
(require 'dbx-graph)
(require 'dbx-jobs)
(require 'dbx-compute)
(require 'dbx-sql)
(require 'dbx-bundle)
(require 'dbx-pipelines)
(require 'dbx-venv)
(require 'dbx-server)
(require 'dbx-doctor)

(defun dbx--context-description ()
  (let ((ctx (dbx-context)))
    (format "Databricks · %s (profile %s%s%s)"
            (plist-get ctx :name) (dbx-context-profile ctx)
            (cond ((plist-get ctx :serverless) " · serverless")
                  ((plist-get ctx :cluster) (format " · cluster %s" (plist-get ctx :cluster)))
                  (t ""))
            (if (plist-get ctx :warehouse) (format " · warehouse %s" (plist-get ctx :warehouse)) ""))))

;;;###autoload (autoload 'dbx-dispatch "dbx" nil t)
(transient-define-prefix dbx-dispatch ()
  "Databricks menu."
  [:description dbx--context-description
   ["Context"
    ("x" "select" dbx-select-context)
    ("D" "doctor" dbx-doctor)
    ("!" "watched runs" dbx-watched-runs)]
   ["Jobs & runs"
    ("o" "open ID/URL" dbx-open)
    ("j" "find job" dbx-find-job)
    ("J" "jobs list" dbx-jobs)
    ("r" "find run" dbx-find-run)
    ("R" "active runs" dbx-runs)]
   ["Compute"
    ("c" "find cluster" dbx-find-cluster)
    ("C" "clusters list" dbx-clusters)
    ("w" "find warehouse" dbx-find-warehouse)
    ("W" "warehouses list" dbx-warehouses)]
   ["SQL"
    ("s" "SQL editor" dbx-sql-new-buffer)
    ("h" "query history" dbx-sql-history)]]
  [["Pipelines"
    ("p" "find pipeline" dbx-find-pipeline)
    ("P" "pipelines list" dbx-pipelines)]
   ["Bundle"
    ("b" "bundle menu…" dbx-bundle-dispatch)]
   ["Python"
    ("z" "REPL" dbx-repl)
    ("k" "interrupt" dbx-server-interrupt)
    ("Z" "restart server" dbx-server-restart)]])

;;;###autoload (autoload 'dbx-bundle-dispatch "dbx" nil t)
(transient-define-prefix dbx-bundle-dispatch ()
  "Bundle menu."
  [:description (lambda () (format "Bundle · target %s"
                                   (or (ignore-errors (dbx-bundle-target)) "default")))
   ["Inspect"
    ("s" "summary" dbx-bundle-summary)
    ("g" "graph" dbx-bundle-graph)
    ("v" "validate" dbx-bundle-validate)
    ("l" "plan" dbx-bundle-plan)]
   ["Change"
    ("d" "deploy" dbx-bundle-deploy)
    ("r" "run resource" dbx-bundle-run)
    ("D" "destroy" dbx-bundle-destroy)]
   ["Other"
    ("t" "target" dbx-bundle-select-target)
    ("o" "open in browser" dbx-bundle-open)]])

;;;###autoload
(defun dbx-open (ref)
  "Open the page of REF, an object id or workspace URL, and return it.
Understands job, run, cluster, warehouse and pipeline URLs (also the
legacy #fragment forms) and bare ids; see `dbx-parse-object-ref'.  A
URL of a known workspace opens in that workspace's context, anything
else in the active context.  Interactively, the latest kill is the
default when it parses.  REF may also be a parsed plist."
  (interactive (list (dbx-read-object-ref "Open ID or URL")))
  (let* ((ref (if (stringp ref)
                  (or (dbx-parse-object-ref ref) (user-error "dbx: not an id or URL: %s" ref))
                ref))
         (id (plist-get ref :id))
         (ctx (plist-get ref :context)))
    (pcase (plist-get ref :kind)
      ('job (dbx-job-page id ctx))
      ('run (dbx-run-page id ctx))
      ('cluster (dbx-cluster-page id ctx))
      ('warehouse (dbx-warehouse-page id ctx))
      ('pipeline (dbx-pipeline-page id ctx)))))

(defun dbx-graph-dwim ()
  "Show the graph for the current buffer: job, run, pipeline or bundle."
  (interactive)
  (cond ((derived-mode-p 'dbx-job-page-mode) (dbx-job-graph))
        ((derived-mode-p 'dbx-run-page-mode) (dbx-run-graph))
        ((derived-mode-p 'dbx-pipeline-page-mode) (dbx-pipeline-graph))
        ((dbx-bundle-root) (dbx-bundle-graph))
        (t (user-error "No graph for this buffer"))))

;;;###autoload (autoload 'dbx-command-map "dbx" nil t 'keymap)
(defvar-keymap dbx-command-map
  :doc "Global dbx commands.  Suggested: (keymap-global-set \"C-c d\" dbx-command-map)."
  "d" #'dbx-dispatch
  "x" #'dbx-select-context
  "o" #'dbx-open
  "j" #'dbx-find-job
  "J" #'dbx-jobs
  "r" #'dbx-find-run
  "R" #'dbx-runs
  "!" #'dbx-watched-runs
  "c" #'dbx-find-cluster
  "C" #'dbx-clusters
  "w" #'dbx-find-warehouse
  "W" #'dbx-warehouses
  "s" #'dbx-sql-new-buffer
  "h" #'dbx-sql-history
  "p" #'dbx-find-pipeline
  "P" #'dbx-pipelines
  "b" #'dbx-bundle-dispatch
  "g" #'dbx-graph-dwim
  "z" #'dbx-repl
  "D" #'dbx-doctor)
(fset 'dbx-command-map dbx-command-map)

;;;###autoload
(add-hook 'find-file-hook #'dbx-bundle-maybe-enable)

(provide 'dbx)
;;; dbx.el ends here
