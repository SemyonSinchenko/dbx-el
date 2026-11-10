;;; dbx-bundle.el --- Declarative Automation Bundles  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; `dbx-bundle-mode' for bundle YAML files: Flymake diagnostics from
;; `bundle validate', commands for plan/deploy/run/destroy in
;; compilation buffers, a summary page and a resource graph built from
;; `bundle validate -o json'.

;;; Code:

(require 'compile)
(require 'flymake)
(require 'dbx-util)
(require 'dbx-cli)
(require 'dbx-page)
(require 'dbx-graph)

(declare-function dbx-job-page "dbx-jobs")
(declare-function dbx-run-page "dbx-jobs")
(declare-function dbx-pipeline-page "dbx-pipelines")

(defcustom dbx-bundle-auto-enable t
  "Non-nil to enable `dbx-bundle-mode' in YAML files of bundle projects."
  :type 'boolean :group 'dbx)

(defcustom dbx-bundle-open-run-page t
  "Non-nil to open the run page when `bundle run' reports a run URL."
  :type 'boolean :group 'dbx)

(defvar dbx-bundle-target-overrides (make-hash-table :test #'equal)
  "Bundle root -> target selected in this session.")

;;;; Root and target

(defun dbx-bundle-root (&optional dir)
  "Bundle root containing DIR (default `default-directory'), or nil."
  (let ((d (or dir default-directory)))
    (and (not (file-remote-p d))
         (or (locate-dominating-file d "databricks.yml")
             (locate-dominating-file d "databricks.yaml")))))

(defun dbx-bundle--root ()
  (or (dbx-bundle-root) (user-error "Not in a bundle project (no databricks.yml)")))

(defun dbx-bundle-targets (root)
  "Target names declared in ROOT's databricks.yml (simple scan)."
  (let ((file (expand-file-name (if (file-exists-p (expand-file-name "databricks.yml" root))
                                    "databricks.yml" "databricks.yaml")
                                root))
        targets)
    (with-temp-buffer
      (insert-file-contents file)
      (goto-char (point-min))
      (when (re-search-forward "^targets:[ \t]*$" nil t)
        (forward-line 1)
        (let (indent)
          (while (and (not (eobp))
                      (or (looking-at-p "^[ \t]*\\(#.*\\)?$")
                          (looking-at-p "^[ \t]+")))
            (when (looking-at "^\\([ \t]+\\)\\([A-Za-z0-9_-]+\\):")
              (let ((ind (length (match-string 1))))
                (unless indent (setq indent ind))
                (when (= ind indent) (push (match-string 2) targets))))
            (forward-line 1)))))
    (nreverse targets)))

(defun dbx-bundle-target (&optional root)
  "Target for ROOT: session choice, context :target, or nil (bundle default)."
  (or (gethash (or root (dbx-bundle-root)) dbx-bundle-target-overrides)
      (dbx-context-get :target)))

(defun dbx-bundle-select-target ()
  "Choose the bundle target for this project."
  (interactive)
  (let* ((root (dbx-bundle--root))
         (target (completing-read "Bundle target: " (dbx-bundle-targets root) nil t)))
    (puthash root target dbx-bundle-target-overrides)
    (force-mode-line-update t)
    (message "dbx: bundle target %s" target)))

(defun dbx-bundle--target-args (&optional root)
  (let ((tg (dbx-bundle-target root))) (and tg (list "-t" tg))))

;;;; Compilation runs

(defvar dbx-bundle-output-error-regexp-alist
  '(("^[ \t]+in \\([^:\n]+\\.ya?ml\\):\\([0-9]+\\):\\([0-9]+\\)" 1 2 3)
    ("File \"\\([^\"]+\\)\", line \\([0-9]+\\)" 1 2))
  "Error locations in bundle CLI output.")

(define-compilation-mode dbx-bundle-output-mode "DBX-Bundle"
  "Output of databricks bundle commands."
  (setq-local compilation-error-regexp-alist dbx-bundle-output-error-regexp-alist))

(defvar-local dbx-bundle--seen-run nil)

(defun dbx-bundle--detect-run ()
  (when (and dbx-bundle-open-run-page (not dbx-bundle--seen-run))
    (save-excursion
      (goto-char (point-min))
      (when (re-search-forward "/jobs/[0-9]+/runs/\\([0-9]+\\)" nil t)
        (setq dbx-bundle--seen-run (match-string 1))
        (let ((rid dbx-bundle--seen-run))
          (run-at-time 0 nil (lambda ()
                               (require 'dbx-jobs)
                               (save-selected-window (dbx-run-page rid)))))))))

(defun dbx-bundle-run-cli (args &optional name)
  "Run `databricks bundle ARGS' in a compilation buffer named after NAME."
  (let* ((root (dbx-bundle--root))
         (default-directory root)
         (process-environment (dbx-context-env))
         (cmd (mapconcat #'shell-quote-argument
                         (append (list dbx-cli-program "bundle") args
                                 (dbx-bundle--target-args root)
                                 (list "--profile" (dbx-context-profile)))
                         " ")))
    (with-current-buffer
        (compilation-start cmd #'dbx-bundle-output-mode
                           (lambda (_) (format "*dbx bundle %s*" (or name (car args)))))
      (setq dbx-bundle--seen-run nil)
      (add-hook 'compilation-filter-hook #'dbx-bundle--detect-run nil t)
      (current-buffer))))

(defun dbx-bundle-validate ()
  "Validate the bundle."
  (interactive)
  (dbx-bundle-run-cli '("validate")))

(defun dbx-bundle-plan ()
  "Show the deployment plan."
  (interactive)
  (dbx-bundle-run-cli '("plan")))

(defun dbx-bundle-deploy ()
  "Deploy the bundle after confirmation."
  (interactive)
  (let ((root (dbx-bundle--root)))
    (when (dbx-confirm (format "Deploy bundle in %s to target %s (profile %s)?"
                               (abbreviate-file-name root) (or (dbx-bundle-target root) "default")
                               (dbx-context-profile)))
      (dbx-bundle-run-cli '("deploy")))))

(defun dbx-bundle-destroy ()
  "Destroy all deployed resources of the bundle after confirmation."
  (interactive)
  (let ((root (dbx-bundle--root)))
    (when (and (dbx-confirm (format "DESTROY all resources of target %s?"
                                    (or (dbx-bundle-target root) "default")))
               (equal (read-string "Type the target name to confirm: ")
                      (or (dbx-bundle-target root) "default")))
      (dbx-bundle-run-cli '("destroy" "--auto-approve")))))

(defun dbx-bundle--resource-keys (callback)
  (dbx-cli (append '("bundle" "summary") (dbx-bundle--target-args))
           :directory (dbx-bundle--root)
           :callback (lambda (s)
                       (funcall callback
                                (cl-loop for (type . items) in (dbx-get s 'resources)
                                         append (mapcar (lambda (kv) (format "%s.%s" type (car kv)))
                                                        items))))))

(defun dbx-bundle-run (key)
  "Run bundle resource KEY (a job or pipeline)."
  (interactive
   (list (let ((keys (dbx-await (lambda (k e) (ignore e) (dbx-bundle--resource-keys k))
                                "bundle resources")))
           (completing-read "Run resource: "
                            (cl-remove-if-not (lambda (k) (string-match-p "\\`\\(jobs\\|pipelines\\)\\." k))
                                              keys)
                            nil t))))
  (when (or (not (dbx-context-get :confirm))
            (dbx-confirm (format "Run %s?" key)))
    (dbx-bundle-run-cli (list "run" (cadr (split-string key "\\."))) (concat "run " key))))

(defun dbx-bundle-open ()
  "Open a deployed bundle resource in the browser."
  (interactive)
  (let* ((s (dbx-await (lambda (k e) (dbx-cli (append '("bundle" "summary") (dbx-bundle--target-args))
                                                :directory (dbx-bundle--root)
                                                :callback k :error-callback e))
                       "bundle summary"))
         (urls (cl-loop for (type . items) in (dbx-get s 'resources)
                        append (cl-loop for (key . r) in items
                                        when (alist-get 'url r)
                                        collect (cons (format "%s.%s" type key) (alist-get 'url r))))))
    (browse-url (cdr (assoc (completing-read "Open: " urls nil t) urls)))))

;;;; Summary page

(defvar-local dbx-bundle--root-dir nil)

(defvar-keymap dbx-bundle-summary-mode-map
  :parent dbx-page-mode-map
  "y" #'dbx-bundle-goto-definition
  "r" #'dbx-bundle-run-at-point
  "v" #'dbx-bundle-validate
  "l" #'dbx-bundle-plan
  "d" #'dbx-bundle-deploy
  "G" #'dbx-bundle-graph)

(define-derived-mode dbx-bundle-summary-mode dbx-page-mode "DBX-Bundle"
  "Summary page of a bundle.

\\{dbx-bundle-summary-mode-map}"
  (setq-local dbx-page-actions
              '(("RET" "Open resource page" dbx-page-open)
                ("y" "Go to YAML definition" dbx-bundle-goto-definition)
                ("r" "Run resource at point" dbx-bundle-run-at-point)
                ("v" "Validate" dbx-bundle-validate) ("l" "Plan" dbx-bundle-plan)
                ("d" "Deploy" dbx-bundle-deploy) ("G" "Graph" dbx-bundle-graph))))

;;;###autoload
(defun dbx-bundle-summary ()
  "Show the summary page of the bundle."
  (interactive)
  (let* ((root (dbx-bundle--root))
         (buf (dbx-page-buffer (format "*dbx bundle: %s*" (file-name-nondirectory
                                                           (directory-file-name root)))
                               #'dbx-bundle-summary-mode)))
    (with-current-buffer buf
      (setq dbx-bundle--root-dir root)
      (setq default-directory root)
      (setq-local dbx-page-refresh-function #'dbx-bundle--summary-refresh)
      (dbx-page-loading buf "bundle summary")
      (dbx-bundle--summary-refresh))
    (pop-to-buffer buf)))

(defun dbx-bundle--summary-refresh ()
  (let ((buf (current-buffer)))
    (dbx-cli (append '("bundle" "summary") (dbx-bundle--target-args dbx-bundle--root-dir))
             :directory dbx-bundle--root-dir
             :error-callback (lambda (m) (dbx-page-error buf m))
             :callback (lambda (s) (when (buffer-live-p buf)
                                     (with-current-buffer buf (dbx-bundle--summary-render s)))))))

(defun dbx-bundle--summary-render (s)
  (dbx-page-with-render
    (insert " " (propertize (format "Bundle %s" (or (dbx-get s 'bundle 'name) "?")) 'face 'dbx-title) "   ")
    (dbx-page-button "Validate" #'dbx-bundle-validate)
    (dbx-page-button "Plan" #'dbx-bundle-plan)
    (dbx-page-button "Deploy" #'dbx-bundle-deploy)
    (dbx-page-button "Graph" #'dbx-bundle-graph)
    (insert "\n")
    (dbx-page-kv "Target" (dbx-get s 'bundle 'target)
                 "Mode" (dbx-get s 'bundle 'mode)
                 "Host" (dbx-get s 'workspace 'host))
    (dbx-page-kv "Root" (dbx-get s 'workspace 'root_path))
    (insert "\n")
    (dolist (group (dbx-get s 'resources))
      (let ((type (car group)) (items (cdr group)))
        (dbx-page-heading (format "%s (%d)" (capitalize (replace-regexp-in-string "_" " " (symbol-name type)))
                                  (length items)))
        (dolist (kv items)
          (let* ((key (symbol-name (car kv))) (r (cdr kv))
                 (id (alist-get 'id r)) (start (point)))
            (insert "  " (dbx-pad key 32) " "
                    (dbx-pad (or (alist-get 'name r) "") 36) " "
                    (propertize (if id (format "%s" id) "not deployed") 'face 'dbx-dim) "\n")
            (dbx-page-item start (1- (point))
                           'dbx-id id 'dbx-url (alist-get 'url r)
                           'dbx-key (cons type key)
                           'dbx-open (dbx-bundle--resource-opener type id (alist-get 'url r)))))
        (insert "\n")))))

(defun dbx-bundle--resource-opener (type id url)
  (lambda ()
    (pcase type
      ('jobs (if id (progn (require 'dbx-jobs) (dbx-job-page id)) (message "Not deployed")))
      ('pipelines (if id (progn (require 'dbx-pipelines) (dbx-pipeline-page id))
                    (message "Not deployed")))
      (_ (if url (browse-url url) (message "No page for %s" type))))))

(defun dbx-bundle-run-at-point ()
  "Run the job or pipeline at point."
  (interactive)
  (let ((k (get-text-property (point) 'dbx-key)))
    (unless (and k (memq (car k) '(jobs pipelines))) (user-error "No job or pipeline at point"))
    (dbx-bundle-run (format "%s.%s" (car k) (cdr k)))))

(defun dbx-bundle-goto-definition ()
  "Jump to the YAML definition of the resource at point."
  (interactive)
  (let* ((k (or (get-text-property (point) 'dbx-key) (user-error "No resource at point")))
         (root (or dbx-bundle--root-dir (dbx-bundle--root)))
         (files (directory-files-recursively root "\\.ya?ml\\'" nil
                                             (lambda (d) (not (string-match-p "/\\.\\(databricks\\|git\\|venv\\)" d)))))
         (re (format "^[ \t]+%s:[ \t]*$" (regexp-quote (cdr k)))))
    (cl-loop for f in files
             when (with-temp-buffer (insert-file-contents f) (re-search-forward re nil t))
             do (find-file f) (goto-char (point-min)) (re-search-forward re nil t)
             (beginning-of-line) (cl-return)
             finally (message "Definition of %s not found" (cdr k)))))

;;;; Graph

(defun dbx-bundle--ref (s kind)
  "Resource key if S is a ${resources.KIND.KEY.id} reference."
  (and (stringp s)
       (string-match (format "\\${resources\\.%s\\.\\([^.}]+\\)\\.id}" kind) s)
       (match-string 1 s)))

(defun dbx-bundle-graph-from-config (config)
  "Build a graph from the resolved bundle CONFIG (validate -o json)."
  (let ((nodes nil) (edges nil)
        (jobs (dbx-get config 'resources 'jobs))
        (pipelines (dbx-get config 'resources 'pipelines)))
    (dolist (p pipelines)
      (push (list :id (format "pipeline:%s" (car p))
                  :label (or (alist-get 'name (cdr p)) (symbol-name (car p)))
                  :kind 'pipeline
                  :action (let ((key (car p)))
                            (lambda () (message "Pipeline %s (open its page from the summary)" key))))
            nodes))
    (dolist (j jobs)
      (let* ((jkey (symbol-name (car j))) (jname (or (alist-get 'name (cdr j)) jkey)))
        (dolist (tk (alist-get 'tasks (cdr j)))
          (let ((id (format "%s/%s" jkey (alist-get 'task_key tk))))
            (push (list :id id :label (alist-get 'task_key tk) :group jname
                        :info (dbx--bundle-task-type tk))
                  nodes)
            (dolist (d (alist-get 'depends_on tk))
              (push (list :from (format "%s/%s" jkey (alist-get 'task_key d)) :to id
                          :label (alist-get 'outcome d))
                    edges))
            (when-let* ((p (dbx-bundle--ref (dbx-get tk 'pipeline_task 'pipeline_id) "pipelines")))
              (push (list :from id :to (format "pipeline:%s" p) :label "runs") edges))
            (when-let* ((other (dbx-bundle--ref (dbx-get tk 'run_job_task 'job_id) "jobs")))
              (push (list :from id :to (format "job:%s" other) :label "runs") edges)
              (unless (cl-find (format "job:%s" other) nodes :key (lambda (n) (plist-get n :id))
                               :test #'equal)
                (push (list :id (format "job:%s" other) :label (format "job %s" other) :kind 'job)
                      nodes)))))))
    (list :title (format "Bundle %s" (or (dbx-get config 'bundle 'name) ""))
          :subtitle (format "target %s" (or (dbx-get config 'bundle 'target) "default"))
          :nodes (nreverse nodes) :edges (nreverse edges))))

(defun dbx--bundle-task-type (tk)
  (let ((type (cl-find-if (lambda (kv) (string-suffix-p "_task" (symbol-name (car kv)))) tk)))
    (and type (string-remove-suffix "_task" (symbol-name (car type))))))

;;;###autoload
(defun dbx-bundle-graph ()
  "Show the graph of the bundle's jobs, tasks and pipelines."
  (interactive)
  (let ((root (or dbx-bundle--root-dir (dbx-bundle--root))))
    (message "dbx: resolving bundle configuration…")
    (dbx-cli (append '("bundle" "validate") (dbx-bundle--target-args root))
             :directory root
             :callback (lambda (config)
                         (message nil)
                         (dbx-graph-show (dbx-bundle-graph-from-config config))))))

;;;; Flymake

(defun dbx-bundle-parse-diagnostics (output)
  "Parse `bundle validate' text OUTPUT into (LEVEL MESSAGE FILE LINE COL) lists."
  (let (diags current)
    (dolist (line (split-string output "\n"))
      (cond
       ((string-match "\\`\\(Error\\|Warning\\|Recommendation\\): \\(.*\\)" line)
        (setq current (list (pcase (match-string 1 line)
                              ("Error" :error) ("Warning" :warning) (_ :note))
                            (match-string 2 line))))
       ((and current (string-match "\\`[ \t]+in \\([^:]+\\):\\([0-9]+\\):\\([0-9]+\\)" line))
        (push (append current (list (match-string 1 line)
                                    (string-to-number (match-string 2 line))
                                    (string-to-number (match-string 3 line))))
              diags))
       ((string-blank-p line) (setq current nil))))
    (nreverse diags)))

(defvar-local dbx-bundle--flymake-proc nil)

(defun dbx-bundle-flymake (report-fn &rest _)
  "Flymake backend running `bundle validate'; reports via REPORT-FN."
  (when (process-live-p dbx-bundle--flymake-proc) (kill-process dbx-bundle--flymake-proc))
  (let* ((source (current-buffer))
         (root (dbx-bundle-root))
         (file (and buffer-file-name root (file-relative-name buffer-file-name root)))
         (out (generate-new-buffer " *dbx-bundle-flymake*"))
         (default-directory (or root default-directory))
         (process-environment (dbx-context-env)))
    (if (not (and root file (executable-find dbx-cli-program)))
        (funcall report-fn nil)
      (setq dbx-bundle--flymake-proc
            (make-process
             :name "dbx-bundle-validate" :buffer out :noquery t :connection-type 'pipe
             :command (append (list dbx-cli-program "bundle" "validate")
                              (dbx-bundle--target-args root)
                              (list "--profile" (dbx-context-profile)))
             :sentinel
             (lambda (proc _)
               (unless (process-live-p proc)
                 (let ((text (with-current-buffer out (buffer-string))))
                   (kill-buffer out)
                   (when (and (buffer-live-p source)
                              (eq proc (buffer-local-value 'dbx-bundle--flymake-proc source)))
                     (with-current-buffer source
                       (funcall report-fn
                                (cl-loop for (level msg f line col) in (dbx-bundle-parse-diagnostics text)
                                         when (equal (file-truename (expand-file-name f root))
                                                     (file-truename buffer-file-name))
                                         collect (let ((reg (flymake-diag-region source line col)))
                                                   (flymake-make-diagnostic source (car reg) (cdr reg)
                                                                            level msg))))))))))))))

;;;; Minor mode

(defvar-keymap dbx-bundle-mode-map
  "C-c C-b" #'dbx-bundle-dispatch)

(declare-function dbx-bundle-dispatch "dbx")

;;;###autoload
(define-minor-mode dbx-bundle-mode
  "Minor mode for files of a Databricks bundle project.

\\{dbx-bundle-mode-map}"
  :lighter (:eval (format " Bundle[%s]" (or (dbx-bundle-target) "default")))
  :group 'dbx
  (if dbx-bundle-mode
      (when (and buffer-file-name (string-match-p "\\.ya?ml\\'" buffer-file-name))
        (add-hook 'flymake-diagnostic-functions #'dbx-bundle-flymake nil t)
        (flymake-mode 1))
    (remove-hook 'flymake-diagnostic-functions #'dbx-bundle-flymake t)))

;;;###autoload
(defun dbx-bundle-maybe-enable ()
  "Enable `dbx-bundle-mode' in bundle project files."
  (when (and dbx-bundle-auto-enable buffer-file-name
             (string-match-p "\\.ya?ml\\'" buffer-file-name)
             (dbx-bundle-root))
    (dbx-bundle-mode 1)))

(provide 'dbx-bundle)
;;; dbx-bundle.el ends here
