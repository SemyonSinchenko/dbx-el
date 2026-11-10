;;; dbx-context.el --- Contexts: profile, compute, warehouse, target  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; A context is what the user works against: a Databricks config
;; profile, optional compute (cluster or serverless), an optional SQL
;; warehouse and an optional bundle target.  Contexts are defined in
;; `dbx-contexts'.  `.dir-locals.el' may only name a context through
;; `dbx-project-context'.

;;; Code:

(require 'dbx-util)

(defcustom dbx-contexts nil
  "Databricks contexts.
Each element is a plist with keys:
  :name       display name (string, required)
  :profile    profile in ~/.databrickscfg (default \"DEFAULT\")
  :cluster    cluster id used for Databricks Connect
  :serverless non-nil to use serverless compute instead of :cluster
  :warehouse  SQL warehouse id for the SQL editor
  :catalog    default catalog for SQL
  :schema     default schema for SQL
  :target     bundle target
  :confirm    non-nil to confirm every execution and destructive action"
  :type '(repeat (plist :key-type symbol :value-type sexp))
  :group 'dbx)

(defcustom dbx-config-file nil
  "Databricks config file; nil means ~/.databrickscfg."
  :type '(choice (const nil) file)
  :group 'dbx)

(defcustom dbx-env-scrub-regexps
  '("\\`DATABRICKS_" "\\`SPARK_REMOTE\\'" "\\`SPARK_CONNECT_MODE_ENABLED\\'"
    "\\`ARM_\\(CLIENT_ID\\|CLIENT_SECRET\\|TENANT_ID\\)\\'")
  "Inherited environment variables removed before running the CLI or Python.
They would otherwise override the selected profile."
  :type '(repeat regexp)
  :group 'dbx)

(defvar-local dbx-project-context nil
  "Name of the context for this project, usually set in .dir-locals.el.")
(put 'dbx-project-context 'safe-local-variable #'dbx--known-context-name-p)

(defvar dbx-current-context-name nil
  "Name of the globally selected context.")

(defvar dbx--context-overrides (make-hash-table :test #'equal)
  "Runtime changes to contexts (NAME -> plist), e.g. a chosen warehouse.")

(defun dbx--known-context-name-p (name)
  "Non-nil if NAME names a context in `dbx-contexts'."
  (and (stringp name)
       (cl-some (lambda (c) (equal (plist-get c :name) name)) dbx-contexts)))

(defun dbx--context-by-name (name)
  "Return the context plist named NAME, with runtime overrides."
  (let ((base (or (cl-find name dbx-contexts
                           :key (lambda (c) (plist-get c :name)) :test #'equal)
                  (and name (list :name name :profile name))))
        (over (gethash name dbx--context-overrides)))
    (when base
      (let ((ctx (copy-sequence base)))
        (cl-loop for (k v) on over by #'cddr do (setq ctx (plist-put ctx k v)))
        ctx))))

(defun dbx-context ()
  "Return the active context plist."
  (let ((name (or dbx-project-context dbx-current-context-name
                  (plist-get (car dbx-contexts) :name))))
    (or (and name (dbx--context-by-name name))
        (list :name "DEFAULT" :profile "DEFAULT"))))

(defun dbx-context-get (key &optional ctx)
  "Return KEY of CTX (default: active context)."
  (plist-get (or ctx (dbx-context)) key))

(defun dbx-context-profile (&optional ctx)
  "Profile name of CTX."
  (or (dbx-context-get :profile ctx) "DEFAULT"))

(defun dbx-context-set (key value &optional ctx)
  "Set KEY to VALUE for CTX for the rest of the session."
  (let* ((name (plist-get (or ctx (dbx-context)) :name))
         (over (gethash name dbx--context-overrides)))
    (puthash name (plist-put over key value) dbx--context-overrides)
    (force-mode-line-update t)))

;;;; Config file

(defun dbx--config-file ()
  (expand-file-name (or dbx-config-file "~/.databrickscfg")))

(defun dbx-config-profiles ()
  "Parse the Databricks config file into ((PROFILE . ALIST) ...).
Values that look like secrets are dropped."
  (let ((file (dbx--config-file)) result current)
    (when (file-readable-p file)
      (with-temp-buffer
        (insert-file-contents file)
        (goto-char (point-min))
        (while (not (eobp))
          (let ((line (string-trim (buffer-substring (line-beginning-position)
                                                     (line-end-position)))))
            (cond
             ((string-match "\\`\\[\\(.+\\)\\]\\'" line)
              (setq current (list (match-string 1 line)))
              (push current result))
             ((and current (string-match "\\`\\([A-Za-z0-9_]+\\)\\s-*=\\s-*\\(.*\\)\\'" line))
              (let ((k (intern (downcase (match-string 1 line))))
                    (v (match-string 2 line)))
                (unless (memq k '(token password client_secret azure_client_secret))
                  (setcdr current (cons (cons k v) (cdr current))))))))
          (forward-line 1))))
    (nreverse result)))

(defun dbx-context-host (&optional ctx)
  "Workspace host URL of CTX, from :host or the config file."
  (let ((h (or (dbx-context-get :host ctx)
               (alist-get 'host (cdr (assoc (dbx-context-profile ctx)
                                            (dbx-config-profiles)))))))
    (and h (string-trim-right
            (if (string-match-p "\\`https?://" h) h (concat "https://" h))
            "/+"))))

(defun dbx-url (path &optional ctx)
  "Absolute workspace URL for PATH in CTX."
  (concat (or (dbx-context-host ctx) "") path))

;;;; Environment

(defun dbx-context-env (&optional ctx with-compute)
  "Return `process-environment' for CTX.
Inherited auth variables are scrubbed and the profile is set.
With WITH-COMPUTE, also set the Databricks Connect compute selector."
  (let* ((ctx (or ctx (dbx-context)))
         (env (cl-remove-if
               (lambda (kv)
                 (let ((name (car (split-string kv "="))))
                   (cl-some (lambda (re) (string-match-p re name))
                            dbx-env-scrub-regexps)))
               process-environment)))
    (push (concat "DATABRICKS_CONFIG_PROFILE=" (dbx-context-profile ctx)) env)
    (when dbx-config-file
      (push (concat "DATABRICKS_CONFIG_FILE=" (dbx--config-file)) env))
    (when with-compute
      (cond ((plist-get ctx :serverless)
             (push "DATABRICKS_SERVERLESS_COMPUTE_ID=auto" env))
            ((plist-get ctx :cluster)
             (push (concat "DATABRICKS_CLUSTER_ID=" (plist-get ctx :cluster)) env))))
    env))

(defun dbx-context-scrubbed-vars ()
  "Names of inherited variables `dbx-context-env' removes."
  (cl-loop for kv in process-environment
           for name = (car (split-string kv "="))
           when (cl-some (lambda (re) (string-match-p re name)) dbx-env-scrub-regexps)
           collect name))

;;;; Selection

(defun dbx--context-annotation (name)
  (let ((c (dbx--context-by-name name)))
    (concat "  " (propertize (format "profile %s" (dbx-context-profile c)) 'face 'dbx-dim)
            (cond ((plist-get c :serverless) "  serverless")
                  ((plist-get c :cluster) (format "  cluster %s" (plist-get c :cluster)))
                  (t ""))
            (if (plist-get c :warehouse) (format "  warehouse %s" (plist-get c :warehouse)) ""))))

;;;###autoload
(defun dbx-select-context (name)
  "Select context NAME globally.
Candidates are `dbx-contexts' plus profiles in ~/.databrickscfg."
  (interactive
   (list (let* ((names (delete-dups
                        (append (mapcar (lambda (c) (plist-get c :name)) dbx-contexts)
                                (mapcar #'car (dbx-config-profiles))))))
           (completing-read
            "Databricks context: "
            (lambda (str pred action)
              (if (eq action 'metadata)
                  `(metadata (category . dbx-context)
                             (annotation-function . dbx--context-annotation))
                (complete-with-action action names str pred)))
            nil t))))
  (setq dbx-current-context-name name)
  (force-mode-line-update t)
  (message "dbx: context %s" name))

;;;; Mode line

(defvar dbx-mode-line-extra-functions nil
  "Functions returning extra mode-line strings (or nil).")

(defun dbx--mode-line-string ()
  (let* ((ctx (dbx-context))
         (parts (delq nil (mapcar #'funcall dbx-mode-line-extra-functions))))
    (concat " " (propertize (concat "DBX:" (plist-get ctx :name))
                            'face 'bold
                            'help-echo "mouse-1: Databricks menu"
                            'mouse-face 'mode-line-highlight
                            'local-map (make-mode-line-mouse-map
                                        'mouse-1 (lambda () (interactive)
                                                   (call-interactively 'dbx-dispatch))))
            (if parts (concat " " (string-join parts " · ")) "")
            " ")))

;;;###autoload
(define-minor-mode dbx-mode-line-mode
  "Show the Databricks context in the mode line."
  :global t :group 'dbx
  (setq global-mode-string (delete '(:eval (dbx--mode-line-string)) global-mode-string))
  (when dbx-mode-line-mode
    (setq global-mode-string
          (append global-mode-string '((:eval (dbx--mode-line-string)))))))

(provide 'dbx-context)
;;; dbx-context.el ends here
