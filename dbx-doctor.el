;;; dbx-doctor.el --- Environment checks  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; `dbx-doctor' checks Emacs, the Databricks CLI, auth, uv, Graphviz
;; and the Python environment, and shows ✔ / ▲ / ✘ lines with hints.

;;; Code:

(require 'dbx-util)
(require 'dbx-cli)
(require 'dbx-page)
(require 'dbx-venv)

(define-derived-mode dbx-doctor-mode dbx-page-mode "DBX-Doctor"
  "Doctor report.")

(defun dbx-doctor--line (level text &optional hint)
  (with-current-buffer (get-buffer-create "*dbx doctor*")
    (let ((inhibit-read-only t))
      (goto-char (point-max))
      (insert " " (dbx-status-glyph (pcase level ('ok 'success) ('warn 'warning) ('info 'absent) (_ 'failed)))
              " " text
              (if hint (propertize (concat "\n     " hint) 'face 'dbx-dim) "")
              "\n"))))

(defun dbx--version-parse (s)
  (and s (string-match "\\([0-9]+\\)\\.\\([0-9]+\\)\\.\\([0-9]+\\)" s)
       (list (string-to-number (match-string 1 s)) (string-to-number (match-string 2 s))
             (string-to-number (match-string 3 s)))))

(defun dbx--version<= (a b)
  "Non-nil if version string A <= B."
  (let ((va (dbx--version-parse a)) (vb (dbx--version-parse b)))
    (and va vb (not (version-list-< vb va)))))

(defun dbx--first-line (program &rest args)
  (ignore-errors (car (apply #'process-lines-ignore-status program args))))

;;;###autoload
(defun dbx-doctor ()
  "Check the dbx environment for the current context."
  (interactive)
  (let ((buf (get-buffer-create "*dbx doctor*")) (ctx (dbx-context)))
    (with-current-buffer buf
      (dbx-doctor-mode)
      (setq-local dbx-page-refresh-function #'dbx-doctor)
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert " " (propertize (format "Databricks doctor · %s" (plist-get ctx :name)) 'face 'dbx-title)
                "\n\n")))
    (pop-to-buffer buf)
    ;; Emacs
    (if (version<= "30.1" emacs-version)
        (dbx-doctor--line 'ok (format "Emacs %s" emacs-version))
      (dbx-doctor--line 'error (format "Emacs %s (30.1 or newer required)" emacs-version)))
    ;; CLI
    (let* ((cli (executable-find dbx-cli-program))
           (ver (and cli (dbx--first-line cli "version"))))
      (cond ((not cli)
             (dbx-doctor--line 'error "databricks CLI not found"
                               "Install: https://docs.databricks.com/dev-tools/cli/install.html"))
            ((dbx--version<= dbx-cli-min-version ver)
             (dbx-doctor--line 'ok (format "databricks CLI %s (minimum %s)"
                                           (mapconcat #'number-to-string (dbx--version-parse ver) ".")
                                           dbx-cli-min-version)))
            (t (dbx-doctor--line 'warn (format "databricks CLI %s is older than %s" ver dbx-cli-min-version)
                                 "Upgrade the CLI; JSON shapes may differ"))))
    ;; Profile
    (let ((prof (assoc (dbx-context-profile ctx) (dbx-config-profiles))))
      (if prof
          (dbx-doctor--line 'ok (format "profile %s → %s" (car prof) (or (dbx-context-host ctx) "?")))
        (dbx-doctor--line 'error (format "profile %s not found in %s" (dbx-context-profile ctx)
                                         (abbreviate-file-name (dbx--config-file)))
                          "Run: databricks auth login --profile NAME")))
    (when-let* ((vars (dbx-context-scrubbed-vars)))
      (dbx-doctor--line 'info (format "ignoring inherited %s" (string-join vars ", "))
                        "They would override the selected profile"))
    ;; Compute
    (dbx-doctor--line (if (or (plist-get ctx :cluster) (plist-get ctx :serverless)) 'ok 'info)
                      (cond ((plist-get ctx :serverless) "compute: serverless")
                            ((plist-get ctx :cluster) (format "compute: cluster %s" (plist-get ctx :cluster)))
                            (t "no compute in context (needed only for Python)")))
    (dbx-doctor--line (if (plist-get ctx :warehouse) 'ok 'info)
                      (if (plist-get ctx :warehouse) (format "SQL warehouse %s" (plist-get ctx :warehouse))
                        "no SQL warehouse in context (the SQL editor will ask)"))
    ;; uv, dot
    (let ((uv (executable-find dbx-uv-program)))
      (if uv (dbx-doctor--line 'ok (or (dbx--first-line uv "--version") "uv"))
        (dbx-doctor--line (if dbx-connect-venv 'info 'warn) "uv not found"
                          "Needed for managed Python environments: https://docs.astral.sh/uv/")))
    (if (executable-find "dot")
        (dbx-doctor--line 'ok (or (dbx--first-line "dot" "-V") "graphviz"))
      (dbx-doctor--line 'info "graphviz `dot' not found: graphs use text"))
    ;; Python environment
    (if dbx-connect-venv
        (let ((py (dbx-venv-python dbx-connect-venv)))
          (if (not (file-executable-p py))
              (dbx-doctor--line 'error (format "no Python in %s" dbx-connect-venv))
            (dbx-doctor--probe py)))
      (let ((dirs (and (file-directory-p dbx-venv-root)
                       (directory-files dbx-venv-root nil "\\`dbconnect-"))))
        (dbx-doctor--line 'info (if dirs (format "managed environments: %s" (string-join dirs ", "))
                                  "no managed environment yet (created on first Python use)"))))
    ;; Auth (async)
    (when (executable-find dbx-cli-program)
      (dbx-cli '("current-user" "me")
               :context ctx
               :callback (lambda (me) (dbx-doctor--line 'ok (format "authenticated as %s"
                                                                    (or (alist-get 'userName me)
                                                                        (alist-get 'displayName me) "?"))))
               :error-callback (lambda (m) (dbx-doctor--line 'error (concat "auth: " m)
                                                             (format "Run: databricks auth login --profile %s"
                                                                     (dbx-context-profile ctx))))))))

(defun dbx-doctor--probe (python)
  (let* ((process-environment (cons (concat "PYTHONPATH=" (dbx-package-python-dir)) process-environment))
         (out (ignore-errors (dbx--first-line python "-m" "dbx_emacs" "probe")))
         (info (and out (ignore-errors (dbx-json-parse-string out)))))
    (cond ((not info) (dbx-doctor--line 'error (format "cannot probe %s" python)))
          ((dbx-truthy (alist-get 'conflict info))
           (dbx-doctor--line 'error "pyspark installed next to databricks-connect"
                             "Remove pyspark / pyspark-client from that environment"))
          ((not (dbx-get info 'packages 'databricks-connect))
           (dbx-doctor--line 'error "databricks-connect is not installed in dbx-connect-venv"))
          (t (dbx-doctor--line 'ok (format "Python %s, databricks-connect %s"
                                           (alist-get 'python info)
                                           (dbx-get info 'packages 'databricks-connect)))))))

(provide 'dbx-doctor)
;;; dbx-doctor.el ends here
