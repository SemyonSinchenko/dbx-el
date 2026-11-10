;;; dbx-venv.el --- Python environment for Databricks Connect via uv  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; dbx needs a Python environment with `databricks-connect' for the
;; companion server.  By default it creates and owns one per Databricks
;; Connect version with uv (uv also provides the Python interpreter).
;; Set `dbx-connect-venv' to use your own environment instead; dbx
;; never installs anything into it.

;;; Code:

(require 'dbx-util)
(require 'dbx-cli)

(defcustom dbx-connect-venv nil
  "Virtual environment with databricks-connect to use.
nil means dbx manages environments with uv under `dbx-venv-root'.
A user environment is used as is and never modified."
  :type '(choice (const :tag "Managed by dbx (uv)" nil) directory)
  :group 'dbx)

(defcustom dbx-uv-program "uv"
  "The uv executable.  uv is the only Python-side requirement."
  :type 'string :group 'dbx)

(defcustom dbx-venv-root (locate-user-emacs-file "dbx/venvs/")
  "Directory for environments managed by dbx."
  :type 'directory :group 'dbx)

(defcustom dbx-connect-version nil
  "Databricks Connect version spec for managed environments, e.g. \"16.4.*\".
nil derives it from the context: the cluster's DBR, or
`dbx-serverless-connect-version' for serverless."
  :type '(choice (const nil) string) :group 'dbx)

(defcustom dbx-serverless-connect-version "17.3.*"
  "Databricks Connect version spec used with serverless compute."
  :type 'string :group 'dbx)

(defcustom dbx-venv-extra-packages nil
  "Extra packages installed into managed environments."
  :type '(repeat string) :group 'dbx)

(defun dbx-venv-python (venv)
  "Python executable inside VENV."
  (let ((dir (file-name-as-directory (expand-file-name venv))))
    (if (eq system-type 'windows-nt)
        (concat dir "Scripts/python.exe")
      (concat dir "bin/python"))))

(defun dbx-venv-python-for-dbr (major)
  "Python minor version matching DBR MAJOR."
  (cond ((<= major 14) "3.10") ((= major 15) "3.11") (t "3.12")))

(defun dbx--venv-spec (callback &optional ctx)
  "Call CALLBACK with (CONNECT-SPEC . PYTHON-VERSION) for CTX."
  (let ((ctx (or ctx (dbx-context))))
    (cond
     (dbx-connect-version
      (let ((major (string-to-number dbx-connect-version)))
        (funcall callback (cons dbx-connect-version (dbx-venv-python-for-dbr major)))))
     ((plist-get ctx :serverless)
      (funcall callback (cons dbx-serverless-connect-version
                              (dbx-venv-python-for-dbr (string-to-number dbx-serverless-connect-version)))))
     ((plist-get ctx :cluster)
      (dbx-api "get" "/api/2.1/clusters/get" :params `((cluster_id . ,(plist-get ctx :cluster)))
               :context ctx
               :callback (lambda (c)
                           (let ((v (or (alist-get 'spark_version c) "")))
                             (if (string-match "\\`\\([0-9]+\\)\\.\\([0-9]+\\)" v)
                                 (funcall callback
                                          (cons (format "%s.%s.*" (match-string 1 v) (match-string 2 v))
                                                (dbx-venv-python-for-dbr
                                                 (string-to-number (match-string 1 v)))))
                               (user-error "Cannot read DBR version of cluster: %s" v))))))
     (t (user-error "Context %s has no :cluster or :serverless; set one or `dbx-connect-version'"
                    (plist-get ctx :name))))))

(defun dbx--managed-venv-dir (spec)
  (expand-file-name (concat "dbconnect-" (replace-regexp-in-string "[^0-9.]" "" (string-remove-suffix ".*" spec)))
                    dbx-venv-root))

(defun dbx-venv-ensure (callback &optional ctx)
  "Call CALLBACK with a Python executable that has databricks-connect.
Uses `dbx-connect-venv' if set; otherwise creates a managed
environment with uv after confirmation."
  (if dbx-connect-venv
      (let ((py (dbx-venv-python dbx-connect-venv)))
        (if (file-executable-p py) (funcall callback py)
          (user-error "No Python in dbx-connect-venv: %s" py)))
    (unless (executable-find dbx-uv-program)
      (user-error "uv not found; install it from https://docs.astral.sh/uv/ or set `dbx-connect-venv'"))
    (dbx--venv-spec
     (lambda (spec)
       (let* ((dir (dbx--managed-venv-dir (car spec)))
              (py (dbx-venv-python dir))
              (marker (expand-file-name ".dbx-ready" dir)))
         (if (file-exists-p marker)
             (funcall callback py)
           (if (not (y-or-n-p (format "Create a Python %s environment with databricks-connect==%s in %s using uv? "
                                      (cdr spec) (car spec) (abbreviate-file-name dir))))
               (user-error "No Python environment")
             (dbx--venv-create dir (car spec) (cdr spec)
                               (lambda () (write-region "" nil marker) (funcall callback py)))))))
     ctx)))

(defun dbx--venv-create (dir spec python done)
  "Create venv DIR with PYTHON and databricks-connect SPEC, then call DONE."
  (let ((buf (get-buffer-create "*dbx venv*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t)) (erase-buffer))
      (special-mode))
    (display-buffer buf)
    (make-directory (file-name-directory (directory-file-name dir)) t)
    (cl-labels
        ((step (cmds)
           (if (null cmds)
               (progn (dbx--venv-log buf "Done.\n") (funcall done))
             (let ((cmd (car cmds)))
               (dbx--venv-log buf (concat "$ " (mapconcat #'shell-quote-argument cmd " ") "\n"))
               (make-process
                :name "dbx-uv" :buffer buf :noquery t :connection-type 'pipe
                :command cmd
                :filter (lambda (_p s) (dbx--venv-log buf s))
                :sentinel (lambda (p _)
                            (unless (process-live-p p)
                              (if (zerop (process-exit-status p))
                                  (step (cdr cmds))
                                (dbx--venv-log buf "FAILED\n")
                                (message "dbx: environment creation failed, see *dbx venv*")))))))))
      (step (list (list dbx-uv-program "venv" "--python" python dir)
                  (append (list dbx-uv-program "pip" "install" "--python" (dbx-venv-python dir)
                                (concat "databricks-connect==" spec))
                          dbx-venv-extra-packages))))))

(defun dbx--venv-log (buf text)
  (when (buffer-live-p buf)
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (goto-char (point-max))
        (insert (replace-regexp-in-string "\r" "\n" text))))))

;;;###autoload
(defun dbx-venv-recreate ()
  "Delete and recreate the managed environment for the current context."
  (interactive)
  (when dbx-connect-venv (user-error "dbx-connect-venv is set; dbx does not manage it"))
  (dbx--venv-spec
   (lambda (spec)
     (let ((dir (dbx--managed-venv-dir (car spec))))
       (when (and (file-directory-p dir) (dbx-confirm (format "Delete %s?" dir)))
         (delete-directory dir t))
       (dbx-venv-ensure (lambda (py) (message "dbx: ready: %s" py)))))))

(defun dbx-package-python-dir ()
  "Directory of the bundled Python package."
  (expand-file-name "python" (file-name-directory (or (locate-library "dbx")
                                                      load-file-name buffer-file-name))))

(provide 'dbx-venv)
;;; dbx-venv.el ends here
