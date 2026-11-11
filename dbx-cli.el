;;; dbx-cli.el --- Async Databricks CLI runner  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; Every control-plane call goes through the `databricks' CLI, run
;; asynchronously.  REST resources are called with `databricks api
;; get|post PATH', which keeps paging under our control and returns the
;; documented REST shapes.  CLI verbs are used for bundles and auth.

;;; Code:

(require 'dbx-util)
(require 'dbx-context)

(defcustom dbx-cli-program "databricks"
  "The Databricks CLI executable."
  :type 'string :group 'dbx)

(defcustom dbx-cli-max-processes 4
  "Maximum number of concurrent CLI processes."
  :type 'integer :group 'dbx)

(defcustom dbx-cli-min-version "1.25.0"
  "Minimum supported Databricks CLI version."
  :type 'string :group 'dbx)

(defvar dbx--cli-running 0)
(defvar dbx--cli-queue nil)

(define-error 'dbx-cli-error "Databricks CLI error")

(defun dbx--cli-default-error (msg)
  (message "dbx: %s" msg))

(defun dbx--local-directory (dir)
  "DIR if local, else the home directory (dbx never runs remotely)."
  (if (and dir (not (file-remote-p dir))) dir (expand-file-name "~/")))

(defun dbx--cli-error-message (stderr stdout code)
  (let* ((text (string-trim (if (string-empty-p (string-trim stderr)) stdout stderr)))
         (line (or (cl-find-if (lambda (l) (string-match-p "\\`Error:" l))
                               (split-string text "\n"))
                   text)))
    (if (string-empty-p line)
        (format "CLI exited with code %s" code)
      (string-remove-prefix "Error: " line))))

(cl-defun dbx-cli (args &key callback error-callback context (json t) directory
                        no-profile)
  "Run the Databricks CLI with ARGS asynchronously.
CALLBACK receives the parsed JSON (or the raw output when JSON is
nil).  ERROR-CALLBACK receives an error message string.  CONTEXT
\(a name or plist) defaults to the active context.  Both callbacks
run with CONTEXT active (see `dbx-with-context'), so calls and pages
they start use it too, whatever buffer is current.  DIRECTORY is
the working directory.  NO-PROFILE omits --profile (for commands
that reject it)."
  (let ((job (list :args args :callback callback
                   :error-callback (or error-callback #'dbx--cli-default-error)
                   :context (if context (dbx--resolve-context context) (dbx-context))
                   :json json
                   :directory (dbx--local-directory (or directory default-directory))
                   :no-profile no-profile)))
    (if (< dbx--cli-running dbx-cli-max-processes)
        (dbx--cli-start job)
      (setq dbx--cli-queue (append dbx--cli-queue (list job))))
    nil))

(defun dbx--cli-command (job)
  (append (list dbx-cli-program)
          (plist-get job :args)
          (when (plist-get job :json) '("-o" "json"))
          (unless (plist-get job :no-profile)
            (list "--profile" (dbx-context-profile (plist-get job :context))))))

(defun dbx--cli-start (job)
  (cl-incf dbx--cli-running)
  (let* ((out (generate-new-buffer " *dbx-cli*"))
         (err (generate-new-buffer " *dbx-cli-err*"))
         (default-directory (plist-get job :directory))
         (process-environment (dbx-context-env (plist-get job :context))))
    (condition-case e
        (make-process
         :name "dbx-cli" :buffer out :stderr err
         :command (dbx--cli-command job)
         :connection-type 'pipe :noquery t :coding 'utf-8
         :sentinel (lambda (proc _event)
                     (unless (process-live-p proc)
                       (dbx--cli-finish proc job out err))))
      (error
       (cl-decf dbx--cli-running)
       (kill-buffer out) (kill-buffer err)
       (dbx--cli-call job :error-callback
                      (format "cannot run %s: %s" dbx-cli-program (error-message-string e)))
       (dbx--cli-next)))))

(defun dbx--cli-call (job key arg)
  "Call JOB's KEY callback with ARG in JOB's context."
  (when-let* ((fn (plist-get job key)))
    (dbx-with-context (plist-get job :context)
      (funcall fn arg))))

(cl-defun dbx--cli-finish (proc job out err)
  (when-let* ((ep (get-buffer-process err)))
    (while (accept-process-output ep 0.05)))
  (cl-decf dbx--cli-running)
  (let ((code (process-exit-status proc))
        (stdout (with-current-buffer out (buffer-string)))
        (stderr (with-current-buffer err (buffer-string))))
    (kill-buffer out)
    (when (buffer-live-p err) (kill-buffer err))
    (dbx--cli-next)
    (if (/= code 0)
        (dbx--cli-call job :error-callback (dbx--cli-error-message stderr stdout code))
      (let ((result (if (plist-get job :json)
                        (if (string-empty-p (string-trim stdout)) nil
                          (condition-case e (dbx-json-parse-string stdout)
                            (error (dbx--cli-call job :error-callback
                                                  (format "invalid JSON from CLI: %s"
                                                          (error-message-string e)))
                                   (cl-return-from dbx--cli-finish nil))))
                      stdout)))
        (dbx--cli-call job :callback result)))))

(defun dbx--cli-next ()
  (when (and dbx--cli-queue (< dbx--cli-running dbx-cli-max-processes))
    (dbx--cli-start (pop dbx--cli-queue))))

(defun dbx-cli-sync (args &rest keys)
  "Run the CLI with ARGS and KEYS like `dbx-cli', waiting for the result.
Signals `dbx-cli-error'.  Only for tests and the gptel key function."
  (let (done result error)
    (apply #'dbx-cli args
           :callback (lambda (r) (setq result r done t))
           :error-callback (lambda (m) (setq error m done t))
           keys)
    (while (not done) (accept-process-output nil 0.05))
    (if error (signal 'dbx-cli-error (list error)) result)))

;;;; REST through `databricks api'

(cl-defun dbx-api (method path &key params body callback error-callback context)
  "Call REST PATH with METHOD (\"get\" or \"post\") through the CLI.
PARAMS is an alist turned into the query string, BODY an object
encoded as JSON.  The body goes through a temporary file, so long
SQL never appears on a command line."
  (let* ((query (and params (dbx-url-encode-params params)))
         (full (if (and query (not (string-empty-p query)))
                   (concat path (if (string-search "?" path) "&" "?") query)
                 path))
         (file (and body (make-temp-file "dbx-body" nil ".json"
                                         (dbx-json-encode body))))
         (cleanup (lambda () (when file (ignore-errors (delete-file file))))))
    (dbx-cli (append (list "api" method full)
                     (and file (list "--json" (concat "@" file))))
             :context context
             :callback (lambda (r) (funcall cleanup) (when callback (funcall callback r)))
             :error-callback (lambda (m) (funcall cleanup)
                               (funcall (or error-callback #'dbx--cli-default-error) m)))))

(cl-defun dbx-api-pages (path &key params (items-key 'items) (max-items 1000)
                              callback error-callback context
                              (token-param 'page_token))
  "GET PATH page by page, collecting ITEMS-KEY until MAX-ITEMS.
Call CALLBACK with the list of items.  Paging follows
`next_page_token' only (`has_more' is unreliable)."
  (let ((acc nil))
    (cl-labels
        ((fetch (token)
           (dbx-api "get" path
                    :params (append params (and token (list (cons token-param token))))
                    :context context :error-callback error-callback
                    :callback
                    (lambda (r)
                      (setq acc (append acc (alist-get items-key r)))
                      (let ((next (alist-get 'next_page_token r)))
                        (if (and next (not (string-empty-p next))
                                 (< (length acc) max-items))
                            (fetch next)
                          (funcall callback (seq-take acc max-items))))))))
      (fetch nil))))

;;;; Waiting for async results in interactive commands

(defun dbx-await (fn &optional what)
  "Call FN with a callback and wait for its value; show WHAT while waiting.
FN receives (CALLBACK ERROR-CALLBACK).  \\[keyboard-quit] aborts."
  (let (done value err)
    (funcall fn (lambda (v) (setq value v done t))
             (lambda (m) (setq err m done t)))
    (when what (message "dbx: loading %s…" what))
    (while (not done) (accept-process-output nil 0.05))
    (when what (message nil))
    (if err (user-error "dbx: %s" err) value)))

;;;; Cache

(defvar dbx--cache (make-hash-table :test #'equal))

(defun dbx-cached (key ttl fetch callback &optional force)
  "Call CALLBACK with a cached value for KEY or fetch it.
FETCH is called with a continuation.  TTL is in seconds.  FORCE
bypasses the cache."
  (let* ((k (cons (dbx-context-name) key))
         (hit (gethash k dbx--cache)))
    (if (and hit (not force) (< (- (float-time) (car hit)) ttl))
        (funcall callback (cdr hit))
      (funcall fetch (lambda (v)
                       (puthash k (cons (float-time) v) dbx--cache)
                       (funcall callback v))))))

(defun dbx-cache-clear ()
  "Forget all cached workspace data."
  (interactive)
  (clrhash dbx--cache))

(defun dbx-confirm (prompt)
  "Ask PROMPT with `yes-or-no-p'.  Used for destructive actions."
  (yes-or-no-p (concat prompt " ")))

(provide 'dbx-cli)
;;; dbx-cli.el ends here
