;;; dbx-server.el --- Companion Python server, REPL and code sending  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; One companion server per Emacs runs user code with Databricks
;; Connect.  Transport: JSON-RPC 2.0 over the server's stdio
;; (`jsonrpc.el', events buffer disabled).  The REPL buffer is a comint
;; buffer whose input becomes `execute' requests; Python buffers send
;; code with `dbx-python-mode'.  Note that `jsonrpc.el' delivers JSON
;; objects as plists.

;;; Code:

(require 'jsonrpc)
(require 'comint)
(require 'dbx-util)
(require 'dbx-context)
(require 'dbx-venv)

(declare-function python-shell-buffer-substring "python")
(declare-function python-nav-beginning-of-defun "python")
(declare-function python-nav-end-of-defun "python")

(defcustom dbx-repl-prompt ">>> "
  "Prompt of the dbx REPL."
  :type 'string :group 'dbx)

(defvar dbx-server--conn nil "The jsonrpc connection to the companion server.")
(defvar dbx-server--session nil "Plist from the last session/state notification.")
(defvar dbx-server--jobs (make-hash-table) "REQ -> plist (:status :origin :start).")
(defvar dbx-server--progress (make-hash-table :test #'equal) "OP-ID -> progress plist.")
(defvar dbx-server--context-name nil)

;;;; Lifecycle

(defun dbx-server-live-p ()
  "Non-nil if the companion server is running."
  (and dbx-server--conn (jsonrpc-running-p dbx-server--conn)))

;;;###autoload
(defun dbx-server-start (&optional callback)
  "Start the companion server for the current context, then call CALLBACK."
  (interactive)
  (if (dbx-server-live-p)
      (when callback (funcall callback))
    (let ((ctx (dbx-context)))
      (dbx-venv-ensure
       (lambda (python) (dbx-server--launch python ctx callback))
       ctx))))

(defun dbx-server--launch (python ctx callback)
  (let* ((default-directory (dbx--local-directory default-directory))
         (pydir (dbx-package-python-dir))
         (process-environment
          (append (list (concat "PYTHONPATH=" pydir
                                (let ((old (getenv "PYTHONPATH"))) (if old (concat path-separator old) "")))
                        "PYTHONUNBUFFERED=1" "PYTHONIOENCODING=utf-8")
                  (dbx-context-env ctx t)))
         (proc (make-process
                :name "dbx-server" :command (list python "-m" "dbx_emacs" "serve")
                :connection-type 'pipe :coding 'utf-8-emacs-unix :noquery t
                :stderr (get-buffer-create "*dbx server stderr*"))))
    (setq dbx-server--context-name (plist-get ctx :name)
          dbx-server--session '(:state "starting"))
    (clrhash dbx-server--jobs)
    (setq dbx-server--conn
          (make-instance 'jsonrpc-process-connection
                         :name "dbx" :process proc
                         :events-buffer-config '(:size 0)
                         :notification-dispatcher #'dbx-server--notification
                         :request-dispatcher #'dbx-server--request
                         :on-shutdown #'dbx-server--on-shutdown))
    (jsonrpc-async-request dbx-server--conn :initialize
                           (list :protocol 1 :clientVersion "0.1.0")
                           :success-fn (lambda (_)
                                         (dbx-repl--ensure-buffer)
                                         (dbx-repl--emit (format "dbx server started (%s, context %s)\n"
                                                                 python (plist-get ctx :name)))
                                         (when callback (funcall callback)))
                           :error-fn (lambda (e) (message "dbx: server failed: %S" e))
                           :timeout 60)
    (force-mode-line-update t)))

(defun dbx-server-stop ()
  "Stop the companion server."
  (interactive)
  (when (dbx-server-live-p)
    (ignore-errors (jsonrpc-shutdown dbx-server--conn t)))
  (setq dbx-server--conn nil dbx-server--session nil)
  (force-mode-line-update t))

(defun dbx-server-restart ()
  "Restart the companion server (all Python state is lost)."
  (interactive)
  (when (or (not (dbx-server-live-p))
            (dbx-confirm "Restart the Python server? All Python state is lost."))
    (dbx-server-stop)
    (dbx-server-start)))

(defun dbx-server--on-shutdown (_conn)
  (maphash (lambda (req job)
             (unless (member (plist-get job :status) '("ok" "error" "interrupted"))
               (puthash req (plist-put job :status "unknown") dbx-server--jobs)))
           dbx-server--jobs)
  (setq dbx-server--session '(:state "stopped"))
  (dbx-repl--emit "\n[dbx server exited]\n")
  (force-mode-line-update t))

(add-hook 'kill-emacs-hook (lambda () (ignore-errors (dbx-server-stop))))

;;;; Messages from the server

(defun dbx-server--notification (_conn method params)
  (pcase method
    ('output
     (dolist (item (append (plist-get params :items) nil))
       (dbx-repl--emit (plist-get item :text) (equal (plist-get item :stream) "stderr"))))
    ('session/state
     (setq dbx-server--session params)
     (pcase (plist-get params :state)
       ("connected" (dbx-repl--emit (format "[session connected: %s · %s]\n"
                                            (plist-get (plist-get params :target) :host)
                                            (plist-get (plist-get params :target) :compute))))
       ("failed" (dbx-repl--emit (format "[session failed: %s]\n" (plist-get params :error)) t)))
     (force-mode-line-update t))
    ('req/start
     (puthash (plist-get params :req)
              (list :status "running" :origin (plist-get params :origin) :start (dbx-now-ms))
              dbx-server--jobs)
     (force-mode-line-update t))
    ('req/end
     (let* ((req (plist-get params :req))
            (job (gethash req dbx-server--jobs))
            (status (plist-get params :status)))
       (puthash req (plist-put job :status status) dbx-server--jobs)
       (dbx-repl--job-finished req status (plist-get params :error)
                               (and (plist-get job :start) (- (dbx-now-ms) (plist-get job :start)))))
     (clrhash dbx-server--progress)
     (force-mode-line-update t))
    ('progress
     (dolist (item (append (plist-get params :items) nil))
       (if (eq (plist-get item :done) t)
           (remhash (plist-get item :opId) dbx-server--progress)
         (puthash (plist-get item :opId) item dbx-server--progress)))
     (force-mode-line-update t))
    ('cancel/remote nil)
    (_ (message "dbx server: %s %S" method params))))

(defun dbx-server--request (_conn method params)
  (pcase method
    ('input (list :line (read-string (or (plist-get params :prompt) "Python input: "))))
    (_ (signal 'jsonrpc-error `((jsonrpc-error-code . -32601)
                                (jsonrpc-error-message . ,(format "unknown method %s" method)))))))

;;;; Executing code

(defvar dbx-server--last-req nil "Req id of the last submitted job.")

(defun dbx-server-execute (code &optional origin mode)
  "Send CODE (string) with ORIGIN plist (:file :line) in MODE.
MODE is \"exec\" or \"interactive\"."
  (dbx-server-start
   (lambda ()
     (when (and (dbx-context-get :confirm)
                (not (dbx-confirm (format "Run code in context %s?" (dbx-context-get :name)))))
       (user-error "Cancelled"))
     (jsonrpc-async-request
      dbx-server--conn :execute
      (list :code code :mode (or mode "exec") :origin (or origin (list :file "<dbx>" :line 1)))
      :success-fn (lambda (r)
                    (if (eq (plist-get r :incomplete) t)
                        (dbx-repl--incomplete)
                      (setq dbx-server--last-req (plist-get r :req))
                      (puthash (plist-get r :req) (list :status "queued" :origin origin)
                               dbx-server--jobs)))
      :error-fn (lambda (e) (message "dbx: execute failed: %S" e))))))

(defun dbx-server-interrupt (&optional req)
  "Interrupt job REQ (default: the running or last job)."
  (interactive)
  (unless (dbx-server-live-p) (user-error "No dbx server"))
  (let ((req (or req (dbx-server--running-req) dbx-server--last-req)))
    (jsonrpc-async-request dbx-server--conn :interrupt (list :req req)
                           :success-fn (lambda (r) (message "dbx: interrupt %s: %s" req (plist-get r :result))))))

(defun dbx-server--running-req ()
  (let (found)
    (maphash (lambda (req job) (when (equal (plist-get job :status) "running") (setq found req)))
             dbx-server--jobs)
    found))

(defun dbx-server-reconnect ()
  "Recreate the Databricks session in the running server."
  (interactive)
  (jsonrpc-async-request dbx-server--conn :session/reconnect nil :success-fn #'ignore))

;;;; Mode line

(defun dbx-server--mode-line ()
  (when dbx-server--session
    (let* ((st (plist-get dbx-server--session :state))
           (running (dbx-server--running-req))
           (stages (let (acc) (maphash (lambda (_ p) (setq acc (append acc (append (plist-get p :stages) nil))))
                                       dbx-server--progress)
                        acc))
           (total (apply #'+ 0 (mapcar (lambda (s) (or (plist-get s :total) 0)) stages)))
           (done (apply #'+ 0 (mapcar (lambda (s) (or (plist-get s :done) 0)) stages))))
      (concat "py "
              (pcase st
                ("connected" (if running
                                 (propertize (concat (dbx-glyph 'running)
                                                     (if (> total 0)
                                                         (format " %s %d%%" (dbx-hbar done total 4)
                                                                 (/ (* 100 done) total))
                                                       ""))
                                             'face 'dbx-running)
                               (propertize (dbx-glyph 'on) 'face 'dbx-success)))
                ("connecting" (propertize (dbx-glyph 'waiting) 'face 'dbx-waiting))
                ("failed" (propertize (dbx-glyph 'failed) 'face 'dbx-failed))
                (_ (propertize (dbx-glyph 'off) 'face 'dbx-dim)))))))

(add-hook 'dbx-mode-line-extra-functions #'dbx-server--mode-line)

;;;; REPL

(defconst dbx-repl-buffer-name "*dbx repl*")

(define-derived-mode dbx-repl-mode comint-mode "DBX-REPL"
  "REPL for the dbx companion server.  Input is sent as `execute' requests."
  (setq-local comint-prompt-regexp (concat "^" (regexp-quote dbx-repl-prompt)))
  (setq-local comint-input-sender #'dbx-repl--send)
  (setq-local comint-process-echoes nil)
  (keymap-set dbx-repl-mode-map "C-c C-c" #'dbx-server-interrupt))

(defun dbx-repl--ensure-buffer ()
  (let ((buf (get-buffer-create dbx-repl-buffer-name)))
    (with-current-buffer buf
      (unless (derived-mode-p 'dbx-repl-mode)
        (dbx-repl-mode))
      (unless (get-buffer-process buf)
        (make-pipe-process :name "dbx-repl" :buffer buf :noquery t)
        (set-marker (process-mark (get-buffer-process buf)) (point-max))
        (comint-output-filter (get-buffer-process buf) dbx-repl-prompt)))
    buf))

(defvar dbx-repl--pending "" "Accumulated multi-line input.")

(defun dbx-repl--send (_proc input)
  (let ((code (if (string-empty-p dbx-repl--pending) input
                (concat dbx-repl--pending "\n" input))))
    (setq dbx-repl--pending code)
    (dbx-server-execute code (list :file "<repl>" :line 1) "interactive")))

(defun dbx-repl--incomplete ()
  (let ((proc (get-buffer-process (dbx-repl--ensure-buffer))))
    (comint-output-filter proc "... ")))

(defun dbx-repl--emit (text &optional stderr)
  "Insert TEXT into the REPL before the prompt."
  (let* ((buf (dbx-repl--ensure-buffer))
         (proc (get-buffer-process buf)))
    (with-current-buffer buf
      (let ((inhibit-read-only t)
            (text (if stderr (propertize text 'font-lock-face 'dbx-failed) text)))
        (save-excursion
          (goto-char (process-mark proc))
          (forward-line 0)
          (if (looking-at (regexp-quote dbx-repl-prompt))
              (progn (insert text) (set-marker (process-mark proc) (+ (process-mark proc) (length text))))
            (goto-char (process-mark proc))
            (insert text)
            (set-marker (process-mark proc) (point))))))))

(defun dbx-repl--job-finished (req status error duration)
  (setq dbx-repl--pending "")
  (let* ((buf (dbx-repl--ensure-buffer)) (proc (get-buffer-process buf))
         (glyph (pcase status ("ok" 'success) ("error" 'failed) ("interrupted" 'cancelled) (_ 'unknown))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (goto-char (process-mark proc))
        (unless (bolp) (insert "\n"))
        (insert (propertize (format "%s [%s] %s%s\n" (dbx-glyph glyph) req status
                                    (if duration (format " · %s" (dbx-format-duration duration)) ""))
                            'font-lock-face (dbx-status-face glyph)))
        (when (and error (equal status "error")) (ignore error))
        (set-marker (process-mark proc) (point))
        (comint-output-filter proc dbx-repl-prompt)))))

;;;###autoload
(defun dbx-repl ()
  "Show the dbx REPL, starting the server if needed."
  (interactive)
  (dbx-server-start)
  (pop-to-buffer (dbx-repl--ensure-buffer)))

;;;; Sending from Python buffers

(defvar-keymap dbx-python-mode-map
  "C-c C-c" #'dbx-send-buffer
  "C-c C-r" #'dbx-send-region
  "C-M-x" #'dbx-send-defun
  "C-c C-k" #'dbx-server-interrupt
  "C-c C-z" #'dbx-repl
  "C-c C-f" #'dbx-run-file)

;;;###autoload
(define-minor-mode dbx-python-mode
  "Send Python code to the Databricks companion server.

\\{dbx-python-mode-map}"
  :lighter " DBX-Py" :group 'dbx)

(defun dbx--origin (beg)
  (list :file (or buffer-file-name (buffer-name)) :line (line-number-at-pos beg)))

(defun dbx-send-region (beg end)
  "Send the region BEG..END."
  (interactive "r")
  (let ((code (if (fboundp 'python-shell-buffer-substring)
                  (python-shell-buffer-substring beg end t)
                (buffer-substring-no-properties beg end))))
    (dbx-server-execute code (dbx--origin (if (fboundp 'python-shell-buffer-substring) (point-min) beg)))
    (deactivate-mark)))

(defun dbx-send-buffer ()
  "Send the whole buffer."
  (interactive)
  (dbx-server-execute (buffer-substring-no-properties (point-min) (point-max))
                      (dbx--origin (point-min))))

(defun dbx-send-defun ()
  "Send the top-level definition at point."
  (interactive)
  (save-excursion
    (let ((end (progn (python-nav-end-of-defun) (point)))
          (beg (progn (python-nav-beginning-of-defun) (point))))
      (dbx-server-execute (buffer-substring-no-properties beg end) (dbx--origin beg)))))

(defun dbx-run-file (file &optional args)
  "Run FILE with ARGS like `python FILE ARGS' with a Databricks session."
  (interactive (list (or buffer-file-name (read-file-name "Run file: "))
                     (split-string-shell-command (read-string "Arguments: "))))
  (let ((ctx (dbx-context)))
    (dbx-venv-ensure
     (lambda (python)
       (let* ((default-directory (file-name-directory (expand-file-name file)))
              (process-environment
               (append (list (concat "PYTHONPATH=" (dbx-package-python-dir)) "PYTHONUNBUFFERED=1")
                       (dbx-context-env ctx t))))
         (compilation-start (mapconcat #'shell-quote-argument
                                       (append (list python "-m" "dbx_emacs" "run" (expand-file-name file))
                                               args)
                                       " ")
                            nil (lambda (_) (format "*dbx run: %s*" (file-name-nondirectory file))))))
     ctx)))

(provide 'dbx-server)
;;; dbx-server.el ends here
