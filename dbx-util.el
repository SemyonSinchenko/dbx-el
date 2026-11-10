;;; dbx-util.el --- Shared helpers for dbx  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; Glyphs, status mapping, text bars, sparklines, time and duration
;; formatting, and small JSON accessors shared by all dbx modules.

;;; Code:

(require 'cl-lib)
(require 'subr-x)

(defgroup dbx nil
  "Databricks workbench."
  :group 'tools
  :prefix "dbx-")

;;;; Faces

(defface dbx-success '((t :inherit success))
  "Face for succeeded states." :group 'dbx)
(defface dbx-failed '((t :inherit error))
  "Face for failed states." :group 'dbx)
(defface dbx-running '((((background dark)) :foreground "#6cb6ff")
                       (t :foreground "#0969da"))
  "Face for running states." :group 'dbx)
(defface dbx-waiting '((t :inherit shadow))
  "Face for pending and queued states." :group 'dbx)
(defface dbx-blocked '((((background dark)) :foreground "#f0883e")
                       (t :foreground "#bc4c00"))
  "Face for blocked states." :group 'dbx)
(defface dbx-skipped '((t :inherit shadow))
  "Face for skipped and cancelled states." :group 'dbx)
(defface dbx-retried '((t :inherit dbx-success :underline (:color "goldenrod")))
  "Face for states that succeeded after a retry." :group 'dbx)
(defface dbx-unknown '((t :inherit warning))
  "Face for states dbx does not recognise." :group 'dbx)
(defface dbx-heading '((t :inherit bold :height 1.05))
  "Face for section headings." :group 'dbx)
(defface dbx-title '((t :inherit bold :height 1.2))
  "Face for page titles." :group 'dbx)
(defface dbx-key '((t :inherit font-lock-keyword-face))
  "Face for labels in page headers." :group 'dbx)
(defface dbx-dim '((t :inherit shadow))
  "Face for secondary text." :group 'dbx)

;;;; Glyphs

(defcustom dbx-glyph-style 'auto
  "Glyph set used in pages, lists and picker annotations.
`unicode' uses symbols such as ✔ and ✘, `ascii' uses plain
characters, `auto' picks `unicode' when the default font can
display it."
  :type '(choice (const auto) (const unicode) (const ascii))
  :group 'dbx)

(defconst dbx--glyphs-unicode
  '((success . "✔") (failed . "✘") (running . "⟳") (waiting . "○")
    (blocked . "■") (skipped . "⊘") (cancelled . "⊘") (absent . "·")
    (unknown . "?") (warning . "▲") (on . "●") (off . "○")
    (wh-on . "◆") (wh-off . "◇") (star . "★") (arrow . "▶")
    (null . "∅") (ellipsis . "…"))
  "Single-width Unicode glyphs.  Emoji are avoided on purpose.")

(defconst dbx--glyphs-ascii
  '((success . "+") (failed . "x") (running . ">") (waiting . "o")
    (blocked . "#") (skipped . "-") (cancelled . "-") (absent . ".")
    (unknown . "?") (warning . "!") (on . "*") (off . "o")
    (wh-on . "W") (wh-off . "w") (star . "*") (arrow . ">")
    (null . "NULL") (ellipsis . "~"))
  "ASCII fallback glyphs.")

(defvar dbx--glyph-style-resolved nil)

(defun dbx--glyph-set ()
  "Return the active glyph alist."
  (let ((style (if (eq dbx-glyph-style 'auto)
                   (or dbx--glyph-style-resolved
                       (setq dbx--glyph-style-resolved
                             (if (or noninteractive
                                     (cl-every (lambda (c) (char-displayable-p c))
                                               '(?✔ ?✘ ?⟳ ?⊘ ?▁ ?█)))
                                 'unicode 'ascii)))
                 dbx-glyph-style)))
    (if (eq style 'ascii) dbx--glyphs-ascii dbx--glyphs-unicode)))

(defun dbx-glyph (name)
  "Return the glyph string for NAME."
  (or (alist-get name (dbx--glyph-set)) "?"))

(defconst dbx--status-faces
  '((success . dbx-success) (failed . dbx-failed) (running . dbx-running)
    (waiting . dbx-waiting) (blocked . dbx-blocked) (skipped . dbx-skipped)
    (cancelled . dbx-skipped) (absent . dbx-dim) (unknown . dbx-unknown)
    (retried . dbx-retried) (on . dbx-success) (off . dbx-dim)
    (wh-on . dbx-success) (wh-off . dbx-dim) (warning . dbx-unknown)))

(defun dbx-status-face (status)
  "Face for normalized STATUS symbol."
  (alist-get status dbx--status-faces 'dbx-unknown))

(defun dbx-status-glyph (status &optional face)
  "Propertized glyph for normalized STATUS, using FACE if given."
  (let ((glyph (dbx-glyph (if (eq status 'retried) 'success status))))
    (propertize glyph 'face (or face (dbx-status-face status)))))

;;;; JSON helpers

(defun dbx-get (obj &rest path)
  "Follow PATH (symbols or integers) through alist/list OBJ."
  (let ((v obj))
    (dolist (k path v)
      (setq v (cond ((null v) nil)
                    ((integerp k) (and (listp v) (nth k v)))
                    ((and (listp v) (consp (car-safe v)))
                     (alist-get k v))
                    (t nil))))))

(defun dbx-json-parse-string (string)
  "Parse JSON STRING the way all dbx modules expect."
  (json-parse-string string :object-type 'alist :array-type 'list
                     :null-object nil :false-object :false))

(defun dbx-json-encode (obj)
  "Encode OBJ (alists, lists as arrays) to a JSON string."
  (json-serialize obj :null-object :null :false-object :false))

(defun dbx-truthy (v)
  "Non-nil if JSON value V is true."
  (and v (not (eq v :false))))

;;;; Status normalization

(defun dbx-run-status (obj)
  "Normalize the state of a job run or task OBJ to a symbol.
Uses `status' (state + termination_details.code) first and falls
back to the legacy `state' object.  Mapping by `code', never by
`type': skipped tasks report type SUCCESS."
  (let* ((st (dbx-get obj 'status 'state))
         (code (dbx-get obj 'status 'termination_details 'code))
         (lc (dbx-get obj 'state 'life_cycle_state))
         (rs (dbx-get obj 'state 'result_state)))
    (cond
     (st
      (pcase st
        ("RUNNING" 'running)
        ((or "PENDING" "QUEUED") 'waiting)
        ("BLOCKED" 'blocked)
        ("TERMINATING" 'running)
        ("TERMINATED"
         (pcase code
           ("SUCCESS" 'success)
           ((or "SKIPPED" "EXCLUDED") 'skipped)
           ((or "CANCELED" "CANCELLED" "USER_CANCELED") 'cancelled)
           ('nil 'unknown)
           (_ 'failed)))
        (_ 'unknown)))
     (lc
      (pcase lc
        ((or "RUNNING" "TERMINATING") 'running)
        ((or "PENDING" "QUEUED" "WAITING_FOR_RETRY") 'waiting)
        ("BLOCKED" 'blocked)
        ("SKIPPED" 'skipped)
        ((or "TERMINATED" "INTERNAL_ERROR")
         (pcase rs
           ((or "SUCCESS" "SUCCESS_WITH_FAILURES") 'success)
           ((or "EXCLUDED" "UPSTREAM_CANCELED" "SKIPPED") 'skipped)
           ((or "CANCELED" "CANCELLED") 'cancelled)
           ('nil (if (equal lc "INTERNAL_ERROR") 'failed 'unknown))
           (_ 'failed)))
        (_ 'unknown)))
     (t 'absent))))

(defun dbx-run-status-label (obj)
  "Human label for job run or task OBJ."
  (pcase (dbx-run-status obj)
    ('success "Succeeded") ('failed "Failed") ('running "Running")
    ('waiting "Pending") ('blocked "Blocked") ('skipped "Skipped")
    ('cancelled "Cancelled") ('absent "—")
    (_ (or (dbx-get obj 'status 'state)
           (dbx-get obj 'state 'life_cycle_state) "?"))))

(defun dbx-cluster-status (state)
  "Normalize cluster STATE string."
  (pcase state
    ("RUNNING" 'on)
    ((or "PENDING" "RESTARTING" "RESIZING" "TERMINATING") 'running)
    ("TERMINATED" 'off)
    ("ERROR" 'failed)
    (_ 'unknown)))

(defun dbx-warehouse-status (state)
  "Normalize SQL warehouse STATE string."
  (pcase state
    ("RUNNING" 'wh-on)
    ((or "STARTING" "STOPPING") 'running)
    ((or "STOPPED" "DELETED") 'wh-off)
    (_ 'unknown)))

(defun dbx-pipeline-status (state)
  "Normalize pipeline, update or flow STATE string."
  (pcase state
    ((or "COMPLETED" "IDLE") 'success)
    ("FAILED" 'failed)
    ((or "CANCELED" "CANCELLED" "STOPPED") 'cancelled)
    ((or "SKIPPED" "EXCLUDED") 'skipped)
    ((or "RUNNING" "STARTING" "INITIALIZING" "SETTING_UP_TABLES"
         "RESETTING" "STOPPING" "PLANNING")
     'running)
    ((or "QUEUED" "CREATED" "WAITING_FOR_RESOURCES" "IDLE_WAITING") 'waiting)
    ("DELETED" 'off)
    (_ 'unknown)))

;;;; Time and duration

(defun dbx-ms-to-time (ms)
  "Convert epoch milliseconds MS to a Lisp time, or nil for 0/nil."
  (and (numberp ms) (> ms 0) (seconds-to-time (/ ms 1000.0))))

(defun dbx-format-time (ms &optional fmt)
  "Format epoch milliseconds MS with FMT (default \"%b %d %H:%M\")."
  (if-let* ((time (dbx-ms-to-time ms)))
      (format-time-string (or fmt "%b %d %H:%M") time)
    "—"))

(defun dbx-format-duration (ms)
  "Format a duration of MS milliseconds like \"1h 37m 59s\"."
  (cond
   ((not (numberp ms)) "—")
   ((< ms 1000) (format "%dms" (round ms)))
   (t
    (let* ((s (round (/ ms 1000.0)))
           (h (/ s 3600)) (m (/ (% s 3600) 60)) (sec (% s 60)))
      (cond ((> h 0) (format "%dh %02dm %02ds" h m sec))
            ((> m 0) (format "%dm %02ds" m sec))
            (t (format "%ds" sec)))))))

(defun dbx-format-ago (ms)
  "Relative age of epoch milliseconds MS, like \"5m ago\"."
  (if (not (dbx-ms-to-time ms)) "—"
    (let ((s (- (float-time) (/ ms 1000.0))))
      (cond ((< s 60) "just now")
            ((< s 3600) (format "%dm ago" (/ s 60)))
            ((< s 86400) (format "%dh ago" (/ s 3600)))
            (t (format "%dd ago" (/ s 86400)))))))

(defun dbx-now-ms ()
  "Current time in epoch milliseconds."
  (round (* 1000 (float-time))))

(defun dbx-task-duration (task)
  "Duration of TASK in ms: end - start, or now - start while running."
  (let ((start (alist-get 'start_time task))
        (end (alist-get 'end_time task)))
    (cond ((not (and (numberp start) (> start 0))) nil)
          ((and (numberp end) (> end 0)) (- end start))
          (t (- (dbx-now-ms) start)))))

(defun dbx-run-duration (run)
  "Duration of job RUN in ms."
  (or (let ((d (alist-get 'run_duration run))) (and (numberp d) (> d 0) d))
      (let ((d (+ (or (alist-get 'setup_duration run) 0)
                  (or (alist-get 'execution_duration run) 0)
                  (or (alist-get 'cleanup_duration run) 0))))
        (and (> d 0) d))
      (dbx-task-duration run)))

;;;; Quartz cron

(defun dbx-describe-quartz (expr &optional tz)
  "Describe Quartz cron EXPR in words for common patterns.
TZ is appended when given.  Unknown patterns return EXPR."
  (let* ((f (split-string (or expr "") " +" t))
         (desc
          (pcase f
            (`(,s ,m "*" "*" "*" ,(or "?" "*") . ,_)
             (and (string-match-p "\\`[0-9]+\\'" m) (equal s "0")
                  (format "At %s minutes past the hour" m)))
            (`(,s ,m ,h "*" "*" ,(or "?" "*") . ,_)
             (and (string-match-p "\\`[0-9]+\\'" m)
                  (string-match-p "\\`[0-9]+\\'" h) (equal s "0")
                  (format "Daily at %02d:%02d" (string-to-number h)
                          (string-to-number m))))
            (`(,s ,m ,h "?" "*" ,dow . ,_)
             (and (string-match-p "\\`[0-9]+\\'" m)
                  (string-match-p "\\`[0-9]+\\'" h) (equal s "0")
                  (format "Weekly on %s at %02d:%02d" dow
                          (string-to-number h) (string-to-number m))))
            (`(,s ,m ,h ,dom "*" "?" . ,_)
             (and (string-match-p "\\`[0-9]+\\'" m)
                  (string-match-p "\\`[0-9]+\\'" h)
                  (string-match-p "\\`[0-9]+\\'" dom) (equal s "0")
                  (format "Monthly on day %s at %02d:%02d" dom
                          (string-to-number h) (string-to-number m)))))))
    (concat (or desc expr) (if tz (format " (%s)" tz) ""))))

;;;; Text layout

(defun dbx-pad (string width &optional right)
  "Pad or truncate STRING to WIDTH columns; RIGHT aligns right."
  (let* ((s (or string ""))
         (s (if (> (string-width s) width)
                (truncate-string-to-width s width nil nil (dbx-glyph 'ellipsis))
              s))
         (pad (make-string (max 0 (- width (string-width s))) ?\s)))
    (if right (concat pad s) (concat s pad))))

(defconst dbx--eighths-v [" " "▁" "▂" "▃" "▄" "▅" "▆" "▇" "█"])
(defconst dbx--eighths-h ["" "▏" "▎" "▍" "▌" "▋" "▊" "▉" "█"])

(defun dbx--ascii-p () (eq (dbx--glyph-set) dbx--glyphs-ascii))

(defun dbx-hbar (value max width)
  "Horizontal bar for VALUE out of MAX in at most WIDTH columns."
  (if (or (not (numberp value)) (not (numberp max)) (<= max 0)) ""
    (let* ((eighths (round (* 8.0 width (/ (float (min value max)) max))))
           (full (/ eighths 8)) (rem (% eighths 8)))
      (if (dbx--ascii-p)
          (make-string (max (if (> value 0) 1 0) (round (/ eighths 8.0))) ?=)
        (concat (apply #'concat (make-list full "█"))
                (aref dbx--eighths-h rem))))))

(defun dbx-sparkline (values)
  "One-row sparkline for numeric VALUES (nil entries become spaces)."
  (let ((mx (apply #'max 1 (delq nil (mapcar (lambda (v) (and (numberp v) v)) values)))))
    (mapconcat
     (lambda (v)
       (cond ((not (numberp v)) " ")
             ((dbx--ascii-p) (if (> v (/ mx 2)) ":" "."))
             (t (aref dbx--eighths-v (max 1 (min 8 (ceiling (* 8.0 (/ (float v) mx)))))))))
     values "")))

(defun dbx-vbars (values height)
  "Vertical bar chart rows (top first) for VALUES over HEIGHT rows.
Each value takes one column.  Return a list of HEIGHT strings."
  (let* ((mx (apply #'max 1 (delq nil (mapcar (lambda (v) (and (numberp v) v)) values))))
         (levels (mapcar (lambda (v)
                           (if (numberp v)
                               (max 1 (round (* 8.0 height (/ (float v) mx))))
                             0))
                         values)))
    (cl-loop for row from (1- height) downto 0
             collect
             (mapconcat
              (lambda (lv)
                (let ((fill (- lv (* row 8))))
                  (cond ((<= fill 0) " ")
                        ((dbx--ascii-p) "|")
                        ((>= fill 8) "█")
                        (t (aref dbx--eighths-v fill)))))
              levels ""))))

(defun dbx-topo-sort (nodes edges)
  "Topologically sort NODES (strings) given EDGES ((FROM . TO) ...).
Ties keep the original order.  Cycles are appended at the end."
  (let ((indeg (make-hash-table :test #'equal))
        (out (make-hash-table :test #'equal))
        result queue)
    (dolist (n nodes) (puthash n 0 indeg))
    (dolist (e edges)
      (when (and (gethash (car e) indeg) (gethash (cdr e) indeg))
        (puthash (cdr e) (1+ (gethash (cdr e) indeg)) indeg)
        (push (cdr e) (gethash (car e) out))))
    (setq queue (cl-remove-if-not (lambda (n) (zerop (gethash n indeg))) nodes))
    (while queue
      (let ((n (pop queue)))
        (push n result)
        (dolist (m (reverse (gethash n out)))
          (puthash m (1- (gethash m indeg)) indeg)
          (when (zerop (gethash m indeg))
            (setq queue (append queue (list m)))))))
    (setq result (nreverse result))
    (append result (cl-remove-if (lambda (n) (member n result)) nodes))))

(defun dbx-url-encode-params (params)
  "Encode PARAMS alist ((KEY . VALUE) ...) as a query string."
  (mapconcat (lambda (p)
               (concat (url-hexify-string (format "%s" (car p))) "="
                       (url-hexify-string (format "%s" (cdr p)))))
             (cl-remove-if (lambda (p) (null (cdr p))) params)
             "&"))

(autoload 'url-hexify-string "url-util")

(provide 'dbx-util)
;;; dbx-util.el ends here
