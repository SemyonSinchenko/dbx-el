;;; dbx-sql.el --- SQL editor with Org-table results  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; `dbx-sql-mode' is a minor mode for `sql-mode' buffers.  Statements
;; run on a SQL warehouse through the Statement Execution API
;; (`databricks api post /api/2.0/sql/statements'), are polled, and
;; their results are rendered as pre-aligned Org tables in a results
;; buffer (`dbx-sql-result-mode').

;;; Code:

(require 'dbx-util)
(require 'dbx-cli)
(require 'dbx-context)

(declare-function dbx--read-warehouse "dbx-compute")
(declare-function dbx-warehouses-fetch "dbx-compute")
(declare-function org-table-get-field "org-table")
(declare-function org-table-export "org-table")
(declare-function org-table-sort-lines "org-table")
(declare-function org-table-to-lisp "org-table")
(declare-function org-at-table-p "org")
(declare-function org-back-to-heading "org")
(declare-function org-next-visible-heading "org")
(declare-function org-previous-visible-heading "org")
(declare-function org-cycle "org-cycle")
(defvar org-startup-folded)

(defcustom dbx-sql-max-rows 2000
  "Maximum rows fetched per statement (sent as row_limit)."
  :type 'integer :group 'dbx)

(defcustom dbx-sql-max-cell-width 60
  "Maximum displayed width of a result cell."
  :type 'integer :group 'dbx)

(defcustom dbx-sql-max-results 30
  "Number of result sections kept in a results buffer."
  :type 'integer :group 'dbx)

(defconst dbx-sql--numeric-types
  '("BYTE" "SHORT" "INT" "LONG" "FLOAT" "DOUBLE" "DECIMAL" "TINYINT" "SMALLINT" "BIGINT")
  "Statement Execution API type names that are right-aligned.")

;;;; Statement splitting

(defun dbx-sql-statement-bounds (text)
  "Return ((START . END) ...) of the statements in TEXT.
Splits on `;' outside strings, quoted identifiers and comments.
Bounds exclude the `;' and surrounding blank space; empty
statements are dropped."
  (let ((i 0) (n (length text)) (start 0) (res nil) (state nil))
    (while (< i n)
      (let ((c (aref text i)) (next (and (< (1+ i) n) (aref text (1+ i)))))
        (pcase state
          ('nil
           (cond ((memq c '(?' ?\" ?`)) (setq state c))
                 ((and (eq c ?-) (eq next ?-)) (setq state 'line))
                 ((and (eq c ?/) (eq next ?*)) (setq state 'block) (cl-incf i))
                 ((eq c ?\;) (push (cons start i) res) (setq start (1+ i)))))
          ('line (when (eq c ?\n) (setq state nil)))
          ('block (when (and (eq c ?*) (eq next ?/)) (setq state nil) (cl-incf i)))
          (_ (cond ((and (eq c ?\\) (not (eq state ?`))) (cl-incf i))
                   ((eq c state)
                    (if (eq next state) (cl-incf i) (setq state nil)))))))
      (cl-incf i))
    (push (cons start n) res)
    (cl-loop for (s . e) in (nreverse res)
             for trimmed = (dbx-sql--trim-bounds text s e)
             when trimmed collect trimmed)))

(defun dbx-sql--trim-bounds (text s e)
  (while (and (< s e) (memq (aref text s) '(?\s ?\t ?\n ?\r))) (cl-incf s))
  (while (and (> e s) (memq (aref text (1- e)) '(?\s ?\t ?\n ?\r))) (cl-decf e))
  (and (< s e)
       (not (string-match-p "\\`\\(?:\\s-\\|--[^\n]*\\(?:\n\\|\\'\\)\\|/\\*\\(?:[^*]\\|\\*[^/]\\)*\\*/\\)*\\'"
                            (substring text s e)))
       (cons s e)))

(defun dbx-sql-statement-at-point ()
  "Return (START . END) buffer positions of the statement at point."
  (let* ((text (buffer-substring-no-properties (point-min) (point-max)))
         (off (- (point) (point-min)))
         (bounds (dbx-sql-statement-bounds text))
         (hit (or (cl-find-if (lambda (b) (and (<= (car b) off) (<= off (1+ (cdr b))))) bounds)
                  (car (last (cl-remove-if (lambda (b) (> (car b) off)) bounds)))
                  (car bounds))))
    (and hit (cons (+ (point-min) (car hit)) (+ (point-min) (cdr hit))))))

(defun dbx-sql-parameter-names (stmt)
  "Names of `:name' parameters in STMT, outside strings and comments.
`::' casts and JSON paths like col:field are not parameters."
  (let ((i 0) (n (length stmt)) (state nil) (names nil))
    (while (< i n)
      (let ((c (aref stmt i)) (next (and (< (1+ i) n) (aref stmt (1+ i))))
            (prev (and (> i 0) (aref stmt (1- i)))))
        (pcase state
          ('nil
           (cond ((memq c '(?' ?\" ?`)) (setq state c))
                 ((and (eq c ?-) (eq next ?-)) (setq state 'line))
                 ((and (eq c ?/) (eq next ?*)) (setq state 'block))
                 ((and (eq c ?:) next (not (eq next ?:))
                       (not (and prev (or (eq prev ?:)
                                          (string-match-p "[]_)`[:alnum:]]" (string prev)))))
                       (string-match-p "[[:alpha:]_]" (string next)))
                  (let ((j (1+ i)))
                    (while (and (< j n) (string-match-p "[[:alnum:]_]" (string (aref stmt j))))
                      (cl-incf j))
                    (push (substring stmt (1+ i) j) names)
                    (setq i (1- j))))))
          ('line (when (eq c ?\n) (setq state nil)))
          ('block (when (and (eq c ?*) (eq next ?/)) (setq state nil)))
          (_ (cond ((eq c ?\\) (cl-incf i))
                   ((eq c state) (setq state nil))))))
      (cl-incf i))
    (delete-dups (nreverse names))))

;;;; Org table rendering

(defun dbx-sql--cell (value)
  "Display string for VALUE, plus the full text when truncated."
  (let* ((s (cond ((null value) (dbx-glyph 'null))
                  ((stringp value) value)
                  (t (format "%s" value))))
         (s (replace-regexp-in-string "[\n\r\t]" " " s))
         (s (replace-regexp-in-string "|" "¦" s)))
    (if (> (string-width s) dbx-sql-max-cell-width)
        (propertize (truncate-string-to-width s dbx-sql-max-cell-width nil nil (dbx-glyph 'ellipsis))
                    'help-echo (if (stringp value) value (format "%s" value)))
      s)))

(defun dbx-sql-org-table (columns rows)
  "Return a pre-aligned Org table string for COLUMNS and ROWS.
COLUMNS is a list of alists with `name' and `type_name'; ROWS a list
of lists of values.  No `org-table-align' pass is needed."
  (let* ((names (mapcar (lambda (c) (dbx-sql--cell (alist-get 'name c))) columns))
         (numeric (mapcar (lambda (c) (member (alist-get 'type_name c) dbx-sql--numeric-types))
                          columns))
         (cells (mapcar (lambda (r) (mapcar #'dbx-sql--cell r)) rows))
         (widths (cl-loop for i from 0 below (length columns)
                          collect (apply #'max 1 (string-width (nth i names))
                                         (mapcar (lambda (r) (string-width (or (nth i r) ""))) cells)))))
    (with-temp-buffer
      (cl-flet ((line (vals &optional header)
                  (insert "|")
                  (cl-loop for v in vals for w in widths for num in numeric do
                           (insert " " (dbx-pad (or v "") w (and num (not header))) " |"))
                  (insert "\n")))
        (line names t)
        (insert "|" (mapconcat (lambda (w) (make-string (+ 2 w) ?-)) widths "+") "|\n")
        (dolist (r cells) (line r)))
      (buffer-string))))

;;;; Editor minor mode

(defvar-local dbx-sql-warehouse nil "SQL warehouse id for this buffer, or nil for the context's.")
(defvar-local dbx-sql-warehouse-name nil)
(defvar-local dbx-sql-catalog nil "Catalog for this buffer, or nil for the context's.")
(defvar-local dbx-sql-schema nil "Schema for this buffer, or nil for the context's.")
(defvar-local dbx-sql--running nil "Plist of the running statement: :id :start.")
(defvar-local dbx-sql--results-buffer nil)

(defvar-keymap dbx-sql-mode-map
  "C-c C-c" #'dbx-sql-execute-statement
  "C-c C-r" #'dbx-sql-execute-region
  "C-c C-b" #'dbx-sql-execute-buffer
  "C-c C-k" #'dbx-sql-cancel
  "C-c C-t" #'dbx-sql-set-target
  "C-c C-s" #'dbx-sql-set-schema
  "C-c C-l" #'dbx-sql-set-row-limit
  "C-c C-h" #'dbx-sql-history
  "C-c C-o" #'dbx-sql-show-results)

;;;###autoload
(define-minor-mode dbx-sql-mode
  "Run SQL against Databricks SQL warehouses.

\\{dbx-sql-mode-map}"
  :lighter " DBX-SQL" :group 'dbx
  (if dbx-sql-mode
      (setq-local header-line-format '(:eval (dbx-sql--header)))
    (kill-local-variable 'header-line-format)))

(defun dbx-sql--warehouse-id ()
  (or dbx-sql-warehouse (dbx-context-get :warehouse)))

(defun dbx-sql--catalog () (or dbx-sql-catalog (dbx-context-get :catalog)))
(defun dbx-sql--schema () (or dbx-sql-schema (dbx-context-get :schema)))

(defun dbx-sql--header ()
  (concat " SQL " (dbx-glyph 'arrow) " "
          (if (dbx-sql--warehouse-id)
              (concat (dbx-glyph 'wh-on) " " (or dbx-sql-warehouse-name (dbx-sql--warehouse-id)))
            (propertize "no warehouse (C-c C-t)" 'face 'dbx-blocked))
          (let ((c (dbx-sql--catalog)) (s (dbx-sql--schema)))
            (if (or c s) (format " · %s%s" (or c "") (if s (concat "." s) "")) ""))
          (format " · limit %d" dbx-sql-max-rows)
          (if dbx-sql--running
              (propertize (format " · %s running %s" (dbx-glyph 'running)
                                  (dbx-format-duration (- (dbx-now-ms) (plist-get dbx-sql--running :start))))
                          'face 'dbx-running)
            "")))

(defun dbx-sql-set-target ()
  "Choose the SQL warehouse for this buffer."
  (interactive)
  (require 'dbx-compute)
  (let ((w (dbx--read-warehouse "SQL warehouse: ")))
    (setq dbx-sql-warehouse (alist-get 'id w)
          dbx-sql-warehouse-name (alist-get 'name w))
    (force-mode-line-update)))

(defun dbx-sql-set-schema (catalog schema)
  "Set CATALOG and SCHEMA for this buffer."
  (interactive (list (read-string "Catalog: " (dbx-sql--catalog))
                     (read-string "Schema: " (dbx-sql--schema))))
  (setq dbx-sql-catalog (and (not (string-empty-p catalog)) catalog)
        dbx-sql-schema (and (not (string-empty-p schema)) schema))
  (force-mode-line-update))

(defun dbx-sql-set-row-limit (n)
  "Set the row limit to N for this session."
  (interactive (list (read-number "Row limit: " dbx-sql-max-rows)))
  (setq dbx-sql-max-rows n)
  (force-mode-line-update t))

;;;###autoload
(defun dbx-sql-new-buffer (&optional text)
  "Open a new SQL buffer, optionally containing TEXT."
  (interactive)
  (let ((buf (generate-new-buffer "*dbx sql*")))
    (with-current-buffer buf
      (sql-mode)
      (dbx-sql-mode 1)
      (when text (insert text)))
    (pop-to-buffer buf)))

(defun dbx-sql-execute-statement ()
  "Run the statement at point."
  (interactive)
  (let ((b (or (dbx-sql-statement-at-point) (user-error "No statement at point"))))
    (pulse-momentary-highlight-region (car b) (cdr b))
    (dbx-sql-execute (buffer-substring-no-properties (car b) (cdr b)))))

(defun dbx-sql-execute-region (beg end)
  "Run all statements between BEG and END."
  (interactive "r")
  (dbx-sql--execute-many (buffer-substring-no-properties beg end)))

(defun dbx-sql-execute-buffer ()
  "Run all statements in the buffer."
  (interactive)
  (dbx-sql--execute-many (buffer-substring-no-properties (point-min) (point-max))))

(defun dbx-sql--execute-many (text)
  (let ((stmts (mapcar (lambda (b) (substring text (car b) (cdr b)))
                       (dbx-sql-statement-bounds text)))
        (buf (current-buffer)))
    (cl-labels ((next ()
                  (when stmts
                    (with-current-buffer buf
                      (dbx-sql-execute (pop stmts) (lambda (ok) (when ok (next))))))))
      (next))))

(defun dbx-sql--ensure-warehouse ()
  (or (dbx-sql--warehouse-id)
      (progn (dbx-sql-set-target) (dbx-sql--warehouse-id))
      (user-error "No SQL warehouse selected")))

(defun dbx-sql-execute (stmt &optional done)
  "Execute STMT on the buffer's warehouse; call DONE with success flag."
  (when dbx-sql--running (user-error "A statement is already running (C-c C-k cancels)"))
  (let* ((wh (dbx-sql--ensure-warehouse))
         (names (dbx-sql-parameter-names stmt))
         (params (mapcar (lambda (nm) `((name . ,nm) (value . ,(read-string (format ":%s = " nm)))))
                         names))
         (editor (current-buffer))
         (start (dbx-now-ms))
         (meta (list :statement stmt :warehouse (or dbx-sql-warehouse-name wh) :start start
                     :editor editor)))
    (when (and (dbx-context-get :confirm)
               (not (dbx-confirm (format "Run on %s (context %s)?" wh (dbx-context-get :name)))))
      (user-error "Cancelled"))
    (setq dbx-sql--running (list :id nil :start start))
    (force-mode-line-update)
    (dbx-api "post" "/api/2.0/sql/statements"
             :body `((warehouse_id . ,wh) (statement . ,stmt)
                     (wait_timeout . "5s") (on_wait_timeout . "CONTINUE")
                     (disposition . "INLINE") (format . "JSON_ARRAY")
                     (row_limit . ,dbx-sql-max-rows)
                     ,@(and (dbx-sql--catalog) `((catalog . ,(dbx-sql--catalog))))
                     ,@(and (dbx-sql--schema) `((schema . ,(dbx-sql--schema))))
                     ,@(and params `((parameters . ,(vconcat params)))))
             :error-callback (lambda (m) (dbx-sql--finish editor meta nil nil m done))
             :callback (lambda (r) (dbx-sql--progress editor meta r done 0.5)))))

(defun dbx-sql--terminal-p (r)
  (member (dbx-get r 'status 'state) '("SUCCEEDED" "FAILED" "CANCELED" "CLOSED")))

(defun dbx-sql--progress (editor meta r done delay)
  (let ((id (alist-get 'statement_id r)))
    (when (buffer-live-p editor)
      (with-current-buffer editor
        (when dbx-sql--running (setq dbx-sql--running (plist-put dbx-sql--running :id id)))
        (force-mode-line-update)))
    (if (dbx-sql--terminal-p r)
        (if (equal (dbx-get r 'status 'state) "SUCCEEDED")
            (dbx-sql--collect editor meta r done)
          (dbx-sql--finish editor meta r nil
                           (or (dbx-get r 'status 'error 'message)
                               (dbx-get r 'status 'state))
                           done))
      (run-at-time delay nil
                   (lambda ()
                     (if (not (and (buffer-live-p editor)
                                   (buffer-local-value 'dbx-sql--running editor)))
                         nil
                       (dbx-api "get" (format "/api/2.0/sql/statements/%s" id)
                                :error-callback (lambda (m) (dbx-sql--finish editor meta nil nil m done))
                                :callback (lambda (r2)
                                            (dbx-sql--progress editor meta r2 done
                                                               (min 2.0 (* delay 1.5)))))))))))

(defun dbx-sql--collect (editor meta r done)
  "Collect chunks of result R up to the row limit, then finish."
  (let ((rows (dbx-get r 'result 'data_array))
        (next (dbx-get r 'result 'next_chunk_internal_link)))
    (cl-labels ((more (link)
                  (if (or (null link) (>= (length rows) dbx-sql-max-rows))
                      (dbx-sql--finish editor meta r (seq-take rows dbx-sql-max-rows) nil done)
                    (dbx-api "get" link
                             :error-callback (lambda (m) (dbx-sql--finish editor meta r rows m done))
                             :callback (lambda (chunk)
                                         (setq rows (append rows (alist-get 'data_array chunk)))
                                         (more (alist-get 'next_chunk_internal_link chunk)))))))
      (more next))))

(defun dbx-sql--finish (editor meta r rows error done)
  (when (buffer-live-p editor)
    (with-current-buffer editor
      (setq dbx-sql--running nil)
      (force-mode-line-update)
      (dbx-sql--append-result editor meta r rows error)))
  (when done (funcall done (null error))))

(defun dbx-sql-cancel ()
  "Cancel the running statement."
  (interactive)
  (let ((id (plist-get dbx-sql--running :id)))
    (setq dbx-sql--running nil)
    (force-mode-line-update)
    (if (not id) (message "dbx: nothing to cancel on the server")
      (dbx-api "post" (format "/api/2.0/sql/statements/%s/cancel" id) :body '()
               :callback (lambda (_) (message "dbx: statement %s cancelled" id))))))

;;;; Results buffer

(defvar-local dbx-sql-result--editor nil)

(defvar-keymap dbx-sql-result-mode-map
  "n" #'dbx-sql-result-next
  "p" #'dbx-sql-result-previous
  "TAB" #'dbx-sql-result-toggle
  "r" #'dbx-sql-result-rerun
  "e" #'dbx-sql-result-export
  "c" #'dbx-sql-result-copy-cell
  "C" #'dbx-sql-result-copy-row
  "y" #'dbx-sql-result-copy-table
  "s" #'dbx-sql-result-sort
  "E" #'dbx-sql-result-editable
  "g" #'dbx-sql-result-rerun
  "q" #'quit-window)

(define-minor-mode dbx-sql-result-mode
  "Read-only view of SQL results as Org tables.

\\{dbx-sql-result-mode-map}"
  :lighter " DBX-Results" :group 'dbx
  (setq buffer-read-only dbx-sql-result-mode))

(defun dbx-sql--results-buffer (editor)
  (let ((buf (get-buffer-create (format "*dbx sql results: %s*" (buffer-name editor)))))
    (with-current-buffer buf
      (unless (derived-mode-p 'org-mode)
        (let ((org-startup-folded nil)) (org-mode))
        (setq-local truncate-lines t)
        (dbx-sql-result-mode 1))
      (setq dbx-sql-result--editor editor))
    buf))

(defun dbx-sql--append-result (editor meta r rows error)
  (let* ((buf (dbx-sql--results-buffer editor))
         (columns (dbx-get r 'manifest 'schema 'columns))
         (truncated (or (dbx-truthy (dbx-get r 'manifest 'truncated))
                        (and rows (>= (length rows) dbx-sql-max-rows))))
         (dur (- (dbx-now-ms) (plist-get meta :start)))
         (stmt (plist-get meta :statement)))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (goto-char (point-max))
        (unless (bolp) (insert "\n"))
        (let ((heading-start (point)))
          (insert "* "
                  (if error
                      (concat (dbx-status-glyph 'failed) " failed")
                    (concat (dbx-status-glyph 'success)
                            (format " %d row%s%s" (length rows) (if (= (length rows) 1) "" "s")
                                    (if truncated " (truncated)" ""))))
                  (format " · %s · %s · %s\n" (dbx-format-duration dur)
                          (plist-get meta :warehouse)
                          (format-time-string "%H:%M:%S")))
          (put-text-property heading-start (point) 'dbx-sql-statement stmt)
          (insert "#+begin_src sql\n" (string-trim stmt) "\n#+end_src\n")
          (if error
              (insert ": " (replace-regexp-in-string "\n" "\n: " error) "\n")
            (when columns
              (insert "# types: "
                      (mapconcat (lambda (c) (format "%s %s" (alist-get 'name c)
                                                     (or (alist-get 'type_text c) (alist-get 'type_name c))))
                                 columns " | ")
                      "\n")
              (insert (dbx-sql-org-table columns rows))))
          (dbx-sql--trim-results)
          (goto-char heading-start))))
    (let ((win (display-buffer buf '(display-buffer-reuse-window display-buffer-below-selected
                                     (window-height . 0.4)))))
      (when win (set-window-point win (with-current-buffer buf (point)))
            (with-selected-window win (recenter 0))))))

(defun dbx-sql--trim-results ()
  (save-excursion
    (goto-char (point-min))
    (let ((count 0))
      (while (re-search-forward "^\\* " nil t) (cl-incf count))
      (when (> count dbx-sql-max-results)
        (goto-char (point-min))
        (dotimes (_ (- count dbx-sql-max-results)) (re-search-forward "^\\* " nil t))
        (when (re-search-forward "^\\* " nil t)
          (delete-region (point-min) (match-beginning 0)))))))

(defun dbx-sql-show-results ()
  "Show the results buffer of this editor."
  (interactive)
  (pop-to-buffer (dbx-sql--results-buffer (current-buffer))))

(defun dbx-sql-result-next () "Next result." (interactive)
       (org-next-visible-heading 1))
(defun dbx-sql-result-previous () "Previous result." (interactive)
       (org-previous-visible-heading 1))
(defun dbx-sql-result-toggle () "Fold or unfold the result at point." (interactive)
       (let ((inhibit-read-only t)) (save-excursion (org-back-to-heading) (org-cycle))))

(defun dbx-sql-result-rerun ()
  "Run the statement of the result at point again in its editor."
  (interactive)
  (let ((stmt (save-excursion (org-back-to-heading) (get-text-property (point) 'dbx-sql-statement)))
        (editor dbx-sql-result--editor))
    (unless (and stmt (buffer-live-p editor)) (user-error "No statement or editor"))
    (with-current-buffer editor (dbx-sql-execute stmt))))

(defun dbx-sql--goto-table ()
  (unless (org-at-table-p)
    (let ((end (save-excursion (or (and (re-search-forward "^\\* " nil t) (point)) (point-max)))))
      (save-restriction
        (narrow-to-region (save-excursion (ignore-errors (org-back-to-heading)) (point)) end)
        (goto-char (point-min))
        (unless (re-search-forward "^|" nil t) (user-error "No table in this result"))
        (forward-line 1))))
  (unless (org-at-table-p) (user-error "No table here")))

(defun dbx-sql-result-export (file format)
  "Export the table at point to FILE in FORMAT."
  (interactive (list (read-file-name "Export to: ")
                     (completing-read "Format: " '("orgtbl-to-csv" "orgtbl-to-tsv" "orgtbl-to-generic") nil t)))
  (save-excursion
    (dbx-sql--goto-table)
    (org-table-export file format)))

(defun dbx-sql-result-copy-cell ()
  "Copy the cell at point."
  (interactive)
  (let ((v (string-trim (substring-no-properties (or (org-table-get-field) "")))))
    (kill-new v) (message "Copied %s" v)))

(defun dbx-sql-result-copy-row ()
  "Copy the row at point as tab-separated values."
  (interactive)
  (let* ((line (buffer-substring-no-properties (line-beginning-position) (line-end-position)))
         (cells (mapcar #'string-trim (split-string (string-trim line "|" "|") "|"))))
    (kill-new (string-join cells "\t")) (message "Row copied")))

(defun dbx-sql-result-copy-table ()
  "Copy the table of the result at point as TSV."
  (interactive)
  (save-excursion
    (dbx-sql--goto-table)
    (kill-new (mapconcat (lambda (row) (if (eq row 'hline) nil (string-join row "\t")))
                         (delq 'hline (org-table-to-lisp)) "\n"))
    (message "Table copied as TSV")))

(defun dbx-sql-result-sort ()
  "Sort the table by the column at point."
  (interactive)
  (let ((inhibit-read-only t)) (org-table-sort-lines nil)))

(defun dbx-sql-result-editable ()
  "Leave the read-only view and edit the results as plain Org."
  (interactive)
  (dbx-sql-result-mode -1)
  (setq buffer-read-only nil)
  (message "Results are editable now; M-x dbx-sql-result-mode returns to the view"))

;;;; History

;;;###autoload
(defun dbx-sql-history ()
  "Pick a statement from the query history and open it in a SQL buffer."
  (interactive)
  (let* ((res (dbx-await (lambda (k e)
                           (dbx-api "get" "/api/2.0/sql/history/queries"
                                    :params '((max_results . 100))
                                    :callback (lambda (r) (funcall k (alist-get 'res r)))
                                    :error-callback e))
                         "query history"))
         (cands (mapcar (lambda (q)
                          (cons (format "%s %s"
                                        (dbx-status-glyph (pcase (alist-get 'status q)
                                                            ("FINISHED" 'success) ("FAILED" 'failed)
                                                            ("RUNNING" 'running) ("QUEUED" 'waiting)
                                                            (_ 'cancelled)))
                                        (truncate-string-to-width
                                         (replace-regexp-in-string "[ \t\n]+" " " (or (alist-get 'query_text q) ""))
                                         90 nil nil "…"))
                                q))
                        res))
         (pick (completing-read
                "Query: "
                (lambda (str pred action)
                  (if (eq action 'metadata)
                      `(metadata (category . dbx-query)
                                 (display-sort-function . identity)
                                 (annotation-function
                                  . ,(lambda (c)
                                       (let ((q (cdr (assoc c cands))))
                                         (format "  %s  %s  %s"
                                                 (dbx-format-time (alist-get 'query_start_time_ms q))
                                                 (dbx-format-duration (alist-get 'duration q))
                                                 (or (alist-get 'user_name q) ""))))))
                    (complete-with-action action cands str pred)))
                nil t)))
    (dbx-sql-new-buffer (alist-get 'query_text (cdr (assoc pick cands))))))

(provide 'dbx-sql)
;;; dbx-sql.el ends here
