;;; dbx-page.el --- Read-only object pages and lists  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; `dbx-page-mode' is the base for job, run, cluster, warehouse,
;; pipeline and bundle pages.  A page has a refresh function that
;; fetches data asynchronously and a render function that inserts text.
;; Sections are top-level lines (column 0) folded by `outline-minor-mode';
;; content lines are indented.  Items carry text properties:
;;   `dbx-open' a function called by RET
;;   `dbx-id'   an id copied by w
;;   `dbx-url'  a URL copied by W and opened by o
;;   `dbx-item' marks the start of an item for n/p

;;; Code:

(require 'outline)
(require 'button)
(require 'dbx-util)
(require 'dbx-cli)

(defcustom dbx-page-refresh-active 3
  "Seconds between refreshes while something on the page is running."
  :type 'number :group 'dbx)

(defcustom dbx-page-refresh-idle 60
  "Seconds between refreshes when nothing on the page is running."
  :type 'number :group 'dbx)

(defvar-local dbx-page-refresh-function nil
  "Function with no arguments that fetches data and re-renders the page.")
(defvar-local dbx-page-actions nil
  "List of (KEY DESCRIPTION COMMAND) shown by `dbx-page-actions'.")
(defvar-local dbx-page-active-p nil
  "Non-nil when the page shows running work (faster refresh).")
(defvar-local dbx-page-newer-function nil)
(defvar-local dbx-page-older-function nil)
(defvar-local dbx-page--timer nil)
(defvar-local dbx-page-watch-function nil
  "Function called by f (watch toggle), if any.")

(defvar-keymap dbx-page-mode-map
  :parent special-mode-map
  "g" #'dbx-page-refresh
  "RET" #'dbx-page-open
  "TAB" #'dbx-page-toggle-section
  "<backtab>" #'dbx-page-cycle-all
  "n" #'dbx-page-next-item
  "p" #'dbx-page-previous-item
  "M-n" #'dbx-page-next-section
  "M-p" #'dbx-page-previous-section
  "]" #'dbx-page-newer
  "[" #'dbx-page-older
  "w" #'dbx-page-copy-id
  "W" #'dbx-page-copy-url
  "o" #'dbx-page-browse
  "f" #'dbx-page-watch
  "?" #'dbx-page-actions)

(define-derived-mode dbx-page-mode special-mode "DBX"
  "Base mode for dbx object pages.

\\{dbx-page-mode-map}"
  :group 'dbx
  (setq-local truncate-lines t)
  (setq-local revert-buffer-function (lambda (&rest _) (dbx-page-refresh)))
  (setq-local outline-regexp "[^ \t\n]")
  (setq-local outline-level (lambda () 1))
  (setq-local outline-minor-mode-use-buttons 'insert)
  (setq-local outline-minor-mode-highlight nil)
  (outline-minor-mode 1)
  (add-hook 'kill-buffer-hook #'dbx-page--cancel-timer nil t))

;;;; Rendering helpers

(defmacro dbx-page-with-render (&rest body)
  "Erase the page, run BODY to insert content, keep point and folds."
  (declare (indent 0) (debug t))
  `(let* ((inhibit-read-only t)
          (line (line-number-at-pos))
          (col (current-column))
          (folded (dbx-page--folded-headings))
          (wins (mapcar (lambda (w) (cons w (with-selected-window w
                                              (line-number-at-pos (window-start)))))
                        (get-buffer-window-list nil nil t))))
     (erase-buffer)
     ,@body
     (dbx-page--refold folded)
     (goto-char (point-min))
     (forward-line (1- line))
     (move-to-column col)
     (dolist (w wins)
       (when (window-live-p (car w))
         (set-window-start (car w) (save-excursion
                                     (goto-char (point-min))
                                     (forward-line (1- (cdr w)))
                                     (point))
                           t)))))

(defun dbx-page--folded-headings ()
  (let (res)
    (save-excursion
      (goto-char (point-min))
      (while (re-search-forward "^[^ \t\n]" nil t)
        (when (save-excursion (end-of-line) (outline-invisible-p))
          (push (dbx-page--heading-key) res))))
    res))

(defun dbx-page--heading-key ()
  (let ((s (buffer-substring-no-properties (line-beginning-position) (line-end-position))))
    (car (split-string (string-trim s) "  "))))

(defun dbx-page--refold (keys)
  (save-excursion
    (goto-char (point-min))
    (while (re-search-forward "^[^ \t\n]" nil t)
      (let ((key (dbx-page--heading-key)))
        (when (or (member key keys) (get-text-property (point) 'dbx-folded))
          (save-excursion (outline-hide-subtree))))
      (forward-line 1))))

(defun dbx-page-heading (title &optional extra folded)
  "Insert a section heading TITLE with optional EXTRA text.
FOLDED non-nil folds the section on first render."
  (insert (propertize title 'face 'dbx-heading 'dbx-folded folded))
  (when extra (insert "  " (propertize extra 'face 'dbx-dim)))
  (insert "\n"))

(defun dbx-page-kv (&rest pairs)
  "Insert a header line of label/value PAIRS separated by dots."
  (insert " ")
  (let (first)
    (cl-loop for (k v) on pairs by #'cddr
             when v do
             (when first (insert (propertize "  ·  " 'face 'dbx-dim)))
             (setq first t)
             (when k (insert (propertize k 'face 'dbx-key) " "))
             (insert (format "%s" v))))
  (insert "\n"))

(defun dbx-page-button (label action &optional help)
  "Insert a [LABEL] button running ACTION (a command)."
  (insert-text-button (concat "[" label "]")
                      'action (lambda (_) (call-interactively action))
                      'follow-link t 'help-echo help 'face 'link)
  (insert " "))

(defun dbx-page-item (start end &rest props)
  "Mark START..END as an item with PROPS (dbx-open, dbx-id, dbx-url, ...)."
  (add-text-properties start end (append (list 'dbx-item t) props))
  (put-text-property start (1+ start) 'dbx-item-start t))

;;;; Commands

(defun dbx-page-refresh ()
  "Refresh the page."
  (interactive)
  (when dbx-page-refresh-function
    (funcall dbx-page-refresh-function)))

(defvar-local dbx-list-open-function nil
  "In lists: function called with the row id by RET.")
(defvar-local dbx-list-url-function nil
  "In lists: function returning the URL for a row id.")

(defun dbx-page--prop (prop)
  (or (and (derived-mode-p 'tabulated-list-mode) (tabulated-list-get-id)
           (let ((id (tabulated-list-get-id)))
             (pcase prop
               ('dbx-open (and dbx-list-open-function
                               (lambda () (funcall dbx-list-open-function id))))
               ('dbx-id (if (consp id) (car id) id))
               ('dbx-url (and dbx-list-url-function
                              (funcall dbx-list-url-function id))))))
      (get-text-property (point) prop)
      (and (> (point) (point-min)) (get-text-property (1- (point)) prop))
      (save-excursion
        (let ((pos (next-single-property-change (line-beginning-position) prop
                                                nil (line-end-position))))
          (and pos (< pos (line-end-position)) (get-text-property pos prop))))))

(defun dbx-page-open ()
  "Open the thing at point."
  (interactive)
  (cond ((button-at (point)) (push-button))
        ((dbx-page--prop 'dbx-open) (funcall (dbx-page--prop 'dbx-open)))
        ((save-excursion (beginning-of-line) (looking-at-p "[^ \t\n]"))
         (dbx-page-toggle-section))
        (t (message "Nothing to open here"))))

(defun dbx-page-toggle-section ()
  "Fold or unfold the section at point."
  (interactive)
  (save-excursion
    (unless (outline-on-heading-p) (outline-previous-heading))
    (if (save-excursion (end-of-line) (outline-invisible-p))
        (outline-show-subtree)
      (outline-hide-subtree))))

(defun dbx-page-cycle-all ()
  "Fold or unfold all sections."
  (interactive)
  (outline-cycle-buffer))

(defun dbx-page-next-item ()
  "Move to the next item."
  (interactive)
  (let ((pos (next-single-property-change (line-end-position) 'dbx-item-start)))
    (while (and pos (not (get-text-property pos 'dbx-item-start)))
      (setq pos (next-single-property-change pos 'dbx-item-start)))
    (if pos (goto-char pos) (message "No more items"))))

(defun dbx-page-previous-item ()
  "Move to the previous item."
  (interactive)
  (let ((pos (previous-single-property-change (line-beginning-position) 'dbx-item-start)))
    (while (and pos (> pos (point-min)) (not (get-text-property pos 'dbx-item-start)))
      (setq pos (previous-single-property-change pos 'dbx-item-start)))
    (if (and pos (get-text-property pos 'dbx-item-start)) (goto-char pos)
      (message "No previous item"))))

(defun dbx-page-next-section ()
  "Move to the next section."
  (interactive)
  (outline-next-heading))

(defun dbx-page-previous-section ()
  "Move to the previous section."
  (interactive)
  (outline-previous-heading))

(defun dbx-page-newer ()
  "Show newer entries."
  (interactive)
  (if dbx-page-newer-function (funcall dbx-page-newer-function)
    (message "No paging here")))

(defun dbx-page-older ()
  "Show older entries."
  (interactive)
  (if dbx-page-older-function (funcall dbx-page-older-function)
    (message "No paging here")))

(defun dbx-page-copy-id ()
  "Copy the id of the thing at point."
  (interactive)
  (if-let* ((id (dbx-page--prop 'dbx-id)))
      (progn (kill-new (format "%s" id)) (message "Copied %s" id))
    (message "No id here")))

(defun dbx-page-copy-url ()
  "Copy the workspace URL of the thing at point."
  (interactive)
  (if-let* ((url (dbx-page--prop 'dbx-url)))
      (progn (kill-new url) (message "Copied %s" url))
    (message "No URL here")))

(defun dbx-page-browse ()
  "Open the thing at point in the browser."
  (interactive)
  (if-let* ((url (dbx-page--prop 'dbx-url)))
      (browse-url url)
    (message "No URL here")))

(defun dbx-page-watch ()
  "Watch the thing at point (notify when it finishes)."
  (interactive)
  (if dbx-page-watch-function (funcall dbx-page-watch-function)
    (message "Nothing to watch here")))

(defun dbx-page-actions ()
  "Choose one of this page's actions."
  (interactive)
  (if (null dbx-page-actions)
      (describe-mode)
    (let* ((choices (mapcar (lambda (a) (cons (format "%-4s %s" (nth 0 a) (nth 1 a)) (nth 2 a)))
                            dbx-page-actions))
           (pick (completing-read "Action: " choices nil t)))
      (call-interactively (cdr (assoc pick choices))))))

;;;; Auto refresh

(defun dbx-page--cancel-timer ()
  (when (timerp dbx-page--timer) (cancel-timer dbx-page--timer))
  (setq dbx-page--timer nil))

(defun dbx-page-schedule-refresh ()
  "Schedule the next refresh according to `dbx-page-active-p'."
  (dbx-page--cancel-timer)
  (let ((buf (current-buffer)))
    (setq dbx-page--timer
          (run-at-time (if dbx-page-active-p dbx-page-refresh-active dbx-page-refresh-idle)
                       nil
                       (lambda ()
                         (when (buffer-live-p buf)
                           (with-current-buffer buf
                             (if (get-buffer-window buf 'visible)
                                 (dbx-page-refresh)
                               (dbx-page-schedule-refresh)))))))))

(defun dbx-page-buffer (name mode)
  "Return buffer NAME in MODE, creating it if needed."
  (let ((buf (get-buffer-create name)))
    (with-current-buffer buf
      (unless (derived-mode-p mode) (funcall mode)))
    buf))

(defun dbx-page-loading (buf what)
  "Show a loading line for WHAT in BUF if it is empty."
  (with-current-buffer buf
    (when (= (buffer-size) 0)
      (let ((inhibit-read-only t))
        (insert (propertize (format " Loading %s…\n" what) 'face 'dbx-dim))))))

(defun dbx-page-error (buf msg)
  "Show error MSG at the top of BUF."
  (when (buffer-live-p buf)
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (save-excursion
          (goto-char (point-min))
          (when (get-text-property (point) 'dbx-error)
            (delete-region (point) (line-beginning-position 2)))
          (insert (propertize (concat " " (dbx-glyph 'failed) " " msg "\n")
                              'face 'dbx-failed 'dbx-error t))))
      (dbx-page-schedule-refresh))))

;;;; Lists (tabulated-list)

(defvar-local dbx-list-filter nil "Current filter string of a dbx list.")
(defvar-local dbx-list-all-entries nil "Unfiltered entries of a dbx list.")

(defvar-keymap dbx-list-mode-map
  :parent tabulated-list-mode-map
  "g" #'dbx-page-refresh
  "RET" #'dbx-page-open
  "/" #'dbx-list-filter
  "w" #'dbx-page-copy-id
  "W" #'dbx-page-copy-url
  "o" #'dbx-page-browse
  "f" #'dbx-page-watch
  "?" #'dbx-page-actions)

(define-derived-mode dbx-list-mode tabulated-list-mode "DBX-List"
  "Base mode for dbx lists.

\\{dbx-list-mode-map}"
  :group 'dbx
  (setq-local revert-buffer-function (lambda (&rest _) (dbx-page-refresh)))
  (add-hook 'kill-buffer-hook #'dbx-page--cancel-timer nil t))

(defun dbx-list-set-entries (entries)
  "Show ENTRIES, applying the current filter."
  (setq dbx-list-all-entries entries)
  (setq tabulated-list-entries
        (if (and dbx-list-filter (not (string-empty-p dbx-list-filter)))
            (cl-remove-if-not
             (lambda (e)
               (string-match-p (regexp-quote dbx-list-filter)
                               (mapconcat (lambda (c) (if (consp c) (car c) (format "%s" c)))
                                          (append (cadr e) nil) " ")))
             entries)
          entries))
  (tabulated-list-print t))

(defun dbx-list-filter (string)
  "Show only entries matching STRING (empty clears the filter)."
  (interactive (list (read-string "Filter: " dbx-list-filter)))
  (setq dbx-list-filter string)
  (dbx-list-set-entries dbx-list-all-entries))

(provide 'dbx-page)
;;; dbx-page.el ends here
