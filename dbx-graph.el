;;; dbx-graph.el --- Graphs: Graphviz SVG, text tree, Mermaid export  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL

;;; Commentary:

;; One graph model rendered three ways.  A graph is a plist:
;;   :title  string
;;   :nodes  list of plists (:id :label :status :kind :group :action :info)
;;   :edges  list of plists (:from :to :label)
;; Graphviz SVG with an image map makes nodes clickable in graphical
;; Emacs.  Without `dot' or in a terminal a text tree is shown.
;; `dbx-graph-mermaid' exports Mermaid flowchart text.

;;; Code:

(require 'dbx-util)
(require 'dbx-page)
(require 'image)
(declare-function image-size "image.c" (spec &optional pixels frame))

(defcustom dbx-graph-dot-program "dot"
  "Graphviz dot executable."
  :type 'string :group 'dbx)

(defcustom dbx-graph-rankdir "LR"
  "Graphviz rank direction: \"LR\" (left to right) or \"TB\"."
  :type '(choice (const "LR") (const "TB")) :group 'dbx)

(defcustom dbx-graph-prefer-text nil
  "Non-nil to always show graphs as text."
  :type 'boolean :group 'dbx)

(defconst dbx--graph-colors
  '((success . ("#d1f0d6" "#1a7f37")) (failed . ("#ffd8d3" "#cf222e"))
    (running . ("#d6e8ff" "#0969da")) (waiting . ("#eeeeee" "#6e7781"))
    (blocked . ("#ffe2c2" "#bc4c00")) (skipped . ("#f2f2f2" "#8c959f"))
    (cancelled . ("#f2f2f2" "#57606a")) (retried . ("#d1f0d6" "#bf8700"))
    (unknown . ("#fff8c5" "#9a6700")) (absent . ("#ffffff" "#8c959f"))
    (nil . ("#ffffff" "#57606a")))
  "Fill and border colors per status.")

;;;; DOT

(defun dbx--dot-quote (s)
  (concat "\"" (replace-regexp-in-string "[\"\\\\]" "\\\\\\&" (format "%s" s)) "\""))

(defun dbx--graph-node-label (n)
  (let ((st (plist-get n :status)))
    (concat (if st (concat (dbx-glyph (if (eq st 'retried) 'success st)) " ") "")
            (plist-get n :label)
            (let ((info (plist-get n :info)))
              (if (and info (not (string-empty-p info))) (concat "\n" info) "")))))

(defun dbx-graph-dot (graph)
  "Return Graphviz DOT source for GRAPH."
  (let ((groups (delete-dups (delq nil (mapcar (lambda (n) (plist-get n :group))
                                               (plist-get graph :nodes))))))
    (with-temp-buffer
      (insert "digraph dbx {\n"
              (format "  rankdir=%s; bgcolor=\"transparent\"; pad=0.2; nodesep=0.3; ranksep=0.45;\n"
                      dbx-graph-rankdir)
              "  node [shape=box, style=\"rounded,filled\", fontname=\"Sans\", fontsize=11];\n"
              "  edge [color=\"#8c959f\", fontname=\"Sans\", fontsize=9];\n")
      (cl-flet ((emit-node (n indent)
                  (let ((c (alist-get (plist-get n :status) dbx--graph-colors)))
                    (insert indent (dbx--dot-quote (plist-get n :id))
                            (format " [label=%s, fillcolor=%s, color=%s, URL=%s, tooltip=%s%s];\n"
                                    (dbx--dot-quote (dbx--graph-node-label n))
                                    (dbx--dot-quote (car c)) (dbx--dot-quote (cadr c))
                                    (dbx--dot-quote (concat "dbx:" (plist-get n :id)))
                                    (dbx--dot-quote (plist-get n :label))
                                    (pcase (plist-get n :kind)
                                      ('pipeline ", shape=component")
                                      ('table ", shape=cylinder, style=filled")
                                      ('file ", shape=note, style=filled")
                                      (_ "")))))))
        (cl-loop for g in groups for i from 0 do
                 (insert (format "  subgraph cluster_%d {\n    label=%s; style=\"rounded,dashed\"; color=\"#8c959f\"; fontname=\"Sans\";\n"
                                 i (dbx--dot-quote g)))
                 (dolist (n (plist-get graph :nodes))
                   (when (equal (plist-get n :group) g) (emit-node n "    ")))
                 (insert "  }\n"))
        (dolist (n (plist-get graph :nodes))
          (unless (plist-get n :group) (emit-node n "  "))))
      (dolist (e (plist-get graph :edges))
        (insert "  " (dbx--dot-quote (plist-get e :from)) " -> "
                (dbx--dot-quote (plist-get e :to))
                (if (plist-get e :label)
                    (format " [label=%s]" (dbx--dot-quote (plist-get e :label)))
                  "")
                ";\n"))
      (insert "}\n")
      (buffer-string))))

;;;; Mermaid

(defun dbx--mermaid-id (graph id)
  (format "n%d" (or (cl-position id (plist-get graph :nodes)
                                  :key (lambda (n) (plist-get n :id)) :test #'equal)
                    0)))

(defun dbx-graph-mermaid (graph)
  "Return Mermaid flowchart text for GRAPH."
  (let ((groups (delete-dups (delq nil (mapcar (lambda (n) (plist-get n :group))
                                               (plist-get graph :nodes))))))
    (with-temp-buffer
      (insert (format "flowchart %s\n" (if (equal dbx-graph-rankdir "TB") "TB" "LR")))
      (cl-flet ((emit (n indent)
                  (insert indent (dbx--mermaid-id graph (plist-get n :id))
                          "[\""
                          (replace-regexp-in-string "\"" "#quot;" (dbx--graph-node-label n))
                          "\"]\n")))
        (cl-loop for g in groups for i from 0 do
                 (insert (format "  subgraph g%d[\"%s\"]\n" i g))
                 (dolist (n (plist-get graph :nodes))
                   (when (equal (plist-get n :group) g) (emit n "    ")))
                 (insert "  end\n"))
        (dolist (n (plist-get graph :nodes))
          (unless (plist-get n :group) (emit n "  "))))
      (dolist (e (plist-get graph :edges))
        (insert "  " (dbx--mermaid-id graph (plist-get e :from))
                (if (plist-get e :label) (format " -->|%s| " (plist-get e :label)) " --> ")
                (dbx--mermaid-id graph (plist-get e :to)) "\n"))
      (buffer-string))))

;;;; Text tree

(defun dbx-graph-text (graph)
  "Insert GRAPH as an indented tree at point.
Nodes reached a second time are shown with a back reference."
  (let* ((nodes (plist-get graph :nodes))
         (edges (plist-get graph :edges))
         (ids (mapcar (lambda (n) (plist-get n :id)) nodes))
         (has-parent (mapcar (lambda (e) (plist-get e :to)) edges))
         (roots (or (cl-remove-if (lambda (id) (member id has-parent)) ids) ids))
         (seen (make-hash-table :test #'equal)))
    (cl-labels
        ((node (id) (cl-find id nodes :key (lambda (n) (plist-get n :id)) :test #'equal))
         (children (id) (cl-remove-if-not (lambda (e) (equal (plist-get e :from) id)) edges))
         (line (n prefix connector)
           (let ((start (point)))
             (insert "  " prefix connector
                     (dbx-status-glyph (or (plist-get n :status) 'absent)) " "
                     (plist-get n :label)
                     (let ((info (plist-get n :info)))
                       (if (and info (not (string-empty-p info)))
                           (propertize (concat "  " info) 'face 'dbx-dim)
                         ""))
                     "\n")
             (dbx-page-item start (1- (point))
                            'dbx-open (plist-get n :action)
                            'dbx-id (plist-get n :id))))
         (walk (id prefix)
           (let ((kids (children id)))
             (cl-loop for e in kids for i from 1
                      for last = (= i (length kids))
                      for child = (node (plist-get e :to))
                      for conn = (concat (if last "└─" "├─")
                                         (if (plist-get e :label)
                                             (format "%s─▶ " (plist-get e :label))
                                           "▶ "))
                      when child do
                      (if (gethash (plist-get child :id) seen)
                          (insert "  " prefix conn
                                  (propertize (concat "↑ " (plist-get child :label)) 'face 'dbx-dim)
                                  "\n")
                        (puthash (plist-get child :id) t seen)
                        (line child prefix conn)
                        (walk (plist-get child :id)
                              (concat prefix (if last "   " "│  "))))))))
      (dolist (r roots)
        (unless (gethash r seen)
          (puthash r t seen)
          (line (node r) "" "")
          (walk r ""))))))

;;;; Graph buffer

(defvar-local dbx-graph--graph nil)
(defvar-local dbx-graph--areas nil "Alist of area symbol -> node id.")

(defvar-keymap dbx-graph-mode-map
  :parent dbx-page-mode-map
  "RET" #'dbx-graph-open-node
  "t" #'dbx-graph-toggle-text
  "m" #'dbx-graph-copy-mermaid
  "d" #'dbx-graph-copy-dot
  "+" #'dbx-graph-zoom-in
  "-" #'dbx-graph-zoom-out)

(define-derived-mode dbx-graph-mode dbx-page-mode "DBX-Graph"
  "Mode for dbx graph buffers.

\\{dbx-graph-mode-map}"
  (setq-local cursor-type nil)
  (setq-local dbx-page-actions
              '(("RET" "Open node" dbx-graph-open-node)
                ("t" "Toggle text/graphics" dbx-graph-toggle-text)
                ("m" "Copy as Mermaid" dbx-graph-copy-mermaid)
                ("d" "Copy as DOT" dbx-graph-copy-dot))))

(defvar-local dbx-graph--text-mode nil)
(defvar-local dbx-graph--scale 1.0)

(defun dbx-graph--node (id)
  (cl-find id (plist-get dbx-graph--graph :nodes)
           :key (lambda (n) (plist-get n :id)) :test #'equal))

(defun dbx-graph-open-node (&optional id)
  "Open node ID, the node at point, or a node chosen by name."
  (interactive)
  (let* ((id (or id (get-text-property (point) 'dbx-id)
                 (let* ((alist (mapcar (lambda (n) (cons (plist-get n :label) (plist-get n :id)))
                                       (plist-get dbx-graph--graph :nodes))))
                   (cdr (assoc (completing-read "Node: " alist nil t) alist)))))
         (n (dbx-graph--node id)))
    (if (plist-get n :action) (funcall (plist-get n :action))
      (message "%s" (plist-get n :label)))))

(defun dbx-graph-copy-mermaid ()
  "Copy the graph as Mermaid text."
  (interactive)
  (kill-new (dbx-graph-mermaid dbx-graph--graph))
  (message "Mermaid copied"))

(defun dbx-graph-copy-dot ()
  "Copy the graph as DOT source."
  (interactive)
  (kill-new (dbx-graph-dot dbx-graph--graph))
  (message "DOT copied"))

(defun dbx-graph-toggle-text ()
  "Switch between Graphviz and text rendering."
  (interactive)
  (setq dbx-graph--text-mode (not dbx-graph--text-mode))
  (dbx-graph--render))

(defun dbx-graph-zoom-in () "Zoom in." (interactive)
       (setq dbx-graph--scale (* dbx-graph--scale 1.25)) (dbx-graph--render))
(defun dbx-graph-zoom-out () "Zoom out." (interactive)
       (setq dbx-graph--scale (/ dbx-graph--scale 1.25)) (dbx-graph--render))

(defun dbx-graph--graphics-p ()
  (and (not dbx-graph-prefer-text) (not dbx-graph--text-mode)
       (display-graphic-p) (image-type-available-p 'svg)
       (executable-find dbx-graph-dot-program)))

(defun dbx-graph--parse-cmapx (file)
  "Parse a Graphviz cmapx FILE into ((ID X0 Y0 X1 Y1) ...)."
  (when (file-readable-p file)
    (with-temp-buffer
      (insert-file-contents file)
      (let (res)
        (goto-char (point-min))
        (while (re-search-forward "<area\\([^>]*\\)>" nil t)
          (let ((attrs (match-string 1)) id nums)
            (when (string-match "href=\"dbx:\\([^\"]*\\)\"" attrs)
              (setq id (xml-substitute-special (match-string 1 attrs))))
            (when (string-match "coords=\"\\([^\"]*\\)\"" attrs)
              (setq nums (mapcar #'string-to-number
                                 (split-string (match-string 1 attrs) "[, ]+" t))))
            (when (and id (>= (length nums) 4))
              (let ((xs (cl-loop for (x _y) on nums by #'cddr collect x))
                    (ys (cl-loop for (_x y) on nums by #'cddr collect y)))
                (push (list id (apply #'min xs) (apply #'min ys)
                            (apply #'max xs) (apply #'max ys))
                      res)))))
        (nreverse res)))))

(autoload 'xml-substitute-special "xml")

(defun dbx-graph--svg-width-pt (svg)
  (when (string-match "<svg[^>]*width=\"\\([0-9.]+\\)pt\"" svg)
    (string-to-number (match-string 1 svg))))

(defun dbx-graph--render ()
  (let ((buf (current-buffer)) (graph dbx-graph--graph))
    (if (not (dbx-graph--graphics-p))
        (dbx-page-with-render
          (dbx-graph--insert-header graph)
          (dbx-graph-text graph))
      (let* ((svgf (make-temp-file "dbx-graph" nil ".svg"))
             (mapf (make-temp-file "dbx-graph" nil ".map"))
             (dotf (make-temp-file "dbx-graph" nil ".dot" (dbx-graph-dot graph))))
        (make-process
         :name "dbx-dot" :noquery t :connection-type 'pipe
         :buffer (generate-new-buffer " *dbx-dot*")
         :command (list dbx-graph-dot-program "-Tsvg" "-o" svgf "-Tcmapx" "-o" mapf dotf)
         :sentinel
         (lambda (proc _)
           (unless (process-live-p proc)
             (let ((ok (zerop (process-exit-status proc))))
               (kill-buffer (process-buffer proc))
               (when (buffer-live-p buf)
                 (with-current-buffer buf
                   (if ok (dbx-graph--insert-svg graph svgf mapf)
                     (setq dbx-graph--text-mode t)
                     (dbx-graph--render))))
               (dolist (f (list svgf mapf dotf)) (ignore-errors (delete-file f)))))))))))

(defun dbx-graph--insert-header (graph)
  (insert " " (propertize (or (plist-get graph :title) "Graph") 'face 'dbx-title) "\n")
  (when (plist-get graph :subtitle)
    (insert " " (propertize (plist-get graph :subtitle) 'face 'dbx-dim) "\n"))
  (insert (propertize " RET open node · t text/graphics · m copy Mermaid · d copy DOT · +/- zoom\n\n"
                      'face 'dbx-dim)))

(defun dbx-graph--insert-svg (graph svgf mapf)
  (let* ((svg (with-temp-buffer (insert-file-contents svgf) (buffer-string)))
         (areas (dbx-graph--parse-cmapx mapf))
         (img (create-image svg 'svg t :scale dbx-graph--scale))
         (px-width (car (image-size img t)))
         (pt-width (dbx-graph--svg-width-pt svg))
         (ratio (if (and pt-width (> pt-width 0)) (/ (float px-width) pt-width) 1.0))
         (map-keys (make-sparse-keymap))
         (alist nil)
         (emap (cl-loop for (id x0 y0 x1 y1) in areas for i from 0
                        for sym = (intern (format "dbx-node-%d" i))
                        do (push (cons sym id) alist)
                        collect (list (cons 'rect (cons (cons (round (* ratio x0)) (round (* ratio y0)))
                                                        (cons (round (* ratio x1)) (round (* ratio y1)))))
                                      sym
                                      (list 'help-echo
                                            (plist-get (dbx-graph--node id) :label)
                                            'pointer 'hand)))))
    (setq dbx-graph--areas alist)
    (dolist (a alist)
      (let ((id (cdr a)))
        (keymap-set map-keys (format "<%s> <mouse-1>" (car a))
                    (lambda () (interactive) (dbx-graph-open-node id)))))
    (setf (image-property img :map) emap)
    (dbx-page-with-render
      (dbx-graph--insert-header graph)
      (let ((start (point)))
        (insert-image img "[graph]")
        ;; `insert-image' adds `image-map'; ours must cover the whole image
        (put-text-property start (point) 'keymap
                           (make-composed-keymap map-keys dbx-graph-mode-map)))
      (insert "\n"))))

(defun dbx-graph-show (graph &optional buffer-name)
  "Display GRAPH in BUFFER-NAME (default from its title)."
  (let ((buf (dbx-page-buffer (or buffer-name (format "*dbx-graph: %s*" (plist-get graph :title)))
                              #'dbx-graph-mode)))
    (with-current-buffer buf
      (setq dbx-graph--graph graph)
      (setq-local dbx-page-refresh-function #'dbx-graph--render)
      (dbx-graph--render))
    (pop-to-buffer buf)
    buf))

(provide 'dbx-graph)
;;; dbx-graph.el ends here
