;;; dbx-el.el --- Databricks workbench: SQL, jobs, bundles, pipelines  -*- lexical-binding: t; -*-

;; Author: Semyon Sinchenko <ssinchenko@apache.org>
;; Version: 0.1.0
;; Package-Requires: ((emacs "30.1") (transient "0.7"))
;; Keywords: tools, data, sql
;; URL: https://github.com/SemyonSinchenko/dbx-el

;; This file is not part of GNU Emacs.
;; SPDX-License-Identifier: WTFPL
;; This program is free software.  It comes without any warranty, to
;; the extent permitted by applicable law.  You can redistribute it
;; and/or modify it under the terms of the Do What The Fuck You Want
;; To Public License, Version 2, as published by Sam Hocevar.  See the
;; LICENSE file for more details.

;;; Commentary:

;; dbx-el is a Databricks workbench for Emacs 30.  It needs the
;; `databricks' CLI for the control plane and uv for the optional
;; Python side (Databricks Connect).
;;
;; - SQL editor on SQL warehouses with results as Org tables
;; - jobs, runs, clusters, warehouses and pipelines as fuzzy pickers,
;;   text pages and lists
;; - bundle minor mode with validation, deployment and graphs
;; - pipeline dataset graphs from the event log
;; - a Python REPL running on Databricks Connect
;;
;; Commands and options use the `dbx-' prefix.  Suggested setup:
;;
;;   (require 'dbx-el)
;;   (keymap-global-set "C-c d" dbx-command-map)
;;   (dbx-mode-line-mode 1)

;;; Code:

(require 'dbx)

(provide 'dbx-el)
;;; dbx-el.el ends here
