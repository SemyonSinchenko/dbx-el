;; -*- mode: eask; lexical-binding: t -*-

(package "dbx-el"
         "0.1.0"
         "Databricks workbench: SQL, jobs, bundles, pipelines")

(keywords "tools" "data" "sql")

(website-url "https://github.com/SemyonSinchenko/dbx-el")

(package-file "dbx-el.el")

(files "dbx*.el" "python")

(script "test" "rm -f *.elc && eask compile && eask load test/compile.el && eask test ert test/dbx-test.el")

(source 'gnu)

(depends-on "emacs" "30.1")
(depends-on "transient" "0.7")

(setq network-security-level 'high)
