;;; compile.el --- Recompile dbx, failing on any warning  -*- lexical-binding: t; -*-

;;; Commentary:

;; Run with `eask load test/compile.el' after `eask compile'.  `eask
;; compile' never fails on warnings.  Worse, a call with the wrong
;; number of arguments to a function of another file is only reported
;; when that file is loaded compiled: on a clean tree it depends on the
;; file order.  So this recompiles, in a fresh Emacs where every
;; `require' loads an .elc, with warnings as errors.

;;; Code:

(require 'bytecomp)

(let* ((root (expand-file-name ".." (file-name-directory load-file-name)))
       (byte-compile-error-on-warn t)
       (failed nil))
  (add-to-list 'load-path root)
  (dolist (f (directory-files root t "\\`dbx.*\\.el\\'"))
    (unless (file-exists-p (concat f "c"))
      (error "Run `eask compile' first: no %sc" (file-name-nondirectory f)))
    (unless (byte-compile-file f) (push (file-name-nondirectory f) failed)))
  (when failed
    (message "Byte-compilation failed (warnings are errors): %s"
             (string-join (nreverse failed) ", "))
    (kill-emacs 1)))

;;; compile.el ends here
