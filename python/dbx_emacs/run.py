"""Run a file like ``python FILE ARGS`` with a Databricks session attached.

Exit codes: the script's own, 1 for an uncaught exception, 3 when the
session could not be created before the script started.
"""

from __future__ import annotations

import os
import runpy
import sys
import traceback

from . import session as sess


def run(argv: list[str]) -> int:
    module = False
    attach = True
    while argv and argv[0].startswith("--"):
        opt = argv.pop(0)
        if opt == "--":
            break
        if opt == "--module":
            module = True
        elif opt == "--no-attach":
            attach = False
    if not argv:
        print("usage: python -m dbx_emacs run [--module] [--no-attach] TARGET [ARGS...]", file=sys.stderr)
        return 2
    target, args = argv[0], argv[1:]

    init: dict = {}
    if attach and not sess.fake_mode():
        try:
            spark = sess.create_session()
        except Exception as exc:  # noqa: BLE001
            print(f"dbx: connection failed before the script started: {type(exc).__name__}: {exc}",
                  file=sys.stderr)
            return 3
        sess.inject_globals(init, spark)

    sys.argv = [target, *args]
    if module:
        sys.path[0] = os.getcwd()
    else:
        sys.path[0] = os.path.dirname(os.path.abspath(target))
    try:
        if module:
            runpy.run_module(target, init_globals=init, run_name="__main__", alter_sys=True)
        else:
            runpy.run_path(target, init_globals=init, run_name="__main__")
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return 0
        if isinstance(code, int):
            return code
        print(code, file=sys.stderr)
        return 1
    except BaseException as exc:  # noqa: BLE001
        tb = traceback.TracebackException.from_exception(exc)
        tb.stack = traceback.StackSummary.from_list(
            [f for f in tb.stack if "runpy" not in f.filename and "dbx_emacs" not in f.filename])
        sys.stderr.write("".join(tb.format()))
        return 1
    return 0
