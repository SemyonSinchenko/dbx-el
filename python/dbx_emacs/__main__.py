"""Entry point: ``python -m dbx_emacs serve|run|probe``."""

import json
import sys


def probe() -> int:
    from importlib import metadata

    info = {"python": sys.version.split()[0], "packages": {}}
    for dist in ("databricks-connect", "pyspark", "pyspark-client", "databricks-sdk"):
        try:
            info["packages"][dist] = metadata.version(dist)
        except metadata.PackageNotFoundError:
            info["packages"][dist] = None
    pk = info["packages"]
    info["conflict"] = bool(pk["databricks-connect"] and (pk["pyspark"] or pk["pyspark-client"]))
    print(json.dumps(info))
    return 0


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    cmd = argv.pop(0) if argv else "serve"
    if cmd == "serve":
        from .server import serve

        return serve()
    if cmd == "run":
        from .run import run

        return run(argv)
    if cmd == "probe":
        return probe()
    print(f"unknown command: {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
