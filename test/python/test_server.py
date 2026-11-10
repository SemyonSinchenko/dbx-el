"""Protocol-level tests for the dbx companion server (fake session, no Spark)."""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PYDIR = ROOT / "python"
sys.path.insert(0, str(PYDIR))

from dbx_emacs.server import frame, read_message  # noqa: E402


def env():
    e = dict(os.environ)
    e["PYTHONPATH"] = str(PYDIR)
    e["DBX_EMACS_FAKE_SESSION"] = "1"
    return e


class Client:
    def __init__(self):
        self.proc = subprocess.Popen([sys.executable, "-m", "dbx_emacs", "serve"], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env())
        self.msgs: queue.Queue = queue.Queue()
        self.ids = iter(range(1, 10_000))
        self.notes: list[dict] = []
        self.input_reply = {"line": "typed"}
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        while True:
            m = read_message(self.proc.stdout)
            if m is None:
                return
            if m.get("method") == "input" and "id" in m:
                self.send({"jsonrpc": "2.0", "id": m["id"], "result": self.input_reply})
                continue
            self.msgs.put(m)

    def send(self, obj):
        self.proc.stdin.write(frame(obj))
        self.proc.stdin.flush()

    def request(self, method, params=None, timeout=10):
        mid = next(self.ids)
        self.send({"jsonrpc": "2.0", "id": mid, "method": method, "params": params or {}})
        return self.wait(lambda m: m.get("id") == mid, timeout)["result"]

    def wait(self, pred, timeout=10):
        for m in self.notes:  # notifications seen while waiting for something else
            if pred(m):
                return m
        end = time.time() + timeout
        while time.time() < end:
            try:
                m = self.msgs.get(timeout=0.1)
            except queue.Empty:
                continue
            if "method" in m:
                self.notes.append(m)
            if pred(m):
                return m
        raise AssertionError(f"timeout; notes so far: {self.notes[-10:]}")

    def run(self, code, mode="exec", timeout=10):
        req = self.request("execute", {"code": code, "mode": mode, "origin": {"file": "t.py", "line": 1}})
        if req.get("incomplete"):
            return req, None
        end = self.wait(lambda m: m.get("method") == "req/end" and m["params"]["req"] == req["req"], timeout)
        return req, end["params"]

    def output(self, req):
        return "".join(i["text"] for n in self.notes if n.get("method") == "output"
                       for i in n["params"]["items"] if i["req"] == req)

    def close(self):
        try:
            self.request("shutdown", timeout=3)
        except Exception:  # noqa: BLE001
            pass
        self.proc.kill()
        self.proc.wait()


@pytest.fixture
def client():
    c = Client()
    c.request("initialize", {"protocol": 1})
    c.wait(lambda m: m.get("method") == "session/state" and m["params"]["state"] == "connected")
    yield c
    c.close()


def test_framing_roundtrip():
    import io

    data = frame({"a": "ü"}) + frame({"b": 2})
    s = io.BytesIO(data)
    assert read_message(s) == {"a": "ü"}
    assert read_message(s) == {"b": 2}
    assert read_message(s) is None


def test_execute_output_and_status(client):
    req, end = client.run("print('hello')\nx = 41")
    assert end["status"] == "ok"
    time.sleep(0.2)
    assert "hello" in client.output(req["req"])
    req, end = client.run("x + 1", mode="interactive")
    assert end["status"] == "ok"
    time.sleep(0.2)
    assert "42" in client.output(req["req"])


def test_c_level_and_subprocess_output_is_captured(client):
    req, end = client.run("import os, subprocess\nos.write(1, b'raw-fd\\n')\n"
                          "subprocess.run(['echo', 'from-child'])")
    assert end["status"] == "ok"
    time.sleep(0.3)
    out = client.output(req["req"])
    assert "raw-fd" in out and "from-child" in out


def test_error_reports_traceback_with_user_line(client):
    req, end = client.run("a = 1\nb = 1 / 0")
    assert end["status"] == "error"
    assert "ZeroDivisionError" in end["error"]
    time.sleep(0.2)
    out = client.output(req["req"])
    assert 't.py", line 2' in out
    assert "dbx_emacs" not in out


def test_system_exit_does_not_kill_server(client):
    _, end = client.run("raise SystemExit(3)")
    assert end["status"] == "error"
    _, end = client.run("y = 2")
    assert end["status"] == "ok"


def test_incomplete_interactive_input(client):
    r, end = client.run("def f():", mode="interactive")
    assert r == {"incomplete": True} and end is None
    _, end = client.run("def f():\n    return 5\n", mode="interactive")
    assert end["status"] == "ok"


def test_interrupt_running_python_loop(client):
    req = client.request("execute", {"code": "import time\nfor _ in range(200):\n    time.sleep(0.05)",
                                     "mode": "exec"})
    client.wait(lambda m: m.get("method") == "req/start" and m["params"]["req"] == req["req"])
    time.sleep(0.2)
    t0 = time.time()
    assert client.request("interrupt", {"req": req["req"]})["result"] == "ack"
    end = client.wait(lambda m: m.get("method") == "req/end" and m["params"]["req"] == req["req"])
    assert end["params"]["status"] == "interrupted"
    assert time.time() - t0 < 3
    # server is usable afterwards and a stale interrupt does nothing
    assert client.request("interrupt", {"req": req["req"]})["result"] == "stale"
    _, end = client.run("z = 1")
    assert end["status"] == "ok"


def test_interrupt_queued_job(client):
    first = client.request("execute", {"code": "import time; time.sleep(1)"})
    second = client.request("execute", {"code": "print('never')"})
    assert client.request("interrupt", {"req": second["req"]})["result"] == "queued-removed"
    end = client.wait(lambda m: m.get("method") == "req/end" and m["params"]["req"] == second["req"])
    assert end["params"]["status"] == "interrupted"
    client.wait(lambda m: m.get("method") == "req/end" and m["params"]["req"] == first["req"])
    time.sleep(0.2)
    assert "never" not in client.output(second["req"])


def test_stray_sigint_is_ignored(client):
    req = client.request("execute", {"code": "import os, signal, time\nos.kill(os.getpid(), signal.SIGINT)\n"
                                             "time.sleep(0.2)\nprint('survived')"})
    end = client.wait(lambda m: m.get("method") == "req/end" and m["params"]["req"] == req["req"])
    assert end["params"]["status"] == "ok"


def test_input_goes_through_client(client):
    client.input_reply = {"line": "from-emacs"}
    req, end = client.run("v = input()\nprint('got', v)")
    assert end["status"] == "ok"
    time.sleep(0.2)
    assert "got from-emacs" in client.output(req["req"])


def test_complete(client):
    client.run("my_long_variable = 1")
    cands = client.request("complete", {"text": "my_long"})["candidates"]
    assert "my_long_variable" in cands


def test_probe():
    out = subprocess.run([sys.executable, "-m", "dbx_emacs", "probe"], env=env(),
                         capture_output=True, text=True, check=True)
    info = json.loads(out.stdout)
    assert "packages" in info and "databricks-connect" in info["packages"]


# ------------------------------------------------------------------ run-file


def _run_both(tmp_path, script, args, module=False):
    target = ["-m", script] if module else [str(script)]
    plain = subprocess.run([sys.executable, *target, *args], cwd=tmp_path, capture_output=True, text=True,
                           env=env())
    ours = subprocess.run([sys.executable, "-m", "dbx_emacs", "run", *(["--module", script] if module else
                                                                       [str(script)]), *args],
                          cwd=tmp_path, capture_output=True, text=True, env=env())
    return plain, ours


def test_run_matches_plain_python(tmp_path):
    (tmp_path / "helper.py").write_text("VALUE = 7\n")
    (tmp_path / "data.txt").write_text("payload\n")
    script = tmp_path / "main.py"
    script.write_text(
        "import sys, helper\n"
        "print(sys.argv)\n"
        "print(helper.VALUE, open('data.txt').read().strip(), __name__)\n"
        "if '--fail' in sys.argv: raise SystemExit(2)\n")
    plain, ours = _run_both(tmp_path, script, ["a", "--flag"])
    assert plain.returncode == ours.returncode == 0
    assert plain.stdout == ours.stdout
    plain, ours = _run_both(tmp_path, script, ["--fail"])
    assert plain.returncode == ours.returncode == 2


def test_run_uncaught_exception(tmp_path):
    script = tmp_path / "boom.py"
    script.write_text("def f():\n    raise ValueError('bad')\nf()\n")
    plain, ours = _run_both(tmp_path, script, [])
    assert plain.returncode == ours.returncode == 1
    assert "ValueError: bad" in ours.stderr
    assert "dbx_emacs" not in ours.stderr and "runpy" not in ours.stderr


def test_run_module(tmp_path):
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "__main__.py").write_text("import sys; print('module', sys.argv[1:])\n")
    plain, ours = _run_both(tmp_path, "pkg", ["x"], module=True)
    assert plain.returncode == ours.returncode == 0
    assert "module ['x']" in ours.stdout
