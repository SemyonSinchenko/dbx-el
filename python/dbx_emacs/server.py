"""JSON-RPC 2.0 server over stdio that executes user code for Emacs.

Threads:
  main     runs jobs (user code) one at a time
  reader   parses requests, queues jobs, validates interrupts
  writer   the only writer of the protocol fd; batches output/progress
  pumps    drain redirected fd 1/2 into output notifications
  connect  creates the Databricks session in the background
"""

from __future__ import annotations

import ast
import codeop
import io
import itertools
import json
import os
import queue
import rlcompleter
import signal
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field

import __main__

from . import PROTOCOL, __version__, session as sess

IDLE, STARTING, EXECUTING, FINALIZING = "idle", "starting", "executing", "finalizing"
MAX_OUTPUT_CHUNK = 32 * 1024
FLUSH_INTERVAL = 0.05
PROGRESS_INTERVAL = 0.2


# ---------------------------------------------------------------- framing


def read_message(stream) -> dict | None:
    """Read one Content-Length framed JSON message, or None at EOF."""
    length = None
    while True:
        line = stream.readline()
        if not line:
            return None
        line = line.strip()
        if not line:
            if length is not None:
                break
            continue
        name, _, value = line.decode("ascii", "replace").partition(":")
        if name.lower() == "content-length":
            length = int(value.strip())
    body = b""
    while len(body) < length:
        chunk = stream.read(length - len(body))
        if not chunk:
            return None
        body += chunk
    return json.loads(body.decode("utf-8"))


def frame(obj: dict) -> bytes:
    data = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
    return b"Content-Length: %d\r\n\r\n" % len(data) + data


# ---------------------------------------------------------------- state


@dataclass
class Job:
    req: int
    kind: str
    code: str = ""
    origin: dict = field(default_factory=dict)
    mode: str = "exec"


@dataclass
class State:
    current: int | None = None
    pending_cancel: int | None = None
    ki_raised: bool = False
    phase: str = IDLE


class Server:
    def __init__(self, proto_in, proto_out):
        self.proto_in = proto_in
        self.proto_out = proto_out
        self.out_q: queue.Queue = queue.Queue(maxsize=1024)
        self.output: list[dict] = []
        self.output_lock = threading.Lock()
        self.progress: dict[str, dict] = {}
        self.progress_lock = threading.Lock()
        self.jobs: queue.Queue = queue.Queue()
        self.queued: dict[int, Job] = {}
        self.state = State()
        self.state_lock = threading.Lock()
        self.req_ids = itertools.count(1)
        self.client_ids = itertools.count(1)
        self.pending_client: dict[int, queue.Queue] = {}
        self.namespace = __main__.__dict__
        self.namespace.setdefault("__name__", "__main__")
        self.spark = None
        self.session_ready = threading.Event()
        self.alive = True
        self.main_ident = threading.get_ident()

    # ------------------------------------------------------------ sending

    def send(self, obj: dict) -> None:
        try:
            self.out_q.put_nowait(obj)
        except queue.Full:
            self.alive = False

    def notify(self, method: str, **params) -> None:
        self.send({"jsonrpc": "2.0", "method": method, "params": params})

    def reply(self, msg_id, result=None, error=None) -> None:
        msg = {"jsonrpc": "2.0", "id": msg_id}
        if error is not None:
            msg["error"] = {"code": -32000, "message": error}
        else:
            msg["result"] = result if result is not None else {}
        self.send(msg)

    def add_output(self, stream: str, text: str) -> None:
        with self.output_lock:
            req = self.state.current
            if self.output and self.output[-1]["stream"] == stream and self.output[-1]["req"] == req:
                self.output[-1]["text"] += text
            else:
                self.output.append({"req": req, "stream": stream, "text": text})

    def writer(self) -> None:
        last_progress = 0.0
        while self.alive or not self.out_q.empty():
            try:
                obj = self.out_q.get(timeout=FLUSH_INTERVAL)
            except queue.Empty:
                obj = None
            batch = []
            with self.output_lock:
                if self.output:
                    batch, self.output = self.output, []
            if batch:
                self._write({"jsonrpc": "2.0", "method": "output", "params": {"items": batch}})
            now = time.monotonic()
            if now - last_progress >= PROGRESS_INTERVAL:
                with self.progress_lock:
                    items = list(self.progress.values())
                    self.progress = {k: v for k, v in self.progress.items() if not v.get("done")}
                if items:
                    self._write({"jsonrpc": "2.0", "method": "progress", "params": {"items": items}})
                last_progress = now
            if obj is not None:
                self._write(obj)

    def _write(self, obj: dict) -> None:
        try:
            self.proto_out.write(frame(obj))
            self.proto_out.flush()
        except (BrokenPipeError, OSError, ValueError):
            self.alive = False

    # ------------------------------------------------------------ client requests

    def ask_client(self, method: str, params: dict, timeout: float | None = None):
        msg_id = next(self.client_ids) + 1_000_000
        box: queue.Queue = queue.Queue(maxsize=1)
        self.pending_client[msg_id] = box
        self.send({"jsonrpc": "2.0", "id": msg_id, "method": method, "params": params})
        try:
            return box.get(timeout=timeout)
        finally:
            self.pending_client.pop(msg_id, None)

    # ------------------------------------------------------------ reader

    def reader(self) -> None:
        while self.alive:
            try:
                msg = read_message(self.proto_in)
            except Exception:  # noqa: BLE001
                msg = None
            if msg is None:
                self.alive = False
                self.jobs.put(None)
                if self.state.phase == EXECUTING:
                    self._wake_main()
                return
            if "method" not in msg:
                box = self.pending_client.get(msg.get("id"))
                if box is not None:
                    box.put(msg.get("result") or {})
                continue
            try:
                self.dispatch(msg)
            except Exception as exc:  # noqa: BLE001
                if "id" in msg:
                    self.reply(msg["id"], error=f"{type(exc).__name__}: {exc}")

    def dispatch(self, msg: dict) -> None:
        method, params, msg_id = msg["method"], msg.get("params") or {}, msg.get("id")
        if method == "initialize":
            self.reply(msg_id, {"serverVersion": __version__, "protocol": PROTOCOL,
                                "python": sys.version.split()[0], "pid": os.getpid()})
            threading.Thread(target=self.connect, name="dbx-connect", daemon=True).start()
        elif method == "execute":
            code = params.get("code") or ""
            mode = params.get("mode", "exec")
            if mode == "interactive":
                try:
                    if codeop.compile_command(code, "<input>", "exec") is None:
                        self.reply(msg_id, {"incomplete": True})
                        return
                except (SyntaxError, ValueError, OverflowError):
                    pass  # reported when the job runs
            job = Job(next(self.req_ids), "code", code, params.get("origin") or {}, mode)
            with self.state_lock:
                self.queued[job.req] = job
                position = len(self.queued)
            self.jobs.put(job)
            self.reply(msg_id, {"req": job.req, "position": position})
        elif method == "interrupt":
            self.reply(msg_id, {"result": self.interrupt(int(params.get("req", 0)))})
        elif method == "interruptAll":
            ok = self._remote_interrupt(None)
            self.reply(msg_id, {"ok": ok})
        elif method == "complete":
            self.reply(msg_id, {"candidates": self.complete(params.get("text", ""))})
        elif method == "ping":
            self.reply(msg_id, {"time": time.time()})
        elif method == "session/reconnect":
            self.session_ready.clear()
            threading.Thread(target=self.connect, name="dbx-connect", daemon=True).start()
            self.reply(msg_id, {"ok": True})
        elif method == "shutdown":
            self.reply(msg_id, {"ok": True})
            self.alive = False
            self.jobs.put(None)
        elif msg_id is not None:
            self.reply(msg_id, error=f"unknown method {method}")

    # ------------------------------------------------------------ interrupt

    def interrupt(self, req: int) -> str:
        with self.state_lock:
            job = self.queued.pop(req, None)
            if job is not None:
                job.kind = "cancelled"
                self.notify("req/end", req=req, status="interrupted")
                return "queued-removed"
            if self.state.current != req or self.state.phase != EXECUTING:
                return "stale"
            if self.state.pending_cancel == req:
                return "ack"
            self.state.pending_cancel = req
        self._wake_main()
        threading.Thread(target=self._remote_interrupt, args=(req,), daemon=True).start()
        return "ack"

    def _wake_main(self) -> None:
        if hasattr(signal, "pthread_kill"):
            signal.pthread_kill(self.main_ident, signal.SIGINT)
        else:  # Windows: takes effect at the next bytecode boundary
            import _thread  # noqa: PLC0415

            _thread.interrupt_main()

    def _remote_interrupt(self, req: int | None) -> bool:
        spark = self.spark
        if spark is None:
            return False
        try:
            if req is None:
                spark.interruptAll()
            else:
                spark.interruptTag(f"dbx-emacs-{req}")
            outcome = "ok"
        except Exception:  # noqa: BLE001
            outcome = "failed"
        if req is not None:
            self.notify("cancel/remote", req=req, outcome=outcome)
        return outcome == "ok"

    def sigint_handler(self, _signum, _frame) -> None:
        st = self.state
        if st.phase == EXECUTING and st.pending_cancel == st.current and not st.ki_raised:
            st.ki_raised = True
            st.pending_cancel = None
            raise KeyboardInterrupt
        # anything else (late wakeups, stray signals) is dropped

    # ------------------------------------------------------------ session

    def connect(self) -> None:
        self.notify("session/state", state="connecting")
        if sess.fake_mode():
            sess.inject_globals(self.namespace, None)
            self.notify("session/state", state="connected", target={"host": "fake", "compute": "none"},
                        caps={"progress": False, "tags": False})
            self.session_ready.set()
            return
        try:
            spark = sess.create_session()
            self.spark = spark
            sess.inject_globals(self.namespace, spark)
            caps = {"tags": hasattr(spark, "addTag"),
                    "progress": hasattr(spark, "registerProgressHandler")}
            if caps["progress"]:
                try:
                    spark.registerProgressHandler(self.on_progress)
                except Exception:  # noqa: BLE001
                    caps["progress"] = False
            session_id = getattr(spark, "session_id", None)
            self.notify("session/state", state="connected", target=sess.describe_target(),
                        sessionId=str(session_id) if session_id else None, caps=caps)
        except Exception as exc:  # noqa: BLE001
            self.notify("session/state", state="failed", error=f"{type(exc).__name__}: {exc}")
        finally:
            self.session_ready.set()

    def on_progress(self, stages, inflight_tasks, operation_id, done) -> None:
        try:
            req = self.state.current if threading.get_ident() == self.main_ident else None
            st = []
            for s in stages or []:
                st.append({"id": getattr(s, "stage_id", None),
                           "total": getattr(s, "num_tasks", 0),
                           "done": getattr(s, "num_completed_tasks", 0)})
            with self.progress_lock:
                self.progress[str(operation_id)] = {"req": req, "opId": str(operation_id), "stages": st,
                                                    "inflight": inflight_tasks, "done": bool(done)}
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------ completion

    def complete(self, text: str) -> list[str]:
        completer = rlcompleter.Completer(self.namespace)
        out = []
        for i in range(500):
            try:
                c = completer.complete(text, i)
            except Exception:  # noqa: BLE001
                break
            if c is None:
                break
            out.append(c)
        return out

    # ------------------------------------------------------------ jobs

    def run_job(self, job: Job) -> None:
        st = self.state
        with self.state_lock:
            self.queued.pop(job.req, None)
            st.current, st.pending_cancel, st.ki_raised = job.req, None, False
            st.phase = STARTING
        status, err, tag, spark = "interrupted", None, None, None
        try:
            self.notify("req/start", req=job.req, kind=job.kind, origin=job.origin)
            try:
                self.session_ready.wait()
                spark = self.spark
                if spark is not None and hasattr(spark, "addTag"):
                    tag = f"dbx-emacs-{job.req}"
                    spark.addTag(tag)
                st.phase = EXECUTING
                try:
                    self.execute(job)
                    status = "ok"
                finally:
                    st.phase = FINALIZING
            except KeyboardInterrupt:
                status = "interrupted"
                sys.stderr.write("KeyboardInterrupt\n")
            except BaseException as exc:  # noqa: BLE001  (SystemExit included: server survives)
                status, err = "error", f"{type(exc).__name__}: {exc}"
                try:
                    tb = traceback.TracebackException.from_exception(exc)
                    stack = [f for f in tb.stack if "dbx_emacs" not in (f.filename or "")]
                    tb.stack = traceback.StackSummary.from_list(stack)
                    sys.stderr.write("".join(tb.format()))
                except Exception:  # noqa: BLE001
                    pass
        except KeyboardInterrupt:
            status = "interrupted"
        finally:
            st.phase = FINALIZING
            if tag and spark is not None:
                try:
                    spark.removeTag(tag)
                except Exception:  # noqa: BLE001
                    pass
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.02)  # let the pumps drain before req/end
            with self.state_lock:
                st.current = st.pending_cancel = None
                st.phase = IDLE
            self.notify("req/end", req=job.req, status=status, error=err)

    def execute(self, job: Job) -> None:
        filename = job.origin.get("file") or "<dbx>"
        line = int(job.origin.get("line") or 1)
        tree = ast.parse(job.code, filename, "exec")
        if line > 1:
            ast.increment_lineno(tree, line - 1)
        last = None
        if job.mode == "interactive" and tree.body and isinstance(tree.body[-1], ast.Expr):
            last = ast.Expression(tree.body.pop().value)
        exec(compile(tree, filename, "exec"), self.namespace)  # noqa: S102
        if last is not None:
            value = eval(compile(last, filename, "eval"), self.namespace)  # noqa: S307
            if value is not None:
                self.namespace["_"] = value
                print(repr(value))

    def job_loop(self) -> None:
        while self.alive:
            try:
                job = self.jobs.get()
            except KeyboardInterrupt:
                continue
            if job is None:
                break
            if job.kind == "cancelled":
                continue
            try:
                self.run_job(job)
            except KeyboardInterrupt:
                pass


# ---------------------------------------------------------------- stdio


class InputProxy(io.TextIOBase):
    """sys.stdin that asks Emacs for each line."""

    def __init__(self, server: Server):
        self.server = server

    def readable(self) -> bool:
        return True

    def readline(self, size: int = -1) -> str:
        result = self.server.ask_client("input", {"req": self.server.state.current, "prompt": ""})
        if result.get("eof"):
            return ""
        return (result.get("line") or "") + "\n"

    def read(self, size: int = -1) -> str:
        return self.readline()


def _pump(server: Server, fd: int, stream: str) -> None:
    with os.fdopen(fd, "rb", buffering=0) as f:
        while True:
            data = f.read(MAX_OUTPUT_CHUNK)
            if not data:
                return
            server.add_output(stream, data.decode("utf-8", "replace"))


def serve() -> int:
    proto_in = os.fdopen(os.dup(0), "rb", buffering=0)
    proto_out = os.fdopen(os.dup(1), "wb", buffering=0)
    server = Server(proto_in, proto_out)

    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)
    os.close(devnull)
    for fd, name in ((1, "stdout"), (2, "stderr")):
        r, w = os.pipe()
        os.dup2(w, fd)
        os.close(w)
        threading.Thread(target=_pump, args=(server, r, name), daemon=True).start()
    sys.stdout = io.TextIOWrapper(os.fdopen(os.dup(1), "wb", buffering=0), encoding="utf-8",
                                  errors="backslashreplace", line_buffering=True, write_through=True)
    sys.stderr = io.TextIOWrapper(os.fdopen(os.dup(2), "wb", buffering=0), encoding="utf-8",
                                  errors="backslashreplace", line_buffering=True, write_through=True)
    sys.stdin = InputProxy(server)

    signal.signal(signal.SIGINT, server.sigint_handler)
    threading.Thread(target=server.writer, name="dbx-writer", daemon=True).start()
    threading.Thread(target=server.reader, name="dbx-reader", daemon=True).start()
    server.job_loop()
    server.alive = False
    time.sleep(FLUSH_INTERVAL * 2)
    return 0
