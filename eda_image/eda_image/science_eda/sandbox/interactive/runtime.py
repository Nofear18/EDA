"""Dedicated, non-reusable Innovus/PrimeTime runtime ownership."""

from __future__ import annotations

import multiprocessing
import os
import signal
import threading
import time
import uuid
from collections.abc import Callable
from multiprocessing.connection import Connection
from typing import Any

from science_eda.config import SandboxConfig
from science_eda.sandbox.interactive.process_identity import (
    process_is_running,
    read_process_identity,
    recorded_group_state,
)
from science_eda.sandbox.interactive.tool_config import (
    module_activated_tool_command,
    validate_tool_version,
)
from science_eda.sandbox.worker import worker_main


class RuntimeStartError(RuntimeError):
    pass


class WorkspaceCwdError(RuntimeStartError):
    """The tool started but could not adopt the caller workspace cwd."""


class RuntimeLostError(RuntimeError):
    pass


class RuntimeCommandTimeout(RuntimeError):
    pass


class RuntimeOutputSinkError(RuntimeError):
    """The worker stayed responsive but could not open the execution sink."""


def _leader_alive(pid: int | None) -> bool:
    return process_is_running(pid)


class DedicatedToolRuntime:
    """One fresh physical tool process owned by one WorkspaceSession."""

    def __init__(
        self,
        config: SandboxConfig,
        *,
        tool_kind: str,
        scratch_dir: str,
        workspace_path: str,
        tool_version: str | None = None,
        tool_env: dict[str, str] | None = None,
        on_lost: Callable[[str], None] | None = None,
    ) -> None:
        if tool_kind not in {"innovus", "primetime"}:
            raise ValueError(f"unsupported interactive tool kind: {tool_kind!r}")
        self.config = config
        self.tool_kind = tool_kind
        self.scratch_dir = os.path.realpath(scratch_dir)
        # The service passes an already canonical path.  Preserve that exact
        # identity so a symlink swap between validation and cwd adoption is
        # detected by `_set_workspace_cwd` instead of silently re-canonicalized.
        self.workspace_path = os.path.abspath(workspace_path)
        self.tool_version = validate_tool_version(tool_version)
        self.runtime_instance_id = f"rti_{uuid.uuid4().hex}"
        self.worker_process_id: int | None = None
        self.worker_process_identity: str | None = None
        self.process_id: int | None = None
        self.process_identity: str | None = None
        self._tool_env = dict(tool_env) if tool_env is not None else None
        self._on_lost = on_lost
        self._ctx = multiprocessing.get_context("spawn")
        self._process: multiprocessing.Process | None = None
        self._conn: Connection | None = None
        self._send_lock = threading.Lock()
        self._command_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._termination_lock = threading.Lock()
        self._stopping = False
        self._lost_notified = False
        self._execution_inflight = False
        self._monitor_loss_suppressed = False
        self._monitor: threading.Thread | None = None
        self._process_started_callback: Callable[
            [int | None, str | None, int | None, str | None],
            None,
        ] | None = None

    @property
    def is_alive(self) -> bool:
        process = self._process
        return bool(
            process is not None
            and process.is_alive()
            and _leader_alive(self.process_id)
            and not self._stopping
        )

    @property
    def processes_alive(self) -> bool:
        process = self._process
        # A child can exist for a tiny interval before its PID is published.
        # Once recorded, birth identity is authoritative: a reused PID/PGID is
        # not one of our processes, while an unknown identity stays
        # conservatively live until ownership can be confirmed.
        if (
            process is not None
            and process.is_alive()
            and self.worker_process_id is None
        ):
            return True
        return any(
            self._recorded_group_state(pid, identity) != "dead"
            for pid, identity in (
                (self.worker_process_id, self.worker_process_identity),
                (self.process_id, self.process_identity),
            )
        )

    def set_process_started_callback(
        self,
        callback: Callable[
            [int | None, str | None, int | None, str | None],
            None,
        ],
    ) -> None:
        """Persist worker/tool PIDs as soon as each process exists."""

        self._process_started_callback = callback

    def _report_processes(self) -> None:
        callback = self._process_started_callback
        if callback is not None:
            callback(
                self.worker_process_id,
                self.worker_process_identity,
                self.process_id,
                self.process_identity,
            )

    def start(self) -> None:
        innovus_bin = self.config.innovus_bin
        innovus_args = tuple(self.config.innovus_args)
        primetime_bin = self.config.primetime_bin
        primetime_args = tuple(self.config.primetime_args)
        if self.tool_version is not None:
            if self.tool_kind == "innovus":
                innovus_bin, innovus_args = module_activated_tool_command(
                    self.config.interactive_module_shell_path,
                    self.tool_kind,
                    self.tool_version,
                    innovus_bin,
                    innovus_args,
                )
            else:
                primetime_bin, primetime_args = module_activated_tool_command(
                    self.config.interactive_module_shell_path,
                    self.tool_kind,
                    self.tool_version,
                    primetime_bin,
                    primetime_args,
                )
        parent_conn, child_conn = self._ctx.Pipe(duplex=True)
        process = self._ctx.Process(
            target=worker_main,
            args=(
                child_conn,
                self.scratch_dir,
                self.tool_kind,
                self.config.shell_path,
                self.config.tcl_shell,
                innovus_bin,
                list(innovus_args),
                self.config.innovus_startup_tcl,
                float(self.config.innovus_startup_timeout),
                int(self.config.memory_mb),
                int(self.config.max_pids),
                primetime_bin,
                list(primetime_args),
                self.config.primetime_startup_tcl,
                float(self.config.primetime_startup_timeout),
                self._tool_env,
                int(self.config.interactive_reader_chunk_bytes),
                os.getpid(),
                int(self.config.interactive_output_preview_max_bytes),
            ),
        )
        try:
            # Serialize only the physical spawn boundary with terminate().  A
            # shutdown which wins first prevents the spawn; one which follows
            # immediately sees the published Process handle and can stop it.
            with self._termination_lock:
                with self._state_lock:
                    if self._stopping:
                        raise RuntimeStartError(
                            "interactive runtime was stopped before startup"
                        )
                    process.start()
                    self._process = process
                    self._conn = parent_conn
                    self.worker_process_id = process.pid
                    self.worker_process_identity = read_process_identity(process.pid)
        except BaseException:
            parent_conn.close()
            child_conn.close()
            raise
        child_conn.close()
        self._report_processes()
        timeout = self._startup_timeout()
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline:
                message = self._recv_one(deadline)
                op = message.get("op")
                if op == "tool_started":
                    raw_pid = message.get("tool_process_pid")
                    self.process_id = int(raw_pid) if raw_pid else None
                    self.process_identity = read_process_identity(self.process_id)
                    self._report_processes()
                    continue
                if op == "ready":
                    raw_pid = message.get("tool_process_pid")
                    if raw_pid:
                        self.process_id = int(raw_pid)
                        self.process_identity = read_process_identity(self.process_id)
                    self._report_processes()
                    break
                if op == "startup_error":
                    raise RuntimeStartError(str(message.get("error", "tool startup failed")))
                raise RuntimeStartError(f"unexpected worker startup message: {message!r}")
            else:
                raise RuntimeStartError("interactive tool startup timed out")
            self._run_healthcheck(timeout)
            self._set_workspace_cwd(timeout)
        except Exception:
            try:
                self.terminate()
            except Exception:
                pass
            raise
        self._monitor = threading.Thread(
            target=self._monitor_loop,
            name=f"science-eda-{self.runtime_instance_id}-monitor",
            daemon=True,
        )
        self._monitor.start()

    def prepare_execution(
        self,
        *,
        execution_id: str,
        log_path: str,
        preview_bytes: int,
        write_budget_bytes: int,
        encoding: str = "utf-8",
    ) -> None:
        with self._command_lock:
            message = self._request(
                {
                    "op": "prepare_execution",
                    "execution_id": execution_id,
                    "log_path": log_path,
                    "preview_bytes": int(preview_bytes),
                    "write_budget_bytes": int(write_budget_bytes),
                    "encoding": encoding,
                },
                timeout=10.0,
            )
        if message.get("op") == "prepare_execution_error":
            detail = message.get("error", "worker did not prepare output sink")
            raise RuntimeOutputSinkError(str(detail))
        if message.get("op") != "output_sink_ready":
            detail = message.get("error", "worker did not prepare output sink")
            raise RuntimeLostError(str(detail))
        if str(message.get("execution_id")) != execution_id:
            raise RuntimeLostError("worker prepared the wrong execution id")

    def execute_prepared(self, execution_id: str, code: str, timeout: float) -> Any:
        with self._state_lock:
            self._execution_inflight = True
        message: dict[str, Any] | None = None
        try:
            with self._command_lock:
                message = self._request(
                    {
                        "op": "execute_prepared",
                        "execution_id": execution_id,
                        "code": code,
                        "timeout": float(timeout),
                    },
                    # The executor may spend up to two seconds waiting for the
                    # killed process and five seconds draining both pipes before
                    # it can publish a trustworthy TIMED_OUT summary.
                    timeout=float(timeout)
                    + max(10.0, min(30.0, float(timeout) * 0.1)),
                )
        finally:
            with self._state_lock:
                if message is not None and message.get("op") == "execution_summary":
                    # The worker summary is authoritative; keep the monitor
                    # from racing the service's durable terminal transition.
                    self._monitor_loss_suppressed = True
                self._execution_inflight = False
        assert message is not None
        if message.get("op") != "execution_summary":
            detail = message.get("error", "worker did not return execution summary")
            raise RuntimeLostError(str(detail))
        if str(message.get("execution_id")) != execution_id:
            raise RuntimeLostError("worker returned the wrong execution id")
        summary = message.get("summary")
        return summary

    def resume_monitoring(self) -> None:
        """Resume process-loss callbacks after a summary is durably recorded."""

        with self._state_lock:
            if not self._stopping:
                self._monitor_loss_suppressed = False

    def healthcheck(self, timeout: float | None = None) -> None:
        """Confirm that the existing physical tool runtime is still usable."""

        effective_timeout = self._startup_timeout() if timeout is None else float(timeout)
        with self._command_lock:
            self._run_healthcheck(effective_timeout)

    def terminate(self) -> bool:
        with self._termination_lock:
            with self._state_lock:
                self._stopping = True
            process = self._process
            conn = self._conn
            was_alive = self.processes_alive
            if conn is not None and process is not None and process.is_alive():
                try:
                    with self._send_lock:
                        conn.send({"op": "shutdown"})
                except Exception:
                    pass
            if process is not None and process.is_alive():
                process.join(timeout=1.0)

            # Kill both independently created process groups.  This remains
            # necessary after either leader exits because descendants may
            # still hold tool pipes or scratch files open.
            self._kill_pid_group(self.process_id, self.process_identity)
            self._kill_pid_group(
                self.worker_process_id,
                self.worker_process_identity,
            )
            if process is not None and process.is_alive():
                process.join(timeout=2.0)
            if process is not None and process.is_alive():
                # ``multiprocessing.Process.kill`` targets the integer PID
                # directly.  Apply the same birth-identity gate as killpg so a
                # stale Process object cannot signal a reused PID.
                if (
                    self._recorded_group_state(
                        self.worker_process_id,
                        self.worker_process_identity,
                    )
                    == "owned"
                ):
                    process.kill()
                    process.join(timeout=1.0)

            if conn is not None:
                try:
                    conn.close()
                except OSError:
                    pass
            self._conn = None

            deadline = time.monotonic() + 2.0
            while self.processes_alive and time.monotonic() < deadline:
                if process is not None and process.is_alive():
                    process.join(timeout=0.05)
                else:
                    time.sleep(0.05)
            if self.processes_alive:
                raise RuntimeLostError(
                    "interactive runtime processes could not be confirmed stopped"
                )
            self.worker_process_id = None
            self.worker_process_identity = None
            self.process_id = None
            self.process_identity = None
            return was_alive

    def _run_healthcheck(self, timeout: float) -> None:
        code = (
            self.config.primetime_interactive_healthcheck_tcl
            if self.tool_kind == "primetime"
            else self.config.innovus_interactive_healthcheck_tcl
        )
        message = self._request(
            {"op": "healthcheck", "code": code, "timeout": timeout},
            timeout=timeout + 1.0,
        )
        if message.get("op") != "healthcheck_done":
            raise RuntimeStartError(str(message.get("error", "tool healthcheck failed")))
        result = message.get("result")
        if result is None or int(result.exit_code) != 0:
            detail = getattr(result, "stderr", "") or getattr(result, "stdout", "")
            raise RuntimeStartError(f"tool healthcheck failed: {str(detail)[:4096]}")

    def _set_workspace_cwd(self, timeout: float) -> None:
        message = self._request(
            {
                "op": "set_workspace_cwd",
                "path": self.workspace_path,
                "timeout": timeout,
            },
            timeout=timeout + 1.0,
        )
        if message.get("op") != "workspace_cwd_set":
            raise WorkspaceCwdError(
                str(message.get("error", "workspace cwd switch failed"))
            )
        result = message.get("result")
        if result is None or int(result.exit_code) != 0:
            detail = getattr(result, "stderr", "") or getattr(result, "stdout", "")
            raise WorkspaceCwdError(
                f"workspace cwd switch failed: {str(detail)[:4096]}"
            )
        reported = os.path.realpath(str(message.get("workspace_path", "")))
        if reported != self.workspace_path:
            raise WorkspaceCwdError(
                f"workspace cwd verification mismatch: expected {self.workspace_path!r}"
            )

    def _request(self, payload: dict[str, Any], *, timeout: float) -> dict[str, Any]:
        if not self.is_alive and payload.get("op") not in {"healthcheck", "set_workspace_cwd"}:
            raise RuntimeLostError("interactive tool runtime is not alive")
        conn = self._conn
        if conn is None:
            raise RuntimeLostError("interactive worker connection is closed")
        try:
            with self._send_lock:
                conn.send(payload)
        except (BrokenPipeError, EOFError, OSError) as exc:
            self._notify_lost("WORKER_PIPE_CLOSED")
            raise RuntimeLostError("interactive worker connection is closed") from exc
        deadline = time.monotonic() + timeout
        try:
            return self._recv_one(deadline)
        except RuntimeCommandTimeout:
            self.terminate()
            raise

    def _recv_one(self, deadline: float) -> dict[str, Any]:
        conn = self._conn
        process = self._process
        if conn is None:
            raise RuntimeLostError("interactive worker connection is closed")
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeCommandTimeout("interactive worker response timed out")
            try:
                if conn.poll(min(0.1, remaining)):
                    message = conn.recv()
                    if not isinstance(message, dict):
                        raise RuntimeLostError("interactive worker returned an invalid message")
                    return message
            except (EOFError, OSError) as exc:
                self._notify_lost("WORKER_PIPE_CLOSED")
                raise RuntimeLostError("interactive worker connection closed") from exc
            if process is not None and not process.is_alive():
                self._notify_lost("WORKER_EXITED")
                raise RuntimeLostError("interactive worker exited")

    def _monitor_loop(self) -> None:
        while True:
            time.sleep(0.5)
            with self._state_lock:
                if self._stopping:
                    return
                if self._execution_inflight or self._monitor_loss_suppressed:
                    continue
            process = self._process
            if process is None or not process.is_alive():
                self._notify_lost("WORKER_EXITED")
                return
            if not _leader_alive(self.process_id):
                self._notify_lost("PROCESS_EXITED")
                return

    def _notify_lost(self, reason: str) -> None:
        with self._state_lock:
            if self._stopping or self._lost_notified:
                return
            self._lost_notified = True
        if self._on_lost is not None:
            try:
                self._on_lost(reason)
            except Exception:
                pass

    def _startup_timeout(self) -> float:
        if self.tool_kind == "primetime":
            return float(self.config.primetime_startup_timeout)
        return float(self.config.innovus_startup_timeout)

    @staticmethod
    def _recorded_group_state(
        pid: int | None,
        expected_identity: str | None,
    ) -> str:
        if pid is None or pid <= 1:
            return "dead"
        # Never signal the daemon itself, even if corrupted durable/runtime
        # state happens to contain its PID.  Unknown ownership must fail
        # conservatively instead.
        if pid == os.getpid():
            return "unknown"
        return recorded_group_state(pid, expected_identity)

    @classmethod
    def _kill_pid_group(
        cls,
        pid: int | None,
        expected_identity: str | None,
    ) -> None:
        if cls._recorded_group_state(pid, expected_identity) != "owned":
            return
        assert pid is not None
        try:
            if hasattr(os, "killpg"):
                os.killpg(pid, signal.SIGKILL)
            else:
                os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass
