"""Shared bounded physical worker pool primitives for Tcl-based EDA tools."""

from __future__ import annotations

import logging
import multiprocessing
import os
import shutil
import threading
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from science_eda.config import SandboxConfig
from science_eda.exceptions import ExecutionError
from science_eda.exceptions import TimeoutError as ExecutionTimeoutError
from science_eda.sandbox.types import ExecutionResult
from science_eda.sandbox.worker import worker_main

if TYPE_CHECKING:
    from multiprocessing.connection import Connection
    from multiprocessing.context import BaseContext


@dataclass
class PooledToolWorker:
    worker_id: str
    working_dir: str
    conn: Connection
    process: multiprocessing.Process
    tool_process_pid: int | None = None
    incomplete_reset_warning_seen: bool = False
    sessions_served: int = 0


@dataclass(frozen=True)
class PoolCounts:
    pool_size: int
    active: int
    idle: int
    starting: int


WorkerT = TypeVar("WorkerT", bound=PooledToolWorker)


class ToolWorkerPoolBase(Generic[WorkerT]):
    """Common lifecycle for one-tool-process-per-logical-session pools."""

    def __init__(
        self,
        config: SandboxConfig,
        ctx: BaseContext,
        sandbox_root: str,
        *,
        pool_name: str,
        worker_root_name: str,
        logger: logging.Logger,
        pool_size: int,
        enabled: bool = True,
        reuse_enabled: bool = True,
        prewarm_enabled: bool = False,
        replenish_enabled: bool = True,
        prewarm_concurrency: int = 16,
    ) -> None:
        if pool_size <= 0:
            raise ValueError(f"{pool_name}_pool_size must be positive")
        self._config = config
        self._ctx = ctx
        self._pool_name = pool_name
        self._logger = logger
        self._enabled = enabled
        self._pool_size = pool_size
        self._reuse_enabled = reuse_enabled
        self._prewarm_enabled = prewarm_enabled
        self._replenish_enabled = replenish_enabled
        self._prewarm_concurrency = max(1, prewarm_concurrency)
        self._worker_root = os.path.join(os.path.abspath(sandbox_root), worker_root_name)
        os.makedirs(self._worker_root, exist_ok=True)
        self._cond = threading.Condition(threading.RLock())
        self._idle: list[WorkerT] = []
        self._leased: dict[str, WorkerT] = {}
        self._starting_workers: dict[str, WorkerT] = {}
        self._starting = 0
        self._close_generation = 0
        self._target_total = self._pool_size if self._prewarm_enabled else 0
        self._prewarm_complete = not self._prewarm_enabled
        self._supervisor_stop = False
        self._supervisor_thread: threading.Thread | None = None
        self._supervisor_backoff_until = 0.0

    @property
    def enabled(self) -> bool:
        return self._enabled

    def lease(self, session_id: str, session_working_dir: str) -> WorkerT:
        if not self._enabled:
            raise ExecutionError(f"{self._display_name()} worker pool is disabled")

        with self._cond:
            lease_generation = self._close_generation
        while True:
            worker, source, wait_seconds, queued = self._reserve_or_start(
                session_id,
                lease_generation,
            )
            try:
                self._prepare_worker(worker, session_id, session_working_dir)
            except ExecutionError as exc:
                self.discard(session_id, f"prepare failed: {exc}", log_warning=True)
                if source == "idle" and self._generation_matches(lease_generation):
                    continue
                raise
            cancelled = False
            with self._cond:
                if self._close_generation != lease_generation:
                    if self._leased.get(session_id) is worker:
                        del self._leased[session_id]
                        self._cond.notify_all()
                    cancelled = True
            if cancelled:
                self._force_kill_worker(worker)
                raise ExecutionError(f"{self._display_name()} pool lease cancelled by close_all")
            if queued:
                self._log_lease_after_wait(session_id, worker, source, wait_seconds)
            return worker

    def execute(
        self,
        session_id: str,
        code: str,
        timeout: float,
    ) -> ExecutionResult | None:
        with self._cond:
            worker = self._leased.get(session_id)
        if worker is None:
            return None
        return self._execute_worker(worker, code, timeout)

    def start_supervisor(self) -> None:
        if not self._enabled:
            return
        with self._cond:
            thread = self._supervisor_thread
            if thread is not None and thread.is_alive():
                self._cond.notify_all()
                return
            self._supervisor_stop = False
            thread = threading.Thread(
                target=self._supervisor_loop,
                name=f"science-eda-{self._pool_name}-pool-supervisor",
                daemon=True,
            )
            self._supervisor_thread = thread
            self._cond.notify_all()
        thread.start()

    def stop_supervisor(self) -> None:
        with self._cond:
            thread = self._supervisor_thread
            self._supervisor_stop = True
            self._cond.notify_all()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)

    def cancel_pending_leases(self) -> None:
        self.stop_supervisor()
        with self._cond:
            self._close_generation += 1
            workers = [*self._idle, *self._starting_workers.values()]
            self._idle.clear()
            self._starting_workers.clear()
            self._cond.notify_all()
        for worker in workers:
            self._force_kill_worker(worker)

    def close_all(self) -> None:
        self.stop_supervisor()
        with self._cond:
            self._close_generation += 1
            workers = [
                *self._idle,
                *self._leased.values(),
                *self._starting_workers.values(),
            ]
            self._idle.clear()
            self._leased.clear()
            self._starting_workers.clear()
            self._target_total = self._pool_size if self._prewarm_enabled else 0
            self._prewarm_complete = not self._prewarm_enabled
            self._cond.notify_all()
        for worker in workers:
            self._force_kill_worker(worker)

    def discard(
        self,
        session_id: str,
        reason: str,
        *,
        log_warning: bool = True,
    ) -> bool:
        with self._cond:
            worker = self._leased.get(session_id)
        if worker is None:
            return False
        if log_warning:
            self._log_discard(session_id, worker, reason)
        self._force_kill_worker(worker)
        with self._cond:
            if self._leased.get(session_id) is worker:
                del self._leased[session_id]
            self._cond.notify_all()
        return True

    def state(self):
        with self._cond:
            return self._state_locked()

    def _supervisor_loop(self) -> None:
        while True:
            with self._cond:
                if self._supervisor_stop:
                    return
                now = time.monotonic()
                if self._supervisor_backoff_until > now:
                    self._cond.wait(timeout=self._supervisor_backoff_until - now)
                    continue
                self._mark_prewarm_complete_locked()
                target = self._supervisor_target_locked()
                counts = self._counts_locked()
                total = counts.active + counts.idle + counts.starting
                starts = min(
                    max(0, target - total),
                    max(0, self._pool_size - total),
                    max(0, self._prewarm_concurrency - counts.starting),
                )
                if starts <= 0:
                    self._cond.wait()
                    continue
                generation = self._close_generation
                for _ in range(starts):
                    self._starting += 1
                    starter = threading.Thread(
                        target=self._start_supervised_worker,
                        args=(generation,),
                        name=f"science-eda-{self._pool_name}-pool-starter",
                        daemon=True,
                    )
                    starter.start()

    def _start_supervised_worker(self, generation: int) -> None:
        worker: WorkerT | None = None
        try:
            worker = self._start_physical_worker(generation)
        except Exception as exc:  # noqa: BLE001
            with self._cond:
                self._starting -= 1
                self._supervisor_backoff_until = max(
                    self._supervisor_backoff_until,
                    time.monotonic() + 1.0,
                )
                self._cond.notify_all()
            self._log_supervisor_start_failed(exc)
            return

        cancel_worker = False
        with self._cond:
            self._starting -= 1
            if self._close_generation != generation or self._supervisor_stop:
                cancel_worker = True
            else:
                self._idle.append(worker)
                self._mark_prewarm_complete_locked()
            self._cond.notify_all()
        if cancel_worker:
            self._force_kill_worker(worker)

    def _reserve_or_start(
        self,
        session_id: str,
        lease_generation: int,
    ) -> tuple[WorkerT, str, float, bool]:
        wait_start = time.monotonic()
        logged_wait = False
        wait_interval = max(0.01, self._wait_log_interval())
        next_log_at = wait_start + wait_interval
        queue_timeout = self._queue_timeout()
        queue_deadline = wait_start + queue_timeout if queue_timeout > 0 else None

        while True:
            with self._cond:
                if self._close_generation != lease_generation:
                    raise ExecutionError(f"{self._display_name()} pool lease cancelled by close_all")
                if self._idle:
                    worker = self._idle.pop()
                    self._leased[session_id] = worker
                    return worker, "idle", time.monotonic() - wait_start, logged_wait

                total = len(self._leased) + len(self._idle) + self._starting
                if total < self._pool_size:
                    self._starting += 1
                    self._target_total = max(
                        self._target_total,
                        min(self._pool_size, total + 1),
                    )
                    break

                now = time.monotonic()
                counts = self._counts_locked()
                if queue_deadline is not None and now >= queue_deadline:
                    detail = self._queue_timeout_detail(session_id, queue_timeout, counts)
                    self._logger.warning(detail)
                    raise ExecutionTimeoutError(detail)
                if not logged_wait:
                    self._log_wait_start(session_id, counts)
                    logged_wait = True
                elif now >= next_log_at:
                    self._log_wait_continue(session_id, now - wait_start, counts)
                    next_log_at = now + wait_interval

                timeout_candidates = [wait_interval, max(0.0, next_log_at - now)]
                if queue_deadline is not None:
                    timeout_candidates.append(max(0.0, queue_deadline - now))
                self._cond.wait(timeout=max(0.01, min(timeout_candidates)))

        try:
            worker = self._start_physical_worker(lease_generation)
        except Exception:
            with self._cond:
                self._starting -= 1
                self._cond.notify_all()
            raise

        cancel_worker = False
        with self._cond:
            self._starting -= 1
            if self._close_generation != lease_generation:
                cancel_worker = True
            else:
                self._leased[session_id] = worker
            self._cond.notify_all()
        if cancel_worker:
            self._force_kill_worker(worker)
            raise ExecutionError(f"{self._display_name()} pool lease cancelled by close_all")
        return worker, "new", time.monotonic() - wait_start, logged_wait

    def _start_physical_worker(self, generation: int) -> WorkerT:
        worker_id = uuid.uuid4().hex
        worker_dir = os.path.join(self._worker_root, f"worker_{worker_id}")
        shutil.rmtree(worker_dir, ignore_errors=True)
        os.makedirs(worker_dir, exist_ok=True)
        parent_conn, child_conn = self._ctx.Pipe(duplex=True)
        proc = self._ctx.Process(
            target=worker_main,
            args=self._worker_main_args(child_conn, worker_dir),
        )
        proc.start()
        worker = self._build_worker(worker_id, worker_dir, parent_conn, proc)
        with self._cond:
            if self._close_generation != generation:
                cancelled = True
            else:
                self._starting_workers[worker_id] = worker
                cancelled = False
        if cancelled:
            self._force_kill_worker(worker)
            raise ExecutionError(f"{self._display_name()} pool lease cancelled by close_all")
        try:
            self._wait_for_worker_ready(worker)
        except Exception:
            self._force_kill_worker(worker)
            raise
        finally:
            with self._cond:
                self._starting_workers.pop(worker_id, None)
                self._cond.notify_all()
        return worker

    def _wait_for_worker_ready(self, worker: WorkerT) -> None:
        deadline = time.monotonic() + self._startup_timeout()
        while time.monotonic() < deadline:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                has_message = worker.conn.poll(min(0.1, remaining))
            except (EOFError, OSError) as exc:
                raise ExecutionError(
                    f"{self._display_name()} pool worker pipe closed before startup readiness"
                ) from exc
            if has_message:
                try:
                    msg = worker.conn.recv()
                except (EOFError, OSError) as exc:
                    raise ExecutionError(
                        f"{self._display_name()} pool worker exited before startup readiness"
                    ) from exc
                op = msg.get("op")
                if op == "ready":
                    raw_pid = msg.get("tool_process_pid")
                    worker.tool_process_pid = int(raw_pid) if raw_pid else None
                    return
                if op == "tool_started":
                    raw_pid = msg.get("tool_process_pid")
                    worker.tool_process_pid = int(raw_pid) if raw_pid else None
                    continue
                if op == "startup_error":
                    detail = str(msg.get("error", "unknown startup failure"))
                    raise ExecutionError(
                        f"{self._display_name()} pool worker startup failed: {detail}"
                    )
                raise ExecutionError(f"unexpected {self._pool_name} pool startup message: {msg!r}")
            if not worker.process.is_alive():
                raise ExecutionError(
                    f"{self._display_name()} pool worker exited before startup readiness"
                )
        raise ExecutionError(f"{self._display_name()} pool worker startup timed out")

    def _prepare_worker(
        self,
        worker: WorkerT,
        session_id: str,
        session_working_dir: str,
    ) -> None:
        if not worker.process.is_alive():
            raise ExecutionError(f"{self._display_name()} pool worker is not alive")
        try:
            worker.conn.send(
                {
                    "op": "prepare_session",
                    "session_id": session_id,
                    "working_dir": session_working_dir,
                    "startup_tcl": self._session_startup_tcl(),
                    "startup_timeout": self._startup_timeout(),
                }
            )
        except (BrokenPipeError, EOFError, OSError) as exc:
            raise ExecutionError(f"{self._display_name()} pool worker pipe is broken") from exc

        deadline = time.monotonic() + self._startup_timeout()
        while time.monotonic() < deadline:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                has_message = worker.conn.poll(min(0.1, remaining))
            except (EOFError, OSError) as exc:
                raise ExecutionError(
                    f"{self._display_name()} pool worker pipe closed during session prepare"
                ) from exc
            if has_message:
                try:
                    msg = worker.conn.recv()
                except (EOFError, OSError) as exc:
                    raise ExecutionError(
                        f"{self._display_name()} pool worker exited during session prepare"
                    ) from exc
                op = msg.get("op")
                if op == "prepared":
                    return
                if op == "prepare_error":
                    detail = str(msg.get("error", "unknown prepare failure"))
                    raise ExecutionError(f"{self._display_name()} session prepare failed: {detail}")
                raise ExecutionError(f"unexpected {self._display_name()} prepare message: {msg!r}")
            if not worker.process.is_alive():
                raise ExecutionError(
                    f"{self._display_name()} pool worker exited during session prepare"
                )
        self._force_kill_worker(worker)
        raise ExecutionError(f"{self._display_name()} session prepare timed out")

    def _execute_worker(
        self,
        worker: WorkerT,
        code: str,
        timeout: float,
    ) -> ExecutionResult | None:
        if not worker.process.is_alive():
            return None
        try:
            worker.conn.send({"op": "execute", "code": code, "timeout": timeout})
        except (BrokenPipeError, EOFError, OSError):
            return None
        return self._wait_for_result(
            worker,
            timeout,
            timeout_stderr=self._execute_timeout_stderr(),
        )

    def _reset_worker(
        self,
        worker: WorkerT,
        reset_tcl: str,
        clean_tcl_state: bool,
        timeout: float,
    ) -> ExecutionResult | None:
        if not worker.process.is_alive():
            return None
        try:
            worker.conn.send(
                {
                    "op": "reset_session",
                    "reset_tcl": reset_tcl,
                    "clean_tcl_state": clean_tcl_state,
                    "timeout": timeout,
                }
            )
        except (BrokenPipeError, EOFError, OSError):
            return None
        return self._wait_for_reset_result(worker, timeout)

    def _wait_for_result(
        self,
        worker: WorkerT,
        timeout: float,
        *,
        timeout_stderr: str,
    ) -> ExecutionResult | None:
        wall = float(timeout) + max(0.25, min(1.0, float(timeout) * 0.1))
        deadline = time.monotonic() + wall
        while time.monotonic() < deadline:
            remaining = min(1.0, deadline - time.monotonic())
            if remaining <= 0:
                break
            try:
                has_message = worker.conn.poll(remaining)
            except (EOFError, OSError):
                return None
            if has_message:
                try:
                    return worker.conn.recv()
                except (EOFError, OSError):
                    return None
        self._force_kill_worker(worker)
        return ExecutionResult("", timeout_stderr, -1, float(timeout))

    def _wait_for_reset_result(
        self,
        worker: WorkerT,
        timeout: float,
    ) -> ExecutionResult | None:
        wall = float(timeout) + max(0.25, min(1.0, float(timeout) * 0.1))
        deadline = time.monotonic() + wall
        while time.monotonic() < deadline:
            remaining = min(1.0, deadline - time.monotonic())
            if remaining <= 0:
                break
            try:
                has_message = worker.conn.poll(remaining)
            except (EOFError, OSError):
                return None
            if has_message:
                try:
                    msg = worker.conn.recv()
                except (EOFError, OSError):
                    return None
                op = msg.get("op")
                if op == "reset_done":
                    result = msg.get("result")
                    if isinstance(result, ExecutionResult):
                        return result
                    return ExecutionResult("", "invalid reset response\n", -1, timeout)
                if op == "reset_error":
                    return ExecutionResult(
                        "",
                        str(msg.get("error", "unknown reset failure")),
                        1,
                        0.0,
                    )
                return ExecutionResult(
                    "",
                    f"unexpected {self._pool_name} pool reset message: {msg!r}",
                    1,
                    0.0,
                )
        self._force_kill_worker(worker)
        return ExecutionResult("", self._reset_timeout_stderr(), -1, float(timeout))

    def _force_kill_worker(self, worker: WorkerT) -> None:
        try:
            worker.conn.close()
        except Exception:
            pass
        if worker.tool_process_pid is not None:
            kill_process_tree(worker.tool_process_pid)
            worker.tool_process_pid = None
        proc = worker.process
        if proc.is_alive():
            kill_process_tree(proc.pid)
            proc.join(timeout=2.0)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=2.0)
        shutil.rmtree(worker.working_dir, ignore_errors=True)

    def _return_worker_to_idle(self, session_id: str, worker: WorkerT) -> None:
        with self._cond:
            if self._leased.get(session_id) is worker:
                del self._leased[session_id]
                worker.sessions_served += 1
                self._idle.append(worker)
                self._cond.notify_all()

    def _generation_matches(self, generation: int) -> bool:
        with self._cond:
            return self._close_generation == generation

    def _counts_locked(self) -> PoolCounts:
        return PoolCounts(
            pool_size=self._pool_size,
            active=len(self._leased),
            idle=len(self._idle),
            starting=self._starting,
        )

    def _state_locked(self):
        return self._counts_locked()

    def _mark_prewarm_complete_locked(self) -> None:
        if self._prewarm_complete:
            return
        ready = len(self._leased) + len(self._idle)
        if ready >= self._pool_size:
            self._prewarm_complete = True

    def _supervisor_target_locked(self) -> int:
        if self._prewarm_enabled and not self._prewarm_complete:
            return self._pool_size
        if self._replenish_enabled:
            return min(self._target_total, self._pool_size)
        return 0

    def _log_lease_after_wait(
        self,
        session_id: str,
        worker: WorkerT,
        source: str,
        wait_seconds: float,
    ) -> None:
        counts = self.state()
        self._logger.info(
            "%s pool leased worker after wait: session_id=%s wait_seconds=%.1f "
            "worker_id=%s source=%s pool_size=%d active=%d idle=%d starting=%d",
            self._pool_name,
            session_id,
            wait_seconds,
            worker.worker_id,
            source,
            counts.pool_size,
            counts.active,
            counts.idle,
            counts.starting,
        )

    def _log_wait_start(self, session_id: str, counts: PoolCounts) -> None:
        self._logger.info(
            "%s pool full; waiting for worker: session_id=%s pool_size=%d "
            "active=%d idle=%d starting=%d reason=%s",
            self._pool_name,
            session_id,
            counts.pool_size,
            counts.active,
            counts.idle,
            counts.starting,
            "pool at capacity",
        )

    def _log_wait_continue(
        self,
        session_id: str,
        wait_seconds: float,
        counts: PoolCounts,
    ) -> None:
        self._logger.info(
            "%s pool still waiting: session_id=%s wait_seconds=%.1f pool_size=%d "
            "active=%d idle=%d starting=%d",
            self._pool_name,
            session_id,
            wait_seconds,
            counts.pool_size,
            counts.active,
            counts.idle,
            counts.starting,
        )

    def _log_discard(self, session_id: str, worker: WorkerT, reason: str) -> None:
        self._logger.warning(
            "%s pool worker discarded; waking waiters: session_id=%s "
            "worker_id=%s sessions_served=%d reason=%s",
            self._pool_name,
            session_id,
            worker.worker_id,
            worker.sessions_served,
            reason,
        )

    def _log_supervisor_start_failed(self, exc: Exception) -> None:
        self._logger.warning(
            "%s pool supervisor failed to start worker: %s",
            self._pool_name,
            exc,
            exc_info=True,
        )

    def _queue_timeout_detail(
        self,
        session_id: str,
        queue_timeout: float,
        counts: PoolCounts,
    ) -> str:
        return (
            f"{self._pool_name} pool queue timed out: "
            f"session_id={session_id!r} queue_timeout={queue_timeout:.1f}s "
            f"pool_size={counts.pool_size} active={counts.active} "
            f"idle={counts.idle} starting={counts.starting}"
        )

    def _display_name(self) -> str:
        return self._pool_name

    def _build_worker(
        self,
        worker_id: str,
        worker_dir: str,
        conn: Connection,
        process: multiprocessing.Process,
    ) -> WorkerT:
        raise NotImplementedError

    def _worker_main_args(self, child_conn: Connection, worker_dir: str) -> tuple[object, ...]:
        raise NotImplementedError

    def _queue_timeout(self) -> float:
        raise NotImplementedError

    def _wait_log_interval(self) -> float:
        raise NotImplementedError

    def _startup_timeout(self) -> float:
        raise NotImplementedError

    def _session_startup_tcl(self) -> str:
        raise NotImplementedError

    def _execute_timeout_stderr(self) -> str:
        return "[sandbox] timed out waiting for worker response\n"

    def _reset_timeout_stderr(self) -> str:
        return f"[sandbox] timed out waiting for {self._display_name()} worker reset response\n"


def result_detail(result: ExecutionResult | None) -> str:
    if result is None:
        return "worker process exited before response"
    text = (result.stderr or result.stdout or "").strip()
    if text:
        return f"exit_code={result.exit_code} {tail(text)}"
    return f"exit_code={result.exit_code}"


def tail(text: str, limit: int = 2000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


def kill_process_tree(pid: int) -> None:
    if pid <= 0:
        return
    try:
        import signal

        if hasattr(os, "killpg"):
            os.killpg(pid, signal.SIGKILL)
            return
    except (ProcessLookupError, PermissionError, OSError):
        pass

    try:
        import signal

        os.kill(pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        pass
