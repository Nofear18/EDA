"""Session registry, worker lifecycle, TTL sweep, and replay recovery."""

from __future__ import annotations

import logging
import multiprocessing
import os
import shutil
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from science_eda.config import SandboxConfig
from science_eda.exceptions import (
    ExecutionError,
    LanguageMismatchError,
    SandboxPathError,
    SessionAlreadyExistsError,
    SessionNotFoundError,
)
from science_eda.sandbox.classifier import ErrorCategory, ErrorClassifier
from science_eda.sandbox.innovus_pool import InnovusWorkerPool, PooledInnovusWorker
from science_eda.sandbox.lang import normalize_lang
from science_eda.sandbox.primetime_pool import PooledPrimeTimeWorker, PrimeTimeWorkerPool
from science_eda.sandbox.runtime_metadata import (
    INNOVUS_RETRY_REASON as _INNOVUS_RETRY_REASON,
    RUNTIME_LOST_EXECUTION_TIMEOUT as _RUNTIME_LOST_EXECUTION_TIMEOUT,
    innovus_retry_metadata as _innovus_retry_metadata,
    mark_runtime_recovered as _mark_runtime_recovered,
    merge_result_metadata as _merge_result_metadata,
    new_runtime_instance_id as _new_runtime_instance_id,
    pooled_runtime_instance_id as _pooled_runtime_instance_id,
    runtime_lost_reason as _runtime_lost_reason,
    runtime_lost_result as _runtime_lost_result,
    tail as _tail,
)
from science_eda.sandbox.types import ExecutionResult
from science_eda.sandbox.worker import worker_main

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

logger = logging.getLogger(__name__)

@dataclass
class Session:
    session_id: str
    lang: str
    created_at: datetime
    last_active: datetime
    ttl: int
    working_dir: str
    runtime_instance_id: str | None = None
    worker_process: multiprocessing.Process | None = None
    conn: Connection | None = None
    tool_process_pid: int | None = None
    innovus_worker_id: str | None = None
    innovus_worker_sessions_served: int = 0
    replay_policy: str = "history"
    execution_history: list[str] = field(default_factory=list)
    innovus_worker_had_incomplete_reset: bool = False
    innovus_execute_attempted: bool = False
    innovus_incomplete_reset_retry_used: bool = False
    primetime_worker_id: str | None = None
    primetime_worker_sessions_served: int = 0
    exec_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class SessionManager:
    def __init__(self, config: SandboxConfig | None = None) -> None:
        self._config = config or SandboxConfig()
        self._sessions: dict[str, Session] = {}
        self._starting_session_ids: set[str] = set()
        self._closing_session_ids: set[str] = set()
        self._close_generation = 0
        self._closing_all = False
        self._lock = threading.RLock()
        self._ctx = multiprocessing.get_context("spawn")
        self._root = self._resolve_root()
        os.makedirs(self._root, exist_ok=True)
        self._innovus_pool = InnovusWorkerPool(self._config, self._ctx, self._root)
        self._primetime_pool = PrimeTimeWorkerPool(self._config, self._ctx, self._root)
        self._tool_adapters: dict[str, ToolSessionAdapter] = {
            "innovus": InnovusSessionAdapter(self),
            "primetime": PrimeTimeSessionAdapter(self),
        }
        self._sweeper = threading.Thread(target=self._ttl_loop, daemon=True)
        self._sweeper.start()

    def _resolve_root(self) -> str:
        if self._config.sandbox_root:
            return os.path.abspath(self._config.sandbox_root)
        return os.path.join(tempfile.gettempdir(), "eda_sandbox")

    def _ttl_loop(self) -> None:
        interval = max(5, int(self._config.ttl_sweep_interval))
        while True:
            time.sleep(float(interval))
            try:
                self._sweep_idle()
            except Exception:  # noqa: BLE001
                logger.exception("sandbox TTL sweep failed")

    def _sweep_idle(self) -> None:
        with self._lock:
            candidates = list(self._sessions.items())
        for session_id, sess in candidates:
            if not sess.exec_lock.acquire(blocking=False):
                continue
            try:
                with self._lock:
                    if self._sessions.get(session_id) is not sess:
                        continue
                    idle_seconds = (datetime.now() - sess.last_active).total_seconds()
                    if idle_seconds <= float(sess.ttl):
                        continue
                self._claim_session_for_close(session_id, sess)
                self._close_claimed_session(session_id, sess)
            finally:
                sess.exec_lock.release()

    def get_session(self, session_id: str) -> Session:
        with self._lock:
            s = self._sessions.get(session_id)
        if s is None:
            raise SessionNotFoundError(f"no such session: {session_id!r}")
        return s

    def start_innovus_pool_supervisor(self) -> None:
        self._innovus_pool.start_supervisor()

    def start_primetime_pool_supervisor(self) -> None:
        self._primetime_pool.start_supervisor()

    def start_pool_supervisors(self) -> None:
        self.start_innovus_pool_supervisor()
        self.start_primetime_pool_supervisor()

    def prewarm_innovus_pool(self) -> None:
        self.start_innovus_pool_supervisor()

    def innovus_pool_state(self) -> dict[str, int | bool]:
        state = self._innovus_pool.state()
        return {
            "pool_size": state.pool_size,
            "active": state.active,
            "idle": state.idle,
            "starting": state.starting,
            "reuse_enabled": state.reuse_enabled,
            "prewarm_enabled": state.prewarm_enabled,
            "replenish_enabled": state.replenish_enabled,
            "prewarm_concurrency": state.prewarm_concurrency,
        }

    def primetime_pool_state(self) -> dict[str, int | bool]:
        state = self._primetime_pool.state()
        return {
            "pool_size": state.pool_size,
            "active": state.active,
            "idle": state.idle,
            "starting": state.starting,
            "enabled": state.enabled,
            "prewarm_enabled": state.prewarm_enabled,
            "replenish_enabled": state.replenish_enabled,
            "prewarm_concurrency": state.prewarm_concurrency,
        }

    @contextmanager
    def locked_session(self, session_id: str) -> Iterator[Session]:
        sess = self.get_session(session_id)
        with sess.exec_lock:
            with self._lock:
                if self._sessions.get(session_id) is not sess:
                    raise SessionNotFoundError(f"no such session: {session_id!r}")
                sess.last_active = datetime.now()
            yield sess

    def create_session(self, config: dict[str, Any]) -> str:
        lang = normalize_lang(str(config["lang"]))
        adapter = self._pooled_adapter_for_lang(lang)
        raw_id = config.get("session_id")
        session_id = self._session_id_from_config(raw_id)
        with self._lock:
            if self._closing_all:
                raise ExecutionError("session creation cancelled by close_all")
            if (
                session_id in self._sessions
                or session_id in self._starting_session_ids
                or session_id in self._closing_session_ids
            ):
                raise SessionAlreadyExistsError(f"session already exists: {session_id!r}")
            self._starting_session_ids.add(session_id)
            close_generation = self._close_generation

        wd = self._session_working_dir(session_id)
        sess: Session | None = None
        try:
            if os.path.exists(wd):
                _remove_session_tree(wd)
            os.makedirs(wd, exist_ok=True)
            now = datetime.now()
            sess = Session(
                session_id=session_id,
                lang=lang,
                created_at=now,
                last_active=now,
                ttl=int(self._config.ttl),
                working_dir=wd,
                replay_policy=self._replay_policy_for_lang(lang),
            )
            with sess.exec_lock:
                if adapter is not None:
                    adapter.assign_worker(sess, adapter.lease(session_id, wd))
                else:
                    self._start_worker(sess)
            with self._lock:
                if self._closing_all or self._close_generation != close_generation:
                    raise ExecutionError("session creation cancelled by close_all")
                self._starting_session_ids.discard(session_id)
                self._sessions[session_id] = sess
        except Exception:
            if sess is not None:
                if adapter is not None:
                    adapter.discard(
                        session_id,
                        "session creation failed",
                        log_warning=False,
                    )
                    adapter.clear_worker(sess)
                else:
                    self._terminate_worker(sess)
            _remove_session_tree(wd)
            with self._lock:
                self._starting_session_ids.discard(session_id)
            raise

        return session_id

    def _pooled_adapter_for_lang(self, lang: str) -> ToolSessionAdapter | None:
        adapter = self._tool_adapters.get(lang)
        if adapter is None or not adapter.uses_pool:
            return None
        return adapter

    def _assign_innovus_worker(
        self,
        sess: Session,
        worker: PooledInnovusWorker,
    ) -> None:
        sess.worker_process = worker.process
        sess.conn = worker.conn
        sess.tool_process_pid = worker.tool_process_pid
        sess.runtime_instance_id = _pooled_runtime_instance_id(worker.worker_id)
        sess.innovus_worker_id = worker.worker_id
        sess.innovus_worker_sessions_served = worker.sessions_served
        sess.innovus_worker_had_incomplete_reset = worker.incomplete_reset_warning_seen

    def _clear_innovus_worker(self, sess: Session) -> None:
        sess.worker_process = None
        sess.conn = None
        sess.tool_process_pid = None
        sess.runtime_instance_id = None
        sess.innovus_worker_id = None
        sess.innovus_worker_sessions_served = 0
        sess.innovus_worker_had_incomplete_reset = False

    def _assign_primetime_worker(
        self,
        sess: Session,
        worker: PooledPrimeTimeWorker,
    ) -> None:
        sess.worker_process = worker.process
        sess.conn = worker.conn
        sess.tool_process_pid = worker.tool_process_pid
        sess.runtime_instance_id = _pooled_runtime_instance_id(worker.worker_id)
        sess.primetime_worker_id = worker.worker_id
        sess.primetime_worker_sessions_served = worker.sessions_served

    def _clear_primetime_worker(self, sess: Session) -> None:
        sess.worker_process = None
        sess.conn = None
        sess.tool_process_pid = None
        sess.runtime_instance_id = None
        sess.primetime_worker_id = None
        sess.primetime_worker_sessions_served = 0

    def _session_id_from_config(self, raw_id: object | None) -> str:
        if raw_id is None or raw_id == "":
            return uuid.uuid4().hex
        session_id = str(raw_id)
        self._validate_session_id(session_id)
        return session_id

    def _validate_session_id(self, session_id: str) -> None:
        if not session_id or not session_id.strip():
            raise SandboxPathError("session_id must be non-empty")
        if "\x00" in session_id:
            raise SandboxPathError("session_id cannot contain null bytes")
        if "/" in session_id or "\\" in session_id:
            raise SandboxPathError(
                f"session_id cannot contain path separators: {session_id!r}"
            )

    def _session_working_dir(self, session_id: str) -> str:
        root = os.path.abspath(self._root)
        wd = os.path.abspath(os.path.join(root, f"session_{session_id}"))
        try:
            if os.path.commonpath([root, wd]) != root:
                raise SandboxPathError(
                    f"session_id escapes sandbox root: {session_id!r}"
                )
        except ValueError as e:
            raise SandboxPathError(
                f"session_id escapes sandbox root: {session_id!r}"
            ) from e
        return wd

    def _replay_policy_for_lang(self, lang: str) -> str:
        if lang == "primetime":
            policy = str(self._config.primetime_replay_policy).strip().lower()
            if policy != "none":
                raise ValueError("primetime_replay_policy only supports 'none' in this release")
            return "none"
        if lang != "innovus":
            return "history"
        policy = str(self._config.innovus_replay_policy).strip().lower()
        if policy != "none":
            raise ValueError("innovus_replay_policy only supports 'none' in this release")
        return "none"

    def _startup_timeout_for_lang(self, lang: str) -> float:
        if lang == "primetime":
            return float(self._config.primetime_startup_timeout)
        if lang == "innovus":
            return float(self._config.innovus_startup_timeout)
        return max(1.0, min(30.0, float(self._config.timeout)))

    def _start_worker(self, sess: Session) -> None:
        parent_conn, child_conn = self._ctx.Pipe(duplex=True)
        proc = self._ctx.Process(
            target=worker_main,
            args=(
                child_conn,
                sess.working_dir,
                sess.lang,
                self._config.shell_path,
                self._config.tcl_shell,
                self._config.innovus_bin,
                list(self._config.innovus_args),
                self._config.innovus_startup_tcl,
                float(self._config.innovus_startup_timeout),
                int(self._config.memory_mb),
                int(self._config.max_pids),
                self._config.primetime_bin,
                list(self._config.primetime_args),
                self._config.primetime_startup_tcl,
                float(self._config.primetime_startup_timeout),
            ),
        )
        proc.start()
        sess.conn = parent_conn
        sess.worker_process = proc
        sess.runtime_instance_id = _new_runtime_instance_id()
        deadline = time.monotonic() + self._startup_timeout_for_lang(sess.lang)
        while time.monotonic() < deadline:
            remaining = max(0.0, deadline - time.monotonic())
            if parent_conn.poll(min(0.1, remaining)):
                try:
                    msg = parent_conn.recv()
                except (EOFError, OSError) as e:
                    raise ExecutionError("worker exited before startup readiness") from e
                op = msg.get("op")
                if op == "ready":
                    raw_pid = msg.get("tool_process_pid")
                    sess.tool_process_pid = int(raw_pid) if raw_pid else None
                    return
                if op == "tool_started":
                    raw_pid = msg.get("tool_process_pid")
                    sess.tool_process_pid = int(raw_pid) if raw_pid else None
                    continue
                if op == "startup_error":
                    detail = str(msg.get("error", "unknown startup failure"))
                    raise ExecutionError(f"worker startup failed: {detail}")
                raise ExecutionError(f"unexpected worker startup message: {msg!r}")
            if not proc.is_alive():
                raise ExecutionError("worker exited before startup readiness")
        self._force_kill_worker(sess)
        raise ExecutionError("worker startup timed out")

    def _terminate_worker(self, sess: Session) -> None:
        conn = sess.conn
        proc = sess.worker_process
        if conn is not None:
            try:
                conn.send({"op": "shutdown"})
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            sess.conn = None
        if proc is None:
            if sess.tool_process_pid is not None:
                self._kill_process_tree(sess.tool_process_pid)
                sess.tool_process_pid = None
            sess.runtime_instance_id = None
            return
        if proc.is_alive():
            proc.join(timeout=2.0)
        if proc.is_alive():
            self._kill_process_tree(proc.pid)
            proc.join(timeout=3.0)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=2.0)
        if sess.tool_process_pid is not None:
            self._kill_process_tree(sess.tool_process_pid)
            sess.tool_process_pid = None
        sess.worker_process = None
        sess.runtime_instance_id = None

    def _kill_process_tree(self, pid: int) -> None:
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

    def _execute_once(self, sess: Session, code: str, timeout: float) -> ExecutionResult | None:
        conn = sess.conn
        proc = sess.worker_process
        if conn is None or proc is None or not proc.is_alive():
            return None
        try:
            conn.send({"op": "execute", "code": code, "timeout": timeout})
        except (BrokenPipeError, EOFError, OSError):
            return None
        # Leave a small grace window for message transit, but keep timeout enforcement
        # centered in SessionManager so hung workers are killed promptly.
        wall = float(timeout) + max(0.25, min(1.0, float(timeout) * 0.1))
        deadline = time.monotonic() + wall
        while time.monotonic() < deadline:
            remaining = min(1.0, deadline - time.monotonic())
            if remaining <= 0:
                break
            if conn.poll(remaining):
                try:
                    return conn.recv()
                except (EOFError, OSError):
                    return None
        self._force_kill_worker(sess)
        return ExecutionResult(
            "",
            "[sandbox] timed out waiting for worker response\n",
            -1,
            float(timeout),
        )

    def _force_kill_worker(self, sess: Session) -> None:
        proc = sess.worker_process
        if sess.tool_process_pid is not None:
            self._kill_process_tree(sess.tool_process_pid)
            sess.tool_process_pid = None
        if proc is not None and proc.is_alive():
            self._kill_process_tree(proc.pid)
            proc.join(timeout=2)
        self._terminate_worker(sess)

    def _touch_session(
        self,
        sess: Session,
        result: ExecutionResult | None,
        code: str | None = None,
    ) -> None:
        with self._lock:
            current = self._sessions.get(sess.session_id)
            if current is not sess:
                return
            current.last_active = datetime.now()
            if (
                code is not None
                and result is not None
                and result.exit_code == 0
                and current.replay_policy == "history"
            ):
                current.execution_history.append(code)

    def _restart_worker(self, sess: Session, replay_timeout: float) -> None:
        self._terminate_worker(sess)
        _remove_session_tree(sess.working_dir)
        os.makedirs(sess.working_dir, exist_ok=True)
        self._start_worker(sess)
        to_replay = list(sess.execution_history)
        for snippet in to_replay:
            res = self._execute_once(sess, snippet, replay_timeout)
            if res is None or res.exit_code != 0:
                raise ExecutionError("replay of execution history failed after worker restart")

    def execute_in_session(
        self,
        session_id: str,
        code: str,
        lang: str,
        timeout: float | None = None,
    ) -> ExecutionResult:
        want = normalize_lang(lang)
        eff_timeout = float(timeout) if timeout is not None else float(self._config.timeout)
        max_attempts = max(1, int(self._config.max_retries) + 1)

        with self._lock:
            sess = self._sessions.get(session_id)
        if sess is None:
            raise SessionNotFoundError(f"no such session: {session_id!r}")
        if want != sess.lang:
            raise LanguageMismatchError(
                f"session is {sess.lang!r} but execute requested {want!r}"
            )
        adapter = self._pooled_adapter_for_lang(sess.lang)
        if adapter is not None:
            return adapter.execute(sess, code, eff_timeout)

        with sess.exec_lock:
            with self._lock:
                if self._sessions.get(session_id) is not sess:
                    raise SessionNotFoundError(f"no such session: {session_id!r}")
            attempt = 0
            last: ExecutionResult | None = None
            recovery_failure: ExecutionResult | None = None
            recovery_count = 0
            while attempt < max_attempts:
                failed_runtime_instance_id = sess.runtime_instance_id
                result = self._execute_once(sess, code, eff_timeout)
                gone = result is None
                cat = ErrorClassifier.classify(result, process_gone=gone)
                if cat != ErrorCategory.PROCESS:
                    if result is not None and recovery_failure is not None:
                        _mark_runtime_recovered(
                            result,
                            failure=recovery_failure,
                            runtime_instance_id=sess.runtime_instance_id,
                            recovery_count=recovery_count,
                        )
                    with self._lock:
                        s2 = self._sessions.get(session_id)
                        if s2 is not None and result is not None:
                            s2.last_active = datetime.now()
                            if result.exit_code == 0 and s2.replay_policy == "history":
                                s2.execution_history.append(code)
                    return result if result is not None else ExecutionResult("", "", -1, eff_timeout)

                failure = _runtime_lost_result(
                    result,
                    timeout=eff_timeout,
                    runtime_instance_id=failed_runtime_instance_id,
                    default_stderr="[sandbox] worker process exited before response\n",
                )
                last = failure
                recovery_failure = failure
                recovery_count += 1
                if sess.replay_policy == "none":
                    try:
                        self._restart_worker(sess, eff_timeout)
                    except ExecutionError:
                        raise
                    except Exception as e:  # noqa: BLE001
                        raise ExecutionError(f"worker restart failed: {e}") from e
                    with self._lock:
                        s2 = self._sessions.get(session_id)
                        if s2 is not None:
                            s2.last_active = datetime.now()
                    return failure

                if _runtime_lost_reason(result) == _RUNTIME_LOST_EXECUTION_TIMEOUT:
                    try:
                        self._restart_worker(sess, eff_timeout)
                    except ExecutionError:
                        raise
                    except Exception as e:  # noqa: BLE001
                        raise ExecutionError(f"worker restart failed after timeout: {e}") from e
                    with self._lock:
                        s2 = self._sessions.get(session_id)
                        if s2 is not None:
                            s2.last_active = datetime.now()
                    return failure

                attempt += 1
                if attempt >= max_attempts:
                    return failure
                try:
                    self._restart_worker(sess, eff_timeout)
                except ExecutionError:
                    raise
                except Exception as e:  # noqa: BLE001
                    raise ExecutionError(f"worker restart failed: {e}") from e

            return last if last is not None else ExecutionResult("", "", -1, eff_timeout)

    def close_session(self, session_id: str) -> None:
        with self._lock:
            sess = self._sessions.get(session_id)
        if sess is None:
            raise SessionNotFoundError(f"no such session: {session_id!r}")
        with sess.exec_lock:
            self._claim_session_for_close(session_id, sess)
            self._close_claimed_session(session_id, sess)

    def _claim_session_for_close(self, session_id: str, sess: Session) -> None:
        """Remove one exec-locked Session from the live registry."""
        with self._lock:
            if self._sessions.get(session_id) is not sess:
                raise SessionNotFoundError(f"no such session: {session_id!r}")
            del self._sessions[session_id]
            self._closing_session_ids.add(session_id)

    def _close_claimed_session(self, session_id: str, sess: Session) -> None:
        """Release resources for a Session already claimed under its exec lock."""
        try:
            adapter = self._pooled_adapter_for_lang(sess.lang)
            if adapter is not None:
                adapter.release(session_id)
                adapter.clear_worker(sess)
            else:
                self._terminate_worker(sess)
            _remove_session_tree(sess.working_dir)
        finally:
            with self._lock:
                self._closing_session_ids.discard(session_id)

    def close_all(self) -> None:
        with self._lock:
            self._closing_all = True
            self._close_generation += 1
            session_ids = list(self._sessions)
        self._innovus_pool.cancel_pending_leases()
        self._primetime_pool.cancel_pending_leases()
        try:
            for session_id in session_ids:
                try:
                    self.close_session(session_id)
                except SessionNotFoundError:
                    continue
            self._innovus_pool.close_all()
            self._primetime_pool.close_all()
        finally:
            with self._lock:
                self._closing_all = False


class ToolSessionAdapter:
    def __init__(self, manager: SessionManager) -> None:
        self._manager = manager

    @property
    def uses_pool(self) -> bool:
        return True

    def lease(self, session_id: str, working_dir: str):
        raise NotImplementedError

    def assign_worker(self, sess: Session, worker: object) -> None:
        raise NotImplementedError

    def clear_worker(self, sess: Session) -> None:
        raise NotImplementedError

    def discard(self, session_id: str, reason: str, *, log_warning: bool = True) -> bool:
        raise NotImplementedError

    def release(self, session_id: str) -> None:
        raise NotImplementedError

    def execute(self, sess: Session, code: str, timeout: float) -> ExecutionResult:
        raise NotImplementedError


class InnovusSessionAdapter(ToolSessionAdapter):
    def lease(self, session_id: str, working_dir: str) -> PooledInnovusWorker:
        return self._manager._innovus_pool.lease(session_id, working_dir)

    def assign_worker(self, sess: Session, worker: object) -> None:
        self._manager._assign_innovus_worker(sess, worker)  # type: ignore[arg-type]

    def clear_worker(self, sess: Session) -> None:
        self._manager._clear_innovus_worker(sess)

    def discard(self, session_id: str, reason: str, *, log_warning: bool = True) -> bool:
        return self._manager._innovus_pool.discard(
            session_id,
            reason,
            log_warning=log_warning,
        )

    def release(self, session_id: str) -> None:
        self._manager._innovus_pool.release(session_id)

    def execute(self, sess: Session, code: str, timeout: float) -> ExecutionResult:
        manager = self._manager
        with sess.exec_lock:
            with manager._lock:
                if manager._sessions.get(sess.session_id) is not sess:
                    raise SessionNotFoundError(f"no such session: {sess.session_id!r}")
            proc = sess.worker_process
            if sess.conn is None or proc is None or not proc.is_alive():
                self.discard(
                    sess.session_id,
                    "leased worker is not alive before execute",
                    log_warning=False,
                )
                self.clear_worker(sess)
                self.assign_worker(sess, self.lease(sess.session_id, sess.working_dir))

            should_retry_first_execute = (
                sess.innovus_worker_had_incomplete_reset
                and not sess.innovus_execute_attempted
                and not sess.innovus_incomplete_reset_retry_used
            )
            sess.innovus_execute_attempted = True
            result = manager._innovus_pool.execute(sess.session_id, code, timeout)
            gone = result is None
            cat = ErrorClassifier.classify(result, process_gone=gone)
            if cat == ErrorCategory.PROCESS:
                return self._handle_process_failure(
                    sess,
                    code,
                    timeout,
                    result,
                    should_retry_first_execute,
                )

            manager._touch_session(sess, result)
            return result if result is not None else ExecutionResult("", "", -1, timeout)

    def _handle_process_failure(
        self,
        sess: Session,
        code: str,
        timeout: float,
        result: ExecutionResult | None,
        should_retry_first_execute: bool,
    ) -> ExecutionResult:
        manager = self._manager
        failed_runtime_instance_id = sess.runtime_instance_id
        failure = _runtime_lost_result(
            result,
            timeout=timeout,
            runtime_instance_id=failed_runtime_instance_id,
            default_stderr="[sandbox] worker process exited before response\n",
        )
        failed_worker_id = sess.innovus_worker_id
        failed_tool_pid = sess.tool_process_pid
        failed_sessions_served = sess.innovus_worker_sessions_served
        self.discard(
            sess.session_id,
            "process failure during execute",
            log_warning=False,
        )
        self.clear_worker(sess)
        if not should_retry_first_execute:
            manager._touch_session(sess, None)
            return failure

        retry_metadata = _innovus_retry_metadata(
            failure,
            failed_runtime_instance_id=failed_runtime_instance_id,
            failed_worker_id=failed_worker_id,
            failed_tool_pid=failed_tool_pid,
            failed_sessions_served=failed_sessions_served,
        )
        sess.innovus_incomplete_reset_retry_used = True
        self.assign_worker(sess, self.lease(sess.session_id, sess.working_dir))
        retry_metadata.update(
            {
                "innovus_retry_worker_id": sess.innovus_worker_id,
                "innovus_retry_tool_pid": sess.tool_process_pid,
                "innovus_retry_runtime_instance_id": sess.runtime_instance_id,
            }
        )
        logger.warning(
            "innovus first execute failed after incomplete reset; "
            "retrying with new worker: session_id=%s reason=%s "
            "failed_worker_id=%s failed_tool_pid=%s "
            "failed_worker_sessions_served=%s retry_worker_id=%s "
            "retry_tool_pid=%s first_failure_exit_code=%s "
            "first_failure_stderr_tail=%r first_failure_stdout_tail=%r",
            sess.session_id,
            _INNOVUS_RETRY_REASON,
            failed_worker_id,
            failed_tool_pid,
            failed_sessions_served,
            sess.innovus_worker_id,
            sess.tool_process_pid,
            failure.exit_code,
            retry_metadata["innovus_first_failure_stderr_tail"],
            retry_metadata["innovus_first_failure_stdout_tail"],
        )
        retry_result = manager._innovus_pool.execute(sess.session_id, code, timeout)
        retry_gone = retry_result is None
        retry_cat = ErrorClassifier.classify(retry_result, process_gone=retry_gone)
        if retry_cat == ErrorCategory.PROCESS:
            retry_failure = _runtime_lost_result(
                retry_result,
                timeout=timeout,
                runtime_instance_id=sess.runtime_instance_id,
                default_stderr="[sandbox] worker process exited before response\n",
            )
            logger.warning(
                "innovus first execute retry also failed; discarding "
                "retry worker: session_id=%s reason=%s "
                "retry_worker_id=%s retry_tool_pid=%s "
                "retry_worker_sessions_served=%s "
                "retry_failure_exit_code=%s "
                "retry_failure_stderr_tail=%r "
                "retry_failure_stdout_tail=%r",
                sess.session_id,
                _INNOVUS_RETRY_REASON,
                sess.innovus_worker_id,
                sess.tool_process_pid,
                sess.innovus_worker_sessions_served,
                retry_failure.exit_code,
                _tail(retry_failure.stderr),
                _tail(retry_failure.stdout),
            )
            _merge_result_metadata(retry_failure, retry_metadata)
            self.discard(
                sess.session_id,
                "process failure during retry after IMPSYC-6379",
                log_warning=False,
            )
            self.clear_worker(sess)
            manager._touch_session(sess, None)
            return retry_failure

        if retry_result is None:
            retry_result = ExecutionResult("", "", -1, timeout)
        _merge_result_metadata(retry_result, retry_metadata)
        manager._touch_session(sess, retry_result)
        return retry_result


class PrimeTimeSessionAdapter(ToolSessionAdapter):
    @property
    def uses_pool(self) -> bool:
        return self._manager._primetime_pool.enabled

    def lease(self, session_id: str, working_dir: str) -> PooledPrimeTimeWorker:
        return self._manager._primetime_pool.lease(session_id, working_dir)

    def assign_worker(self, sess: Session, worker: object) -> None:
        self._manager._assign_primetime_worker(sess, worker)  # type: ignore[arg-type]

    def clear_worker(self, sess: Session) -> None:
        self._manager._clear_primetime_worker(sess)

    def discard(self, session_id: str, reason: str, *, log_warning: bool = True) -> bool:
        return self._manager._primetime_pool.discard(
            session_id,
            reason,
            log_warning=log_warning,
        )

    def release(self, session_id: str) -> None:
        self._manager._primetime_pool.release(session_id)

    def execute(self, sess: Session, code: str, timeout: float) -> ExecutionResult:
        manager = self._manager
        with sess.exec_lock:
            with manager._lock:
                if manager._sessions.get(sess.session_id) is not sess:
                    raise SessionNotFoundError(f"no such session: {sess.session_id!r}")
            proc = sess.worker_process
            if sess.conn is None or proc is None or not proc.is_alive():
                self.discard(
                    sess.session_id,
                    "leased worker is not alive before execute",
                    log_warning=False,
                )
                self.clear_worker(sess)
                self.assign_worker(sess, self.lease(sess.session_id, sess.working_dir))

            result = manager._primetime_pool.execute(sess.session_id, code, timeout)
            gone = result is None
            if ErrorClassifier.classify(result, process_gone=gone) == ErrorCategory.PROCESS:
                failure = _runtime_lost_result(
                    result,
                    timeout=timeout,
                    runtime_instance_id=sess.runtime_instance_id,
                    default_stderr=(
                        "[sandbox] PrimeTime worker process exited before response\n"
                    ),
                )
                self.discard(
                    sess.session_id,
                    "process failure during execute",
                    log_warning=False,
                )
                self.clear_worker(sess)
                manager._touch_session(sess, None)
                return failure

            manager._touch_session(sess, result)
            return result if result is not None else ExecutionResult("", "", -1, timeout)


def _remove_session_tree(path: str) -> None:
    if not os.path.exists(path) and not os.path.islink(path):
        return
    if os.path.islink(path) or os.path.isfile(path):
        try:
            os.unlink(path)
        except OSError:
            return
        return

    for dirpath, dirnames, filenames in os.walk(path):
        for name in [*dirnames, *filenames]:
            child = os.path.join(dirpath, name)
            if not os.path.islink(child):
                try:
                    os.chmod(child, 0o700)
                except OSError:
                    pass
        try:
            os.chmod(dirpath, 0o700)
        except OSError:
            pass
    shutil.rmtree(path, ignore_errors=True)
