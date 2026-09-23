"""Bounded physical worker pool for Innovus sandbox sessions."""

from __future__ import annotations

import logging
import multiprocessing
from dataclasses import dataclass
from typing import TYPE_CHECKING

from science_eda.config import SandboxConfig
from science_eda.sandbox.tool_pool import (
    PooledToolWorker,
    ToolWorkerPoolBase,
    kill_process_tree,
    result_detail,
    tail,
)
from science_eda.sandbox.types import ExecutionResult

if TYPE_CHECKING:
    from multiprocessing.connection import Connection
    from multiprocessing.context import BaseContext

logger = logging.getLogger(__name__)

_INCOMPLETE_RESET_POLICIES = frozenset({"reuse_with_retry", "discard", "ignore"})


@dataclass
class PooledInnovusWorker(PooledToolWorker):
    pass


@dataclass(frozen=True)
class InnovusPoolState:
    pool_size: int
    active: int
    idle: int
    starting: int
    reuse_enabled: bool
    prewarm_enabled: bool
    replenish_enabled: bool
    prewarm_concurrency: int


class InnovusWorkerPool(ToolWorkerPoolBase[PooledInnovusWorker]):
    """Lease one long-lived Innovus worker to one logical session at a time."""

    def __init__(
        self,
        config: SandboxConfig,
        ctx: BaseContext,
        sandbox_root: str,
    ) -> None:
        self._incomplete_reset_policy = _normalize_incomplete_reset_policy(
            str(config.innovus_pool_incomplete_reset_policy),
        )
        super().__init__(
            config,
            ctx,
            sandbox_root,
            pool_name="innovus",
            worker_root_name="innovus_pool",
            logger=logger,
            pool_size=int(config.innovus_pool_size),
            reuse_enabled=bool(config.innovus_pool_reuse),
            prewarm_enabled=bool(config.innovus_pool_prewarm),
            replenish_enabled=bool(config.innovus_pool_replenish),
            prewarm_concurrency=int(config.innovus_pool_prewarm_concurrency),
        )

    def release(self, session_id: str) -> None:
        with self._cond:
            worker = self._leased.get(session_id)
        if worker is None:
            return
        if not self._reuse_enabled:
            if self._replenish_enabled:
                with self._cond:
                    self._target_total = self._pool_size
            logger.info(
                "innovus pool reuse disabled; discarding worker on session close: "
                "session_id=%s worker_id=%s sessions_served=%d",
                session_id,
                worker.worker_id,
                worker.sessions_served,
            )
            self.discard(session_id, "pool reuse disabled", log_warning=False)
            return

        reset = self._reset_worker(
            worker,
            str(self._config.innovus_pool_reset_tcl).strip(),
            bool(self._config.innovus_pool_clean_tcl_state),
            float(self._config.timeout),
        )
        if reset is None or reset.exit_code != 0:
            logger.warning(
                "innovus pool worker reset failed; discarding worker and "
                "waking waiters: session_id=%s worker_id=%s "
                "sessions_served=%d detail=%s",
                session_id,
                worker.worker_id,
                worker.sessions_served,
                result_detail(reset),
            )
            self.discard(session_id, "reset failed", log_warning=False)
            return
        if _reset_reported_incomplete_design_cleanup(reset):
            if self._incomplete_reset_policy == "discard":
                logger.warning(
                    "innovus pool worker reset reported incomplete design cleanup; "
                    "discarding worker and waking waiters: session_id=%s "
                    "worker_id=%s sessions_served=%d signal=IMPSYC-6379 "
                    "policy=discard",
                    session_id,
                    worker.worker_id,
                    worker.sessions_served,
                )
                self.discard(
                    session_id,
                    "reset reported IMPSYC-6379 incomplete design cleanup",
                    log_warning=False,
                )
                return
            if self._incomplete_reset_policy == "reuse_with_retry":
                worker.incomplete_reset_warning_seen = True
                logger.warning(
                    "innovus pool worker reset reported incomplete design cleanup; "
                    "marking worker for first-execute retry: session_id=%s "
                    "worker_id=%s sessions_served=%d signal=IMPSYC-6379 "
                    "policy=reuse_with_retry",
                    session_id,
                    worker.worker_id,
                    worker.sessions_served,
                )
            else:
                worker.incomplete_reset_warning_seen = False
                logger.info(
                    "innovus pool worker reset reported incomplete design cleanup; "
                    "reusing worker without retry marker: session_id=%s "
                    "worker_id=%s sessions_served=%d signal=IMPSYC-6379 "
                    "policy=ignore",
                    session_id,
                    worker.worker_id,
                    worker.sessions_served,
                )
        else:
            worker.incomplete_reset_warning_seen = False

        healthcheck_tcl = str(self._config.innovus_pool_healthcheck_tcl).strip()
        if healthcheck_tcl:
            health = self._execute_worker(
                worker,
                healthcheck_tcl,
                float(self._config.timeout),
            )
            if health is None or health.exit_code != 0:
                logger.warning(
                    "innovus pool worker healthcheck failed; discarding worker "
                    "and waking waiters: session_id=%s worker_id=%s "
                    "sessions_served=%d detail=%s",
                    session_id,
                    worker.worker_id,
                    worker.sessions_served,
                    result_detail(health),
                )
                self.discard(session_id, "healthcheck failed", log_warning=False)
                return

        self._return_worker_to_idle(session_id, worker)

    def _build_worker(
        self,
        worker_id: str,
        worker_dir: str,
        conn: Connection,
        process: multiprocessing.Process,
    ) -> PooledInnovusWorker:
        return PooledInnovusWorker(worker_id, worker_dir, conn, process)

    def _worker_main_args(self, child_conn: Connection, worker_dir: str) -> tuple[object, ...]:
        return (
            child_conn,
            worker_dir,
            "innovus",
            self._config.shell_path,
            self._config.tcl_shell,
            self._config.innovus_bin,
            list(self._config.innovus_args),
            "",
            float(self._config.innovus_startup_timeout),
            int(self._config.memory_mb),
            int(self._config.max_pids),
        )

    def _queue_timeout(self) -> float:
        return float(self._config.innovus_pool_queue_timeout)

    def _wait_log_interval(self) -> float:
        return float(self._config.innovus_pool_wait_log_interval)

    def _startup_timeout(self) -> float:
        return float(self._config.innovus_startup_timeout)

    def _session_startup_tcl(self) -> str:
        return str(self._config.innovus_startup_tcl)

    def _reset_timeout_stderr(self) -> str:
        return "[sandbox] timed out waiting for worker reset response\n"

    def _state_locked(self) -> InnovusPoolState:
        counts = self._counts_locked()
        return InnovusPoolState(
            pool_size=counts.pool_size,
            active=counts.active,
            idle=counts.idle,
            starting=counts.starting,
            reuse_enabled=self._reuse_enabled,
            prewarm_enabled=self._prewarm_enabled,
            replenish_enabled=self._replenish_enabled,
            prewarm_concurrency=self._prewarm_concurrency,
        )


def _reset_reported_incomplete_design_cleanup(result: ExecutionResult) -> bool:
    return "IMPSYC-6379" in result.stdout or "IMPSYC-6379" in result.stderr


def _normalize_incomplete_reset_policy(policy: str) -> str:
    normalized = policy.strip().lower()
    if normalized not in _INCOMPLETE_RESET_POLICIES:
        allowed = ", ".join(sorted(_INCOMPLETE_RESET_POLICIES))
        raise ValueError(
            "innovus_pool_incomplete_reset_policy must be one of: " + allowed
        )
    return normalized


def _result_detail(result: ExecutionResult | None) -> str:
    return result_detail(result)


def _tail(text: str, limit: int = 2000) -> str:
    return tail(text, limit)


def _kill_process_tree(pid: int) -> None:
    kill_process_tree(pid)
