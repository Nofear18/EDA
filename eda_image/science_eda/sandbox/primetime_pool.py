"""Optional bounded physical worker pool for PrimeTime sandbox sessions."""

from __future__ import annotations

import logging
import multiprocessing
from dataclasses import dataclass
from typing import TYPE_CHECKING

from science_eda.config import SandboxConfig
from science_eda.sandbox.tool_pool import (
    PooledToolWorker,
    ToolWorkerPoolBase,
    result_detail,
)

if TYPE_CHECKING:
    from multiprocessing.connection import Connection
    from multiprocessing.context import BaseContext

logger = logging.getLogger(__name__)


@dataclass
class PooledPrimeTimeWorker(PooledToolWorker):
    pass


@dataclass(frozen=True)
class PrimeTimePoolState:
    pool_size: int
    active: int
    idle: int
    starting: int
    enabled: bool
    prewarm_enabled: bool
    replenish_enabled: bool
    prewarm_concurrency: int


class PrimeTimeWorkerPool(ToolWorkerPoolBase[PooledPrimeTimeWorker]):
    """Lease one long-lived pt_shell process to one logical session at a time."""

    def __init__(
        self,
        config: SandboxConfig,
        ctx: BaseContext,
        sandbox_root: str,
    ) -> None:
        enabled = bool(config.primetime_use_pool)
        super().__init__(
            config,
            ctx,
            sandbox_root,
            pool_name="PrimeTime",
            worker_root_name="primetime_pool",
            logger=logger,
            enabled=enabled,
            pool_size=int(config.primetime_pool_size),
            prewarm_enabled=enabled and bool(config.primetime_pool_prewarm),
            replenish_enabled=enabled and bool(config.primetime_pool_replenish),
            prewarm_concurrency=int(config.primetime_pool_prewarm_concurrency),
        )

    def release(self, session_id: str) -> None:
        with self._cond:
            worker = self._leased.get(session_id)
        if worker is None:
            return
        reset_tcl = str(self._config.primetime_pool_reset_tcl).strip()
        if not reset_tcl:
            logger.warning(
                "PrimeTime pool reset Tcl is not configured; discarding worker "
                "instead of risking cross-session design state: session_id=%s "
                "worker_id=%s",
                session_id,
                worker.worker_id,
            )
            self.discard(session_id, "reset Tcl is not configured", log_warning=False)
            return
        reset = self._reset_worker(
            worker,
            reset_tcl,
            bool(self._config.primetime_pool_clean_tcl_state),
            float(self._config.timeout),
        )
        if reset is None or reset.exit_code != 0:
            logger.warning(
                "PrimeTime pool worker reset failed; discarding worker: "
                "session_id=%s worker_id=%s detail=%s",
                session_id,
                worker.worker_id,
                result_detail(reset),
            )
            self.discard(session_id, "reset failed", log_warning=False)
            return

        healthcheck = str(self._config.primetime_pool_healthcheck_tcl).strip()
        if healthcheck:
            health = self._execute_worker(worker, healthcheck, float(self._config.timeout))
            if health is None or health.exit_code != 0:
                logger.warning(
                    "PrimeTime pool worker healthcheck failed; discarding worker: "
                    "session_id=%s worker_id=%s detail=%s",
                    session_id,
                    worker.worker_id,
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
    ) -> PooledPrimeTimeWorker:
        return PooledPrimeTimeWorker(worker_id, worker_dir, conn, process)

    def _worker_main_args(self, child_conn: Connection, worker_dir: str) -> tuple[object, ...]:
        return (
            child_conn,
            worker_dir,
            "primetime",
            self._config.shell_path,
            self._config.tcl_shell,
            self._config.innovus_bin,
            list(self._config.innovus_args),
            "",
            float(self._config.innovus_startup_timeout),
            int(self._config.memory_mb),
            int(self._config.max_pids),
            self._config.primetime_bin,
            list(self._config.primetime_args),
            "",
            float(self._config.primetime_startup_timeout),
        )

    def _queue_timeout(self) -> float:
        return float(self._config.primetime_pool_queue_timeout)

    def _wait_log_interval(self) -> float:
        return float(self._config.primetime_pool_wait_log_interval)

    def _startup_timeout(self) -> float:
        return float(self._config.primetime_startup_timeout)

    def _session_startup_tcl(self) -> str:
        return str(self._config.primetime_startup_tcl)

    def _execute_timeout_stderr(self) -> str:
        return "[sandbox] timed out waiting for PrimeTime worker response\n"

    def _reset_timeout_stderr(self) -> str:
        return "[sandbox] timed out waiting for PrimeTime worker reset response\n"

    def _display_name(self) -> str:
        return "PrimeTime"

    def _state_locked(self) -> PrimeTimePoolState:
        counts = self._counts_locked()
        return PrimeTimePoolState(
            pool_size=counts.pool_size,
            active=counts.active,
            idle=counts.idle,
            starting=counts.starting,
            enabled=self._enabled,
            prewarm_enabled=self._prewarm_enabled,
            replenish_enabled=self._replenish_enabled,
            prewarm_concurrency=self._prewarm_concurrency,
        )
