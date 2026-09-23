"""Retention, reconciliation, and process-ownership helpers for the service."""

from __future__ import annotations

import logging
import time
from typing import Any

from science_eda.sandbox.interactive.models import OrphanRuntimeRecord, RuntimeState
from science_eda.sandbox.interactive.process_identity import recorded_group_state
from science_eda.sandbox.interactive.runtime import DedicatedToolRuntime
from science_eda.sandbox.interactive.scheduler import CapacityLease
from science_eda.sandbox.interactive.service_support import (
    OrphanRuntimeController,
    RuntimeController,
    pid_is_alive,
    runtime_processes_alive,
    terminate_recorded_processes,
)
from science_eda.sandbox.interactive.workspace import (
    remove_runtime_scratch,
    remove_session_logs,
)

logger = logging.getLogger(__name__)


class InteractiveServiceLifecycleMixin:
    """Own background sweeps and release capacity only after process death."""

    def _sweep_loop(self) -> None:
        interval = float(self.config.interactive_sweep_interval)
        while not self._stop_event.wait(interval):
            try:
                self.sweep_once()
            except Exception:  # noqa: BLE001 - a later sweep must still run
                logger.exception("interactive retention sweep failed")

    def sweep_once(self) -> None:
        with self._lock:
            pending_controllers = tuple(
                self._controllers[workspace_session_id]
                for workspace_session_id in self._pending_release_ids
                if workspace_session_id in self._controllers
            )
            orphan_runtimes = tuple(self._orphan_runtimes.values())
            pending_cleanup = tuple(self._pending_ownership_cleanup.values())
        for record in pending_cleanup:
            if self._finish_durable_cleanup(record):
                with self._lock:
                    self._pending_ownership_cleanup.pop(
                        record.scratch_relative_path,
                        None,
                    )
        for controller in pending_controllers:
            self._release_controller(
                controller,
                terminate=True,
                remove_scratch=True,
            )
        for orphan in orphan_runtimes:
            # A reconciled process cannot be reattached to its Session.  Keep
            # the lease/log unsealed until ownership is known, then retry full
            # process-group termination on each sweep.
            self._release_orphan(orphan, terminate=True)

        self._retry_safe_unsealed_lost_logs()

        for candidate in self.registry.list_idle_runtime_candidates():
            lock = self._session_lock(candidate.workspace_session_id)
            if not lock.acquire(blocking=False):
                continue
            try:
                _session, current = self.registry.get_workspace_session_bundle(
                    candidate.workspace_session_id
                )
                if (
                    current.state is not RuntimeState.READY
                    or current.current_execution_id is not None
                    or current.runtime_instance_id != candidate.runtime_instance_id
                    or current.idle_expires_at != candidate.idle_expires_at
                ):
                    continue
                controller = self._controller(candidate.workspace_session_id)
                if controller is not None:
                    if not self._release_controller(
                        controller,
                        terminate=True,
                        remove_scratch=True,
                    ):
                        continue
                else:
                    if pid_is_alive(current.worker_process_id) or pid_is_alive(
                        current.process_id
                    ):
                        self.registry.mark_runtime_lost(
                            candidate.workspace_session_id,
                            "RUNTIME_HANDLE_LOST",
                        )
                        continue
                    self._safe_remove_scratch(candidate.scratch_relative_path)
                self.registry.mark_runtime_stopped(candidate.workspace_session_id)
                self.metrics.increment("runtime.idle_stopped")
            finally:
                lock.release()

        for workspace_session_id in self.registry.list_sessions_due_for_retention():
            try:
                self.destroy_workspace_session(workspace_session_id)
            except Exception:
                logger.exception("failed to close retained workspace session")

        # A destroy/timeout can clear durable ownership after the earlier
        # retry scan.  Retry again before retention may remove any log files.
        self._retry_safe_unsealed_lost_logs()

        garbage = self.registry.gc_retention()
        for workspace_session_id, relative in zip(
            garbage.workspace_session_ids,
            garbage.scratch_relative_paths,
            strict=True,
        ):
            scratch_removed = self._safe_remove_scratch_if_unoccupied(relative)
            logs_removed = False
            try:
                remove_session_logs(self.paths, workspace_session_id)
                logs_removed = True
            except Exception:
                logger.exception("failed to remove retained interactive logs")
            if not (scratch_removed and logs_removed):
                continue
            if not self.registry.finalize_session_gc(workspace_session_id):
                continue
            with self._lock:
                self._session_locks.pop(workspace_session_id, None)

    def _reconcile_after_restart(self) -> None:
        prior = self.registry.list_workspace_sessions(include_closed=True)
        result = self.registry.reconcile_after_restart()
        orphan_paths: set[str] = set()
        for record in result.orphan_runtimes:
            orphan_paths.add(record.scratch_relative_path)
            if self._orphan_is_alive(record):
                lease = self.scheduler.reserve_existing(record.tool_kind)
                startup_timeout = (
                    self.config.primetime_startup_timeout
                    if record.tool_kind == "primetime"
                    else self.config.innovus_startup_timeout
                )
                with self._lock:
                    self._orphan_runtimes[record.scratch_relative_path] = (
                        OrphanRuntimeController(
                            record=record,
                            lease=lease,
                            unknown_release_at=(
                                time.monotonic() + max(10.0, float(startup_timeout) + 5.0)
                                if record.ownership_unknown
                                else None
                            ),
                        )
                    )
                continue
            if not self._finish_durable_cleanup(record):
                self._queue_durable_cleanup(record)

        cleanup_candidates = set(result.scratch_relative_paths) - orphan_paths
        cleanup_candidates.update(
            runtime.scratch_relative_path
            for _session, runtime in prior
            if runtime.state in {RuntimeState.LOST, RuntimeState.STOPPED}
            and runtime.scratch_relative_path not in orphan_paths
        )
        orphan_session_ids = {
            record.workspace_session_id
            for record in result.orphan_runtimes
            if record.workspace_session_id is not None
        }
        for session, runtime in prior:
            if (
                runtime.state in {RuntimeState.READY, RuntimeState.BUSY}
                and session.workspace_session_id not in orphan_session_ids
            ):
                # Reconciliation found no durable process ownership for this
                # runtime, so no writer remains which could race the scan.
                self._seal_lost_logs_for_session(session.workspace_session_id)
        for relative in cleanup_candidates:
            self._safe_remove_scratch(relative)
        self._retry_safe_unsealed_lost_logs()
        self.metrics.increment("reconcile.pending_create", result.pending_creates_failed)
        self.metrics.increment("reconcile.execution_lost", result.executions_lost)
        self.metrics.increment("reconcile.runtime_lost", result.runtimes_lost)

    def _cleanup_failed_create(
        self,
        request_id: str,
        controller: RuntimeController | None,
        runtime: DedicatedToolRuntime | None,
        lease: CapacityLease | None,
        _scratch: Any,
    ) -> None:
        if controller is not None:
            self._release_controller(
                controller,
                terminate=True,
                remove_scratch=True,
            )
            return
        if runtime is not None:  # pragma: no cover - constructor sequencing guard
            try:
                runtime.terminate()
            except Exception:
                logger.exception("failed to terminate partial interactive runtime")
            if runtime_processes_alive(runtime):
                return
        if lease is not None:
            lease.release()
        try:
            startup = self.registry.get_runtime_startup(request_id)
        except Exception:
            logger.exception("failed to read partial runtime ownership")
            return
        if startup is None:
            return
        cleanup = OrphanRuntimeRecord(
            request_id=request_id,
            workspace_session_id=None,
            tool_kind=startup.tool_kind,
            scratch_relative_path=startup.scratch_relative_path,
            worker_process_id=startup.worker_process_id,
            worker_process_identity=startup.worker_process_identity,
            process_id=startup.process_id,
            process_identity=startup.process_identity,
            ownership_unknown=(
                startup.spawn_attempted and startup.process_id is None
            ),
        )
        if not self._finish_durable_cleanup(cleanup):
            self._queue_durable_cleanup(cleanup)

    def _release_controller(
        self,
        controller: RuntimeController,
        *,
        terminate: bool,
        remove_scratch: bool,
    ) -> bool:
        with controller.release_lock:
            if controller.released:
                return True
            if terminate:
                try:
                    controller.runtime.terminate()
                except Exception:
                    if runtime_processes_alive(controller.runtime):
                        logger.exception("failed to terminate interactive runtime")
            if runtime_processes_alive(controller.runtime):
                with self._lock:
                    self._controllers[controller.workspace_session_id] = controller
                    self._pending_release_ids.add(controller.workspace_session_id)
                return False

            self._seal_lost_logs_for_session(controller.workspace_session_id)
            controller.lease.release()
            with self._lock:
                controller.released = True
                self._controllers.pop(controller.workspace_session_id, None)
                self._pending_release_ids.discard(controller.workspace_session_id)
            if remove_scratch:
                cleanup = OrphanRuntimeRecord(
                    request_id=controller.startup_request_id,
                    workspace_session_id=(
                        None
                        if controller.startup_request_id is not None
                        else controller.workspace_session_id
                    ),
                    tool_kind=controller.lease.tool_kind,
                    scratch_relative_path=controller.scratch_relative_path,
                    worker_process_id=getattr(
                        controller.runtime,
                        "worker_process_id",
                        None,
                    ),
                    process_id=getattr(controller.runtime, "process_id", None),
                )
                if not self._finish_durable_cleanup(cleanup):
                    self._queue_durable_cleanup(cleanup)
            return True

    def _orphan_for_session(
        self,
        workspace_session_id: str,
    ) -> OrphanRuntimeController | None:
        with self._lock:
            return next(
                (
                    orphan
                    for orphan in self._orphan_runtimes.values()
                    if orphan.record.workspace_session_id == workspace_session_id
                ),
                None,
            )

    @staticmethod
    def _orphan_is_alive(record: OrphanRuntimeRecord) -> bool:
        return record.ownership_unknown or recorded_group_state(
            record.worker_process_id,
            record.worker_process_identity,
        ) != "dead" or recorded_group_state(
            record.process_id,
            record.process_identity,
        ) != "dead"

    def _clear_orphan_ownership(self, record: OrphanRuntimeRecord) -> bool:
        try:
            if record.request_id is not None:
                self.registry.clear_runtime_startup(record.request_id)
            elif record.workspace_session_id is not None:
                self.registry.clear_runtime_process_ownership(
                    record.workspace_session_id
                )
            return True
        except Exception:
            logger.exception("failed to clear reconciled runtime ownership")
            return False

    def _finish_durable_cleanup(self, record: OrphanRuntimeRecord) -> bool:
        if record.workspace_session_id is not None:
            if not self._seal_lost_logs_for_session(record.workspace_session_id):
                return False
        if not self._safe_remove_scratch(record.scratch_relative_path):
            return False
        return self._clear_orphan_ownership(record)

    def _retry_safe_unsealed_lost_logs(self) -> None:
        try:
            workspace_session_ids = (
                self.registry.list_safe_unsealed_lost_sessions()
            )
        except Exception:
            logger.warning(
                "could not enumerate retryable LOST interactive logs",
                exc_info=True,
            )
            return
        for workspace_session_id in workspace_session_ids:
            # The durable PID-null predicate is authoritative after restart;
            # these in-memory checks guard a future ordering change where a
            # controller might outlive the ownership transaction briefly.
            controller = self._controller(workspace_session_id)
            if controller is not None and runtime_processes_alive(
                controller.runtime
            ):
                continue
            orphan = self._orphan_for_session(workspace_session_id)
            if orphan is not None and self._orphan_is_alive(orphan.record):
                continue
            self._seal_lost_logs_for_session(workspace_session_id)

    def _queue_durable_cleanup(self, record: OrphanRuntimeRecord) -> None:
        with self._lock:
            self._pending_ownership_cleanup[record.scratch_relative_path] = record

    def _release_orphan(
        self,
        orphan: OrphanRuntimeController,
        *,
        terminate: bool,
    ) -> bool:
        record = orphan.record
        unknown_confirmed = False
        if record.ownership_unknown:
            known_groups_gone = (
                recorded_group_state(
                    record.worker_process_id,
                    record.worker_process_identity,
                )
                == "dead"
                and recorded_group_state(
                    record.process_id,
                    record.process_identity,
                )
                == "dead"
            )
            if (
                not known_groups_gone
                or orphan.unknown_release_at is None
                or time.monotonic() < orphan.unknown_release_at
            ):
                return False
            # Interactive workers know their expected parent PID and refuse to
            # start a tool after it changes.  One full startup-time quarantine
            # therefore closes the only interval where no PID was committed.
            unknown_confirmed = True
        confirmed = (
            True
            if unknown_confirmed
            else terminate_recorded_processes(
                record.worker_process_id,
                record.process_id,
                worker_process_identity=record.worker_process_identity,
                process_identity=record.process_identity,
            )
            if terminate
            else not self._orphan_is_alive(record)
        )
        if not confirmed:
            return False
        orphan.lease.release()
        with self._lock:
            self._orphan_runtimes.pop(record.scratch_relative_path, None)
        if not self._finish_durable_cleanup(record):
            self._queue_durable_cleanup(record)
        return True

    def _safe_remove_scratch(self, relative_path: str) -> bool:
        try:
            remove_runtime_scratch(self.paths, relative_path)
            return True
        except FileNotFoundError:
            return True
        except Exception:
            logger.exception("failed to remove interactive runtime scratch")
            return False

    def _safe_remove_scratch_if_unoccupied(self, relative_path: str) -> bool:
        with self._lock:
            orphan = self._orphan_runtimes.get(relative_path)
            live_controller = next(
                (
                    controller
                    for controller in self._controllers.values()
                    if controller.scratch_relative_path == relative_path
                    and runtime_processes_alive(controller.runtime)
                ),
                None,
            )
        if live_controller is not None:
            return False
        if orphan is not None:
            if self._orphan_is_alive(orphan.record):
                return False
            self._release_orphan(orphan, terminate=False)
            return True
        return self._safe_remove_scratch(relative_path)
