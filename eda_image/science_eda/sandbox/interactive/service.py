"""Lifecycle orchestration for the trusted-local interactive control plane.

The service deliberately owns no high-level client API.  It is the durable
control layer used by the versioned HTTP routes: SQLite is authoritative for
idempotency and externally visible state, while the in-memory controllers own
only live process handles and capacity leases.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from science_eda.config import SandboxConfig
from science_eda.exceptions import (
    CreateCapacityTimeoutError,
    InteractiveInternalError,
    InteractiveInvalidRequestError,
    InteractiveRuntimeLostError,
    InteractiveRuntimeStoppedError,
    InteractiveSandboxError,
    InvalidHistoryCursorError,
    InvalidWorkspacePathError,
    RequestIdConflictError,
    RuntimeStartFailedError,
    SessionIdConflictError,
)
from science_eda.sandbox.interactive.metrics import InteractiveMetrics
from science_eda.sandbox.interactive.models import (
    CreateRequestRecord,
    CreateRequestState,
    ExecutionRecord,
    ExecutionState,
    OrphanRuntimeRecord,
    SessionState,
    new_id,
)
from science_eda.sandbox.interactive.registry import InteractiveRegistry
from science_eda.sandbox.interactive.runtime import (
    DedicatedToolRuntime,
    RuntimeCommandTimeout,
    RuntimeLostError,
    RuntimeOutputSinkError,
    RuntimeStartError,
    WorkspaceCwdError,
)
from science_eda.sandbox.interactive.scheduler import (
    CapacityLease,
    CapacityWaitTimeout,
    InteractiveCapacityScheduler,
    SchedulerClosed,
)
from science_eda.sandbox.interactive.serialization import (
    capabilities_payload,
    history_item_payload,
    workspace_session_payload,
    workspace_session_summary,
)
from science_eda.sandbox.interactive.service_execution import (
    InteractiveServiceExecutionMixin,
)
from science_eda.sandbox.interactive.service_lifecycle import (
    InteractiveServiceLifecycleMixin,
)
from science_eda.sandbox.interactive.service_support import (
    OrphanRuntimeController,
    RuntimeController,
    bounded_message,
    pid_is_alive,
    runtime_processes_alive,
)
from science_eda.sandbox.interactive.tool_config import (
    get_interactive_tool_config,
    interactive_capacities,
    validate_tool_version,
)
from science_eda.sandbox.interactive.workspace import (
    InteractivePaths,
    canonicalize_workspace_path,
    create_execution_log,
    create_runtime_scratch,
    remove_execution_log,
    resolve_interactive_paths,
    validate_requested_session_id,
)

logger = logging.getLogger(__name__)

RuntimeFactory = Callable[..., DedicatedToolRuntime]


class InteractiveService(
    InteractiveServiceLifecycleMixin,
    InteractiveServiceExecutionMixin,
):
    """Own durable interactive sessions and their one-shot tool runtimes."""

    def __init__(
        self,
        config: SandboxConfig,
        *,
        registry: InteractiveRegistry | None = None,
        runtime_factory: RuntimeFactory = DedicatedToolRuntime,
        start_background_tasks: bool = True,
    ) -> None:
        config.validate_interactive()
        self.config = config
        self.paths: InteractivePaths = resolve_interactive_paths(config)
        self.registry = registry or InteractiveRegistry(config, paths=self.paths)
        self.scheduler = InteractiveCapacityScheduler(interactive_capacities(config))
        self.metrics = InteractiveMetrics()
        self._runtime_factory = runtime_factory
        self._controllers: dict[str, RuntimeController] = {}
        self._starting_runtimes: dict[str, DedicatedToolRuntime] = {}
        self._orphan_runtimes: dict[str, OrphanRuntimeController] = {}
        self._pending_release_ids: set[str] = set()
        self._pending_ownership_cleanup: dict[str, OrphanRuntimeRecord] = {}
        self._session_locks: dict[str, threading.RLock] = {}
        self._execution_events: dict[str, threading.Event] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._closed = False
        runtime_slots = sum(interactive_capacities(config).values())
        self._create_background = ThreadPoolExecutor(
            max_workers=max(4, runtime_slots + 2),
            thread_name_prefix="science-eda-interactive-create",
        )
        self._execution_background = ThreadPoolExecutor(
            max_workers=max(2, runtime_slots + 1),
            thread_name_prefix="science-eda-interactive-execute",
        )
        self._sweeper: threading.Thread | None = None
        self._reconcile_after_restart()
        if start_background_tasks:
            self._sweeper = threading.Thread(
                target=self._sweep_loop,
                name="science-eda-interactive-sweeper",
                daemon=True,
            )
            self._sweeper.start()

    # ------------------------------------------------------------------
    # HTTP-facing operations
    # ------------------------------------------------------------------
    def capabilities(self) -> dict[str, Any]:
        return capabilities_payload(self.config, self.paths)

    def create_workspace_session(
        self,
        *,
        session_id: str | None = None,
        tool_kind: str,
        version: str | None = None,
        workspace_path: str,
    ) -> dict[str, Any]:
        self._ensure_open()
        workspace_session_id = validate_requested_session_id(
            session_id if session_id is not None else new_id("wss")
        )
        normalized_version = validate_tool_version(version)
        # The durable create-request key is now an internal implementation
        # detail.  Reusing the requested Session ID preserves transport
        # idempotency while ensuring the caller-selected ID is the actual
        # WorkspaceSession identity.
        create_request_id = workspace_session_id
        try:
            claim = self.registry.begin_create_request(
                create_request_id,
                tool_kind,
                workspace_path,
                version=normalized_version,
            )
        except RequestIdConflictError as exc:
            raise SessionIdConflictError(
                "session_id was already used with different immutable fields",
                session_id=workspace_session_id,
            ) from exc
        except InteractiveSandboxError as exc:
            exc.request_id = None
            exc.session_id = workspace_session_id
            raise
        if claim.created:
            self.metrics.increment("create.accepted")
            if claim.record.state is CreateRequestState.PENDING:
                capacity_deadline = time.monotonic() + float(
                    self.config.interactive_create_capacity_timeout
                )
                try:
                    self._create_background.submit(
                        self._create_workspace_session_task,
                        create_request_id,
                        workspace_session_id,
                        tool_kind,
                        normalized_version,
                        workspace_path,
                        capacity_deadline,
                    )
                except RuntimeError as exc:
                    self.registry.fail_create_request(
                        create_request_id,
                        RuntimeStartFailedError.error_code,
                        "interactive daemon is shutting down",
                    )
                    raise RuntimeStartFailedError(
                        "interactive daemon is shutting down",
                        session_id=workspace_session_id,
                    ) from exc
        else:
            self.metrics.increment("create.idempotent_retry")

        record = self.registry.wait_for_create_terminal(create_request_id)
        if record.state is CreateRequestState.PENDING:
            raise RuntimeStartFailedError(
                "interactive create did not reach a terminal state",
                session_id=workspace_session_id,
            )
        if record.state is CreateRequestState.FAILED:
            try:
                self._raise_stored_create_error(record)
            except InteractiveSandboxError as exc:
                exc.request_id = None
                exc.session_id = workspace_session_id
                raise
        if record.workspace_session_id is None:  # pragma: no cover - corruption guard
            raise InteractiveInternalError(
                "successful create record has no workspace session",
                session_id=workspace_session_id,
            )
        session, runtime = self.registry.get_workspace_session_bundle(
            record.workspace_session_id
        )
        return {
            "session_id": workspace_session_id,
            "workspace_session": workspace_session_payload(session, runtime),
        }

    def list_workspace_sessions(self) -> dict[str, Any]:
        sessions = [
            workspace_session_summary(session, runtime)
            for session, runtime in self.registry.list_workspace_sessions()
        ]
        return {"count": len(sessions), "sessions": sessions}

    def get_workspace_session(self, workspace_session_id: str) -> dict[str, Any]:
        session, runtime = self.registry.get_workspace_session_bundle(
            workspace_session_id
        )
        return {"workspace_session": workspace_session_payload(session, runtime)}

    def workspace_session_history(
        self,
        workspace_session_id: str,
        *,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        page = self.registry.history_page(
            workspace_session_id,
            limit=limit,
            cursor=cursor,
        )
        return {
            "workspace_session_id": workspace_session_id,
            "order": "sequence_asc",
            "history": [history_item_payload(item) for item in page.records],
            "page": {
                "count": len(page.records),
                "has_more": page.has_more,
                "next_cursor": page.next_cursor,
            },
        }

    def execute_workspace_session(
        self,
        workspace_session_id: str,
        *,
        request_id: str,
        code: str,
        timeout_ms: int,
    ) -> dict[str, Any]:
        self._ensure_open()
        session_lock = self._session_lock(workspace_session_id)
        with session_lock:
            existing = self.registry.get_execution_by_request(
                workspace_session_id,
                request_id,
            )
            if existing is not None:
                claim = self.registry.begin_execution(
                    workspace_session_id,
                    request_id,
                    code,
                    timeout_ms,
                    execution_id=existing.execution_id,
                    full_log_ref=existing.full_log_ref,
                    local_log_path=existing.local_log_path,
                    log_quota_reserved_bytes=existing.log_quota_reserved_bytes,
                )
                return self._execution_response(claim.record)

            execution_id = new_id("exec")
            log_location = create_execution_log(
                self.paths,
                workspace_session_id,
                execution_id,
            )
            try:
                claim = self.registry.begin_execution(
                    workspace_session_id,
                    request_id,
                    code,
                    timeout_ms,
                    execution_id=execution_id,
                    full_log_ref=log_location.full_log_ref,
                    local_log_path=str(log_location.path),
                    log_quota_reserved_bytes=(
                        self.config.interactive_execution_log_max_bytes
                    ),
                )
            except BaseException:
                remove_execution_log(self.paths, workspace_session_id, execution_id)
                raise
            if not claim.created:
                # Another request thread won the unique request-id transaction.
                remove_execution_log(self.paths, workspace_session_id, execution_id)
                return self._execution_response(claim.record)

            self.metrics.observe(
                "quota.reserved_bytes",
                float(claim.record.log_quota_reserved_bytes),
            )

            controller = self._controller(workspace_session_id)
            if controller is None:
                record = self._complete_without_summary(
                    claim.record,
                    state=ExecutionState.LOST,
                    reason="RUNTIME_HANDLE_LOST",
                    message="live runtime handle is unavailable",
                )
                return self._execution_response(record)

            event = threading.Event()
            with self._lock:
                self._execution_events[execution_id] = event
            try:
                # The worker must successfully open the pre-created 0600 log
                # before the durable execution is allowed to become RUNNING.
                controller.runtime.prepare_execution(
                    execution_id=execution_id,
                    log_path=str(log_location.path),
                    preview_bytes=self.config.interactive_output_preview_bytes,
                    write_budget_bytes=claim.record.log_quota_reserved_bytes,
                )
                running = self.registry.mark_execution_running(execution_id)
                self.metrics.increment("execution.started")
                self._execution_background.submit(
                    self._execute_task,
                    controller,
                    running,
                    event,
                )
            except RuntimeOutputSinkError as exc:
                record = self._complete_output_sink_prepare_failure(
                    controller,
                    claim.record,
                    str(exc),
                )
                event.set()
                with self._lock:
                    self._execution_events.pop(execution_id, None)
                return self._execution_response(record)
            except RuntimeCommandTimeout:
                self._release_controller(
                    controller,
                    terminate=True,
                    remove_scratch=True,
                )
                record = self._complete_without_summary(
                    claim.record,
                    state=ExecutionState.LOST,
                    reason="OUTPUT_SINK_PREPARE_TIMEOUT",
                    message="worker timed out while preparing the output sink",
                )
                event.set()
                with self._lock:
                    self._execution_events.pop(execution_id, None)
                return self._execution_response(record)
            except (RuntimeLostError, RuntimeError, OSError) as exc:
                self._release_controller(
                    controller,
                    terminate=True,
                    remove_scratch=True,
                )
                record = self._complete_without_summary(
                    claim.record,
                    state=ExecutionState.LOST,
                    reason="OUTPUT_SINK_PREPARE_FAILED",
                    message=str(exc),
                )
                event.set()
                with self._lock:
                    self._execution_events.pop(execution_id, None)
                return self._execution_response(record)
            except Exception as exc:  # noqa: BLE001 - never strand a QUEUED record
                self._release_controller(
                    controller,
                    terminate=True,
                    remove_scratch=True,
                )
                record = self._complete_without_summary(
                    claim.record,
                    state=ExecutionState.LOST,
                    reason="OUTPUT_SINK_PREPARE_FAILED",
                    message="worker failed while publishing execution state",
                )
                event.set()
                with self._lock:
                    self._execution_events.pop(execution_id, None)
                logger.exception("unexpected output-sink preparation failure", exc_info=exc)
                return self._execution_response(record)

        # Only the thread which created the execution waits for its terminal
        # result.  Idempotent retries observe RUNNING immediately.
        event.wait()
        return self._execution_response(self.registry.get_execution(execution_id))

    def destroy_workspace_session(self, workspace_session_id: str) -> dict[str, Any]:
        session_lock = self._session_lock(workspace_session_id)
        with session_lock:
            session, runtime_record = self.registry.get_workspace_session_bundle(
                workspace_session_id
            )
            if session.state is SessionState.CLOSED:
                self.metrics.increment("session.destroy.idempotent_retry")
                return dict(
                    session.close_result
                    or {
                        "workspace_session_id": workspace_session_id,
                        "state": SessionState.CLOSED.value,
                        "runtime_terminated": False,
                        "closed_at": session.closed_at,
                    }
                )
            controller = self._controller(workspace_session_id)
            terminated = False
            if controller is not None:
                confirmed = self._release_controller(
                    controller,
                    terminate=True,
                    remove_scratch=True,
                )
                # The public result reports whether the runtime was confirmed
                # terminated, including a process which exited immediately
                # before DELETE acquired the Session lock.
                terminated = bool(confirmed)
            else:
                orphan = self._orphan_for_session(workspace_session_id)
                if orphan is not None:
                    confirmed = self._release_orphan(orphan, terminate=True)
                    terminated = bool(confirmed)
                elif pid_is_alive(runtime_record.worker_process_id) or pid_is_alive(
                    runtime_record.process_id
                ):
                    orphan = OrphanRuntimeController(
                        record=OrphanRuntimeRecord(
                            tool_kind=runtime_record.tool_kind,
                            scratch_relative_path=(
                                runtime_record.scratch_relative_path
                            ),
                            worker_process_id=runtime_record.worker_process_id,
                            worker_process_identity=(
                                runtime_record.worker_process_identity
                            ),
                            process_id=runtime_record.process_id,
                            process_identity=runtime_record.process_identity,
                            workspace_session_id=workspace_session_id,
                        ),
                        lease=self.scheduler.recover_existing(
                            runtime_record.tool_kind,
                            runtime_record.capacity_lease_id,
                        ),
                    )
                    with self._lock:
                        self._orphan_runtimes[
                            runtime_record.scratch_relative_path
                        ] = orphan
                    terminated = self._release_orphan(orphan, terminate=True)
            process_tree_stopped = terminated or (
                controller is None
                and self._orphan_for_session(workspace_session_id) is None
                and not pid_is_alive(runtime_record.worker_process_id)
                and not pid_is_alive(runtime_record.process_id)
            )
            closed = self.registry.close_workspace_session(
                workspace_session_id,
                runtime_terminated=terminated,
            )
            if process_tree_stopped:
                self._seal_lost_logs_for_session(workspace_session_id)
            if controller is None and self._orphan_for_session(
                workspace_session_id
            ) is None:
                self._safe_remove_scratch_if_unoccupied(
                    runtime_record.scratch_relative_path
                )
            if runtime_record.current_execution_id:
                self._signal_execution(runtime_record.current_execution_id)
            self.metrics.increment("session.destroyed")
            return dict(
                closed.close_result
                or {
                    "workspace_session_id": workspace_session_id,
                    "state": SessionState.CLOSED.value,
                    "runtime_terminated": terminated,
                    "closed_at": closed.closed_at,
                }
            )

    # ------------------------------------------------------------------
    # Background lifecycle
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._stop_event.set()
            controllers = list(self._controllers.values())
            starting = list(self._starting_runtimes.values())
            orphans = list(self._orphan_runtimes.values())
        self.scheduler.close()
        for runtime in starting:
            try:
                runtime.terminate()
            except Exception:  # noqa: BLE001 - shutdown is best effort
                logger.exception("failed to terminate a starting interactive runtime")
        for controller in controllers:
            try:
                self.registry.mark_runtime_lost(
                    controller.workspace_session_id,
                    "DAEMON_SHUTDOWN",
                )
            except Exception:  # noqa: BLE001 - continue releasing other runtimes
                logger.exception("failed to reconcile runtime during shutdown")
            self._release_controller(
                controller,
                terminate=True,
                remove_scratch=True,
            )
        for orphan in orphans:
            self._release_orphan(orphan, terminate=True)
        for event in list(self._execution_events.values()):
            event.set()
        if self._sweeper is not None and self._sweeper is not threading.current_thread():
            self._sweeper.join(timeout=2.0)
        self._create_background.shutdown(wait=True, cancel_futures=False)
        self._execution_background.shutdown(wait=True, cancel_futures=False)
        self.registry.close()

    close = shutdown

    def _create_workspace_session_task(
        self,
        create_request_id: str,
        workspace_session_id: str,
        tool_kind: str,
        version: str | None,
        workspace_path: str,
        capacity_deadline: float,
    ) -> None:
        started = time.monotonic()
        canonical: Path | None = None
        scratch = None
        lease: CapacityLease | None = None
        runtime: DedicatedToolRuntime | None = None
        controller: RuntimeController | None = None
        publication_lock = threading.Lock()
        publication = {"published": False, "lost_reason": None}

        def on_runtime_lost(reason: str) -> None:
            with publication_lock:
                publication["lost_reason"] = reason
                published = bool(publication["published"])
            if published:
                self._runtime_lost_callback(workspace_session_id, reason)

        try:
            if time.monotonic() >= capacity_deadline:
                raise CreateCapacityTimeoutError(
                    "interactive runtime capacity wait timed out",
                    request_id=create_request_id,
                    details={"tool_kind": tool_kind},
                )
            canonical = canonicalize_workspace_path(workspace_path, self.paths)
            tool = get_interactive_tool_config(self.config, tool_kind)
            try:
                capacity_wait_started = time.monotonic()
                remaining_capacity_wait = capacity_deadline - capacity_wait_started
                if remaining_capacity_wait <= 0:
                    raise CapacityWaitTimeout(
                        f"interactive {tool.tool_kind} capacity wait timed out"
                    )
                lease = self.scheduler.acquire(
                    tool.tool_kind,
                    remaining_capacity_wait,
                )
            except (CapacityWaitTimeout, SchedulerClosed) as exc:
                self.metrics.increment(f"capacity.{tool.tool_kind}.timeout")
                raise CreateCapacityTimeoutError(
                    "interactive runtime capacity wait timed out",
                    request_id=create_request_id,
                    details={"tool_kind": tool.tool_kind},
                ) from exc
            else:
                self.metrics.observe(
                    f"capacity.{tool.tool_kind}.wait_seconds",
                    time.monotonic() - capacity_wait_started,
                )
            scratch_relative_path = f"session_{workspace_session_id}"
            self.registry.register_runtime_startup(
                create_request_id,
                workspace_session_id,
                scratch_relative_path,
            )
            scratch = create_runtime_scratch(self.paths, workspace_session_id)
            runtime = self._runtime_factory(
                self.config,
                tool_kind=tool.tool_kind,
                scratch_dir=str(scratch.path),
                workspace_path=str(canonical),
                tool_version=version,
                tool_env=tool.build_environment(),
                on_lost=on_runtime_lost,
            )
            controller = RuntimeController(
                workspace_session_id=workspace_session_id,
                runtime=runtime,
                lease=lease,
                scratch_relative_path=scratch.relative_path,
                startup_request_id=create_request_id,
            )
            set_process_callback = getattr(
                runtime,
                "set_process_started_callback",
                None,
            )
            if callable(set_process_callback):
                set_process_callback(
                    lambda worker_pid, worker_identity, tool_pid, tool_identity: (
                        self.registry.update_runtime_startup_processes(
                            create_request_id,
                            worker_process_id=worker_pid,
                            process_id=tool_pid,
                            worker_process_identity=worker_identity,
                            process_identity=tool_identity,
                        )
                    )
                )
            with self._lock:
                if self._closed:
                    raise RuntimeStartError("interactive daemon is shutting down")
                self._starting_runtimes[create_request_id] = runtime
            try:
                self.registry.mark_runtime_startup_spawn_attempted(create_request_id)
                runtime_start_started = time.monotonic()
                runtime.start()
                self.metrics.observe(
                    f"runtime.{tool.tool_kind}.startup_seconds",
                    time.monotonic() - runtime_start_started,
                )
            finally:
                with self._lock:
                    self._starting_runtimes.pop(create_request_id, None)
            self.registry.update_runtime_startup_processes(
                create_request_id,
                worker_process_id=getattr(runtime, "worker_process_id", None),
                process_id=getattr(runtime, "process_id", None),
                worker_process_identity=getattr(
                    runtime,
                    "worker_process_identity",
                    None,
                ),
                process_identity=getattr(runtime, "process_identity", None),
            )
            with publication_lock:
                if (
                    publication["lost_reason"] is not None
                    or not runtime.is_alive
                    or runtime.process_id is None
                ):
                    raise RuntimeStartError("interactive runtime exited during startup")
                lease.mark_ready()
                with self._lock:
                    if self._closed:
                        raise RuntimeStartError("interactive daemon is shutting down")
                    # Registry publication and in-memory visibility form one
                    # shutdown boundary.  If shutdown wins the lock first,
                    # this create fails and its private runtime is cleaned up;
                    # if create wins, shutdown must observe the published
                    # controller and durably mark/terminate it.
                    self.registry.publish_workspace_session(
                        create_request_id,
                        workspace_session_id,
                        str(canonical),
                        runtime.runtime_instance_id,
                        int(runtime.process_id),
                        scratch.relative_path,
                        worker_process_id=getattr(
                            runtime,
                            "worker_process_id",
                            None,
                        ),
                        worker_process_identity=getattr(
                            runtime,
                            "worker_process_identity",
                            None,
                        ),
                        process_identity=getattr(
                            runtime,
                            "process_identity",
                            None,
                        ),
                        capacity_lease_id=lease.lease_id,
                    )
                    controller.startup_request_id = None
                    self._controllers[workspace_session_id] = controller
                    publication["published"] = True
            self.metrics.increment("create.succeeded")
        except WorkspaceCwdError as exc:
            error = InvalidWorkspacePathError(
                "EDA runtime could not use the requested workspace as cwd",
                request_id=create_request_id,
            )
            self._cleanup_failed_create(
                create_request_id,
                controller,
                runtime,
                lease,
                scratch,
            )
            self._fail_create(create_request_id, error, canonical)
        except InteractiveSandboxError as exc:
            self._cleanup_failed_create(
                create_request_id,
                controller,
                runtime,
                lease,
                scratch,
            )
            self._fail_create(create_request_id, exc, canonical)
        except (RuntimeStartError, RuntimeLostError, OSError, ValueError) as exc:
            error = RuntimeStartFailedError(
                "interactive tool runtime failed to start",
                request_id=create_request_id,
            )
            self._cleanup_failed_create(
                create_request_id,
                controller,
                runtime,
                lease,
                scratch,
            )
            self._fail_create(create_request_id, error, canonical)
            logger.info("interactive runtime startup failed: %s", exc)
        except BaseException as exc:  # noqa: BLE001 - persist every accepted create
            error = InteractiveInternalError(
                "interactive workspace session creation failed",
                request_id=create_request_id,
            )
            self._cleanup_failed_create(
                create_request_id,
                controller,
                runtime,
                lease,
                scratch,
            )
            self._fail_create(create_request_id, error, canonical)
            logger.exception("unexpected interactive create failure", exc_info=exc)
        finally:
            self.metrics.observe("create.duration_seconds", time.monotonic() - started)

    def _execute_task(
        self,
        controller: RuntimeController,
        execution: ExecutionRecord,
        event: threading.Event,
    ) -> None:
        started = time.monotonic()
        try:
            summary = controller.runtime.execute_prepared(
                execution.execution_id,
                execution.code,
                execution.timeout_ms / 1000.0,
            )
            if summary is None:
                raise RuntimeLostError("worker returned no execution summary")
            termination = str(getattr(summary, "termination_reason", "") or "").upper()
            protocol_complete = bool(
                getattr(summary, "status_received", False)
                and getattr(summary, "stdout_fenced", False)
                and getattr(summary, "stderr_fenced", False)
            )
            if termination in {"TIMEOUT", "TIMED_OUT", "EXECUTION_TIMEOUT"}:
                state = ExecutionState.TIMED_OUT
                preserved = False
                error = {
                    "code": "EXECUTION_TIMEOUT",
                    "message": f"execution exceeded {execution.timeout_ms} ms",
                }
                lost_reason = "EXECUTION_TIMEOUT"
            elif termination or not protocol_complete or not controller.runtime.is_alive:
                state = ExecutionState.LOST
                preserved = False
                lost_reason = termination or "PROCESS_LOST"
                error = {
                    "code": lost_reason,
                    "message": "interactive tool process was lost during execution",
                }
            elif int(summary.exit_code) == 0:
                state = ExecutionState.SUCCEEDED
                preserved = True
                error = None
                lost_reason = None
            else:
                state = ExecutionState.FAILED
                preserved = True
                error = {
                    "code": "TCL_ERROR",
                    "message": bounded_message(
                        str(getattr(summary, "output_preview", ""))
                        or f"Tcl execution failed with exit code {summary.exit_code}",
                        self.config.interactive_output_preview_max_bytes,
                    ),
                }
                lost_reason = None

            preview = str(getattr(summary, "output_preview", ""))
            record = self.registry.complete_execution(
                execution.execution_id,
                state,
                exit_code=int(summary.exit_code),
                error=error,
                output_preview=preview,
                returned_bytes=len(preview.encode("utf-8")),
                output_bytes=int(getattr(summary, "output_bytes", 0)),
                output_lines=int(getattr(summary, "output_lines", 0)),
                output_truncated=bool(getattr(summary, "output_truncated", False)),
                full_log_complete=bool(getattr(summary, "full_log_complete", False)),
                full_log_written_bytes=int(getattr(summary, "written_bytes", 0)),
                full_log_dropped_bytes=getattr(summary, "dropped_bytes", None),
                full_log_incomplete_reason=getattr(summary, "incomplete_reason", None),
                runtime_preserved=preserved,
                runtime_lost_reason=lost_reason,
            )
            self.metrics.observe(
                "quota.written_bytes",
                float(getattr(summary, "written_bytes", 0)),
            )
            dropped_bytes = getattr(summary, "dropped_bytes", None)
            if dropped_bytes is not None:
                self.metrics.observe("quota.dropped_bytes", float(dropped_bytes))
            self.metrics.increment(f"execution.{record.state.value.lower()}")
            if preserved:
                resume_monitoring = getattr(
                    controller.runtime,
                    "resume_monitoring",
                    None,
                )
                if callable(resume_monitoring):
                    resume_monitoring()
            else:
                self._release_controller(
                    controller,
                    terminate=True,
                    remove_scratch=True,
                )
        except RuntimeCommandTimeout:
            self._release_controller(controller, terminate=True, remove_scratch=True)
            self._complete_without_summary(
                execution,
                state=ExecutionState.TIMED_OUT,
                reason="EXECUTION_TIMEOUT",
                message=f"execution exceeded {execution.timeout_ms} ms",
            )
        except (RuntimeLostError, EOFError, BrokenPipeError, OSError) as exc:
            self._release_controller(controller, terminate=True, remove_scratch=True)
            self._complete_without_summary(
                execution,
                state=ExecutionState.LOST,
                reason="PROCESS_LOST",
                message=str(exc),
            )
        except BaseException as exc:  # noqa: BLE001 - never strand RUNNING state
            self._release_controller(controller, terminate=True, remove_scratch=True)
            self._complete_without_summary(
                execution,
                state=ExecutionState.LOST,
                reason="WORKER_PROTOCOL_ERROR",
                message="interactive worker failed to produce a terminal summary",
            )
            logger.exception("unexpected interactive execution failure", exc_info=exc)
        finally:
            self.metrics.observe("execution.duration_seconds", time.monotonic() - started)
            event.set()
            with self._lock:
                self._execution_events.pop(execution.execution_id, None)

    def _runtime_lost_callback(self, workspace_session_id: str, reason: str) -> None:
        try:
            previous = self.registry.get_runtime(workspace_session_id)
            # The execution thread owns termination, partial-log recovery, and
            # the atomic execution/runtime terminal transition.  Completing it
            # here would discard the bounded diagnostics already on disk.
            if previous.current_execution_id:
                return
            self.registry.mark_runtime_lost(workspace_session_id, reason)
        except Exception:
            # A process can disappear in the narrow interval before create is
            # published.  The create task's liveness check owns that outcome.
            return
        controller = self._controller(workspace_session_id)
        if controller is not None:
            self._release_controller(
                controller,
                terminate=True,
                remove_scratch=True,
            )
        if previous.current_execution_id:
            self._signal_execution(previous.current_execution_id)
        self.metrics.increment("runtime.lost")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fail_create(
        self,
        request_id: str,
        error: InteractiveSandboxError,
        canonical: Path | None,
    ) -> None:
        self.registry.fail_create_request(
            request_id,
            error.error_code,
            str(error),
            canonical_workspace_path=None if canonical is None else str(canonical),
        )
        self.metrics.increment("create.failed")

    def _raise_stored_create_error(self, record: CreateRequestRecord) -> None:
        error_type: type[InteractiveSandboxError]
        error_type = {
            InvalidWorkspacePathError.error_code: InvalidWorkspacePathError,
            CreateCapacityTimeoutError.error_code: CreateCapacityTimeoutError,
            RuntimeStartFailedError.error_code: RuntimeStartFailedError,
            RequestIdConflictError.error_code: RequestIdConflictError,
            InvalidHistoryCursorError.error_code: InvalidHistoryCursorError,
            InteractiveInvalidRequestError.error_code: InteractiveInvalidRequestError,
        }.get(record.error_code or "", InteractiveInternalError)
        raise error_type(
            record.error_message or "interactive create failed",
            request_id=record.request_id,
        )

    def _controller(self, workspace_session_id: str) -> RuntimeController | None:
        with self._lock:
            return self._controllers.get(workspace_session_id)

    def _session_lock(self, workspace_session_id: str) -> threading.RLock:
        with self._lock:
            return self._session_locks.setdefault(
                workspace_session_id,
                threading.RLock(),
            )

    def _signal_execution(self, execution_id: str) -> None:
        with self._lock:
            event = self._execution_events.get(execution_id)
        if event is not None:
            event.set()

    def _ensure_open(self) -> None:
        with self._lock:
            if self._closed:
                raise InteractiveInternalError("interactive daemon is shutting down")
