"""Execution response and terminal-state helpers for the interactive service."""

from __future__ import annotations

import logging
from typing import Any

from science_eda.exceptions import InteractiveSessionNotFoundError
from science_eda.sandbox.interactive.models import ExecutionRecord, ExecutionState
from science_eda.sandbox.interactive.partial_log import (
    PartialLogSnapshot,
    read_partial_log,
)
from science_eda.sandbox.interactive.runtime import RuntimeLostError
from science_eda.sandbox.interactive.serialization import execution_payload
from science_eda.sandbox.interactive.service_support import (
    RuntimeController,
    bounded_message,
)

logger = logging.getLogger(__name__)


class InteractiveServiceExecutionMixin:
    """Build execution responses and persist exceptional terminal outcomes."""

    def _execution_response(self, execution: ExecutionRecord) -> dict[str, Any]:
        runtime = self.registry.get_runtime(execution.workspace_session_id)
        return execution_payload(execution, runtime)

    def _seal_lost_logs_for_session(self, workspace_session_id: str) -> bool:
        """Seal LOST logs only after the caller confirmed process-tree death."""

        try:
            executions = self.registry.list_unsealed_lost_executions(
                workspace_session_id
            )
        except InteractiveSessionNotFoundError:
            # Startup ownership can name its reserved future Session before a
            # WorkspaceSession row is published.  Foreign keys guarantee that
            # such an id cannot own any execution log yet.
            return True
        except Exception:
            logger.warning(
                "could not enumerate unsealed LOST logs for %s",
                workspace_session_id,
                exc_info=True,
            )
            return False

        sealed_all = True

        for execution in executions:
            try:
                snapshot = read_partial_log(
                    execution.local_log_path,
                    preview_bytes=self.config.interactive_output_preview_bytes,
                    reader_chunk_bytes=self.config.interactive_reader_chunk_bytes,
                    max_log_bytes=execution.log_quota_reserved_bytes,
                )
            except FileNotFoundError:
                # The log is pre-created before an execution is admitted.  If
                # it was externally removed, no process can recreate it after
                # confirmed termination, so an empty incomplete seal is final.
                snapshot = PartialLogSnapshot(
                    output_preview="",
                    returned_bytes=0,
                    output_bytes=0,
                    output_lines=0,
                    output_truncated=False,
                    written_bytes=0,
                )
            except (OSError, UnicodeError, LookupError, ValueError):
                # A transient filesystem failure must not publish guessed
                # counts.  Leave the NULL written count as a durable retry
                # marker for the next sweep.
                logger.warning(
                    "could not seal partial interactive log for %s",
                    execution.execution_id,
                    exc_info=True,
                )
                sealed_all = False
                continue

            reason = execution.full_log_incomplete_reason
            if not reason and execution.error:
                reason = str(execution.error.get("code") or "")
            reason = reason or "PROCESS_LOST"
            try:
                sealed = self.registry.seal_lost_execution_log(
                    execution.execution_id,
                    output_preview=snapshot.output_preview,
                    returned_bytes=snapshot.returned_bytes,
                    output_bytes=snapshot.output_bytes,
                    output_lines=snapshot.output_lines,
                    output_truncated=snapshot.output_truncated,
                    full_log_written_bytes=snapshot.written_bytes,
                    full_log_incomplete_reason=reason,
                )
            except Exception:
                logger.warning(
                    "could not persist sealed interactive log for %s",
                    execution.execution_id,
                    exc_info=True,
                )
                sealed_all = False
                continue
            if sealed.full_log_written_bytes is not None:
                self.metrics.observe(
                    "quota.written_bytes",
                    float(sealed.full_log_written_bytes),
                )
                self.metrics.observe(
                    "quota.conservative_accounted_bytes",
                    float(sealed.log_quota_reserved_bytes),
                )
        return sealed_all

    def _complete_without_summary(
        self,
        execution: ExecutionRecord,
        *,
        state: ExecutionState,
        reason: str,
        message: str,
    ) -> ExecutionRecord:
        self.metrics.observe(
            "quota.conservative_accounted_bytes",
            float(execution.log_quota_reserved_bytes),
        )
        snapshot = None
        try:
            snapshot = read_partial_log(
                execution.local_log_path,
                preview_bytes=self.config.interactive_output_preview_bytes,
                reader_chunk_bytes=self.config.interactive_reader_chunk_bytes,
                max_log_bytes=execution.log_quota_reserved_bytes,
            )
        except (OSError, UnicodeError, LookupError, ValueError):
            logger.warning(
                "could not recover partial interactive log for %s",
                execution.execution_id,
                exc_info=True,
            )
        return self.registry.complete_execution(
            execution.execution_id,
            state,
            error={
                "code": reason,
                "message": bounded_message(
                    message or reason,
                    self.config.interactive_output_preview_max_bytes,
                ),
            },
            output_preview=(None if snapshot is None else snapshot.output_preview),
            preview_strategy=(None if snapshot is None else "head_tail"),
            returned_bytes=(None if snapshot is None else snapshot.returned_bytes),
            output_bytes=(None if snapshot is None else snapshot.output_bytes),
            output_lines=(None if snapshot is None else snapshot.output_lines),
            output_truncated=(
                None if snapshot is None else snapshot.output_truncated
            ),
            full_log_complete=False,
            full_log_written_bytes=(
                None if snapshot is None else snapshot.written_bytes
            ),
            full_log_dropped_bytes=None,
            full_log_incomplete_reason=reason,
            runtime_preserved=False,
            runtime_lost_reason=reason,
            # The on-disk byte count is useful output metadata, but no summary
            # means it cannot safely release the rest of the reservation.
            account_full_reservation=True,
        )

    def _complete_output_sink_prepare_failure(
        self,
        controller: RuntimeController,
        execution: ExecutionRecord,
        message: str,
    ) -> ExecutionRecord:
        """Fail before Tcl dispatch, preserving only a healthchecked runtime."""

        healthcheck = getattr(controller.runtime, "healthcheck", None)
        try:
            if not callable(healthcheck):
                raise RuntimeLostError("runtime does not expose a healthcheck")
            healthcheck()
        except Exception as exc:  # noqa: BLE001 - healthcheck is the trust boundary
            self._release_controller(
                controller,
                terminate=True,
                remove_scratch=True,
            )
            return self._complete_without_summary(
                execution,
                state=ExecutionState.LOST,
                reason="RUNTIME_HEALTHCHECK_FAILED",
                message=f"{message}; runtime healthcheck failed: {exc}",
            )

        self.metrics.increment("execution.failed")
        return self.registry.complete_execution(
            execution.execution_id,
            ExecutionState.FAILED,
            error={
                "code": "OUTPUT_SINK_PREPARE_FAILED",
                "message": bounded_message(
                    message or "worker could not open the execution log",
                    self.config.interactive_output_preview_max_bytes,
                ),
            },
            output_preview="",
            preview_strategy="head_tail",
            returned_bytes=0,
            output_bytes=0,
            output_lines=0,
            output_truncated=False,
            full_log_complete=False,
            full_log_written_bytes=0,
            full_log_dropped_bytes=0,
            full_log_incomplete_reason="OUTPUT_SINK_PREPARE_FAILED",
            runtime_preserved=True,
        )
