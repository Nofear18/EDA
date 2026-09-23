"""Batch runtime identity and process-loss result metadata helpers."""

from __future__ import annotations

import uuid
from typing import Any

from science_eda.sandbox.types import ExecutionResult

INNOVUS_RETRY_REASON = "incomplete_reset_first_execute_process_failure"
RUNTIME_LOST_PROCESS_EXITED = "PROCESS_EXITED"
RUNTIME_LOST_EXECUTION_TIMEOUT = "EXECUTION_TIMEOUT"
_LOG_TAIL_LIMIT = 2000


def new_runtime_instance_id() -> str:
    return f"rti_{uuid.uuid4().hex}"


def pooled_runtime_instance_id(worker_id: str) -> str:
    return f"rti_{worker_id}"


def merge_result_metadata(
    result: ExecutionResult,
    metadata: dict[str, Any],
) -> None:
    merged = dict(result.metadata)
    merged.update(metadata)
    result.metadata = merged


def runtime_lost_reason(result: ExecutionResult | None) -> str:
    if result is not None:
        termination = str(result.metadata.get("termination_reason", "")).upper()
        if termination in {"TIMEOUT", "TIMED_OUT", "EXECUTION_TIMEOUT"}:
            return RUNTIME_LOST_EXECUTION_TIMEOUT
        if termination:
            return RUNTIME_LOST_PROCESS_EXITED
        stderr = result.stderr.lower()
        if "timed out waiting for" in stderr or "exceeded timeout" in stderr:
            return RUNTIME_LOST_EXECUTION_TIMEOUT
    return RUNTIME_LOST_PROCESS_EXITED


def runtime_lost_result(
    result: ExecutionResult | None,
    *,
    timeout: float,
    runtime_instance_id: str | None,
    default_stderr: str,
) -> ExecutionResult:
    failure = result if result is not None else ExecutionResult(
        "",
        default_stderr,
        -1,
        timeout,
    )
    reason = runtime_lost_reason(result)
    merge_result_metadata(
        failure,
        {
            "runtime_instance_id": runtime_instance_id,
            "runtime_lost": True,
            "runtime_lost_reason": reason,
            "reason": reason,
        },
    )
    return failure


def mark_runtime_recovered(
    result: ExecutionResult,
    *,
    failure: ExecutionResult,
    runtime_instance_id: str | None,
    recovery_count: int,
) -> None:
    failed_runtime_instance_id = failure.metadata.get("runtime_instance_id")
    reason = str(
        failure.metadata.get("runtime_lost_reason", RUNTIME_LOST_PROCESS_EXITED)
    )
    merge_result_metadata(
        result,
        {
            "runtime_instance_id": runtime_instance_id,
            "runtime_failed_instance_id": failed_runtime_instance_id,
            "runtime_lost": True,
            "runtime_lost_reason": reason,
            "reason": reason,
            "runtime_recovered": True,
            "runtime_recovery_count": max(1, int(recovery_count)),
        },
    )


def innovus_retry_metadata(
    failure: ExecutionResult,
    *,
    failed_runtime_instance_id: str | None,
    failed_worker_id: str | None,
    failed_tool_pid: int | None,
    failed_sessions_served: int,
) -> dict[str, Any]:
    return {
        "innovus_retry_count": 1,
        "innovus_retry_reason": INNOVUS_RETRY_REASON,
        "innovus_failed_runtime_instance_id": failed_runtime_instance_id,
        "innovus_failed_worker_id": failed_worker_id,
        "innovus_failed_tool_pid": failed_tool_pid,
        "innovus_failed_worker_sessions_served": failed_sessions_served,
        "innovus_retry_worker_id": None,
        "innovus_retry_tool_pid": None,
        "innovus_retry_runtime_instance_id": None,
        "innovus_first_failure_exit_code": failure.exit_code,
        "innovus_first_failure_duration": failure.duration,
        "innovus_first_failure_stdout_tail": tail(failure.stdout),
        "innovus_first_failure_stderr_tail": tail(failure.stderr),
    }


def tail(text: str, limit: int = _LOG_TAIL_LIMIT) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]
