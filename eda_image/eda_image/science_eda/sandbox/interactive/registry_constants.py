"""Shared persistence constants for interactive registry operations."""

from science_eda.sandbox.interactive.models import ExecutionState


ACTIVE_EXECUTION_STATES = (
    ExecutionState.QUEUED.value,
    ExecutionState.RUNNING.value,
)

# SQLite INTEGER values are signed 64-bit integers. Validate request values
# before persistence so an oversized timeout is a client error, not an sqlite3
# overflow reported as an internal failure.
SQLITE_INTEGER_MAX = (1 << 63) - 1
