"""Classify execution failures for replay vs user-facing errors."""

from __future__ import annotations

from enum import Enum

from science_eda.sandbox.types import ExecutionResult


class ErrorCategory(Enum):
    SUCCESS = 0
    PROCESS = 1  # worker crash, timeout, OOM, signal — may trigger replay
    CODE = 2     # user/code error — no replay


class ErrorClassifier:
    @staticmethod
    def classify(result: ExecutionResult | None, process_gone: bool = False) -> ErrorCategory:
        if process_gone:
            return ErrorCategory.PROCESS
        if result is None:
            return ErrorCategory.PROCESS
        code = result.exit_code
        # Negative exit code often indicates signal termination on Unix
        if code < 0:
            return ErrorCategory.PROCESS
        if code == 0:
            return ErrorCategory.SUCCESS
        # Heuristic: typical Python/shell user errors stay CODE
        if code in (1, 2, 127, 126):
            return ErrorCategory.CODE
        # Large exit codes may come from signals encoded differently
        if code > 128:
            return ErrorCategory.PROCESS
        return ErrorCategory.CODE
