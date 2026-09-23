"""Shared datatypes for sandbox execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExecutionResult:
    stdout: str
    stderr: str
    exit_code: int
    duration: float
    metadata: dict[str, Any] = field(default_factory=dict)
