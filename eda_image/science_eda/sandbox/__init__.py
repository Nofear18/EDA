"""Sandbox: isolated multi-language execution (import or HTTP backend)."""

from science_eda.sandbox.classifier import ErrorCategory, ErrorClassifier
from science_eda.sandbox.client import (
    ExecutionBackend,
    ExecutionClient,
    HTTPBackend,
    ImportBackend,
    StatefulSession,
    execute_tcl_script,
)
from science_eda.sandbox.lang import normalize_lang
from science_eda.sandbox.session import Session, SessionManager
from science_eda.sandbox.types import ExecutionResult

__all__ = [
    "ErrorCategory",
    "ErrorClassifier",
    "ExecutionBackend",
    "ExecutionClient",
    "ExecutionResult",
    "HTTPBackend",
    "ImportBackend",
    "Session",
    "SessionManager",
    "StatefulSession",
    "execute_tcl_script",
    "normalize_lang",
]
