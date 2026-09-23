class ScienceEDAError(Exception):
    """Base exception for science_eda project."""
    pass

class InferenceError(ScienceEDAError):
    """Base exception for LLM inference."""
    pass

class APIConnectionError(InferenceError):
    """API connection or authentication failure."""
    pass

class RateLimitError(InferenceError):
    """API rate limit exceeded."""
    pass

class ParseError(InferenceError):
    """Failed to parse structured output from LLM."""
    pass

class CodexProviderError(InferenceError):
    """Local Codex provider execution or protocol failure."""
    pass


class EvaluationError(ScienceEDAError):
    """Base exception for evaluation workflows."""
    pass


class JudgeError(EvaluationError):
    """Judge specification or orchestration failure."""
    pass

class ExecutionError(ScienceEDAError):
    """Base exception for code execution."""
    pass


class SessionError(ExecutionError):
    """Session lifecycle or policy violation."""
    pass


class SessionNotFoundError(SessionError):
    """Unknown or already closed session id."""
    pass


class SessionAlreadyExistsError(SessionError):
    """Explicit session_id collides with an active session."""
    pass


class LanguageMismatchError(SessionError):
    """Code language does not match the session-bound language."""
    pass

class SandboxPathError(ExecutionError):
    """Invalid or unsafe sandbox workspace path."""
    pass


class InteractiveSandboxError(ExecutionError):
    """Base error for the versioned trusted-local interactive API."""

    error_code = "INTERNAL_ERROR"
    status_code = 500

    def __init__(
        self,
        message: str,
        *,
        details: dict | None = None,
        request_id: str | None = None,
        session_id: str | None = None,
        workspace_session_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.details = dict(details or {})
        self.request_id = request_id
        self.session_id = session_id
        self.workspace_session_id = workspace_session_id


class InteractiveInvalidRequestError(InteractiveSandboxError):
    error_code = "INVALID_REQUEST"
    status_code = 400


class InvalidHistoryCursorError(InteractiveSandboxError):
    error_code = "INVALID_CURSOR"
    status_code = 400


class InteractiveCodeTooLargeError(InteractiveSandboxError):
    error_code = "CODE_TOO_LARGE"
    status_code = 400


class InvalidWorkspacePathError(InteractiveSandboxError):
    error_code = "INVALID_WORKSPACE_PATH"
    status_code = 400


class InteractiveSessionNotFoundError(InteractiveSandboxError):
    error_code = "SESSION_NOT_FOUND"
    status_code = 404


class RequestIdConflictError(InteractiveSandboxError):
    error_code = "REQUEST_ID_CONFLICT"
    status_code = 409


class SessionIdConflictError(InteractiveSandboxError):
    error_code = "SESSION_ID_CONFLICT"
    status_code = 409


class InteractiveSessionBusyError(InteractiveSandboxError):
    error_code = "SESSION_BUSY"
    status_code = 409


class InteractiveRuntimeLostError(InteractiveSandboxError):
    error_code = "RUNTIME_LOST"
    status_code = 409


class InteractiveRuntimeStoppedError(InteractiveSandboxError):
    error_code = "RUNTIME_STOPPED"
    status_code = 409


class CreateCapacityTimeoutError(InteractiveSandboxError):
    error_code = "CREATE_CAPACITY_TIMEOUT"
    status_code = 503


class RuntimeStartFailedError(InteractiveSandboxError):
    error_code = "RUNTIME_START_FAILED"
    status_code = 503


class InteractiveInternalError(InteractiveSandboxError):
    error_code = "INTERNAL_ERROR"
    status_code = 500

class TimeoutError(ExecutionError):
    """Code execution timeout."""
    pass

class MemoryLimitError(ExecutionError):
    """Code execution exceeded memory limit."""
    pass

class DataError(ScienceEDAError):
    """Base exception for data processing."""
    pass

class PromptError(DataError):
    """Invalid prompt template or missing template context."""
    pass

class DatagenError(ScienceEDAError):
    """Base exception for data generation and validation."""
    pass
