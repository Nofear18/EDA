"""Versioned SQLite schema for the interactive registry."""

REGISTRY_USER_VERSION = 1


SCHEMA_SQL = """
CREATE TABLE create_requests (
    request_id TEXT PRIMARY KEY,
    tool_kind TEXT NOT NULL CHECK (tool_kind IN ('innovus', 'primetime')),
    version TEXT,
    workspace_path_raw TEXT NOT NULL,
    canonical_workspace_path TEXT,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'SUCCEEDED', 'FAILED')),
    workspace_session_id TEXT,
    error_code TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    retain_until TEXT NOT NULL
);

CREATE TABLE workspace_sessions (
    workspace_session_id TEXT PRIMARY KEY,
    tool_kind TEXT NOT NULL CHECK (tool_kind IN ('innovus', 'primetime')),
    version TEXT,
    workspace_path TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'CLOSING', 'CLOSED')),
    created_at TEXT NOT NULL,
    last_active_at TEXT NOT NULL,
    idle_expires_at TEXT,
    closed_at TEXT,
    audit_expires_at TEXT,
    close_result_json TEXT,
    next_sequence INTEGER NOT NULL DEFAULT 1 CHECK (next_sequence >= 1),
    log_quota_used_bytes INTEGER NOT NULL DEFAULT 0 CHECK (log_quota_used_bytes >= 0)
);

CREATE TABLE runtime_startups (
    request_id TEXT PRIMARY KEY
        REFERENCES create_requests(request_id) ON DELETE CASCADE,
    workspace_session_id TEXT NOT NULL UNIQUE,
    tool_kind TEXT NOT NULL CHECK (tool_kind IN ('innovus', 'primetime')),
    scratch_relative_path TEXT NOT NULL UNIQUE,
    spawn_attempted INTEGER NOT NULL DEFAULT 0 CHECK (spawn_attempted IN (0, 1)),
    worker_process_id INTEGER,
    worker_process_identity TEXT,
    process_id INTEGER,
    process_identity TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE runtimes (
    workspace_session_id TEXT PRIMARY KEY
        REFERENCES workspace_sessions(workspace_session_id) ON DELETE CASCADE,
    runtime_instance_id TEXT NOT NULL UNIQUE,
    tool_kind TEXT NOT NULL CHECK (tool_kind IN ('innovus', 'primetime')),
    state TEXT NOT NULL CHECK (state IN ('STOPPED', 'READY', 'BUSY', 'LOST')),
    worker_process_id INTEGER,
    worker_process_identity TEXT,
    process_id INTEGER,
    process_identity TEXT,
    scratch_relative_path TEXT NOT NULL UNIQUE,
    capacity_lease_id TEXT,
    current_execution_id TEXT,
    lost_reason TEXT,
    lost_at TEXT,
    started_at TEXT NOT NULL,
    last_active_at TEXT NOT NULL,
    idle_expires_at TEXT,
    stopped_at TEXT
);

CREATE TABLE executions (
    execution_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    workspace_session_id TEXT NOT NULL
        REFERENCES workspace_sessions(workspace_session_id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    tool_kind TEXT NOT NULL CHECK (tool_kind IN ('innovus', 'primetime')),
    runtime_instance_id TEXT NOT NULL,
    code TEXT NOT NULL,
    code_bytes INTEGER NOT NULL CHECK (code_bytes >= 0),
    code_sha256 TEXT NOT NULL,
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms > 0),
    state TEXT NOT NULL CHECK (
        state IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'LOST')
    ),
    submitted_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    exit_code INTEGER,
    error_json TEXT,
    output_preview TEXT,
    preview_strategy TEXT,
    returned_bytes INTEGER,
    output_bytes INTEGER,
    output_lines INTEGER,
    output_truncated INTEGER,
    full_log_ref TEXT NOT NULL UNIQUE,
    local_log_path TEXT NOT NULL UNIQUE,
    log_quota_reserved_bytes INTEGER NOT NULL CHECK (log_quota_reserved_bytes > 0),
    log_quota_accounted_bytes INTEGER NOT NULL CHECK (log_quota_accounted_bytes >= 0),
    full_log_complete INTEGER,
    full_log_written_bytes INTEGER,
    full_log_dropped_bytes INTEGER,
    full_log_incomplete_reason TEXT,
    runtime_preserved INTEGER,
    log_retained INTEGER NOT NULL DEFAULT 1 CHECK (log_retained IN (0, 1)),
    UNIQUE (workspace_session_id, request_id),
    UNIQUE (workspace_session_id, sequence)
);

CREATE TABLE session_events (
    event_id TEXT PRIMARY KEY,
    workspace_session_id TEXT NOT NULL
        REFERENCES workspace_sessions(workspace_session_id) ON DELETE CASCADE,
    event_sequence INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    transition_json TEXT,
    runtime_instance_id TEXT,
    execution_id TEXT,
    reason_json TEXT,
    UNIQUE (workspace_session_id, event_sequence)
);

CREATE INDEX idx_create_requests_retention
    ON create_requests(state, retain_until);
CREATE INDEX idx_sessions_state
    ON workspace_sessions(state, created_at);
CREATE INDEX idx_runtime_startups_scratch
    ON runtime_startups(scratch_relative_path);
CREATE INDEX idx_runtimes_idle
    ON runtimes(state, idle_expires_at);
CREATE INDEX idx_executions_history
    ON executions(workspace_session_id, sequence);
CREATE INDEX idx_events_session
    ON session_events(workspace_session_id, event_sequence);
"""
