"""Connection ownership and low-level SQLite operations for the registry."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from science_eda.config import SandboxConfig
from science_eda.exceptions import (
    InteractiveInternalError,
    InteractiveInvalidRequestError,
    InteractiveSessionNotFoundError,
)
from science_eda.sandbox.interactive.daemon_lock import (
    CONTROL_PLANE_LOCK_FILENAME,
    DaemonFileLockGroup,
)
from science_eda.sandbox.interactive.models import new_id
from science_eda.sandbox.interactive.registry_codec import bounded_json, bounded_text
from science_eda.sandbox.interactive.registry_schema import (
    REGISTRY_USER_VERSION,
    SCHEMA_SQL,
)
from science_eda.sandbox.interactive.workspace import (
    InteractivePaths,
    ensure_private_directory,
    prepare_private_file_path,
    resolve_interactive_paths,
)


_V1_TABLES = (
    "create_requests",
    "workspace_sessions",
    "runtime_startups",
    "runtimes",
    "executions",
    "session_events",
)
_V1_TABLE_DROP_ORDER = tuple(reversed(_V1_TABLES))
_V1_INDEXES = (
    "idx_create_requests_retention",
    "idx_sessions_state",
    "idx_runtime_startups_scratch",
    "idx_runtimes_idle",
    "idx_executions_history",
    "idx_events_session",
)
_V1_OBJECT_TYPES = {
    **{name: "table" for name in _V1_TABLES},
    **{name: "index" for name in _V1_INDEXES},
}


class RegistryStorage:
    """Own registry limits, process locking, connections, and transactions."""

    def __init__(
        self,
        config_or_path: SandboxConfig | str | Path,
        *,
        paths: InteractivePaths | None = None,
        acquire_process_lock: bool = True,
        create_request_retention_ttl: int | None = None,
        audit_retention_ttl: int | None = None,
        runtime_idle_ttl: int | None = None,
        session_retention_ttl: int | None = None,
        max_sessions: int | None = None,
        max_code_bytes: int | None = None,
        session_log_max_bytes: int | None = None,
        execution_log_max_bytes: int | None = None,
        history_default_page_size: int | None = None,
        history_max_page_size: int | None = None,
        history_page_max_bytes: int | None = None,
        output_preview_max_bytes: int | None = None,
        session_event_max_records: int | None = None,
    ) -> None:
        resolved: InteractivePaths | None = None
        if isinstance(config_or_path, SandboxConfig):
            config = config_or_path
            config.validate_interactive()
            resolved = paths or resolve_interactive_paths(config)
            path = resolved.registry_path
            defaults = {
                "create_request_retention_ttl": config.interactive_create_request_retention_ttl,
                "audit_retention_ttl": config.interactive_audit_retention_ttl,
                "runtime_idle_ttl": config.interactive_runtime_idle_ttl,
                "session_retention_ttl": config.interactive_session_retention_ttl,
                "max_sessions": config.interactive_max_sessions,
                "max_code_bytes": config.interactive_max_code_bytes,
                "session_log_max_bytes": config.interactive_session_log_max_bytes,
                "execution_log_max_bytes": config.interactive_execution_log_max_bytes,
                "history_default_page_size": config.interactive_history_default_page_size,
                "history_max_page_size": config.interactive_history_max_page_size,
                "history_page_max_bytes": config.interactive_history_page_max_bytes,
                "output_preview_max_bytes": config.interactive_output_preview_max_bytes,
                "session_event_max_records": config.interactive_session_event_max_records,
            }
        else:
            path = Path(config_or_path)
            defaults = {
                "create_request_retention_ttl": 30 * 24 * 60 * 60,
                "audit_retention_ttl": 90 * 24 * 60 * 60,
                "runtime_idle_ttl": 48 * 60 * 60,
                "session_retention_ttl": 0,
                "max_sessions": 100,
                "max_code_bytes": 1024 * 1024,
                "session_log_max_bytes": 4 * 1024 * 1024 * 1024,
                "execution_log_max_bytes": 1024 * 1024 * 1024,
                "history_default_page_size": 20,
                "history_max_page_size": 100,
                "history_page_max_bytes": 4 * 1024 * 1024,
                "output_preview_max_bytes": 32 * 1024,
                "session_event_max_records": 10_000,
            }
        overrides = {
            "create_request_retention_ttl": create_request_retention_ttl,
            "audit_retention_ttl": audit_retention_ttl,
            "runtime_idle_ttl": runtime_idle_ttl,
            "session_retention_ttl": session_retention_ttl,
            "max_sessions": max_sessions,
            "max_code_bytes": max_code_bytes,
            "session_log_max_bytes": session_log_max_bytes,
            "execution_log_max_bytes": execution_log_max_bytes,
            "history_default_page_size": history_default_page_size,
            "history_max_page_size": history_max_page_size,
            "history_page_max_bytes": history_page_max_bytes,
            "output_preview_max_bytes": output_preview_max_bytes,
            "session_event_max_records": session_event_max_records,
        }
        values = {
            name: defaults[name] if override is None else override
            for name, override in overrides.items()
        }
        self._validate_limits(values)
        self.path = prepare_private_file_path(path)
        self._create_retention = int(values["create_request_retention_ttl"])
        self._audit_retention = int(values["audit_retention_ttl"])
        self._runtime_idle_ttl = int(values["runtime_idle_ttl"])
        self._session_retention_ttl = int(values["session_retention_ttl"])
        self._max_sessions = int(values["max_sessions"])
        self._max_code_bytes = int(values["max_code_bytes"])
        self._session_log_max_bytes = int(values["session_log_max_bytes"])
        self._execution_log_max_bytes = int(values["execution_log_max_bytes"])
        self._history_default = int(values["history_default_page_size"])
        self._history_max = int(values["history_max_page_size"])
        self._history_bytes = int(values["history_page_max_bytes"])
        self._preview_max = int(values["output_preview_max_bytes"])
        self._event_max = int(values["session_event_max_records"])
        self._write_lock = threading.RLock()
        self._terminal_condition = threading.Condition(self._write_lock)
        self._process_lock = (
            _ControlPlaneProcessLock(self.path, resolved)
            if acquire_process_lock
            else None
        )
        if self._process_lock is not None:
            self._process_lock.acquire()
        try:
            self._prepare_database()
        except BaseException:
            if self._process_lock is not None:
                self._process_lock.release()
            raise

    def close(self) -> None:
        if self._process_lock is not None:
            self._process_lock.release()
            self._process_lock = None

    def __enter__(self) -> "RegistryStorage":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def user_version(self) -> int:
        with self._connect() as connection:
            return int(connection.execute("PRAGMA user_version").fetchone()[0])

    def integrity_check(self) -> str:
        with self._connect() as connection:
            return str(connection.execute("PRAGMA integrity_check").fetchone()[0])

    def _prepare_database(self) -> None:
        if not self.path.exists():
            descriptor = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            os.close(descriptor)
        os.chmod(self.path, 0o600)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, REGISTRY_USER_VERSION}:
                raise InteractiveInternalError(
                    f"unsupported interactive registry user_version: {version}"
                )
            if version == 0:
                _initialize_v1_schema(connection)
            else:
                self._ensure_v1_runtime_ownership_schema(connection)
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(f"{self.path}{suffix}")
            if candidate.exists() and not candidate.is_symlink():
                os.chmod(candidate, 0o600)

    @staticmethod
    def _ensure_v1_runtime_ownership_schema(
        connection: sqlite3.Connection,
    ) -> None:
        """Backfill compatible columns added while the unreleased v1 was built."""

        create_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(create_requests)")
        }
        if "version" not in create_columns:
            connection.execute("ALTER TABLE create_requests ADD COLUMN version TEXT")
        session_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(workspace_sessions)")
        }
        if "version" not in session_columns:
            connection.execute("ALTER TABLE workspace_sessions ADD COLUMN version TEXT")

        runtime_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(runtimes)")
        }
        if "worker_process_id" not in runtime_columns:
            connection.execute(
                "ALTER TABLE runtimes ADD COLUMN worker_process_id INTEGER"
            )
        if "worker_process_identity" not in runtime_columns:
            connection.execute(
                "ALTER TABLE runtimes ADD COLUMN worker_process_identity TEXT"
            )
        if "process_identity" not in runtime_columns:
            connection.execute(
                "ALTER TABLE runtimes ADD COLUMN process_identity TEXT"
            )
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS runtime_startups (
                request_id TEXT PRIMARY KEY
                    REFERENCES create_requests(request_id) ON DELETE CASCADE,
                workspace_session_id TEXT NOT NULL UNIQUE,
                tool_kind TEXT NOT NULL
                    CHECK (tool_kind IN ('innovus', 'primetime')),
                scratch_relative_path TEXT NOT NULL UNIQUE,
                spawn_attempted INTEGER NOT NULL DEFAULT 0
                    CHECK (spawn_attempted IN (0, 1)),
                worker_process_id INTEGER,
                worker_process_identity TEXT,
                process_id INTEGER,
                process_identity TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_runtime_startups_scratch
                ON runtime_startups(scratch_relative_path);
            """
        )
        startup_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(runtime_startups)")
        }
        if "spawn_attempted" not in startup_columns:
            connection.execute(
                """
                ALTER TABLE runtime_startups
                ADD COLUMN spawn_attempted INTEGER NOT NULL DEFAULT 0
                    CHECK (spawn_attempted IN (0, 1))
                """
            )
        if "worker_process_identity" not in startup_columns:
            connection.execute(
                """
                ALTER TABLE runtime_startups
                ADD COLUMN worker_process_identity TEXT
                """
            )
        if "process_identity" not in startup_columns:
            connection.execute(
                "ALTER TABLE runtime_startups ADD COLUMN process_identity TEXT"
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    yield connection
                except BaseException:
                    connection.rollback()
                    raise
                else:
                    connection.commit()

    def _require_create(
        self,
        connection: sqlite3.Connection,
        request_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM create_requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        if row is None:
            raise InteractiveInvalidRequestError(
                f"unknown create request_id: {request_id!r}",
                request_id=request_id,
            )
        return row

    def _require_session(
        self,
        connection: sqlite3.Connection,
        workspace_session_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM workspace_sessions WHERE workspace_session_id = ?",
            (workspace_session_id,),
        ).fetchone()
        if row is None:
            raise InteractiveSessionNotFoundError(
                f"workspace session not found: {workspace_session_id!r}",
                workspace_session_id=workspace_session_id,
            )
        return row

    def _require_runtime(
        self,
        connection: sqlite3.Connection,
        workspace_session_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM runtimes WHERE workspace_session_id = ?",
            (workspace_session_id,),
        ).fetchone()
        if row is None:
            raise InteractiveInternalError(
                f"workspace session has no runtime record: {workspace_session_id!r}",
                workspace_session_id=workspace_session_id,
            )
        return row

    def _require_execution(
        self,
        connection: sqlite3.Connection,
        execution_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM executions WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()
        if row is None:
            raise InteractiveInternalError(f"execution record not found: {execution_id!r}")
        return row

    def _append_event(
        self,
        connection: sqlite3.Connection,
        workspace_session_id: str,
        event_type: str,
        occurred_at: str,
        *,
        transition: dict[str, Any] | None = None,
        runtime_instance_id: str | None = None,
        execution_id: str | None = None,
        reason: dict[str, Any] | None = None,
    ) -> None:
        next_sequence = int(
            connection.execute(
                """
                SELECT COALESCE(MAX(event_sequence), 0) + 1
                FROM session_events WHERE workspace_session_id = ?
                """,
                (workspace_session_id,),
            ).fetchone()[0]
        )
        connection.execute(
            """
            INSERT INTO session_events (
                event_id, workspace_session_id, event_sequence, occurred_at,
                event_type, actor_kind, actor_id, transition_json,
                runtime_instance_id, execution_id, reason_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_id("evt"),
                workspace_session_id,
                next_sequence,
                occurred_at,
                bounded_text(event_type, 128),
                "daemon",
                "local-daemon",
                bounded_json(transition, 16 * 1024),
                runtime_instance_id,
                execution_id,
                bounded_json(reason, 16 * 1024),
            ),
        )
        connection.execute(
            """
            DELETE FROM session_events
            WHERE workspace_session_id = ? AND event_sequence <= (
                SELECT COALESCE(MAX(event_sequence), 0) - ?
                FROM session_events WHERE workspace_session_id = ?
            )
            """,
            (workspace_session_id, self._event_max, workspace_session_id),
        )

    @staticmethod
    def _validate_limits(values: dict[str, Any]) -> None:
        for name, value in values.items():
            minimum = 0 if name == "session_retention_ttl" else 1
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"{name} must be an integer >= {minimum}")
        if values["history_default_page_size"] > values["history_max_page_size"]:
            raise ValueError("history default page size exceeds maximum")
        if values["execution_log_max_bytes"] > values["session_log_max_bytes"]:
            raise ValueError("execution log limit exceeds session log quota")


def _control_plane_lock_paths(
    registry_path: Path,
    paths: InteractivePaths | None,
) -> tuple[Path, ...]:
    """Return fixed root locks plus a registry-identity lock.

    The fixed filename makes separately overridden registries conflict when
    they still share state, runtime, or log ownership.  The registry-specific
    lock also prevents two otherwise-disjoint configurations from opening the
    same SQLite control plane.
    """

    roots = (
        (registry_path.parent,)
        if paths is None
        else (paths.state_root, paths.runtime_root, paths.log_root)
    )
    root_locks = tuple(root / CONTROL_PLANE_LOCK_FILENAME for root in roots)
    registry_lock = registry_path.with_name(f".{registry_path.name}.lock")
    return (*root_locks, registry_lock)


def _control_plane_ownership_roots(
    registry_path: Path,
    paths: InteractivePaths | None,
) -> tuple[Path, ...]:
    roots = (
        (registry_path.parent,)
        if paths is None
        else (
            paths.state_root,
            paths.runtime_root,
            paths.log_root,
            registry_path.parent,
        )
    )
    unique: dict[str, Path] = {}
    for root in roots:
        canonical = Path(os.path.realpath(root, strict=False))
        unique.setdefault(os.path.normcase(str(canonical)), canonical)
    return tuple(unique[key] for key in sorted(unique))


class _ControlPlaneProcessLock:
    """Combine legacy exact-root locks with hierarchical ownership locks."""

    def __init__(
        self,
        registry_path: Path,
        paths: InteractivePaths | None,
    ) -> None:
        self._hierarchical = _HierarchicalRootLockGroup(
            _control_plane_ownership_roots(registry_path, paths)
        )
        self._exact = DaemonFileLockGroup(
            _control_plane_lock_paths(registry_path, paths)
        )
        self._acquired = False

    def acquire(self) -> None:
        if self._acquired:
            return
        self._hierarchical.acquire()
        try:
            self._exact.acquire()
        except BaseException:
            self._hierarchical.release()
            raise
        self._acquired = True

    def release(self) -> None:
        if not self._acquired:
            return
        try:
            self._exact.release()
        finally:
            self._hierarchical.release()
            self._acquired = False


class _HierarchicalRootLockGroup:
    """Use shared ancestor intents and exclusive managed-root ownership.

    Lock files live in one owner-private per-UID namespace rather than in the
    ancestors themselves, some of which may be read-only.  Disjoint roots share
    only shared ancestor locks.  If one root is an ancestor of another, the
    ancestor owner's exclusive lock conflicts with the descendant owner's
    shared intent at that same canonical path in either acquisition order.
    """

    def __init__(self, roots: tuple[Path, ...]) -> None:
        base = Path("/tmp")
        if not base.is_dir():  # pragma: no cover - non-POSIX compatibility guard
            base = Path(tempfile.gettempdir())
        self._namespace = ensure_private_directory(
            base / f"science_eda-interactive-locks-{os.getuid()}"
        )
        requirements: dict[str, tuple[Path, bool]] = {}
        for root in roots:
            canonical = Path(os.path.realpath(root, strict=False))
            root_key = os.path.normcase(str(canonical))
            for candidate in (canonical, *canonical.parents):
                key = os.path.normcase(str(candidate))
                previous = requirements.get(key)
                exclusive = key == root_key or (
                    previous is not None and previous[1]
                )
                requirements[key] = (candidate, exclusive)
        self._requirements = tuple(
            requirements[key]
            for key in sorted(
                requirements,
                key=lambda item: (
                    len(requirements[item][0].parts),
                    item,
                ),
            )
        )
        self._descriptors: list[int] = []

    def acquire(self) -> None:
        import fcntl

        if self._descriptors:
            return
        try:
            for canonical, exclusive in self._requirements:
                digest = hashlib.sha256(
                    os.fsencode(os.path.normcase(str(canonical)))
                ).hexdigest()
                lock_path = self._namespace / f"root-{digest}.lock"
                flags = os.O_CREAT | os.O_RDWR
                flags |= getattr(os, "O_CLOEXEC", 0)
                flags |= getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(lock_path, flags, 0o600)
                try:
                    metadata = os.fstat(descriptor)
                    if (
                        not stat.S_ISREG(metadata.st_mode)
                        or metadata.st_uid != os.getuid()
                    ):
                        raise RuntimeError(
                            "interactive hierarchy lock is not an "
                            f"owner-controlled regular file: {lock_path}"
                        )
                    os.fchmod(descriptor, 0o600)
                    mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
                    fcntl.flock(descriptor, mode | fcntl.LOCK_NB)
                except BaseException:
                    os.close(descriptor)
                    raise
                self._descriptors.append(descriptor)
        except OSError as exc:
            self.release()
            raise RuntimeError(
                "another interactive sandbox daemon owns an overlapping "
                "managed root"
            ) from exc
        except BaseException:
            self.release()
            raise

    def release(self) -> None:
        import fcntl

        while self._descriptors:
            descriptor = self._descriptors.pop()
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


def _initialize_v1_schema(connection: sqlite3.Connection) -> None:
    """Atomically recover an empty v0 fragment and install schema version 1."""

    connection.execute("BEGIN IMMEDIATE")
    try:
        _reset_empty_partial_v1_schema(connection)
        for statement in _iter_sql_statements(SCHEMA_SQL):
            connection.execute(statement)
        connection.execute(f"PRAGMA user_version={REGISTRY_USER_VERSION}")
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def _reset_empty_partial_v1_schema(connection: sqlite3.Connection) -> None:
    """Remove only empty, recognized objects left by pre-atomic v1 startup."""

    objects = connection.execute(
        """
        SELECT type, name FROM sqlite_master
        WHERE name NOT LIKE 'sqlite_%'
        ORDER BY type, name
        """
    ).fetchall()
    for row in objects:
        object_type = str(row[0])
        name = str(row[1])
        if _V1_OBJECT_TYPES.get(name) != object_type:
            raise InteractiveInternalError(
                "unversioned interactive registry contains an unrecognized "
                f"object: {object_type} {name}"
            )

    present_tables = {
        str(row[1]) for row in objects if str(row[0]) == "table"
    }
    for table in present_tables:
        if connection.execute(
            f'SELECT 1 FROM "{table}" LIMIT 1'
        ).fetchone() is not None:
            raise InteractiveInternalError(
                "cannot replace a non-empty unversioned interactive registry "
                f"table: {table}"
            )

    for index in _V1_INDEXES:
        connection.execute(f'DROP INDEX IF EXISTS "{index}"')
    for table in _V1_TABLE_DROP_ORDER:
        connection.execute(f'DROP TABLE IF EXISTS "{table}"')


def _iter_sql_statements(script: str) -> Iterator[str]:
    pending = ""
    for line in script.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statement = pending.strip()
            if statement:
                yield statement
            pending = ""
    if pending.strip():
        raise InteractiveInternalError("interactive registry schema SQL is incomplete")
