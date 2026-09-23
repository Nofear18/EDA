"""Trusted-local workspace validation and daemon-owned path operations.

No cleanup function in this module accepts an arbitrary absolute path. Runtime
scratch and log cleanup are addressed only by validated service-generated IDs or
relative names beneath previously validated daemon roots.
"""

from __future__ import annotations

import os
import re
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from science_eda.config import SandboxConfig
from science_eda.exceptions import (
    InteractiveInternalError,
    InteractiveInvalidRequestError,
    InvalidWorkspacePathError,
)
from science_eda.sandbox.interactive.models import new_id


_SERVICE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{1,127}$")
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


@dataclass(frozen=True)
class InteractivePaths:
    state_root: Path
    runtime_root: Path
    log_root: Path
    registry_path: Path
    managed_roots: tuple[Path, ...]


@dataclass(frozen=True)
class RuntimeScratch:
    relative_path: str
    path: Path


@dataclass(frozen=True)
class ExecutionLogLocation:
    full_log_ref: str
    path: Path


def validate_requested_session_id(value: str) -> str:
    """Validate a caller-selected session id before using it in managed paths."""

    if not isinstance(value, str) or not _SESSION_ID_RE.fullmatch(value):
        raise InteractiveInvalidRequestError(
            "session_id must be 1-128 ASCII letters, digits, underscores, or "
            "hyphens and must start with a letter or digit",
            session_id=value if isinstance(value, str) else None,
        )
    return value


def resolve_interactive_paths(
    config: SandboxConfig,
    *,
    create: bool = True,
) -> InteractivePaths:
    """Resolve derived interactive paths and optionally create private roots."""

    config.validate_interactive()
    state_raw = _absolute_config_path(
        config.interactive_state_root,
        "interactive_state_root",
    )
    runtime_raw = _absolute_config_path(
        config.interactive_runtime_root,
        "interactive_runtime_root",
        default=state_raw / "runtime",
    )
    log_raw = _absolute_config_path(
        config.interactive_log_root,
        "interactive_log_root",
        default=state_raw / "logs",
    )
    registry_raw = _absolute_config_path(
        config.interactive_registry_path,
        "interactive_registry_path",
        default=state_raw / "registry.sqlite3",
    )

    if create:
        state_root = ensure_private_directory(state_raw)
        runtime_root = ensure_private_directory(runtime_raw)
        log_root = ensure_private_directory(log_raw)
        registry_parent = ensure_private_directory(registry_raw.parent)
        registry_path = registry_parent / registry_raw.name
        prepare_private_file_path(registry_path)
    else:
        state_root = _canonical_without_creation(state_raw)
        runtime_root = _canonical_without_creation(runtime_raw)
        log_root = _canonical_without_creation(log_raw)
        registry_path = _canonical_without_creation(registry_raw)

    sandbox_raw = (
        Path(config.sandbox_root).expanduser()
        if config.sandbox_root
        else Path(tempfile.gettempdir()) / "eda_sandbox"
    )
    if not sandbox_raw.is_absolute():
        sandbox_raw = Path(os.path.abspath(sandbox_raw))
    sandbox_root = _canonical_without_creation(sandbox_raw)
    managed = _deduplicate_paths(
        (state_root, runtime_root, log_root, registry_path, sandbox_root)
    )
    return InteractivePaths(
        state_root=state_root,
        runtime_root=runtime_root,
        log_root=log_root,
        registry_path=registry_path,
        managed_roots=managed,
    )


def canonicalize_workspace_path(
    workspace_path: str,
    paths_or_roots: InteractivePaths | Iterable[str | Path],
) -> Path:
    """Validate and canonicalize a caller workspace without mutating it."""

    if not isinstance(workspace_path, str) or not workspace_path or "\x00" in workspace_path:
        raise InvalidWorkspacePathError(
            "workspace_path must be a non-empty absolute path"
        )
    raw = Path(workspace_path)
    if not raw.is_absolute():
        raise InvalidWorkspacePathError("workspace_path must be absolute")
    try:
        canonical = Path(os.path.realpath(raw, strict=True))
    except (OSError, RuntimeError) as exc:
        raise InvalidWorkspacePathError(
            f"workspace_path cannot be resolved: {workspace_path!r}"
        ) from exc
    try:
        metadata = canonical.stat()
    except OSError as exc:
        raise InvalidWorkspacePathError(
            f"workspace_path is not accessible: {workspace_path!r}"
        ) from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise InvalidWorkspacePathError("workspace_path must name a directory")

    # Reading a directory and searching/chdir'ing into it are separate Unix
    # permissions.  ``os.open(..., O_RDONLY)`` alone can succeed without the
    # execute/search bit, only for runtime startup to fail later when it tries
    # to establish the requested cwd.  Reject that case before any tool process
    # is spawned; the runtime performs its own final cwd check for TOCTOU races.
    try:
        searchable = os.access(canonical, os.X_OK, effective_ids=True)
    except TypeError:  # pragma: no cover - platforms without effective_ids
        searchable = os.access(canonical, os.X_OK)
    if not searchable:
        raise InvalidWorkspacePathError(
            "workspace_path cannot be used as a runtime working directory"
        )

    flags = os.O_RDONLY
    flags |= getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(canonical, flags)
    except OSError as exc:
        raise InvalidWorkspacePathError(
            "workspace_path cannot be used as a runtime working directory"
        ) from exc
    else:
        os.close(descriptor)

    managed_roots = (
        paths_or_roots.managed_roots
        if isinstance(paths_or_roots, InteractivePaths)
        else tuple(Path(item) for item in paths_or_roots)
    )
    for managed in managed_roots:
        managed_canonical = _canonical_without_creation(managed)
        if _paths_overlap(canonical, managed_canonical):
            raise InvalidWorkspacePathError(
                "workspace_path overlaps a daemon-managed path",
                details={"managed_path": str(managed_canonical)},
            )
    return canonical


def ensure_private_directory(path: str | Path) -> Path:
    """Create or validate an owner-only, non-symlink daemon directory."""

    raw = Path(path).expanduser()
    if not raw.is_absolute():
        raise InteractiveInternalError(f"managed directory must be absolute: {raw}")
    if raw == Path(raw.anchor):
        raise InteractiveInternalError("filesystem root cannot be a managed directory")
    try:
        raw.mkdir(mode=0o700, parents=True, exist_ok=True)
        metadata = raw.lstat()
    except OSError as exc:
        raise InteractiveInternalError(
            f"cannot prepare managed directory: {raw}"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise InteractiveInternalError(
            f"managed directory must be a non-symlink directory: {raw}"
        )
    if metadata.st_uid != os.getuid():
        raise InteractiveInternalError(
            f"managed directory is not owned by the daemon user: {raw}"
        )
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise InteractiveInternalError(
            f"managed directory must have owner-only permissions: {raw}"
        )
    canonical = Path(os.path.realpath(raw, strict=True))
    return canonical


def prepare_private_file_path(path: str | Path) -> Path:
    """Validate an absent or owner-owned non-symlink private file path."""

    candidate = Path(path)
    parent = ensure_private_directory(candidate.parent)
    candidate = parent / candidate.name
    try:
        metadata = candidate.lstat()
    except FileNotFoundError:
        return candidate
    except OSError as exc:
        raise InteractiveInternalError(f"cannot inspect private file: {candidate}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise InteractiveInternalError(
            f"private file must be a non-symlink regular file: {candidate}"
        )
    if metadata.st_uid != os.getuid():
        raise InteractiveInternalError(
            f"private file is not owned by the daemon user: {candidate}"
        )
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise InteractiveInternalError(
            f"private file must have owner-only permissions: {candidate}"
        )
    return candidate


def create_runtime_scratch(
    paths: InteractivePaths,
    workspace_session_id: str,
) -> RuntimeScratch:
    _validate_workspace_session_id(workspace_session_id)
    root = ensure_private_directory(paths.runtime_root)
    relative = f"session_{workspace_session_id}"
    target = root / relative
    try:
        os.mkdir(target, mode=0o700)
    except FileExistsError as exc:
        raise InteractiveInternalError(
            f"runtime scratch already exists: {relative}"
        ) from exc
    except OSError as exc:
        raise InteractiveInternalError(
            f"cannot create runtime scratch: {relative}"
        ) from exc
    return RuntimeScratch(relative_path=relative, path=target)


def remove_runtime_scratch(paths: InteractivePaths, relative_path: str) -> bool:
    """Remove one validated scratch relative name; never accepts a workspace."""

    workspace_session_id = _session_id_from_scratch(relative_path)
    _validate_workspace_session_id(workspace_session_id)
    root = ensure_private_directory(paths.runtime_root)
    target = root / relative_path
    return _remove_controlled_tree(root, target)


def create_execution_log(
    paths: InteractivePaths,
    workspace_session_id: str,
    execution_id: str,
    *,
    full_log_ref: str | None = None,
) -> ExecutionLogLocation:
    _validate_workspace_session_id(workspace_session_id)
    _validate_service_id(execution_id, "execution_id", prefix="exec_")
    reference = full_log_ref or new_id("log")
    _validate_service_id(reference, "full_log_ref", prefix="log_")
    root = ensure_private_directory(paths.log_root)
    sessions = _ensure_private_child(root, "sessions")
    session_dir = _ensure_private_child(sessions, workspace_session_id)
    executions = _ensure_private_child(session_dir, "executions")
    execution_dir = executions / execution_id
    try:
        os.mkdir(execution_dir, mode=0o700)
        descriptor = os.open(
            execution_dir / "output.log",
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    except FileExistsError as exc:
        raise InteractiveInternalError(
            f"execution log already exists for {execution_id}"
        ) from exc
    except OSError as exc:
        try:
            _remove_controlled_tree(root, execution_dir)
        except Exception:
            # Preserve the original log-creation error.  A later retention or
            # startup reconciliation pass can retry a cleanup that raced with
            # another same-UID filesystem mutation.
            pass
        raise InteractiveInternalError(
            f"cannot create execution log for {execution_id}"
        ) from exc
    else:
        os.close(descriptor)
    return ExecutionLogLocation(reference, execution_dir / "output.log")


def remove_execution_log(
    paths: InteractivePaths,
    workspace_session_id: str,
    execution_id: str,
) -> bool:
    _validate_workspace_session_id(workspace_session_id)
    _validate_service_id(execution_id, "execution_id", prefix="exec_")
    root = ensure_private_directory(paths.log_root)
    execution_root = root / "sessions" / workspace_session_id / "executions"
    if not execution_root.exists():
        return False
    target = execution_root / execution_id
    return _remove_controlled_tree(root, target)


def remove_session_logs(paths: InteractivePaths, workspace_session_id: str) -> bool:
    _validate_workspace_session_id(workspace_session_id)
    root = ensure_private_directory(paths.log_root)
    target = root / "sessions" / workspace_session_id
    return _remove_controlled_tree(root, target)


def _absolute_config_path(
    value: str,
    name: str,
    *,
    default: Path | None = None,
) -> Path:
    candidate = Path(value).expanduser() if value else default
    if candidate is None or not candidate.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    if candidate.name in {"", ".", ".."}:
        raise ValueError(f"{name} must name a concrete path")
    return candidate


def _canonical_without_creation(path: str | Path) -> Path:
    return Path(os.path.realpath(Path(path).expanduser(), strict=False))


def _deduplicate_paths(paths: Iterable[Path]) -> tuple[Path, ...]:
    result: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = os.path.normcase(str(path))
        if key not in seen:
            seen.add(key)
            result.append(path)
    return tuple(result)


def _paths_overlap(left: Path, right: Path) -> bool:
    try:
        left.relative_to(right)
        return True
    except ValueError:
        pass
    try:
        right.relative_to(left)
        return True
    except ValueError:
        return False


def _validate_service_id(value: str, name: str, *, prefix: str) -> None:
    if (
        not isinstance(value, str)
        or not value.startswith(prefix)
        or not _SERVICE_ID_RE.fullmatch(value)
    ):
        raise InteractiveInternalError(f"invalid service-generated {name}: {value!r}")


def _validate_workspace_session_id(value: str) -> None:
    if not isinstance(value, str) or not _SESSION_ID_RE.fullmatch(value):
        raise InteractiveInternalError(f"invalid workspace_session_id: {value!r}")


def _session_id_from_scratch(relative_path: str) -> str:
    if (
        not isinstance(relative_path, str)
        or "/" in relative_path
        or "\\" in relative_path
        or not relative_path.startswith("session_")
    ):
        raise InteractiveInternalError(
            f"invalid runtime scratch relative path: {relative_path!r}"
        )
    workspace_session_id = relative_path.removeprefix("session_")
    _validate_workspace_session_id(workspace_session_id)
    return workspace_session_id


def _ensure_private_child(parent: Path, name: str) -> Path:
    child = parent / name
    try:
        os.mkdir(child, mode=0o700)
    except FileExistsError:
        pass
    try:
        metadata = child.lstat()
    except OSError as exc:
        raise InteractiveInternalError(f"cannot inspect managed child: {child}") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise InteractiveInternalError(f"unsafe managed child directory: {child}")
    return child


def _remove_controlled_tree(root: Path, target: Path) -> bool:
    try:
        relative = target.relative_to(root)
    except ValueError as exc:  # pragma: no cover - defense in depth
        raise InteractiveInternalError("cleanup target escaped its managed root") from exc
    parts = relative.parts
    if not parts:
        raise InteractiveInternalError("cleanup cannot remove a managed root")

    root_fd = _open_directory_path(root, "managed cleanup root")
    opened_parents: list[int] = []
    try:
        parent_fd = root_fd
        for index, component in enumerate(parts[:-1]):
            display = root.joinpath(*parts[: index + 1])
            try:
                next_fd, _metadata = _open_directory_at(
                    parent_fd,
                    component,
                    display,
                )
            except FileNotFoundError:
                return False
            opened_parents.append(next_fd)
            parent_fd = next_fd

        target_name = parts[-1]
        try:
            target_fd, target_metadata = _open_directory_at(
                parent_fd,
                target_name,
                target,
            )
        except FileNotFoundError:
            return False
        try:
            _remove_directory_contents(target_fd, target)
            _verify_directory_entry(parent_fd, target_name, target_metadata, target)
            os.rmdir(target_name, dir_fd=parent_fd)
        finally:
            os.close(target_fd)
        return True
    finally:
        for descriptor in reversed(opened_parents):
            os.close(descriptor)
        os.close(root_fd)


def _directory_open_flags() -> int:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    return flags


def _open_directory_path(path: Path, description: str) -> int:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise InteractiveInternalError(f"cannot inspect {description}: {path}") from exc
    _validate_controlled_directory(metadata, path)
    try:
        descriptor = os.open(path, _directory_open_flags())
    except OSError as exc:
        raise InteractiveInternalError(f"cannot safely open {description}: {path}") from exc
    opened_metadata = os.fstat(descriptor)
    if _directory_identity(metadata) != _directory_identity(opened_metadata):
        os.close(descriptor)
        raise InteractiveInternalError(f"{description} changed while opening: {path}")
    return descriptor


def _open_directory_at(
    parent_fd: int,
    name: str,
    display_path: Path,
) -> tuple[int, os.stat_result]:
    try:
        metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise InteractiveInternalError(
            f"cannot inspect cleanup directory component: {display_path}"
        ) from exc
    _validate_controlled_directory(metadata, display_path)
    try:
        descriptor = os.open(
            name,
            _directory_open_flags(),
            dir_fd=parent_fd,
        )
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise InteractiveInternalError(
            f"cannot safely open cleanup directory component: {display_path}"
        ) from exc
    opened_metadata = os.fstat(descriptor)
    if _directory_identity(metadata) != _directory_identity(opened_metadata):
        os.close(descriptor)
        raise InteractiveInternalError(
            f"cleanup directory component changed while opening: {display_path}"
        )
    return descriptor, opened_metadata


def _remove_directory_contents(directory_fd: int, display_path: Path) -> None:
    try:
        names = os.listdir(directory_fd)
    except OSError as exc:
        raise InteractiveInternalError(
            f"cannot list controlled cleanup directory: {display_path}"
        ) from exc
    for name in names:
        child_path = display_path / name
        try:
            metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise InteractiveInternalError(
                f"cannot inspect controlled cleanup entry: {child_path}"
            ) from exc
        if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
            try:
                child_fd, child_metadata = _open_directory_at(
                    directory_fd,
                    name,
                    child_path,
                )
            except FileNotFoundError:
                continue
            try:
                _remove_directory_contents(child_fd, child_path)
                _verify_directory_entry(
                    directory_fd,
                    name,
                    child_metadata,
                    child_path,
                )
                os.rmdir(name, dir_fd=directory_fd)
            finally:
                os.close(child_fd)
            continue
        try:
            os.unlink(name, dir_fd=directory_fd)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise InteractiveInternalError(
                f"cannot remove controlled cleanup entry: {child_path}"
            ) from exc


def _verify_directory_entry(
    parent_fd: int,
    name: str,
    expected: os.stat_result,
    display_path: Path,
) -> None:
    try:
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        raise InteractiveInternalError(
            f"cleanup directory changed before removal: {display_path}"
        ) from exc
    if _directory_identity(current) != _directory_identity(expected):
        raise InteractiveInternalError(
            f"cleanup directory changed before removal: {display_path}"
        )


def _validate_controlled_directory(metadata: os.stat_result, path: Path) -> None:
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise InteractiveInternalError(
            f"cleanup path contains an unsafe directory component: {path}"
        )
    if metadata.st_uid != os.getuid():
        raise InteractiveInternalError(
            f"cleanup directory is not owned by the daemon user: {path}"
        )


def _directory_identity(metadata: os.stat_result) -> tuple[int, int, int]:
    return (metadata.st_dev, metadata.st_ino, stat.S_IFMT(metadata.st_mode))
