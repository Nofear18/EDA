"""Session-scoped file helpers for sandbox HTTP and import backends."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

from science_eda.exceptions import SandboxPathError


def resolve_session_path(root: str, path: str) -> Path:
    """Resolve a session-relative path and reject workspace escapes."""

    if not path or not path.strip():
        raise SandboxPathError("sandbox path must be non-empty")
    raw = Path(path)
    if raw.is_absolute():
        raise SandboxPathError(f"sandbox path must be relative: {path!r}")
    if any(part == ".." for part in raw.parts):
        raise SandboxPathError(f"sandbox path cannot contain '..': {path!r}")

    root_path = Path(root).resolve()
    target = (root_path / raw).resolve()
    try:
        target.relative_to(root_path)
    except ValueError as e:
        raise SandboxPathError(f"sandbox path escapes session workspace: {path!r}") from e
    return target


def read_text_file(root: str, path: str, encoding: str = "utf-8", errors: str = "strict") -> str:
    target = resolve_session_path(root, path)
    return target.read_text(encoding=encoding, errors=errors)


def write_text_file(root: str, path: str, content: str, encoding: str = "utf-8") -> None:
    target = resolve_session_path(root, path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding=encoding)


def write_upload(root: str, target_path: str, content: bytes, *, unzip: bool = False) -> None:
    target = resolve_session_path(root, target_path)
    if unzip:
        _extract_zip_bytes(root, target, content)
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)


def copy_upload(root: str, source_path: str, target_path: str, *, unzip: bool = False) -> None:
    source = Path(source_path).resolve()
    if source.is_dir():
        target = resolve_session_path(root, target_path)
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        shutil.copytree(source, target)
        return
    if not source.is_file():
        raise FileNotFoundError(f"upload source does not exist: {source_path!r}")
    write_upload(root, target_path, source.read_bytes(), unzip=unzip)


@dataclass(frozen=True)
class SnapshotCacheEntry:
    source_dir: Path
    snapshot_path: str


class SnapshotCache:
    """Startup index of sandbox-local Innovus snapshot manifests."""

    def __init__(self, cache_root: str | Path) -> None:
        self.root = Path(cache_root).resolve() if str(cache_root).strip() else None
        self._entries: dict[str, SnapshotCacheEntry] = {}
        if self.root is not None and self.root.is_dir():
            self._build_index()

    def resolve(self, snapshot_ref: str) -> SnapshotCacheEntry:
        key = _normalize_snapshot_ref(snapshot_ref)
        entry = self._entries.get(key)
        if entry is not None:
            return entry

        source_dir = resolve_snapshot_cache_dir(self.root or "", snapshot_ref)
        snapshot_path = _snapshot_path_from_manifest(source_dir)
        return SnapshotCacheEntry(source_dir=source_dir, snapshot_path=snapshot_path)

    def _build_index(self) -> None:
        assert self.root is not None
        for manifest_path in self.root.rglob("snapshot_manifest.json"):
            source_dir = manifest_path.parent.resolve()
            try:
                source_dir.relative_to(self.root)
                snapshot_path = _snapshot_path_from_manifest(source_dir)
            except (OSError, SandboxPathError, ValueError):
                continue
            entry = SnapshotCacheEntry(source_dir=source_dir, snapshot_path=snapshot_path)
            for key in _snapshot_ref_keys(self.root, source_dir):
                self._entries[key] = entry


def stage_snapshot_from_cache(
    root: str,
    cache: SnapshotCache | str | Path,
    snapshot_ref: str,
    target_path: str,
) -> str:
    """Materialize one cached Innovus snapshot as a read-only session copy."""

    entry = cache.resolve(snapshot_ref) if isinstance(cache, SnapshotCache) else None
    source_dir = (
        entry.source_dir
        if entry is not None
        else resolve_snapshot_cache_dir(cache, snapshot_ref)
    )
    target = _resolve_session_entry_path(root, target_path)
    snapshot_path = (
        entry.snapshot_path
        if entry is not None
        else _snapshot_path_from_manifest(source_dir)
    )
    snapshot_source = source_dir / snapshot_path
    if not snapshot_source.exists():
        raise FileNotFoundError(f"snapshot payload does not exist: {snapshot_source}")
    cache_root = _snapshot_cache_root(cache)
    _validate_snapshot_source_tree(
        source_dir,
        allowed_symlink_roots=_snapshot_allowed_symlink_roots(cache_root),
    )

    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        _remove_stage_target(target)
    shutil.copytree(source_dir, target)
    _make_tree_read_only(target)
    _stage_snapshot_support_assets(root, cache, target)
    return _session_relative_snapshot_path(root, target, snapshot_path)


def resolve_snapshot_cache_dir(cache_root: str | Path, snapshot_ref: str) -> Path:
    """Resolve a snapshot reference under a sandbox-local snapshot cache."""

    if not str(cache_root).strip():
        raise FileNotFoundError("innovus_snapshot_cache_root is not configured")
    raw = Path(snapshot_ref)
    if raw.is_absolute():
        raise SandboxPathError(f"snapshot_ref must be relative: {snapshot_ref!r}")
    if any(part == ".." for part in raw.parts):
        raise SandboxPathError(f"snapshot_ref cannot contain '..': {snapshot_ref!r}")

    parts = list(raw.parts)
    if parts and parts[0] == "snapshots":
        parts = parts[1:]
    if parts and parts[-1] == "snapshot_manifest.json":
        parts = parts[:-1]
    if not parts:
        raise SandboxPathError(f"snapshot_ref does not identify a snapshot: {snapshot_ref!r}")

    root_path = Path(cache_root).resolve()
    source_dir = root_path.joinpath(*parts).resolve()
    try:
        source_dir.relative_to(root_path)
    except ValueError as e:
        raise SandboxPathError(f"snapshot_ref escapes snapshot cache: {snapshot_ref!r}") from e
    if not source_dir.is_dir():
        raise FileNotFoundError(f"snapshot cache entry does not exist: {source_dir}")
    return source_dir


def _normalize_snapshot_ref(snapshot_ref: str) -> str:
    raw = Path(snapshot_ref)
    if raw.is_absolute():
        raise SandboxPathError(f"snapshot_ref must be relative: {snapshot_ref!r}")
    if any(part == ".." for part in raw.parts):
        raise SandboxPathError(f"snapshot_ref cannot contain '..': {snapshot_ref!r}")

    parts = list(raw.parts)
    if parts and parts[0] == "snapshots":
        parts = parts[1:]
    if parts and parts[-1] == "snapshot_manifest.json":
        parts = parts[:-1]
    if not parts:
        raise SandboxPathError(f"snapshot_ref does not identify a snapshot: {snapshot_ref!r}")
    return Path(*parts).as_posix()


def _snapshot_ref_keys(root: Path, source_dir: Path) -> list[str]:
    rel = source_dir.relative_to(root).as_posix()
    return [
        rel,
        f"{rel}/snapshot_manifest.json",
        f"snapshots/{rel}",
        f"snapshots/{rel}/snapshot_manifest.json",
    ]


def _resolve_session_entry_path(root: str, path: str) -> Path:
    """Resolve a session path without following the final path component."""

    if not path or not path.strip():
        raise SandboxPathError("sandbox path must be non-empty")
    raw = Path(path)
    if raw.is_absolute():
        raise SandboxPathError(f"sandbox path must be relative: {path!r}")
    if any(part == ".." for part in raw.parts):
        raise SandboxPathError(f"sandbox path cannot contain '..': {path!r}")

    root_path = Path(root).resolve()
    target = root_path / raw
    try:
        target.parent.resolve().relative_to(root_path)
    except ValueError as e:
        raise SandboxPathError(f"sandbox path escapes session workspace: {path!r}") from e
    return target


def _validate_snapshot_source_tree(
    source_dir: Path,
    *,
    allowed_symlink_roots: tuple[Path, ...] = (),
) -> None:
    source_root = source_dir.resolve()
    allowed_roots = tuple(
        root.resolve() for root in allowed_symlink_roots if root.is_dir()
    )
    for path in source_dir.rglob("*"):
        if not path.is_symlink():
            continue
        try:
            target = path.resolve(strict=True)
        except OSError as exc:
            raise SandboxPathError(
                f"snapshot cache contains broken symlink: {path}"
            ) from exc
        if not target.is_file():
            raise SandboxPathError(
                f"snapshot cache symlink must target a file: {path} -> {target}"
            )
        if _path_is_relative_to(target, source_root) or any(
            _path_is_relative_to(target, root) for root in allowed_roots
        ):
            continue
        raise SandboxPathError(
            f"snapshot cache symlink escapes allowed roots: {path} -> {target}"
        )


def _snapshot_cache_root(cache: SnapshotCache | str | Path) -> Path | None:
    if isinstance(cache, SnapshotCache):
        return cache.root
    if str(cache).strip():
        return Path(cache).resolve()
    return None


def _snapshot_allowed_symlink_roots(cache_root: Path | None) -> tuple[Path, ...]:
    if cache_root is None:
        return ()
    return (cache_root.parent / "designs",)


def _path_is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _remove_stage_target(target: Path) -> None:
    if target.is_symlink() or target.is_file():
        target.unlink()
        return
    if target.is_dir():
        _make_tree_writable(target)
        shutil.rmtree(target)
        return
    target.unlink()


def _make_tree_writable(root: Path) -> None:
    for dirpath, _dirnames, filenames in os.walk(root):
        Path(dirpath).chmod(0o755)
        for filename in filenames:
            path = Path(dirpath) / filename
            if not path.is_symlink():
                path.chmod(0o644)


def _make_tree_read_only(root: Path) -> None:
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            path = Path(dirpath) / filename
            if not path.is_symlink():
                path.chmod(0o444)
    for dirpath, dirnames, _filenames in os.walk(root, topdown=False):
        for dirname in dirnames:
            path = Path(dirpath) / dirname
            if not path.is_symlink():
                path.chmod(0o555)
        Path(dirpath).chmod(0o555)


def _stage_snapshot_support_assets(
    root: str,
    cache: SnapshotCache | str | Path,
    snapshot_target: Path,
) -> None:
    cache_root = _snapshot_cache_root(cache)
    if cache_root is None:
        return
    designs_source = cache_root.parent / "designs"
    if not designs_source.is_dir():
        return

    asset_root = _session_asset_root_for_snapshot_target(root, snapshot_target)
    if asset_root is None:
        return
    designs_target = asset_root / "designs"
    if designs_target.exists() or designs_target.is_symlink():
        return

    _validate_snapshot_source_tree(designs_source)
    designs_target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(designs_source, designs_target)
    _make_tree_read_only(designs_target)


def _session_asset_root_for_snapshot_target(root: str, snapshot_target: Path) -> Path | None:
    root_path = Path(root).resolve()
    try:
        rel_parts = snapshot_target.relative_to(root_path).parts
    except ValueError:
        return None
    try:
        snapshots_index = rel_parts.index("snapshots")
    except ValueError:
        return None
    asset_root = root_path.joinpath(*rel_parts[:snapshots_index])
    try:
        asset_root.resolve().relative_to(root_path)
    except ValueError:
        return None
    return asset_root


def _extract_zip_bytes(root: str, target: Path, content: bytes) -> None:
    root_path = Path(root).resolve()
    with tempfile.TemporaryDirectory(dir=str(root_path)) as temp_dir:
        temp_path = Path(temp_dir)
        archive_path = temp_path / "upload.zip"
        staging = temp_path / "extract"
        archive_path.write_bytes(content)
        with zipfile.ZipFile(archive_path, "r") as archive:
            members = archive.infolist()
            _validate_zip_members(root_path, target, members)
            staging.mkdir()
            for member in members:
                member_path = Path(member.filename)
                destination = (staging / member_path).resolve()
                if member.is_dir():
                    destination.mkdir(parents=True, exist_ok=True)
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member, "r") as src, open(destination, "wb") as dst:
                    shutil.copyfileobj(src, dst)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        shutil.move(str(staging), str(target))


def _validate_zip_members(
    root_path: Path,
    target: Path,
    members: list[zipfile.ZipInfo],
) -> None:
    for member in members:
        member_path = Path(member.filename)
        if member_path.is_absolute() or any(part == ".." for part in member_path.parts):
            raise SandboxPathError(
                f"zip member escapes session workspace: {member.filename!r}"
            )
        destination = (target / member_path).resolve()
        try:
            destination.relative_to(root_path)
        except ValueError as e:
            raise SandboxPathError(
                f"zip member escapes session workspace: {member.filename!r}"
            ) from e


def _snapshot_path_from_manifest(source_dir: Path) -> str:
    manifest_path = source_dir / "snapshot_manifest.json"
    if not manifest_path.is_file():
        return "design.enc"
    with manifest_path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    if not isinstance(manifest, dict):
        raise ValueError(f"Expected object JSON in {manifest_path}")
    snapshot_path = str(manifest.get("snapshot_path", "design.enc"))
    raw = Path(snapshot_path)
    if raw.is_absolute() or any(part == ".." for part in raw.parts):
        raise SandboxPathError(f"snapshot_path escapes snapshot cache: {snapshot_path!r}")
    return snapshot_path


def _session_relative_snapshot_path(root: str, target: Path, snapshot_path: str) -> str:
    path = target / snapshot_path
    return path.relative_to(Path(root).resolve()).as_posix()
