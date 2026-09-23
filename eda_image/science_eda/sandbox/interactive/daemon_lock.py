"""Single-control-plane guard and trusted-loopback validation."""

from __future__ import annotations

import ipaddress
import os
import stat
from pathlib import Path
from types import TracebackType


CONTROL_PLANE_LOCK_FILENAME = ".science_eda_interactive.lock"


def is_loopback_host(host: str) -> bool:
    if not isinstance(host, str) or host != host.strip():
        return False
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def require_loopback_host(host: str) -> None:
    if not is_loopback_host(host):
        raise ValueError(
            "interactive sandbox requires a loopback server_host; "
            "set interactive_enabled=false for a non-local legacy batch server"
        )


class DaemonFileLock:
    """Advisory process lock held for the lifetime of the SQLite control plane."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._fd: int | None = None

    def acquire(self) -> None:
        import fcntl

        if self._fd is not None:
            return
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise RuntimeError(
                f"cannot safely open interactive daemon lock {self.path}"
            ) from exc
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
            os.close(fd)
            raise RuntimeError(
                f"interactive daemon lock is not a daemon-owned regular file: {self.path}"
            )
        os.fchmod(fd, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise RuntimeError(
                f"another interactive sandbox daemon owns {self.path}"
            ) from exc
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        import fcntl

        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "DaemonFileLock":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()


class DaemonFileLockGroup:
    """Acquire several root locks in a stable order and release them together."""

    def __init__(self, paths: list[str | Path] | tuple[str | Path, ...]) -> None:
        unique: dict[str, Path] = {}
        for raw_path in paths:
            path = Path(raw_path)
            key = os.path.normcase(os.path.abspath(path))
            unique.setdefault(key, path)
        self._locks = [DaemonFileLock(unique[key]) for key in sorted(unique)]
        self._acquired: list[DaemonFileLock] = []

    def acquire(self) -> None:
        if self._acquired:
            return
        try:
            for lock in self._locks:
                lock.acquire()
                self._acquired.append(lock)
        except BaseException:
            self.release()
            raise

    def release(self) -> None:
        while self._acquired:
            self._acquired.pop().release()

    def __enter__(self) -> "DaemonFileLockGroup":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
