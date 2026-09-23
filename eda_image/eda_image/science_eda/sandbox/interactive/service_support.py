"""Small process/accounting helpers used by interactive service orchestration."""

from __future__ import annotations

import os
import signal
import threading
import time
from dataclasses import dataclass, field as dataclass_field

from science_eda.sandbox.interactive.models import OrphanRuntimeRecord
from science_eda.sandbox.interactive.process_identity import (
    process_group_exists,
    recorded_group_state,
)
from science_eda.sandbox.interactive.runtime import DedicatedToolRuntime
from science_eda.sandbox.interactive.scheduler import CapacityLease


@dataclass
class RuntimeController:
    workspace_session_id: str
    runtime: DedicatedToolRuntime
    lease: CapacityLease
    scratch_relative_path: str
    startup_request_id: str | None = None
    released: bool = False
    release_lock: threading.Lock = dataclass_field(default_factory=threading.Lock)


@dataclass
class OrphanRuntimeController:
    record: OrphanRuntimeRecord
    lease: CapacityLease
    unknown_release_at: float | None = None


def process_group_is_alive(process_group_id: int | None) -> bool:
    """Check the whole daemon-created process group, not only its leader."""

    if process_group_exists(process_group_id):
        return True
    # The worker can be observed in the tiny interval before `setsid`.
    if process_group_id is None or process_group_id <= 0:
        return False
    try:
        os.kill(process_group_id, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


def pid_is_alive(pid: int | None) -> bool:
    """Compatibility name for daemon-owned process-group liveness."""

    return process_group_is_alive(pid)


def runtime_processes_alive(runtime: DedicatedToolRuntime) -> bool:
    value = getattr(runtime, "processes_alive", None)
    if isinstance(value, bool):
        return value
    return bool(getattr(runtime, "is_alive", False))


def terminate_recorded_processes(
    worker_process_id: int | None,
    process_id: int | None,
    *,
    worker_process_identity: str | None = None,
    process_identity: str | None = None,
    timeout: float = 2.0,
) -> bool:
    """Terminate recorded daemon-owned process groups and confirm they exited."""

    identities: dict[int, str | None] = {}
    for pid, identity in (
        (process_id, process_identity),
        (worker_process_id, worker_process_identity),
    ):
        if pid is not None and pid > 1 and pid != os.getpid():
            identities.setdefault(pid, identity)
    if not identities:
        return True
    if any(
        recorded_group_state(pid, identity) == "unknown"
        for pid, identity in identities.items()
    ):
        return False
    for sig in (signal.SIGTERM, signal.SIGKILL):
        for pid, identity in identities.items():
            if recorded_group_state(pid, identity) != "owned":
                continue
            try:
                if hasattr(os, "killpg"):
                    os.killpg(pid, sig)
                else:
                    os.kill(pid, sig)
            except (ProcessLookupError, PermissionError, OSError):
                pass
        deadline = time.monotonic() + max(0.0, timeout / 2.0)
        while time.monotonic() < deadline:
            if all(
                recorded_group_state(pid, identity) == "dead"
                for pid, identity in identities.items()
            ):
                return True
            time.sleep(0.05)
    return all(
        recorded_group_state(pid, identity) == "dead"
        for pid, identity in identities.items()
    )


def bounded_message(value: str, byte_limit: int) -> str:
    encoded = str(value).encode("utf-8", errors="replace")
    if len(encoded) <= byte_limit:
        return encoded.decode("utf-8", errors="replace")
    return encoded[:byte_limit].decode("utf-8", errors="ignore")
