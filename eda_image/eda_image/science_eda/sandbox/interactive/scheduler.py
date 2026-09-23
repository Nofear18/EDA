"""Independent capacity scheduler for interactive EDA runtimes."""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass


class CapacityWaitTimeout(RuntimeError):
    """No interactive runtime slot became available before the deadline."""


class SchedulerClosed(RuntimeError):
    """The daemon is shutting down and no new leases may be acquired."""


@dataclass(frozen=True)
class CapacitySnapshot:
    capacity: int
    starting: int
    active: int
    waiting: int


@dataclass
class CapacityLease:
    """A one-process permit owned by exactly one WorkspaceSession runtime."""

    scheduler: "InteractiveCapacityScheduler"
    tool_kind: str
    lease_id: str
    _released: bool = False

    def mark_ready(self) -> None:
        self.scheduler.mark_ready(self)

    def release(self) -> None:
        if self._released:
            return
        self.scheduler.release(self)
        self._released = True


class InteractiveCapacityScheduler:
    """Hard-partitioned per-tool slots; no worker or capacity borrowing."""

    def __init__(self, capacities: dict[str, int]) -> None:
        normalized = {str(kind): int(value) for kind, value in capacities.items()}
        if not normalized or any(value <= 0 for value in normalized.values()):
            raise ValueError("interactive tool capacities must be positive")
        self._capacities = normalized
        self._leases: dict[str, tuple[str, str]] = {}
        self._waiting = {kind: 0 for kind in normalized}
        self._closed = False
        self._cond = threading.Condition(threading.RLock())

    def acquire(self, tool_kind: str, timeout: float) -> CapacityLease:
        if tool_kind not in self._capacities:
            raise ValueError(f"unsupported interactive tool kind: {tool_kind!r}")
        deadline = time.monotonic() + timeout if timeout > 0 else None
        with self._cond:
            self._waiting[tool_kind] += 1
            try:
                while True:
                    if self._closed:
                        raise SchedulerClosed("interactive capacity scheduler is closed")
                    used = sum(
                        1 for kind, _state in self._leases.values() if kind == tool_kind
                    )
                    if used < self._capacities[tool_kind]:
                        lease_id = f"lease_{uuid.uuid4().hex}"
                        self._leases[lease_id] = (tool_kind, "starting")
                        return CapacityLease(self, tool_kind, lease_id)
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise CapacityWaitTimeout(
                                f"interactive {tool_kind} capacity wait timed out"
                            )
                        self._cond.wait(timeout=remaining)
                    else:
                        self._cond.wait()
            finally:
                self._waiting[tool_kind] -= 1

    def mark_ready(self, lease: CapacityLease) -> None:
        with self._cond:
            current = self._leases.get(lease.lease_id)
            if current is None:
                raise SchedulerClosed("interactive capacity lease is no longer active")
            if current[0] != lease.tool_kind:
                raise RuntimeError("interactive capacity lease tool kind mismatch")
            self._leases[lease.lease_id] = (lease.tool_kind, "active")

    def reserve_existing(self, tool_kind: str) -> CapacityLease:
        """Quarantine capacity for a process surviving a daemon restart.

        A live orphan must count against the per-tool limit even if an older
        daemon died before publishing (or releasing) its ordinary lease.  The
        reservation may temporarily put accounting above a newly reduced
        configured capacity; new acquisitions remain blocked until it exits.
        """

        if tool_kind not in self._capacities:
            raise ValueError(f"unsupported interactive tool kind: {tool_kind!r}")
        with self._cond:
            if self._closed:
                raise SchedulerClosed("interactive capacity scheduler is closed")
            lease_id = f"quarantine_{uuid.uuid4().hex}"
            self._leases[lease_id] = (tool_kind, "quarantined")
            return CapacityLease(self, tool_kind, lease_id)

    def recover_existing(
        self,
        tool_kind: str,
        lease_id: str | None,
    ) -> CapacityLease:
        """Recover a known lease handle or quarantine an untracked runtime."""

        if tool_kind not in self._capacities:
            raise ValueError(f"unsupported interactive tool kind: {tool_kind!r}")
        with self._cond:
            if self._closed:
                raise SchedulerClosed("interactive capacity scheduler is closed")
            if lease_id is not None:
                current = self._leases.get(lease_id)
                if current is not None:
                    if current[0] != tool_kind:
                        raise RuntimeError(
                            "interactive capacity lease tool kind mismatch"
                        )
                    return CapacityLease(self, tool_kind, lease_id)
            recovered_id = lease_id or f"quarantine_{uuid.uuid4().hex}"
            self._leases[recovered_id] = (tool_kind, "quarantined")
            return CapacityLease(self, tool_kind, recovered_id)

    def release(self, lease: CapacityLease) -> None:
        with self._cond:
            current = self._leases.get(lease.lease_id)
            if current is None:
                return
            if current[0] != lease.tool_kind:
                raise RuntimeError("interactive capacity lease tool kind mismatch")
            del self._leases[lease.lease_id]
            self._cond.notify_all()

    def snapshot(self) -> dict[str, CapacitySnapshot]:
        with self._cond:
            result: dict[str, CapacitySnapshot] = {}
            for kind, capacity in self._capacities.items():
                states = [state for lease_kind, state in self._leases.values() if lease_kind == kind]
                result[kind] = CapacitySnapshot(
                    capacity=capacity,
                    starting=sum(state == "starting" for state in states),
                    active=sum(state in {"active", "quarantined"} for state in states),
                    waiting=self._waiting[kind],
                )
            return result

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()
