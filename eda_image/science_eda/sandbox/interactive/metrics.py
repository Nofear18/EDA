"""Small dependency-free metrics registry for the local interactive daemon."""

from __future__ import annotations

import threading
from collections import Counter
from dataclasses import dataclass


@dataclass(frozen=True)
class MetricsSnapshot:
    counters: dict[str, int]
    totals: dict[str, float]


class InteractiveMetrics:
    def __init__(self) -> None:
        self._counters: Counter[str] = Counter()
        self._totals: Counter[str] = Counter()
        self._lock = threading.Lock()

    def increment(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] += int(value)

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            self._counters[f"{name}.count"] += 1
            self._totals[f"{name}.total"] += float(value)

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return MetricsSnapshot(dict(self._counters), dict(self._totals))
