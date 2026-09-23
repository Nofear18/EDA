"""Bounded recovery of a partial interactive execution log.

The worker normally returns an :class:`ExecutionSummary`.  If its control
channel disappears first, the daemon still owns a bounded append-only log and
can expose useful diagnostics without loading the whole file into memory.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from science_eda.sandbox.interactive.service_support import bounded_message


_OMITTED_MARKER = b"\n... output omitted ...\n"


@dataclass(frozen=True)
class PartialLogSnapshot:
    """The trustworthy subset recoverable from an incomplete local log."""

    output_preview: str
    returned_bytes: int
    output_bytes: int
    output_lines: int
    output_truncated: bool
    written_bytes: int


def read_partial_log(
    path: str | Path,
    *,
    preview_bytes: int,
    reader_chunk_bytes: int,
    max_log_bytes: int,
    encoding: str = "utf-8",
) -> PartialLogSnapshot:
    """Read a daemon-owned partial log with fixed memory and byte budgets.

    ``max_log_bytes`` is the durable reservation sent to the worker.  It also
    prevents a same-user replacement of the local data-plane file from making
    recovery scan an unbounded amount of data.  Under normal ownership the
    file can never exceed this value.
    """

    preview_budget = max(0, int(preview_bytes))
    chunk_size = max(1, int(reader_chunk_bytes))
    scan_budget = max(0, int(max_log_bytes))
    head = bytearray()
    tail = bytearray()
    total = 0
    newline_count = 0
    last_byte: int | None = None

    with Path(path).open("rb", buffering=0) as stream:
        while total < scan_budget:
            data = stream.read(min(chunk_size, scan_budget - total))
            if not data:
                break
            total += len(data)
            newline_count += data.count(b"\n")
            last_byte = data[-1]

            if len(head) < preview_budget:
                head.extend(data[: preview_budget - len(head)])
            if preview_budget:
                if len(data) >= preview_budget:
                    tail[:] = data[-preview_budget:]
                else:
                    tail.extend(data)
                    overflow = len(tail) - preview_budget
                    if overflow > 0:
                        del tail[:overflow]

        # A conforming worker never writes past the reservation.  Treat an
        # extra byte as truncation, but do not scan attacker-controlled data.
        exceeds_scan_budget = bool(scan_budget and stream.read(1))

    truncated = exceeds_scan_budget or total > preview_budget
    if total <= preview_budget and not exceeds_scan_budget:
        preview_raw = bytes(head[:total])
    elif preview_budget == 0:
        preview_raw = b""
    else:
        marker = _OMITTED_MARKER[:preview_budget]
        payload_budget = preview_budget - len(marker)
        head_bytes = payload_budget // 2
        tail_bytes = payload_budget - head_bytes
        preview_raw = (
            bytes(head[:head_bytes])
            + marker
            + (bytes(tail[-tail_bytes:]) if tail_bytes else b"")
        )

    decoded = preview_raw.decode(encoding, errors="replace")
    replacement_expanded_past_budget = (
        len(decoded.encode("utf-8")) > preview_budget
    )
    preview = bounded_message(decoded, preview_budget)
    return PartialLogSnapshot(
        output_preview=preview,
        returned_bytes=len(preview.encode("utf-8")),
        output_bytes=total,
        output_lines=newline_count + int(total > 0 and last_byte != ord("\n")),
        output_truncated=truncated or replacement_expanded_past_budget,
        written_bytes=total,
    )
