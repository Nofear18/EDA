"""Bounded subprocess output framing and sinks for sandbox executions.

The EDA interpreters are long lived, so EOF cannot delimit one command from the
next.  :class:`ProtocolDecoder` removes per-execution control records before
passing raw bytes to an :class:`OutputSink`.  It intentionally retains only a
small possible-control-record suffix; a very long line without a newline is
otherwise forwarded immediately.
"""

from __future__ import annotations

import io
import os
import stat
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import asdict, dataclass
from typing import Any, Literal

from science_eda.sandbox.types import ExecutionResult

OutputStream = Literal["stdout", "stderr"]
_STREAMS: tuple[OutputStream, OutputStream] = ("stdout", "stderr")
_CONTROL_PREFIX = b"__SCIENCE_EDA_CONTROL__:"


@dataclass(frozen=True)
class ExecutionProtocol:
    """Unpredictable control-record namespace for exactly one execution."""

    token: str

    @classmethod
    def create(cls) -> "ExecutionProtocol":
        return cls(uuid.uuid4().hex)

    @property
    def record_prefix(self) -> bytes:
        return _CONTROL_PREFIX + self.token.encode("ascii") + b":"

    def record(self, kind: str, value: str | int | None = None) -> str:
        suffix = kind if value is None else f"{kind}:{value}"
        return f"{_CONTROL_PREFIX.decode('ascii')}{self.token}:{suffix}"

    @property
    def status_prefix(self) -> str:
        return self.record("STATUS", "")

    @property
    def auxiliary_status_prefix(self) -> str:
        return self.record("AUX", "")

    @property
    def intermediate_record(self) -> str:
        return self.record("MID")

    def start_record(self, stream: OutputStream) -> str:
        return self.record("START", stream)

    def fence_record(self, stream: OutputStream) -> str:
        return self.record("FENCE", stream)


@dataclass(frozen=True)
class OutputSnapshot:
    output_preview: str
    observed_bytes: int
    written_bytes: int
    dropped_bytes: int | None
    output_lines: int
    output_truncated: bool
    full_log_complete: bool
    incomplete_reason: str | None


@dataclass(frozen=True)
class ExecutionSummary:
    """Bounded terminal result returned by an interactive worker."""

    execution_id: str
    exit_code: int
    duration: float
    output_preview: str
    output_bytes: int
    output_lines: int
    output_truncated: bool
    observed_bytes: int
    written_bytes: int
    dropped_bytes: int | None
    full_log_complete: bool
    incomplete_reason: str | None
    status_received: bool
    stdout_fenced: bool
    stderr_fenced: bool
    termination_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class OutputSink(ABC):
    """Receives filtered raw output bytes in collector observation order."""

    @abstractmethod
    def write(self, stream: OutputStream, data: bytes) -> None:
        raise NotImplementedError

    @abstractmethod
    def finish(
        self,
        *,
        protocol_complete: bool,
        incomplete_reason: str | None = None,
    ) -> OutputSnapshot:
        raise NotImplementedError

    def abort(self) -> None:
        """Close resources for a prepared execution that was never run."""


class _OutputAccounting:
    def __init__(self) -> None:
        self.observed_bytes = 0
        self._newlines = 0
        self._stream_seen = {stream: False for stream in _STREAMS}
        self._stream_ends_newline = {stream: True for stream in _STREAMS}

    def observe(self, stream: OutputStream, data: bytes) -> None:
        if not data:
            return
        self.observed_bytes += len(data)
        self._newlines += data.count(b"\n")
        self._stream_seen[stream] = True
        self._stream_ends_newline[stream] = data.endswith(b"\n")

    @property
    def output_lines(self) -> int:
        unterminated = sum(
            self._stream_seen[stream] and not self._stream_ends_newline[stream]
            for stream in _STREAMS
        )
        return self._newlines + unterminated


class MemoryOutputSink(OutputSink):
    """Unbounded compatibility sink used only by the legacy batch API."""

    def __init__(self, encoding: str = "utf-8") -> None:
        self.encoding = encoding
        self._buffers: dict[OutputStream, io.BytesIO] = {
            "stdout": io.BytesIO(),
            "stderr": io.BytesIO(),
        }
        self._accounting = _OutputAccounting()
        self._finished: OutputSnapshot | None = None

    def write(self, stream: OutputStream, data: bytes) -> None:
        _validate_stream(stream)
        if self._finished is not None:
            raise RuntimeError("output sink is already finished")
        if not data:
            return
        self._accounting.observe(stream, data)
        self._buffers[stream].write(data)

    def _decode(self, stream: OutputStream) -> str:
        value = self._buffers[stream].getvalue().decode(
            self.encoding,
            errors="replace",
        )
        # Popen(text=True) used universal-newline translation.  Preserve that
        # detail of the pre-framing ExecutionResult contract.
        return value.replace("\r\n", "\n").replace("\r", "\n")

    @property
    def stdout(self) -> str:
        return self._decode("stdout")

    @property
    def stderr(self) -> str:
        return self._decode("stderr")

    def finish(
        self,
        *,
        protocol_complete: bool,
        incomplete_reason: str | None = None,
    ) -> OutputSnapshot:
        if self._finished is None:
            observed = self._accounting.observed_bytes
            combined = self.stdout + self.stderr
            self._finished = OutputSnapshot(
                output_preview=combined,
                observed_bytes=observed,
                written_bytes=observed,
                dropped_bytes=0 if incomplete_reason is None else None,
                output_lines=self._accounting.output_lines,
                output_truncated=False,
                full_log_complete=protocol_complete and incomplete_reason is None,
                incomplete_reason=incomplete_reason,
            )
        return self._finished

    def to_execution_result(
        self,
        *,
        exit_code: int,
        duration: float,
        metadata: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        return ExecutionResult(
            self.stdout,
            self.stderr,
            exit_code,
            duration,
            metadata=dict(metadata or {}),
        )


class _PreviewAccumulator:
    """Fixed-memory merged head/tail/diagnostic preview."""

    _DIAGNOSTIC_WORDS = (b"error", b"warning", b"warn", b"fatal")

    def __init__(self, budget: int, encoding: str) -> None:
        self.budget = max(0, int(budget))
        self.encoding = encoding
        self._all: bytearray | None = bytearray()
        self._head_cap = (self.budget * 2) // 5
        self._tail_cap = (self.budget * 2) // 5
        self._diagnostic_cap = self.budget - self._head_cap - self._tail_cap
        self._head = bytearray()
        self._tail = bytearray()
        self._diagnostics: deque[bytes] = deque()
        self._diagnostic_bytes = 0
        self._line_cap = max(128, min(4096, self.budget or 128))
        self._line = bytearray()
        self._line_overflow = False
        self._text_truncated = False
        self.total = 0

    def add(self, data: bytes) -> None:
        if not data:
            return
        self.total += len(data)
        if self._all is not None:
            if len(self._all) + len(data) <= self.budget:
                self._all.extend(data)
            else:
                self._all = None

        head_remaining = self._head_cap - len(self._head)
        if head_remaining > 0:
            self._head.extend(data[:head_remaining])

        if self._tail_cap:
            if len(data) >= self._tail_cap:
                self._tail[:] = data[-self._tail_cap :]
            else:
                self._tail.extend(data)
                overflow = len(self._tail) - self._tail_cap
                if overflow > 0:
                    del self._tail[:overflow]

        self._scan_diagnostics(data)

    def _scan_diagnostics(self, data: bytes) -> None:
        start = 0
        while start < len(data):
            newline = data.find(b"\n", start)
            end = len(data) if newline < 0 else newline + 1
            piece = data[start:end]
            remaining = self._line_cap - len(self._line)
            if remaining > 0:
                self._line.extend(piece[:remaining])
            if len(piece) > remaining:
                self._line_overflow = True
            if newline >= 0:
                self._capture_diagnostic_line()
                self._line.clear()
                self._line_overflow = False
            start = end

    def _capture_diagnostic_line(self) -> None:
        if self._diagnostic_cap <= 0:
            return
        lowered = bytes(self._line).lower()
        if not any(word in lowered for word in self._DIAGNOSTIC_WORDS):
            return
        line = bytes(self._line[: self._diagnostic_cap])
        if self._line_overflow and line:
            line = line[: max(0, self._diagnostic_cap - 4)] + b"...\n"
        self._diagnostics.append(line)
        self._diagnostic_bytes += len(line)
        while self._diagnostic_bytes > self._diagnostic_cap and self._diagnostics:
            removed = self._diagnostics.popleft()
            self._diagnostic_bytes -= len(removed)

    def bytes_value(self) -> bytes:
        if self._all is not None:
            return bytes(self._all)
        if not self.budget:
            return b""
        diagnostics = b"".join(self._diagnostics)
        sections = [bytes(self._head)]
        if diagnostics:
            sections.extend((b"\n[diagnostics]\n", diagnostics))
        if self._tail:
            sections.extend((b"\n[tail]\n", bytes(self._tail)))
        value = b"".join(sections)
        return value[: self.budget]

    def text_value(self) -> str:
        value = self.bytes_value().decode(self.encoding, errors="replace")
        # Replacement characters can expand a malformed one-byte sequence to
        # three UTF-8 bytes.  Bound the actual string representation returned
        # through IPC/JSON, not only the raw accumulator.
        encoded = value.encode("utf-8")
        if len(encoded) <= self.budget:
            return value
        self._text_truncated = True
        return encoded[: self.budget].decode("utf-8", errors="ignore")

    @property
    def truncated(self) -> bool:
        return self.total > self.budget or self._text_truncated


class BoundedMemoryOutputSink(OutputSink):
    """Keep bounded per-stream diagnostics for internal tool commands.

    Interactive startup, healthcheck, baseline, and cwd probes do not expose a
    batch output contract.  They still need enough stdout/stderr for validation
    and diagnostics without allowing a noisy tool to create an unbounded IPC
    object before the first user execution.
    """

    def __init__(self, encoding: str = "utf-8", *, budget_bytes: int) -> None:
        if budget_bytes <= 0:
            raise ValueError("bounded memory output budget must be positive")
        self.encoding = encoding
        stdout_budget = (int(budget_bytes) + 1) // 2
        stderr_budget = int(budget_bytes) - stdout_budget
        self._previews = {
            "stdout": _PreviewAccumulator(stdout_budget, encoding),
            "stderr": _PreviewAccumulator(stderr_budget, encoding),
        }
        self._accounting = _OutputAccounting()
        self._finished: OutputSnapshot | None = None

    def write(self, stream: OutputStream, data: bytes) -> None:
        _validate_stream(stream)
        if self._finished is not None:
            raise RuntimeError("output sink is already finished")
        if not data:
            return
        raw = bytes(data)
        self._accounting.observe(stream, raw)
        self._previews[stream].add(raw)

    @property
    def stdout(self) -> str:
        return self._previews["stdout"].text_value()

    @property
    def stderr(self) -> str:
        return self._previews["stderr"].text_value()

    def finish(
        self,
        *,
        protocol_complete: bool,
        incomplete_reason: str | None = None,
    ) -> OutputSnapshot:
        if self._finished is None:
            preview = self.stdout + self.stderr
            observed = self._accounting.observed_bytes
            self._finished = OutputSnapshot(
                output_preview=preview,
                observed_bytes=observed,
                written_bytes=observed,
                dropped_bytes=None,
                output_lines=self._accounting.output_lines,
                output_truncated=any(
                    accumulator.truncated
                    for accumulator in self._previews.values()
                ),
                full_log_complete=(
                    protocol_complete and incomplete_reason is None
                ),
                incomplete_reason=incomplete_reason,
            )
        return self._finished

    def to_execution_result(
        self,
        *,
        exit_code: int,
        duration: float,
        metadata: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        merged = dict(metadata or {})
        merged["output_truncated"] = any(
            accumulator.truncated for accumulator in self._previews.values()
        )
        return ExecutionResult(
            self.stdout,
            self.stderr,
            exit_code,
            duration,
            metadata=merged,
        )


class FilePreviewOutputSink(OutputSink):
    """Append raw merged bytes to a log while retaining bounded diagnostics."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        preview_bytes: int,
        write_budget_bytes: int,
        encoding: str = "utf-8",
    ) -> None:
        if preview_bytes < 0:
            raise ValueError("preview_bytes must be >= 0")
        if write_budget_bytes < 0:
            raise ValueError("write_budget_bytes must be >= 0")
        self.path = os.fspath(path)
        self.encoding = encoding
        # The daemon owns creation and permissions.  Refuse creation and
        # symlink traversal so a same-user path swap cannot redirect worker
        # output into an unrelated file between pre-creation and preparation.
        path_metadata = os.lstat(self.path)
        if stat.S_ISLNK(path_metadata.st_mode):
            raise OSError("interactive output log must not be a symlink")
        flags = (
            os.O_RDWR
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(self.path, flags)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise OSError("interactive output log is not a regular file")
            if (metadata.st_dev, metadata.st_ino) != (
                path_metadata.st_dev,
                path_metadata.st_ino,
            ):
                raise OSError("interactive output log changed while it was opened")
            if metadata.st_nlink != 1:
                raise OSError("interactive output log must not have hard links")
            get_effective_uid = getattr(os, "geteuid", None)
            if (
                get_effective_uid is not None
                and metadata.st_uid != get_effective_uid()
            ):
                raise PermissionError(
                    "interactive output log is not owned by the worker user"
                )
            self._file = os.fdopen(descriptor, "r+b", buffering=0)
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            raise
        self._file.seek(0, os.SEEK_END)
        self._preview = _PreviewAccumulator(preview_bytes, encoding)
        self._write_budget = int(write_budget_bytes)
        self._accounting = _OutputAccounting()
        self._written_bytes = 0
        self._sink_reason: str | None = None
        self._finished: OutputSnapshot | None = None

    def write(self, stream: OutputStream, data: bytes) -> None:
        _validate_stream(stream)
        if self._finished is not None:
            raise RuntimeError("output sink is already finished")
        if not data:
            return
        raw = bytes(data)
        self._accounting.observe(stream, raw)
        self._preview.add(raw)
        if self._sink_reason in {"WRITE_FAILED", "QUOTA_EXCEEDED"}:
            return

        remaining = max(0, self._write_budget - self._written_bytes)
        to_write = raw[:remaining]
        if not to_write:
            self._sink_reason = "QUOTA_EXCEEDED"
            return
        try:
            view = memoryview(to_write)
            accepted = 0
            while accepted < len(view):
                count = self._file.write(view[accepted:])
                if not isinstance(count, int) or count <= 0:
                    raise OSError("output log write made no progress")
                accepted += count
                # The sink is unbuffered, so each successful write is already
                # accepted by the OS.  Preserve that exact count even if a
                # later partial write or flush fails.
                self._written_bytes += count
            self._file.flush()
        except (OSError, ValueError):
            self._sink_reason = "WRITE_FAILED"
            return
        if len(to_write) < len(raw):
            self._sink_reason = "QUOTA_EXCEEDED"

    def finish(
        self,
        *,
        protocol_complete: bool,
        incomplete_reason: str | None = None,
    ) -> OutputSnapshot:
        if self._finished is not None:
            return self._finished

        close_failed = False
        try:
            self._file.flush()
        except (OSError, ValueError):
            close_failed = True
        try:
            self._file.close()
        except OSError:
            close_failed = True
        if close_failed:
            self._sink_reason = "WRITE_FAILED"

        # A missing process/protocol terminus means the collector cannot know
        # how many bytes were never observed.  Preserve that stronger outcome
        # even if the local file quota had already been exhausted.
        reason = incomplete_reason or self._sink_reason
        observed = self._accounting.observed_bytes
        if reason == "WRITE_FAILED" or incomplete_reason is not None:
            dropped: int | None = None
        else:
            dropped = max(0, observed - self._written_bytes)
        self._finished = OutputSnapshot(
            output_preview=self._preview.text_value(),
            observed_bytes=observed,
            written_bytes=self._written_bytes,
            dropped_bytes=dropped,
            output_lines=self._accounting.output_lines,
            output_truncated=self._preview.truncated,
            full_log_complete=protocol_complete and reason is None,
            incomplete_reason=reason,
        )
        return self._finished

    def abort(self) -> None:
        if self._finished is not None:
            return
        try:
            self._file.close()
        except OSError:
            pass


class ProtocolDecoder:
    """Strip bounded control records independently from stdout and stderr."""

    def __init__(
        self,
        protocol: ExecutionProtocol,
        sink: OutputSink,
        *,
        max_control_record_bytes: int = 512,
    ) -> None:
        if max_control_record_bytes < len(protocol.record_prefix) + 16:
            raise ValueError("max_control_record_bytes is too small")
        self.protocol = protocol
        self.sink = sink
        self.max_control_record_bytes = int(max_control_record_bytes)
        self._buffers = {stream: bytearray() for stream in _STREAMS}
        # A per-stream start fence separates output that arrived after the
        # previous execution's terminal fence from output caused by this one.
        # Without it, queued tool chatter can be attributed to the next result.
        self._started = {stream: False for stream in _STREAMS}
        self._fenced = {stream: False for stream in _STREAMS}
        self.status_code: int | None = None

    def feed(self, stream: OutputStream, data: bytes) -> None:
        _validate_stream(stream)
        if not data or self._fenced[stream]:
            return
        self._buffers[stream].extend(data)
        self._drain(stream)

    def _drain(self, stream: OutputStream) -> None:
        buf = self._buffers[stream]
        prefix = self.protocol.record_prefix
        while buf and not self._fenced[stream]:
            index = buf.find(prefix)
            if index < 0:
                keep = min(len(buf), max(0, len(prefix) - 1))
                emit = len(buf) - keep
                if emit:
                    self._emit_user_bytes(stream, bytes(buf[:emit]))
                    del buf[:emit]
                return
            if index:
                self._emit_user_bytes(stream, bytes(buf[:index]))
                del buf[:index]
            newline = buf.find(b"\n")
            if newline < 0:
                if len(buf) <= self.max_control_record_bytes:
                    return
                # A matching prefix followed by an overlong non-record is user
                # output.  Forward enough at once to restore bounded memory.
                emit = len(buf) - len(prefix) + 1
                self._emit_user_bytes(stream, bytes(buf[:emit]))
                del buf[:emit]
                continue
            raw_record = bytes(buf[:newline]).rstrip(b"\r")
            del buf[: newline + 1]
            if not self._consume_record(stream, raw_record):
                self._emit_user_bytes(stream, raw_record + b"\n")
            if self._fenced[stream]:
                # Anything after a same-stream fence belongs to asynchronous
                # tool chatter, not this execution.
                buf.clear()

    def _emit_user_bytes(self, stream: OutputStream, data: bytes) -> None:
        if self._started[stream]:
            self.sink.write(stream, data)

    def _consume_record(self, stream: OutputStream, record: bytes) -> bool:
        prefix = self.protocol.record_prefix
        if not record.startswith(prefix):
            return False
        body = record[len(prefix) :]
        expected_start = f"START:{stream}".encode("ascii")
        if body == expected_start:
            self._started[stream] = True
            return True
        if not self._started[stream]:
            # Before this execution's start fence all bytes on this stream are
            # delayed output from an earlier command.  Current-token control
            # records are internal even if malformed or unexpectedly ordered.
            return True
        if body.startswith(b"STATUS:"):
            raw = body[len(b"STATUS:") :]
            try:
                status = int(raw.decode("ascii"))
            except (UnicodeDecodeError, ValueError):
                return False
            if self.status_code is None:
                self.status_code = status
            return True
        if body.startswith(b"AUX:"):
            raw = body[len(b"AUX:") :]
            try:
                int(raw.decode("ascii"))
            except (UnicodeDecodeError, ValueError):
                return False
            return True
        if body == b"MID":
            return True
        expected_fence = f"FENCE:{stream}".encode("ascii")
        if body == expected_fence:
            self._fenced[stream] = True
            return True
        return False

    def finish_stream(self, stream: OutputStream) -> None:
        """Flush non-control remainder after a pipe reaches EOF."""

        _validate_stream(stream)
        if self._fenced[stream]:
            self._buffers[stream].clear()
            return
        self._drain(stream)
        remainder = bytes(self._buffers[stream])
        self._buffers[stream].clear()
        if not self._started[stream]:
            return
        # A partial record with this execution's unpredictable prefix is an
        # internal record interrupted by process death and must not leak.
        if remainder.startswith(self.protocol.record_prefix):
            return
        self.sink.write(stream, remainder)

    @property
    def stdout_fenced(self) -> bool:
        return self._fenced["stdout"]

    @property
    def stderr_fenced(self) -> bool:
        return self._fenced["stderr"]

    @property
    def complete(self) -> bool:
        return self.stdout_fenced and self.stderr_fenced


def build_execution_summary(
    *,
    execution_id: str,
    exit_code: int,
    duration: float,
    decoder: ProtocolDecoder,
    snapshot: OutputSnapshot,
    termination_reason: str | None = None,
) -> ExecutionSummary:
    return ExecutionSummary(
        execution_id=execution_id,
        exit_code=exit_code,
        duration=duration,
        output_preview=snapshot.output_preview,
        output_bytes=snapshot.observed_bytes,
        output_lines=snapshot.output_lines,
        output_truncated=snapshot.output_truncated,
        observed_bytes=snapshot.observed_bytes,
        written_bytes=snapshot.written_bytes,
        dropped_bytes=snapshot.dropped_bytes,
        full_log_complete=snapshot.full_log_complete,
        incomplete_reason=snapshot.incomplete_reason,
        status_received=decoder.status_code is not None,
        stdout_fenced=decoder.stdout_fenced,
        stderr_fenced=decoder.stderr_fenced,
        termination_reason=termination_reason,
    )


def _validate_stream(stream: str) -> None:
    if stream not in _STREAMS:
        raise ValueError(f"unknown output stream: {stream!r}")
