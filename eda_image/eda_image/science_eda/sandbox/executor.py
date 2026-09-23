"""Per-language executors (run inside the sandbox worker process only)."""

from __future__ import annotations

import io
import os
import queue
import shlex
import signal
import subprocess
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from science_eda.sandbox.output import (
    BoundedMemoryOutputSink,
    ExecutionProtocol,
    ExecutionSummary,
    MemoryOutputSink,
    OutputSink,
    OutputStream,
    ProtocolDecoder,
    build_execution_summary,
)
from science_eda.sandbox.tcl_protocol import (
    prepare_tcl_source_payload,
    remove_tcl_source_files,
    tcl_double_quoted_word,
)
from science_eda.sandbox.types import ExecutionResult

if TYPE_CHECKING:
    pass


class BaseExecutor(ABC):
    @abstractmethod
    def execute(self, code: str, timeout: float) -> ExecutionResult:
        raise NotImplementedError

    def close(self) -> None:
        pass

    def execute_to_sink(
        self,
        code: str,
        timeout: float,
        sink: OutputSink,
        execution_id: str,
    ) -> ExecutionSummary:
        """Execute into a prepared sink.

        Interactive runtimes are currently limited to the long-lived Tcl tool
        executors.  Keeping this method on the low-level interface gives the
        worker one stable call site without changing the legacy execute API.
        """

        raise NotImplementedError(
            f"{type(self).__name__} does not support prepared output sinks"
        )


class PythonExecutor(BaseExecutor):
    def __init__(self, working_dir: str) -> None:
        self._local_ns: dict[str, object] = {"__name__": "__sandbox__"}
        self._working_dir = working_dir

    def execute(self, code: str, timeout: float) -> ExecutionResult:
        import os

        start = time.perf_counter()
        prev = os.getcwd()
        os.chdir(self._working_dir)
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        exit_code = 0
        try:
            from contextlib import redirect_stderr, redirect_stdout

            with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                exec(compile(code, "<sandbox>", "exec"), self._local_ns, self._local_ns)
        except BaseException as e:  # noqa: BLE001 — user code errors surface as stderr
            stderr_buf.write(f"{type(e).__name__}: {e}\n")
            exit_code = 1
        finally:
            os.chdir(prev)
        duration = time.perf_counter() - start
        if duration > timeout:
            return ExecutionResult(
                stdout_buf.getvalue(),
                stderr_buf.getvalue() + f"\n[sandbox] exceeded timeout {timeout}s\n",
                1,
                duration,
            )
        return ExecutionResult(stdout_buf.getvalue(), stderr_buf.getvalue(), exit_code, duration)


class _PopenExecutor(BaseExecutor):
    """Long-lived interpreter with bounded fixed-size binary pipe readers."""

    def __init__(
        self,
        argv: list[str],
        working_dir: str,
        *,
        read_chunk_bytes: int = 64 * 1024,
        encoding: str = "utf-8",
    ) -> None:
        self._working_dir = working_dir
        self._encoding = encoding
        self._read_chunk_bytes = max(1, int(read_chunk_bytes))
        self._proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=working_dir,
            text=False,
            bufsize=0,
            start_new_session=True,
        )
        # start_new_session makes the interpreter PID its process-group ID.
        # Keep it after the leader exits so inherited pipe holders can still be
        # terminated before stdout/stderr are drained to EOF.
        self._process_group_id = self._proc.pid
        # Reader memory is independent of command output size.  Blocking a
        # reader briefly on this small queue is preferable to an unbounded IPC
        # accumulator; the execution collector continuously drains it.
        self._output_q: queue.Queue[tuple[OutputStream, bytes | None]] = queue.Queue(
            maxsize=32
        )
        self._stream_eof = {"stdout": False, "stderr": False}
        self._stdout_thread = threading.Thread(
            target=self._drain_pipe,
            args=("stdout", self._proc.stdout),
            daemon=True,
        )
        self._stderr_thread = threading.Thread(
            target=self._drain_pipe,
            args=("stderr", self._proc.stderr),
            daemon=True,
        )
        self._stdout_thread.start()
        self._stderr_thread.start()

    def _drain_pipe(self, stream: OutputStream, pipe: object) -> None:
        read = getattr(pipe, "read")
        while True:
            chunk = read(self._read_chunk_bytes)
            if not chunk:
                self._output_q.put((stream, None))
                break
            self._output_q.put((stream, bytes(chunk)))

    def _write_stdin(self, block: str) -> None:
        assert self._proc.stdin is not None
        view = memoryview(block.encode(self._encoding))
        written = 0
        while written < len(view):
            count = self._proc.stdin.write(view[written:])
            if not isinstance(count, int) or count <= 0:
                raise BrokenPipeError("interpreter stdin write made no progress")
            written += count
        self._proc.stdin.flush()

    def _consume_output_event(
        self,
        decoder: ProtocolDecoder,
        event: tuple[OutputStream, bytes | None],
    ) -> None:
        stream, chunk = event
        if chunk is None:
            self._stream_eof[stream] = True
            decoder.finish_stream(stream)
        else:
            decoder.feed(stream, chunk)

    def _drain_after_termination(
        self,
        decoder: ProtocolDecoder,
        *,
        timeout: float = 5.0,
    ) -> bool:
        deadline = time.monotonic() + max(0.1, timeout)
        while not all(self._stream_eof.values()) and time.monotonic() < deadline:
            try:
                event = self._output_q.get(
                    timeout=min(0.05, max(0.001, deadline - time.monotonic()))
                )
            except queue.Empty:
                continue
            self._consume_output_event(decoder, event)
        for stream in ("stdout", "stderr"):
            if self._stream_eof[stream]:
                decoder.finish_stream(stream)  # type: ignore[arg-type]
        return all(self._stream_eof.values())

    def _execute_protocol_block(
        self,
        block: str,
        timeout: float,
        *,
        sink: OutputSink,
        execution_id: str,
        protocol: ExecutionProtocol,
        stdin_broken_message: str,
    ) -> ExecutionSummary:
        start = time.perf_counter()
        decoder = ProtocolDecoder(protocol, sink)
        termination_reason: str | None = None
        try:
            self._write_stdin(block)
        except (BrokenPipeError, OSError):
            sink.write("stderr", stdin_broken_message.encode(self._encoding))
            termination_reason = "PROCESS_LOST"
        else:
            deadline = time.monotonic() + float(timeout)
            while not decoder.complete:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    termination_reason = "TIMED_OUT"
                    self._kill_interpreter()
                    try:
                        self._proc.wait(timeout=2.0)
                    except subprocess.TimeoutExpired:
                        pass
                    self._drain_after_termination(decoder)
                    message = f"[sandbox] exceeded timeout {timeout}s\n"
                    sink.write("stderr", message.encode(self._encoding))
                    break
                try:
                    event = self._output_q.get(timeout=min(0.05, max(0.001, remaining)))
                except queue.Empty:
                    if self._proc.poll() is not None:
                        termination_reason = "PROCESS_LOST"
                        # Descendants may keep both pipes open after the leader
                        # exits.  Kill the saved process group, then drain it,
                        # instead of waiting until the command deadline.
                        self._kill_interpreter()
                        self._drain_after_termination(decoder)
                        break
                    continue
                self._consume_output_event(decoder, event)
                if all(self._stream_eof.values()) and not decoder.complete:
                    termination_reason = "PROCESS_LOST"
                    break

        duration = time.perf_counter() - start
        if termination_reason == "TIMED_OUT":
            # A forced process exit followed by both pipe EOFs still gives a
            # complete byte log even though no trustworthy Tcl status exists.
            protocol_complete = all(self._stream_eof.values())
            incomplete_reason = None if protocol_complete else "PROCESS_LOST"
        elif termination_reason is not None:
            protocol_complete = False
            incomplete_reason = "PROCESS_LOST"
        elif decoder.status_code is None:
            protocol_complete = False
            incomplete_reason = "PROTOCOL_ERROR"
            termination_reason = "PROTOCOL_ERROR"
        else:
            protocol_complete = decoder.complete
            incomplete_reason = None

        snapshot = sink.finish(
            protocol_complete=protocol_complete,
            incomplete_reason=incomplete_reason,
        )
        exit_code = (
            decoder.status_code
            if decoder.status_code is not None and termination_reason is None
            else -1
        )
        return build_execution_summary(
            execution_id=execution_id,
            exit_code=exit_code,
            duration=duration,
            decoder=decoder,
            snapshot=snapshot,
            termination_reason=termination_reason,
        )

    def close(self) -> None:
        self._signal_process_group(signal.SIGTERM)
        if self._proc.poll() is None:
            try:
                self._proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
        if self._proc.poll() is None or any(
            t.is_alive() for t in (self._stdout_thread, self._stderr_thread)
        ):
            self._signal_process_group(signal.SIGKILL)
            if self._proc.poll() is None:
                try:
                    self._proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
        # Let the fixed-size readers publish EOF instead of leaving them
        # blocked forever on a full queue during normal shutdown.
        deadline = time.monotonic() + 1.0
        while any(t.is_alive() for t in (self._stdout_thread, self._stderr_thread)):
            if time.monotonic() >= deadline:
                break
            try:
                self._output_q.get(timeout=0.02)
            except queue.Empty:
                pass
        for t in (self._stdout_thread, self._stderr_thread):
            t.join(timeout=1)

    def _signal_process_group(self, sig: int) -> None:
        try:
            if hasattr(os, "killpg"):
                os.killpg(self._process_group_id, sig)
            elif self._proc.poll() is None:
                if sig == signal.SIGTERM:
                    self._proc.terminate()
                else:
                    self._proc.kill()
        except (ProcessLookupError, PermissionError, OSError):
            pass

    def _kill_interpreter(self) -> None:
        self._signal_process_group(signal.SIGKILL)

    def _execute_tcl_source_payload_to_sink(
        self,
        code: str,
        timeout: float,
        *,
        sink: OutputSink,
        execution_id: str,
        stdin_broken_message: str,
        scratch_dir: str,
        file_prefix: str,
    ) -> ExecutionSummary:
        protocol = ExecutionProtocol.create()
        block, paths = prepare_tcl_source_payload(
            code,
            protocol,
            scratch_dir=scratch_dir,
            file_prefix=file_prefix,
        )
        try:
            return self._execute_protocol_block(
                block,
                timeout,
                sink=sink,
                execution_id=execution_id,
                protocol=protocol,
                stdin_broken_message=stdin_broken_message,
            )
        finally:
            remove_tcl_source_files(paths)

    def _execute_tcl_catch_block(
        self,
        code: str,
        timeout: float,
        *,
        stdin_broken_message: str,
    ) -> ExecutionResult:
        sink = MemoryOutputSink(self._encoding)
        summary = self._execute_tcl_catch_block_to_sink(
            code,
            timeout,
            sink=sink,
            execution_id=uuid.uuid4().hex,
            stdin_broken_message=stdin_broken_message,
        )
        return sink.to_execution_result(
            exit_code=summary.exit_code,
            duration=summary.duration,
            metadata=_execution_summary_metadata(summary),
        )

    def _execute_tcl_catch_block_to_sink(
        self,
        code: str,
        timeout: float,
        *,
        sink: OutputSink,
        execution_id: str,
        stdin_broken_message: str,
    ) -> ExecutionSummary:
        return self._execute_tcl_source_payload_to_sink(
            code,
            timeout,
            sink=sink,
            execution_id=execution_id,
            stdin_broken_message=stdin_broken_message,
            scratch_dir=self._working_dir,
            file_prefix="tclsh",
        )


class ShellExecutor(_PopenExecutor):
    def __init__(
        self,
        working_dir: str,
        shell_path: str = "/bin/bash",
        *,
        read_chunk_bytes: int = 64 * 1024,
    ) -> None:
        self._shell_path = shell_path
        super().__init__([shell_path], working_dir, read_chunk_bytes=read_chunk_bytes)

    def execute(self, code: str, timeout: float) -> ExecutionResult:
        sink = MemoryOutputSink(self._encoding)
        summary = self.execute_to_sink(
            code,
            timeout,
            sink,
            uuid.uuid4().hex,
        )
        return sink.to_execution_result(
            exit_code=summary.exit_code,
            duration=summary.duration,
            metadata=_execution_summary_metadata(summary),
        )

    def execute_to_sink(
        self,
        code: str,
        timeout: float,
        sink: OutputSink,
        execution_id: str,
    ) -> ExecutionSummary:
        protocol = ExecutionProtocol.create()
        status_prefix = protocol.status_prefix
        stdout_start = protocol.start_record("stdout")
        stderr_start = protocol.start_record("stderr")
        stdout_fence = protocol.fence_record("stdout")
        stderr_fence = protocol.fence_record("stderr")
        # Feed the long-lived bash directly so state (exports, cwd, vars) persists.
        block = (
            f"printf '%s\\n' {shlex.quote(stderr_start)} >&2\n"
            f"printf '%s\\n' {shlex.quote(stdout_start)}\n"
            "__eda_restore_errexit=0\n"
            "case $- in\n"
            "  *e*) __eda_restore_errexit=1; set +e ;;\n"
            "esac\n"
            f"{code}\n"
            "__eda_status=$?\n"
            "if [ \"$__eda_restore_errexit\" -eq 1 ]; then set -e; fi\n"
            f"printf '%s%s\\n' {shlex.quote(status_prefix)} \"$__eda_status\"\n"
            f"printf '%s\\n' {shlex.quote(stderr_fence)} >&2\n"
            f"printf '%s\\n' {shlex.quote(stdout_fence)}\n"
        )
        return self._execute_protocol_block(
            block,
            timeout,
            sink=sink,
            execution_id=execution_id,
            protocol=protocol,
            stdin_broken_message="[sandbox] shell stdin broken\n",
        )


class TCLExecutor(_PopenExecutor):
    def __init__(
        self,
        working_dir: str,
        tcl_shell: str = "tclsh",
        *,
        read_chunk_bytes: int = 64 * 1024,
    ) -> None:
        self._tcl = tcl_shell
        super().__init__([tcl_shell], working_dir, read_chunk_bytes=read_chunk_bytes)

    def execute(self, code: str, timeout: float) -> ExecutionResult:
        return self._execute_tcl_catch_block(
            code,
            timeout,
            stdin_broken_message="[sandbox] tclsh stdin broken\n",
        )

    def execute_to_sink(
        self,
        code: str,
        timeout: float,
        sink: OutputSink,
        execution_id: str,
    ) -> ExecutionSummary:
        return self._execute_tcl_catch_block_to_sink(
            code,
            timeout,
            sink=sink,
            execution_id=execution_id,
            stdin_broken_message="[sandbox] tclsh stdin broken\n",
        )


class TclToolExecutor(_PopenExecutor):
    """Long-lived Tcl-based EDA tool using sourced Tcl wrappers."""

    def __init__(
        self,
        working_dir: str,
        tool_bin: str,
        tool_args: list[str] | None = None,
        *,
        tool_name: str,
        read_chunk_bytes: int = 64 * 1024,
        internal_output_max_bytes: int | None = None,
    ) -> None:
        self._tool_name = tool_name
        self._tool_bin = tool_bin
        self._tool_args = list(tool_args or [])
        # Runtime-owned scratch and user-visible Tcl cwd are deliberately
        # separate.  Wrapper files are always created in scratch, even after a
        # logical session changes the interpreter cwd to a workspace.
        self._runtime_scratch_dir = working_dir
        self._physical_working_dir = working_dir
        self._workspace_cwd: str | None = None
        self._internal_output_max_bytes = (
            None
            if internal_output_max_bytes is None
            else max(1, int(internal_output_max_bytes))
        )
        super().__init__(
            [tool_bin, *self._tool_args],
            working_dir,
            read_chunk_bytes=read_chunk_bytes,
        )

    def _stdin_broken_message(self, action: str = "") -> str:
        suffix = f" during {action}" if action else ""
        return f"[sandbox] {self._tool_name} stdin broken{suffix}\n"

    def capture_baseline(self, startup_timeout: float) -> None:
        res = self._execute_tcl_source_file(
            _INNOVUS_TCL_BASELINE_SCRIPT,
            float(startup_timeout),
            stdin_broken_message=self._stdin_broken_message("baseline capture"),
        )
        if res.exit_code != 0:
            self.close()
            detail = (res.stderr or res.stdout or "unknown baseline failure").strip()
            raise RuntimeError(f"{self._tool_name} baseline capture failed: {detail}")

    def prepare_session(
        self,
        working_dir: str,
        startup_tcl: str,
        startup_timeout: float,
    ) -> None:
        os.makedirs(working_dir, exist_ok=True)
        self.set_workspace_cwd(working_dir, startup_timeout)
        self.startup(startup_tcl, startup_timeout)

    def healthcheck(self, code: str, timeout: float) -> ExecutionResult:
        """Run a tool healthcheck through the same framed transport.

        Interactive creation invokes this while the Tcl cwd is still runtime
        scratch.  Pool release retains its existing explicit cwd restoration
        before calling a healthcheck.
        """

        healthcheck = code.strip()
        if not healthcheck:
            healthcheck = "puts __SCIENCE_EDA_INTERACTIVE_HEALTHCHECK__"
        return self._execute_tcl_source_file(
            healthcheck,
            float(timeout),
            stdin_broken_message=self._stdin_broken_message("healthcheck"),
        )

    def set_workspace_cwd(self, workspace_path: str, timeout: float) -> ExecutionResult:
        canonical = os.path.realpath(workspace_path)
        cd_result = self._execute_tcl_source_file(
            f"cd {tcl_double_quoted_word(canonical)}",
            float(timeout),
            stdin_broken_message=self._stdin_broken_message("cwd switch"),
        )
        if cd_result.exit_code != 0:
            detail = (cd_result.stderr or cd_result.stdout or "cwd switch failed").strip()
            raise RuntimeError(f"{self._tool_name} cwd switch failed: {detail}")
        probe_result = self._execute_tcl_source_file(
            # Real Tcl returns pwd as a command result, while the lightweight
            # fake EDA shells used in compatibility tests print bare `pwd`.
            # Running both gives one canonical path in either implementation.
            "puts [pwd]\npwd",
            float(timeout),
            stdin_broken_message=self._stdin_broken_message("cwd probe"),
        )
        observed = {os.path.realpath(line.strip()) for line in probe_result.stdout.splitlines()}
        if probe_result.exit_code != 0 or canonical not in observed:
            detail = (
                probe_result.stderr or probe_result.stdout or "cwd probe mismatch"
            ).strip()
            raise RuntimeError(f"{self._tool_name} cwd switch failed: {detail}")
        self._workspace_cwd = canonical
        return _combine_execution_results([cd_result, probe_result], 0)

    def reset_session(
        self,
        reset_tcl: str,
        clean_tcl_state: bool,
        timeout: float,
    ) -> ExecutionResult:
        results: list[ExecutionResult] = []
        reset = reset_tcl.strip()
        if reset:
            res = self._execute_tcl_source_file(
                reset,
                float(timeout),
                stdin_broken_message=self._stdin_broken_message("reset"),
            )
            results.append(res)
            if res.exit_code != 0:
                return _combine_execution_results(results, res.exit_code)
        if clean_tcl_state:
            res = self._execute_tcl_source_file(
                "::science_eda_pool::cleanup_tcl_state 1",
                float(timeout),
                stdin_broken_message=self._stdin_broken_message("Tcl cleanup"),
            )
            results.append(res)
            if res.exit_code != 0:
                return _combine_execution_results(results, res.exit_code)
        cwd_res = self._execute_tcl_source_file(
            f"cd {tcl_double_quoted_word(self._physical_working_dir)}",
            float(timeout),
            stdin_broken_message=self._stdin_broken_message("cwd restore"),
        )
        results.append(cwd_res)
        if cwd_res.exit_code != 0:
            return _combine_execution_results(results, cwd_res.exit_code)
        self._workspace_cwd = None
        return _combine_execution_results(results, 0)

    def startup(self, startup_tcl: str, startup_timeout: float) -> None:
        startup = startup_tcl.strip()
        if not startup:
            return
        res = self._execute_tcl_source_file(
            startup,
            float(startup_timeout),
            stdin_broken_message=self._stdin_broken_message("startup"),
        )
        if res.exit_code != 0:
            self.close()
            detail = (res.stderr or res.stdout or "unknown startup failure").strip()
            raise RuntimeError(f"{self._tool_name} startup failed: {detail}")

    def execute(self, code: str, timeout: float) -> ExecutionResult:
        return self._execute_tcl_source_file(
            code,
            timeout,
            stdin_broken_message=self._stdin_broken_message(),
        )

    def execute_to_sink(
        self,
        code: str,
        timeout: float,
        sink: OutputSink,
        execution_id: str,
    ) -> ExecutionSummary:
        return self._execute_tcl_source_file_to_sink(
            code,
            timeout,
            sink=sink,
            execution_id=execution_id,
            stdin_broken_message=self._stdin_broken_message(),
        )

    def _execute_tcl_source_file(
        self,
        code: str,
        timeout: float,
        *,
        stdin_broken_message: str,
    ) -> ExecutionResult:
        sink: MemoryOutputSink | BoundedMemoryOutputSink
        if self._internal_output_max_bytes is None:
            sink = MemoryOutputSink(self._encoding)
        else:
            sink = BoundedMemoryOutputSink(
                self._encoding,
                budget_bytes=self._internal_output_max_bytes,
            )
        summary = self._execute_tcl_source_file_to_sink(
            code,
            timeout,
            sink=sink,
            execution_id=uuid.uuid4().hex,
            stdin_broken_message=stdin_broken_message,
        )
        return sink.to_execution_result(
            exit_code=summary.exit_code,
            duration=summary.duration,
            metadata=_execution_summary_metadata(summary),
        )

    def _execute_tcl_source_file_to_sink(
        self,
        code: str,
        timeout: float,
        *,
        sink: OutputSink,
        execution_id: str,
        stdin_broken_message: str,
    ) -> ExecutionSummary:
        return self._execute_tcl_source_payload_to_sink(
            code,
            timeout,
            sink=sink,
            execution_id=execution_id,
            stdin_broken_message=stdin_broken_message,
            scratch_dir=self._runtime_scratch_dir,
            file_prefix=self._tool_name,
        )


class InnovusExecutor(TclToolExecutor):
    def __init__(
        self,
        working_dir: str,
        innovus_bin: str = "innovus",
        innovus_args: list[str] | None = None,
        *,
        read_chunk_bytes: int = 64 * 1024,
        internal_output_max_bytes: int | None = None,
    ) -> None:
        super().__init__(
            working_dir,
            innovus_bin,
            innovus_args,
            tool_name="innovus",
            read_chunk_bytes=read_chunk_bytes,
            internal_output_max_bytes=internal_output_max_bytes,
        )


class PrimeTimeExecutor(TclToolExecutor):
    def __init__(
        self,
        working_dir: str,
        primetime_bin: str = "pt_shell",
        primetime_args: list[str] | None = None,
        *,
        read_chunk_bytes: int = 64 * 1024,
        internal_output_max_bytes: int | None = None,
    ) -> None:
        super().__init__(
            working_dir,
            primetime_bin,
            primetime_args,
            tool_name="primetime",
            read_chunk_bytes=read_chunk_bytes,
            internal_output_max_bytes=internal_output_max_bytes,
        )


def build_executor(
    lang: str,
    working_dir: str,
    shell_path: str,
    tcl_shell: str,
    innovus_bin: str = "innovus",
    innovus_args: list[str] | None = None,
    innovus_startup_tcl: str = "",
    innovus_startup_timeout: float = 300.0,
    primetime_bin: str = "pt_shell",
    primetime_args: list[str] | None = None,
    primetime_startup_tcl: str = "",
    primetime_startup_timeout: float = 300.0,
    read_chunk_bytes: int = 64 * 1024,
    internal_output_max_bytes: int | None = None,
) -> BaseExecutor:
    if lang == "python":
        return PythonExecutor(working_dir)
    if lang == "shell":
        return ShellExecutor(
            working_dir,
            shell_path,
            read_chunk_bytes=read_chunk_bytes,
        )
    if lang == "tcl":
        return TCLExecutor(
            working_dir,
            tcl_shell,
            read_chunk_bytes=read_chunk_bytes,
        )
    if lang == "innovus":
        return InnovusExecutor(
            working_dir,
            innovus_bin,
            innovus_args,
            read_chunk_bytes=read_chunk_bytes,
            internal_output_max_bytes=internal_output_max_bytes,
        )
    if lang == "primetime":
        return PrimeTimeExecutor(
            working_dir,
            primetime_bin,
            primetime_args,
            read_chunk_bytes=read_chunk_bytes,
            internal_output_max_bytes=internal_output_max_bytes,
        )
    raise ValueError(f"unknown lang: {lang}")


def _combine_execution_results(
    results: list[ExecutionResult],
    exit_code: int,
) -> ExecutionResult:
    metadata: dict[str, object] = {}
    for result in results:
        metadata.update(result.metadata)
    return ExecutionResult(
        "".join(result.stdout for result in results),
        "".join(result.stderr for result in results),
        exit_code,
        sum(result.duration for result in results),
        metadata=metadata,
    )


def _execution_summary_metadata(summary: ExecutionSummary) -> dict[str, str]:
    if summary.termination_reason is None:
        return {}
    return {"termination_reason": summary.termination_reason}


_INNOVUS_TCL_BASELINE_SCRIPT = r"""
namespace eval ::science_eda_pool {
    proc list_namespaces {root} {
        set result {}
        foreach ns [namespace children $root] {
            if {$ns eq "::science_eda_pool"} {
                continue
            }
            if {[string match "::science_eda_pool::*" $ns]} {
                continue
            }
            lappend result $ns
            foreach child [::science_eda_pool::list_namespaces $ns] {
                lappend result $child
            }
        }
        return $result
    }

    proc procs_in_namespace {ns} {
        if {$ns eq "::"} {
            return [info procs ::*]
        }
        return [info procs ${ns}::*]
    }

    proc list_procs {} {
        set result {}
        foreach ns [concat [list ::] [::science_eda_pool::list_namespaces ::]] {
            foreach proc_name [::science_eda_pool::procs_in_namespace $ns] {
                if {![string match "::science_eda_pool::*" $proc_name]} {
                    lappend result $proc_name
                }
            }
        }
        return $result
    }

    proc namespace_depth_compare {a b} {
        set da [llength [split $a :]]
        set db [llength [split $b :]]
        if {$da == $db} {
            return [string compare $b $a]
        }
        return [expr {$db - $da}]
    }

    proc capture_baseline {} {
        variable baseline_cwd [pwd]
        variable baseline_globals [info globals]
        variable baseline_namespaces [::science_eda_pool::list_namespaces ::]
        variable baseline_procs [::science_eda_pool::list_procs]
        variable baseline_aliases [interp aliases {}]
        variable baseline_env [array get ::env]
    }

    proc cleanup_tcl_state {clean_env} {
        variable baseline_cwd
        variable baseline_globals
        variable baseline_namespaces
        variable baseline_procs
        variable baseline_aliases
        variable baseline_env

        foreach alias_name [interp aliases {}] {
            if {[lsearch -exact $baseline_aliases $alias_name] < 0} {
                catch {interp alias {} $alias_name {}}
            }
        }

        foreach proc_name [::science_eda_pool::list_procs] {
            if {[lsearch -exact $baseline_procs $proc_name] < 0} {
                if {![string match "::science_eda_pool::*" $proc_name]} {
                    catch {rename $proc_name ""}
                }
            }
        }

        set current_namespaces [::science_eda_pool::list_namespaces ::]
        foreach ns [lsort -command ::science_eda_pool::namespace_depth_compare $current_namespaces] {
            if {[lsearch -exact $baseline_namespaces $ns] < 0} {
                if {![string match "::science_eda_pool::*" $ns]} {
                    catch {namespace delete $ns}
                }
            }
        }

        foreach var_name [info globals] {
            if {[lsearch -exact $baseline_globals $var_name] < 0} {
                catch {unset -nocomplain ::$var_name}
            }
        }

        if {$clean_env && [array exists ::env]} {
            foreach env_name [array names ::env] {
                if {![dict exists $baseline_env $env_name]} {
                    catch {unset -nocomplain ::env($env_name)}
                }
            }
            dict for {env_name env_value} $baseline_env {
                set ::env($env_name) $env_value
            }
        }

        cd $baseline_cwd
    }
}

::science_eda_pool::capture_baseline
"""
