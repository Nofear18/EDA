"""Sandbox worker subprocess entry (long-lived Python process + optional tools)."""

from __future__ import annotations

import logging
import os
import threading
import time
from multiprocessing.connection import Connection
from typing import Any

from science_eda.sandbox.executor import BaseExecutor, build_executor
from science_eda.sandbox.output import FilePreviewOutputSink, OutputSink

logger = logging.getLogger(__name__)

_TOOL_LANGS = {"innovus", "primetime"}


def _apply_resource_limits(memory_mb: int, max_pids: int) -> None:
    try:
        import resource

        if memory_mb and memory_mb > 0:
            limit = memory_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
        if max_pids and max_pids > 0 and hasattr(resource, "RLIMIT_NPROC"):
            resource.setrlimit(resource.RLIMIT_NPROC, (max_pids, max_pids))
    except (ValueError, OSError):
        # Best-effort: macOS/limits may differ from Linux cgroup semantics.
        pass


def _watch_parent_exit(
    executor: BaseExecutor | None = None,
    expected_parent_pid: int | None = None,
) -> None:
    initial = expected_parent_pid or os.getppid()
    while True:
        time.sleep(2.0)
        if os.getppid() != initial:
            if executor is not None:
                try:
                    executor.close()
                except Exception:  # noqa: BLE001
                    logger.exception("failed to close executor during parent-exit cleanup")
            os._exit(0)


def _start_parent_exit_watcher(
    executor: BaseExecutor | None = None,
    expected_parent_pid: int | None = None,
) -> None:
    try:
        watcher = threading.Thread(
            target=_watch_parent_exit,
            args=(executor, expected_parent_pid),
            daemon=True,
        )
        watcher.start()
    except Exception:  # noqa: BLE001
        logger.warning("failed to start sandbox parent-exit watcher", exc_info=True)


def _tool_startup(lang: str, innovus_tcl: str, innovus_timeout: float, pt_tcl: str, pt_timeout: float) -> tuple[str, float]:
    if lang == "primetime":
        return pt_tcl, pt_timeout
    return innovus_tcl, innovus_timeout


def worker_main(
    conn: Connection,
    working_dir: str,
    lang: str,
    shell_path: str,
    tcl_shell: str,
    innovus_bin: str,
    innovus_args: list[str],
    innovus_startup_tcl: str,
    innovus_startup_timeout: float,
    memory_mb: int,
    max_pids: int,
    primetime_bin: str = "pt_shell",
    primetime_args: list[str] | None = None,
    primetime_startup_tcl: str = "",
    primetime_startup_timeout: float = 300.0,
    tool_env: dict[str, str] | None = None,
    reader_chunk_bytes: int = 64 * 1024,
    expected_parent_pid: int | None = None,
    internal_output_max_bytes: int | None = None,
) -> None:
    if hasattr(os, "setsid"):
        try:
            os.setsid()
        except OSError:
            pass

    executor: BaseExecutor | None = None
    prepared_execution: tuple[str, OutputSink] | None = None
    tool_startup_tcl, tool_startup_timeout = _tool_startup(
        lang,
        innovus_startup_tcl,
        innovus_startup_timeout,
        primetime_startup_tcl,
        primetime_startup_timeout,
    )
    try:
        if expected_parent_pid is not None and os.getppid() != expected_parent_pid:
            return
        if tool_env is not None:
            os.environ.clear()
            os.environ.update({str(key): str(value) for key, value in tool_env.items()})
        executor = build_executor(
            lang,
            working_dir,
            shell_path,
            tcl_shell,
            innovus_bin,
            innovus_args,
            innovus_startup_tcl,
            innovus_startup_timeout,
            primetime_bin,
            list(primetime_args or []),
            primetime_startup_tcl,
            primetime_startup_timeout,
            reader_chunk_bytes,
            internal_output_max_bytes,
        )
        tool_proc = getattr(executor, "_proc", None)
        tool_pid = getattr(tool_proc, "pid", None)
        if expected_parent_pid is not None and os.getppid() != expected_parent_pid:
            executor.close()
            return
        if tool_pid is not None:
            conn.send({"op": "tool_started", "tool_process_pid": tool_pid})
        _start_parent_exit_watcher(executor, expected_parent_pid)
        if lang in _TOOL_LANGS:
            getattr(executor, "startup")(tool_startup_tcl, tool_startup_timeout)
            getattr(executor, "capture_baseline")(tool_startup_timeout)
        os.chdir(working_dir)
        _apply_resource_limits(memory_mb, max_pids)
    except BaseException as e:  # noqa: BLE001
        try:
            conn.send({"op": "startup_error", "error": f"{type(e).__name__}: {e}"})
        except Exception:
            pass
        if executor is not None:
            executor.close()
        return

    conn.send({"op": "ready", "tool_process_pid": tool_pid})
    try:
        while True:
            try:
                msg: dict[str, Any] = conn.recv()
            except EOFError:
                break
            op = msg.get("op")
            if op == "shutdown":
                break
            if op == "prepare_session":
                _handle_prepare_session(conn, executor, lang, msg, tool_startup_timeout)
                continue
            if op == "reset_session":
                _handle_reset_session(conn, executor, lang, msg, working_dir, tool_startup_timeout)
                continue
            if op == "healthcheck":
                _handle_healthcheck(conn, executor, lang, msg, tool_startup_timeout)
                continue
            if op == "set_workspace_cwd":
                _handle_set_workspace_cwd(
                    conn,
                    executor,
                    lang,
                    msg,
                    tool_startup_timeout,
                )
                continue
            if op == "prepare_execution":
                if prepared_execution is not None:
                    conn.send(
                        {
                            "op": "prepare_execution_error",
                            "execution_id": msg.get("execution_id"),
                            "error": "another execution sink is already prepared",
                        }
                    )
                    continue
                prepared_execution = _handle_prepare_execution(conn, msg)
                continue
            if op == "execute_prepared":
                prepared_execution = _handle_execute_prepared(
                    conn,
                    executor,
                    msg,
                    prepared_execution,
                )
                continue
            if op == "execute":
                result = executor.execute(
                    str(msg["code"]),
                    float(msg.get("timeout", 30.0)),
                )
                conn.send(result)
    finally:
        if prepared_execution is not None:
            prepared_execution[1].abort()
        executor.close()


def _handle_prepare_session(
    conn: Connection,
    executor: BaseExecutor,
    lang: str,
    msg: dict[str, Any],
    default_timeout: float,
) -> None:
    if lang not in _TOOL_LANGS:
        conn.send(
            {
                "op": "prepare_error",
                "error": "prepare_session is only supported for tool languages",
            }
        )
        return
    try:
        next_working_dir = str(msg["working_dir"])
        os.makedirs(next_working_dir, exist_ok=True)
        getattr(executor, "prepare_session")(
            next_working_dir,
            str(msg.get("startup_tcl", "")),
            float(msg.get("startup_timeout", default_timeout)),
        )
        conn.send({"op": "prepared", "session_id": msg.get("session_id")})
    except BaseException as e:  # noqa: BLE001
        conn.send({"op": "prepare_error", "error": f"{type(e).__name__}: {e}"})


def _handle_reset_session(
    conn: Connection,
    executor: BaseExecutor,
    lang: str,
    msg: dict[str, Any],
    physical_working_dir: str,
    default_timeout: float,
) -> None:
    if lang not in _TOOL_LANGS:
        conn.send(
            {
                "op": "reset_error",
                "error": "reset_session is only supported for tool languages",
            }
        )
        return
    try:
        result = getattr(executor, "reset_session")(
            str(msg.get("reset_tcl", "")),
            bool(msg.get("clean_tcl_state", True)),
            float(msg.get("timeout", default_timeout)),
        )
        os.chdir(physical_working_dir)
        conn.send({"op": "reset_done", "result": result})
    except BaseException as e:  # noqa: BLE001
        conn.send({"op": "reset_error", "error": f"{type(e).__name__}: {e}"})


def _handle_healthcheck(
    conn: Connection,
    executor: BaseExecutor,
    lang: str,
    msg: dict[str, Any],
    default_timeout: float,
) -> None:
    if lang not in _TOOL_LANGS:
        conn.send(
            {
                "op": "healthcheck_error",
                "error": "healthcheck is only supported for tool languages",
            }
        )
        return
    try:
        result = getattr(executor, "healthcheck")(
            str(msg.get("code", "")),
            float(msg.get("timeout", default_timeout)),
        )
        conn.send({"op": "healthcheck_done", "result": result})
    except BaseException as e:  # noqa: BLE001
        conn.send({"op": "healthcheck_error", "error": f"{type(e).__name__}: {e}"})


def _handle_set_workspace_cwd(
    conn: Connection,
    executor: BaseExecutor,
    lang: str,
    msg: dict[str, Any],
    default_timeout: float,
) -> None:
    execution_path = str(msg.get("path", ""))
    if lang not in _TOOL_LANGS:
        conn.send(
            {
                "op": "workspace_cwd_error",
                "workspace_path": execution_path,
                "error": "set_workspace_cwd is only supported for tool languages",
            }
        )
        return
    try:
        result = getattr(executor, "set_workspace_cwd")(
            execution_path,
            float(msg.get("timeout", default_timeout)),
        )
        conn.send(
            {
                "op": "workspace_cwd_set",
                "workspace_path": os.path.realpath(execution_path),
                "result": result,
            }
        )
    except BaseException as e:  # noqa: BLE001
        conn.send(
            {
                "op": "workspace_cwd_error",
                "workspace_path": execution_path,
                "error": f"{type(e).__name__}: {e}",
            }
        )


def _handle_prepare_execution(
    conn: Connection,
    msg: dict[str, Any],
) -> tuple[str, OutputSink] | None:
    execution_id = str(msg.get("execution_id", ""))
    if not execution_id:
        conn.send(
            {
                "op": "prepare_execution_error",
                "execution_id": execution_id,
                "error": "execution_id must be non-empty",
            }
        )
        return None
    try:
        sink = FilePreviewOutputSink(
            str(msg["log_path"]),
            preview_bytes=int(msg["preview_bytes"]),
            write_budget_bytes=int(msg["write_budget_bytes"]),
            encoding=str(msg.get("encoding", "utf-8")),
        )
    except BaseException as e:  # noqa: BLE001
        conn.send(
            {
                "op": "prepare_execution_error",
                "execution_id": execution_id,
                "error": f"{type(e).__name__}: {e}",
            }
        )
        return None
    conn.send({"op": "output_sink_ready", "execution_id": execution_id})
    return execution_id, sink


def _handle_execute_prepared(
    conn: Connection,
    executor: BaseExecutor,
    msg: dict[str, Any],
    prepared: tuple[str, OutputSink] | None,
) -> tuple[str, OutputSink] | None:
    execution_id = str(msg.get("execution_id", ""))
    if prepared is None or prepared[0] != execution_id:
        conn.send(
            {
                "op": "execute_prepared_error",
                "execution_id": execution_id,
                "error": "no matching prepared execution sink",
            }
        )
        return prepared
    _, sink = prepared
    try:
        summary = executor.execute_to_sink(
            str(msg["code"]),
            float(msg.get("timeout", 30.0)),
            sink,
            execution_id,
        )
    except BaseException as e:  # noqa: BLE001
        sink.abort()
        conn.send(
            {
                "op": "execute_prepared_error",
                "execution_id": execution_id,
                "error": f"{type(e).__name__}: {e}",
            }
        )
        return None
    conn.send(
        {
            "op": "execution_summary",
            "execution_id": execution_id,
            "summary": summary,
        }
    )
    return None
