"""MCP stdio server for the interactive sandbox HTTP API.

This module intentionally stays outside the interactive v1 control plane.  It
is a thin Claude Code-facing adapter over the daemon's loopback HTTP API.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

DEFAULT_ENDPOINT = "http://127.0.0.1:8765"
DEFAULT_PROTOCOL_VERSION = "2024-11-05"
ENDPOINT_ENV = "SCIENCE_EDA_INTERACTIVE_SANDBOX_ENDPOINT"
SUPPORTED_TOOL_KINDS = {"innovus", "primetime"}
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


class InteractiveSandboxMCPServer:
    """Minimal JSON-RPC MCP server that talks to a local sandbox daemon."""

    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint.rstrip("/")
        self._handlers: dict[str, Callable[[dict[str, Any]], Any]] = {
            "initialize": self._handle_initialize,
            "ping": self._handle_ping,
            "tools/list": self._handle_tools_list,
            "tools/call": self._handle_tools_call,
        }

    def serve_forever(self) -> None:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            response = self._handle_raw_message(line)
            if response is not None:
                self._write_message(response)

    def _handle_raw_message(self, line: str) -> dict[str, Any] | None:
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            return _error_response(None, -32700, f"Parse error: {exc}")

        request_id = message.get("id")
        method = message.get("method")
        if request_id is None:
            # MCP notifications, such as notifications/initialized, do not get
            # responses.
            return None
        if not isinstance(method, str):
            return _error_response(request_id, -32600, "Invalid Request")

        handler = self._handlers.get(method)
        if handler is None:
            return _error_response(request_id, -32601, f"Method not found: {method}")

        params = message.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, dict):
            return _error_response(request_id, -32602, "params must be an object")

        try:
            result = handler(params)
        except Exception as exc:  # noqa: BLE001 - MCP tools should return errors
            return _error_response(request_id, -32603, str(exc))
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    def _write_message(self, message: dict[str, Any]) -> None:
        sys.stdout.write(json.dumps(message, ensure_ascii=False, separators=(",", ":")))
        sys.stdout.write("\n")
        sys.stdout.flush()

    def _handle_initialize(self, params: dict[str, Any]) -> dict[str, Any]:
        protocol_version = params.get("protocolVersion")
        if not isinstance(protocol_version, str) or not protocol_version:
            protocol_version = DEFAULT_PROTOCOL_VERSION
        return {
            "protocolVersion": protocol_version,
            "capabilities": {"tools": {}},
            "serverInfo": {
                "name": "eda-sandbox",
                "version": "0.1.0",
            },
        }

    def _handle_ping(self, _params: dict[str, Any]) -> dict[str, Any]:
        return {}

    def _handle_tools_list(self, _params: dict[str, Any]) -> dict[str, Any]:
        return {"tools": _tool_definitions()}

    def _handle_tools_call(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(name, str):
            raise ValueError("tools/call requires string params.name")
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict):
            raise ValueError("tools/call params.arguments must be an object")

        if name == "create_session":
            return self._tool_create_session(arguments)
        if name == "execute_in_session":
            return self._tool_execute_in_session(arguments)
        if name == "destroy_session":
            return self._tool_destroy_session(arguments)
        raise ValueError(f"unknown tool: {name}")

    def _tool_create_session(self, arguments: dict[str, Any]) -> dict[str, Any]:
        try:
            payload = _create_session_payload(arguments)
            response = self._request_json(
                "POST",
                "/v1/workspace-sessions/create",
                json_body=payload,
            )
        except Exception as exc:  # noqa: BLE001 - tool failures belong in tool results
            return _tool_exception_result("create_session", exc)

        workspace_session = (
            response.get("workspace_session")
            or response.get("session")
            or response.get("sessions")
            or {}
        )
        state = _upper_text(workspace_session.get("state"))
        created_session_id = response.get("session_id") or workspace_session.get(
            "workspace_session_id"
        )

        if state == "ACTIVE" and isinstance(created_session_id, str):
            return _text_tool_result(created_session_id)

        return _text_tool_result(
            "Session创建失败\n",
            is_error=True,
        )

    def _tool_execute_in_session(
        self,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            session_id = _validate_session_id(
                _required_str(arguments, "session_id"),
                "session_id",
            )
            command = _required_str(arguments, "command")
            payload = {
                "request_id": f"mcp-{uuid.uuid4().hex}",
                "code": command,
            }
            response = self._request_json(
                "POST",
                f"/v1/workspace-sessions/{session_id}:execute",
                json_body=payload,
            )
        except Exception as exc:  # noqa: BLE001 - tool failures belong in tool results
            return _execute_exception_result(exc)

        full_log_path = _find_full_log_path(response)
        preview = _find_output_preview(response)
        state = _upper_text(response.get("state"))
        result_payload = {
            "state": state or "",
            "exit_code": _dig(response, "result", "exit_code"),
            "error": _dig(response, "result", "error"),
            "full_log_path": full_log_path or "",
            "output_preview": preview or "",
        }
        return _structured_tool_result(
            result_payload,
            is_error=_execution_response_failed(response),
        )

    def _tool_destroy_session(self, arguments: dict[str, Any]) -> dict[str, Any]:
        try:
            instance_id = _validate_session_id(
                _required_str(arguments, "instance_id"),
                "instance_id",
            )
            response = self._request_json(
                "DELETE",
                f"/v1/workspace-sessions/{instance_id}",
            )
        except Exception as exc:  # noqa: BLE001 - tool failures belong in tool results
            return _tool_exception_result("destroy_session", exc)

        state = _upper_text(response.get("state"))
        if state == "CLOSED":
            return _text_tool_result("销毁成功")
        return _text_tool_result(
            "Session销毁失败\n",
            is_error=True,
        )

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.endpoint}{path}"
        try:
            with httpx.Client(timeout=None) as client:
                response = client.request(method, url, json=json_body)
        except httpx.HTTPError as exc:
            raise RuntimeError(f"无法连接 interactive sandbox daemon: {exc}") from exc

        data = response.json()

        if response.status_code >= 400:
            raise RuntimeError(
                f"sandbox 请求失败: HTTP {response.status_code}\n{_pretty_json(data)}"
            )
        return data


def _tool_definitions() -> list[dict[str, Any]]:
    return [
        {
            "name": "create_session",
            "description": (
                "Use this tool to start a persistent, stateful EDA "
                " tool session for Innovus or "
                "PrimeTime work. When a task requires compiling or validating "
                "Innovus Tcl code with Innovus, or PrimeTime Tcl code with "
                "PrimeTime, call create_session first, then call "
                "execute_in_session with the returned session_id to run concrete "
                "EDA commands. It creates a session bound to the requested local "
                "workspace and returns the internally generated session_id only "
                "when the sandbox "
                "reports the session state as ACTIVE."
                "All later execute_in_session calls for the same session_id run in the "
                "same live EDA process, so commands are cumulative "
                "and may depend on state created by earlier commands, such as "
                "loaded designs, variables."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tool_kind": {
                        "type": "string",
                        "enum": ["innovus", "primetime"],
                        "description": "Choose innovus for Innovus code or primetime for PrimeTime code.",
                    },
                    "workspace_path": {
                        "type": "string",
                        "description": "Local absolute workspace directory used as the EDA runtime cwd.",
                    },
                },
                "required": ["tool_kind", "workspace_path"],
                "additionalProperties": False,
            },
        },
        {
            "name": "execute_in_session",
            "description": (
                "Execute a concrete Innovus or PrimeTime Tcl command/code block "
                "inside an existing EDA session created by create_session. Use "
                "this for repeated compile, check, source, report, or validation "
                "commands while preserving the tool runtime state. Returns two "
                "values: the complete full-log file path from "
                "result.output.full_log.path when available, and the bounded "
                "output preview returned by the sandbox."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "EDA workspace session id returned by create_session.",
                    },
                    "command": {
                        "type": "string",
                        "description": "Innovus or PrimeTime Tcl command/code to execute.",
                    },
                },
                "required": ["session_id", "command"],
                "additionalProperties": False,
            },
        },
        {
            "name": "destroy_session",
            "description": (
                "Destroy an EDA session when the overall task is complete and no "
                "more Innovus or PrimeTime interaction is needed. This closes the "
                "workspace session and releases the dedicated EDA tool runtime. "
                "The input instance_id is the session id returned by create_session."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "instance_id": {
                        "type": "string",
                        "description": "EDA workspace session id to destroy.",
                    },
                },
                "required": ["instance_id"],
                "additionalProperties": False,
            },
        },
    ]


def _required_str(arguments: dict[str, Any], name: str) -> str:
    value = arguments.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"missing or empty string argument: {name}")
    return value


def _create_session_payload(arguments: dict[str, Any]) -> dict[str, str]:
    payload = {
        "tool_kind": _validate_tool_kind(_required_str(arguments, "tool_kind")),
        "session_id": uuid.uuid4().hex,
    }
    payload["workspace_path"] = _validate_workspace_path(
        _required_str(arguments, "workspace_path")
    )
    return payload


def _validate_tool_kind(value: str) -> str:
    if value not in SUPPORTED_TOOL_KINDS:
        supported = ", ".join(sorted(SUPPORTED_TOOL_KINDS))
        raise ValueError(f"tool_kind must be one of: {supported}")
    return value


def _validate_session_id(value: str, field_name: str) -> str:
    if not _SESSION_ID_RE.fullmatch(value):
        raise ValueError(
            f"{field_name} must be 1-128 ASCII letters, digits, underscores, "
            "or hyphens and must start with a letter or digit"
        )
    return value


def _validate_workspace_path(value: str) -> str:
    if "\x00" in value:
        raise ValueError("workspace_path must not contain NUL bytes")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("workspace_path must be valid UTF-8 text") from exc

    raw = Path(value)
    if not raw.is_absolute():
        raise ValueError("workspace_path must be an absolute path")
    try:
        resolved = raw.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"workspace_path cannot be resolved: {value!r}") from exc
    if not resolved.is_dir():
        raise ValueError("workspace_path must name a directory")
    try:
        searchable = os.access(resolved, os.X_OK, effective_ids=True)
    except (TypeError, NotImplementedError):  # pragma: no cover - platform-specific
        searchable = os.access(resolved, os.X_OK)
    if not searchable:
        raise ValueError("workspace_path cannot be used as a runtime working directory")
    return value


def _upper_text(value: Any) -> str:
    return str(value).upper() if value is not None else ""


def _find_full_log_path(response: dict[str, Any]) -> str | None:
    candidates: list[Any] = [
        _dig(response, "result", "output", "full_log", "path"),
        _dig(response, "result", "output", "full_log", "local_log_path"),
        _dig(response, "result", "output", "local_log_path"),
        _dig(response, "result", "output", "full_log_path"),
        _dig(response, "result", "full_log", "path"),
        _dig(response, "local_log_path"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate
    return None


def _find_output_preview(response: dict[str, Any]) -> str | None:
    preview = _dig(response, "result", "output", "preview")
    if isinstance(preview, str):
        return preview
    summary_preview = _dig(response, "result_summary", "output", "preview")
    if isinstance(summary_preview, str):
        return summary_preview
    return None


def _execution_response_failed(response: dict[str, Any]) -> bool:
    state = _upper_text(response.get("state"))
    if state in {"FAILED", "TIMED_OUT", "LOST"}:
        return True
    exit_code = _dig(response, "result", "exit_code")
    if isinstance(exit_code, int) and not isinstance(exit_code, bool) and exit_code != 0:
        return True
    return _dig(response, "result", "error") is not None


def _dig(mapping: dict[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _text_tool_result(text: str, *, is_error: bool = False) -> dict[str, Any]:
    result: dict[str, Any] = {"content": [{"type": "text", "text": text}]}
    if is_error:
        result["isError"] = True
    return result


def _structured_tool_result(
    payload: dict[str, Any],
    *,
    is_error: bool = False,
) -> dict[str, Any]:
    result = {"structuredContent": payload}
    if is_error:
        result["isError"] = True
    return result


def _execute_exception_result(exc: Exception) -> dict[str, Any]:
    return _structured_tool_result(
        {
            "state": "",
            "exit_code": None,
            "error": {
                "code": "MCP_TOOL_ERROR",
                "message": f"execute_in_session failed: {exc}",
            },
            "full_log_path": "",
            "output_preview": "",
        },
        is_error=True,
    )


def _tool_exception_result(tool_name: str, exc: Exception) -> dict[str, Any]:
    return _text_tool_result(f"{tool_name} failed: {exc}", is_error=True)


def _pretty_json(value: Any) -> str:
    return json.dumps(
        _strip_public_response_fields(value),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )


def _pretty_public_json(value: Any) -> str:
    return _pretty_json(value)


def _strip_public_response_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_public_response_fields(item)
            for key, item in value.items()
            if key not in {"schema_version", "request_id"}
        }
    if isinstance(value, list):
        return [_strip_public_response_fields(item) for item in value]
    return value


def _error_response(
    request_id: Any,
    code: int,
    message: str,
) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": code, "message": message},
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the ScienceEDA interactive sandbox MCP stdio server.",
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get(ENDPOINT_ENV, DEFAULT_ENDPOINT),
        help=(
            "Interactive sandbox daemon endpoint. Defaults to "
            f"{ENDPOINT_ENV} or {DEFAULT_ENDPOINT}."
        ),
    )
    args = parser.parse_args(argv)

    InteractiveSandboxMCPServer(args.endpoint).serve_forever()


if __name__ == "__main__":  # pragma: no cover
    main()
