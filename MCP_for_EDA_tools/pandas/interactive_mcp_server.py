"""MCP stdio server for the internal EDA sandbox worker API.

This module keeps the external MCP tool surface aligned with
``science_eda.sandbox.interactive_mcp_server`` while adapting calls to the
internal worker API documented in ``docs.txt``.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import subprocess
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

DEFAULT_ENDPOINT = "http://127.0.0.1:8765"
DEFAULT_PROTOCOL_VERSION = "2024-11-05"
ENDPOINT_ENV = "PANDAS_MCP_SANDBOX_ENDPOINT"
WORKER_URL_ENV = "PANDAS_MCP_WORKER_URL"
USERNAME_ENV = "PANDAS_MCP_USERNAME"
SUPPORTED_TOOL_KINDS = {"innovus", "primetime"}
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


class InternalSandboxMCPServer:
    """Minimal JSON-RPC MCP server over the internal sandbox HTTP API."""

    def __init__(
        self,
        endpoint: str,
        *,
        worker_url: str | None = None,
        username: str | None = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self._worker_url = worker_url.rstrip("/") if worker_url else None
        self.username = username or _current_username()
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
            create_payload = _create_session_payload(arguments)
            payload = {
                "root_path": create_payload["workspace_path"],
                "eda_type": _to_internal_eda_type(create_payload["tool_kind"]),
                "instance_id": create_payload["session_id"],
            }
            response = self._worker_request_json(
                "POST",
                "/worker/instances/start",
                json_body=payload,
            )
        except Exception as exc:  # noqa: BLE001 - tool failures belong in tool results
            return _tool_exception_result("create_session", exc)

        created_instance_id = (
            _dig(response, "data", "instance_id") or create_payload["session_id"]
        )
        if response.get("success") is True and isinstance(created_instance_id, str):
            return _text_tool_result(created_instance_id)

        return _text_tool_result(
            "Session閸掓稑缂撴径杈Е\n",
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

            response = self._worker_request_json(
                "POST",
                f"/worker/instances/{session_id}/eda/execute-tcl",
                json_body={"command": command},
            )
            instance_response = self._worker_request_json(
                "GET",
                f"/worker/instances/{session_id}",
            )
        except Exception as exc:  # noqa: BLE001 - tool failures belong in tool results
            return _execute_exception_result(exc)

        payload = {
            "state": _worker_execution_state(response),
            "exit_code": _worker_execution_exit_code(response),
            "error": None
            if _worker_execution_succeeded(response)
            else _worker_execution_error(response),
            "full_log_path": _find_apr_log(instance_response) or "",
            "output_preview": _strip_prompt(_dig(response, "data", "result")),
        }
        return _structured_tool_result(
            payload,
            is_error=_execution_response_failed(payload),
        )

    def _tool_destroy_session(self, arguments: dict[str, Any]) -> dict[str, Any]:
        try:
            instance_id = _validate_session_id(
                _required_str(arguments, "instance_id"),
                "instance_id",
            )
            response = self._worker_request_json(
                "POST",
                f"/worker/instances/{instance_id}/stop",
            )
        except Exception as exc:  # noqa: BLE001 - tool failures belong in tool results
            return _tool_exception_result("destroy_session", exc)

        status = _dig(response, "data", "status")
        if response.get("success") is True and status == "stopped":
            return _text_tool_result("销毁成功")

        return _text_tool_result(
            "Session销毁失败\n",
            is_error=True,
        )

    def _worker_request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            method,
            f"{self._resolve_worker_url()}{path}",
            json_body=json_body,
        )

    def _resolve_worker_url(self) -> str:
        if self._worker_url:
            return self._worker_url

        response = self._request_json(
            "GET",
            f"{self.endpoint}/eda_agent/workers/detail",
        )
        data =response.get("data")
        if isinstance(data, list):
            data = data[0] if data else None
        worker_url = _dig(data, "worker_url")
        if not isinstance(worker_url, str) or not worker_url.strip():
            raise RuntimeError(
                "sandbox 濞屸剝婀佹潻鏂挎礀 data.worker_url閵嗕繐n"
                f"response={_pretty_json(response)}"
            )
        self._worker_url = worker_url.rstrip("/")
        return self._worker_url

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers = {
            "Content-Type": "application/json",
            "X-Username": self.username,
        }
        try:
            with httpx.Client(timeout=None) as client:
                response = client.request(method, url, headers=headers, json=json_body)
        except httpx.HTTPError as exc:
            raise RuntimeError(f"閺冪姵纭舵潻鐐村复 interactive sandbox daemon: {exc}") from exc

        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"sandbox 鏉╂柨娲栨禍鍡涙姜 JSON 閸濆秴绨? HTTP {response.status_code} "
                f"{response.text[:4096]}"
            ) from exc

        if response.status_code >= 400:
            raise RuntimeError(
                f"sandbox 鐠囬攱鐪版径杈Е: HTTP {response.status_code}\n{_pretty_json(data)}"
            )
        if not isinstance(data, dict):
            raise RuntimeError(f"sandbox 鏉╂柨娲?JSON 妞よ泛鐪版稉宥嗘Ц object: {data!r}")
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


def _current_username() -> str:
    env_username = os.environ.get(USERNAME_ENV)
    if env_username:
        return env_username
    try:
        username = subprocess.check_output(
            ["whoami"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        username = getpass.getuser()
    return username or getpass.getuser()


def _to_internal_eda_type(tool_kind: str) -> str:
    if tool_kind == "primetime":
        return "pt"
    if tool_kind == "innovus":
        return "innovus"
    raise ValueError("tool_kind must be one of: innovus, primetime")


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


def _find_apr_log(response: dict[str, Any]) -> str | None:
    apr_log = _dig(response, "data", "apr_log")
    if isinstance(apr_log, str) and apr_log.strip():
        return apr_log
    return None


def _strip_prompt(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    text = value
    for prompt in ("pt_shell>", "innovus>"):
        if text.startswith(prompt):
            return text[len(prompt) :].lstrip()
    return text


def _dig(mapping: dict[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _worker_execution_error(response: dict[str, Any]) -> dict[str, Any]:
    return {
        "code": "TCL_ERROR",
        "message": _worker_error_message(response),
    }


def _worker_execution_state(response: dict[str, Any]) -> str:
    if _worker_execution_succeeded(response):
        return "SUCCEEDED"
    return "FAILED"


def _worker_execution_exit_code(response: dict[str, Any]) -> int | None:
    if _worker_execution_succeeded(response):
        return 0
    return 1


def _worker_execution_succeeded(response: dict[str, Any]) -> bool:
    return response.get("success") is True and not _worker_result_has_error(response)


def _worker_result_has_error(response: dict[str, Any]) -> bool:
    result = _dig(response, "data", "result")
    return (
        isinstance(result, str)
        and re.search(r"error", result, re.IGNORECASE) is not None
    )


def _worker_error_message(response: dict[str, Any]) -> str:
    message = response.get("message")
    if isinstance(message, str) and message:
        return message
    result = _dig(response, "data", "result")
    if isinstance(result, str) and result:
        return _strip_prompt(result)
    return "Tcl command failed"


def _execution_response_failed(response: dict[str, Any]) -> bool:
    state = _upper_text(response.get("state"))
    if state in {"FAILED", "TIMED_OUT", "LOST"}:
        return True
    exit_code = response.get("exit_code")
    if isinstance(exit_code, int) and not isinstance(exit_code, bool) and exit_code != 0:
        return True
    return response.get("error") is not None


def _upper_text(value: Any) -> str:
    return str(value).upper() if value is not None else ""


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
    result = {
        "content": [{"type": "text", "text": _format_execute_content(payload)}],
        "structuredContent": payload,
    }
    if is_error:
        result["isError"] = True
    return result


def _format_execute_content(payload: dict[str, Any]) -> str:
    error = payload.get("error")
    if error is None:
        error_text = "null"
    elif isinstance(error, dict):
        code = error.get("code")
        message = error.get("message")
        if isinstance(code, str) and isinstance(message, str):
            error_text = f"{code}: {message}"
        else:
            error_text = _pretty_public_json(error)
    else:
        error_text = str(error)

    exit_code = payload.get("exit_code")
    lines = [
        f"state={payload.get('state') or ''}",
        f"exit_code={exit_code if exit_code is not None else 'null'}",
        f"error={error_text}",
        f"full_log_path={payload.get('full_log_path') or ''}",
        "output_preview:",
        str(payload.get("output_preview") or ""),
    ]
    return "\n".join(lines)


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
        description="Run the internal EDA sandbox MCP stdio server.",
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get(ENDPOINT_ENV, DEFAULT_ENDPOINT),
        help=(
            "Internal sandbox control-plane endpoint used to discover worker_url. "
            f"Defaults to {ENDPOINT_ENV} or {DEFAULT_ENDPOINT}."
        ),
    )
    parser.add_argument(
        "--worker-url",
        default=os.environ.get(WORKER_URL_ENV),
        help=(
            "Direct worker URL. When set, the server skips "
            "/eda_agent/workers/detail discovery."
        ),
    )
    parser.add_argument(
        "--username",
        default=os.environ.get(USERNAME_ENV),
        help=(
            "Username for X-Username. Defaults to "
            f"{USERNAME_ENV} or the current shell whoami value."
        ),
    )
    args = parser.parse_args(argv)

    InternalSandboxMCPServer(
        args.endpoint,
        worker_url=args.worker_url,
        username=args.username,
    ).serve_forever()


if __name__ == "__main__":  # pragma: no cover
    main()
