r"""Manual real-backend checks for the pandas interactive MCP adapter.

This script intentionally is not a pytest test. It calls
``interactive_mcp_server.py`` against a real pandas worker backend and
prints a compact report. Run it from this directory, for example:

    python check_backend.py \
      --endpoint http://HOST \
      --workspace-path D:\code\gitcode\pandas_mcp

If you already know the worker URL, skip worker discovery:

    python check_backend.py \
      --worker-url http://WORKER \
      --workspace-path D:\code\gitcode\pandas_mcp
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from interactive_mcp_server import InternalSandboxMCPServer


@dataclass
class CaseReport:
    name: str
    status: str
    tool: str | None = None
    backend_hit: bool | None = None
    expected_error: bool | None = None
    actual_error: bool | None = None
    input: dict[str, Any] | None = None
    output: dict[str, Any] | None = None
    backend_requests: list[dict[str, Any]] | None = None
    note: str = ""


class CountingMCPServer(InternalSandboxMCPServer):
    def __init__(
        self,
        endpoint: str,
        *,
        worker_url: str | None = None,
        username: str | None = None,
    ) -> None:
        super().__init__(endpoint, worker_url=worker_url, username=username)
        self.backend_requests: list[dict[str, Any]] = []

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.backend_requests.append(
            {
                "method": method,
                "url": url,
                "json_body": _shorten(json_body),
            }
        )
        return super()._request_json(method, url, json_body=json_body)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Call pandas MCP tools against a real pandas worker backend URL."
    )
    parser.add_argument(
        "--endpoint",
        default="http://127.0.0.1:8765",
        help=(
            "Main control-plane URL used to discover worker_url. Ignored when "
            "--worker-url is set."
        ),
    )
    parser.add_argument(
        "--worker-url",
        help="Direct worker URL. When set, the script skips worker discovery.",
    )
    parser.add_argument(
        "--username",
        help="Username for X-Username. Defaults to the MCP server's whoami logic.",
    )
    parser.add_argument(
        "--workspace-path",
        required=True,
        help=(
            "Absolute workspace directory. The MCP adapter validates it locally; "
            "the worker must also be able to use it as root_path."
        ),
    )
    parser.add_argument(
        "--tool-kind",
        default="innovus",
        choices=["innovus", "primetime"],
    )
    parser.add_argument("--success-command", default="puts hi")
    parser.add_argument(
        "--fail-command",
        default='error "manual pandas MCP error"',
        help=(
            "Command expected to make data.result contain 'error' "
            "case-insensitively."
        ),
    )
    parser.add_argument(
        "--include-busy",
        action="store_true",
        help="Try to trigger a busy-session error by running two commands concurrently.",
    )
    parser.add_argument(
        "--busy-command",
        default="after 5000; puts busy_done",
        help="Long-running Tcl command used by --include-busy.",
    )
    parser.add_argument("--busy-delay-sec", type=float, default=0.25)
    parser.add_argument(
        "--json-report",
        help="Optional path to write the full JSON report.",
    )
    args = parser.parse_args()

    server = CountingMCPServer(
        args.endpoint,
        worker_url=args.worker_url,
        username=args.username,
    )
    reports: list[CaseReport] = []

    _run_protocol_cases(server, reports)
    _run_static_preflight_cases(server, reports, args.workspace_path)
    _run_backend_error_probes(server, reports)

    create_result = _call_case(
        server,
        reports,
        name="create_session success",
        tool="create_session",
        arguments={
            "tool_kind": args.tool_kind,
            "workspace_path": args.workspace_path,
        },
        expected_error=False,
        expect_backend_hit=True,
        validators=[_validate_content_text],
    )

    session_id = _content_text(create_result)
    if _is_error(create_result) or not session_id:
        _skip_session_dependent_cases(
            reports,
            "create_session did not return a usable session id.",
        )
    else:
        _run_session_cases(server, reports, args, session_id)

    _add_known_unstable_skips(reports)
    _print_report(reports)

    if args.json_report:
        Path(args.json_report).write_text(
            json.dumps(
                [report.__dict__ for report in reports],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    failed = [report for report in reports if report.status == "FAIL"]
    return 1 if failed else 0


def _run_protocol_cases(
    server: CountingMCPServer,
    reports: list[CaseReport],
) -> None:
    cases = [
        (
            "protocol initialize",
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2024-11-05"},
            },
            False,
        ),
        (
            "protocol tools/list",
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            False,
        ),
        (
            "protocol notification initialized",
            {
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {},
            },
            False,
        ),
        (
            "protocol unknown method",
            {"jsonrpc": "2.0", "id": 3, "method": "bad", "params": {}},
            True,
        ),
        (
            "protocol bad params",
            {"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": "bad"},
            True,
        ),
        (
            "protocol unknown tool",
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {"name": "bad_tool", "arguments": {}},
            },
            True,
        ),
        (
            "protocol bad arguments",
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {"name": "execute_in_session", "arguments": "bad"},
            },
            True,
        ),
    ]
    for name, message, expected_error in cases:
        output = server._handle_raw_message(json.dumps(message))
        actual_error = _is_json_rpc_error(output)
        reports.append(
            CaseReport(
                name=name,
                status="PASS" if actual_error == expected_error else "FAIL",
                expected_error=expected_error,
                actual_error=actual_error,
                input=_shorten(message),
                output=_shorten(output),
                note="Notifications should produce no response."
                if output is None
                else "",
            )
        )


def _run_static_preflight_cases(
    server: CountingMCPServer,
    reports: list[CaseReport],
    workspace_path: str,
) -> None:
    missing_workspace = str(Path(workspace_path) / "__missing_mcp_workspace__")
    cases = [
        (
            "create_session local validation: invalid tool_kind",
            "create_session",
            {"tool_kind": "dc_shell", "workspace_path": workspace_path},
        ),
        (
            "create_session local validation: relative workspace_path",
            "create_session",
            {"tool_kind": "innovus", "workspace_path": "relative/path"},
        ),
        (
            "create_session local validation: missing workspace_path",
            "create_session",
            {"tool_kind": "innovus"},
        ),
        (
            "create_session local validation: unresolved workspace_path",
            "create_session",
            {"tool_kind": "innovus", "workspace_path": missing_workspace},
        ),
        (
            "create_session local validation: NUL in workspace_path",
            "create_session",
            {"tool_kind": "innovus", "workspace_path": "C:\\tmp\x00bad"},
        ),
        (
            "execute_in_session local validation: missing command",
            "execute_in_session",
            {"session_id": "missing-command"},
        ),
        (
            "execute_in_session local validation: invalid session_id",
            "execute_in_session",
            {"session_id": "-bad", "command": "puts hi"},
        ),
        (
            "destroy_session local validation: missing instance_id",
            "destroy_session",
            {},
        ),
        (
            "destroy_session local validation: invalid instance_id",
            "destroy_session",
            {"instance_id": "-bad"},
        ),
    ]
    for name, tool, arguments in cases:
        _call_case(
            server,
            reports,
            name=name,
            tool=tool,
            arguments=arguments,
            expected_error=True,
            expect_backend_hit=False,
        )


def _run_backend_error_probes(
    server: CountingMCPServer,
    reports: list[CaseReport],
) -> None:
    missing_session_id = f"missing-{uuid.uuid4().hex}"
    _call_case(
        server,
        reports,
        name="execute_in_session backend error: unknown session",
        tool="execute_in_session",
        arguments={"session_id": missing_session_id, "command": "puts hi"},
        expected_error=True,
        expect_backend_hit=True,
    )
    _call_case(
        server,
        reports,
        name="destroy_session backend error: unknown session",
        tool="destroy_session",
        arguments={"instance_id": missing_session_id},
        expected_error=True,
        expect_backend_hit=True,
    )


def _run_session_cases(
    server: CountingMCPServer,
    reports: list[CaseReport],
    args: argparse.Namespace,
    session_id: str,
) -> None:
    _call_case(
        server,
        reports,
        name="execute_in_session success",
        tool="execute_in_session",
        arguments={"session_id": session_id, "command": args.success_command},
        expected_error=False,
        expect_backend_hit=True,
        validators=[_validate_execute_success],
    )

    _call_case(
        server,
        reports,
        name="execute_in_session Tcl failure from data.result",
        tool="execute_in_session",
        arguments={"session_id": session_id, "command": args.fail_command},
        expected_error=True,
        expect_backend_hit=True,
        validators=[_validate_tcl_error],
        note=(
            "This should fail only when the worker returns data.result containing "
            "'error' case-insensitively, even if success=true."
        ),
    )

    if args.include_busy:
        _run_busy_case(server, reports, args, session_id)
    else:
        reports.append(
            CaseReport(
                name="execute_in_session backend error: session busy",
                status="SKIP",
                tool="execute_in_session",
                note="Use --include-busy to run a concurrent execution probe.",
            )
        )

    _call_case(
        server,
        reports,
        name="destroy_session success",
        tool="destroy_session",
        arguments={"instance_id": session_id},
        expected_error=False,
        expect_backend_hit=True,
    )

    _call_case(
        server,
        reports,
        name="destroy_session stopped-session retry",
        tool="destroy_session",
        arguments={"instance_id": session_id},
        expected_error=False,
        expect_backend_hit=True,
        note=(
            "Expected to pass only if the worker treats an already-stopped "
            "instance as idempotent success."
        ),
    )

    _call_case(
        server,
        reports,
        name="execute_in_session backend error: execute after destroy",
        tool="execute_in_session",
        arguments={"session_id": session_id, "command": args.success_command},
        expected_error=True,
        expect_backend_hit=True,
    )


def _run_busy_case(
    server: CountingMCPServer,
    reports: list[CaseReport],
    args: argparse.Namespace,
    session_id: str,
) -> None:
    first_result: dict[str, Any] | None = None

    def run_first() -> None:
        nonlocal first_result
        first_result = _call_tool(
            server,
            "execute_in_session",
            {"session_id": session_id, "command": args.busy_command},
        )

    thread = threading.Thread(target=run_first, daemon=True)
    thread.start()
    time.sleep(args.busy_delay_sec)

    _call_case(
        server,
        reports,
        name="execute_in_session backend error: session busy",
        tool="execute_in_session",
        arguments={"session_id": session_id, "command": args.success_command},
        expected_error=True,
        expect_backend_hit=True,
        note="This depends on busy-command staying active long enough.",
    )

    thread.join()
    reports.append(
        CaseReport(
            name="execute_in_session busy setup command completed",
            status="PASS" if first_result is not None else "FAIL",
            tool="execute_in_session",
            actual_error=_is_error(first_result or {}),
            output=_shorten(first_result),
        )
    )


def _skip_session_dependent_cases(
    reports: list[CaseReport],
    note: str,
) -> None:
    for name, tool in [
        ("execute_in_session success", "execute_in_session"),
        ("execute_in_session Tcl failure from data.result", "execute_in_session"),
        ("execute_in_session backend error: session busy", "execute_in_session"),
        ("destroy_session success", "destroy_session"),
        ("destroy_session stopped-session retry", "destroy_session"),
        ("execute_in_session backend error: execute after destroy", "execute_in_session"),
    ]:
        reports.append(CaseReport(name=name, status="SKIP", tool=tool, note=note))


def _add_known_unstable_skips(reports: list[CaseReport]) -> None:
    for name, note in [
        (
            "create_session backend error: INSTANCE_ID_CONFLICT",
            "create_session generates uuid.uuid4().hex internally; callers cannot choose a duplicate id.",
        ),
        (
            "create_session backend error: capacity/runtime start failure",
            "Requires worker capacity exhaustion or runtime startup failure.",
        ),
        (
            "create_session HTTP success but success=false",
            "Requires worker start to return HTTP 200 with success=false.",
        ),
        (
            "execute_in_session HTTP success but success=false",
            "Covered only if a real command/backend path returns success=false.",
        ),
        (
            "destroy_session HTTP success but status is not stopped",
            "Requires worker stop to return HTTP 200 with status other than stopped.",
        ),
    ]:
        reports.append(CaseReport(name=name, status="SKIP", note=note))


def _call_case(
    server: CountingMCPServer,
    reports: list[CaseReport],
    *,
    name: str,
    tool: str,
    arguments: dict[str, Any],
    expected_error: bool,
    expect_backend_hit: bool,
    validators: list[Any] | None = None,
    note: str = "",
) -> dict[str, Any]:
    before = len(server.backend_requests)
    output = _call_tool(server, tool, arguments)
    backend_requests = server.backend_requests[before:]
    backend_hit = bool(backend_requests)
    actual_error = _is_error(output)
    failures: list[str] = []
    if actual_error != expected_error:
        failures.append(f"expected_error={expected_error}, actual_error={actual_error}")
    if backend_hit != expect_backend_hit:
        failures.append(f"expect_backend_hit={expect_backend_hit}, backend_hit={backend_hit}")
    effective_validators = list(validators or [])
    if tool == "execute_in_session":
        effective_validators.insert(0, _validate_execute_tool_result)
    for validator in effective_validators:
        validation_error = validator(output)
        if validation_error:
            failures.append(validation_error)

    reports.append(
        CaseReport(
            name=name,
            status="FAIL" if failures else "PASS",
            tool=tool,
            backend_hit=backend_hit,
            expected_error=expected_error,
            actual_error=actual_error,
            input=_shorten(arguments),
            output=_shorten(output),
            backend_requests=_shorten(backend_requests),
            note="; ".join([item for item in [note, *failures] if item]),
        )
    )
    return output


def _call_tool(
    server: CountingMCPServer,
    tool: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    if tool == "create_session":
        return server._tool_create_session(arguments)
    if tool == "execute_in_session":
        return server._tool_execute_in_session(arguments)
    if tool == "destroy_session":
        return server._tool_destroy_session(arguments)
    raise ValueError(f"unknown tool: {tool}")


def _validate_content_text(result: dict[str, Any]) -> str | None:
    if _content_text(result):
        return None
    return "expected non-empty content[0].text"


def _validate_execute_tool_result(result: dict[str, Any]) -> str | None:
    structured = result.get("structuredContent")
    if not isinstance(structured, dict):
        return "expected execute_in_session structuredContent object"
    text = _content_text(result)
    if not text:
        return "expected execute_in_session content[0].text"
    for marker in ["state=", "exit_code=", "error=", "full_log_path=", "output_preview:"]:
        if marker not in text:
            return f"expected execute_in_session content text to contain {marker!r}"
    return None


def _validate_execute_success(result: dict[str, Any]) -> str | None:
    structured = result.get("structuredContent")
    if not isinstance(structured, dict):
        return "expected structuredContent object"
    expected = {
        "state": "SUCCEEDED",
        "exit_code": 0,
        "error": None,
    }
    for key, value in expected.items():
        if structured.get(key) != value:
            return f"expected structuredContent.{key}={value!r}"
    text = _content_text(result) or ""
    if "state=SUCCEEDED" not in text:
        return "expected execute_in_session content text to contain state=SUCCEEDED"
    if "error=null" not in text:
        return "expected execute_in_session content text to contain error=null"
    return None


def _validate_tcl_error(result: dict[str, Any]) -> str | None:
    structured = result.get("structuredContent")
    if not isinstance(structured, dict):
        return "expected structuredContent object"
    error = structured.get("error")
    if structured.get("state") != "FAILED":
        return "expected structuredContent.state='FAILED'"
    if structured.get("exit_code") != 1:
        return "expected structuredContent.exit_code=1"
    if not isinstance(error, dict) or error.get("code") != "TCL_ERROR":
        return "expected structuredContent.error.code='TCL_ERROR'"
    text = _content_text(result) or ""
    if "state=FAILED" not in text:
        return "expected execute_in_session content text to contain state=FAILED"
    if "error=TCL_ERROR:" not in text:
        return "expected execute_in_session content text to contain error=TCL_ERROR:"
    return None


def _is_error(result: dict[str, Any]) -> bool:
    return result.get("isError") is True


def _is_json_rpc_error(result: dict[str, Any] | None) -> bool:
    return isinstance(result, dict) and isinstance(result.get("error"), dict)


def _content_text(result: dict[str, Any]) -> str | None:
    content = result.get("content")
    if not isinstance(content, list) or not content:
        return None
    first = content[0]
    if not isinstance(first, dict):
        return None
    text = first.get("text")
    return text if isinstance(text, str) and text else None


def _shorten(value: Any, limit: int = 800) -> Any:
    if isinstance(value, str):
        if len(value) <= limit:
            return value
        return f"{value[:limit]}...<truncated {len(value) - limit} chars>"
    if isinstance(value, dict):
        return {key: _shorten(item, limit=limit) for key, item in value.items()}
    if isinstance(value, list):
        return [_shorten(item, limit=limit) for item in value]
    return value


def _print_report(reports: list[CaseReport]) -> None:
    print("\npandas MCP real-backend check report")
    print("=" * 44)
    for index, report in enumerate(reports, start=1):
        line = f"{index:02d}. [{report.status}] {report.name}"
        if report.tool:
            line += f" ({report.tool})"
        if report.backend_hit is not None:
            line += f" backend_hit={report.backend_hit}"
        if report.actual_error is not None:
            line += f" actual_error={report.actual_error}"
        print(line)
        if report.note:
            print(f"    note: {report.note}")
        if report.status == "FAIL" or report.actual_error:
            print(
                "    output: "
                + json.dumps(report.output, ensure_ascii=False, sort_keys=True)
            )
    totals = {status: 0 for status in ["PASS", "FAIL", "SKIP"]}
    for report in reports:
        totals[report.status] = totals.get(report.status, 0) + 1
    print("-" * 44)
    print(
        "summary: "
        + ", ".join(f"{status}={count}" for status, count in totals.items())
    )


if __name__ == "__main__":
    raise SystemExit(main())
