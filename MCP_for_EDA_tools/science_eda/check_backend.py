"""Manual real-backend checks for the interactive MCP tool adapter.

This script intentionally is not a pytest test. It calls the MCP tool adapter
against a real interactive sandbox daemon endpoint and prints a compact report.
Run it from this directory, for example:

    python check_backend.py \
      --endpoint http://HOST:8765 \
      --workspace-path /path/visible/to/the/daemon
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

from interactive_mcp_server import InteractiveSandboxMCPServer


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
    note: str = ""


class CountingMCPServer(InteractiveSandboxMCPServer):
    def __init__(self, endpoint: str) -> None:
        super().__init__(endpoint)
        self.backend_requests: list[dict[str, Any]] = []

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.backend_requests.append(
            {
                "method": method,
                "path": path,
                "json_body": _shorten(json_body),
            }
        )
        return super()._request_json(method, path, json_body=json_body)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Call ScienceEDA MCP tools against a real interactive sandbox backend URL."
        )
    )
    parser.add_argument("--endpoint", required=True, help="Interactive sandbox URL.")
    parser.add_argument(
        "--workspace-path",
        required=True,
        help=(
            "Absolute workspace directory. The MCP adapter validates it locally; "
            "the daemon must also be able to use it as runtime cwd."
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
        default="__science_eda_mcp_unknown_command__",
        help="Command expected to produce an EDA/Tcl execution failure.",
    )
    parser.add_argument(
        "--skip-large-code",
        action="store_true",
        help="Skip the code-too-large HTTP error probe.",
    )
    parser.add_argument(
        "--large-code-bytes",
        type=int,
        default=1_200_000,
        help="Approximate command size for the CODE_TOO_LARGE probe.",
    )
    parser.add_argument(
        "--include-busy",
        action="store_true",
        help="Try to trigger SESSION_BUSY by running two executions concurrently.",
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

    server = CountingMCPServer(args.endpoint)
    reports: list[CaseReport] = []

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
            json.dumps([report.__dict__ for report in reports], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    failed = [report for report in reports if report.status == "FAIL"]
    return 1 if failed else 0


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
    )

    _call_case(
        server,
        reports,
        name="execute_in_session business failure",
        tool="execute_in_session",
        arguments={"session_id": session_id, "command": args.fail_command},
        expected_error=True,
        expect_backend_hit=True,
    )

    if args.skip_large_code:
        reports.append(
            CaseReport(
                name="execute_in_session backend error: code too large",
                status="SKIP",
                tool="execute_in_session",
                note="Skipped by --skip-large-code.",
            )
        )
    else:
        large_command = "puts large\n#" + ("x" * max(args.large_code_bytes, 1))
        _call_case(
            server,
            reports,
            name="execute_in_session backend error: code too large",
            tool="execute_in_session",
            arguments={"session_id": session_id, "command": large_command},
            expected_error=True,
            expect_backend_hit=True,
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
        name="destroy_session closed-session retry",
        tool="destroy_session",
        arguments={"instance_id": session_id},
        expected_error=False,
        expect_backend_hit=True,
        note="Expected to pass only if the daemon treats already-CLOSED as idempotent success.",
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

    second_result = _call_case(
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
    return second_result


def _skip_session_dependent_cases(
    reports: list[CaseReport],
    note: str,
) -> None:
    for name, tool in [
        ("execute_in_session success", "execute_in_session"),
        ("execute_in_session business failure", "execute_in_session"),
        ("execute_in_session backend error: code too large", "execute_in_session"),
        ("execute_in_session backend error: session busy", "execute_in_session"),
        ("destroy_session success", "destroy_session"),
        ("destroy_session closed-session retry", "destroy_session"),
        ("execute_in_session backend error: execute after destroy", "execute_in_session"),
    ]:
        reports.append(CaseReport(name=name, status="SKIP", tool=tool, note=note))


def _add_known_unstable_skips(reports: list[CaseReport]) -> None:
    for name, note in [
        (
            "create_session backend error: SESSION_ID_CONFLICT",
            "create_session now generates uuid.uuid4().hex internally; callers cannot choose a duplicate id.",
        ),
        (
            "create_session backend error: CREATE_CAPACITY_TIMEOUT",
            "Requires daemon capacity exhaustion or scheduler configuration.",
        ),
        (
            "create_session backend error: RUNTIME_START_FAILED",
            "Requires tool startup failure or daemon shutdown timing.",
        ),
        (
            "create_session HTTP success but non-ACTIVE state",
            "Requires daemon to return a successful create response with a non-ACTIVE state.",
        ),
        (
            "execute_in_session backend error: REQUEST_ID_CONFLICT",
            "MCP generates a fresh internal request_id for each execute call.",
        ),
        (
            "execute_in_session backend error: log quota exhausted",
            "Requires daemon/session log quota state.",
        ),
        (
            "execute_in_session business failure: TIMED_OUT",
            "MCP tool does not expose timeout_ms; use daemon configuration or a known timeout command.",
        ),
        (
            "execute_in_session business failure: LOST",
            "Requires killing or losing the runtime process during execution.",
        ),
        (
            "destroy_session HTTP success but non-CLOSED state",
            "Requires daemon to return a successful destroy response with a non-CLOSED state.",
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
    note: str = "",
) -> dict[str, Any]:
    before = len(server.backend_requests)
    output = _call_tool(server, tool, arguments)
    backend_hit = len(server.backend_requests) > before
    actual_error = _is_error(output)
    status = "PASS"
    if actual_error != expected_error:
        status = "FAIL"
    if backend_hit != expect_backend_hit:
        status = "FAIL"
    reports.append(
        CaseReport(
            name=name,
            status=status,
            tool=tool,
            backend_hit=backend_hit,
            expected_error=expected_error,
            actual_error=actual_error,
            input=_shorten(arguments),
            output=_shorten(output),
            note=note,
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


def _is_error(result: dict[str, Any]) -> bool:
    return result.get("isError") is True


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
    print("\nScienceEDA MCP real-backend check report")
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
