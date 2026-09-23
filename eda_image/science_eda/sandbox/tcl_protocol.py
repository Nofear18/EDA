"""Tcl quoting and private-payload helpers for sandbox executors."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterable

from science_eda.sandbox.output import ExecutionProtocol


def tcl_double_quoted_word(value: str) -> str:
    """Return *value* as one Tcl double-quoted word without substitutions."""

    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("$", "\\$")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def prepare_tcl_source_payload(
    code: str,
    protocol: ExecutionProtocol,
    *,
    scratch_dir: str,
    file_prefix: str,
) -> tuple[str, tuple[str, ...]]:
    """Write independent user/control scripts and return a source command.

    Tcl counts braces inside quoted strings and comments while parsing a
    brace-delimited word. Embedding arbitrary user text in ``catch {}``
    therefore changes the syntax of otherwise valid Tcl such as ``puts "}"``.
    Sourcing a private payload file keeps the wrapper fixed while retaining
    the same catch/status/fence protocol.  The random control file has no
    name relationship with the payload and unlinks itself before sourcing
    user Tcl.  This is important twice over: ``[info script]`` reveals source
    paths, while a wrapper sent inline remains readable from non-blocking
    ``stdin`` before Tcl executes it.  In both cases user Tcl could otherwise
    recover the status/fence tokens and forge an early completion.
    """

    user_script_path = os.path.join(
        scratch_dir,
        f".science_eda_{file_prefix}_payload_{uuid.uuid4().hex}.user.tcl",
    )
    control_script_path = os.path.join(
        scratch_dir,
        f".science_eda_control_{uuid.uuid4().hex}.tcl",
    )
    command_script, fence_script = _build_tcl_catch_scripts(
        user_script_path,
        protocol,
    )
    control_script = (
        # ``source`` has already opened the control file, so unlinking it is
        # safe on the Unix platforms supported by the EDA tools.  Do this
        # before publishing start records or entering any user-controlled Tcl.
        "::file delete -force -- [::info script]\n"
        + command_script
        + fence_script
    )
    created: list[str] = []
    try:
        for path, content in (
            (user_script_path, code),
            (control_script_path, control_script),
        ):
            fd = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            created.append(path)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(content)
    except BaseException:
        remove_tcl_source_files(created)
        raise
    # Stdin carries no protocol material.  The opened control script removes
    # its directory entry before it sources the arbitrary payload, preserving
    # brace safety without leaving a readable token-bearing file.
    block = f"source {tcl_double_quoted_word(control_script_path)}\n"
    return block, (user_script_path, control_script_path)


def remove_tcl_source_files(paths: Iterable[str]) -> None:
    """Best-effort removal of one execution's private Tcl source files."""

    for path in paths:
        try:
            os.remove(path)
        except OSError:
            pass


def _build_tcl_catch_scripts(
    user_script_path: str,
    protocol: ExecutionProtocol,
) -> tuple[str, str]:
    status_prefix = protocol.status_prefix
    intermediate = protocol.intermediate_record
    auxiliary_prefix = protocol.auxiliary_status_prefix
    stdout_start = protocol.start_record("stdout")
    stderr_start = protocol.start_record("stderr")
    stdout_fence = protocol.fence_record("stdout")
    stderr_fence = protocol.fence_record("stderr")
    command_script = (
        f'puts stderr "{stderr_start}"\n'
        f"puts {{{stdout_start}}}\n"
        "flush stderr\n"
        "flush stdout\n"
        f"set __eda_user_script {tcl_double_quoted_word(user_script_path)}\n"
        "set __eda_code [catch {\n"
        "source $__eda_user_script\n"
        "} __eda_result __eda_opts]\n"
        "if {$__eda_code != 0} {\n"
        "    if {[dict exists $__eda_opts -errorinfo]} {\n"
        "        puts stderr [dict get $__eda_opts -errorinfo]\n"
        "    } else {\n"
        "        puts stderr $__eda_result\n"
        "    }\n"
        "}\n"
        f'puts "{status_prefix}$__eda_code"\n'
        f"puts {{{intermediate}}}\n"
        "flush stdout\n"
        "flush stderr\n"
    )
    fence_script = (
        # A second tiny catch transaction keeps compatibility with the fake
        # EDA shells used by batch tests while guaranteeing the stderr fence is
        # emitted after all command stderr. Its AUX status never replaces the
        # user status.
        "set __eda_code [catch {\n"
        f'puts stderr "{stderr_fence}"\n'
        "} __eda_result __eda_opts]\n"
        f'puts "{auxiliary_prefix}$__eda_code"\n'
        f"puts {{{stdout_fence}}}\n"
        "flush stderr\n"
        "flush stdout\n"
    )
    return command_script, fence_script
