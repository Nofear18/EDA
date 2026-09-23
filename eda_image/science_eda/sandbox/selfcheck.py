"""Dependency-light smoke checks for the PrimeTime sandbox support.

Run from the ScienceEDA repository root:
PYTHONDONTWRITEBYTECODE=1 python3 -m science_eda.sandbox.selfcheck
"""

from __future__ import annotations

import os
import sys

# When this file is executed by path, Python puts the sandbox package itself on
# sys.path. That would shadow stdlib modules such as "types" with sandbox/types.py.
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if sys.path and os.path.abspath(sys.path[0] or os.curdir) == THIS_DIR:
    sys.path.pop(0)
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, "../../.."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import multiprocessing
import shutil
import tempfile
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# The host used for lightweight checks may not have PyYAML installed. Config loading is
# not exercised here, so a tiny import stub keeps the smoke check dependency-light.
if "yaml" not in sys.modules:
    yaml_stub = types.ModuleType("yaml")
    yaml_stub.safe_load = lambda _handle: {}
    sys.modules["yaml"] = yaml_stub

from science_eda.config import SandboxConfig
from science_eda.sandbox.client import ExecutionClient
from science_eda.sandbox.lang import normalize_lang


def main() -> None:
    tclsh = shutil.which("tclsh")
    if tclsh is None:
        raise RuntimeError("tclsh is required for sandbox selfcheck")
    root = tempfile.mkdtemp(prefix="science_eda_sandbox_selfcheck_")
    try:
        _check_non_pooled(root, tclsh)
        _check_pooled(root, tclsh)
        _check_pool_without_reset_discards(root, tclsh)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print("sandbox selfcheck: ok")


def _base_config(root: str, tclsh: str) -> SandboxConfig:
    cfg = SandboxConfig()
    cfg.client_mode = "import"
    cfg.sandbox_root = root
    cfg.timeout = 3
    cfg.ttl = 3600
    cfg.ttl_sweep_interval = 600
    cfg.primetime_bin = tclsh
    cfg.primetime_args = []
    cfg.primetime_startup_timeout = 3
    cfg.primetime_startup_tcl = r"""
proc redirect {flag variable_name body} {
    if {$flag ne "-variable"} {
        error "selfcheck redirect only supports -variable"
    }
    upvar 1 $variable_name destination
    set ::__science_eda_selfcheck_capture ""
    rename puts ::__science_eda_selfcheck_original_puts
    proc puts {args} {
        set value [lindex $args end]
        if {[llength $args] > 1 && [lindex $args 0] eq "-nonewline"} {
            append ::__science_eda_selfcheck_capture $value
        } else {
            append ::__science_eda_selfcheck_capture $value "\n"
        }
    }
    set code [catch {uplevel 1 $body} message options]
    rename puts {}
    rename ::__science_eda_selfcheck_original_puts puts
    set destination $::__science_eda_selfcheck_capture
    unset ::__science_eda_selfcheck_capture
    if {$code != 0} {
        return -options $options $message
    }
}
"""
    cfg.primetime_pool_healthcheck_tcl = "puts __PT_HEALTHCHECK__"
    return cfg


def _check_non_pooled(root: str, tclsh: str) -> None:
    assert normalize_lang("pt_shell") == "primetime"
    cfg = _base_config(root + "/non_pooled", tclsh)
    cfg.primetime_use_pool = False
    client = ExecutionClient(cfg)
    _force_fork_for_dependency_light_check(client)
    sid = client.create_session("primetime")
    try:
        assert client.execute("set x 41", "primetime", sid).exit_code == 0
        result = client.execute("incr x\nputs $x", "primetime", sid)
        assert result.exit_code == 0 and "42" in result.stdout
        failed = client.execute('error "boom"', "primetime", sid)
        assert failed.exit_code != 0 and "boom" in failed.stderr
        script = client.execute_tcl_script(
            "puts helper_ok",
            "primetime",
            sid,
        )
        assert script.exit_code == 0 and "helper_ok" in script.stdout
    finally:
        client.close_session(sid)
        client.close()


def _check_pooled(root: str, tclsh: str) -> None:
    cfg = _base_config(root + "/pooled", tclsh)
    cfg.primetime_use_pool = True
    cfg.primetime_pool_size = 1
    cfg.primetime_pool_prewarm = False
    cfg.primetime_pool_reset_tcl = "set __pt_reset_completed 1"
    client = ExecutionClient(cfg)
    _force_fork_for_dependency_light_check(client)
    first = client.create_session("primetime")
    try:
        first_pid = client.execute("puts [pid]", "primetime", first).stdout.strip()
    finally:
        client.close_session(first)
    second = client.create_session("pt")
    try:
        second_pid = client.execute("puts [pid]", "pt_shell", second).stdout.strip()
    finally:
        client.close_session(second)
        client.close()
    assert first_pid and second_pid == first_pid


def _check_pool_without_reset_discards(root: str, tclsh: str) -> None:
    cfg = _base_config(root + "/pooled_without_reset", tclsh)
    cfg.primetime_use_pool = True
    cfg.primetime_pool_size = 1
    cfg.primetime_pool_prewarm = False
    cfg.primetime_pool_replenish = False
    cfg.primetime_pool_reset_tcl = ""
    client = ExecutionClient(cfg)
    _force_fork_for_dependency_light_check(client)
    manager = client._backend._session_mgr
    first = client.create_session("primetime")
    first_worker_id = manager.get_session(first).primetime_worker_id
    client.close_session(first)
    assert manager.primetime_pool_state()["idle"] == 0

    second = client.create_session("primetime")
    try:
        second_worker_id = manager.get_session(second).primetime_worker_id
    finally:
        client.close_session(second)
        client.close()
    assert first_worker_id and second_worker_id and second_worker_id != first_worker_id


def _force_fork_for_dependency_light_check(client: ExecutionClient) -> None:
    """Avoid re-importing optional project dependencies in spawned smoke workers."""

    if "fork" not in multiprocessing.get_all_start_methods():
        return
    ctx = multiprocessing.get_context("fork")
    manager = client._backend._session_mgr
    manager._ctx = ctx
    manager._innovus_pool._ctx = ctx
    manager._primetime_pool._ctx = ctx


if __name__ == "__main__":
    main()
