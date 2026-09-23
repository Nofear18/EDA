"""Portable best-effort birth identities for daemon-owned process leaders."""

from __future__ import annotations

import os
import subprocess
import sys
from ctypes import (
    CDLL,
    POINTER,
    Structure,
    byref,
    c_char,
    c_int,
    c_int32,
    c_uint32,
    c_uint64,
    sizeof,
)
from pathlib import Path


def read_process_identity(pid: int | None) -> str | None:
    """Return a value which changes when an integer PID is reused."""

    if pid is None or pid <= 0:
        return None
    proc_stat = Path(f"/proc/{pid}/stat")
    try:
        raw = proc_stat.read_text(encoding="ascii")
        after_name = raw[raw.rfind(")") + 2 :].split()
        start_ticks = after_name[19]
        try:
            boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(
                encoding="ascii"
            ).strip()
        except OSError:
            boot_id = "unknown-boot"
        return f"proc:{boot_id}:{start_ticks}"
    except (OSError, IndexError, ValueError):
        pass

    if sys.platform == "darwin":
        identity = _darwin_process_identity(pid)
        if identity is not None:
            return identity

    try:
        completed = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            check=False,
            capture_output=True,
            text=True,
            timeout=1.0,
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError):
        return None
    started = " ".join(completed.stdout.split())
    return None if completed.returncode != 0 or not started else f"ps:{started}"


def process_is_running(pid: int | None) -> bool:
    """Return false for missing processes and unreaped zombie leaders."""

    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False

    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        state = raw[raw.rfind(")") + 2 :].split(maxsplit=1)[0]
        return state != "Z"
    except (OSError, IndexError, ValueError):
        pass

    if sys.platform == "darwin":
        status = _darwin_process_status(pid)
        # libproc.h: SZOMB == 5.  On current macOS, PROC_PIDTBSDINFO returns
        # zero for an unreaped SIGKILLed child even though signal 0 still
        # succeeds; either representation means it is no longer runnable.
        return status is not None and status != 5

    try:
        completed = subprocess.run(
            ["ps", "-o", "stat=", "-p", str(pid)],
            check=False,
            capture_output=True,
            text=True,
            timeout=1.0,
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError):
        # signal 0 already proved that the PID exists; an unavailable process
        # status probe must stay conservative.
        return True
    state = completed.stdout.strip()
    if completed.returncode != 0 or not state:
        return True
    return not state.startswith("Z")


class _ProcBSDInfo(Structure):
    _fields_ = [
        ("pbi_flags", c_uint32),
        ("pbi_status", c_uint32),
        ("pbi_xstatus", c_uint32),
        ("pbi_pid", c_uint32),
        ("pbi_ppid", c_uint32),
        ("pbi_uid", c_uint32),
        ("pbi_gid", c_uint32),
        ("pbi_ruid", c_uint32),
        ("pbi_rgid", c_uint32),
        ("pbi_svuid", c_uint32),
        ("pbi_svgid", c_uint32),
        ("pbi_rfu_1", c_uint32),
        ("pbi_comm", c_char * 16),
        ("pbi_name", c_char * 32),
        ("pbi_nfiles", c_uint32),
        ("pbi_pgid", c_uint32),
        ("pbi_pjobc", c_uint32),
        ("e_tdev", c_uint32),
        ("e_tpgid", c_uint32),
        ("pbi_nice", c_int32),
        ("pbi_start_tvsec", c_uint64),
        ("pbi_start_tvusec", c_uint64),
    ]


def _darwin_process_identity(pid: int) -> str | None:
    try:
        library = CDLL("/usr/lib/libproc.dylib")
        proc_pidinfo = library.proc_pidinfo
        proc_pidinfo.argtypes = [c_int, c_int, c_uint64, POINTER(_ProcBSDInfo), c_int]
        proc_pidinfo.restype = c_int
        info = _ProcBSDInfo()
        copied = proc_pidinfo(pid, 3, 0, byref(info), sizeof(info))
        if copied != sizeof(info) or int(info.pbi_pid) != pid:
            return None
        return f"darwin:{int(info.pbi_start_tvsec)}:{int(info.pbi_start_tvusec)}"
    except (OSError, AttributeError, ValueError):
        return None


def _darwin_process_status(pid: int) -> int | None:
    try:
        library = CDLL("/usr/lib/libproc.dylib")
        proc_pidinfo = library.proc_pidinfo
        proc_pidinfo.argtypes = [c_int, c_int, c_uint64, POINTER(_ProcBSDInfo), c_int]
        proc_pidinfo.restype = c_int
        info = _ProcBSDInfo()
        copied = proc_pidinfo(pid, 3, 0, byref(info), sizeof(info))
        if copied != sizeof(info) or int(info.pbi_pid) != pid:
            return None
        return int(info.pbi_status)
    except (OSError, AttributeError, ValueError):
        return None


def process_group_exists(process_group_id: int | None) -> bool:
    if process_group_id is None or process_group_id <= 0:
        return False
    try:
        if hasattr(os, "killpg"):
            os.killpg(process_group_id, 0)
        else:
            os.kill(process_group_id, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


def recorded_group_state(
    process_group_id: int | None,
    expected_identity: str | None,
) -> str:
    """Return ``dead``, ``owned``, or ``unknown`` for a recorded process group.

    A group may remain after its leader exits.  In that case the integer PGID
    cannot be reused while members remain, so it still belongs to the recorded
    runtime.  If a new leader exists, its birth identity must match before the
    daemon may signal the group.
    """

    if not process_group_exists(process_group_id):
        return "dead"
    current_identity = read_process_identity(process_group_id)
    if current_identity is None:
        return "owned"
    if expected_identity is None:
        return "unknown"
    return "owned" if current_identity == expected_identity else "dead"
