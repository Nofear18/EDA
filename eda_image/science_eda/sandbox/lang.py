"""Language string normalization."""

from __future__ import annotations


def normalize_lang(lang: str) -> str:
    m = lang.strip().lower().replace("-", "_")
    if m in ("python", "py"):
        return "python"
    if m in ("shell", "sh", "bash"):
        return "shell"
    if m in ("tcl", "tclsh"):
        return "tcl"
    if m in ("innovus", "cadence_innovus"):
        return "innovus"
    if m in ("primetime", "prime_time", "pt", "pt_shell", "synopsys_primetime"):
        return "primetime"
    raise ValueError(f"Unsupported language: {lang!r}")
