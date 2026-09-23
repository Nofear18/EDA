"""Validated daemon-owned configuration for interactive EDA tools."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Mapping, Sequence

from science_eda.config import SandboxConfig
from science_eda.exceptions import InteractiveInvalidRequestError


SUPPORTED_TOOL_KINDS = ("innovus", "primetime")
_TOOL_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_CSH_MODULE_LAUNCH_SCRIPT = 'ma "$argv[1]" && shift argv && exec $argv:q'


@dataclass(frozen=True)
class InteractiveToolConfig:
    tool_kind: str
    lang: str
    executable: str
    args: tuple[str, ...]
    startup_tcl: str
    startup_timeout: float
    healthcheck_tcl: str
    env_allowlist: tuple[str, ...] | None

    def build_environment(
        self,
        source: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        """Build the child environment without ever persisting its values."""

        environment = source if source is not None else os.environ
        if self.env_allowlist is None:
            return {str(key): str(value) for key, value in environment.items()}
        return {
            name: str(environment[name])
            for name in self.env_allowlist
            if name in environment
        }


def get_interactive_tool_config(
    config: SandboxConfig,
    tool_kind: str,
) -> InteractiveToolConfig:
    """Return one strict tool definition owned by the daemon configuration."""

    config.validate_interactive()
    normalized = str(tool_kind).strip().lower()
    if normalized == "innovus":
        result = InteractiveToolConfig(
            tool_kind="innovus",
            lang="innovus",
            executable=config.innovus_bin,
            args=tuple(config.innovus_args),
            startup_tcl=config.innovus_startup_tcl,
            startup_timeout=float(config.innovus_startup_timeout),
            healthcheck_tcl=config.innovus_interactive_healthcheck_tcl,
            env_allowlist=(
                None
                if config.innovus_interactive_env_allowlist is None
                else tuple(config.innovus_interactive_env_allowlist)
            ),
        )
    elif normalized == "primetime":
        result = InteractiveToolConfig(
            tool_kind="primetime",
            lang="primetime",
            executable=config.primetime_bin,
            args=tuple(config.primetime_args),
            startup_tcl=config.primetime_startup_tcl,
            startup_timeout=float(config.primetime_startup_timeout),
            healthcheck_tcl=config.primetime_interactive_healthcheck_tcl,
            env_allowlist=(
                None
                if config.primetime_interactive_env_allowlist is None
                else tuple(config.primetime_interactive_env_allowlist)
            ),
        )
    else:
        raise InteractiveInvalidRequestError(
            f"unsupported interactive tool_kind: {tool_kind!r}",
            details={"supported_tool_kinds": list(SUPPORTED_TOOL_KINDS)},
        )
    _validate_tool_config(result)
    return result


def interactive_capacities(config: SandboxConfig) -> dict[str, int]:
    config.validate_interactive()
    return {
        "innovus": config.interactive_innovus_capacity,
        "primetime": config.interactive_primetime_capacity,
    }


def validate_tool_version(value: str | None) -> str | None:
    """Validate an optional caller-selected EDA module version."""

    if value is None:
        return None
    if not isinstance(value, str) or not _TOOL_VERSION_RE.fullmatch(value):
        raise InteractiveInvalidRequestError(
            "version must be 1-128 ASCII letters, digits, dots, underscores, "
            "or hyphens and must start with a letter or digit"
        )
    return value


def module_activated_tool_command(
    module_shell_path: str,
    tool_kind: str,
    version: str,
    executable: str,
    args: Sequence[str],
) -> tuple[str, tuple[str, ...]]:
    """Activate an EDA module in csh, then replace csh with the tool binary."""

    normalized_version = validate_tool_version(version)
    if normalized_version is None:  # pragma: no cover - signature requires a value
        raise ValueError("version is required for module activation")
    if tool_kind not in SUPPORTED_TOOL_KINDS:
        raise ValueError(f"unsupported interactive tool_kind: {tool_kind!r}")
    module_spec = f"{tool_kind}/{normalized_version}"
    return (
        module_shell_path,
        (
            "-c",
            _CSH_MODULE_LAUNCH_SCRIPT,
            module_spec,
            executable,
            *args,
        ),
    )


def _validate_tool_config(config: InteractiveToolConfig) -> None:
    if not isinstance(config.executable, str) or not config.executable.strip():
        raise ValueError(f"{config.tool_kind} interactive executable must not be empty")
    if any(not isinstance(argument, str) or "\x00" in argument for argument in config.args):
        raise ValueError(f"{config.tool_kind} interactive args must be strings")
    if config.startup_timeout <= 0:
        raise ValueError(f"{config.tool_kind} interactive startup timeout must be positive")
    if not config.healthcheck_tcl.strip():
        raise ValueError(f"{config.tool_kind} interactive healthcheck must not be empty")
