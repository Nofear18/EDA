from __future__ import annotations

import ipaddress
import math
import os
import tomli as tomllib
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, TypeVar

import yaml

CompatibilityHook = Callable[[Mapping[str, Any]], None]
T = TypeVar("T")

DEFAULT_SANDBOX_HTTP_PORT = 8765
DEFAULT_SANDBOX_ENDPOINT = f"http://localhost:{DEFAULT_SANDBOX_HTTP_PORT}"
DEFAULT_CODEX_PROVIDER_PORT = 8766


def _is_loopback_host(host: str) -> bool:
    if not isinstance(host, str):
        return False
    # Interactive is an unauthenticated trusted-local control plane.  Validate
    # the exact value later handed to uvicorn instead of resolving hostnames or
    # accepting a stripped variant that the socket layer will not bind.
    if not host or host != host.strip():
        return False
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _require_int_range(
    name: str,
    value: object,
    *,
    minimum: int,
    maximum: int | None = None,
) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        if maximum is None:
            raise ValueError(f"{name} must be >= {minimum}")
        raise ValueError(f"{name} must be between {minimum} and {maximum}")


def load_config_mapping(path: str | Path) -> dict[str, Any]:
    """Load a YAML/TOML config file and return its top-level mapping."""

    config_path = Path(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    suffix = config_path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        with config_path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
    elif suffix == ".toml":
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle) or {}
    else:
        raise ValueError(
            "Unsupported config file format. Expected .yaml, .yml, or .toml.",
        )

    if not isinstance(raw, Mapping):
        raise ValueError("Config file root must be a mapping/object.")

    return dict(raw)


def merge_dataclass_config(
    instance: T,
    overrides: Mapping[str, Any],
    *,
    compatibility_hook: CompatibilityHook | None = None,
) -> T:
    """Return a new dataclass instance with nested mapping overrides applied."""

    if not is_dataclass(instance):
        raise TypeError(f"Expected dataclass instance, got: {type(instance)!r}")
    if not isinstance(overrides, Mapping):
        raise TypeError(f"Expected mapping overrides, got: {type(overrides)!r}")

    if compatibility_hook is not None:
        compatibility_hook(overrides)

    updated: dict[str, Any] = {}
    for field_info in fields(instance):
        current_value = getattr(instance, field_info.name)
        if field_info.name not in overrides:
            updated[field_info.name] = current_value
            continue

        override_value = overrides[field_info.name]
        if is_dataclass(current_value) and isinstance(override_value, Mapping):
            updated[field_info.name] = merge_dataclass_config(
                current_value,
                override_value,
            )
        else:
            updated[field_info.name] = override_value

    return type(instance)(**updated)


def load_dataclass_config(
    config_cls: type[T],
    path: str | Path,
    *,
    compatibility_hook: CompatibilityHook | None = None,
) -> T:
    """Instantiate a dataclass config with file overrides applied."""

    return merge_dataclass_config(
        config_cls(),
        load_config_mapping(path),
        compatibility_hook=compatibility_hook,
    )


@dataclass
class LLMConfig:
    provider: str = "openai"       # 主要专注 openai 协议（兼容 vLLM 等）
    api_key: str = "codex-local"   # 使用 SCIENCE_EDA_LLM_API_KEY / OPENAI_API_KEY 覆盖
    model: str = "codex-local"
    # Local Codex OpenAI-compatible provider
    base_url: str = "http://127.0.0.1:8766/v1"


@dataclass
class CodexProviderConfig:
    """Configuration for the local OpenAI-compatible Codex provider."""

    api_key: str = ""
    host: str = "127.0.0.1"
    port: int = DEFAULT_CODEX_PROVIDER_PORT
    served_model: str = "codex-local"
    codex_model: str = ""
    codex_bin: str = "codex"
    cwd: str = field(default_factory=os.getcwd)
    timeout: int = 300
    max_concurrency: int = 1


@dataclass
class SandboxClientConfig:
    """Narrow HTTP client settings for remote sandbox access."""

    endpoint: str = DEFAULT_SANDBOX_ENDPOINT
    timeout: int = 300


@dataclass
class SandboxConfig:
    # 客户端/服务端通信模式
    client_mode: str = "import"    # import / http
    endpoint: str = DEFAULT_SANDBOX_ENDPOINT
    server_host: str = "127.0.0.1" # HTTP sandbox 服务监听地址
    server_port: int = DEFAULT_SANDBOX_HTTP_PORT  # HTTP sandbox 服务监听端口
    legacy_batch_enabled: bool = True  # 挂载 legacy batch routes 并启用工具 pool

    # Worker 隔离执行模式与运行资源
    worker_mode: str = "process"   # process / docker（未来扩展）
    timeout: int = 30
    memory_mb: int = 512           # Worker 进程树内存上限（见 docs/design_sandbox.md）
    cpus: float = 1.0              # CPU 配额（实现侧可映射为 cgroup cpu.max 等）
    max_pids: int = 256            # Worker 进程树最大进程数，缓解 fork 炸弹
    network_mode: str = "allow"   # allow / none（是否允许外联；详见 design_sandbox）

    # Session 保活与回收 (对应设计文档)
    ttl: int = 3600                # Session 空闲最大秒数，超时自动回收
    ttl_sweep_interval: int = 30   # 后台扫描间隔（秒）
    max_retries: int = 3           # 进程级崩溃时的最大重试/重放 (Replay) 次数
    sandbox_root: str = ""         # Session 工作目录根；空则使用系统临时目录下 eda_sandbox

    # 执行器可执行路径配置 (对应设计文档：多语言长驻策略)
    python_path: str = "python"
    shell_path: str = "/bin/bash"  # 真实的 bash / sh 路径
    tcl_shell: str = "tclsh"       # 支持配置为特定 EDA 工具的 tclsh，如 /usr/local/bin/tclsh8.5
    innovus_bin: str = "innovus"
    innovus_args: list[str] = field(default_factory=lambda: ["-nowin"])
    innovus_startup_tcl: str = ""
    innovus_startup_timeout: int = 300
    innovus_replay_policy: str = "none"
    innovus_pool_size: int = 1
    innovus_pool_reuse: bool = True
    innovus_pool_prewarm: bool = True
    innovus_pool_prewarm_concurrency: int = 16
    innovus_pool_replenish: bool = True
    innovus_pool_queue_timeout: float = 300.0
    innovus_pool_reset_tcl: str = "freeDesign"
    innovus_pool_clean_tcl_state: bool = True
    innovus_pool_healthcheck_tcl: str = "puts __SCIENCE_EDA_POOL_HEALTHCHECK__"
    innovus_pool_incomplete_reset_policy: str = "reuse_with_retry"
    innovus_pool_wait_log_interval: int = 30
    innovus_snapshot_cache_root: str = ""  # sandbox 侧已同步的 Innovus snapshots 根目录

    # PrimeTime 长驻工具与可选物理 worker pool 配置
    primetime_bin: str = "pt_shell"
    primetime_args: list[str] = field(default_factory=list)
    primetime_startup_tcl: str = ""
    primetime_startup_timeout: int = 300
    primetime_replay_policy: str = "none"
    primetime_use_pool: bool = False
    primetime_pool_size: int = 1
    primetime_pool_prewarm: bool = False
    primetime_pool_prewarm_concurrency: int = 16
    primetime_pool_replenish: bool = True
    primetime_pool_queue_timeout: float = 300.0
    primetime_pool_reset_tcl: str = ""
    primetime_pool_clean_tcl_state: bool = True
    primetime_pool_healthcheck_tcl: str = "puts __SCIENCE_EDA_PT_POOL_HEALTHCHECK__"
    primetime_pool_wait_log_interval: int = 30

    http_thread_limit: int = 0

    # Trusted-local interactive control plane. Empty derived-path fields are
    # resolved below interactive_state_root by the interactive workspace layer.
    interactive_enabled: bool = True
    interactive_module_shell_path: str = "/bin/csh"
    interactive_state_root: str = "~/.science_eda/interactive"
    interactive_runtime_root: str = ""
    interactive_log_root: str = ""
    interactive_registry_path: str = ""

    # Interactive runtimes never borrow batch pool workers or capacity.
    interactive_innovus_capacity: int = 1
    interactive_primetime_capacity: int = 1
    interactive_create_capacity_timeout: float = 300.0
    interactive_execute_timeout: int = 60 * 60
    interactive_max_sessions: int = 100

    # Runtime/process retention and durable-record retention.
    interactive_runtime_idle_ttl: int = 48 * 60 * 60
    interactive_session_retention_ttl: int = 0
    interactive_create_request_retention_ttl: int = 30 * 24 * 60 * 60
    interactive_audit_retention_ttl: int = 90 * 24 * 60 * 60
    interactive_sweep_interval: int = 30
    interactive_session_event_max_records: int = 10_000

    # Bounded request, output, log, and history limits.
    interactive_max_code_bytes: int = 1024 * 1024
    interactive_output_preview_bytes: int = 8 * 1024
    interactive_output_preview_max_bytes: int = 32 * 1024
    interactive_execution_log_max_bytes: int = 1024 * 1024 * 1024
    interactive_session_log_max_bytes: int = 4 * 1024 * 1024 * 1024
    interactive_reader_chunk_bytes: int = 64 * 1024
    interactive_history_default_page_size: int = 20
    interactive_history_max_page_size: int = 100
    interactive_history_page_max_bytes: int = 4 * 1024 * 1024

    # ``None`` preserves the current process environment for compatibility;
    # an explicit list (including []) restricts inheritance to those names.
    innovus_interactive_healthcheck_tcl: str = (
        "puts __SCIENCE_EDA_INTERACTIVE_HEALTHCHECK__"
    )
    primetime_interactive_healthcheck_tcl: str = (
        "puts __SCIENCE_EDA_PT_INTERACTIVE_HEALTHCHECK__"
    )
    innovus_interactive_env_allowlist: list[str] | None = None
    primetime_interactive_env_allowlist: list[str] | None = None

    def __post_init__(self) -> None:
        self.validate_interactive()

    def validate_interactive(self) -> None:
        """Validate HTTP interface and interactive control-plane settings.

        This method is public because tests and legacy callers commonly mutate a
        ``SandboxConfig`` after construction. Daemon startup must call it again
        before mounting interactive routes.
        """

        if not isinstance(self.legacy_batch_enabled, bool):
            raise ValueError("legacy_batch_enabled must be a boolean")
        if not isinstance(self.interactive_enabled, bool):
            raise ValueError("interactive_enabled must be a boolean")
        if self.interactive_enabled and not _is_loopback_host(self.server_host):
            raise ValueError(
                "interactive sandbox requires a loopback server_host; "
                "set interactive_enabled=false for a non-local legacy batch server"
            )
        _require_int_range("server_port", self.server_port, minimum=1, maximum=65535)

        for name in (
            "interactive_state_root",
            "interactive_runtime_root",
            "interactive_log_root",
            "interactive_registry_path",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or "\x00" in value:
                raise ValueError(f"{name} must be a path string without NUL bytes")
        if not self.interactive_state_root.strip():
            raise ValueError("interactive_state_root must not be empty")
        if (
            not isinstance(self.interactive_module_shell_path, str)
            or not self.interactive_module_shell_path.strip()
            or "\x00" in self.interactive_module_shell_path
        ):
            raise ValueError(
                "interactive_module_shell_path must be a non-empty path string "
                "without NUL bytes"
            )

        for name in (
            "interactive_innovus_capacity",
            "interactive_primetime_capacity",
            "interactive_execute_timeout",
            "interactive_max_sessions",
            "interactive_runtime_idle_ttl",
            "interactive_create_request_retention_ttl",
            "interactive_audit_retention_ttl",
            "interactive_sweep_interval",
            "interactive_session_event_max_records",
            "interactive_max_code_bytes",
            "interactive_output_preview_bytes",
            "interactive_output_preview_max_bytes",
            "interactive_execution_log_max_bytes",
            "interactive_session_log_max_bytes",
            "interactive_reader_chunk_bytes",
            "interactive_history_default_page_size",
            "interactive_history_max_page_size",
            "interactive_history_page_max_bytes",
        ):
            _require_int_range(name, getattr(self, name), minimum=1)
        _require_int_range(
            "interactive_session_retention_ttl",
            self.interactive_session_retention_ttl,
            minimum=0,
        )
        if (
            isinstance(self.interactive_create_capacity_timeout, bool)
            or not isinstance(self.interactive_create_capacity_timeout, (int, float))
            or not math.isfinite(float(self.interactive_create_capacity_timeout))
            or self.interactive_create_capacity_timeout <= 0
        ):
            raise ValueError(
                "interactive_create_capacity_timeout must be finite and positive"
            )

        if self.interactive_output_preview_bytes > self.interactive_output_preview_max_bytes:
            raise ValueError(
                "interactive_output_preview_bytes must not exceed "
                "interactive_output_preview_max_bytes"
            )
        if self.interactive_execution_log_max_bytes > self.interactive_session_log_max_bytes:
            raise ValueError(
                "interactive_execution_log_max_bytes must not exceed "
                "interactive_session_log_max_bytes"
            )
        if (
            self.interactive_history_default_page_size
            > self.interactive_history_max_page_size
        ):
            raise ValueError(
                "interactive_history_default_page_size must not exceed "
                "interactive_history_max_page_size"
            )
        minimum_history_budget = (
            self.interactive_max_code_bytes
            + self.interactive_output_preview_max_bytes
            + 64 * 1024
        )
        if self.interactive_history_page_max_bytes < minimum_history_budget:
            raise ValueError(
                "interactive_history_page_max_bytes must fit one maximum-sized "
                "history record"
            )

        for name in (
            "innovus_interactive_healthcheck_tcl",
            "primetime_interactive_healthcheck_tcl",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty Tcl command")
        for name in (
            "innovus_interactive_env_allowlist",
            "primetime_interactive_env_allowlist",
        ):
            value = getattr(self, name)
            if value is None:
                continue
            if not isinstance(value, list) or any(
                not isinstance(item, str) or not item or "=" in item or "\x00" in item
                for item in value
            ):
                raise ValueError(
                    f"{name} must be null or a list of non-empty environment names"
                )
            if len(set(value)) != len(value):
                raise ValueError(f"{name} must not contain duplicate names")


@dataclass
class DataConfig:
    data_dir: str = "./data"
    format: str = "jsonl"          # jsonl / parquet


@dataclass
class LogConfig:
    level: str = "INFO"            # DEBUG 时记录完整 prompt/response
    log_file: str = ""             # 为空则只输出到 console
    log_llm_calls: bool = True     # 是否记录 LLM 调用详情
    log_executions: bool = True    # 是否记录代码执行详情
    track_tokens: bool = True      # 是否追踪 token 用量及费用


@dataclass
class ScienceEDAConfig:
    llm: LLMConfig = field(default_factory=LLMConfig)
    codex: CodexProviderConfig = field(default_factory=CodexProviderConfig)
    sandbox: SandboxConfig = field(default_factory=SandboxConfig)
    data: DataConfig = field(default_factory=DataConfig)
    log: LogConfig = field(default_factory=LogConfig)

    @classmethod
    def from_env(cls) -> "ScienceEDAConfig":
        """从环境变量加载（SCIENCE_EDA_ 前缀）"""

        llm_overrides: dict[str, Any] = {}
        for env_name, field_name in (
            ("SCIENCE_EDA_LLM_API_KEY", "api_key"),
            ("SCIENCE_EDA_LLM_MODEL", "model"),
            ("SCIENCE_EDA_LLM_BASE_URL", "base_url"),
            ("SCIENCE_EDA_LLM_PROVIDER", "provider"),
        ):
            if env_name in os.environ:
                llm_overrides[field_name] = os.environ[env_name]

        codex_overrides: dict[str, Any] = {}
        for env_name, field_name in (
            ("SCIENCE_EDA_CODEX_API_KEY", "api_key"),
            ("SCIENCE_EDA_CODEX_HOST", "host"),
            ("SCIENCE_EDA_CODEX_SERVED_MODEL", "served_model"),
            ("SCIENCE_EDA_CODEX_MODEL", "codex_model"),
            ("SCIENCE_EDA_CODEX_BIN", "codex_bin"),
            ("SCIENCE_EDA_CODEX_CWD", "cwd"),
        ):
            if env_name in os.environ:
                codex_overrides[field_name] = os.environ[env_name]

        for env_name, field_name in (
            ("SCIENCE_EDA_CODEX_PORT", "port"),
            ("SCIENCE_EDA_CODEX_TIMEOUT", "timeout"),
            ("SCIENCE_EDA_CODEX_MAX_CONCURRENCY", "max_concurrency"),
        ):
            if env_name in os.environ:
                codex_overrides[field_name] = int(os.environ[env_name])

        overrides: dict[str, Any] = {}
        if llm_overrides:
            overrides["llm"] = llm_overrides
        if codex_overrides:
            overrides["codex"] = codex_overrides

        if not overrides:
            return cls()

        return cls().merge(overrides)

    @classmethod
    def from_file(cls, path: str | Path) -> "ScienceEDAConfig":
        """从 YAML/TOML 文件加载。"""

        return load_dataclass_config(cls, path)

    def merge(self, overrides: Mapping[str, Any]) -> "ScienceEDAConfig":
        """合并覆盖配置（代码传参优先）。"""

        return merge_dataclass_config(self, overrides)
