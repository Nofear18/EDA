# Sandbox（代码沙箱）

`science_eda.sandbox` 提供多语言、带会话状态的执行能力。现有 batch API 可以通过 **Import
后端** 直接调用，或通过 **HTTP 后端** 连接独立 FastAPI 服务。interactive v1 另在同一 daemon 上提供
loopback TCP 版本化 HTTP API，让每个 `WorkspaceSession` 独占一个全新 Innovus/PrimeTime 进程。

## 模块一览

| 文件 | 职责 |
|------|------|
| `client.py` | `ExecutionClient`、`ImportBackend`、`HTTPBackend`、`StatefulSession`、`execute_tcl_script()` |
| `session.py` | `Session`、`SessionManager`、tool session adapters：会话注册、Worker 生命周期、空闲 TTL 回收、崩溃后重放 |
| `tool_pool.py` | bounded physical worker pool 公共基类：lease 队列、预热/补池 supervisor、worker handshake、execute/reset/discard |
| `innovus_pool.py` | `InnovusWorkerPool`：Innovus pool 配置、reset/healthcheck、`IMPSYC-6379` incomplete-reset 策略 |
| `primetime_pool.py` | `PrimeTimeWorkerPool`：PrimeTime pool 配置、reset/healthcheck、无 reset Tcl 时 discard 策略 |
| `worker.py` | 单一子进程入口：资源限制、与主进程 PIPE 通信、调度执行器、tool session prepare/reset |
| `executor.py` | `PythonExecutor`、`ShellExecutor`、`TCLExecutor`、`TclToolExecutor`、`InnovusExecutor`、`PrimeTimeExecutor` 及 `build_executor` |
| `tcl_protocol.py` | Tcl word quoting、每次 execution 的随机 `0600` payload/control source 文件与 catch/status/fence wrapper 构造、清理 |
| `output.py` | 固定块二进制输出协议、双 fence、控制记录过滤、batch 内存 sink 和 interactive 日志/preview sink |
| `runtime_metadata.py` | batch runtime instance、process-lost、timeout 与恢复结果 metadata helper |
| `server.py` | `create_app()`：FastAPI HTTP 服务；`main()` 用 uvicorn 启动；`/pool_state` 同时报告 Innovus / PrimeTime pool |
| `file_io.py` | Session-scoped 文件读写、上传落盘与安全 zip 解压 |
| `types.py` | `ExecutionResult`（stdout / stderr / exit_code / duration / metadata） |
| `lang.py` | `normalize_lang()`：语言别名归一化 |
| `classifier.py` | `ErrorClassifier` / `ErrorCategory`：区分用户代码错误与进程级故障（触发重放） |
| `interactive/` | SQLite registry、workspace/scratch 边界、专属 runtime、独立 capacity scheduler、service（含 execution/lifecycle mixin）和 `/v1` schema/routes |

公开 API 见 `science_eda.sandbox.__init__` 的 `__all__`。

## 架构要点

- **会话**：每个 logical session 有独立工作目录。Python / Shell / TCL 各自拥有一条 **spawn** 起的 Python Worker；Innovus logical session 默认从 bounded physical worker pool lease 一个长驻 Innovus worker。PrimeTime logical session 默认每个 session 启动一个长驻 `pt_shell`，也可以启用 bounded physical worker pool。`SessionManager` 通过 tool session adapter 分派 pooled tool 的 lease / execute / release 路径。
- **串行执行**：同一 Session 内的 `execute` 会被串行化，避免多线程/多请求同时写入同一 Worker pipe 或长驻解释器 stdin 导致输出串扰。
- **语言**：
  - **Python**：在 Worker 内 `exec`，命名空间跨次执行保持。
  - **Shell / TCL**：Worker 内 `Popen` 长驻 `bash`（可配置路径）或 `tclsh`，通过 stdin 写入代码，以每次 execution 唯一的 status record 和 stdout/stderr 双 fence 解帧，并回传真实退出码（见 `executor.py`）。
  - **Innovus / PrimeTime**：physical worker 内长驻 `innovus -nowin` 或 `pt_shell`（可配置命令与参数），每次执行把用户 Tcl 和 control wrapper 写入 runtime scratch 中两个不可相互推导的随机 `0600` 文件；stdin 只发送 `source` 命令，wrapper 先 unlink 自身，再在 `catch` 中 `source` 用户脚本，并通过唯一 status record 和双 fence 返回结果。`innovus` 和 `primetime` 不是普通 `tcl` 的别名；两者默认禁用 sample history replay。
- **启动握手**：Worker 创建后会先完成执行器初始化并向 `SessionManager` 回报 ready。Innovus physical worker 启动时只启动工具进程；每次 lease 给 logical session 时切换 worker cwd 和 Innovus cwd 到该 session 工作目录，再执行 `innovus_startup_tcl`。PrimeTime 非 pool session 在 worker startup 阶段执行 `primetime_startup_tcl`；pool session 每次 lease 时切换 cwd 并执行 startup Tcl。对应 startup timeout 覆盖工具启动、license checkout、cwd 切换和 startup Tcl。
- **Tool pool**：Innovus 和 PrimeTime pool 共用 `tool_pool.py` 中的 bounded worker pool 生命周期：排队、预热/补池、启动握手、session prepare、execute/reset、discard 和 close-all。各工具模块只保留配置、日志文案、reset 策略和 healthcheck 差异。
- **Innovus pool**：`innovus_pool_size` 是 physical Innovus 进程硬上限，默认 `1`。HTTP server 启动时默认按 `innovus_pool_prewarm=true` 在后台预热到该上限，`/is_alive` 不等待预热完成；预热与补池并发由 `innovus_pool_prewarm_concurrency` 限制，默认 `16`。`innovus_pool_replenish=true` 时，worker 被丢弃后后台 supervisor 会自动把 pool 补回目标容量；`innovus_pool_reuse=false` 时 logical session 关闭会直接丢弃进程，并把补池目标提升到 `innovus_pool_size`。Import 后端不会自动启动 supervisor。当 active + idle + starting 达到上限时，新的 `create_session("innovus")` 会阻塞等待 worker 归还或被丢弃；等待超过 `innovus_pool_queue_timeout` 会失败，设为 `0` 或负数可恢复无限等待，并按 `innovus_pool_wait_log_interval` 周期记录等待状态。physical worker 启动后会捕获 Tcl 基线，关闭 logical session 时默认在 `freeDesign` 后删除新增 globals/procs/namespaces/aliases、恢复 `env`，始终切回 physical cwd 并执行 healthcheck。如果 reset 输出 Innovus `IMPSYC-6379`，默认把 worker 标记为 risky 并继续复用；下一 logical session 的第一次执行若发生进程级失败，会丢弃该 worker、换新 worker 并重试该第一次执行一次。
- **PrimeTime pool**：默认 `primetime_use_pool=false`，即每个 logical session 拥有独立 `pt_shell` worker。启用 pool 后，`primetime_pool_size` 限制 physical PrimeTime 进程数量；`primetime_pool_prewarm`、`primetime_pool_replenish`、`primetime_pool_queue_timeout`、`primetime_pool_wait_log_interval` 与 Innovus pool 语义一致。PrimeTime pool 默认没有 reset Tcl，关闭 logical session 时会丢弃 worker，只有配置了经过站点验证的 `primetime_pool_reset_tcl` 且 healthcheck 成功时才回到 idle pool。
- **容错**：Worker 异常退出或通信失败时，`ErrorClassifier` 判为 `PROCESS` 类；`SessionManager` 可 **终止并重启 Worker**，按 `execution_history` **重放** 历史成功片段，再重试当前代码（次数由 `max_retries` 控制）。`innovus` 和 `primetime` session 默认不记录或重放 sample history；执行超时或进程级失败会丢弃对应 physical worker、唤醒等待者，并把当前失败返回上层。
- **并发创建**：非 pool `create_session()` 只在短临界区内预留 session id；实际 worker 启动在全局锁外完成。Innovus 和 pooled PrimeTime 也只短暂持有 SessionManager 锁，随后在 pool condition variable 上等待或启动 physical worker。
- **超时**：墙钟超时由 `SessionManager` 主导。普通 Python/Shell/TCL 会重启 Worker 并重放历史成功片段来恢复会话，但**不会自动重试本次超时的代码**；Innovus 和 pooled PrimeTime 会丢弃失败的 physical worker，后续继续执行时重新从 pool lease 干净 worker。
- **回收**：后台线程按 `ttl_sweep_interval` 扫描，空闲超过 `ttl` 的会话会 `close_session`。
- **资源**：Worker 启动时尽力设置 `RLIMIT_AS`（内存）与 `RLIMIT_NPROC`（进程数）；平台差异见根目录 `docs/design_sandbox.md`。
- **Interactive v1**：每个 `WorkspaceSession` 启动全新的专属 Innovus 或 PrimeTime 进程；不借用 batch
  pool 槽位或进程，不跨 Session 复用，不在 process lost 后 replacement/history replay。它只通过
  `/v1` HTTP API 调用，没有 interactive Import Backend、MCP、CLI、Claude Adapter 或高层 Python client。
- **有界输出**：子进程 stdout/stderr 使用固定二进制块和每次 execution 唯一的双 fence 读取。Batch 继续
  返回完整 stdout/stderr；interactive 直接落盘 full log，HTTP 仅返回固定预算的 diagnostics/head/tail、计数和
  日志引用。interactive startup、baseline、healthcheck 和 cwd probe 同样只经有界内存 sink 返回诊断，
  不会在首个 execution 前聚合无限工具输出。

## Innovus 结构图

```text
                  reward / benchmark runner
    static check -> stage snapshot -> restore -> run Tcl -> probes
                              |
                              v
                       ExecutionClient
                    /                  \
             HTTPBackend            ImportBackend
                 |                      |
                 v                      |
        FastAPI sandbox server          |
        stage_snapshot API              |
                 |                      |
                 +----------+-----------+
                            v
                    SessionManager
              logical session_<id>/ dirs
                            |
                            v
          InnovusWorkerPool (extends ToolWorkerPoolBase)
        common lease/queue/prewarm/replenish lifecycle
               + Innovus-specific reset policy
                            |
                            v
           physical worker active/idle/starting
                            |
                            v
       pooled worker.py -> InnovusExecutor -> innovus -nowin
                            ^
                            |
          close: freeDesign + Tcl cleanup + healthcheck
```

HTTP reward 路径下，`stage_snapshot` 只负责把 `SnapshotCache` 中的
`start_snapshot` materialize 到当前 session 的
`.science_eda_assets/snapshots/...` 目录。真正的 DB restore 由上层 runner
随后在同一个 Innovus session 中执行 `source <session snapshot path>` 完成。

## 配置

通过 `SandboxConfig`（`science_eda.config`）控制通用 sandbox、Innovus 和 PrimeTime 行为。HTTP server 未显式传入配置时使用 `SandboxConfig()` 默认值。

| 字段 | 说明 |
|------|------|
| `client_mode` | `"import"`（默认）或 `"http"` |
| `endpoint` | HTTP 模式下的服务根 URL（默认 `http://localhost:8765`） |
| `server_host` / `server_port` | HTTP server 启动时监听的地址和端口，默认 `127.0.0.1:8765` |
| `timeout` | 单次执行超时（秒），HTTP 客户端会在此基础上额外放宽 |
| `ttl` / `ttl_sweep_interval` | 会话空闲秒数与扫描间隔 |
| `max_retries` | Worker 进程级故障时的重试/重放轮数 |
| `sandbox_root` | 会话工作目录根；空则使用系统临时目录下的 `eda_sandbox` |
| `memory_mb` / `max_pids` | Worker 资源提示（`setrlimit`） |
| `python_path` | Python 可执行文件配置表面；当前本地 Worker 由运行中的 Python 进程启动 |
| `shell_path` / `tcl_shell` | Shell 与 TCL 解释器路径 |
| `interactive_module_shell_path` | Interactive create 传入 `version` 时执行 `ma` 的 csh 路径，默认 `/bin/csh` |
| `innovus_bin` / `innovus_args` | Innovus 可执行文件与启动参数，默认 `innovus -nowin` |
| `innovus_startup_tcl` | Innovus session ready 前执行的初始化 Tcl |
| `innovus_startup_timeout` | Innovus 启动、license checkout 和 startup Tcl 超时 |
| `innovus_replay_policy` | Innovus replay 策略；当前只支持默认 `"none"` |
| `innovus_pool_size` | physical Innovus 最大并发数；默认 `1` |
| `innovus_pool_reuse` | logical session 关闭后是否复用 physical Innovus worker；默认 `true`，设为 `false` 时每次关闭都丢弃进程，并在 `innovus_pool_replenish=true` 时按 `innovus_pool_size` 补新 worker |
| `innovus_pool_prewarm` | HTTP server 启动时是否后台预热 idle Innovus worker 到 `innovus_pool_size`；默认 `true` |
| `innovus_pool_prewarm_concurrency` | 后台预热/补池同时启动的 worker 上限；默认 `16` |
| `innovus_pool_replenish` | worker 丢弃后是否后台补回目标 pool 容量；默认 `true` |
| `innovus_pool_queue_timeout` | pool 满时 `create_session("innovus")` 最长等待秒数；默认 `300.0`，`<=0` 表示无限等待 |
| `innovus_pool_reset_tcl` | logical session 关闭时执行的 reset Tcl；默认 `freeDesign` |
| `innovus_pool_clean_tcl_state` | reset 后是否清理 session 新增 Tcl 状态并恢复 `env`；默认 `true` |
| `innovus_pool_healthcheck_tcl` | reset 后确认 worker 可复用的 healthcheck Tcl |
| `innovus_pool_incomplete_reset_policy` | reset 输出 `IMPSYC-6379` 时的策略；默认 `"reuse_with_retry"`，也支持 `"discard"` 和 `"ignore"` |
| `innovus_pool_wait_log_interval` | pool 满时周期性等待日志间隔秒数 |
| `innovus_snapshot_cache_root` | HTTP/远端 reward 使用的 sandbox 本地 Innovus snapshot cache 根目录，通常指向已同步的 `science_eda/dataset/innovus_grpo/snapshots` |
| `primetime_bin` / `primetime_args` | PrimeTime 可执行文件与启动参数，默认 `pt_shell` 和空参数 |
| `primetime_startup_tcl` / `primetime_startup_timeout` | PrimeTime session 初始化 Tcl 与启动/license 超时 |
| `primetime_replay_policy` | PrimeTime replay 策略；当前只支持 `"none"` |
| `primetime_use_pool` | 是否启用 PrimeTime physical worker pool；默认 `false` |
| `primetime_pool_size` / `primetime_pool_queue_timeout` | PrimeTime physical worker 上限与等待超时 |
| `primetime_pool_prewarm` / `primetime_pool_prewarm_concurrency` / `primetime_pool_replenish` | HTTP 启动预热、预热/补池并发上限与 worker 丢弃后补池 |
| `primetime_pool_reset_tcl` | logical session 关闭时执行的工具级 reset Tcl；默认空，表示关闭时丢弃 worker 而不回池 |
| `primetime_pool_clean_tcl_state` | reset 后是否清理 session 新增 Tcl 状态并恢复 `env`；默认 `true` |
| `primetime_pool_healthcheck_tcl` | reset 后确认 `pt_shell` 可复用的 healthcheck Tcl |
| `primetime_pool_wait_log_interval` | PrimeTime pool 满时周期性等待日志间隔秒数 |
| `legacy_batch_enabled` | 默认 `true`；控制 legacy batch HTTP routes、`SessionManager` 和 batch 工具 pool |
| `http_thread_limit` | HTTP 同步路由 worker 线程上限；`0` 表示根据已启用的 batch pool 和 interactive capacity 自动推导 |
| `interactive_enabled` | 默认 `true`；挂载 `/v1` 并要求 `server_host` 为 IPv4/IPv6 loopback |
| `interactive_state_root` | `~/.science_eda/interactive` |
| `interactive_runtime_root` / `interactive_log_root` / `interactive_registry_path` | 空值分别派生为 `<state>/runtime`、`<state>/logs`、`<state>/registry.sqlite3` |
| `interactive_innovus_capacity` / `interactive_primetime_capacity` | 与 batch pool 独立的 runtime 槽位，默认各 `1` |
| `interactive_create_capacity_timeout` / `interactive_max_sessions` | create 容量等待 `300.0` 秒；最多 `100` 个非 `CLOSED` Session |
| `interactive_execute_timeout` | `3600`（1 小时）；execute 未传 `timeout_ms` 时使用 |
| `interactive_runtime_idle_ttl` | `172800`（48 小时） |
| `interactive_session_retention_ttl` | `0`，默认关闭 Session 自动 GC |
| `interactive_create_request_retention_ttl` / `interactive_audit_retention_ttl` | `2592000`（30 天）/ `7776000`（90 天） |
| `interactive_sweep_interval` / `interactive_session_event_max_records` | `30` 秒 / `10000` 条事件 |
| `interactive_max_code_bytes` | `1048576`（1 MiB） |
| `interactive_output_preview_bytes` / `interactive_output_preview_max_bytes` | `8192`（8 KiB）/ `32768`（32 KiB 硬上限） |
| `interactive_execution_log_max_bytes` / `interactive_session_log_max_bytes` | `1073741824`（1 GiB）/ `4294967296`（4 GiB） |
| `interactive_reader_chunk_bytes` | `65536`（64 KiB） |
| `interactive_history_default_page_size` / `interactive_history_max_page_size` | `20` / `100` |
| `interactive_history_page_max_bytes` | `4194304`（4 MiB） |
| `innovus_interactive_healthcheck_tcl` / `primetime_interactive_healthcheck_tcl` | 两种工具的 interactive startup healthcheck Tcl |
| `innovus_interactive_env_allowlist` / `primetime_interactive_env_allowlist` | 默认 `null` 以兼容性继承当前环境；显式列表只继承所列名称 |

`worker_mode`、`cpus`、`network_mode` 等在设计文档中有说明；当前实现以进程级 Worker 为主。

## 使用示例

### Import 模式（库内嵌）

```python
from science_eda.config import SandboxConfig
from science_eda.sandbox import ExecutionClient, StatefulSession

cfg = SandboxConfig()
cfg.client_mode = "import"
cfg.sandbox_root = "/tmp/my_sandbox_root"  # 可选

client = ExecutionClient(cfg)
sid = client.create_session("python")
try:
    r = client.execute("print(1 + 1)", "python", sid)
    print(r.stdout, r.exit_code)
finally:
    client.close_session(sid)
```

省略 `session_id` 时，Import 和 HTTP 后端都会执行 one-shot：创建临时 session、执行一次并关闭。
需要保持状态时，应显式创建 session 或使用 `StatefulSession`。
显式 `session_id` 必须是单个文件名片段，不能包含路径分隔符或空字节。

使用 `StatefulSession` 可自动创建会话并在上下文结束时关闭：

```python
with StatefulSession(client, "python") as sess:
    sess.execute("x = 10")
    r = sess.execute("print(x)", timeout=5)
```

### 语言标识

`normalize_lang` 接受别名，例如：`py`→`python`，`sh`/`bash`→`shell`，`tclsh`→`tcl`，`cadence_innovus`→`innovus`，`pt`/`pt_shell`/`synopsys_primetime`→`primetime`。会话创建后语言固定，混用语言会触发 `LanguageMismatchError`。

### Innovus 模式

```python
cfg = SandboxConfig()
cfg.innovus_bin = "innovus"          # 也可以是设置 license/PDK 环境的 wrapper
cfg.innovus_args = ["-nowin"]
cfg.innovus_startup_timeout = 300
cfg.innovus_pool_size = 1
cfg.innovus_pool_reuse = False      # 可选：每个 logical session 使用新的 physical 进程
cfg.innovus_pool_queue_timeout = 300.0

client = ExecutionClient(cfg)
sid = client.create_session("innovus")
try:
    r = client.execute('puts "ready"', "innovus", sid, timeout=30)
finally:
    client.close_session(sid)
```

`create_session("innovus")` 创建的是 logical session。底层 physical Innovus worker 数量由
`innovus_pool_size` 限制；HTTP server 默认后台预热到该上限，pool 满时调用会等待已有 logical session 关闭并完成 reset/healthcheck，
最长等待 `innovus_pool_queue_timeout` 秒。
关闭 session 时 reset、可选 Tcl 状态清理、physical cwd 恢复和 healthcheck 成功则 worker 回池，失败则关闭并丢弃 worker、唤醒等待队列。设定 `innovus_pool_reuse=false` 时关闭 session 会直接丢弃当前 physical worker；如果 `innovus_pool_replenish=true`，后台 supervisor 会按 `innovus_pool_size` 自动补新的 physical worker。reset 成功但输出 `IMPSYC-6379` 时，默认策略会继续复用 worker，并只在下一 logical session 的第一次执行发生进程级失败时换新 worker 重试一次；可通过 `innovus_pool_incomplete_reset_policy="discard"` 恢复立即丢弃，或用 `"ignore"` 完全忽略该 warning。

`innovus` session 的 `ExecutionResult.metadata` 可携带 sandbox 诊断信息，例如 incomplete reset 后第一次执行重试的 worker id、tool pid、worker age 和失败输出尾部。run workspace、wrapper Tcl、snapshot restore、checker 和 reward artifact 应由上层 `pipeline/innovus_grpo_data` 管理。

### PrimeTime 模式

```python
from science_eda.config import SandboxConfig
from science_eda.sandbox import ExecutionClient

cfg = SandboxConfig()
cfg.primetime_bin = "pt_shell"     # 也可以是设置 license/PDK 环境的 wrapper
cfg.primetime_args = []
cfg.primetime_startup_timeout = 300
cfg.primetime_use_pool = False     # 默认：每个 logical session 使用独立 pt_shell

client = ExecutionClient(cfg)
sid = client.create_session("primetime")
try:
    client.execute("source run_sb1.tcl", "primetime", sid)
    result = client.execute_tcl_script(
        'puts [get_object_name [get_clocks *]]',
        "primetime",
        sid,
    )
finally:
    client.close_session(sid)
```

`primetime` 使用与 Innovus 相同的 Tcl wrapper、唯一 status 和双 fence 协议。`execute_tcl_script()`
会把脚本文本写入 session 工作目录后再 `source`；PrimeTime 默认使用
`redirect -variable` 捕获脚本输出，可传 `capture_output=False` 直接 source。
同一个 helper 也可用于 `tcl` 和 `innovus` session。

启用 PrimeTime pool 时，必须先在真实设计上验证工具级 reset，否则默认空 reset Tcl 会让
logical session 关闭时直接丢弃 physical worker：

```python
cfg.primetime_use_pool = True
cfg.primetime_pool_size = 4
cfg.primetime_pool_reset_tcl = "source /path/to/site_verified_pt_reset.tcl"
cfg.primetime_pool_healthcheck_tcl = "puts __PT_READY__"
```

如果 reset 或 healthcheck 返回失败，physical worker 会被丢弃，不会回到 idle pool。

### HTTP 模式

HTTP 模式适合把 sandbox 独立起成服务进程，让 pipeline、agent 或远端客户端通过 RPC 风格接口
访问同一套 `SessionManager`。

#### 启动服务

需安装 `fastapi`、`uvicorn`、`pydantic`：

```bash
python -m science_eda.sandbox.server
```

默认监听 `127.0.0.1:8765`，并使用 `SandboxConfig()` 默认值。监听地址由
`server_host` / `server_port` 控制。也可以通过
`--config` / `-c` 显式加载顶层 `science_eda.yaml` 或 `science_eda.toml` 中的 `sandbox`
配置：

```bash
python -m science_eda.sandbox.server --config science_eda/sandbox/science_eda.example.yaml
```

该示例保留完整服务端/worker 配置，并按 interactive-only、两套接口共用、legacy batch-only、
physical pool 和当前预留字段分组。`legacy_batch_enabled: false` 时 batch/pool 字段不会产生运行期资源。
`client_mode` 和 `endpoint` 属于 `ExecutionClient` 客户端配置，不是服务端启动参数。

interactive 默认启用，因此 `server_host` 必须是 loopback；`0.0.0.0` 和其它非 loopback 地址会在
daemon 启动时被拒绝。若需保留非 loopback 的旧式远程 batch HTTP 服务，必须显式配置
`interactive_enabled: false`；此时不挂载 `/v1/workspace-sessions*`。

也可以在自己的服务中挂载 app：

```python
from science_eda.config import SandboxConfig
from science_eda.sandbox.server import create_app

cfg = SandboxConfig()
cfg.sandbox_root = "/tmp/eda_sandbox_http"
app = create_app(cfg)
```

#### Python 客户端

`ExecutionClient` 会根据是否传入 `session_id` 自动选择 one-shot 或 session 内执行：

```python
from science_eda.config import SandboxConfig
from science_eda.sandbox import ExecutionClient, StatefulSession

cfg = SandboxConfig(client_mode="http", endpoint="http://127.0.0.1:8765")
client = ExecutionClient(cfg)

# 健康检查
assert client.is_alive()["is_alive"] is True

# one-shot 执行：服务端创建临时 session、执行一次、立即关闭
result = client.execute("print(1 + 1)", "python", timeout=5)
assert result.exit_code == 0

# 多步状态：显式创建 session，或使用 StatefulSession
with StatefulSession(client, "python") as sess:
    sess.execute("x = 10")
    result = sess.execute("print(x + 1)")
    assert "11" in result.stdout
```

文件接口限制在 session 工作目录内：

```python
sid = client.create_session("shell")
try:
    client.write_file(sid, "inputs/run.tcl", "puts hello\n")
    client.upload(sid, "./local_assets.zip", "assets", unzip=True)
    content = client.read_file(sid, "inputs/run.tcl")
finally:
    client.close_session(sid)
```

`upload(..., unzip=True)` 会把 zip 安全解压到 `target_path`；普通文件上传会写到目标文件路径。
所有路径都是 session-relative。绝对路径、`..` 逃逸路径和 zip-slip 成员会被拒绝。

远端 Innovus reward 不通过 `upload` 传输 snapshot。sandbox 服务启动时应把
`innovus_snapshot_cache_root` 指向已同步的 snapshot cache；FastAPI app 创建时会扫描
该目录并建立 snapshot manifest 索引。客户端调用
`stage_snapshot(session_id, snapshot_ref, target_path)` 时只提交 record 内的
`start_snapshot` 引用，服务端用启动时索引把对应 snapshot 目录复制为 session 内的
只读副本，并返回 session-relative 的 snapshot 路径。若 cache 根目录旁存在
`designs/`，staging 也会把它复制到同一个 session asset root 下，供 empty-session
任务通过 `${::IMEX::dataVar}/../../../..` 读取 common LEF/lib 等输入。
snapshot payload 内指向 snapshot 自身或相邻 `designs/` 下文件的 symlink 会在 staging
时解引用为普通只读文件；其他 symlink 会被拒绝。

#### 原始 HTTP 接口

HTTP 后端使用 SWE-ReX 风格的 RPC 路由：

| Method | Path | 语义 |
|--------|------|------|
| `GET` | `/is_alive` | 健康检查 |
| `GET` | `/pool_state` | 返回 `innovus` 和 `primetime` pool 的 `pool_size` / `active` / `idle` / `starting` / prewarm 与 replenish 配置 |
| `POST` | `/execute` | one-shot 执行，不保留 session |
| `POST` | `/create_session` | 创建有状态 session |
| `POST` | `/run_in_session` | 在已有 session 内执行 |
| `POST` | `/close_session` | 关闭指定 session |
| `POST` | `/read_file` | 读取 session 工作目录内文本文件 |
| `POST` | `/write_file` | 写入 session 工作目录内文本文件 |
| `POST` | `/upload` | multipart 上传文件，可选 zip 解压 |
| `POST` | `/stage_snapshot` | 从 sandbox 本地 snapshot cache materialize 一个只读 Innovus snapshot 到 session |

HTTP 模式下的 `ExecutionClient.close()` 只关闭当前 client 创建并仍在追踪的 session，
不会清空共享 sandbox server 上其它调用方的 session。

健康检查：

```bash
curl http://127.0.0.1:8765/is_alive
```

响应：

```json
{
  "is_alive": true,
  "message": ""
}
```

one-shot 执行：

```bash
curl -X POST http://127.0.0.1:8765/execute \
  -H 'Content-Type: application/json' \
  -d '{"lang": "python", "code": "print(1 + 1)", "timeout": 5}'
```

响应只包含 `ExecutionResult` 字段：

```json
{
  "stdout": "2\n",
  "stderr": "",
  "exit_code": 0,
  "duration": 0.01,
  "metadata": {}
}
```

创建 session 并执行多步代码：

```bash
curl -X POST http://127.0.0.1:8765/create_session \
  -H 'Content-Type: application/json' \
  -d '{"lang": "python", "session_id": "demo"}'

curl -X POST http://127.0.0.1:8765/run_in_session \
  -H 'Content-Type: application/json' \
  -d '{"session_id": "demo", "lang": "python", "code": "x = 41"}'

curl -X POST http://127.0.0.1:8765/run_in_session \
  -H 'Content-Type: application/json' \
  -d '{"session_id": "demo", "lang": "python", "code": "print(x + 1)"}'
```

`run_in_session` 的 `lang` 可省略；省略时使用 session 绑定语言。`ExecutionClient` 会显式传入
`lang`，以保持 import/http 后端一致的 `LanguageMismatchError` 行为。

读写文本文件：

```bash
curl -X POST http://127.0.0.1:8765/write_file \
  -H 'Content-Type: application/json' \
  -d '{"session_id": "demo", "path": "inputs/a.tcl", "content": "puts hello\n"}'

curl -X POST http://127.0.0.1:8765/read_file \
  -H 'Content-Type: application/json' \
  -d '{"session_id": "demo", "path": "inputs/a.tcl"}'
```

上传普通文件：

```bash
curl -X POST http://127.0.0.1:8765/upload \
  -F session_id=demo \
  -F target_path=uploads/input.tcl \
  -F unzip=false \
  -F file=@./input.tcl
```

上传并解压 zip：

```bash
curl -X POST http://127.0.0.1:8765/upload \
  -F session_id=demo \
  -F target_path=assets \
  -F unzip=true \
  -F file=@./assets.zip
```

完成后关闭 session：

```bash
curl -X POST http://127.0.0.1:8765/close_session \
  -H 'Content-Type: application/json' \
  -d '{"session_id": "demo"}'
```

#### Interactive v1（loopback TCP only）

Interactive 没有 Import Backend 或高层 Python client，调用方直接使用任意 HTTP client。固定接口是：

| Method | Path |
|---|---|
| `GET` | `/v1/capabilities` |
| `POST` | `/v1/workspace-sessions/create` |
| `GET` | `/v1/workspace-sessions/list` |
| `GET` | `/v1/workspace-sessions/{workspace_session_id}` |
| `GET` | `/v1/workspace-sessions/{workspace_session_id}/history` |
| `DELETE` | `/v1/workspace-sessions/{workspace_session_id}` |
| `POST` | `/v1/workspace-sessions/{workspace_session_id}:execute` |

Claude Code 可通过本仓库的 stdio MCP adapter 调用上述 HTTP API。该 adapter 假设 sandbox daemon
已经在本机启动，只负责把 MCP tool call 转发到 loopback HTTP，并在 execute 后返回
`result.output.full_log.path` 指向的完整日志文件路径和 output preview：

```json
{
  "mcpServers": {
    "science-eda-sandbox": {
      "command": "python",
      "args": [
        "-c",
        "import sys; sys.path.insert(0, r'D:\\code\\gitcode\\ScienceEDA'); from science_eda.sandbox.interactive_mcp_server import main; main()",
        "--endpoint",
        "http://127.0.0.1:8765"
      ]
    }
  }
}

```

暴露的 MCP tools 为 `create_session(tool_kind, workspace_path, session_id)`、
`execute_in_session(session_id, command)` 和 `destroy_session(instance_id)`。使用 Innovus 编译或验证
Innovus Tcl、使用 PrimeTime 编译或验证 PrimeTime Tcl 时，先用 `create_session` 创建并确认一个
`ACTIVE` 会话，再用 `execute_in_session` 在同一 session 中连续执行具体 command；整个任务完成且不再需要
与该 EDA runtime 交互时，用 `destroy_session` 关闭会话并释放工具进程。

先查询 capability，再注册本机绝对 workspace path。`session_id` 可由调用方指定，也可省略并让
Sandbox 生成：

```bash
curl http://127.0.0.1:8765/v1/capabilities

curl -X POST http://127.0.0.1:8765/v1/workspace-sessions/create \
  -H 'Content-Type: application/json' \
  -d '{
    "session_id": "innovus-demo-1",
    "tool_kind": "innovus",
    "version": "23.10-s100_1",
    "workspace_path": "/absolute/path/to/chip-workspace"
  }'
```

create 在完成 capacity wait、新进程启动、scratch 中的 startup/healthcheck 和 workspace `cd/pwd`
校验后才返回 `ACTIVE + READY`。调用方提供的 `session_id` 会原样成为 `workspace_session_id`；省略时
Sandbox 返回生成的 `wss_<uuid>`。后续只按该 ID 定位。容量等待 deadline 从首次接受 session id 时开始，后台线程排队也消耗该预算；未发布的
`PENDING` create 也受有界 admission 保护。执行 Tcl：

`version` 可省略。显式传入时，Sandbox 在启动工具 bin 前通过配置的
`interactive_module_shell_path`（默认 `/bin/csh`）执行 `ma <tool_kind>/<version>`，成功后在同一
csh 环境中 `exec` 工具 bin；例如 PrimeTime 的
`version="2019.03sp1"` 会先执行 `ma primetime/2019.03sp1`。省略时不执行 `ma`。

```bash
curl -X POST \
  http://127.0.0.1:8765/v1/workspace-sessions/wss_01JEXAMPLE:execute \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "agent-turn-1-check-1",
    "code": "source {verify.tcl}",
    "timeout_ms": 300000
  }'
```

create `session_id` 必须是 1–128 个 ASCII 字母、数字、下划线或连字符，并以字母或数字开头。
`version` 必须是 1–128 个 ASCII 字母、数字、点、下划线或连字符，并以字母或数字开头。
execute `request_id` 必须是 UTF-8 编码后 1–256 bytes。`timeout_ms` 可省略，有效默认值为
`SandboxConfig.interactive_execute_timeout * 1000`；幂等比较使用规范化后的有效值。HTTP 断开不取消已接受的 create 或
execution；create 使用相同 session id 和字段、execute 使用相同 request id 和字段重试，即可获取原操作的进行中或终态结果。

`interactive_max_code_bytes` 是原始 UTF-8 代码的硬上限。服务端还会在接受 execution 前按实际 JSON
序列化验证该记录能够完整放入一页 history；极端大量 JSON 控制字符导致 4 MiB 页面预算不足时，会在
执行前返回 `CODE_TOO_LARGE`，不会保存一个之后无法分页的记录。

所有 `/v1` 响应含 `schema_version="1"`。完整 stdout/stderr 持续写入响应中
`result.output.full_log.path` 指向的本地 `0600` 日志；HTTP 只返回有界 preview、计数和完整性元数据。
运行时可直接 tail 该路径。查询和关闭：

```bash
curl http://127.0.0.1:8765/v1/workspace-sessions/list
curl http://127.0.0.1:8765/v1/workspace-sessions/wss_01JEXAMPLE
curl 'http://127.0.0.1:8765/v1/workspace-sessions/wss_01JEXAMPLE/history?limit=20'
curl -X DELETE http://127.0.0.1:8765/v1/workspace-sessions/wss_01JEXAMPLE
```

Session 状态固定为 `ACTIVE/CLOSING/CLOSED`，runtime 为 `READY/BUSY/LOST/STOPPED`，execution 对外为
`RUNNING/SUCCEEDED/FAILED/TIMED_OUT/LOST`。Tcl 错误、超时和 process lost 在 execution envelope 中返回；
请求级错误使用大写码。现有 legacy routes 继续使用原小写协议。

SQLite v1 首次建库把 schema 与 `user_version` 原子提交，并可安全恢复已知且空的 version-0 部分 schema。
control plane 对 state/runtime/log/registry parent 持有层级进程锁；相同或祖先/后代 managed roots 不允许
被第二个 daemon 同时管理，互不相交的 roots 可以并行运行。

有真实 Innovus 和 license 的站点可运行 opt-in 的完整 TCP 验收（默认测试会跳过）：

```bash
SCIENCE_EDA_RUN_INTERACTIVE_SMOKE=1 \
SCIENCE_EDA_INTERACTIVE_SMOKE_INNOVUS_BIN=/path/to/innovus \
SCIENCE_EDA_INTERACTIVE_SMOKE_WORKSPACE=/absolute/design/workspace \
pytest -q tests/test_interactive_http_smoke.py
```

如需覆盖启动参数，另设 `SCIENCE_EDA_INTERACTIVE_SMOKE_INNOVUS_ARGS`。

#### 错误语义

用户代码失败不会变成 HTTP 错误；服务端仍返回 `200`，通过 `exit_code != 0` 表示：

```json
{
  "stdout": "",
  "stderr": "ValueError: bad\n",
  "exit_code": 1,
  "duration": 0.02
}
```

协议、session 或路径错误返回结构化 envelope：

```json
{
  "error": {
    "code": "session_not_found",
    "message": "no such session: 'demo'"
  }
}
```

常见错误码：`session_not_found`、`session_already_exists`、`language_mismatch`、`invalid_path`、
`file_not_found`、`invalid_request`、`execution_error`。HTTP 客户端会把 session、language、
非法路径和缺失文件错误分别映射回对应 SDK/Python 异常，未知 HTTP 错误映射为
`APIConnectionError`。

## 异常

常见类型（`science_eda.exceptions`）：`SessionNotFoundError`、`SessionAlreadyExistsError`、`LanguageMismatchError`、`SandboxPathError`、`ExecutionError`（重放失败等）、HTTP 路径上的 `APIConnectionError` 与 `TimeoutError`（执行超时映射名称为 `TimeoutError` 子类）。HTTP 服务端返回结构化错误 envelope；HTTP 客户端会把 session、language、非法路径和缺失文件错误映射回对应 SDK/Python 异常，未知 HTTP 错误映射为 `APIConnectionError`。

## 进一步阅读

当前 batch 协议、边界与安全说明见 **[docs/design_sandbox.md](../../docs/design_sandbox.md)**；interactive v1
的状态机、幂等、日志、路径和 retention 契约见
**[docs/design_interactive_sandbox.md](../../docs/design_interactive_sandbox.md)**。
