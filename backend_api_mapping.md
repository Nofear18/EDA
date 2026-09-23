# MCP 与后端 API 映射

本文对照 `MCP_for_EDA_tools/` 中两套适配器的当前实现，分别连接 ScienceEDA interactive API 和 pandas worker API。

## HTTP 接口

ScienceEDA 路径相对于 `endpoint`，pandas worker 路径相对于发现或指定的 `worker_url`。

| 功能 | ScienceEDA | pandas |
| --- | --- | --- |
| 发现 worker | 无，直接使用 `endpoint` | `GET {endpoint}/eda_agent/workers/detail`，读取 `data` 对象或数组第一项 |
| 创建会话 | `POST /v1/workspace-sessions/create` | `POST /worker/instances/start` |
| 执行 Tcl | `POST /v1/workspace-sessions/{id}:execute` | `POST /worker/instances/{id}/eda/execute-tcl` |
| 获取日志路径 | 从执行响应读取 | `GET /worker/instances/{id}`，读取 `data.apr_log` |
| 关闭会话 | `DELETE /v1/workspace-sessions/{id}` | `POST /worker/instances/{id}/stop` |

## 字段与状态

| MCP 字段 / 行为 | ScienceEDA | pandas |
| --- | --- | --- |
| `tool_kind` | `innovus` / `primetime` | 映射为 `eda_type=innovus` / `pt` |
| `workspace_path` | `workspace_path` | `root_path` |
| 创建时内部生成的 UUID hex | `session_id` | `instance_id` |
| `command` | `code`，另生成 `request_id` | `command` |
| 创建成功 | 会话状态为 `ACTIVE` 且返回会话 ID | `success=true`，返回 `data.instance_id` 或内部生成的 ID |
| 执行状态 | 响应的 `state`、`result.exit_code`、`result.error` | 根据 `success` 和输出中的 `error` 子串映射成功或失败 |
| `output_preview` | 执行结果的输出预览 | `data.result` 移除开头提示符 |
| `full_log_path` | 优先读取 `result.output.full_log.path` 等字段 | 实例详情的 `data.apr_log` |
| 关闭成功 | `state=CLOSED` | `success=true` 且 `data.status=stopped` |

## 对外 MCP 约定

- `create_session` 输入 `tool_kind`、`workspace_path`，成功时在文本 `content` 中返回会话 ID；无需调用方提供 ID。
- `execute_in_session` 输入 `session_id`、`command`，输出 `content` 和 `structuredContent`，包含 `state`、`exit_code`、`error`、`full_log_path`、`output_preview`。
- `destroy_session` 输入 `instance_id`，值为创建时返回的会话 ID，成功时返回“销毁成功”。
- 工具错误设置 `isError=true`；JSON-RPC 请求格式错误使用协议层错误。

pandas 的执行状态由适配器推断，`exit_code` 不是后端原始进程退出码；详细限制见接入方案。

## 详细说明

- [ScienceEDA MCP](MCP_for_EDA_tools/science_eda/README.md)
- [pandas MCP](MCP_for_EDA_tools/pandas/README.md)
- [pandas 接入方案](MCP_for_EDA_tools/pandas/integration_plan.md)
