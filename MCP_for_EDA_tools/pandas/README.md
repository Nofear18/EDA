# pandas interactive MCP 工具调用说明

本文档说明 `interactive_mcp_server.py` 当前版本暴露的 MCP 工具输入输出形态。该 server 的外部 MCP tool 定义与 `science_eda.sandbox.interactive_mcp_server` 对齐，底层 HTTP 调用适配 pandas worker API。

接入流程与设计约定见[接入方案](integration_plan.md)，两个后端的对照见[接口映射](../../backend_api_mapping.md)。本目录脚本依赖 `httpx`，可用 `python -m pip install httpx` 安装；`python check_backend.py --help` 可查看真实后端联调参数。

## 总体规则

- MCP 暴露三个 tool：`create_session`、`execute_in_session`、`destroy_session`。
- `create_session` 和 `destroy_session` 返回 `content` 文本结果。
- `execute_in_session` 同时返回 `content` 和 `structuredContent`：`content` 给 Claude/模型直接阅读，`structuredContent` 给客户端或程序化逻辑读取。
- tool 内部捕获到的失败会返回 `isError: true`，通常不会变成 JSON-RPC 顶层 `error`。
- worker/control-plane HTTP 返回 `>= 400` 时，MCP 会把 HTTP 状态码和返回体包装进 tool error。
- worker/control-plane 响应里用于内部追踪的 `schema_version` 和 `request_id` 会在 MCP 对外错误文本里递归删除。
- JSON-RPC/MCP 请求本身不合法，例如未知 method、`params` 不是对象，属于协议层错误，会返回顶层 `error`，不属于 tool result。

## 启动参数与 worker 发现

server 默认从 stdin/stdout 读写 JSON-RPC MCP 消息。

```bash
python interactive_mcp_server.py
```

可选参数：

```bash
python interactive_mcp_server.py \
  --endpoint http://xxx \
  --worker-url http://worker-xxx \
  --username zhangjiawen
```

| 参数 | 环境变量 | 默认值 |
|---|---|---|
| `--endpoint` | `PANDAS_MCP_SANDBOX_ENDPOINT` | `http://127.0.0.1:8765` |
| `--worker-url` | `PANDAS_MCP_WORKER_URL` | 空 |
| `--username` | `PANDAS_MCP_USERNAME` | 当前 shell 的 `whoami`，失败时使用 `getpass.getuser()` |

如果没有传入 `--worker-url`，server 会先请求：

```text
GET {endpoint}/eda_agent/workers/detail
```

请求头：

```json
{
  "Content-Type": "application/json",
  "X-Username": "<username>"
}
```

从返回中读取 `data.worker_url`；当前实现也兼容 `data` 为数组，此时只读取第一项的 `worker_url`，不会自动选择其他 worker：

```json
{
  "agent": {},
  "data": {
    "worker_url": "http://worker-xxx"
  }
}
```

后续 worker 请求都会使用同一个 `worker_url` 和相同请求头。

## create_session

### 输入 schema

```json
{
  "tool_kind": "innovus | primetime",
  "workspace_path": "本地绝对目录路径"
}
```

必填字段：

- `tool_kind`：只能是 `innovus` 或 `primetime`。
- `workspace_path`：本地绝对路径，必须存在、可解析、是目录，并且可作为 runtime cwd。

不再支持调用方传入：

- `session_id`：由 MCP 工具内部生成并放入 worker 请求体。
- `version`：`create_session` 不需要该字段。

MCP 发给 worker 的 HTTP body 形态如下，其中 `instance_id` 是工具内部生成的裸 UUID hex：

```json
{
  "root_path": "D:\\code\\gitcode\\pandas_mcp",
  "eda_type": "pt",
  "instance_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789"
}
```

字段映射：

| MCP 字段 | worker 字段 |
|---|---|
| `workspace_path` | `root_path` |
| 内部生成的 `session_id` | `instance_id` |
| `tool_kind=primetime` | `eda_type=pt` |
| `tool_kind=innovus` | `eda_type=innovus` |

worker 请求：

```text
POST {worker_url}/worker/instances/start
```

### 1. 成功

MCP tool 输入：

```json
{
  "tool_kind": "primetime",
  "workspace_path": "D:\\code\\gitcode\\pandas_mcp"
}
```

触发条件：

```text
HTTP 请求成功
worker 返回 success == true
并且 data.instance_id 是字符串；如果缺失，则 fallback 为内部生成的 session_id
```

假设 worker 返回：

```json
{
  "success": true,
  "data": {
    "instance_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789"
  }
}
```

MCP tool 输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "4f8d2a9c7b2e4a0c8d9e6f1023456789"
    }
  ]
}
```

### 2. MCP 本地请求字段不合法

这些情况会在 MCP 层拦截，不发送 worker start 请求：

- 缺少 `tool_kind`
- 缺少 `workspace_path`
- 字段不是字符串或为空字符串
- `tool_kind` 不是 `innovus` 或 `primetime`
- `workspace_path` 包含 NUL 字节
- `workspace_path` 不是可编码的 UTF-8 文本
- `workspace_path` 不是绝对路径
- `workspace_path` 不存在或不可解析
- `workspace_path` 不是目录
- `workspace_path` 不能作为 runtime cwd

示例：`tool_kind` 非法。

```json
{
  "tool_kind": "dc_shell",
  "workspace_path": "D:\\code\\gitcode\\pandas_mcp"
}
```

输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "create_session failed: tool_kind must be one of: innovus, primetime"
    }
  ],
  "isError": true
}
```

### 3. control-plane/worker/HTTP 失败

这些问题需要请求 control-plane 或 worker 后才能知道：

- control-plane 或 worker 连接失败、超时
- 未能发现 `worker_url`
- worker start 返回 HTTP `>= 400`
- worker 返回非 JSON 响应
- worker 返回 JSON 顶层不是 object
- worker 内部异常

连接失败输出示例：

```json
{
  "content": [
    {
      "type": "text",
      "text": "create_session failed: 无法连接 interactive sandbox daemon: [Errno 111] Connection refused"
    }
  ],
  "isError": true
}
```

### 4. HTTP 成功但状态异常

如果 worker HTTP 返回成功，但 `success != true`，MCP 会按 tool error 返回。

假设 worker 返回：

```json
{
  "success": false,
  "message": "capacity exhausted"
}
```

MCP 输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "Session创建失败\n"
    }
  ],
  "isError": true
}
```

## execute_in_session

### 输入 schema

```json
{
  "session_id": "create_session 返回的 session id",
  "command": "Innovus 或 PrimeTime Tcl 命令/代码块"
}
```

字段说明：

- `session_id`：1-128 个 ASCII 字母、数字、下划线或连字符，并且必须以字母或数字开头。
- `command`：要执行的 Tcl 命令或代码块，必须是非空字符串。

worker 执行请求：

```text
POST {worker_url}/worker/instances/{session_id}/eda/execute-tcl
```

worker 执行请求体：

```json
{
  "command": "puts hi"
}
```

执行请求完成后，MCP 会额外请求 instance 详情以获取完整日志路径：

```text
GET {worker_url}/worker/instances/{session_id}
```

字段映射：

| MCP 输出字段 | worker 来源 |
|---|---|
| `output_preview` | `POST /eda/execute-tcl` 返回的 `data.result`，去掉开头 `pt_shell>` 或 `innovus>` |
| `full_log_path` | `GET /worker/instances/{session_id}` 返回的 `data.apr_log` |

### 1. 成功

输入：

```json
{
  "session_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789",
  "command": "puts hi"
}
```

假设 execute-tcl 返回：

```json
{
  "success": true,
  "data": {
    "instance_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789",
    "result": "pt_shell>hi\n"
  }
}
```

假设 instance 详情返回：

```json
{
  "success": true,
  "data": {
    "apr_log": "/tmp/pandas_mcp/apr.log"
  }
}
```

MCP 输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=SUCCEEDED\nexit_code=0\nerror=null\nfull_log_path=/tmp/pandas_mcp/apr.log\noutput_preview:\nhi\n"
    }
  ],
  "structuredContent": {
    "state": "SUCCEEDED",
    "exit_code": 0,
    "error": null,
    "full_log_path": "/tmp/pandas_mcp/apr.log",
    "output_preview": "hi\n"
  }
}
```

注意：pandas worker 的 `success=true` 只表示 execute-tcl 接口调用成功，不一定表示 Tcl 命令没有报错。MCP 会继续检查 `data.result`，如果其中包含 `error` 字样（忽略大小写），会按 Tcl/EDA 命令失败处理。

`full_log_path` 可能为空，这仍然可以是成功：

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=SUCCEEDED\nexit_code=0\nerror=null\nfull_log_path=\noutput_preview:\nhi\n"
    }
  ],
  "structuredContent": {
    "state": "SUCCEEDED",
    "exit_code": 0,
    "error": null,
    "full_log_path": "",
    "output_preview": "hi\n"
  }
}
```

### 2. MCP 本地请求字段不合法

这些情况会在 MCP 层拦截，不发送 HTTP 请求：

- 缺少 `session_id`
- 缺少 `command`
- `session_id` 或 `command` 不是字符串或为空字符串
- `session_id` 格式非法

示例：缺少 `command`。

输入：

```json
{
  "session_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789"
}
```

输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=\nexit_code=null\nerror=MCP_TOOL_ERROR: execute_in_session failed: missing or empty string argument: command\nfull_log_path=\noutput_preview:\n"
    }
  ],
  "structuredContent": {
    "state": "",
    "exit_code": null,
    "error": {
      "code": "MCP_TOOL_ERROR",
      "message": "execute_in_session failed: missing or empty string argument: command"
    },
    "full_log_path": "",
    "output_preview": ""
  },
  "isError": true
}
```

示例：`session_id` 格式非法。

```json
{
  "session_id": "-bad",
  "command": "puts hi"
}
```

输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=\nexit_code=null\nerror=MCP_TOOL_ERROR: execute_in_session failed: session_id must be 1-128 ASCII letters, digits, underscores, or hyphens and must start with a letter or digit\nfull_log_path=\noutput_preview:\n"
    }
  ],
  "structuredContent": {
    "state": "",
    "exit_code": null,
    "error": {
      "code": "MCP_TOOL_ERROR",
      "message": "execute_in_session failed: session_id must be 1-128 ASCII letters, digits, underscores, or hyphens and must start with a letter or digit"
    },
    "full_log_path": "",
    "output_preview": ""
  },
  "isError": true
}
```

### 3. worker/HTTP 失败

这些问题需要请求 worker 后才能知道：

- worker 连接失败或超时
- session 不存在
- session 已关闭或 runtime stopped
- runtime lost
- session busy
- command 过大
- worker 执行接口返回 HTTP `>= 400`
- execute-tcl 成功返回后，查询 instance 详情失败
- worker 返回非 JSON 响应
- worker 返回 JSON 顶层不是 object
- worker 内部错误

session 不存在时输出示例。注意 worker 返回体内容以实际 worker 为准，MCP 只负责包装。

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=\nexit_code=null\nerror=MCP_TOOL_ERROR: execute_in_session failed: sandbox 请求失败: HTTP 404\n{\n  \"error\": {\n    \"code\": \"SESSION_NOT_FOUND\",\n    \"message\": \"workspace session not found\",\n    \"workspace_session_id\": \"4f8d2a9c7b2e4a0c8d9e6f1023456789\"\n  }\n}\nfull_log_path=\noutput_preview:\n"
    }
  ],
  "structuredContent": {
    "state": "",
    "exit_code": null,
    "error": {
      "code": "MCP_TOOL_ERROR",
      "message": "execute_in_session failed: sandbox 请求失败: HTTP 404\n{\n  \"error\": {\n    \"code\": \"SESSION_NOT_FOUND\",\n    \"message\": \"workspace session not found\",\n    \"workspace_session_id\": \"4f8d2a9c7b2e4a0c8d9e6f1023456789\"\n  }\n}"
    },
    "full_log_path": "",
    "output_preview": ""
  },
  "isError": true
}
```

### 4. 执行业务失败

HTTP 是 200 时，如果 worker execute-tcl 返回 `success != true`，或者 `success == true` 但 `data.result` 中包含 `error` 字样（忽略大小写），MCP 都会映射为执行失败并设置 `isError: true`。

worker execute-tcl 的原始返回形态示例：

```json
{
  "data": {
    "instance_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789",
    "result": "pt_shell>bad command"
  },
  "success": false,
  "message": "bad command"
}
```

MCP 输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=FAILED\nexit_code=1\nerror=TCL_ERROR: bad command\nfull_log_path=/tmp/pandas_mcp/apr.log\noutput_preview:\nbad command\n"
    }
  ],
  "structuredContent": {
    "state": "FAILED",
    "exit_code": 1,
    "error": {
      "code": "TCL_ERROR",
      "message": "bad command"
    },
    "full_log_path": "/tmp/pandas_mcp/apr.log",
    "output_preview": "bad command\n"
  },
  "isError": true
}
```

如果 worker 返回顶层 `message` 字符串，MCP 会把该字符串作为错误 message；如果没有 `message`，会从 `data.result` 去掉开头 `pt_shell>` 或 `innovus>` 后作为错误 message。

## destroy_session

### 输入 schema

```json
{
  "instance_id": "create_session 返回的 session id"
}
```

`instance_id` 使用和 `execute_in_session.session_id` 相同的本地格式校验规则。

worker 请求：

```text
POST {worker_url}/worker/instances/{instance_id}/stop
```

### 1. 成功

输入：

```json
{
  "instance_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789"
}
```

触发条件：

```text
HTTP 请求成功
worker 返回 success == true
worker 返回 data.status == stopped
```

假设 worker 返回：

```json
{
  "success": true,
  "data": {
    "instance_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789",
    "status": "stopped"
  }
}
```

MCP 输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "销毁成功"
    }
  ]
}
```

### 2. MCP 本地请求字段不合法

这些情况会在 MCP 层拦截，不发送 HTTP 请求：

- 缺少 `instance_id`
- `instance_id` 不是字符串或为空字符串
- `instance_id` 格式非法

示例：缺少 `instance_id`。

```json
{}
```

输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "destroy_session failed: missing or empty string argument: instance_id"
    }
  ],
  "isError": true
}
```

示例：`instance_id` 格式非法。

```json
{
  "instance_id": "-bad"
}
```

输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "destroy_session failed: instance_id must be 1-128 ASCII letters, digits, underscores, or hyphens and must start with a letter or digit"
    }
  ],
  "isError": true
}
```

### 3. worker/HTTP 失败

这些问题需要请求 worker 后才能知道：

- worker 连接失败或超时
- session 不存在
- registry/runtime 记录不一致
- worker stop 返回 HTTP `>= 400`
- worker 返回非 JSON 响应
- worker 返回 JSON 顶层不是 object
- worker 内部错误

session 不存在时输出示例：

```json
{
  "content": [
    {
      "type": "text",
      "text": "destroy_session failed: sandbox 请求失败: HTTP 404\n{\n  \"error\": {\n    \"code\": \"SESSION_NOT_FOUND\",\n    \"message\": \"workspace session not found\",\n    \"workspace_session_id\": \"4f8d2a9c7b2e4a0c8d9e6f1023456789\"\n  }\n}"
    }
  ],
  "isError": true
}
```

### 4. HTTP 成功但状态异常

只有 `success == true && data.status == "stopped"` 才算成功，否则 MCP 按 tool error 返回。

假设 worker 返回：

```json
{
  "success": true,
  "data": {
    "status": "running"
  }
}
```

MCP 输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "Session销毁失败\n"
    }
  ],
  "isError": true
}
```

## 协议层错误附录

以下错误不是三个 tool 的业务输入输出，而是 JSON-RPC/MCP 请求本身不合法。

### 未知 tool

输入：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "bad_tool",
    "arguments": {}
  }
}
```

输出：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32603,
    "message": "unknown tool: bad_tool"
  }
}
```

### `arguments` 不是 object

输入：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "execute_in_session",
    "arguments": "bad"
  }
}
```

输出：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32603,
    "message": "tools/call params.arguments must be an object"
  }
}
```

### `params` 不是 object

输入：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/list",
  "params": "bad"
}
```

输出：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32602,
    "message": "params must be an object"
  }
}
```