# ScienceEDA MCP 工具调用说明

本文档说明 `interactive_mcp_server.py` 当前版本暴露的 MCP 工具输入输出形态，并按成功、MCP 本地校验失败、daemon/HTTP 失败、业务执行失败等类别整理示例。

## 启动与联调

在本目录安装依赖并启动 stdio MCP 服务：

```bash
python -m pip install httpx
python interactive_mcp_server.py --endpoint http://127.0.0.1:8765
```

`--endpoint` 也可通过 `SCIENCE_EDA_INTERACTIVE_SANDBOX_ENDPOINT` 设置，默认值为 `http://127.0.0.1:8765`。需先启动 ScienceEDA 沙箱后端。

使用 `python check_backend.py --help` 查看真实后端联调参数。脚本会创建会话、执行命令并关闭会话，需要可用的 EDA 工具环境。

两个后端的对照见[接口映射](../../backend_api_mapping.md)。

## 总体规则

- MCP 暴露三个 tool：`create_session`、`execute_in_session`、`destroy_session`。
- `create_session` 和 `destroy_session` 返回 `content` 文本结果。
- `execute_in_session` 同时返回 `content` 和 `structuredContent`：`content` 给 Claude/模型直接阅读，`structuredContent` 给客户端或程序化逻辑读取。
- tool 内部捕获到的失败会返回 `isError: true`，通常不会变成 JSON-RPC 顶层 `error`。
- daemon HTTP 返回 `>= 400` 时，MCP 会把 HTTP 状态码和 daemon 返回体包装进 tool error。
- daemon 响应里用于内部追踪的 `schema_version` 和 `request_id` 会在 MCP 对外错误文本里递归删除。
- JSON-RPC/MCP 请求本身不合法，例如未知 method、`params` 不是对象，属于协议层错误，会返回顶层 `error`，不属于 tool result。

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

- `session_id`：由 MCP 工具内部生成并放入 HTTP 请求体。
- `version`：`create_session` 不需要该字段。

MCP 发给 daemon 的 HTTP body 形态如下，其中 `session_id` 是工具内部生成的裸 UUID hex：

```json
{
  "tool_kind": "innovus",
  "workspace_path": "D:\\code\\gitcode\\ScienceEDA\\tests",
  "session_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789"
}
```

### 1. 成功

MCP tool 输入：

```json
{
  "tool_kind": "innovus",
  "workspace_path": "D:\\code\\gitcode\\ScienceEDA\\tests"
}
```

触发条件：

```text
HTTP 请求成功
workspace_session.state == ACTIVE
并且响应中能拿到 session_id 或 workspace_session.workspace_session_id
```

假设 daemon 返回：

```json
{
  "session_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789",
  "workspace_session": {
    "state": "ACTIVE"
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

这些情况会在 MCP 层拦截，不发送 HTTP 请求：

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

输入：

```json
{
  "tool_kind": "dc_shell",
  "workspace_path": "D:\\code\\gitcode\\ScienceEDA\\tests"
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

示例：`workspace_path` 不是绝对路径。

输入：

```json
{
  "tool_kind": "innovus",
  "workspace_path": "tests"
}
```

输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "create_session failed: workspace_path must be an absolute path"
    }
  ],
  "isError": true
}
```

### 3. daemon/HTTP 失败

这些问题依赖 daemon registry、capacity、runtime 启动状态或外部进程行为，MCP 层不能可靠提前判断，只能发送 HTTP 后包装 daemon 的返回结果：

- daemon 连接失败或超时
- 同一个内部生成的 `session_id` 与已有记录冲突，理论概率极低，但仍由 daemon 最终判断
- pending create 或 active session 容量不足
- scheduler/capacity 等待超时
- runtime 启动失败
- daemon 正在关闭
- registry/runtime 内部异常

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

HTTP `>= 400` 输出示例。注意：`error.code`、`message`、`details` 是 daemon 返回的内容，MCP 只负责包装和去掉内部字段。

```json
{
  "content": [
    {
      "type": "text",
      "text": "create_session failed: sandbox 请求失败: HTTP 503\n{\n  \"error\": {\n    \"code\": \"CREATE_CAPACITY_TIMEOUT\",\n    \"message\": \"interactive runtime capacity wait timed out\"\n  }\n}"
    }
  ],
  "isError": true
}
```

### 4. HTTP 成功但状态异常

如果 daemon HTTP 返回成功，但 MCP 没有看到 `workspace_session.state == ACTIVE`，或者拿不到 session id，MCP 会按 tool error 返回。

输入：

```json
{
  "tool_kind": "innovus",
  "workspace_path": "D:\\code\\gitcode\\ScienceEDA\\tests"
}
```

假设 daemon 返回：

```json
{
  "session_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789",
  "workspace_session": {
    "state": "CLOSED"
  }
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

MCP 发给 daemon 的 HTTP body 形态如下：

```json
{
  "request_id": "mcp-9f4f0b7c8a7e4f619a3c2b1d0e9f8123",
  "code": "puts hi"
}
```

说明：`request_id` 是 MCP 内部给 execute 请求生成的幂等 id，不属于 tool 输入。HTTP 错误返回给用户时，`request_id` 会被递归删除。

### 1. 成功

输入：

```json
{
  "session_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789",
  "command": "puts hi"
}
```

假设 daemon 返回：

```json
{
  "state": "SUCCEEDED",
  "result": {
    "exit_code": 0,
    "error": null,
    "output": {
      "preview": "hi\n",
      "full_log": {
        "path": "/tmp/science_eda/execution.log"
      }
    }
  }
}
```

MCP 输出：

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=SUCCEEDED\nexit_code=0\nerror=null\nfull_log_path=/tmp/science_eda/execution.log\noutput_preview:\nhi\n"
    }
  ],
  "structuredContent": {
    "state": "SUCCEEDED",
    "exit_code": 0,
    "error": null,
    "full_log_path": "/tmp/science_eda/execution.log",
    "output_preview": "hi\n"
  }
}
```

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

`session_id` 格式规则：1-128 个 ASCII 字母、数字、下划线或连字符，并且必须以字母或数字开头。

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

输入：

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

### 3. daemon/HTTP 失败

这些问题需要请求 daemon 后才能知道：

- daemon 连接失败或超时
- session 不存在
- session 已关闭或 runtime stopped
- runtime lost
- session busy
- code 过大
- request id 幂等冲突
- session log quota 耗尽
- daemon 内部错误

输入示例：

```json
{
  "session_id": "4f8d2a9c7b2e4a0c8d9e6f1023456789",
  "command": "puts hi"
}
```

session 不存在时输出示例。注意 daemon 返回体内容以实际 daemon 为准，MCP 只负责包装。

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

session busy 时输出示例：

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=\nexit_code=null\nerror=MCP_TOOL_ERROR: execute_in_session failed: sandbox 请求失败: HTTP 409\n{\n  \"error\": {\n    \"code\": \"SESSION_BUSY\",\n    \"message\": \"interactive runtime already has an active execution\",\n    \"workspace_session_id\": \"4f8d2a9c7b2e4a0c8d9e6f1023456789\"\n  }\n}\nfull_log_path=\noutput_preview:\n"
    }
  ],
  "structuredContent": {
    "state": "",
    "exit_code": null,
    "error": {
      "code": "MCP_TOOL_ERROR",
      "message": "execute_in_session failed: sandbox 请求失败: HTTP 409\n{\n  \"error\": {\n    \"code\": \"SESSION_BUSY\",\n    \"message\": \"interactive runtime already has an active execution\",\n    \"workspace_session_id\": \"4f8d2a9c7b2e4a0c8d9e6f1023456789\"\n  }\n}"
    },
    "full_log_path": "",
    "output_preview": ""
  },
  "isError": true
}
```

code 过大时输出示例。注意输出里没有 `request_id` 字段，即使 daemon 原始响应包含该字段，MCP 也会删除。

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=\nexit_code=null\nerror=MCP_TOOL_ERROR: execute_in_session failed: sandbox 请求失败: HTTP 400\n{\n  \"error\": {\n    \"code\": \"CODE_TOO_LARGE\",\n    \"details\": {\n      \"actual_code_bytes\": 2000000,\n      \"max_code_bytes\": 1048576\n    },\n    \"message\": \"code exceeds 1048576 UTF-8 bytes\",\n    \"workspace_session_id\": \"4f8d2a9c7b2e4a0c8d9e6f1023456789\"\n  }\n}\nfull_log_path=\noutput_preview:\n"
    }
  ],
  "structuredContent": {
    "state": "",
    "exit_code": null,
    "error": {
      "code": "MCP_TOOL_ERROR",
      "message": "execute_in_session failed: sandbox 请求失败: HTTP 400\n{\n  \"error\": {\n    \"code\": \"CODE_TOO_LARGE\",\n    \"details\": {\n      \"actual_code_bytes\": 2000000,\n      \"max_code_bytes\": 1048576\n    },\n    \"message\": \"code exceeds 1048576 UTF-8 bytes\",\n    \"workspace_session_id\": \"4f8d2a9c7b2e4a0c8d9e6f1023456789\"\n  }\n}"
    },
    "full_log_path": "",
    "output_preview": ""
  },
  "isError": true
}
```

### 4. 执行业务失败

HTTP 是 200，但 execution 本身失败、超时或 runtime 丢失时，MCP 会检查以下条件并设置 `isError: true`：

```text
state in FAILED/TIMED_OUT/LOST
或 exit_code != 0
或 result.error != null
```

Tcl/EDA 命令失败：

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=FAILED\nexit_code=1\nerror=TCL_ERROR: bad command\nfull_log_path=/tmp/science_eda/execution.log\noutput_preview:\nbad command\n"
    }
  ],
  "structuredContent": {
    "state": "FAILED",
    "exit_code": 1,
    "error": {
      "code": "TCL_ERROR",
      "message": "bad command"
    },
    "full_log_path": "/tmp/science_eda/execution.log",
    "output_preview": "bad command\n"
  },
  "isError": true
}
```

执行超时：

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=TIMED_OUT\nexit_code=null\nerror=EXECUTION_TIMEOUT: execution exceeded 300000 ms\nfull_log_path=/tmp/science_eda/execution.log\noutput_preview:\n"
    }
  ],
  "structuredContent": {
    "state": "TIMED_OUT",
    "exit_code": null,
    "error": {
      "code": "EXECUTION_TIMEOUT",
      "message": "execution exceeded 300000 ms"
    },
    "full_log_path": "/tmp/science_eda/execution.log",
    "output_preview": ""
  },
  "isError": true
}
```

执行中 runtime 丢失：

```json
{
  "content": [
    {
      "type": "text",
      "text": "state=LOST\nexit_code=null\nerror=PROCESS_LOST: interactive tool process was lost during execution\nfull_log_path=/tmp/science_eda/execution.log\noutput_preview:\npartial output\n"
    }
  ],
  "structuredContent": {
    "state": "LOST",
    "exit_code": null,
    "error": {
      "code": "PROCESS_LOST",
      "message": "interactive tool process was lost during execution"
    },
    "full_log_path": "/tmp/science_eda/execution.log",
    "output_preview": "partial output\n"
  },
  "isError": true
}
```

## destroy_session

### 输入 schema

```json
{
  "instance_id": "create_session 返回的 session id"
}
```

`instance_id` 使用和 `execute_in_session.session_id` 相同的本地格式校验规则。

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
响应顶层 state == CLOSED
```

输出：

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

如果 session 已经是 `CLOSED`，daemon 仍返回 `state=CLOSED`，MCP 也按成功处理。

### 2. MCP 本地请求字段不合法

这些情况会在 MCP 层拦截，不发送 HTTP 请求：

- 缺少 `instance_id`
- `instance_id` 不是字符串或为空字符串
- `instance_id` 格式非法

示例：缺少 `instance_id`。

输入：

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

输入：

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

### 3. daemon/HTTP 失败

这些问题需要请求 daemon 后才能知道：

- daemon 连接失败或超时
- session 不存在
- registry/runtime 记录不一致
- daemon 内部错误

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

内部错误时输出示例：

```json
{
  "content": [
    {
      "type": "text",
      "text": "destroy_session failed: sandbox 请求失败: HTTP 500\n{\n  \"error\": {\n    \"code\": \"INTERNAL_ERROR\",\n    \"message\": \"internal interactive sandbox error\",\n    \"workspace_session_id\": \"4f8d2a9c7b2e4a0c8d9e6f1023456789\"\n  }\n}"
    }
  ],
  "isError": true
}
```

### 4. HTTP 成功但状态异常

只有响应顶层 `state == CLOSED` 才算成功，否则 MCP 按 tool error 返回。

假设 daemon 返回：

```json
{
  "state": "ACTIVE"
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
