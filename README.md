# EDA

用于 EDA 工具交互执行与会话监控的项目，包含 Innovus / PrimeTime 沙箱服务、Docker 镜像配置和 Web 监控界面。

## 主要功能

- **沙箱服务**：管理工具进程和工作区会话，通过 HTTP API 执行任务、查询状态与执行历史。
- **MCP 接入**：提供 ScienceEDA 和 pandas worker 两套适配器，统一支持创建会话、执行 Tcl 和关闭会话。
- **Web 监控**：查看会话列表、状态和执行历史，并关闭会话。
- **镜像配置**：保留多个版本的 Dockerfile，用于构建 EDA 运行环境。

## 目录结构

```text
MCP_for_EDA_tools/
├── README.md            # MCP 适配器使用入口
├── science_eda/         # ScienceEDA 适配器、联调脚本与说明
└── pandas/              # pandas 适配器、联调脚本与接入方案
backend_api_mapping.md  # 两种后端的接口与字段映射
eda_image/
├── Dockerfile*           # 镜像构建配置
├── requirements-sandbox.txt # 沙箱服务依赖
└── science_eda/          # 沙箱服务代码
sandbox_monitor/
├── server.py            # 监控页面后端与沙箱 API 代理
├── requirements.txt     # 监控服务依赖
└── static/              # HTML、CSS 和 JavaScript
```

## 启动沙箱服务

在已配置 EDA 工具及有效许可证的 Linux 环境中，使用 Python 3.10 或更新版本。从仓库根目录执行：

```bash
cd eda_image
python -m pip install -r requirements-sandbox.txt
python -m science_eda.sandbox.server --config science_eda/sandbox/science_eda.example.yaml
```

示例配置启用交互式 API，默认监听 `127.0.0.1:8765`。运行前请根据环境调整工具路径和会话配置，详见[沙箱文档](eda_image/science_eda/sandbox/README.md)及[配置示例](eda_image/science_eda/sandbox/science_eda.example.yaml)。

## 启动监控界面

在另一个终端中，从仓库根目录执行：

```bash
python -m pip install -r sandbox_monitor/requirements.txt
python sandbox_monitor/server.py
```

浏览器访问 `http://localhost:8766`。监控界面需要连接已启动的沙箱服务；默认连接本机的 `8765` 端口。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SANDBOX_HOST` | `http://127.0.0.1:8765` | 监控后端连接的沙箱地址 |
| `MONITOR_PORT` | `8766` | 监控页面端口 |
| `SANDBOX_TIMEOUT` | `30` | 代理请求超时时间，单位为秒 |

## MCP 工具接入

两套适配器通过 stdio 提供 `create_session`、`execute_in_session` 和 `destroy_session`，分别连接 ScienceEDA 沙箱与 pandas EDA worker。此处 pandas 指 EDA worker 服务。

在仓库根目录安装依赖，再按后端选择启动命令：

```bash
python -m pip install httpx

# ScienceEDA 沙箱
python MCP_for_EDA_tools/science_eda/interactive_mcp_server.py --endpoint http://127.0.0.1:8765

# pandas 控制面：请替换为实际部署地址
python MCP_for_EDA_tools/pandas/interactive_mcp_server.py --endpoint http://control-plane.example:8765
```

MCP 客户端使用上述命令启动适配器；底层 HTTP 后端需已启动。pandas 也可通过 `--worker-url` 直接指定 worker。工作区必须在 MCP 本机及后端以相同绝对路径可访问。

- [MCP 目录说明](MCP_for_EDA_tools/README.md)
- [ScienceEDA 工具说明](MCP_for_EDA_tools/science_eda/README.md)
- [pandas 工具说明](MCP_for_EDA_tools/pandas/README.md)
- [pandas 接入方案](MCP_for_EDA_tools/pandas/integration_plan.md)
- [后端 API 映射](backend_api_mapping.md)

各后端目录中的 `check_backend.py` 用于真实环境联调，运行前可用 `--help` 查看参数。

## Docker 镜像

镜像配置位于 `eda_image/`。构建前需准备 Dockerfile 指定的基础镜像和本地资源；部分版本依赖 `bin/claude`，该文件未纳入 Git。请按所选 Dockerfile 准备构建上下文。
