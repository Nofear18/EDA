# EDA

用于 EDA 工具交互执行与会话监控的项目，包含 Innovus / PrimeTime 沙箱服务、Docker 镜像配置和 Web 监控界面。

## 主要功能

- **沙箱服务**：管理工具进程和工作区会话，通过 HTTP API 执行任务、查询状态与执行历史。
- **MCP 接入**：提供交互式沙箱的 MCP 服务入口。
- **Web 监控**：查看会话列表、状态和执行历史，并关闭会话。
- **镜像配置**：保留多个版本的 Dockerfile，用于构建 EDA 运行环境。

## 目录结构

```text
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

## Docker 镜像

镜像配置位于 `eda_image/`。构建前需准备 Dockerfile 指定的基础镜像和本地资源；部分版本依赖 `bin/claude`，该文件未纳入 Git。请按所选 Dockerfile 准备构建上下文。
