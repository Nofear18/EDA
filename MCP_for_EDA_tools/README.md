# EDA 工具 MCP 适配器

本目录提供 ScienceEDA 和 pandas worker 两种后端的 stdio MCP 适配器，对外统一暴露 `create_session`、`execute_in_session`、`destroy_session`。这里的 pandas 指 EDA worker 服务，不是 Python 数据分析库。

## 文件组织

| 目录 / 文件 | 用途 |
| --- | --- |
| `science_eda/interactive_mcp_server.py` | ScienceEDA HTTP API 适配器 |
| `pandas/interactive_mcp_server.py` | pandas worker HTTP API 适配器 |
| 各目录的 `check_backend.py` | 真实后端联调脚本 |
| 各目录的 `README.md` | 启动参数、工具输入输出与错误示例 |
| `pandas/integration_plan.md` | pandas 接入流程、字段映射和验收方案 |

## 使用方式

使用 Python 3.10 或更新版本，在仓库根目录安装依赖，再选择一个后端启动：

```bash
python -m pip install httpx

# ScienceEDA：直接连接沙箱 daemon
python MCP_for_EDA_tools/science_eda/interactive_mcp_server.py --endpoint http://127.0.0.1:8765

# pandas：通过控制面发现 worker，替换为实际部署地址
python MCP_for_EDA_tools/pandas/interactive_mcp_server.py --endpoint http://control-plane.example:8765
```

上述命令运行 stdio MCP 服务，由 MCP 客户端通过标准输入输出通信。底层 HTTP 服务需另行启动。`workspace_path` 必须是 MCP 本机存在的绝对目录，并能被后端以相同路径访问。

运行对应目录的 `check_backend.py --help` 查看联调参数；实际联调会创建和销毁 EDA 会话，并执行成功及失败用例。

## 文档

- [ScienceEDA 工具说明](science_eda/README.md)
- [pandas 工具说明](pandas/README.md)
- [pandas 接入方案](pandas/integration_plan.md)
- [后端接口映射](../backend_api_mapping.md)

## 命名调整

原 `ours/` 改为 `science_eda/`；两套适配器统一使用 `interactive_mcp_server.py`、`check_backend.py`、`README.md`。原 `pandas_docs.txt` 已整理为 `pandas/integration_plan.md`。
