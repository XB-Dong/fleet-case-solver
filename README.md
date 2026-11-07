# FleetOps Lab · 无人车云控与智能排障

一个可以复现故障、手动取证、使用只读 Agent 诊断的全栈实验项目。面向无人配送云控平台的运力监控、Oncall 和知识沉淀场景。

**独立构建，全部使用合成数据，不代表任何公司的内部系统。原始参考材料不包含在本仓库或 Git 历史中。**

## 已可运行的闭环

1. 300 辆车、6 个站点、14 天历史，默认生成 **1,209,600 条历史遥测和 16,800 个任务**；运行后会继续写入模拟事件。
2. 运营看板、车辆与任务、SQL 数据工作台、告警工单、人工排查、Agent 和案例库。
3. 三种真实执行的沙箱故障：路线批次中断、遥测数据延迟、运力阈值变更。
4. 模拟时钟推进 → 故障产生 → 告警认领 → 查询证据 → 恢复 → 指标核验 → 结案。
5. 两种 Agent 模式：无需凭证的**确定性策略回放**，以及可配置端点的**真实模型工具调用**。两者清晰标识，不能把回放称为大模型推理。
6. 每次工具调用保存参数、结果和耗时，结论按事实、推断、待确认和排除项分级；生成可下载的 Markdown 案例。

## 快速启动

需要 Python 3.12+ 和 Node.js 22+；本地轻量模式不需要 Docker 或模型 API Key。

```powershell
# Windows PowerShell，在仓库根目录
powershell -ExecutionPolicy Bypass -File scripts/start.ps1
```

```sh
# Linux / macOS
sh scripts/start.sh
```

打开 <http://127.0.0.1:8765>。第一次启动会生成约百万行记录，需要等待；后续启动复用本地数据库。默认只监听回环地址。

已有依赖时可手动启动：

```sh
node node_modules/typescript/bin/tsc --noEmit
node node_modules/vite/bin/vite.js build
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8765
```

请使用安装了 `requirements.txt` 的 Python 环境。前端开发可运行 `npm run dev`，Vite 将 `/api` 代理到 8765。

## 运行配置与验证边界

| 配置 | 实际用途 | 状态 |
| --- | --- | --- |
| Lite | React / TypeScript / ECharts + FastAPI + SQLite + 本地 TTL 缓存 | 已执行功能测试与浏览器验证 |
| Monitoring | Prometheus / Alertmanager / Grafana 读取沙箱 `/metrics` | 已启动；Prometheus / Alertmanager 告警链路实测见验证记录 |
| Full lab | Java / Spring Boot、MySQL、Redis、Kafka、Flink、Doris、复制桥接 | 真实组件已联通；百万条遥测回灌与查询实测见验证记录 |
| Model | 兼容 Chat Completions 的真实工具调用循环 | 协议实现与模拟端点测试；未使用真实凭证调用 |

Full lab 使用同一沙箱生成事件，实际经过 Kafka → Flink → Doris。SQL 工作台可显式切换 SQLite / MySQL / Doris；Agent 回放额外查询 Doris 与 Redis。运营看板与基础诊断证据仍来自模拟器，Java 服务提供真实 MySQL / Redis 读取接口。完整链路为单机实验，不代表生产部署。

```sh
# 仅容器化 Lite
docker compose up --build -d app

# 启用真实监控（Grafana :3001，Prometheus :9090，Alertmanager :9093）
docker compose --profile monitoring up --build -d

# 完整数据栈，建议为 Docker 预留至少 10 GB 内存；按需启动
# PowerShell：$env:FULL_STACK_ENABLED="1"; $env:FLEET_HTTP_PORT="18765"
# bash/zsh：export FULL_STACK_ENABLED=1 FLEET_HTTP_PORT=18765
docker compose --profile full up --build -d
# 服务就绪后提交一次 Flink SQL 作业（不要重复提交同一作业）
docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/pipeline.sql
```

完整栈是本地实验用途，端口仅发布到 `127.0.0.1`。MySQL 默认密码是公开的演示值，可用本地 `.env` 的 `MYSQL_PASSWORD` 覆盖。详见 [完整栈说明](docs/full-stack.md)。

## 3 分钟体验

1. 选择“南山科技园”，点击“注入故障 → 路线同步中断”。
2. 点击“+10 分钟”：车辆心跳仍在线，但可调度车辆下降、任务积压、告警出现。
3. 在“告警与 Oncall”认领事件；在“手动排障”依次查询指标、拒绝原因、日志、接口、TTL、代码。
4. 在“诊断 Agent”运行确定性回放，展开工具调用，逐项核对证据。
5. 点击“恢复沙箱”，再推进数分钟清理积压；填写结案结论并关闭工单。
6. 在“案例知识库”打开记录并下载 Markdown。

Agent 的完整设计、案例推演与面试追问见 **[无人车 Agent 设计与实践](docs/fleet-agent-design.md)**。

更完整的讲解见 [10 分钟演示脚本](docs/demo.md)、[架构与技术选型](docs/architecture.md)、[人工排查 SOP](docs/runbook.md)。

## 可选真实模型模式（本次面试演示不需要）

复制 `.env.example` 为本地 `.env`，填写 `MODEL_API_KEY`、`MODEL_BASE_URL` 和 `MODEL_NAME`。不要将密钥提交到 Git，也不要在聊天中粘贴密钥。

```sh
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8765 --env-file .env
```

刷新页面后选择“真实模型工具调用”。模型最多 18 轮请求，只能调用只读白名单工具；SQL 限制到当前站点和事件窗口。对模型的语义判断仍需人工核验证据，引用编号检查不能保证因果正确。

模型协议参考 [官方工具调用文档](https://developers.openai.com/api/docs/guides/function-calling) 和 [Chat Completions API](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)。默认模型只是可覆盖示例，不保证任意账户或兼容服务均可用。

## MCP 接入

`agent/mcp_server.py` 将正在运行的本地沙箱封装为 stdio MCP：数据字典 + 站点隔离的只读取证。示例配置需把路径换成你的绝对路径：

```json
{
  "mcpServers": {
    "fleet-case-solver": {
      "command": "python",
      "args": ["/absolute/path/to/fleet-case-solver/agent/mcp_server.py"],
      "env": {"FLEET_API_URL": "http://127.0.0.1:8765"}
    }
  }
}
```

此处的 Python 必须安装本项目依赖。工具无法触发故障、回滚配置或修改数据库。

## 测试和公开上传检查

```sh
python -m pytest -q
node node_modules/typescript/bin/tsc --noEmit
node node_modules/vite/bin/vite.js build
docker compose --profile full config --quiet
# 在明确选择文件 git add 后，检查将要上传的暂存区
python scripts/check_public.py
```

测试覆盖故障链路、恢复门禁、数据延迟误判、SQL 只读与站点隔离、状态持久化、规模回归和模型协议。没有捏造排障提效比例或生产 SLA。

## 目录

```text
web/                  React 工作台
backend/              SQLite 数据底座、故障引擎、API、只读 Agent harness
agent/                通用诊断 workflow 与 MCP stdio 入口
services/business/    Spring Boot 业务读取服务
infra/                监控、Flink SQL、MySQL/Doris DDL、复制桥接
tests/                故障与隔离测试
scripts/              启动、数据生成与公开源码检查
docs/                 架构、SOP、完整栈与面试演示说明
```

`data/`、`runtime/`、`.env`、原始材料、数据库及运行案例均被忽略。只发布独立代码和通用方法说明。
