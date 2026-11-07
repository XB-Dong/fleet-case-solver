# 完整栈实验配置

2026-11-07 已启动真实 MySQL、Redis、Kafka、Flink、Doris、Spring Boot 和监控组件，并运行实际数据库查询。完整数据量与场景验证结果见 [验证记录](validation.md)。Agent 按面试演示要求使用确定性策略回放，不依赖模型凭证。

## 连接关系

```text
Lite API ── bridge ── MySQL ── Spring Boot 查询 API (:8080)
                  ├─ Redis ── Spring Boot 路线缓存 API
                  └─ Kafka raw ── Flink SQL ── Kafka clean / minute ── bridge ── Doris

Lite /metrics ── Prometheus ── Alertmanager / Grafana
```

源端桥接读取真实生成的 SQLite 数据，经 HTTP 分页发送 Kafka。Flink 清洗电量范围、计算数据延迟、执行一分钟事件时间窗口。Doris 使用唯一键支持重复事件覆盖。

这是 at-least-once 实验链路，不保证 exactly-once。源端在 Redis DB 1 保存 Kafka 已确认批次的游标；确认到游标写入之间崩溃或 Redis 丢失仍可能重放；Doris 明细唯一键可去重，但窗口计数在重放时可能重复。Kafka 与 Flink checkpoint 未配置持久卷，容器重建不是生产级恢复演示。

## 启动与检查

1. 先释放内存并启动 Docker Desktop Linux 引擎；检查 `docker info`。
2. `docker compose --profile full config --quiet` 检查配置。
3. PowerShell 设置 `$env:FULL_STACK_ENABLED="1"; $env:FLEET_HTTP_PORT="18765"`，再执行 `docker compose --profile full up --build -d`。bash/zsh 可用 `export FULL_STACK_ENABLED=1 FLEET_HTTP_PORT=18765`。工作台打开 <http://127.0.0.1:18765>。
4. 检查 `docker compose ps` 与各服务日志，等待 MySQL、Doris、Kafka、Flink 就绪。
5. 提交 Flink SQL：`docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/pipeline.sql`。
6. Flink UI <http://127.0.0.1:8081> 中确认一个运行中的 Statement Set 作业。
7. 业务 API：<http://127.0.0.1:8080/business/vehicles?station=SZ-01>。
8. Doris SQL 端口 9030，查询 `fleet.telemetry_clean` 和 `fleet.station_minute`；不能只看容器 up 就宣称数据打通。
9. Grafana <http://127.0.0.1:3001> 查看 FleetOps Dashboard。

首次回灌百万行可能需要较长时间。镜像与 connector 下载需要联网。Doris 使用官方 [Compose 文档](https://doris.apache.org/docs/dev/observability/dogstack/docker/) 所列的 `4.0.3-all-slim`，仅用于连接性实验；本配置不授予 privileged 权限。若当前平台无法运行，应检查宿主机内存映射与文件句柄参数，不能静默替换成 SQLite 后仍声称 Doris 已运行。

Flink Docker 与 SQL connector 参考 [官方 Docker 文档](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/standalone/docker/)。固定为 Flink 1.20.x 与匹配 connector，避免任意混搭版本。

## 与 Lite 的差异

- 运营总览和基础诊断读取模拟器；SQL 工作台显式选择 SQLite / MySQL / Doris。真实组件失败时返回错误，禁止回退 SQLite 冒充真实结果。
- Agent 回放增加 `query_warehouse` 与 `inspect_live_cache`，真实查询当前事件窗口 Doris 遥测与 Redis TTL；不同来源各自标明。
- Java 服务是真实读取 MySQL 和 Redis 的扩展示例，尚未替换故障引擎。
- 任务复制是每站点最新 100 条快照，不是全量 CDC。
- Redis TTL 用墙上时钟倒计时，桥接根据模拟时钟刷新；暂停期间与纯 Lite 内存状态存在短暂差异。
- 历史数据应按时间递增回灌，否则 Flink 的水位线会丢弃过晚窗口事件。新生成库已按此顺序建立。
- Full 模式禁用重置；先恢复当前故障，再切换下一场景，保持事件时间单调。脚本不会自动删除完整栈数据。
- Alertmanager 配置只在本地展示，不对外发消息。

## 故障与恢复

完整栈复用 Lite 故障事件的复制和观测；当前三个故障并不自动停止真实 Kafka/Flink 容器。若需要基础设施级故障，应另行记录真实消费者 lag、checkpoint 状态、反压与恢复时间，并与业务时间关联。

## 可复现验收

```sh
# 只读核对所有组件、全量遥测与聚合
python scripts/verify_full_stack.py --base http://127.0.0.1:18765
# 明确执行故障注入、Oncall、Agent、监控告警、恢复和结案
python scripts/verify_full_stack.py --base http://127.0.0.1:18765 --scenario --monitoring
```

报告保存到本地 `runtime/full-stack-verification.json`，不会提交 Git。首次回灌需等待，不能在回灌未完成时把空的现场查询理解为正常。脚本默认最多等待 20 分钟。

本机验证时 Docker 约 7.6 GiB 可用，采用实验内存配置：Flink JobManager 768 MiB 进程内存，TaskManager 1024 MiB；Doris FE 堆 768 MiB、BE 上限 2600 MiB（Doris 容器上限 4 GiB）。该设置用于低并发演示，没有生产容量结论。Grafana 默认主机端口 3001，可用 `GRAFANA_HTTP_PORT` 覆盖。
