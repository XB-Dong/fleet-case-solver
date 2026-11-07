# 完整栈实验配置

当前交付包含可构建的组件定义，但此电脑未启动 Docker 引擎，因此**尚未验证容器启动与端到端流处理**。请先完成下面的检查再将其描述为已联调系统。

## 连接关系

```text
Lite API ── bridge ── MySQL ── Spring Boot 查询 API (:8080)
                  ├─ Redis ── Spring Boot 路线缓存 API
                  └─ Kafka raw ── Flink SQL ── Kafka clean / minute ── bridge ── Doris

Lite /metrics ── Prometheus ── Alertmanager / Grafana
```

源端桥接读取真实生成的 SQLite 数据，经 HTTP 分页发送 Kafka。Flink 清洗电量范围、计算数据延迟、执行一分钟事件时间窗口。Doris 使用唯一键支持重复事件覆盖。

这是 at-least-once 实验链路，不保证 exactly-once。源端重启可能重放；Doris 明细唯一键可去重，但窗口计数在重放时可能重复。Kafka 与 Flink checkpoint 未配置持久卷，容器重建不是生产级恢复演示。

## 启动与检查

1. 先释放内存并启动 Docker Desktop Linux 引擎；检查 `docker info`。
2. `docker compose --profile full config --quiet` 检查配置。
3. `docker compose --profile full up --build -d`。
4. 检查 `docker compose ps` 与各服务日志，等待 MySQL、Doris、Kafka、Flink 就绪。
5. 提交 Flink SQL：`docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/pipeline.sql`。
6. Flink UI <http://127.0.0.1:8081> 中确认一个运行中的 Statement Set 作业。
7. 业务 API：<http://127.0.0.1:8080/business/vehicles?station=SZ-01>。
8. Doris SQL 端口 9030，查询 `fleet.telemetry_clean` 和 `fleet.station_minute`；不能只看容器 up 就宣称数据打通。
9. Grafana <http://127.0.0.1:3000> 查看 FleetOps Dashboard。

首次回灌百万行可能需要较长时间。镜像与 connector 下载需要联网。Doris 单节点镜像仅用于实验；若当前平台无法运行，请按 [Doris 官方快速部署文档](https://doris.apache.org/docs/3.x/gettingStarted/quick-start/) 调整版本与主机参数，不能静默替换成 SQLite 后仍声称 Doris 已运行。

Flink Docker 与 SQL connector 参考 [官方 Docker 文档](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/standalone/docker/)。固定为 Flink 1.20.x 与匹配 connector，避免任意混搭版本。

## 与 Lite 的差异

- 前端与诊断工具继续读取 Lite，避免在完整栈未就绪时隐式切换数据来源。
- Java 服务是真实读取 MySQL 和 Redis 的扩展示例，尚未替换故障引擎。
- 任务复制是每站点最新 100 条快照，不是全量 CDC。
- Redis TTL 用墙上时钟倒计时，桥接根据模拟时钟刷新；暂停期间与纯 Lite 内存状态存在短暂差异。
- 历史数据应按时间递增回灌，否则 Flink 的水位线会丢弃过晚窗口事件。新生成库已按此顺序建立。
- 重置沙箱会复用合成 ID；完整栈运行中应先停止桥接与 Flink，并明确清理实验主题/表后重新回灌，避免旧窗口污染新实验。脚本不会自动删除完整栈数据。
- Alertmanager 配置只在本地展示，不对外发消息。

## 故障与恢复

完整栈复用 Lite 故障事件的复制和观测；当前三个故障并不自动停止真实 Kafka/Flink 容器。若需要基础设施级故障，应另行记录真实消费者 lag、checkpoint 状态、反压与恢复时间，并与业务时间关联。
