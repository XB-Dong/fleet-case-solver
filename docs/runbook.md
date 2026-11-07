# 人工 Oncall 排查 SOP

## 先确认现场

记录城市/站点、时间窗口、告警对象、业务影响和当前数据新鲜度。不要把遥测显示离线直接等同于车辆真实离线，也不要仅看到状态结果就停止追查机制。

## 路线故障的手动取证

| 动作 | 查询入口 | 要回答的问题 |
| --- | --- | --- |
| 认领工单 | 告警与 Oncall | 谁负责？何时开始？ |
| 对比指标 | 指标与真实心跳 | 在线车辆是否正常，可调度数是否下降？ |
| 统计拒绝 | 调度拒绝原因 | 是否集中在 ROUTE_EXPIRED？ |
| 查同步日志 | 结构化服务日志 | 是否发生数组越界，批次何时中断？ |
| 查原始响应 | 原始路线响应 | 是否存在 routes=[]？ |
| 查当前状态 | 路线缓存 TTL | 哪些车已过期，范围与业务影响是否相符？ |
| 阅读代码 | 登记代码实现 | 异常是否在批次提交前发生？单条异常是否影响整批？ |
| 恢复验证 | 恢复沙箱、推进时间 | 拒绝消失、可调度回升、积压下降了吗？ |
| 结案 | 工单记录 | 根因、操作、验证结果是否完整？ |

具体代码证据由工具实时读取 `backend/route_sync.py`。故意有缺陷的函数与修正版同时保留，用于说明边界检查和单条隔离。

## 数据延迟案例

注入遥测积压，推进超过三分钟。看板在线数下降，但 `query_metrics.actual_online` 仍来自最新心跳。比较事件时间、入库时间和调度成功率。

当前回放应得出“数据陈旧造成离线假象”，并把消费者进程、分区积压的内部原因留作待确认；没有工具证据就不能写“Kafka 某分区崩溃已确认”。

## 配置案例

查 `BATTERY_POLICY` 拒绝与 `config_audit` 的阈值变化。当前值不代表事故前的历史值；应结合变更时间和影响范围。回退前先确认业务意图，沙箱按钮仅演示恢复动作。

## 生命周期

`firing → acknowledged → recovered → closed`。未恢复的工单不能关闭；结案必须填写记录。告警可以在未认领前恢复，但仍保留恢复记录。恢复告警不等于所有积压已清零，应继续核验。

## SQL 示例

```sql
SELECT reason, COUNT(*) AS attempts
FROM dispatch_attempts
WHERE station_id = 'SZ-01'
GROUP BY reason;

SELECT service, level, event_time, trace_id, message
FROM logs WHERE station_id = 'SZ-01'
ORDER BY event_time DESC LIMIT 50;

SELECT config_key, old_value, new_value, event_time
FROM config_audit WHERE station_id = 'SZ-01'
ORDER BY event_time DESC;
```

Agent 通过临时只读数据副本强制站点和时间窗口。人工数据工作台允许查全部合成数据，但同样禁止写入。
