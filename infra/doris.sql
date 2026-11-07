CREATE DATABASE IF NOT EXISTS fleet;
CREATE TABLE IF NOT EXISTS fleet.telemetry_clean (
 event_id BIGINT, vehicle_id VARCHAR(32), station_id VARCHAR(32), event_time BIGINT,
 ingest_time BIGINT, battery DOUBLE, speed DOUBLE, online INT, lag_seconds BIGINT
)
UNIQUE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 4
PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true");
CREATE TABLE IF NOT EXISTS fleet.station_minute (
 station_id VARCHAR(32), window_start DATETIME, event_count BIGINT, avg_battery DOUBLE, max_lag BIGINT
)
UNIQUE KEY(station_id,window_start)
DISTRIBUTED BY HASH(station_id) BUCKETS 4
PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true");
