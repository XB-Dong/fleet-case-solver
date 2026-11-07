SET 'execution.checkpointing.interval' = '10 s';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.source.idle-timeout' = '10 s';
CREATE TABLE telemetry_raw (
 event_id BIGINT, vehicle_id STRING, station_id STRING, event_time BIGINT,
 ingest_time BIGINT, battery DOUBLE, speed DOUBLE, online INT,
 ts AS TO_TIMESTAMP_LTZ(event_time * 1000, 3),
 WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
 'connector'='kafka', 'topic'='fleet.telemetry.raw', 'properties.bootstrap.servers'='kafka:29092',
 'properties.group.id'='fleet-clean-v1', 'scan.startup.mode'='earliest-offset', 'format'='json'
);
CREATE TABLE telemetry_clean (
 event_id BIGINT, vehicle_id STRING, station_id STRING, event_time BIGINT,
 ingest_time BIGINT, battery DOUBLE, speed DOUBLE, online INT, lag_seconds BIGINT
) WITH (
 'connector'='kafka', 'topic'='fleet.telemetry.clean', 'properties.bootstrap.servers'='kafka:29092', 'format'='json'
);
CREATE TABLE station_minute (
 station_id STRING, window_start TIMESTAMP(3), event_count BIGINT, avg_battery DOUBLE, max_lag BIGINT
) WITH (
 'connector'='kafka', 'topic'='fleet.station.minute', 'properties.bootstrap.servers'='kafka:29092', 'format'='json'
);
EXECUTE STATEMENT SET
BEGIN
INSERT INTO telemetry_clean
SELECT event_id,vehicle_id,station_id,event_time,ingest_time,battery,speed,online,
 GREATEST(ingest_time-event_time,CAST(0 AS BIGINT))
FROM telemetry_raw WHERE battery BETWEEN 0 AND 100 AND vehicle_id IS NOT NULL;
INSERT INTO station_minute
SELECT station_id,window_start,COUNT(*),AVG(battery),MAX(ingest_time-event_time)
FROM TABLE(TUMBLE(TABLE telemetry_raw,DESCRIPTOR(ts),INTERVAL '1' MINUTE))
GROUP BY station_id,window_start,window_end;
END;
