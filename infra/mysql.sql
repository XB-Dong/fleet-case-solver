USE fleet;
CREATE TABLE IF NOT EXISTS vehicles (
 vehicle_id VARCHAR(32) PRIMARY KEY, station_id VARCHAR(32), battery DOUBLE,
 online BOOLEAN, dispatchable BOOLEAN, longitude DOUBLE, latitude DOUBLE, heartbeat_at BIGINT,
 INDEX station_idx(station_id)
);
CREATE TABLE IF NOT EXISTS tasks (
 task_id VARCHAR(64) PRIMARY KEY, vehicle_id VARCHAR(32), station_id VARCHAR(32),
 status VARCHAR(32), created_at BIGINT, finished_at BIGINT, trace_id VARCHAR(96),
 INDEX task_scope(station_id,created_at), INDEX trace_idx(trace_id)
);
