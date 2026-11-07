import json
import random
import sqlite3
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
DB = DATA / 'fleet.db'
BASE_TIME = 1794024000  # 2026-11-07 12:00 Asia/Shanghai
SCHEMA = '''
CREATE TABLE IF NOT EXISTS stations(station_id TEXT PRIMARY KEY, name TEXT, city TEXT, longitude REAL, latitude REAL);
CREATE TABLE IF NOT EXISTS vehicles(vehicle_id TEXT PRIMARY KEY, station_id TEXT, battery REAL, online INTEGER, dispatchable INTEGER, longitude REAL, latitude REAL, heartbeat_at INTEGER);
CREATE TABLE IF NOT EXISTS tasks(task_id TEXT PRIMARY KEY, vehicle_id TEXT, station_id TEXT, status TEXT, created_at INTEGER, finished_at INTEGER, trace_id TEXT);
CREATE TABLE IF NOT EXISTS telemetry(event_id INTEGER PRIMARY KEY, vehicle_id TEXT, station_id TEXT, event_time INTEGER, ingest_time INTEGER, battery REAL, speed REAL, online INTEGER);
CREATE INDEX IF NOT EXISTS telemetry_scope ON telemetry(station_id,event_time);
CREATE INDEX IF NOT EXISTS telemetry_vehicle ON telemetry(vehicle_id,event_time);
CREATE INDEX IF NOT EXISTS tasks_scope ON tasks(station_id,created_at);
CREATE TABLE IF NOT EXISTS dispatch_attempts(id INTEGER PRIMARY KEY, task_id TEXT, vehicle_id TEXT, station_id TEXT, reason TEXT, event_time INTEGER, trace_id TEXT);
CREATE TABLE IF NOT EXISTS logs(id INTEGER PRIMARY KEY, service TEXT, level TEXT, station_id TEXT, event_time INTEGER, trace_id TEXT, message TEXT);
CREATE TABLE IF NOT EXISTS config_audit(id INTEGER PRIMARY KEY, station_id TEXT, event_time INTEGER, config_key TEXT, old_value TEXT, new_value TEXT, operator TEXT);
CREATE TABLE IF NOT EXISTS metric_history(event_time INTEGER, station_id TEXT, online INTEGER, dispatchable INTEGER, backlog INTEGER, success_rate REAL, lag_seconds INTEGER);
CREATE TABLE IF NOT EXISTS alerts(alert_id TEXT PRIMARY KEY, station_id TEXT, title TEXT, severity TEXT, status TEXT, owner TEXT, opened_at INTEGER, recovered_at INTEGER, notes TEXT);
CREATE TABLE IF NOT EXISTS runtime_state(key TEXT PRIMARY KEY, value TEXT);
'''


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def connect(path=DB):
    conn = sqlite3.connect(path, timeout=20, factory=ClosingConnection)
    conn.row_factory = sqlite3.Row
    return conn


def seed(path=DB, days=14, vehicle_count=300):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(path)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.executescript(SCHEMA)
    if conn.execute('SELECT COUNT(*) FROM vehicles').fetchone()[0]:
        conn.close()
        return
    rng = random.Random(42)
    stations = [('SZ-01', '南山科技园', '深圳', 113.947, 22.543), ('SZ-02', '福田中心', '深圳', 114.057, 22.535), ('SZ-03', '宝安滨海', '深圳', 113.891, 22.555), ('SZ-04', '龙华园区', '深圳', 114.028, 22.652), ('SZ-05', '前海自贸区', '深圳', 113.891, 22.518), ('SZ-06', '大学城', '深圳', 113.964, 22.591)]
    conn.executemany('INSERT INTO stations VALUES(?,?,?,?,?)', stations)
    vehicles = [(f'AV-{i+1:04d}', stations[i % 6][0], rng.randint(35, 98), 1, 1, stations[i % 6][3] + rng.uniform(-.008,.008), stations[i % 6][4] + rng.uniform(-.006,.006), BASE_TIME) for i in range(vehicle_count)]
    conn.executemany('INSERT INTO vehicles VALUES(?,?,?,?,?,?,?,?)', vehicles)
    tasks = []
    for d in range(days):
        for i in range(1200):
            v = vehicles[i % vehicle_count]
            at = BASE_TIME - (d+1)*86400 + i*60
            tasks.append((f'T-{d:02d}-{i:05d}', v[0], v[1], 'completed' if i % 43 else 'timeout', at, at + rng.randint(300,1200), f'tr-{d:02d}-{i:05d}'))
    conn.executemany('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)', tasks)
    # Bounded batches keep the million-row seed usable on ordinary laptops.
    for d in reversed(range(days)):
        rows = ((None, v[0], v[1], BASE_TIME-(d+1)*86400+slot*300, BASE_TIME-(d+1)*86400+slot*300+2, max(20,v[2]-slot%12), round(rng.uniform(0,18),2), 1) for slot in range(288) for v in vehicles)
        conn.executemany('INSERT INTO telemetry VALUES(?,?,?,?,?,?,?,?)', rows)
        conn.commit()
    conn.close()


TABLE_NOTES = {
    'stations': '站点维表；经纬度为合成示意坐标',
    'vehicles': '当前车辆状态；online 为真实心跳状态，dispatchable 为业务可调度状态',
    'tasks': '配送任务；时间为 Unix 秒；status=completed/timeout/queued',
    'telemetry': '遥测明细；event_time=事件时间，ingest_time=入库时间，二者差值为数据延迟',
    'dispatch_attempts': '调度决策明细；reason 是拒绝原因或 ACCEPTED',
    'logs': '结构化服务日志；trace_id 用于关联任务',
    'config_audit': '历史配置审计；不能仅用当前配置解释历史异常',
    'metric_history': '一分钟粒度的模拟运行指标',
    'alerts': '告警工单；firing → acknowledged → recovered → closed',
}


def context(path=DB):
    with connect(path) as conn:
        return {table: {'description': note, 'columns': [dict(r) for r in conn.execute(f'PRAGMA table_info({table})')], 'rows': conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]} for table,note in TABLE_NOTES.items()}


def query(sql, path=DB):
    if len(sql) > 12000:
        raise ValueError('SQL 过长')
    if not sql.lstrip().lower().startswith(('select ', 'with ')):
        raise ValueError('仅允许 SELECT / WITH 查询')
    conn = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE}
    def authorize(action, arg1, arg2, db, trigger):
        if action == sqlite3.SQLITE_READ and arg1 not in TABLE_NOTES:
            return sqlite3.SQLITE_DENY
        if action == sqlite3.SQLITE_FUNCTION and str(arg2).lower() in {'load_extension','writefile','readfile'}:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY
    conn.set_authorizer(authorize)
    started = time.monotonic()
    conn.set_progress_handler(lambda: int(time.monotonic()-started > 3), 10000)
    try:
        cur = conn.execute(sql)
        rows = [dict(r) for r in cur.fetchmany(201)]
        return {'rows': rows[:200], 'truncated': len(rows)>200, 'elapsed_ms': round((time.monotonic()-started)*1000,2), 'columns': [d[0] for d in cur.description or []]}
    finally:
        conn.close()
