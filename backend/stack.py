"""Actual Docker data-source adapters; no silent SQLite fallback."""
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import httpx
import pymysql
import redis
import sqlglot
from sqlglot import exp

TABLES={
    'mysql': {'vehicles':'MySQL 业务车辆快照', 'tasks':'MySQL 最新任务快照'},
    'doris': {'telemetry_clean':'Kafka → Flink 清洗后的 Doris 明细', 'station_minute':'Flink 事件时间窗口聚合，Doris 持久化'},
}


def enabled(): return os.getenv('FULL_STACK_ENABLED','0')=='1'


def connect(source):
    if source not in TABLES: raise ValueError('未知数据源')
    prefix=source.upper()
    return pymysql.connect(host=os.getenv(prefix+'_HOST',source),port=int(os.getenv(prefix+'_PORT','3306' if source=='mysql' else '9030')),user=os.getenv(prefix+'_READ_USER','fleet_reader'),password=os.getenv(prefix+'_READ_PASSWORD','fleet_reader_demo'),database='fleet',cursorclass=pymysql.cursors.DictCursor,connect_timeout=3,read_timeout=8,write_timeout=3,autocommit=True)


def normalize(row):
    return {k:float(v) if isinstance(v,Decimal) else v.isoformat() if hasattr(v,'isoformat') else v for k,v in row.items()}


def validate_sql(sql,source):
    if source not in TABLES or len(sql)>12000: raise ValueError('未知数据源或 SQL 过长')
    try: statements=sqlglot.parse(sql,read='mysql')
    except sqlglot.errors.ParseError as exc: raise ValueError('SQL 解析失败，仅允许受支持的 SELECT') from exc
    if len(statements)!=1 or not isinstance(statements[0],exp.Select): raise ValueError('仅支持单条 SELECT 查询')
    tree=statements[0]
    if any(isinstance(n,(exp.Into,exp.Lock,exp.Command)) for n in tree.walk()): raise ValueError('禁止 SELECT INTO / 锁定 / 命令')
    if re.search(r'\b(sleep|benchmark|load_file|get_lock|release_lock|outfile|dumpfile)\b',sql,re.I): raise ValueError('禁止副作用或资源滥用函数')
    for table in tree.find_all(exp.Table):
        if table.db or table.catalog or table.name not in TABLES[source]: raise ValueError('仅可查询当前数据源的登记表')
    return tree.sql(dialect='mysql')


def query(sql,source):
    sql=validate_sql(sql,source)
    # Bound the database result itself; buffered PyMySQL cursors otherwise read every row.
    tree=sqlglot.parse_one(sql,read='mysql')
    limit=tree.args.get('limit')
    requested=int(limit.expression.this) if limit and isinstance(limit.expression,exp.Literal) and limit.expression.is_int else 201
    sql=tree.limit(min(max(requested,0),201),copy=False).sql(dialect='mysql')
    started=time.monotonic()
    conn=connect(source)
    try:
        with conn.cursor() as c:
            c.execute('SET SESSION max_execution_time=3000' if source=='mysql' else 'SET query_timeout=3')
            c.execute(sql)
            rows=[normalize(r) for r in c.fetchmany(201)]
            return {'source':source,'rows':rows[:200],'truncated':len(rows)>200,'columns':[d[0] for d in c.description or []],'elapsed_ms':round((time.monotonic()-started)*1000,2)}
    finally: conn.close()


def context(source):
    conn=connect(source); tables={}
    try:
        with conn.cursor() as c:
            for name,note in TABLES[source].items():
                c.execute(f'DESCRIBE `{name}`')
                cols=[{'name':r['Field'],'type':r['Type']} for r in c.fetchall()]
                c.execute(f'SELECT COUNT(*) AS n FROM `{name}`')
                tables[name]={'description':note,'columns':cols,'rows':c.fetchone()['n'],'source':source}
    finally: conn.close()
    return tables


def cache(station,vehicle_ids,clock):
    client=redis.Redis(host=os.getenv('REDIS_HOST','redis'),port=int(os.getenv('REDIS_PORT','6379')),socket_timeout=3,decode_responses=True)
    import json
    result=[]
    try:
        for vid in vehicle_ids:
            value=client.get('fleet:route:'+vid)
            ttl=client.ttl('fleet:route:'+vid)
            result.append({'vehicle_id':vid,'redis_ttl_seconds':ttl,'value':json.loads(value) if value else None})
        return {'source':'redis','station':station,'clock':clock,'semantics':'真实 Redis 墙上时钟 TTL；模拟器暂停时仍倒计时，桥接定期续写快照。','rows':result}
    finally: client.close()


def warehouse(station,start,end):
    if not re.fullmatch(r'SZ-0[1-6]',station): raise ValueError('无效站点')
    result=query(f"SELECT station_id,COUNT(*) AS events,MIN(event_time) AS first_event,MAX(event_time) AS last_event,MAX(lag_seconds) AS max_lag_seconds FROM telemetry_clean WHERE station_id='{station}' AND event_time BETWEEN {int(start)} AND {int(end)} GROUP BY station_id",'doris')
    result['scope']={'station':station,'start':start,'end':end}
    result['path']='Kafka raw → Flink SQL → Kafka clean → Doris Stream Load'
    return result


def status():
    if not enabled(): return {'enabled':False,'components':[]}
    def check(source):
        start=time.monotonic()
        try:
            if source in TABLES:
                details=context(source)
                result={'tables':{k:v['rows'] for k,v in details.items()}}
            elif source=='redis':
                client=redis.Redis(host=os.getenv('REDIS_HOST','redis'),port=int(os.getenv('REDIS_PORT','6379')),socket_timeout=3)
                result={'ping':client.ping(),'keys':client.dbsize()};client.close()
            elif source=='flink':
                with httpx.Client(timeout=3,trust_env=False) as c:
                    r=c.get(os.getenv('FLINK_URL','http://jobmanager:8081')+'/jobs/overview');r.raise_for_status()
                    result={'jobs':[{'id':j['jid'],'state':j['state']} for j in r.json()['jobs']]}
            else:
                with httpx.Client(timeout=3,trust_env=False) as c:
                    r=c.get(os.getenv('BUSINESS_URL','http://business:8080')+'/business/vehicles?station=SZ-01');r.raise_for_status();result={'vehicles_returned':len(r.json())}
            return {'name':source,'status':'connected','elapsed_ms':round((time.monotonic()-start)*1000,2),**result}
        except Exception as e:
            return {'name':source,'status':'unavailable','error':type(e).__name__}
    with ThreadPoolExecutor(max_workers=5) as pool: parts=list(pool.map(check,['mysql','redis','flink','doris','java']))
    return {'enabled':True,'components':parts,'mode':'hybrid','note':'业务故障由本地模拟器产生；MySQL/Redis 为复制快照，遥测通过 Kafka/Flink 实际写入 Doris。'}
