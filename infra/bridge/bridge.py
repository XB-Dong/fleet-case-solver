"""Optional full-stack replication. Sandbox remains the source of synthetic events.
Snapshots -> MySQL/Redis; telemetry -> Kafka -> Flink -> Kafka -> Doris.
Delivery is at-least-once; Doris unique keys make replay idempotent.
"""
import json
import os
import time
import uuid
import threading
import httpx
import pymysql
import redis
from kafka import KafkaProducer, KafkaConsumer

API='http://app:8765/api'

def connect_mysql():
    return pymysql.connect(host='mysql',user='fleet',password=os.environ['MYSQL_PASSWORD'],database='fleet',autocommit=True)

def sink():
    while True:
        try:
            db=pymysql.connect(host='doris',port=9030,user='root',autocommit=True)
            with db.cursor() as c:
                for sql in open('/app/doris.sql',encoding='utf-8').read().split(';'):
                    if sql.strip(): c.execute(sql)
            db.close()
            consumer=KafkaConsumer('fleet.telemetry.clean','fleet.station.minute',bootstrap_servers='kafka:29092',group_id='fleet-doris-v1',auto_offset_reset='earliest',enable_auto_commit=False,value_deserializer=lambda b:json.loads(b))
            with httpx.Client(timeout=60,follow_redirects=True) as client:
                while True:
                    batches=consumer.poll(timeout_ms=1000,max_records=500)
                    for partition, records in batches.items():
                        table='telemetry_clean' if partition.topic.endswith('clean') else 'station_minute'
                        # Stream Load directly to the local BE HTTP port avoids FE redirect/auth differences.
                        response=client.put(f'http://doris:8040/api/fleet/{table}/_stream_load',auth=('root',''),headers={'format':'json','strip_outer_array':'true','label':str(uuid.uuid4())},content=json.dumps([r.value for r in records]))
                        response.raise_for_status()
                        loaded=response.json()
                        if loaded.get('Status') not in ('Success','Publish Timeout') or int(loaded.get('NumberFilteredRows',0)):
                            raise RuntimeError('Doris rejected batch: '+json.dumps(loaded))
                    if batches: consumer.commit()
        except Exception as e:
            print('sink retry:',type(e).__name__,flush=True)
            try: consumer.close()
            except (NameError,Exception): pass
            time.sleep(10)

def source():
    producer=KafkaProducer(bootstrap_servers='kafka:29092',value_serializer=lambda v:json.dumps(v).encode(),acks='all')
    cache=redis.Redis(host='redis',decode_responses=True)
    db=connect_mysql()
    last_event=0; last_clock=None; revision=None
    with httpx.Client(timeout=40) as client:
        while True:
            overview=client.get(API+'/overview'); overview.raise_for_status(); o=overview.json()
            if last_clock is not None and o['clock']<last_clock:
                # Explicit reset: remove replicated mutable state; telemetry IDs restart in the sandbox.
                last_event=0
                with db.cursor() as c: c.execute('DELETE FROM tasks')
            last_clock=o['clock']
            for station in o['stations']:
                sid=station['station_id']
                vs=client.get(API+'/vehicles',params={'station':sid}).json()
                tasks=client.get(API+'/tasks',params={'station':sid}).json()
                with db.cursor() as c:
                    for table,rows in [('vehicles',vs),('tasks',tasks)]:
                        if not rows: continue
                        columns=list(rows[0]); names=','.join('`'+k+'`' for k in columns)
                        updates=','.join(f'`{k}`=VALUES(`{k}`)' for k in columns[1:])
                        c.executemany(f'INSERT INTO {table} ({names}) VALUES ({",".join(["%s"]*len(columns))}) ON DUPLICATE KEY UPDATE {updates}',[tuple(r[k] for k in columns) for r in rows])
                evidence=client.post(API+'/investigate',json={'station':sid,'tool':'inspect_cache'}).json()['result']
                for row in evidence:
                    key='fleet:route:'+row['vehicle_id']; ttl=int(row['ttl_seconds'])
                    if ttl>0: cache.set(key,json.dumps(row),ex=ttl)
                    else: cache.delete(key)
            # Page actual DB rows, not fabricated snapshots. Pause does not advance the event clock.
            while True:
                sql=f'SELECT * FROM telemetry WHERE event_id>{last_event} ORDER BY event_id LIMIT 200'
                result=client.post(API+'/data/query',json={'sql':sql});result.raise_for_status();rows=result.json()['rows']
                if not rows: break
                for row in rows: producer.send('fleet.telemetry.raw',row).get(timeout=30)
                last_event=rows[-1]['event_id']
            time.sleep(5)

if __name__=='__main__':
    threading.Thread(target=sink,daemon=True).start()
    while True:
        try: source()
        except Exception as e:
            print('source retry:',type(e).__name__,flush=True);time.sleep(10)
