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
                    batches=consumer.poll(timeout_ms=1000,max_records=5000)
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
            print('sink retry:',type(e).__name__,str(e)[:500],flush=True)
            try: consumer.close()
            except (NameError,Exception): pass
            time.sleep(10)

def source():
    producer=KafkaProducer(bootstrap_servers='kafka:29092',value_serializer=lambda v:json.dumps(v).encode(),acks='all')
    # Separate Redis DB from route caches. Persist only after Kafka acknowledges the batch.
    checkpoint=redis.Redis(host='redis',db=1,decode_responses=True)
    last_event=int(checkpoint.get('fleet:source:last_event') or 0)
    with httpx.Client(timeout=40,trust_env=False) as client:
        while True:
            r=client.get(API+'/replication/telemetry',params={'after':last_event,'limit':5000})
            r.raise_for_status(); rows=r.json()
            if not rows:
                time.sleep(2); continue
            pending=[producer.send('fleet.telemetry.raw',row) for row in rows]
            producer.flush(timeout=30)
            for future in pending: future.get(timeout=30)
            last_event=rows[-1]['event_id']
            checkpoint.set('fleet:source:last_event',last_event)
            if last_event%100000==0: print('source sent:',last_event,flush=True)

def snapshots():
    cache=redis.Redis(host='redis',decode_responses=True)
    db=connect_mysql()
    with httpx.Client(timeout=40,trust_env=False) as client:
        while True:
            overview=client.get(API+'/overview'); overview.raise_for_status(); o=overview.json()
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
            time.sleep(5)

def supervise(fn):
    while True:
        try: fn()
        except Exception as e:
            print(fn.__name__,'retry:',type(e).__name__,str(e)[:250],flush=True)
            time.sleep(10)

if __name__=='__main__':
    threading.Thread(target=sink,daemon=True).start()
    threading.Thread(target=supervise,args=(snapshots,),daemon=True).start()
    supervise(source)
