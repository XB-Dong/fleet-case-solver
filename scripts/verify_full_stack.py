"""Verify actual Docker sources. --scenario explicitly runs a recoverable route drill."""
import argparse
import json
from pathlib import Path
import time
import httpx

p=argparse.ArgumentParser()
p.add_argument('--base',default='http://127.0.0.1:18765')
p.add_argument('--scenario',action='store_true')
p.add_argument('--monitoring',action='store_true')
p.add_argument('--timeout',type=int,default=1200)
args=p.parse_args()
c=httpx.Client(base_url=args.base+'/api',timeout=30,trust_env=False,limits=httpx.Limits(keepalive_expiry=1))
report={}
def get(path):
    r=c.get(path);r.raise_for_status();return r.json()
def post(path,body=None):
    r=c.post(path,json=body or {});r.raise_for_status();return r.json()
def wait_for(fn,label,timeout=120):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        result=fn()
        if result:return result
        time.sleep(5)
    raise AssertionError('Timed out: '+label)
def query(source,sql):return post('/data/query',{'source':source,'sql':sql})

assert get('/health')['stack_enabled']
target=get('/overview')['metrics']['telemetry_rows']
def caught_up():
    result=query('doris','SELECT COUNT(*) AS n FROM telemetry_clean')
    count=result['rows'][0]['n']
    print('Doris rows:',count,'target:',target,flush=True)
    return result if count==target else None
report['initial_doris']=wait_for(caught_up,'Doris backfill',args.timeout)
report['stack']=get('/stack')
assert all(x['status']=='connected' for x in report['stack']['components'])
flink=next(x for x in report['stack']['components'] if x['name']=='flink')
assert len(flink['jobs'])==1 and flink['jobs'][0]['state']=='RUNNING'
assert query('mysql','SELECT COUNT(*) AS n FROM vehicles')['rows'][0]['n']==300
report['station_aggregate']=query('doris','SELECT station_id, COUNT(*) AS events FROM telemetry_clean GROUP BY station_id ORDER BY station_id')
report['minute_aggregate']=query('doris','SELECT COUNT(*) AS windows, SUM(event_count) AS events FROM station_minute')

if args.scenario:
    assert get('/overview')['scenario'] is None,'Recover active scene before running this drill'
    post('/sandbox/inject',{'scenario':'route','station':'SZ-01'})
    try:
        o=post('/sandbox/advance',{'seconds':660})
        report['fault_metrics']=o['metrics']
        assert o['metrics']['online']==300 and o['metrics']['dispatchable']==250
        aid=next(a['alert_id'] for a in o['alerts'] if a['station_id']=='SZ-01' and a['status']=='firing')
        post('/alerts/'+aid,{'action':'ack','owner':'full-stack-drill'})
        target=o['metrics']['telemetry_rows']
        wait_for(caught_up,'current fault telemetry',180)
        report['manual']=post('/investigate',{'station':'SZ-01','tool':'query_sql','args':{'sql':'SELECT reason, COUNT(*) AS attempts FROM dispatch_attempts GROUP BY reason'}})
        run=post('/agent/runs',{'station':'SZ-01','mode':'replay'})
        def completed():
            r=get('/agent/runs/'+run['id'])
            return r if r['status']!='running' else None
        report['agent']=wait_for(completed,'agent')
        assert report['agent']['status']=='completed',report['agent']
        assert '缓存过期' in report['agent']['conclusion']['summary']
        calls={x['tool']:x['result'] for x in report['agent']['calls']}
        assert calls['query_warehouse']['source']=='doris' and calls['query_warehouse']['rows'][0]['events']>0
        assert calls['inspect_live_cache']['source']=='redis'
        assert len(report['agent']['calls'])==10
        if args.monitoring:
            def firing():
                alerts=httpx.get('http://127.0.0.1:9093/api/v2/alerts',timeout=10).json()
                return [a for a in alerts if a['labels'].get('alertname')=='FleetDispatchDegraded' and a['labels'].get('station')=='SZ-01']
            report['alertmanager']=wait_for(firing,'Prometheus -> Alertmanager',90)
    finally:
        post('/sandbox/recover')
    o=post('/sandbox/advance',{'seconds':180})
    assert o['metrics']['dispatchable']==300 and o['metrics']['backlog']==0
    post('/alerts/'+aid,{'action':'close','owner':'full-stack-drill','note':'Verified live Doris/Redis evidence and restored dispatch capacity; backlog cleared.'})
    report['recovered_metrics']=o['metrics']
    target=o['metrics']['telemetry_rows']
    report['final_doris']=wait_for(caught_up,'recovery telemetry',180)

Path('runtime').mkdir(exist_ok=True)
Path('runtime/full-stack-verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
print('PASS: actual full-stack verification. Report: runtime/full-stack-verification.json',flush=True)
