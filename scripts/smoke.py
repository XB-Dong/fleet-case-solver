"""Explicit end-to-end smoke against the disposable running sandbox. Resets it."""
import argparse
import time
import httpx

p=argparse.ArgumentParser()
p.add_argument('--base',default='http://127.0.0.1:8765')
args=p.parse_args()
c=httpx.Client(base_url=args.base+'/api',trust_env=False,timeout=60)
def get(path):
    r=c.get(path); r.raise_for_status(); return r.json()
def post(path,body=None):
    r=c.post(path,json=body or {});r.raise_for_status();return r.json()
for scenario, station, keyword in [('route','SZ-01','缓存过期'),('lag','SZ-03','陈旧'),('config','SZ-02','阈值')]:
    post('/sandbox/reset')
    post('/sandbox/inject',{'scenario':scenario,'station':station})
    o=post('/sandbox/advance',{'seconds':600})
    assert o['alerts']
    aid=o['alerts'][0]['alert_id']
    post('/alerts/'+aid,{'action':'ack','owner':'smoke-test'})
    run=post('/agent/runs',{'station':station,'mode':'replay','question':'请排查当前现场'})
    deadline=time.monotonic()+40
    while time.monotonic()<deadline:
        result=get('/agent/runs/'+run['id'])
        if result['status']!='running': break
        time.sleep(.1)
    assert result['status']=='completed',result
    assert keyword in result['conclusion']['summary'],result['conclusion']
    assert c.get('/cases/'+run['id']+'/download').status_code==200
    o=post('/sandbox/recover')
    assert o['alerts'][0]['status']=='recovered'
    post('/alerts/'+aid,{'action':'close','note':'恢复后的业务指标已核验','owner':'smoke-test'})
    print(scenario,station,'PASS',len(result['calls']),'tool calls',result['elapsed_ms'],'ms')
post('/sandbox/reset')
print('Reset to healthy baseline. All three HTTP smoke scenarios passed.')
