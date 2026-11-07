import json
import threading
import uuid
import traceback
from pathlib import Path
from .database import DB, BASE_TIME, connect, seed
from .route_sync import sync_routes, sync_routes_fixed

SCENARIOS = {
 'route': {'name': '路线同步中断', 'description': '外部路线响应出现空数组，观察批次失败与缓存失效的级联影响。'},
 'lag': {'name': '遥测消费积压', 'description': '暂停遥测消费，区分数据陈旧和车辆真实离线。'},
 'config': {'name': '运力阈值变更', 'description': '提高电量准入阈值，观察调度拒绝与任务积压。'},
}


class Engine:
    def __init__(self, path=DB, days=14, vehicle_count=300):
        self.path = Path(path)
        self.lock = threading.RLock()
        seed(path, days, vehicle_count)
        with connect(path) as c:
            saved = c.execute("SELECT value FROM runtime_state WHERE key='engine'").fetchone()
        if saved:
            self.s = json.loads(saved[0])
        else:
            self.reset()

    def persist(self):
        with connect(self.path) as c:
            c.execute("INSERT OR REPLACE INTO runtime_state VALUES('engine',?)", (json.dumps(self.s),))

    def reset(self):
        with self.lock, connect(self.path) as c:
            for t in ('dispatch_attempts','logs','config_audit','metric_history','alerts'):
                c.execute(f'DELETE FROM {t}')
            c.execute("DELETE FROM tasks WHERE task_id LIKE 'SIM-%'")
            c.execute('DELETE FROM telemetry WHERE event_time>=?', (BASE_TIME,))
            c.execute('UPDATE vehicles SET online=1,dispatchable=1,heartbeat_at=?', (BASE_TIME,))
            vs = [dict(r) for r in c.execute('SELECT * FROM vehicles')]
            self.s = {'now': BASE_TIME, 'fault': None, 'station': 'SZ-01', 'start': None, 'threshold': 20, 'cache': {v['vehicle_id']:{'route':'R-'+v['station_id'],'expires_at':BASE_TIME+600} for v in vs}, 'revision': uuid.uuid4().hex, 'last_ingest': BASE_TIME, 'fixed':False, 'ticks':0}
        self.advance(0)
        return self.overview()

    def payload(self, station, connection=None):
        if connection is not None:
            ids = [r[0] for r in connection.execute('SELECT vehicle_id FROM vehicles WHERE station_id=? ORDER BY vehicle_id', (station,))]
        else:
            with connect(self.path) as c:
                ids = [r[0] for r in c.execute('SELECT vehicle_id FROM vehicles WHERE station_id=? ORDER BY vehicle_id', (station,))]
        return [{'vehicle_id':v,'routes': [] if i==0 and self.s['fault']=='route' and station==self.s['station'] else ['R-'+station]} for i,v in enumerate(ids)]

    def inject(self, scenario, station):
        if scenario not in SCENARIOS:
            raise ValueError('未知故障类型')
        with self.lock, connect(self.path) as c:
            if not c.execute('SELECT 1 FROM stations WHERE station_id=?',(station,)).fetchone():
                raise ValueError('站点不存在')
            if self.s['fault']:
                raise ValueError('请先恢复当前故障或重置沙箱')
            self.s.update(fault=scenario, station=station, start=self.s['now'], revision=uuid.uuid4().hex, fixed=False)
            if scenario=='config':
                self.s['threshold']=92
                c.execute('INSERT INTO config_audit VALUES(NULL,?,?,?,?,?,?)',(station,self.s['now'],'min_battery','20','92','sandbox-operator'))
        self.advance(60)
        return self.overview()

    def log(self, c, service, level, station, trace, message):
        c.execute('INSERT INTO logs VALUES(NULL,?,?,?,?,?,?)',(service,level,station,self.s['now'],trace,message))

    def advance(self, seconds=60):
        if seconds<0 or seconds>3600 or seconds%60:
            raise ValueError('时间推进必须是 0–3600 之间的整分钟')
        with self.lock:
            for _ in range(max(1,seconds//60)):
                if seconds:
                    self.s['now']+=60
                    self.s['ticks']+=1
                now=self.s['now']
                with connect(self.path) as c:
                    stations=[r[0] for r in c.execute('SELECT station_id FROM stations')]
                    for station in stations:
                        vs=[dict(r) for r in c.execute('SELECT * FROM vehicles WHERE station_id=?',(station,))]
                        if self.s['ticks']%3==1 or seconds==0:
                            try:
                                fn=sync_routes_fixed if self.s['fixed'] else sync_routes
                                skipped=fn(self.payload(station,c),self.s['cache'],now)
                                self.log(c,'route-sync','INFO',station,f'sync-{now}-{station}',f'batch committed; skipped={skipped or []}; ttl=600s')
                            except IndexError as exc:
                                line=traceback.extract_tb(exc.__traceback__)[-1].lineno
                                self.log(c,'route-sync','ERROR',station,f'sync-{now}-{station}',f'{type(exc).__name__}: {exc}; batch aborted before cache commit; backend/route_sync.py:{line}')
                        lag=now-self.s['last_ingest'] if self.s['fault']=='lag' and station==self.s['station'] else 2
                        eligible=[]
                        for v in vs:
                            cache=self.s['cache'].get(v['vehicle_id'],{})
                            reason='ROUTE_EXPIRED' if cache.get('expires_at',0)<=now else 'BATTERY_POLICY' if v['battery']<(self.s['threshold'] if station==self.s['station'] else 20) else 'ACCEPTED'
                            c.execute('UPDATE vehicles SET dispatchable=?,heartbeat_at=? WHERE vehicle_id=?',(int(reason=='ACCEPTED'),now,v['vehicle_id']))
                            if reason=='ACCEPTED': eligible.append(v)
                            if not(self.s['fault']=='lag' and station==self.s['station']):
                                c.execute('INSERT INTO telemetry VALUES(NULL,?,?,?,?,?,?,?)',(v['vehicle_id'],station,now,now+2,v['battery'],8,1))
                        accepted=min(len(eligible),10)
                        if seconds:
                            for i in range(10):
                                tid=f'SIM-{now}-{station}-{i}'
                                v=eligible[i] if i<accepted else vs[i%len(vs)]
                                reason='ACCEPTED' if i<accepted else ('ROUTE_EXPIRED' if self.s['cache'][v['vehicle_id']]['expires_at']<=now else 'BATTERY_POLICY')
                                trace=f'tr-{tid}'
                                c.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)',(tid,v['vehicle_id'],station,'completed' if i<accepted else 'queued',now,now+30 if i<accepted else None,trace))
                                c.execute('INSERT INTO dispatch_attempts VALUES(NULL,?,?,?,?,?,?)',(tid,v['vehicle_id'],station,reason,now,trace))
                                if reason!='ACCEPTED': self.log(c,'dispatcher','WARN',station,trace,f'{v["vehicle_id"]}: rejected={reason}')
                            if accepted:
                                c.execute("UPDATE tasks SET status='completed',finished_at=? WHERE task_id IN (SELECT task_id FROM tasks WHERE station_id=? AND status='queued' ORDER BY created_at LIMIT ?)",(now,station,accepted))
                        backlog=c.execute("SELECT COUNT(*) FROM tasks WHERE station_id=? AND status='queued'",(station,)).fetchone()[0]
                        online=len(vs) if lag<=180 else 0
                        c.execute('INSERT INTO metric_history VALUES(?,?,?,?,?,?,?)',(now,station,online,len(eligible),backlog,accepted/10,lag))
                        title='遥测新鲜度超过 180 秒' if lag>180 else '派单成功率低于 80%' if accepted<8 else None
                        active=c.execute("SELECT * FROM alerts WHERE station_id=? AND status IN ('firing','acknowledged')",(station,)).fetchone()
                        if title and not active:
                            aid='INC-'+uuid.uuid4().hex[:8].upper()
                            c.execute('INSERT INTO alerts VALUES(?,?,?,?,?,?,?,?,?)',(aid,station,title,'P2','firing','',now,None,'[]'))
                        if not title and active:
                            c.execute("UPDATE alerts SET status='recovered',recovered_at=? WHERE alert_id=?",(now,active['alert_id']))
                    if self.s['fault']!='lag': self.s['last_ingest']=now
            self.persist()
        return self.overview()

    def recover(self):
        with self.lock, connect(self.path) as c:
            if self.s['fault']=='config':
                c.execute('INSERT INTO config_audit VALUES(NULL,?,?,?,?,?,?)',(self.s['station'],self.s['now'],'min_battery','92','20','sandbox-operator'))
            self.s.update(fault=None, threshold=20, fixed=True, revision=uuid.uuid4().hex)
            for station in [r[0] for r in c.execute('SELECT station_id FROM stations')]:
                sync_routes_fixed(self.payload(station,c),self.s['cache'],self.s['now'])
            self.log(c,'sandbox','INFO',self.s['station'],'recovery-'+str(self.s['now']),'Recovery applied; waiting for business metrics to confirm')
        return self.advance(60)

    def overview(self):
        with self.lock, connect(self.path) as c:
            stations=[]
            for r in c.execute('SELECT * FROM stations'):
                m=c.execute('SELECT * FROM metric_history WHERE station_id=? ORDER BY rowid DESC LIMIT 1',(r['station_id'],)).fetchone()
                stations.append({**dict(r),**(dict(m) if m else {})})
            metrics={'vehicles':c.execute('SELECT COUNT(*) FROM vehicles').fetchone()[0], 'telemetry_rows':c.execute('SELECT COUNT(*) FROM telemetry').fetchone()[0], 'tasks':c.execute('SELECT COUNT(*) FROM tasks').fetchone()[0], 'online':sum(s.get('online',0) for s in stations), 'dispatchable':sum(s.get('dispatchable',0) for s in stations), 'backlog':sum(s.get('backlog',0) for s in stations), 'success_rate':sum(s.get('success_rate',1) for s in stations)/len(stations)}
            return {'clock':self.s['now'],'scenario':self.s['fault'],'station':self.s['station'],'revision':self.s['revision'],'metrics':metrics,'stations':stations,'alerts':[dict(r) for r in c.execute('SELECT * FROM alerts ORDER BY opened_at DESC')], 'history':[dict(r) for r in c.execute('SELECT * FROM metric_history ORDER BY rowid DESC LIMIT 360')][::-1], 'mode':'lite · SQLite / 本地状态缓存', 'scenarios':SCENARIOS}

    def alert_action(self, aid, action, owner='', note=''):
        with self.lock, connect(self.path) as c:
            row=c.execute('SELECT * FROM alerts WHERE alert_id=?',(aid,)).fetchone()
            if not row: raise ValueError('告警不存在')
            notes=json.loads(row['notes'])
            if action=='ack':
                if row['status']!='firing' or not owner.strip(): raise ValueError('仅待认领告警可认领，且负责人不能为空')
                c.execute("UPDATE alerts SET status='acknowledged',owner=? WHERE alert_id=?",(owner.strip(),aid))
            elif action=='close':
                if row['status']!='recovered': raise ValueError('业务指标恢复后才可关闭告警')
                if not note.strip(): raise ValueError('结案需要填写排查结论')
                c.execute("UPDATE alerts SET status='closed' WHERE alert_id=?",(aid,))
            elif action!='note': raise ValueError('未知动作')
            if note.strip(): notes.append({'at':self.s['now'],'text':note.strip(),'owner':owner or row['owner']})
            c.execute('UPDATE alerts SET notes=? WHERE alert_id=?',(json.dumps(notes,ensure_ascii=False),aid))
        return self.overview()
