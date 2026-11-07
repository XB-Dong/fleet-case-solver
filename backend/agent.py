"""Read-only diagnostic harness. Replay is explicit; model mode uses tool calls."""
import json
import os
import time
import uuid
from pathlib import Path
import httpx
from .database import ROOT, DATA, context, query, connect

STAGES=['快速分类','检索历史案例','理解问题','制定排查路径','收集证据','验证原因','输出与沉淀']
TOOLS={
 'get_data_context': '获取数据字典、字段语义与查询约束。SQL 前必须调用。',
 'query_sql': '执行只读 SQL；必须限定当前站点和事件时间，最多返回 200 行。',
 'query_metrics': '查询站点当前指标、真实心跳数和数据新鲜度。',
 'query_logs': '查询该站点当前事件窗口内的结构化日志。',
 'get_route_payload': '读取该站点当前原始路线接口响应。',
 'inspect_cache': '读取当前站点路线缓存及剩余 TTL。',
 'config_diff': '读取该站点当前事件窗口的配置变更历史。',
 'read_registered_code': '读取登记的路线同步实现，不搜索其他文件。',
 'search_cases': '检索历史案例；相似案例仅复用路径，必须重新取证。',
}


class Harness:
    def __init__(self, engine):
        self.engine=engine
        self.runs={}
        self.case_dir=engine.path.parent/'cases'
        self.case_dir.mkdir(parents=True,exist_ok=True)

    def cases(self):
        cases=[]
        for p in sorted(self.case_dir.glob('*.json'),reverse=True):
            try: cases.append(json.loads(p.read_text(encoding='utf-8')))
            except (ValueError,OSError): pass
        return cases

    def new_run(self, station, question, mode):
        if station not in [s['station_id'] for s in self.engine.overview()['stations']]: raise ValueError('未知站点')
        if mode not in {'replay','model'}: raise ValueError('未知模式')
        if mode=='model' and not os.getenv('MODEL_API_KEY'): raise ValueError('尚未配置 MODEL_API_KEY；可以使用明确标注的回放模式')
        with self.engine.lock:
            now=self.engine.s['now']
            run={'id':uuid.uuid4().hex[:12],'station':station,'question':question,'mode':mode,'status':'running','stage':0,'steps':STAGES,'calls':[], 'evidence':[], 'conclusion':None,'started':time.time(),'window_start':self.engine.s['start'] or now-900,'window_end':now,'revision':self.engine.s['revision'],'schema_seen':False,'usage':{}}
        self.runs[run['id']]=run
        return run

    def tool(self, run, name, args=None):
        args=args or {}
        if name not in TOOLS: raise ValueError('工具不在只读白名单中')
        start=time.monotonic()
        with self.engine.lock:
            if self.engine.s['revision']!=run['revision'] or self.engine.s['now']!=run['window_end']:
                raise ValueError('沙箱已变化，请重新运行诊断以避免混合不同现场')
            station=run['station']; at=run['window_start']; end=run['window_end']
            if name=='get_data_context':
                result={'tables':context(self.engine.path),'rules':'SQL 只读、3 秒超时、200 行限制；运行范围由 harness 限制到 station_id 与事件窗口。'}
                run['schema_seen']=True
            elif name=='query_sql':
                if not run['schema_seen']: raise ValueError('先获取数据上下文，再写 SQL')
                # Use a scoped in-memory copy: arbitrary SELECTs cannot escape tenant/time boundaries.
                import sqlite3
                import tempfile
                from .database import SCHEMA
                with tempfile.TemporaryDirectory() as folder:
                    scoped=Path(folder)/'scope.db'
                    with connect(self.engine.path) as source, connect(scoped) as dest:
                        dest.executescript(SCHEMA)
                        for table, timecol in {'stations':None,'vehicles':None,'tasks':'created_at','telemetry':'event_time','dispatch_attempts':'event_time','logs':'event_time','config_audit':'event_time','metric_history':'event_time','alerts':'opened_at'}.items():
                            where='station_id=?'+(f' AND {timecol} BETWEEN ? AND ?' if timecol else '')
                            params=(station,at,end) if timecol else (station,)
                            cursor=source.execute(f'SELECT * FROM {table} WHERE {where}',params)
                            dest.executemany(f'INSERT INTO {table} VALUES({",".join("?" for _ in cursor.description)})',cursor)
                    result=query(args.get('sql',''),scoped)
                    result['scope']={'station':station,'start':at,'end':end}
            elif name=='query_metrics':
                overview=self.engine.overview()
                result=next(s for s in overview['stations'] if s['station_id']==station).copy()
                with connect(self.engine.path) as c:
                    result['actual_online']=c.execute('SELECT COUNT(*) FROM vehicles WHERE station_id=? AND online=1 AND heartbeat_at>=?',(station,end-180)).fetchone()[0]
                    result['fleet_size']=c.execute('SELECT COUNT(*) FROM vehicles WHERE station_id=?',(station,)).fetchone()[0]
            elif name in {'query_logs','config_diff'}:
                table='logs' if name=='query_logs' else 'config_audit'
                with connect(self.engine.path) as c:
                    result=[dict(r) for r in c.execute(f'SELECT * FROM {table} WHERE station_id=? AND event_time BETWEEN ? AND ? ORDER BY id DESC LIMIT 100',(station,at,end))]
            elif name=='get_route_payload': result=self.engine.payload(station)
            elif name=='inspect_cache':
                ids=[r['vehicle_id'] for r in self.engine.payload(station)]
                result=[{'vehicle_id':v,**self.engine.s['cache'].get(v,{}),'ttl_seconds':self.engine.s['cache'].get(v,{}).get('expires_at',0)-end} for v in ids]
            elif name=='read_registered_code':
                p=ROOT/'backend'/'route_sync.py'
                result={'path':'backend/route_sync.py','source':'\n'.join(f'{i+1}: {line}' for i,line in enumerate(p.read_text(encoding='utf-8').splitlines()))}
            elif name=='search_cases':
                result=[{'id':c['id'],'station':c['station'],'window_start':c['window_start'],'summary':c['conclusion']['summary'],'reuse':'仅参考排查路径，当前证据需重新验证'} for c in self.cases() if c['station']==station][:10]
        call={'id':f'E{len(run["calls"])+1:02d}','tool':name,'args':args,'result':result,'elapsed_ms':round((time.monotonic()-start)*1000,2)}
        run['calls'].append(call)
        return call

    def execute(self, run):
        try:
            if run['mode']=='model': self.model(run)
            else: self.replay(run)
            run['stage']=6
            run['elapsed_ms']=round((time.time()-run['started'])*1000)
            run['status']='completed'
            saved={k:v for k,v in run.items() if k!='schema_seen'}
            (self.case_dir/(run['id']+'.json')).write_text(json.dumps(saved,ensure_ascii=False,indent=2),encoding='utf-8')
            conclusion=run['conclusion']
            md=f'# Case {run["id"]}\n\n{conclusion["summary"]}\n\n模式：{run["mode"]}；站点：{run["station"]}；耗时：{run["elapsed_ms"]} ms\n\n'
            for section in ['verified','inferred','pending','excluded','actions']:
                md+=f'## {section}\n\n'+ '\n'.join('- '+str(x) for x in conclusion.get(section,[]))+'\n\n'
            md+='## 工具证据\n\n'+ '\n'.join(f'- {c["id"]} {c["tool"]}: {c["elapsed_ms"]} ms' for c in run['calls'])
            (self.case_dir/(run['id']+'.md')).write_text(md,encoding='utf-8')
        except Exception as exc:
            run['status']='failed'
            run['error']=str(exc)
            run['elapsed_ms']=round((time.time()-run['started'])*1000)

    def replay(self, run):
        # Deterministic diagnostic policy, deliberately never presented as LLM reasoning.
        run['stage']=1
        self.tool(run,'search_cases')
        run['stage']=2
        m=self.tool(run,'query_metrics'); metrics=m['result']
        run['stage']=3
        self.tool(run,'get_data_context')
        run['stage']=4
        decisions=self.tool(run,'query_sql',{'sql':'SELECT reason, COUNT(*) AS attempts FROM dispatch_attempts GROUP BY reason ORDER BY attempts DESC'})
        reasons={r['reason']:r['attempts'] for r in decisions['result']['rows']}
        conclusion={'summary':'当前窗口未证实业务故障。','verified':[f'[{m["id"]}] 真实在线 {metrics["actual_online"]} 辆，可调度 {metrics["dispatchable"]} 辆，积压 {metrics["backlog"]} 单。'],'inferred':[],'pending':[],'excluded':[],'actions':[]}
        if metrics['lag_seconds']>180:
            logs=self.tool(run,'query_logs')
            conclusion.update(summary='遥测数据陈旧造成看板离线假象；实时心跳仍在线。', verified=conclusion['verified']+[f'[{m["id"]}] 数据延迟 {metrics["lag_seconds"]} 秒，超过 180 秒阈值；看板在线数为 {metrics["online"]}。'], inferred=['消费链路异常，需要继续检查消费者运行状态和分区积压。'], excluded=[f'[{m["id"]}] 当前证据不支持车辆真实批量离线。'], pending=['当前工具未提供消费者进程/分区指标，不能判定消费暂停的内部根因。'], actions=['检查消费者 lag 与进程状态；沙箱恢复后核验数据新鲜度和告警恢复。'])
        elif reasons.get('ROUTE_EXPIRED'):
            logs=self.tool(run,'query_logs'); payload=self.tool(run,'get_route_payload'); cache=self.tool(run,'inspect_cache'); code=self.tool(run,'read_registered_code')
            has_error=any('IndexError' in row['message'] for row in logs['result'])
            empty=sum(not row['routes'] for row in payload['result'])
            expired=sum(row['ttl_seconds']<=0 for row in cache['result'])
            if has_error and empty and expired:
                conclusion.update(summary='路线同步遇到空数组导致批次刷新中断；缓存过期后，在线车辆无法派单。',verified=conclusion['verified']+[f'[{decisions["id"]}] ROUTE_EXPIRED 拒绝 {reasons["ROUTE_EXPIRED"]} 次。',f'[{logs["id"]}] 同步日志记录 IndexError 与批次提交中断。',f'[{payload["id"]}] 原始响应存在 {empty} 条空路线数组。',f'[{cache["id"]}] {expired} 辆车的路线缓存 TTL 已到期。',f'[{code["id"]}] 实现直接访问 routes[0]，缓存提交在循环之后。'],inferred=['多轮同步失败导致旧缓存未续期，达到 TTL 后触发可调度运力下降。'],excluded=[f'[{m["id"]}] 心跳正常，不支持车辆真实离线。'],actions=['修正空数组边界检查并隔离单条异常；沙箱恢复刷新路线缓存。','验证拒绝原因消失、积压下降、告警恢复；再关闭工单。'])
            else: conclusion['pending']=['存在路线过期，但当前证据不足以确认空数组批次故障。']
        elif reasons.get('BATTERY_POLICY'):
            config=self.tool(run,'config_diff')
            audit=[r for r in config['result'] if r['config_key']=='min_battery']
            if audit:
                conclusion.update(summary='电量准入阈值上调压缩可调度运力，导致任务积压。',verified=conclusion['verified']+[f'[{decisions["id"]}] BATTERY_POLICY 拒绝 {reasons["BATTERY_POLICY"]} 次。',f'[{config["id"]}] 阈值变更记录：{audit[-1]["old_value"]} → {audit[-1]["new_value"]}。'],inferred=['配置变更与拒绝原因相符；恢复前应确认调整意图。'],excluded=[f'[{m["id"]}] 实时心跳仍正常。'],actions=['在沙箱回退阈值；对比派单成功率和积压，验证恢复。'])
            else: conclusion['pending']=['缺少当前窗口的配置审计，不能将拒绝直接归因到配置变更。']
        else:
            logs=self.tool(run,'query_logs')
            if any(r['level']=='ERROR' for r in logs['result']): conclusion['pending']=['服务已有错误但业务影响尚未显现；继续观察缓存 TTL 与后续调度决策。']
            conclusion['actions']=['若已恢复，核查工单恢复状态；若故障刚注入，推进模拟时钟后重新取证。']
        run['stage']=5
        run['conclusion']=conclusion

    def model(self, run):
        base=os.getenv('MODEL_BASE_URL','https://api.openai.com/v1').rstrip('/')
        model=os.getenv('MODEL_NAME','gpt-4.1-mini')
        specs=[{'type':'function','function':{'name':n,'description':d,'parameters':{'type':'object','properties':{'sql':{'type':'string'}} if n=='query_sql' else {},'required':['sql'] if n=='query_sql' else [],'additionalProperties':False}}} for n,d in TOOLS.items()]
        policy=(ROOT/'agent'/'workflow.md').read_text(encoding='utf-8')
        messages=[{'role':'system','content':policy+'\n最后仅输出 JSON：summary:string, verified/inferred/pending/excluded/actions:string[]。证据引用 [E01] 格式。不得编造证据，不得读取或修改工具以外的系统。'}, {'role':'user','content':f'站点 {run["station"]}，时间窗口 {run["window_start"]} 至 {run["window_end"]}。问题：{run["question"]}'}]
        with httpx.Client(timeout=60) as client:
            for _ in range(18):
                response=client.post(base+'/chat/completions',headers={'Authorization':'Bearer '+os.environ['MODEL_API_KEY']},json={'model':model,'messages':messages,'tools':specs,'temperature':0})
                if response.status_code>=400: raise ValueError(f'模型端点返回 HTTP {response.status_code}，请检查本地配置')
                body=response.json()
                for k,v in body.get('usage',{}).items():
                    if isinstance(v,int): run['usage'][k]=run['usage'].get(k,0)+v
                msg=body['choices'][0]['message']; messages.append(msg)
                calls=msg.get('tool_calls') or []
                if not calls:
                    text=msg.get('content','').strip()
                    if text.startswith('```'): text=text.split('\n',1)[1].rsplit('```',1)[0]
                    result=json.loads(text)
                    if not isinstance(result.get('summary'),str) or not all(isinstance(result.get(k),list) and all(isinstance(v,str) for v in result[k]) for k in ('verified','inferred','pending','excluded','actions')): raise ValueError('模型结论格式不合规')
                    names={c['tool'] for c in run['calls']}
                    if not {'get_data_context','search_cases','query_metrics'}.issubset(names): raise ValueError('模型未完成必要的上下文与当前事实核验')
                    import re
                    refs=set(re.findall(r'\[(E\d+)\]',json.dumps(result)))
                    if not refs or not refs.issubset({c['id'] for c in run['calls']}): raise ValueError('模型证据引用缺失或无效')
                    run['conclusion']=result; return
                run['stage']=4
                for call in calls:
                    try: out=self.tool(run,call['function']['name'],json.loads(call['function']['arguments']))
                    except ValueError as exc: out={'error':str(exc)}
                    messages.append({'role':'tool','tool_call_id':call['id'],'content':json.dumps(out,ensure_ascii=False)})
        raise ValueError('达到 18 轮工具调用上限，未得到完整结论')
