import os
import threading
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from .database import ROOT, context, query, connect
from .engine import Engine
from .agent import Harness

app=FastAPI(title='FleetOps Lab',description='Synthetic fleet operations and read-only case solver')
engine=Engine()
harness=Harness(engine)


@app.exception_handler(ValueError)
async def value_error(request, exc):
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=400,content={'detail':str(exc)})


@app.middleware('http')
async def local_origin(request:Request, call_next):
    # This unauthenticated sandbox binds to loopback. Reject browser cross-site mutations.
    origin=request.headers.get('origin')
    if request.method not in ('GET','HEAD','OPTIONS') and origin:
        from urllib.parse import urlsplit
        if urlsplit(origin).netloc!=request.headers.get('host'):
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=403,content={'detail':'跨站写请求已拒绝'})
    return await call_next(request)


@app.get('/api/health')
def health(): return {'status':'ok','mode':'lite','model_configured':bool(os.getenv('MODEL_API_KEY'))}

@app.get('/api/overview')
def overview(): return engine.overview()

class Injection(BaseModel):
    scenario:str
    station:str='SZ-01'

@app.post('/api/sandbox/inject')
def inject(body:Injection): return engine.inject(body.scenario,body.station)

class Advance(BaseModel):
    seconds:int=60

@app.post('/api/sandbox/advance')
def advance(body:Advance): return engine.advance(body.seconds)

@app.post('/api/sandbox/recover')
def recover(): return engine.recover()

@app.post('/api/sandbox/reset')
def reset(): return engine.reset()

@app.get('/api/vehicles')
def vehicles(station:str='SZ-01'):
    with connect(engine.path) as c: return [dict(r) for r in c.execute('SELECT * FROM vehicles WHERE station_id=?',(station,))]

@app.get('/api/tasks')
def tasks(station:str='SZ-01'):
    with connect(engine.path) as c: return [dict(r) for r in c.execute('SELECT * FROM tasks WHERE station_id=? ORDER BY created_at DESC LIMIT 100',(station,))]

@app.get('/api/trace/{trace_id}')
def trace(trace_id:str):
    with connect(engine.path) as c:
        return {t:[dict(r) for r in c.execute(f'SELECT * FROM {t} WHERE trace_id=?',(trace_id,))] for t in ('tasks','dispatch_attempts','logs')}

@app.get('/api/data/context')
def data_context(): return context(engine.path)

class SQL(BaseModel):
    sql:str=Field(max_length=12000)

@app.post('/api/data/query')
def data_query(body:SQL):
    try: return query(body.sql,engine.path)
    except Exception as exc: raise HTTPException(400,str(exc))

class ToolRequest(BaseModel):
    station:str='SZ-01'
    tool:str
    args:dict={}

@app.post('/api/investigate')
def investigate(body:ToolRequest):
    run=harness.new_run(body.station,'人工取证','replay')
    try:
        if body.tool=='query_sql': harness.tool(run,'get_data_context')
        return harness.tool(run,body.tool,body.args)
    finally: harness.runs.pop(run['id'],None)

class AlertAction(BaseModel):
    action:str
    owner:str='值班工程师'
    note:str=''

@app.post('/api/alerts/{aid}')
def alert_action(aid:str,body:AlertAction): return engine.alert_action(aid,body.action,body.owner,body.note)

class AgentRequest(BaseModel):
    station:str='SZ-01'
    question:str='为什么该站点的任务积压？请排查并提供证据。'
    mode:str='replay'

@app.post('/api/agent/runs')
def start_agent(body:AgentRequest):
    if sum(r['status']=='running' for r in harness.runs.values())>=2: raise HTTPException(429,'最多同时运行两个诊断')
    run=harness.new_run(body.station,body.question,body.mode)
    threading.Thread(target=harness.execute,args=(run,),daemon=True).start()
    return {'id':run['id']}

@app.get('/api/agent/runs/{rid}')
def agent_run(rid:str):
    if rid not in harness.runs: raise HTTPException(404,'诊断不存在')
    return harness.runs[rid]

@app.get('/api/cases')
def cases(): return harness.cases()

@app.get('/api/cases/{cid}/download')
def download(cid:str):
    import re
    if not re.fullmatch('[0-9a-f]{12}',cid): raise HTTPException(404)
    p=harness.case_dir/(cid+'.md')
    if not p.exists(): raise HTTPException(404)
    return FileResponse(p,filename=p.name)

@app.get('/metrics',response_class=PlainTextResponse)
def metrics():
    o=engine.overview(); lines=[]
    for station in o['stations']:
        for key in ('online','dispatchable','backlog','success_rate','lag_seconds'):
            lines.append(f'fleet_{key}{{station="{station["station_id"]}"}} {station[key]}')
    return '\n'.join(lines)+'\n'

if (ROOT/'dist').exists():
    app.mount('/',StaticFiles(directory=ROOT/'dist',html=True),name='web')
