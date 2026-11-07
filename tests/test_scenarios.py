import json
import sqlite3
import pytest
from backend.engine import Engine
from backend.database import query, connect
from backend.agent import Harness

@pytest.fixture
def engine(tmp_path):
    return Engine(tmp_path/'fleet.db',days=1,vehicle_count=60)

def test_delayed_route_failure_and_recovery(engine):
    engine.inject('route','SZ-01')
    assert engine.overview()['metrics']['backlog']==0
    engine.advance(600)
    s=next(s for s in engine.overview()['stations'] if s['station_id']=='SZ-01')
    assert s['online']==10 and s['dispatchable']==0 and s['backlog']>0
    alert=engine.overview()['alerts'][0]
    with pytest.raises(ValueError): engine.alert_action(alert['alert_id'],'close',note='premature')
    engine.alert_action(alert['alert_id'],'ack',owner='Tester')
    engine.recover()
    assert engine.overview()['alerts'][0]['status']=='recovered'
    engine.alert_action(alert['alert_id'],'close',note='Recovery metrics verified')
    assert engine.overview()['alerts'][0]['status']=='closed'

def test_lag_is_not_real_vehicle_offline(engine):
    engine.inject('lag','SZ-03'); engine.advance(240)
    h=Harness(engine); run=h.new_run('SZ-03','为什么离线？','replay'); h.replay(run)
    assert '陈旧' in run['conclusion']['summary']
    m=next(c['result'] for c in run['calls'] if c['tool']=='query_metrics')
    assert m['actual_online']==10 and m['online']==0
    assert run['conclusion']['pending']

def test_config_requires_audit(engine):
    engine.inject('config','SZ-02')
    h=Harness(engine); run=h.new_run('SZ-02','为什么积压？','replay'); h.replay(run)
    assert '阈值' in run['conclusion']['summary']
    assert any(c['tool']=='config_diff' for c in run['calls'])

def test_replay_uses_evidence_at_selected_station(engine):
    engine.inject('route','SZ-04'); engine.advance(600)
    h=Harness(engine); good=h.new_run('SZ-04','排查','replay'); h.replay(good)
    assert '缓存过期' in good['conclusion']['summary']
    other=h.new_run('SZ-01','排查','replay'); h.replay(other)
    assert '未证实' in other['conclusion']['summary']
    scope=h.tool(good,'query_sql',{'sql':'SELECT DISTINCT station_id FROM vehicles'})
    assert scope['result']['rows']==[{'station_id':'SZ-04'}]

@pytest.mark.parametrize('sql',['DELETE FROM vehicles','WITH x AS (SELECT 1) DELETE FROM vehicles',"SELECT * FROM runtime_state","SELECT load_extension('x')",'SELECT 1; DROP TABLE tasks'])
def test_sql_write_and_internal_state_denied(engine,sql):
    with pytest.raises((ValueError,sqlite3.DatabaseError)): query(sql,engine.path)
    assert len(query('SELECT * FROM vehicles',engine.path)['rows'])==60

def test_schema_gate_and_scene_revision(engine):
    h=Harness(engine); run=h.new_run('SZ-01','test','replay')
    with pytest.raises(ValueError,match='上下文'): h.tool(run,'query_sql',{'sql':'SELECT * FROM vehicles'})
    engine.advance(60)
    with pytest.raises(ValueError,match='变化'): h.tool(run,'query_metrics')

def test_persistence_and_reset(engine):
    engine.inject('route','SZ-06'); engine.advance(600)
    loaded=Engine(engine.path)
    assert loaded.s['now']==engine.s['now']
    assert loaded.overview()['metrics']['backlog']>0
    loaded.reset()
    assert loaded.overview()['metrics']['backlog']==0
    assert not loaded.overview()['alerts']

def test_corrected_sync_isolates_malformed_record():
    from backend.route_sync import sync_routes_fixed
    cache={}; skipped=sync_routes_fixed([{'vehicle_id':'a','routes':[]},{'vehicle_id':'b','routes':['r']}],cache,100)
    assert skipped==['a'] and cache['b']['expires_at']==700

def test_case_persisted_only_after_success(engine,tmp_path):
    h=Harness(engine)
    engine.inject('route','SZ-01'); engine.advance(600)
    run=h.new_run('SZ-01','test','replay'); h.execute(run)
    assert run['status']=='completed'
    assert (h.case_dir/(run['id']+'.md')).exists()
    assert json.loads((h.case_dir/(run['id']+'.json')).read_text(encoding='utf-8'))['calls']
