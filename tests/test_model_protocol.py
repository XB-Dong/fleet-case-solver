import json
import httpx
from backend.agent import Harness
from backend.engine import Engine


def test_model_calls_tools_and_returns_evidence(monkeypatch,tmp_path):
    engine=Engine(tmp_path/'model.db',days=1,vehicle_count=12)
    harness=Harness(engine)
    requests=[]
    def respond(request):
        payload=json.loads(request.content)
        requests.append(payload)
        if len(requests)==1:
            message={'role':'assistant','content':None,'tool_calls':[{'id':'tool-'+str(i),'type':'function','function':{'name':name,'arguments':'{}'}} for i,name in enumerate(['search_cases','get_data_context','query_metrics'])]}
        else:
            assert len([m for m in payload['messages'] if m['role']=='tool'])==3
            conclusion={'summary':'当前站点正常。','verified':['[E03] 当前心跳正常。'],'inferred':[],'pending':[],'excluded':[],'actions':['继续观察。']}
            message={'role':'assistant','content':json.dumps(conclusion,ensure_ascii=False)}
        return httpx.Response(200,json={'choices':[{'message':message}],'usage':{'total_tokens':100}})
    real_client=httpx.Client
    monkeypatch.setattr('backend.agent.httpx.Client',lambda **kw:real_client(transport=httpx.MockTransport(respond),**kw))
    monkeypatch.setenv('MODEL_API_KEY','local-mock-placeholder')
    monkeypatch.setenv('MODEL_BASE_URL','https://model.example.test/v1')
    run=harness.new_run('SZ-01','检查站点','model'); harness.model(run)
    assert len(requests)==2
    assert run['usage']['total_tokens']==200
    assert run['conclusion']['verified'][0].startswith('[E03]')


def test_model_fabricated_evidence_rejected(monkeypatch,tmp_path):
    engine=Engine(tmp_path/'model.db',days=1,vehicle_count=12)
    harness=Harness(engine)
    monkeypatch.setenv('MODEL_API_KEY','local-mock-placeholder')
    run=harness.new_run('SZ-01','test','model')
    for name in ('search_cases','get_data_context','query_metrics'): harness.tool(run,name)
    result={'summary':'unsupported','verified':['[E99] fabricated'],'inferred':[],'pending':[],'excluded':[],'actions':[]}
    real_client=httpx.Client
    monkeypatch.setattr('backend.agent.httpx.Client',lambda **kw:real_client(transport=httpx.MockTransport(lambda req:httpx.Response(200,json={'choices':[{'message':{'role':'assistant','content':json.dumps(result)}}]})),**kw))
    import pytest
    with pytest.raises(ValueError,match='引用'): harness.model(run)
