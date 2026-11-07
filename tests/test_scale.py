from backend.engine import Engine

def test_default_fleet_does_not_deadlock_during_batch_refresh(tmp_path):
    engine=Engine(tmp_path/'fleet.db',days=1,vehicle_count=300)
    engine.inject('route','SZ-01')
    engine.advance(600)
    o=engine.overview()
    assert o['metrics']['dispatchable']==250
    assert o['metrics']['online']==300
    assert o['metrics']['backlog']>0
    engine.recover()
    assert engine.overview()['metrics']['dispatchable']==300
