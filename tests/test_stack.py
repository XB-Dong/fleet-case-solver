import pytest
from backend.stack import validate_sql

@pytest.mark.parametrize('sql',[
 'DELETE FROM telemetry_clean',
 'SELECT * FROM telemetry_clean; SELECT 1',
 'SELECT * FROM mysql.user',
 'SELECT * FROM runtime_state',
 "SELECT * FROM telemetry_clean INTO OUTFILE '/tmp/stolen'",
 'SELECT SLEEP(100)',
 'SELECT * FROM telemetry_clean FOR UPDATE',
])
def test_actual_database_guard_blocks_unsafe_sql(sql):
    with pytest.raises(ValueError): validate_sql(sql,'doris')

def test_actual_database_guard_keeps_aggregation():
    result=validate_sql('SELECT station_id, COUNT(*) AS n FROM telemetry_clean GROUP BY station_id','doris')
    assert 'GROUP BY' in result

def test_sql_cannot_cross_source_routes():
    with pytest.raises(ValueError): validate_sql('SELECT * FROM telemetry_clean','mysql')
