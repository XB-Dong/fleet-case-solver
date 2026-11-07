"""MCP adapter to the running local sandbox; never opens production systems."""
import os
import httpx
from mcp.server.fastmcp import FastMCP

mcp=FastMCP('fleet-case-solver')
BASE=os.getenv('FLEET_API_URL','http://127.0.0.1:8765')

@mcp.tool()
def get_data_context() -> dict:
    """Read fleet schemas and data semantics before writing SQL."""
    r=httpx.get(BASE+'/api/data/context',timeout=30,trust_env=False); r.raise_for_status(); return r.json()

@mcp.tool()
def investigate(station: str, tool: str, sql: str = '') -> dict:
    """Read evidence. Tools: query_metrics, query_sql, query_logs, inspect_cache,
    get_route_payload, config_diff, read_registered_code, search_cases.
    SQL is isolated to the selected station and active incident time window.
    """
    r=httpx.post(BASE+'/api/investigate',json={'station':station,'tool':tool,'args':{'sql':sql}},timeout=30,trust_env=False)
    r.raise_for_status(); return r.json()

if __name__=='__main__': mcp.run()
