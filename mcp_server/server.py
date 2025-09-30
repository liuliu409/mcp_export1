import os, json, asyncio
import httpx
from dotenv import load_dotenv

# Giả định API mcp chuẩn có Server + decorator tool
try:
    from mcp.server import Server
except Exception as e:
    print("ERROR: mcp library API changed or not installed:", e)
    print("Install: pip install -r mcp_server/requirements.txt")
    raise

load_dotenv()

FASTAPI_BASE = os.getenv("FASTAPI_BASE", "http://127.0.0.1:8000")

server = Server(name="mcp-export-tools")

async def _post_json(path: str, payload: dict):
    url = FASTAPI_BASE.rstrip("/") + path
    async with httpx.AsyncClient(timeout=300.0) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        return r.json()

async def _get(path: str, params: dict | None = None):
    url = FASTAPI_BASE.rstrip("/") + path
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.get(url, params=params or {})
        r.raise_for_status()
        return r.json()

@server.tool("ping")
async def tool_ping() -> str:
    """Ping FastAPI service"""
    return json.dumps(await _get("/ping"))

@server.tool("glm_mapping_columns")
async def tool_glm_mapping_columns(url_file: str) -> str:
    """GLM: lấy mapping cột từ file nguồn"""
    return json.dumps(await _get("/glm/mapping-columns/", {"url_file": url_file}))

@server.tool("glm_valid_data")
async def tool_glm_valid_data(request_json: str) -> str:
    """GLM: validate data theo setting"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-valid-data/", payload))

@server.tool("glm_import_after_mapping")
async def tool_glm_import_after_mapping(request_json: str) -> str:
    """GLM: import parquet sau mapping"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-import-data-after-mapping/", payload))

@server.tool("glm_1wa")
async def tool_glm_1wa(request_json: str) -> str:
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-1wa/", payload))

@server.tool("glm_2wa")
async def tool_glm_2wa(request_json: str) -> str:
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-2wa/", payload))

@server.tool("glm_3wa")
async def tool_glm_3wa(request_json: str) -> str:
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-3wa/", payload))

@server.tool("glm_4wa")
async def tool_glm_4wa(request_json: str) -> str:
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-4wa/", payload))

@server.tool("mof_valid_data")
async def tool_mof_valid_data(request_json: str) -> str:
    """MOF: validate dữ liệu import"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/mof-report/mof-valid-data/", payload))

@server.tool("mof_import_after_mapping")
async def tool_mof_import_after_mapping(request_json: str) -> str:
    """MOF: import parquet sau mapping"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/mof-report/mof-import-data-after-mapping/", payload))

@server.tool("mof_pnt_11")
async def tool_mof_pnt_11(request_json: str) -> str:
    """MOF: tổng hợp PNT-11"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/mof-report/mof-pnt-11/", payload))

@server.tool("mof_bctcq")
async def tool_mof_bctcq(request_json: str) -> str:
    """MOF: lập báo cáo tài chính tổng hợp"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/mof-report/mof-pnt-bctcq/", payload))

if __name__ == "__main__":
    # Run MCP stdio server
    asyncio.run(server.run_stdio())
