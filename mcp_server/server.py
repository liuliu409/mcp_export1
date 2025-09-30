import os, json, asyncio  # Import các thư viện hỗ trợ môi trường, JSON và async
import httpx  # Thư viện gửi HTTP requests bất đồng bộ
from dotenv import load_dotenv  # Thư viện load biến môi trường từ file .env

# Thử nhập khẩu thư viện MCP Server, đây là công cụ giúp tạo server cho mcp và tích hợp các công cụ vào server
try:
    from mcp.server import Server  # Cố gắng import Server từ thư viện mcp
except Exception as e:
    print("ERROR: mcp library API changed or not installed:", e)  # Nếu lỗi, in ra thông báo lỗi
    print("Install: pip install -r mcp_server/requirements.txt")  # Đưa ra hướng dẫn cài đặt thư viện
    raise  # Nếu có lỗi, dừng chương trình

# Load biến môi trường từ file .env
load_dotenv()

# Lấy base URL cho FastAPI từ biến môi trường, nếu không có thì sử dụng localhost mặc định
FASTAPI_BASE = os.getenv("FASTAPI_BASE", "http://127.0.0.1:8000")

# Khởi tạo server mcp với tên "mcp-export-tools"
server = Server(name="mcp-export-tools")

# Hàm POST JSON bất đồng bộ gửi dữ liệu đến API FastAPI
async def _post_json(path: str, payload: dict):
    """Gửi yêu cầu POST đến API FastAPI với dữ liệu JSON"""
    url = FASTAPI_BASE.rstrip("/") + path  # Loại bỏ dấu / thừa ở cuối URL
    async with httpx.AsyncClient(timeout=300.0) as client:  # Gửi yêu cầu HTTP POST bất đồng bộ
        r = await client.post(url, json=payload)
        r.raise_for_status()  # Kiểm tra nếu có lỗi HTTP, raise exception
        return r.json()  # Trả về kết quả JSON

# Hàm GET bất đồng bộ để lấy dữ liệu từ API FastAPI
async def _get(path: str, params: dict | None = None):
    """Gửi yêu cầu GET đến API FastAPI với các tham số nếu có"""
    url = FASTAPI_BASE.rstrip("/") + path  # Loại bỏ dấu / thừa ở cuối URL
    async with httpx.AsyncClient(timeout=120.0) as client:  # Gửi yêu cầu HTTP GET bất đồng bộ
        r = await client.get(url, params=params or {})
        r.raise_for_status()  # Kiểm tra nếu có lỗi HTTP, raise exception
        return r.json()  # Trả về kết quả JSON

# Định nghĩa các công cụ mà server sẽ cung cấp, công cụ này giúp chatbot tương tác với các API của FastAPI

@server.tool("ping")
async def tool_ping() -> str:
    """Ping FastAPI service để kiểm tra kết nối"""
    return json.dumps(await _get("/ping"))  # Gửi yêu cầu GET tới endpoint /ping và trả về kết quả

@server.tool("glm_mapping_columns")
async def tool_glm_mapping_columns(url_file: str) -> str:
    """GLM: Lấy thông tin mapping cột từ file nguồn"""
    return json.dumps(await _get("/glm/mapping-columns/", {"url_file": url_file}))  # Gửi yêu cầu GET đến API FastAPI

@server.tool("glm_valid_data")
async def tool_glm_valid_data(request_json: str) -> str:
    """GLM: Kiểm tra tính hợp lệ của dữ liệu theo yêu cầu"""
    payload = json.loads(request_json)  # Chuyển JSON thành dictionary
    return json.dumps(await _post_json("/glm/glm-valid-data/", payload))  # Gửi yêu cầu POST và trả về kết quả

@server.tool("glm_import_after_mapping")
async def tool_glm_import_after_mapping(request_json: str) -> str:
    """GLM: Nhập dữ liệu sau khi đã thực hiện mapping"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-import-data-after-mapping/", payload))

@server.tool("glm_1wa")
async def tool_glm_1wa(request_json: str) -> str:
    """GLM: Thực hiện phép toán 1WA"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-1wa/", payload))

@server.tool("glm_2wa")
async def tool_glm_2wa(request_json: str) -> str:
    """GLM: Thực hiện phép toán 2WA"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-2wa/", payload))

@server.tool("glm_3wa")
async def tool_glm_3wa(request_json: str) -> str:
    """GLM: Thực hiện phép toán 3WA"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-3wa/", payload))

@server.tool("glm_4wa")
async def tool_glm_4wa(request_json: str) -> str:
    """GLM: Thực hiện phép toán 4WA"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/glm/glm-4wa/", payload))

@server.tool("mof_valid_data")
async def tool_mof_valid_data(request_json: str) -> str:
    """MOF: Kiểm tra tính hợp lệ dữ liệu khi nhập vào"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/mof-report/mof-valid-data/", payload))

@server.tool("mof_import_after_mapping")
async def tool_mof_import_after_mapping(request_json: str) -> str:
    """MOF: Nhập dữ liệu sau khi thực hiện mapping"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/mof-report/mof-import-data-after-mapping/", payload))

@server.tool("mof_pnt_11")
async def tool_mof_pnt_11(request_json: str) -> str:
    """MOF: Tổng hợp dữ liệu PNT-11"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/mof-report/mof-pnt-11/", payload))

@server.tool("mof_bctcq")
async def tool_mof_bctcq(request_json: str) -> str:
    """MOF: Lập báo cáo tài chính tổng hợp"""
    payload = json.loads(request_json)
    return json.dumps(await _post_json("/mof-report/mof-pnt-bctcq/", payload))

# Phần khởi động server và lắng nghe yêu cầu từ người dùng
if __name__ == "__main__":
    """Khởi động MCP stdio server để chờ yêu cầu từ người dùng và thực hiện các công cụ đã đăng ký"""
    asyncio.run(server.run_stdio())  # Chạy server MCP với giao diện điều khiển (stdio)
