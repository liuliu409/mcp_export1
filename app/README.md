# mcp_export (FastAPI)

## Chạy dev
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
mkdir -p data temp_exports
uvicorn app.main:app --reload --port 8000

## Thử ping
curl http://127.0.0.1:8000/ping

## Ghi chú dữ liệu
- Đặt các file `.parquet` test vào `./data/` (local mode).
- MOF/GLM đọc/ghi parquet qua modules/db_parquet.py.
