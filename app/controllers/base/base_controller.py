from fastapi import APIRouter, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from dotenv import load_dotenv
import os

load_dotenv()

VALID_API_KEYS = set(
    filter(None, [os.getenv("DEFAULT_API_KEY")] + os.getenv("API_KEYS", "").split(","))
)
api_key_header = APIKeyHeader(name="Authorization", auto_error=True)

def verify_api_key(api_key: str = Security(api_key_header)):
    if VALID_API_KEYS and api_key not in VALID_API_KEYS:
        raise HTTPException(status_code=403, detail="Could not validate credentials")

class BaseController:
    def __init__(self, prefix: str, tags: list[str]):
        # Nếu muốn bật API key, thêm dependencies=[Depends(verify_api_key)]
        self.router = APIRouter(prefix=prefix, tags=tags)

    def add_route(self, path: str, endpoint, methods: list[str]):
        self.router.add_api_route(path, endpoint, methods=methods)
