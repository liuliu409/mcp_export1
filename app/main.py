from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from controllers.ping_controller import router as ping_router
from controllers.glm_controller import router as glm_router
from controllers.mof_controller import router as mof_router

app = FastAPI(title="mcp_export", version="0.1.0")

# CORS (tuỳ chọn)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Mount routers
app.include_router(ping_router)
app.include_router(glm_router)
app.include_router(mof_router)

@app.get("/")
async def root():
    return {"status": "ok", "service": "mcp_export"}

# chạy: uvicorn app.main:app --reload --port 8000
