import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.database import init_db

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await init_db()
    except Exception as e:
        logger.error("DB 초기화 실패 (DATABASE_URL 설정을 확인하세요): %s", e)
    yield

app = FastAPI(title="약조심 API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

from app.routers import medicines, interactions, ocr  # noqa: E402
app.include_router(medicines.router, prefix="/api/v1")
app.include_router(interactions.router, prefix="/api/v1")
app.include_router(ocr.router, prefix="/api/v1")

@app.get("/health")
async def health():
    return {"status": "ok"}
