"""
Phần 3.3 - Entrypoint ứng dụng FastAPI cung cấp dịch vụ tra cứu C360.
Tích hợp Oracle Connection Pool, Middleware đo thời gian phản hồi & Async Batch Audit Logger.
"""
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request

from src.api.db import close_db_pool, ensure_default_dev_api_key, init_db_pool
from src.api.routers.ca_nhan import router as ca_nhan_router
from src.api.security import audit_logger, hash_api_key


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db_pool()
    ensure_default_dev_api_key(hash_api_key("dev-secret-key"))
    await audit_logger.start_worker()
    yield
    await audit_logger.stop_worker()
    close_db_pool()


app = FastAPI(
    title="C360 Citizen Lookup Service API",
    description="Dịch vụ REST API Tra cứu Thông tin Công dân C360 (Bảo hiểm Xã hội)",
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def async_audit_log_middleware(request: Request, call_next):
    start_ts = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = round((time.perf_counter() - start_ts) * 1000, 2)

    if request.url.path.startswith("/api/v1/ca-nhan"):
        client_info = getattr(request.state, "client_info", None)
        audit_logger.enqueue({
            "client_name": client_info["client_name"] if client_info else "ANONYMOUS",
            "endpoint": request.url.path,
            "query_params": str(request.query_params),
            "http_status": response.status_code,
            "response_ms": elapsed_ms,
        })
    return response


app.include_router(ca_nhan_router)
