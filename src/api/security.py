"""
Phần 3.3 - Bảo mật X-API-Key (băm SHA-256 + phân quyền Scope từ C360.C360_API_KEYS) &
Ghi nhật ký tra cứu bất đồng bộ theo lô (Async Batch Audit Log -> C360.C360_API_AUDIT_LOG).
"""
import asyncio
import hashlib
import logging
import time
from typing import Dict, Any, List, Optional, Tuple
from fastapi import Header, HTTPException, Request, status

from src.api.db import fetch_one_dict, get_target_connection
from src.config import AUDIT_LOG_BATCH_SIZE, AUDIT_LOG_FLUSH_INTERVAL_SEC

logger = logging.getLogger("c360_security")

# Cache ngắn hạn (30s) cho API key lookup để giảm tải DB
_API_KEY_CACHE: Dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}
_API_KEY_CACHE_TTL = 30.0


def hash_api_key(raw_key: str) -> str:
    """Băm SHA-256 cho X-API-Key trước khi đối chiếu với cột KEY_HASH_SHA256 trong DB."""
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def _lookup_api_key_from_db(key_hash: str) -> Optional[Dict[str, Any]]:
    now = time.monotonic()
    cached = _API_KEY_CACHE.get(key_hash)
    if cached and (now - cached[0]) < _API_KEY_CACHE_TTL:
        return cached[1]

    row = fetch_one_dict(
        """
        SELECT CLIENT_NAME, KEY_HASH_SHA256, SCOPES, IS_ACTIVE
        FROM C360.C360_API_KEYS
        WHERE KEY_HASH_SHA256 = :key_hash
          AND IS_ACTIVE = 1
        """,
        {"key_hash": key_hash},
    )
    _API_KEY_CACHE[key_hash] = (now, row)
    return row


class AsyncBatchAuditLogger:
    """
    Hàng đợi ghi Audit Log bất đồng bộ theo lô (Async Batch Audit Log).
    API đẩy log vào bộ đệm RAM trong < 0.01ms và trả kết quả ngay cho client;
    Background Worker định kỳ gom lô (batch) ghi xuống bảng C360.C360_API_AUDIT_LOG.
    """

    def __init__(self, batch_size: int = AUDIT_LOG_BATCH_SIZE, flush_interval: float = AUDIT_LOG_FLUSH_INTERVAL_SEC):
        self.queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=10000)
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._worker_task: Optional[asyncio.Task] = None

    def enqueue(self, entry: Dict[str, Any]) -> None:
        try:
            self.queue.put_nowait(entry)
        except asyncio.QueueFull:
            pass

    async def start_worker(self) -> None:
        self._worker_task = asyncio.create_task(self._flush_loop())

    async def stop_worker(self) -> None:
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        # Flush remaining items on shutdown
        remaining: List[Dict[str, Any]] = []
        while not self.queue.empty():
            try:
                remaining.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if remaining:
            await self._write_batch_to_db(remaining)

    async def _flush_loop(self) -> None:
        buffer: List[Dict[str, Any]] = []
        last_flush = time.monotonic()
        while True:
            try:
                timeout = max(0.1, self.flush_interval - (time.monotonic() - last_flush))
                item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                buffer.append(item)
            except asyncio.TimeoutError:
                pass

            if buffer and (len(buffer) >= self.batch_size or (time.monotonic() - last_flush) >= self.flush_interval):
                await self._write_batch_to_db(buffer)
                buffer.clear()
                last_flush = time.monotonic()

    async def _write_batch_to_db(self, batch: List[Dict[str, Any]]) -> None:
        def _sync_insert() -> None:
            sql = """
                INSERT INTO C360.C360_API_AUDIT_LOG (
                    CLIENT_NAME, ENDPOINT, QUERY_PARAMS, HTTP_STATUS, RESPONSE_MS
                ) VALUES (
                    :client_name, :endpoint, :query_params, :http_status, :response_ms
                )
            """
            rows = [
                {
                    "client_name": str(item.get("client_name", "ANONYMOUS"))[:100],
                    "endpoint": str(item.get("endpoint", ""))[:200],
                    "query_params": str(item.get("query_params", ""))[:1000],
                    "http_status": int(item.get("http_status", 0)),
                    "response_ms": float(item.get("response_ms", 0.0)),
                }
                for item in batch
            ]
            with get_target_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                conn.commit()

        try:
            await asyncio.to_thread(_sync_insert)
        except Exception as exc:
            logger.warning("Failed to flush %d audit log rows: %s", len(batch), exc)


audit_logger = AsyncBatchAuditLogger()


def require_scope(required_scope: str):
    """
    Dependency kiểm tra header X-API-Key (so khớp mã băm SHA-256 trên bảng C360.C360_API_KEYS
    và quyền truy cập theo từng scope API).
    """
    async def _verify(
        request: Request,
        x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    ) -> Dict[str, Any]:
        if not x_api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing X-API-Key header",
            )
        key_hash = hash_api_key(x_api_key)
        key_record = await asyncio.to_thread(_lookup_api_key_from_db, key_hash)
        if not key_record:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid or inactive X-API-Key",
            )
        allowed_scopes = {s.strip() for s in str(key_record.get("SCOPES", "")).split(",") if s.strip()}
        if required_scope not in allowed_scopes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"API key lacks required scope: {required_scope}",
            )
        client_info = {
            "client_name": key_record["CLIENT_NAME"],
            "key_hash": key_hash,
            "scopes": sorted(allowed_scopes),
        }
        request.state.client_info = client_info
        return client_info

    return _verify
