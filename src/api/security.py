"""
Phần 3.3 - Bảo mật X-API-Key (băm SHA-256 + phân quyền Scope từ C360.C360_API_KEYS) &
Ghi nhật ký tra cứu bất đồng bộ theo lô (Async Batch Audit Log -> C360.C360_API_AUDIT_LOG).
"""
import asyncio
import hashlib
import logging
import time
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode
from fastapi import Header, HTTPException, Request, status

from src.api.db import fetch_one_dict, get_target_connection
from src.config import AUDIT_LOG_BATCH_SIZE, AUDIT_LOG_FLUSH_INTERVAL_SEC

logger = logging.getLogger("c360_security")

_SENSITIVE_QUERY_KEYS = {
    "ho_ten",
    "ngay_sinh",
    "ma",
    "so_dinh_danh",
    "cccd",
    "so_cccd",
    "so_cmtnd",
    "phone",
    "sdt",
}


def redact_query_params(raw_query: str) -> str:
    """Ẩn các tham số định danh cá nhân (PII) trước khi ghi Audit Log."""
    if not raw_query:
        return ""
    pairs = parse_qsl(raw_query, keep_blank_values=True)
    if not pairs:
        return raw_query
    redacted = [
        (k, "***" if k.lower() in _SENSITIVE_QUERY_KEYS else v)
        for k, v in pairs
    ]
    return urlencode(redacted)

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
        self._buffer: List[Dict[str, Any]] = []
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._worker_task: Optional[asyncio.Task] = None
        self.enqueued_count: int = 0
        self.dropped_count: int = 0
        self.flushed_count: int = 0
        self.failed_batches: int = 0

    def enqueue(self, entry: Dict[str, Any]) -> None:
        try:
            self.queue.put_nowait(entry)
            self.enqueued_count += 1
        except asyncio.QueueFull:
            self.dropped_count += 1
            logger.warning("Audit log queue full; dropped_count=%d", self.dropped_count)

    def get_metrics(self) -> Dict[str, int]:
        return {
            "enqueued": self.enqueued_count,
            "dropped": self.dropped_count,
            "flushed": self.flushed_count,
            "failed_batches": self.failed_batches,
            "queue_size": self.queue.qsize(),
        }

    async def start_worker(self) -> None:
        self._worker_task = asyncio.create_task(self._flush_loop())

    async def stop_worker(self) -> None:
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        # Flush remaining items from both _buffer and queue on shutdown
        remaining: List[Dict[str, Any]] = list(self._buffer)
        self._buffer.clear()
        while not self.queue.empty():
            try:
                remaining.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if remaining:
            await self._write_batch_to_db(remaining)

    async def _flush_loop(self) -> None:
        last_flush = time.monotonic()
        while True:
            try:
                timeout = max(0.1, self.flush_interval - (time.monotonic() - last_flush))
                item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                self._buffer.append(item)
            except asyncio.TimeoutError:
                pass

            if self._buffer and (len(self._buffer) >= self.batch_size or (time.monotonic() - last_flush) >= self.flush_interval):
                batch_to_write = list(self._buffer)
                self._buffer.clear()
                await self._write_batch_to_db(batch_to_write)
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

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                await asyncio.to_thread(_sync_insert)
                self.flushed_count += len(batch)
                return
            except Exception as exc:
                if attempt < max_attempts:
                    await asyncio.sleep(0.2 * attempt)
                else:
                    self.failed_batches += 1
                    logger.warning(
                        "Failed to flush %d audit log rows after %d attempts: %s",
                        len(batch),
                        max_attempts,
                        exc,
                    )


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
