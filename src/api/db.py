"""
Quản lý kết nối Oracle (Connection Pool) cho tầng REST API & Dashboard C360.
"""
from __future__ import annotations

import datetime
import decimal
from typing import Any, Dict, List, Optional, Sequence
import oracledb

from src.config import TARGET_DB, SOURCE_DB

_target_pool: Optional[oracledb.ConnectionPool] = None


def init_db_pool() -> oracledb.ConnectionPool:
    global _target_pool
    if _target_pool is None:
        _target_pool = oracledb.create_pool(
            user=TARGET_DB.user,
            password=TARGET_DB.password,
            dsn=TARGET_DB.dsn,
            min=2,
            max=10,
            increment=1,
        )
    return _target_pool


def close_db_pool() -> None:
    global _target_pool
    if _target_pool is not None:
        _target_pool.close(force=True)
        _target_pool = None


def get_target_connection() -> oracledb.Connection:
    if _target_pool is None:
        init_db_pool()
    assert _target_pool is not None
    return _target_pool.acquire()


def get_source_connection() -> oracledb.Connection:
    return oracledb.connect(
        user=SOURCE_DB.user,
        password=SOURCE_DB.password,
        dsn=SOURCE_DB.dsn,
    )


def _serialize_value(val: Any) -> Any:
    if isinstance(val, (datetime.datetime, datetime.date)):
        return val.isoformat()
    if isinstance(val, decimal.Decimal):
        return int(val) if val == int(val) else float(val)
    return val


def rows_to_dicts(cursor: oracledb.Cursor, rows: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    cols = [col[0] for col in cursor.description]
    return [
        {col: _serialize_value(val) for col, val in zip(cols, row)}
        for row in rows
    ]


def fetch_all_dicts(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    with get_target_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            return rows_to_dicts(cur, cur.fetchall())


def fetch_one_dict(sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    with get_target_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            row = cur.fetchone()
            if row is None:
                return None
            cols = [col[0] for col in cur.description]
            return {col: _serialize_value(val) for col, val in zip(cols, row)}


def ensure_default_dev_api_key(key_hash: str) -> None:
    """Đảm bảo có sẵn dev API key ('dev-secret-key') trong bảng C360.C360_API_KEYS."""
    sql = """
        MERGE INTO C360.C360_API_KEYS tgt
        USING (SELECT :key_hash AS KEY_HASH_SHA256 FROM DUAL) src
        ON (tgt.KEY_HASH_SHA256 = src.KEY_HASH_SHA256)
        WHEN NOT MATCHED THEN INSERT (
            CLIENT_NAME, KEY_HASH_SHA256, SCOPES, IS_ACTIVE
        ) VALUES (
            'DEV_INTERNAL_PORTAL',
            :key_hash,
            'CA_NHAN:READ,CA_NHAN:BATCH,CA_NHAN:SEARCH,CA_NHAN:DELTA',
            1
        )
    """
    with get_target_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"key_hash": key_hash})
        conn.commit()
