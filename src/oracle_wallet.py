"""
Tiện ích dùng chung cho Oracle Cloud Autonomous DB Wallet và xác thực định danh Oracle.
Không phụ thuộc PySpark để các script Python thuần (như simulate_cdc_changes.py) có thể dùng chung.
"""
from __future__ import annotations

import re
from pathlib import Path

IDENTIFIER_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_$#]*")
TNS_ALIAS_PATTERN = re.compile(r"^\s*([A-Za-z0-9_.-]+)\s*=")


def validate_identifier(value: str, name: str) -> str:
    """Kiểm tra tên schema/table/column hợp lệ theo chuẩn định danh Oracle để tránh SQL injection."""
    cleaned = (value or "").strip()
    if not IDENTIFIER_PATTERN.fullmatch(cleaned):
        raise ValueError(f"Invalid Oracle identifier: {name}={value!r}")
    return cleaned


def resolve_wallet_dir(wallet_location: str) -> Path:
    """Xác minh thư mục Oracle Wallet tồn tại và tìm thấy file tnsnames.ora."""
    cleaned = (wallet_location or "").strip()
    if not cleaned:
        raise FileNotFoundError("Missing FULLLOAD_DB_WALLET_LOCATION in .env or --wallet")

    root_dir = Path(cleaned)
    if not root_dir.is_dir():
        raise FileNotFoundError(f"Wallet directory not found: {wallet_location!r}")

    if (root_dir / "tnsnames.ora").is_file():
        return root_dir
    for child in sorted(root_dir.iterdir()):
        if child.is_dir() and (child / "tnsnames.ora").is_file():
            return child

    raise FileNotFoundError(
        f"Wallet directory does not contain tnsnames.ora: {str(root_dir)!r}"
    )


def parse_tns_aliases(tnsnames_path: Path) -> list[str]:
    """Đọc danh sách TNS alias có trong file tnsnames.ora."""
    if not tnsnames_path.is_file():
        raise FileNotFoundError(f"tnsnames.ora not found: {str(tnsnames_path)!r}")
    aliases: list[str] = []
    seen: set[str] = set()
    for line in tnsnames_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = TNS_ALIAS_PATTERN.match(stripped)
        if match:
            alias = match.group(1)
            key = alias.lower()
            if key not in seen:
                seen.add(key)
                aliases.append(alias)
    return aliases


def resolve_tns_alias(tnsnames_path: Path, configured_alias: str = "") -> str:
    """
    Đối chiếu TNS alias với tnsnames.ora trong thư mục Wallet.
    Nếu configured_alias để trống, tự động chọn alias ưu tiên *_high -> *_medium -> alias đầu tiên.
    """
    aliases = parse_tns_aliases(tnsnames_path)
    if not aliases:
        raise ValueError(f"No TNS aliases found in {str(tnsnames_path)!r}")

    requested = (configured_alias or "").strip()
    if requested:
        for alias in aliases:
            if alias.lower() == requested.lower():
                return alias
        raise ValueError(
            f"TNS alias {requested!r} not found in {tnsnames_path.name}. "
            f"Available aliases: {', '.join(aliases)}"
        )

    for suffix in ("_high", "_medium"):
        for alias in aliases:
            if alias.lower().endswith(suffix):
                return alias
    return aliases[0]
