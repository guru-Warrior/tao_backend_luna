"""
Substrate JSON-RPC helpers: block numbers in headers are often hex strings (0x…).
Single source of truth avoids off-by-one / wrong-head bugs from inconsistent parsing.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def decode_block_number_field(number: Any) -> int | None:
    """Parse ``number`` from a block header (``chain_getHeader`` / subscription callback)."""
    if number is None:
        return None
    if isinstance(number, int):
        return number
    if isinstance(number, str):
        s = number.strip()
        if s.startswith("0x") or s.startswith("0X"):
            return int(s, 16)
        return int(s)
    logger.debug("decode_block_number_field: unexpected type %s", type(number))
    return None


def block_number_from_chain_get_header(header_result: dict[str, Any] | None) -> int | None:
    """Parse best head from ``substrate.rpc_request('chain_getHeader', [])`` response."""
    if not header_result or "result" not in header_result:
        return None
    result = header_result["result"]
    if not isinstance(result, dict):
        return None
    return decode_block_number_field(result.get("number"))
