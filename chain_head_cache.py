"""Shared chain head cache.

Single-writer (the mempool monitor's block subscription thread / head-poll
path) publishes the latest observed head block number here. Single-reader
hot path is the :mod:`trader` module, which uses it to pre-fill
``era['current']`` on ``create_signed_extrinsic`` and thereby skip two
``chain_getFinalisedHead`` + ``chain_getHeader`` RPCs per trade.

Why this is safe:
  - Substrate's mortal era (``period``/``phase``) only needs ``current`` to
    be a real, recent block number — the chain anchors the era to
    ``current & ~(period-1)``. Finalized vs best-head is interchangeable
    here; both are within a few blocks of each other. Slippage tolerance
    (or MEV Shield's separate flow) absorbs the tiny drift.
  - If the cache is empty or stale, the reader falls back to the
    library's normal RPC path (no behaviour change).
  - TTL is deliberately generous (~2 blocks = 24s on Bittensor) so that
    momentary WS hiccups in the monitor don't demote us back to the slow
    path unnecessarily.

Kept intentionally tiny & dependency-free — imported from both the monitor
thread and the trade request thread, must not pull in heavy modules.
"""
from __future__ import annotations

import threading
import time
from typing import Optional

_lock = threading.Lock()
_head_block_number: Optional[int] = None
_head_block_hash: Optional[str] = None
_updated_at: float = 0.0

# Generous TTL: 2 Bittensor blocks. Beyond this we prefer to let the library
# fetch a fresh era anchor itself rather than risk signing with a stale
# ``current``. In practice the monitor refreshes every ~12s so this never
# expires during healthy operation.
_TTL_SEC = 24.0


def put(block_number: int, block_hash: Optional[str] = None) -> None:
    """Publish the latest observed head. Called by the monitor on every block."""
    global _head_block_number, _head_block_hash, _updated_at
    try:
        bn = int(block_number)
    except (TypeError, ValueError):
        return
    if bn <= 0:
        return
    with _lock:
        # Guard against out-of-order updates (head-poll vs subscription race).
        if _head_block_number is not None and bn < _head_block_number:
            return
        _head_block_number = bn
        if block_hash is not None:
            _head_block_hash = block_hash
        _updated_at = time.monotonic()


def get_fresh_head_number() -> Optional[int]:
    """Return the cached head block number if younger than TTL, else ``None``."""
    with _lock:
        bn = _head_block_number
        ts = _updated_at
    if bn is None:
        return None
    if time.monotonic() - ts > _TTL_SEC:
        return None
    return bn


def get_fresh_head_hash() -> Optional[str]:
    """Return the cached head block hash if younger than TTL, else ``None``."""
    with _lock:
        h = _head_block_hash
        ts = _updated_at
    if h is None:
        return None
    if time.monotonic() - ts > _TTL_SEC:
        return None
    return h
