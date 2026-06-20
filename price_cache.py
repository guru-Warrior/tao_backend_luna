"""Shared subnet spot-price cache (alpha_per_tao).

Populated by the mempool monitor on each block; read by the Trader (and any
other hot-path consumer) to avoid a heavy ``subtensor.subnet(netuid=…)`` RPC
on every stake/unstake submission.

Only the spot price is cached here — amount conversions / stake balances stay
owned by their respective callers. Keeping this module tiny and
dependency-free makes it safe to import from both the monitor thread and the
trade request thread.
"""
from __future__ import annotations

import threading
import time
from typing import Optional

# Single-value tuple keeps reads lock-free enough for our needs; we use a Lock
# for correctness on 32-bit platforms and to serialize updates.
_lock = threading.Lock()
_cache: dict[int, tuple[float, float]] = {}

# TTL roughly one block so limit-price computations always use a near-live
# spot. Slippage tolerance absorbs the ±1-block drift.
_TTL_SEC = 12.0


def put(netuid: int, alpha_per_tao: Optional[float]) -> None:
    """Store a freshly observed alpha_per_tao for ``netuid``."""
    if alpha_per_tao is None or alpha_per_tao <= 0:
        return
    try:
        nid = int(netuid)
    except (TypeError, ValueError):
        return
    with _lock:
        _cache[nid] = (float(alpha_per_tao), time.monotonic())


def get_fresh_alpha_per_tao(netuid: int) -> Optional[float]:
    """Return cached alpha_per_tao if younger than TTL, else ``None``."""
    try:
        nid = int(netuid)
    except (TypeError, ValueError):
        return None
    with _lock:
        hit = _cache.get(nid)
    if hit is None:
        return None
    price, ts = hit
    if time.monotonic() - ts > _TTL_SEC:
        return None
    return price


def get_fresh_tao_per_alpha(netuid: int) -> Optional[float]:
    """Convenience: 1 / alpha_per_tao when fresh, else ``None``."""
    p = get_fresh_alpha_per_tao(netuid)
    if not p:
        return None
    return 1.0 / p
