"""
Helpers for mempool.monitor: SubstrateInterface price map, event shape normalization,
extrinsic success sets. Core logic lives in stake_tracker.py.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Set

from config import BACKEND_DIR


def ensure_tracker_loaded() -> None:
    """No-op; stake_tracker is a normal import."""
    pass


def load_my_addresses_from_seeds() -> Set[str]:
    path = BACKEND_DIR / "wallet_seeds.json"
    try:
        with open(path, encoding="utf-8") as f:
            seeds = json.load(f)
        return {
            v["ss58_address"]
            for v in seeds.values()
            if isinstance(v, dict) and v.get("ss58_address")
        }
    except Exception:
        return set()


def fetch_prices_sync(substrate) -> Dict[int, Dict[str, int]]:
    try:
        am = substrate.query_map("SubtensorModule", "SubnetAlphaIn", max_results=512)
        tm = substrate.query_map("SubtensorModule", "SubnetTAO", max_results=512)
        alpha = {k: v.value for k, v in am.records}
        tao = {k: v.value for k, v in tm.records}
        out: Dict[int, Dict[str, int]] = {}
        for netuid in set(alpha) & set(tao):
            if netuid != 0 and alpha.get(netuid):
                out[netuid] = {"tao_in": tao[netuid], "alpha_in": alpha[netuid]}
        return out
    except Exception:
        return {}


def _normalize_event_value(v: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(v.get("event"), dict):
        return v
    return {
        "event": {
            "module_id": v.get("module_id"),
            "event_id": v.get("event_id"),
            "attributes": v.get("attributes"),
        },
        "extrinsic_idx": v.get("extrinsic_idx"),
        "phase": v.get("phase"),
    }


class _EventView:
    __slots__ = ("value",)

    def __init__(self, value: Dict[str, Any]) -> None:
        self.value = value


def parse_stake_events_normalized(events: list) -> List[dict]:
    import stake_tracker

    wrapped: List[Any] = []
    for ev in events:
        v = ev.value if hasattr(ev, "value") else ev
        if not isinstance(v, dict):
            continue
        wrapped.append(_EventView(_normalize_event_value(v)))
    return stake_tracker.parse_stake_events(wrapped)


def parse_balance_transfer_events_normalized(events: list) -> List[dict]:
    import stake_tracker

    wrapped: List[Any] = []
    for ev in events:
        v = ev.value if hasattr(ev, "value") else ev
        if not isinstance(v, dict):
            continue
        norm = _normalize_event_value(v)
        norm["extrinsic_idx"] = _extrinsic_idx_event(norm)
        wrapped.append(_EventView(norm))
    return stake_tracker.parse_balance_transfer_events(wrapped)


def _extrinsic_idx_event(v: Dict[str, Any]) -> Any:
    idx = v.get("extrinsic_idx")
    if idx is not None:
        return idx
    ph = v.get("phase")
    if isinstance(ph, dict):
        for k, val in ph.items():
            if k == "ApplyExtrinsic" or (isinstance(k, str) and "ApplyExtrinsic" in k):
                if isinstance(val, (list, tuple)) and val:
                    return val[0]
                if isinstance(val, int):
                    return val
    return None


def extrinsic_success_sets(events: list) -> tuple[set, set]:
    success: Set[int] = set()
    failed: Set[int] = set()
    for ev in events:
        v = ev.value if hasattr(ev, "value") else ev
        if not isinstance(v, dict):
            continue
        ph = v.get("phase")
        if ph != "ApplyExtrinsic" and not (
            isinstance(ph, dict)
            and ("ApplyExtrinsic" in ph or any("ApplyExtrinsic" in str(x) for x in ph))
        ):
            continue
        idx = _extrinsic_idx_event(v)
        if idx is None:
            continue
        ev_inner = v.get("event")
        if isinstance(ev_inner, dict):
            eid = ev_inner.get("event_id", "")
        else:
            eid = v.get("event_id", "")
        if eid == "ExtrinsicSuccess":
            success.add(idx)
        elif eid == "ExtrinsicFailed":
            failed.add(idx)
    return success, failed
