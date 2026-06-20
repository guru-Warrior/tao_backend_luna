#!/usr/bin/env python3
"""
Core staking ops parsing and aggregation (extract_stake_ops, aggregate_ops, parse_stake_events, …).
Used by `mempool.monitor` (`MempoolMonitor`).
"""

import time


EVM_STAKE_SELECTORS = {
    "0x1fc9b141": "addStake",
    "0x7d691e30": "removeStake",
    "0x5beb6b74": "addStakeLimit",
    "0x176848b2": "removeStakeLimit",
    "0xd4626bb9": "removeStakeFull",
    "0x1149f659": "moveStake",
    "0x17ce5f62": "transferStake",
    "0xc84654a8": "addStake_v1",
    "0x77b3061e": "addStake_wrap",
    "0x3161b7f6": "removeStake_wrap",
    "0x60fe5239": "addStakeLimit_c",
    "0x0f3e3aa7": "removeStake_c",
}

STAKE_FUNCTIONS = {
    "add_stake", "add_stake_limit",
    "remove_stake", "remove_stake_limit", "remove_stake_full_limit",
    "unstake_all", "move_stake", "swap_stake_limit", "transfer_stake",
}


def _normalize_ss58(value) -> str | None:
    """Normalise an address value that may arrive as:
      - plain ss58 string: ``"5Abc..."``
      - MultiAddress variant dict: ``{"Id": "5Abc..."}`` / ``{"AccountId": "0x..."}``
      - raw hex AccountId: ``"0xdeadbeef..."``

    substrate-interface decodes ``Proxy.proxy.real`` (type ``MultiAddress``) as
    a variant dict. Storing that dict straight into ``proxy_real`` and later
    calling ``str()`` on it produced literal ``"{'Id': '5...'}"`` strings in
    the mempool "from" column, which then failed to match the user-entered
    address. Normalise to a clean ss58 string here so every consumer
    (aggregate_ops grouping key, UI, signer resolution) sees the same form.
    """
    if not value:
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.startswith("0x"):
            try:
                from scalecodec.utils.ss58 import ss58_encode

                return ss58_encode(bytes.fromhex(s[2:]), ss58_format=42)
            except Exception:
                return None
        return s
    if isinstance(value, dict):
        inner = next(iter(value.values()), None)
        return _normalize_ss58(inner)
    return None


def _is_alpha_add_leg(op: dict) -> bool:
    """Same `:add` vs real-ADD discrimination as `MempoolMonitor._add_leg_is_alpha_rao_not_tao`.

    Kept here so `tao_equiv` (used by the CLI `track.py` filter) agrees with the
    backend renderer on the TAO equivalent of EVM move/transfer `:add` legs.
    """
    if op.get("_leg") != "add":
        return False
    k = str(op.get("kind") or "")
    return k.startswith((
        "move_stake", "transfer_stake", "swap_stake",
        "EVM.moveStake", "EVM.transferStake", "EVM.swapStake",
    ))


def tao_equiv(op: dict, prices: dict) -> float:
    t      = stake_type(op["kind"])
    leg    = op.get("_leg")
    netuid = op.get("netuid")
    # `:add` legs of move/transfer/swap carry alpha RAO in the origin subnet,
    # not TAO RAO — look up the price against the origin, not the destination.
    price_netuid = op.get("_origin_netuid_for_amount", netuid) if leg == "add" else netuid
    p      = prices.get(price_netuid) if price_netuid is not None else None

    if t == "EVM" or t == "MEV":
        v = op.get("amount_rao")
        return (v / 1_000_000_000) if v else 0.0

    if (t == "ADD" or leg == "add") and not _is_alpha_add_leg(op):
        v = op.get("amount_rao")
        return (v / 1_000_000_000) if v else 0.0

    v = op.get("amount_rao") or op.get("alpha_amount")
    if not v:
        return 0.0
    if p and p.get("alpha_in", 0) > 0:
        return (v / 1_000_000_000) * (p["tao_in"] / p["alpha_in"])
    return v / 1_000_000_000


def extract_stake_ops(ext: dict) -> list[dict]:
    ops  = []
    call = ext.get("call")
    if not call:
        return ops

    if call.get("call_module") == "Ethereum" and call.get("call_function") == "transact":
        ops.extend(_build_evm_op(call))
        return ops

    if call.get("call_module") == "MevShield" and call.get("call_function") == "submit_encrypted":
        ops.append({
            "kind":         "mev_shield",
            "wrapper":      "mev_shield",
            "hotkey":       None,
            "netuid":       None,
            "amount_rao":   None,
            "alpha_amount": None,
            "limit_price":  None,
            "allow_partial": None,
            "origin_netuid": None,
            "destination_netuid": None,
            "origin_hotkey": None,
            "destination_hotkey": None,
            "destination_coldkey": None,
            "raw_call":     call,
            "_leg":         None,
        })
        return ops

    _recurse_call(call, wrapper="direct", proxy_real=None, ops=ops)
    return ops


def _recurse_call(
    call: dict,
    wrapper: str,
    proxy_real: str | None,
    ops: list,
) -> None:
    fn  = call.get("call_function", "")
    mod = call.get("call_module", "")

    if fn in STAKE_FUNCTIONS:
        inner_ops = _build_native_op(call, wrapper)
        for op in inner_ops:
            if proxy_real:
                op["proxy_real"] = proxy_real
        ops.extend(inner_ops)
        return

    if mod == "Proxy" and fn == "proxy":
        call_args  = {a["name"]: a["value"] for a in call.get("call_args", [])}
        # ``real`` decodes to a MultiAddress variant dict
        # (``{"Id": "5..."}`` / ``{"AccountId": "0x..."}``) via substrate-
        # interface — normalise to a plain ss58 before it propagates down so
        # the mempool "from" column shows the actual address the user typed
        # into the Faker ``real`` field, not ``str(dict)``.
        real       = _normalize_ss58(call_args.get("real")) or proxy_real
        inner_call = call_args.get("call")
        # Same defensive unwrap as in the batch branch — some decode paths
        # nest the call inside `{"call": {...}}`.
        if isinstance(inner_call, dict) and "call" in inner_call and isinstance(inner_call["call"], dict) and "call_module" not in inner_call:
            inner_call = inner_call["call"]
        if isinstance(inner_call, dict):
            new_wrapper = "proxy" if wrapper != "proxy" else wrapper
            _recurse_call(inner_call, wrapper=new_wrapper, proxy_real=real, ops=ops)
        return

    if mod == "Utility" and fn in ("batch_all", "force_batch", "batch"):
        call_args = {a["name"]: a["value"] for a in call.get("call_args", [])}
        for inner in call_args.get("calls", []):
            # substrate-interface emits batch children in TWO shapes depending
            # on decode path: the sync monitor decoder produces the direct call
            # dict, while async substrate wraps it as `{"call": {...}}`. Accept
            # both — unwrap the `call` key if present, fall through otherwise.
            if isinstance(inner, dict) and "call" in inner and isinstance(inner["call"], dict):
                inner = inner["call"]
            if not isinstance(inner, dict):
                continue
            new_wrapper = wrapper if wrapper == "proxy" else "batch"
            _recurse_call(inner, wrapper=new_wrapper, proxy_real=proxy_real, ops=ops)
        return

    if mod == "Ethereum" and fn == "transact":
        ops.extend(_build_evm_op(call))


def _build_native_op(call: dict, wrapper: str) -> list[dict]:
    fn   = call.get("call_function", "")
    args = {a["name"]: a["value"] for a in call.get("call_args", [])}

    base = {
        "wrapper":             wrapper,
        "hotkey":              args.get("hotkey"),
        "limit_price":         args.get("limit_price"),
        "allow_partial":       args.get("allow_partial"),
        "origin_netuid":       args.get("origin_netuid"),
        "destination_netuid":  args.get("destination_netuid"),
        "origin_hotkey":       args.get("origin_hotkey"),
        "destination_hotkey":  args.get("destination_hotkey"),
        "destination_coldkey": args.get("destination_coldkey"),
        "raw_call":            call,
    }

    if fn in ("move_stake", "swap_stake_limit", "transfer_stake"):
        alpha = args.get("alpha_amount") or args.get("amount_staked") or args.get("amount")
        origin_netuid = args.get("origin_netuid")
        dest_netuid   = args.get("destination_netuid")

        if origin_netuid is not None and origin_netuid == dest_netuid:
            return []

        remove_leg = {**base,
            "kind":         f"{fn}:remove",
            "netuid":       origin_netuid,
            "amount_rao":   alpha,
            "alpha_amount": alpha,
            "_leg":         "remove",
        }
        add_leg = {**base,
            "kind":         f"{fn}:add",
            "netuid":       dest_netuid,
            "amount_rao":   alpha,
            "alpha_amount": alpha,
            "_origin_netuid_for_amount": origin_netuid,
            "_leg":         "add",
        }
        return [remove_leg, add_leg]

    return [{
        **base,
        "kind":         fn,
        "netuid":       args.get("netuid"),
        "amount_rao":   args.get("amount_staked") or args.get("amount_unstaked") or args.get("amount"),
        "alpha_amount": args.get("alpha_amount"),
        "_leg":         None,
    }]


def _decode_compact(data: bytes, offset: int) -> tuple[int, int]:
    b0 = data[offset]
    mode = b0 & 0x3
    if mode == 0:
        return b0 >> 2, offset + 1
    elif mode == 1:
        return int.from_bytes(data[offset:offset+2], 'little') >> 2, offset + 2
    elif mode == 2:
        return int.from_bytes(data[offset:offset+4], 'little') >> 2, offset + 4
    else:
        n = (b0 >> 2) + 4
        return int.from_bytes(data[offset+1:offset+1+n], 'little'), offset + 1 + n


def _decode_wrap_params(inp: str) -> tuple[int | None, int | None, str | None]:
    payload = inp[10:]

    def slot_int(n: int) -> int:
        s = payload[n*64:(n+1)*64]
        return int(s, 16) if s else 0

    amount_rao = slot_int(0)

    try:
        off1 = slot_int(1)
        off2 = slot_int(2)
        idx1 = off1 // 32
        idx2 = off2 // 32
        len1 = slot_int(idx1)
        len2 = slot_int(idx2)

        p1_data = payload[(idx1+1)*64 : (idx1+1)*64 + len1*2]
        hotkey_hex = "0x" + p1_data[:64] if len(p1_data) >= 64 else None

        if len2 >= 35:
            p2_hex  = payload[(idx2+1)*64 : (idx2+1)*64 + len2*2]
            p2_data = bytes.fromhex(p2_hex) if len(p2_hex) >= len2*2 else None
            if p2_data and len(p2_data) >= 35:
                netuid, _ = _decode_compact(p2_data, 34)
            else:
                netuid = None
        else:
            netuid = None
    except Exception:
        hotkey_hex, netuid = None, None

    return amount_rao, netuid, hotkey_hex


def _build_evm_op(call: dict) -> list[dict]:
    args  = {a["name"]: a["value"] for a in call.get("call_args", [])}
    tx    = args.get("transaction", {})
    inner = tx.get("EIP1559") or tx.get("Legacy") or tx.get("EIP2930") or {}
    inp   = str(inner.get("input", "0x"))
    sel   = "0x" + inp[2:10] if len(inp) >= 10 else "0x"

    evm_fn = EVM_STAKE_SELECTORS.get(sel)

    tx_type     = list(tx.keys())[0] if tx else "?"
    action      = inner.get("action", {})
    if isinstance(action, str):
        action_addr = action.lower() or None
    else:
        action_addr = action.get("Call") or str(inner.get("to", "")).lower() or None
    msg_value   = inner.get("value", 0)

    payload = inp[10:]

    def slot_int(n: int) -> int:
        s = payload[n*64:(n+1)*64]
        return int(s, 16) if s else 0

    def slot_hex(n: int) -> str:
        s = payload[n*64:(n+1)*64]
        return "0x" + s if s else None

    base = {
        "wrapper":              "evm",
        "evm_action":           action_addr,
        "evm_tx_type":          tx_type,
        "evm_selector":         sel,
        "limit_price":          None,
        "allow_partial":        None,
        "origin_netuid":        None,
        "destination_netuid":   None,
        "origin_hotkey":        None,
        "destination_hotkey":   None,
        "destination_coldkey":  None,
        "alpha_amount":         None,
        "raw_call":             call,
    }

    if not evm_fn:
        amount_rao = int(msg_value // 1_000_000_000) if msg_value else None
        return [{**base,
            "kind":       "EVM.transact",
            "hotkey":     None,
            "amount_rao": amount_rao,
            "netuid":     None,
            "_leg":       None,
        }]

    if evm_fn == "addStake":
        return [{**base,
            "kind":       "EVM.addStake",
            "hotkey":     slot_hex(0),
            "amount_rao": slot_int(1),
            "netuid":     slot_int(2),
            "_leg":       None,
        }]

    if evm_fn == "addStake_v1":
        return [{**base,
            "kind":       "EVM.addStake_v1",
            "hotkey":     slot_hex(0),
            "amount_rao": msg_value,
            "netuid":     slot_int(1),
            "_leg":       None,
        }]

    if evm_fn == "removeStake":
        return [{**base,
            "kind":       "EVM.removeStake",
            "hotkey":     slot_hex(0),
            "amount_rao": slot_int(1),
            "netuid":     slot_int(2),
            "_leg":       None,
        }]

    if evm_fn == "addStakeLimit":
        return [{**base,
            "kind":          "EVM.addStakeLimit",
            "hotkey":        slot_hex(0),
            "amount_rao":    slot_int(1),
            "limit_price":   slot_int(2),
            "allow_partial": bool(slot_int(3)),
            "netuid":        slot_int(4),
            "_leg":          None,
        }]

    if evm_fn == "removeStakeLimit":
        return [{**base,
            "kind":          "EVM.removeStakeLimit",
            "hotkey":        slot_hex(0),
            "amount_rao":    slot_int(1),
            "limit_price":   slot_int(2),
            "allow_partial": bool(slot_int(3)),
            "netuid":        slot_int(4),
            "_leg":          None,
        }]

    if evm_fn == "removeStakeFull":
        return [{**base,
            "kind":       "EVM.removeStakeFull",
            "hotkey":     slot_hex(0),
            "amount_rao": None,
            "netuid":     slot_int(1),
            "_leg":       None,
        }]

    if evm_fn == "moveStake":
        origin_netuid = slot_int(2)
        dest_netuid   = slot_int(3)
        if origin_netuid == dest_netuid:
            return []
        amount = slot_int(4)
        return [
            {**base,
                "kind":         "EVM.moveStake:remove",
                "hotkey":       slot_hex(0),
                "amount_rao":   amount,
                "alpha_amount": amount,
                "netuid":       origin_netuid,
                "origin_netuid":    origin_netuid,
                "destination_netuid": dest_netuid,
                "destination_hotkey": slot_hex(1),
                "_leg":         "remove",
            },
            {**base,
                "kind":         "EVM.moveStake:add",
                "hotkey":       slot_hex(1),
                "amount_rao":   amount,
                "alpha_amount": amount,
                "netuid":       dest_netuid,
                "origin_netuid":    origin_netuid,
                "destination_netuid": dest_netuid,
                "_origin_netuid_for_amount": origin_netuid,
                "_leg":         "add",
            },
        ]

    if evm_fn == "transferStake":
        origin_netuid = slot_int(2)
        dest_netuid   = slot_int(3)
        if origin_netuid == dest_netuid:
            return []
        amount = slot_int(4)
        return [
            {**base,
                "kind":              "EVM.transferStake:remove",
                "hotkey":            slot_hex(1),
                "amount_rao":        amount,
                "alpha_amount":      amount,
                "netuid":            origin_netuid,
                "origin_netuid":     origin_netuid,
                "destination_netuid": dest_netuid,
                "destination_coldkey": slot_hex(0),
                "_leg":              "remove",
            },
            {**base,
                "kind":              "EVM.transferStake:add",
                "hotkey":            slot_hex(1),
                "amount_rao":        amount,
                "alpha_amount":      amount,
                "netuid":            dest_netuid,
                "origin_netuid":     origin_netuid,
                "destination_netuid": dest_netuid,
                "destination_coldkey": slot_hex(0),
                "_origin_netuid_for_amount": origin_netuid,
                "_leg":              "add",
            },
        ]

    if evm_fn == "addStake_wrap":
        _amount, netuid, hotkey = _decode_wrap_params(inp)
        return [{**base,
            "kind":       "EVM.addStake_wrap",
            "hotkey":     hotkey,
            "amount_rao": None,
            "netuid":     netuid,
            "_leg":       None,
        }]

    if evm_fn == "removeStake_wrap":
        _amount, netuid, hotkey = _decode_wrap_params(inp)
        return [{**base,
            "kind":       "EVM.removeStake_wrap",
            "hotkey":     hotkey,
            "amount_rao": None,
            "netuid":     netuid,
            "_leg":       None,
        }]

    if evm_fn == "addStakeLimit_c":
        amount_wei = slot_int(0)
        netuid_val = slot_int(1)
        lp_wei     = slot_int(2)
        amount_rao = int(amount_wei // 1_000_000_000) if amount_wei else None
        lp_rao     = int(lp_wei     // 1_000_000_000) if lp_wei     else None
        return [{**base,
            "kind":        "EVM.addStakeLimit_c",
            "hotkey":      None,
            "amount_rao":  amount_rao,
            "limit_price": lp_rao,
            "netuid":      netuid_val or None,
            "_leg":        None,
        }]

    if evm_fn == "removeStake_c":
        amount_wei = slot_int(0)
        netuid_val = slot_int(1)
        amount_rao = int(amount_wei // 1_000_000_000) if amount_wei else None
        return [{**base,
            "kind":       "EVM.removeStake_c",
            "hotkey":     None,
            "amount_rao": amount_rao,
            "netuid":     netuid_val or None,
            "_leg":       None,
        }]

    return []


WRAPPER_BADGE = {
    "direct":     "direct",
    "proxy":      "proxy",
    "batch":      "batch",
    "evm":        "evm",
    "mev_shield": "mev",
}

def stake_type(kind: str) -> str:
    if kind in ("add_stake", "add_stake_limit",
                "EVM.addStake", "EVM.addStakeLimit", "EVM.addStake_v1",
                "EVM.addStake_wrap", "EVM.addStakeLimit_c"):
        return "ADD"
    if kind in ("remove_stake", "remove_stake_limit",
                "remove_stake_full_limit", "unstake_all",
                "EVM.removeStake", "EVM.removeStakeLimit", "EVM.removeStakeFull",
                "EVM.removeStake_wrap", "EVM.removeStake_c"):
        return "REMOVE"
    if kind.endswith(":remove"):
        return "REMOVE"
    if kind.endswith(":add"):
        return "ADD"
    if kind == "EVM.transact":
        return "EVM"
    if kind == "mev_shield":
        return "MEV"
    return "OTHER"


_EV_KIND_TYPE = {
    "StakeAdded":        "ADD",
    "StakeRemoved":      "REMOVE",
    "StakeMoved:remove": "REMOVE",
    "StakeMoved:add":    "ADD",
}


def _op_address(ext: dict, op: dict) -> str:
    if op.get("evm_action"):
        return str(op["evm_action"])
    pr = op.get("proxy_real")
    if pr:
        norm = _normalize_ss58(pr)
        if norm:
            return norm
    return str(ext.get("address") or op.get("hotkey") or "-")


def aggregate_ops(
    stake:   dict[str, list],
    pool:    dict[str, dict],
    seen_at: dict[str, float],
) -> list[dict]:
    """Merge ops only within the same pending extrinsic (`h`).

    Keying only (type, netuid, addr) merged *different* extrinsics from the same
    account — wrong amounts and rows that linger until all such txs cleared.
    """
    from collections import defaultdict

    now    = time.monotonic()
    groups: dict[tuple, list[tuple[dict, dict, float]]] = defaultdict(list)

    for h, ops in stake.items():
        ext   = pool.get(h, {})
        age_s = now - seen_at.get(h, now)
        for op in ops:
            t      = stake_type(op["kind"])
            netuid = op.get("netuid")
            addr   = _op_address(ext, op)
            key    = (h, t, netuid, addr)
            groups[key].append((ext, op, age_s))

    result = []
    for key, items in groups.items():
        max_age = max(age_s for _, _, age_s in items)

        first_ext, first_op, _ = items[0]
        merged = dict(first_op)
        merged["_address"]  = key[3]
        merged["_tx_count"] = len(items)
        merged["_age_s"]    = max_age
        # Propagate the canonical extrinsic hash from the decoded ext (set by
        # `monitor._extrinsic_hex_to_tracker_ext`). Rows coming from the same
        # pending ext will share this id, which the frontend uses as a stable
        # React key instead of array index + (address, netuid) tuples.
        try:
            merged["_tx_hash"] = str(first_ext.get("tx_hash") or "") or None
        except Exception:
            merged["_tx_hash"] = None

        if len(items) == 1:
            result.append(merged)
            continue

        total_rao   = None
        total_alpha = None
        for _, op, _ in items:
            v = op.get("amount_rao")
            if v is not None:
                total_rao = (total_rao or 0) + v
            v = op.get("alpha_amount")
            if v is not None:
                total_alpha = (total_alpha or 0) + v

        merged["amount_rao"]   = total_rao
        merged["alpha_amount"] = total_alpha
        result.append(merged)

    result.sort(key=lambda m: -m["_age_s"])
    return result


def aggregate_events(events: list[dict]) -> list[dict]:
    """Merge only events from the same extrinsic (same ext_idx).

    (type, netuid, coldkey) alone merged different extrinsics in one block.
    """
    from collections import defaultdict

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for i, ev in enumerate(events):
        t      = _EV_KIND_TYPE.get(ev["kind"], "OTHER")
        netuid = ev.get("netuid")
        addr   = str(ev.get("coldkey") or "-")
        ext_idx = ev.get("ext_idx")
        if ext_idx is None:
            ext_idx = ("_", i)
        key    = (t, netuid, addr, ext_idx)
        groups[key].append(ev)

    result = []
    for (t, netuid, addr, _ext_idx), evs in groups.items():
        if len(evs) == 1:
            merged = dict(evs[0])
            merged["_address"]  = addr
            merged["_tx_count"] = 1
            result.append(merged)
            continue

        first = evs[0]
        merged = dict(first)
        merged["_address"]  = addr
        merged["_tx_count"] = len(evs)

        total_tao   = None
        total_alpha = None
        for ev in evs:
            v = ev.get("tao_rao")
            if v is not None:
                total_tao = (total_tao or 0) + v
            v = ev.get("alpha_rao")
            if v is not None:
                total_alpha = (total_alpha or 0) + v
        merged["tao_rao"]   = total_tao
        merged["alpha_rao"] = total_alpha
        result.append(merged)

    result.sort(key=lambda e: (e.get("ext_idx") if e.get("ext_idx") is not None else 9999))
    return result


def parse_stake_events(events: list) -> list[dict]:
    ops = []
    for ev in events:
        v   = ev.value if hasattr(ev, "value") else ev
        mod = v.get("event", {}).get("module_id", "")
        eid = v.get("event", {}).get("event_id", "")
        if mod != "SubtensorModule" or eid not in ("StakeAdded", "StakeRemoved", "StakeMoved"):
            continue
        attrs = v.get("event", {}).get("attributes") or v.get("attributes", ())
        ext_idx = v.get("extrinsic_idx")

        if eid == "StakeAdded":
            coldkey, hotkey, tao_rao, alpha_rao, netuid, _ = attrs
            ops.append({
                "kind":       "StakeAdded",
                "coldkey":    coldkey,
                "hotkey":     hotkey,
                "tao_rao":    tao_rao,
                "alpha_rao":  alpha_rao,
                "netuid":     netuid,
                "ext_idx":    ext_idx,
            })

        elif eid == "StakeRemoved":
            coldkey, hotkey, tao_rao, alpha_rao, netuid, _ = attrs
            ops.append({
                "kind":       "StakeRemoved",
                "coldkey":    coldkey,
                "hotkey":     hotkey,
                "tao_rao":    tao_rao,
                "alpha_rao":  alpha_rao,
                "netuid":     netuid,
                "ext_idx":    ext_idx,
            })

        elif eid == "StakeMoved":
            coldkey, origin_hk, origin_netuid, dest_hk, dest_netuid, alpha_rao = attrs
            if origin_netuid == dest_netuid:
                continue
            ops.append({
                "kind":           "StakeMoved:remove",
                "coldkey":        coldkey,
                "hotkey":         origin_hk,
                "tao_rao":        None,
                "alpha_rao":      alpha_rao,
                "netuid":         origin_netuid,
                "dest_hotkey":    dest_hk,
                "dest_netuid":    dest_netuid,
                "ext_idx":        ext_idx,
            })
            ops.append({
                "kind":           "StakeMoved:add",
                "coldkey":        coldkey,
                "hotkey":         dest_hk,
                "tao_rao":        None,
                "alpha_rao":      alpha_rao,
                "netuid":         dest_netuid,
                "origin_netuid":  origin_netuid,
                "ext_idx":        ext_idx,
            })
    return ops


def _parse_balance_transfer_attrs(attrs) -> tuple | None:
    """Return ``(from, to, amount_rao)`` from ``Balances.Transfer`` attributes.

    substrate-interface may decode event fields as a tuple or as a named dict
    depending on version / metadata shape.
    """
    if isinstance(attrs, dict):
        from_val = attrs.get("from") or attrs.get("sender")
        to_val = (
            attrs.get("to")
            or attrs.get("dest")
            or attrs.get("recipient")
        )
        amount_val = attrs.get("amount") or attrs.get("value")
        if from_val is None or to_val is None or amount_val is None:
            return None
        return from_val, to_val, amount_val
    if isinstance(attrs, (list, tuple)) and len(attrs) >= 3:
        return attrs[0], attrs[1], attrs[2]
    return None


def parse_balance_transfer_events(events: list) -> list[dict]:
    """Parse ``Balances.Transfer`` events; amounts as TAO float."""
    ops: list[dict] = []
    for ev in events:
        v = ev.value if hasattr(ev, "value") else ev
        if not isinstance(v, dict):
            continue
        ev_inner = v.get("event", {}) if isinstance(v.get("event"), dict) else {}
        mod = ev_inner.get("module_id", "") or v.get("module_id", "")
        eid = ev_inner.get("event_id", "") or v.get("event_id", "")
        if mod != "Balances" or eid != "Transfer":
            continue
        attrs = ev_inner.get("attributes") or v.get("attributes", ())
        parsed = _parse_balance_transfer_attrs(attrs)
        if parsed is None:
            continue
        from_raw, to_raw, amount_raw = parsed
        from_addr = _normalize_ss58(from_raw)
        to_addr = _normalize_ss58(to_raw)
        if not from_addr or not to_addr:
            continue
        try:
            amount_tao = int(amount_raw) / 1e9
        except (TypeError, ValueError):
            continue
        ext_idx = v.get("extrinsic_idx")
        ops.append({
            "from": from_addr,
            "to": to_addr,
            "amountTao": amount_tao,
            "extrinsicIdx": ext_idx,
        })
    return ops
