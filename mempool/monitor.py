"""
MempoolMonitor: real-time stake operations (mempool + last block) for the FastAPI WebSocket API.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timezone

from substrateinterface import SubstrateInterface
from substrateinterface.utils.ss58 import ss58_encode

import stake_tracker_bridge as st_bridge
import price_cache

try:
    import bittensor as bt

    BITTENSOR_AVAILABLE = True
except ImportError:
    BITTENSOR_AVAILABLE = False

from config import FINNEY_WS, MEMPOOL_MAX_AGE_S

logger = logging.getLogger(__name__)

# SS58 prefixes omitted from whitelist TAO transfer history.
_TRANSFER_EXCLUDED_ADDR_PREFIXES = ("5EYCAe5", "5DceuTr7", "5DknG1aS")
_TRANSFER_MIN_AMOUNT_TAO = 1.0


class MempoolMonitor:
    def __init__(self) -> None:
        self.substrate = SubstrateInterface(url=FINNEY_WS)
        self.running = False
        # Mempool: stake_tracker extract_stake_ops + aggregate_ops
        self._mempool_decoded = {}
        self._mempool_seen_at = {}
        self._mempool_pool = {}
        self._mempool_stake = {}
        self._tracker_prices = {}
        # Throttle the (expensive) full price-table refresh. ``fetch_prices_sync``
        # runs two ``query_map`` calls (~512 records each) that measured at
        # 120–780 ms per block on the public Finney RPC — by far the single
        # largest cost in ``process_new_block``. Subnet prices drift only
        # slightly per block, so refreshing every ``_price_refresh_sec`` (≈ one
        # block) instead of on *every* block removes that cost from the hot
        # block-processing path without any visible accuracy loss in the UI.
        self._price_refresh_sec: float = float(
            os.environ.get("PRICE_REFRESH_SEC", "12")
        )
        self._last_price_fetch: float = 0.0
        # Separate throttle for the main-loop snapshot path (alpha→TAO conversion
        # for sell rows). Block thread also refreshes ``_tracker_prices``; this
        # lets the mempool panel fill sell amounts immediately even before the
        # next block is processed.
        self._last_mempool_prices_fetch: float = 0.0
        self._my_addresses = self._build_my_addresses()
        st_bridge.ensure_tracker_loaded()
        self.last_block_data = []
        self.last_block_transfers: list = []
        self._transfer_history: list = []
        self._transfer_history_keys: set = set()
        self._whitelist_addresses: set = set()
        self._transfer_history_limit = 1000
        # Cache subnet prices: netuid -> {'price': alpha_per_tao, 'time': monotonic}
        self.subnet_price_cache = {}
        self._price_cache_ttl = 12  # refresh every ~12s (~1 block)
        # Cache subnet types (netuid -> is_dynamic) to avoid repeated queries
        self.subnet_type_cache = {}
        # Cache stake TAO for full-unstake mempool rows: (coldkey, hotkey, netuid) -> float
        self.stake_cache = {}
        # Max synchronous stake lookups per snapshot for Rem·F rows (immediate amount).
        self._mempool_stake_sync_max: int = int(
            os.environ.get("MEMPOOL_STAKE_SYNC_MAX", "6")
        )
        # Per-snapshot scratch: coldkey -> StakeInfoRuntimeApi rows (avoid N identical RPCs).
        self._runtime_stake_entries_scratch: dict[str, list] = {}
        # Spot τ/α frozen when a pending extrinsic first enters our mempool view.
        # Comparing limit_price to *current* spot drifts while the tx sits in the
        # pool (often showing 8–10% when the user submitted 1–5%). Keyed by
        # (canonical tx_hash, netuid).
        self._mempool_spot_at_seen: dict[tuple[str, int], float] = {}
        # Spot table captured when the last block was processed (for Last-block Slip).
        self._last_block_tao_per_alpha: dict[int, float] = {}
        # Initialize bittensor subtensor for price queries if available
        self.subtensor = None
        # Invalidate mempool stake display cache only when pending extrinsic set changes
        self._mempool_keys_fp = None
        self._last_seen_block = None
        # Track non-stake extrinsics so they aren't re-decoded every cycle
        self._non_stake_exts: set = set()
        # Track extrinsics that we've already observed inside a confirmed block.
        # Finney's tx pool does NOT evict included txs immediately — observed
        # ~30 s between block inclusion and ``author_pendingExtrinsics`` no
        # longer returning the tx. Without this set, ``poll_mempool`` would
        # keep re-admitting the confirmed tx into ``_mempool_stake`` for those
        # ~30 s and the UI would keep showing a "live" row long after the user
        # has seen the tx in Last block. Populated by ``process_new_block``
        # when it fetches the block body and pruned at the bottom of
        # ``poll_mempool`` via ``&= current_pending`` so the flag only lives
        # as long as the chain keeps advertising the tx.
        self._confirmed_exts: set = set()
        # Max wall-clock age (monotonic seconds) of a pending extrinsic before
        # we forcibly hide it from the UI. Guards against chain nodes that
        # legitimately return stuck / invalid txs from ``author_pendingExtrinsics``
        # for many blocks — most common after brief WS or network interruptions
        # on long-running backends. See ``config.MEMPOOL_MAX_AGE_S``.
        self._mempool_max_age_s: float = float(MEMPOOL_MAX_AGE_S)
        # Cache for free TAO balances (address -> {'balance': float, 'time': float})
        self.free_balance_cache = {}
        # Free balance only changes on finalized blocks; monitor clears cache each new block.
        # Long TTL avoids repeated System.Account queries between block boundaries.
        self.free_balance_cache_ttl = 3600.0
        # Per-address background refresh cadence for the displayed free balance.
        # Replaces the old per-block mass invalidation (which re-queued a
        # ``System.Account`` query for EVERY address that appeared in a block,
        # every block — an RPC storm on busy blocks that contended with block
        # processing on the shared public endpoint). Now ``_free_tao_cached``
        # always returns the last known value immediately and only queues a
        # background refresh once the cached entry ages past this interval, so a
        # displayed balance can lag reality by at most ``_balance_refresh_sec``
        # (cheap, never-blanking, and bounded RPC volume).
        self._balance_refresh_sec: float = float(
            os.environ.get("BALANCE_REFRESH_SEC", "60")
        )
        # Max addresses collapsed into a single ``query_multi`` balance refresh.
        # One round trip per batch keeps balance RPC volume flat even when a busy
        # block re-stales many transacting addresses.
        self._balance_batch_size: int = int(
            os.environ.get("BALANCE_BATCH_SIZE", "64")
        )
        # Background RPC fetch queue: addresses/stakes to query without blocking snapshot
        self._bg_fetch_balance_queue = set()   # set of addresses
        self._bg_fetch_stake_queue = set()     # set of (address, netuid)
        if BITTENSOR_AVAILABLE:
            try:
                self.subtensor = bt.Subtensor(network='finney')
            except Exception:
                pass

    def _build_my_addresses(self):
        """Addresses from wallet_seeds.json — used to tag mempool rows as \"mine\" (optional filter)."""
        return set(st_bridge.load_my_addresses_from_seeds())

    def set_whitelist_addresses(self, addresses: set[str]) -> None:
        """Normalized SS58 coldkeys whose ``Balances.Transfer`` events we track."""
        self._whitelist_addresses = {a for a in addresses if a}
        if not self._whitelist_addresses:
            self._transfer_history = []
            self._transfer_history_keys = set()

    @staticmethod
    def _transfer_excluded_addr(addr: str) -> bool:
        if not addr:
            return False
        return any(addr.startswith(p) for p in _TRANSFER_EXCLUDED_ADDR_PREFIXES)

    @classmethod
    def _transfer_event_excluded(
        cls, from_a: str, to_a: str, amount_tao=None
    ) -> bool:
        if cls._transfer_excluded_addr(from_a) or cls._transfer_excluded_addr(to_a):
            return True
        try:
            amt = float(amount_tao) if amount_tao is not None else 0.0
        except (TypeError, ValueError):
            return True
        return amt < _TRANSFER_MIN_AMOUNT_TAO

    def _append_block_transfers(self, block_number: int) -> None:
        """Keep a FIFO ring of whitelist-only TAO transfer events."""
        import stake_tracker as st

        wl = self._whitelist_addresses
        if not wl:
            return
        for t in self.last_block_transfers or []:
            from_a = st._normalize_ss58(t.get("from")) or t.get("from")
            to_a = st._normalize_ss58(t.get("to")) or t.get("to")
            if not from_a or not to_a:
                continue
            if from_a not in wl and to_a not in wl:
                continue
            if self._transfer_event_excluded(from_a, to_a, t.get("amountTao")):
                continue
            row = {
                "block": block_number,
                "from": from_a,
                "to": to_a,
                "amountTao": t.get("amountTao"),
                "extrinsicIdx": t.get("extrinsicIdx"),
            }
            key = (
                row["block"],
                row.get("extrinsicIdx"),
                row["from"],
                row["to"],
                row.get("amountTao"),
            )
            if key in self._transfer_history_keys:
                continue
            self._transfer_history_keys.add(key)
            self._transfer_history.insert(0, row)
        limit = self._transfer_history_limit
        if len(self._transfer_history) > limit:
            for dropped in self._transfer_history[limit:]:
                self._transfer_history_keys.discard(
                    (
                        dropped["block"],
                        dropped.get("extrinsicIdx"),
                        dropped["from"],
                        dropped["to"],
                        dropped.get("amountTao"),
                    )
                )
            self._transfer_history = self._transfer_history[:limit]

    def _extrinsic_hex_to_tracker_ext(self, ext_hex, substrate=None):
        sub = substrate or self.substrate
        extrinsic = sub.decode_scale(
            type_string="Extrinsic",
            scale_bytes=ext_hex,
        )
        extrinsic_data = extrinsic.value if hasattr(extrinsic, "value") else extrinsic
        if not extrinsic_data or "call" not in extrinsic_data:
            return None
        addr = extrinsic_data.get("address") or extrinsic_data.get("account_id")
        # Compute the canonical Substrate extrinsic hash (blake2b-256 of the
        # SCALE bytes). This is identical to the `extrinsic_hash` field the
        # async `substrate-interface` path exposes on `Extrinsic.value`, so the
        # two code paths emit the same id for the same transaction. We prefer
        # the decoder's own `extrinsic_hash` if it already exists in the
        # decoded value; otherwise fall back to hashing the raw hex.
        tx_hash = ""
        try:
            tx_hash = str(extrinsic_data.get("extrinsic_hash") or "")
        except Exception:
            tx_hash = ""
        if not tx_hash:
            try:
                raw = bytes.fromhex(
                    ext_hex[2:] if isinstance(ext_hex, str) and ext_hex.startswith("0x")
                    else ext_hex
                )
                tx_hash = "0x" + hashlib.blake2b(raw, digest_size=32).hexdigest()
            except Exception:
                tx_hash = ""
        return {
            "address": str(addr) if addr else "",
            "call": extrinsic_data["call"],
            "tx_hash": tx_hash,
        }

    @staticmethod
    def _add_leg_is_alpha_rao_not_tao(merged: dict) -> bool:
        """`:add` legs of move/transfer/swap extrinsics carry alpha RAO, not TAO RAO.

        The `:add` leg shares the same amount value as its paired `:remove` leg,
        and that value is the user-specified `alpha_amount` denominated in the
        *origin* subnet's alpha tokens. Without this flag, the caller's ADD
        short-circuit (`v / 1e9` → TAO) would mis-render the amount by a factor
        of `1 / tao_per_alpha` (often 10–1000×).

        Covers both native kinds (`move_stake:add`, `transfer_stake:add`,
        `swap_stake_limit:add`) and their EVM precompile twins
        (`EVM.moveStake:add`, `EVM.transferStake:add`).
        """
        if merged.get("_leg") != "add":
            return False
        k = str(merged.get("kind") or "")
        return k.startswith((
            "move_stake", "transfer_stake", "swap_stake",
            "EVM.moveStake", "EVM.transferStake", "EVM.swapStake",
        ))

    def _tracker_merged_op_amount_tao(self, merged, prices, substrate=None):
        import stake_tracker as st

        kind = merged["kind"]
        st_t = st.stake_type(kind)
        leg = merged.get("_leg")
        netuid = merged.get("netuid")
        if st_t in ("EVM", "MEV"):
            v = merged.get("amount_rao")
            return (v / 1e9) if v is not None else 0.0
        # add_stake: amount is TAO RAO. move_stake:add leg: same alpha RAO as remove leg — convert.
        if (st_t == "ADD" or leg == "add") and not self._add_leg_is_alpha_rao_not_tao(merged):
            v = merged.get("amount_rao")
            return (v / 1e9) if v is not None else 0.0
        v = merged.get("amount_rao") or merged.get("alpha_amount")
        if not v:
            return 0.0
        pn = merged.get("_origin_netuid_for_amount", netuid)
        if isinstance(pn, tuple):
            pn = pn[0] if len(pn) else None
        if pn is not None and prices:
            p = prices.get(pn)
            if p and p.get("alpha_in", 0) > 0:
                return (v / 1e9) * (p["tao_in"] / p["alpha_in"])
        if pn is not None:
            tao = self._convert_alpha_to_tao(v, pn, substrate=substrate)
            if tao is not None:
                return float(tao)
        return None

    def _tracker_event_to_last_block_row(self, ev, success, substrate=None):
        kind = ev["kind"]
        coldkey = str(ev.get("coldkey", ""))
        hotkey = str(ev.get("hotkey", "")) if ev.get("hotkey") is not None else coldkey
        if kind in ("StakeAdded", "StakeRemoved"):
            tao_rao = ev.get("tao_rao")
            amount = (tao_rao / 1e9) if tao_rao is not None else 0.0
            return {
                "extrinsic_idx": ev.get("ext_idx"),
                "type": "Stake" if kind == "StakeAdded" else "Unstake",
                "method": "Add" if kind == "StakeAdded" else "Remove",
                "address": coldkey,
                "coldkey": coldkey,
                "hotkey": hotkey,
                "amount": amount,
                "netuid": ev.get("netuid"),
                "success": success,
                "limit_price": None,
            }
        if kind == "StakeMoved:remove":
            ar = ev.get("alpha_rao")
            origin = ev.get("netuid")
            dest = ev.get("dest_netuid")
            amount = 0.0
            if ar is not None and origin is not None:
                c = self._convert_alpha_to_tao(ar, origin, substrate=substrate)
                amount = float(c) if c is not None else None
            return {
                "extrinsic_idx": ev.get("ext_idx"),
                "type": "Unstake",
                "method": "Move",
                "address": coldkey,
                "coldkey": coldkey,
                "hotkey": hotkey,
                "amount": amount,
                "netuid": (origin, dest) if origin is not None and dest is not None else origin,
                "success": success,
                "limit_price": None,
            }
        if kind == "StakeMoved:add":
            ar = ev.get("alpha_rao")
            origin = ev.get("origin_netuid")
            dest = ev.get("netuid")
            amount = 0.0
            pn = origin if origin is not None else dest
            if ar is not None and pn is not None:
                c = self._convert_alpha_to_tao(ar, pn, substrate=substrate)
                amount = float(c) if c is not None else None
            return {
                "extrinsic_idx": ev.get("ext_idx"),
                "type": "Stake",
                "method": "Move",
                "address": coldkey,
                "coldkey": coldkey,
                "hotkey": hotkey,
                "amount": amount,
                "netuid": (origin, dest) if origin is not None and dest is not None else dest,
                "success": success,
                "limit_price": None,
            }
        import stake_tracker as st

        st_t = st.stake_type(kind)
        if st_t == "REMOVE":
            row_type = "Unstake"
        elif st_t == "MEV":
            row_type = "MEV"
        else:
            row_type = "Stake"
        return {
            "extrinsic_idx": ev.get("ext_idx"),
            "type": row_type,
            "method": self._kind_to_ui_method_core(kind),
            "address": coldkey,
            "coldkey": coldkey,
            "hotkey": hotkey,
            "amount": 0.0,
            "netuid": ev.get("netuid"),
            "success": success,
            "limit_price": None,
        }

    def _resolve_signer_for_tracker_merged(self, merged):
        import stake_tracker as st

        t = st.stake_type(merged["kind"])
        netuid = merged.get("netuid")
        want = merged.get("_address")
        for h, ops in self._mempool_stake.items():
            ext = self._mempool_pool.get(h, {})
            for op in ops:
                if st.stake_type(op["kind"]) != t or op.get("netuid") != netuid:
                    continue
                if st._op_address(ext, op) == want:
                    return str(ext.get("address") or "")
        return ""

    # Short labels for WebSocket `type` / `method` (UI + API consumers).
    _WRAPPER_UI_SUFFIX = {
        "mev_shield": "Shield",
        "evm": "EVM",
        "proxy": "Proxy",
        "batch": "Batch",
    }

    @classmethod
    def _kind_to_ui_method_core(cls, kind: str) -> str:
        """Map internal stake `kind` to a short professional method label."""
        if not kind:
            return "—"
        if kind == "mev_shield":
            return "Shield"
        if kind.startswith("EVM."):
            rest = kind[4:]
            rl = rest.lower()
            if "addstakelimit" in rl or "addstake_v1" in rl:
                return "EVM·Add·L"
            if "addstake" in rl:
                return "EVM·Add"
            if "removestakefull" in rl:
                return "EVM·Rem·F"
            if "removestake" in rl:
                return "EVM·Rem"
            if "movestake" in rl:
                return "EVM·Move"
            if "transferstake" in rl:
                return "EVM·Trans"
            if "wrap" in rl or "_c" in rl:
                return "EVM·" + rest.replace("_", "")[:6].title()
            return "EVM·" + rest[:8]
        if kind.startswith("StakeMoved"):
            return "Move"
        if kind == "add_stake_limit":
            return "Add·Lim"
        if kind == "add_stake":
            return "Add"
        if kind in (
            "remove_stake",
            "remove_stake_limit",
            "remove_stake_full_limit",
            "unstake_all",
        ):
            if kind == "remove_stake_full_limit":
                return "Rem·F"
            if kind == "remove_stake_limit":
                return "Rem·L"
            if kind == "unstake_all":
                return "Rem·A"
            return "Remove"
        if kind == "move_stake":
            return "Move"
        if kind == "swap_stake_limit":
            return "Swap"
        if kind == "transfer_stake":
            return "Xfer"
        if kind == "StakeAdded":
            return "Add"
        if kind == "StakeRemoved":
            return "Remove"
        if kind.endswith(":add"):
            head = kind.split(":")[0]
            if "move" in head.lower():
                return "Move"
            if "transfer" in head.lower():
                return "Xfer"
            return "Add"
        if kind.endswith(":remove"):
            head = kind.split(":")[0]
            if "move" in head.lower() or "transfer" in head.lower():
                return "Move"
            return "Remove"
        tail = kind.split(".")[-1]
        if len(tail) <= 14:
            return tail.replace("_", " ").title()
        return tail[:14]

    @classmethod
    def _ui_mempool_method(cls, merged: dict) -> str:
        """Short professional `method` for mempool rows."""
        kind = merged.get("kind") or ""
        w = merged.get("wrapper") or "direct"
        core = cls._kind_to_ui_method_core(kind)
        suf = cls._WRAPPER_UI_SUFFIX.get(w)
        if kind == "mev_shield":
            return "Shield"
        if suf and w != "direct":
            return f"{core}·{suf}"[:20]
        return core[:20]

    @staticmethod
    def _ui_mempool_type(merged: dict) -> str:
        """Short professional `type` for mempool rows."""
        import stake_tracker as st

        st_t = st.stake_type(merged["kind"])
        if st_t == "MEV" or merged.get("kind") == "mev_shield":
            return "MEV"
        if st_t == "REMOVE":
            return "Unstake"
        return "Stake"

    @staticmethod
    def _ui_last_block_method(raw: str, max_width: int = 18) -> str:
        """Normalize last-block method: short UI labels; legacy long strings use _method_label."""
        m = (raw or "").strip()
        if not m:
            return "—"
        legacy = (
            "StakeAdded",
            "StakeRemoved",
            "add_stake",
            "remove_",
            "mev_shield",
            "move_stake",
            "transfer_stake",
            "swap_stake",
            "EVM.",
            ">",
            "BATCH",
            "PROXY",
        )
        if any(x in m for x in legacy) or ("_" in m and len(m) > 4):
            return MempoolMonitor._method_label(m, max_width)
        return m[:max_width]

    # Module indices that can contain stake operations (from runtime metadata).
    # SubtensorModule(7): direct stake calls;  Utility(11): batch calls;
    # Proxy(16): delegated calls;  Ethereum(21): EVM wrapping;  MevShield(30): encrypted wrapping.
    _INTERESTING_MODULES = frozenset({7, 11, 16, 21, 30})

    # SubtensorModule stake-related call indices
    _STAKE_CALL_INDICES = frozenset({
        2, 3, 83, 84, 85, 86, 87, 88, 89, 90, 103, 114, 132,
    })

    @staticmethod
    def _read_compact(data, off):
        first = data[off]
        mode = first & 0x03
        if mode == 0:
            return first >> 2, off + 1
        elif mode == 1:
            return int.from_bytes(data[off:off+2], 'little') >> 2, off + 2
        elif mode == 2:
            return int.from_bytes(data[off:off+4], 'little') >> 2, off + 4
        else:
            nb = (first >> 2) + 4
            return int.from_bytes(data[off+1:off+1+nb], 'little'), off + 1 + nb

    def _fast_is_interesting(self, ext_hex):
        """Extract (module_index, call_index) from raw SCALE bytes in ~0ms.

        Returns True only if the extrinsic might be a stake/MEV/batch call,
        allowing us to skip the expensive decode_scale() for irrelevant txs.
        """
        try:
            raw = bytes.fromhex(ext_hex[2:] if ext_hex.startswith('0x') else ext_hex)
            _, off = self._read_compact(raw, 0)          # length prefix
            version = raw[off]; off += 1
            if not (version & 0x80):
                return False                              # unsigned — skip
            addr_type = raw[off]; off += 1
            if addr_type in (0x00, 0xff):
                off += 32                                 # AccountId (32 bytes)
            else:
                return True                               # unknown format — be safe, decode it
            sig_type = raw[off]; off += 1
            if sig_type in (0x00, 0x01):
                off += 64                                 # Ed25519/Sr25519
            elif sig_type == 0x02:
                off += 65                                 # Ecdsa
            else:
                return True
            if raw[off] == 0x00:
                off += 1                                  # Immortal era
            else:
                off += 2                                  # Mortal era
            _, off = self._read_compact(raw, off)         # nonce
            _, off = self._read_compact(raw, off)         # tip
            off += 1                                      # CheckMetadataHash mode byte
            mod_idx = raw[off]
            if mod_idx not in self._INTERESTING_MODULES:
                return False
            # For SubtensorModule, also check call index to skip set_weights etc.
            if mod_idx == 7:
                call_idx = raw[off + 1]
                return call_idx in self._STAKE_CALL_INDICES
            return True                                   # MevShield, Utility, Ethereum — always decode
        except Exception:
            return True                                   # on error, be safe — decode it

    # Call index → function name mapping for SubtensorModule stake calls
    _CALL_IDX_TO_NAME = {
        2: 'add_stake', 3: 'remove_stake',
        83: 'unstake_all', 84: 'unstake_all_alpha',
        85: 'move_stake', 86: 'transfer_stake', 87: 'swap_stake',
        88: 'add_stake_limit', 89: 'remove_stake_limit',
        90: 'swap_stake_limit', 103: 'remove_stake_full_limit',
        114: 'set_coldkey_auto_stake_hotkey', 132: 'add_stake_burn',
    }


    
    def _get_subnet_price(self, netuid, substrate=None):
        """Get subnet price (alpha per TAO) for dynamic subnets.

        Cached with TTL; stale entries are refreshed automatically.
        """
        if netuid is None:
            return None

        # Prefer the warm price table (``_tracker_prices``) that the dedicated
        # block thread refreshes every ~12 s. ``subnet.price`` IS the AMM spot
        # ``tao_in / alpha_in``, so ``alpha_per_tao = alpha_in / tao_in`` here is
        # the same value — but read from memory instead of a slow
        # ``subtensor.subnet()`` RPC on the main snapshot loop. This removes the
        # recurring per-12s main-loop stall that delayed snapshot (and thus
        # block-number) pushes.
        key = netuid[0] if isinstance(netuid, tuple) else netuid
        tp = self._tracker_prices.get(key) if self._tracker_prices else None
        if tp:
            try:
                ai = float(tp.get("alpha_in") or 0)
                ti = float(tp.get("tao_in") or 0)
                if ai > 0 and ti > 0:
                    return ai / ti
            except (TypeError, ValueError):
                pass

        now = time.monotonic()
        cached = self.subnet_price_cache.get(netuid)
        if cached is not None:
            if now - cached['time'] < self._price_cache_ttl:
                return cached['price']

        price_alpha_per_tao = self._fetch_subnet_price(netuid, substrate=substrate)
        if price_alpha_per_tao is not None:
            self.subnet_price_cache[netuid] = {'price': price_alpha_per_tao, 'time': now}
            # Expose the fresh spot to other threads (e.g. Trader) so they can
            # skip their own subtensor.subnet(…) RPC on the hot trade path.
            try:
                price_cache.put(netuid, price_alpha_per_tao)
            except Exception:
                pass
        elif cached is not None and cached['price'] is not None:
            cached['time'] = now
        else:
            self.subnet_price_cache[netuid] = {
                'price': None,
                'time': now - self._price_cache_ttl + 2,
            }
        return price_alpha_per_tao if price_alpha_per_tao is not None else (cached['price'] if cached else None)

    def _fetch_subnet_price(self, netuid, substrate=None):
        """Query on-chain price for a subnet. Returns alpha_per_tao or None.

        When ``substrate`` is provided (dedicated block-processing thread), the
        query is issued on that connection and the shared ``self.subtensor``
        (bittensor SDK, not thread-safe) is skipped to avoid cross-thread WS
        corruption.
        """
        if substrate is None and self.subtensor is not None:
            try:
                subnet_info = self.subtensor.subnet(netuid=netuid)
                if subnet_info and hasattr(subnet_info, 'price') and subnet_info.price:
                    price_tao_per_alpha = float(subnet_info.price.tao)
                    if price_tao_per_alpha > 0:
                        return 1.0 / price_tao_per_alpha
            except Exception:
                pass

        try:
            result = (substrate or self.substrate).query(
                module='SubtensorModule',
                storage_function='SubnetInfo',
                params=[netuid]
            )
            if result and hasattr(result, 'value') and result.value:
                subnet_info = result.value
                price_value = None
                if isinstance(subnet_info, dict):
                    price_value = subnet_info.get('price') or subnet_info.get('Price')
                elif hasattr(subnet_info, 'price'):
                    price_value = subnet_info.price
                elif hasattr(subnet_info, 'Price'):
                    price_value = subnet_info.Price
                if price_value is not None:
                    price_tao_per_alpha = self._parse_price_value(price_value)
                    if price_tao_per_alpha is not None and price_tao_per_alpha > 0:
                        return 1.0 / price_tao_per_alpha
        except Exception:
            pass
        return None

    @staticmethod
    def _parse_price_value(price_value):
        """Extract tao_per_alpha float from various on-chain formats."""
        if isinstance(price_value, dict):
            tao_value = (price_value.get('tao') or price_value.get('value') or
                         price_value.get('amount') or price_value.get('Value'))
            if tao_value is not None:
                try:
                    tao_int = int(tao_value)
                    return float(tao_int) / 1e9 if tao_int > 1e12 else float(tao_int)
                except (ValueError, TypeError):
                    pass
        elif isinstance(price_value, (int, float, str)):
            try:
                pf = float(price_value)
                return pf / 1e9 if pf > 1e12 else pf
            except (ValueError, TypeError):
                pass
        elif hasattr(price_value, 'tao'):
            try:
                return float(price_value.tao)
            except (ValueError, TypeError, AttributeError):
                pass
        return None

    def _free_tao_cached(self, address, scratch):
        """Cached free-balance lookup with TTL-based background refresh.

        Always returns the last known value immediately (never blanks the cell);
        queues a background ``System.Account`` refresh only when the cached
        entry is older than ``_balance_refresh_sec`` or absent. This replaces
        the previous per-block mass invalidation so busy blocks no longer
        trigger an RPC storm on the shared endpoint.
        """
        if not address:
            return None
        if address in scratch:
            return scratch[address]
        cached = self.free_balance_cache.get(address)
        if cached:
            scratch[address] = cached.get('balance')
            ts = cached.get('time') or 0.0
            if (time.time() - ts) >= self._balance_refresh_sec:
                self._bg_fetch_balance_queue.add(address)
            return scratch[address]
        self._bg_fetch_balance_queue.add(address)
        return None

    @staticmethod
    def _decode_stake_hotkey(raw) -> str | None:
        """Normalize hotkey from StakeInfoRuntimeApi entries to SS58."""
        if raw is None:
            return None
        if isinstance(raw, str):
            return raw
        try:
            from substrateinterface.utils.ss58 import ss58_encode

            if isinstance(raw, (tuple, list)) and raw and isinstance(raw[0], (tuple, list)):
                return ss58_encode(bytes(raw[0]), ss58_format=42)
            return ss58_encode(bytes(raw), ss58_format=42)
        except Exception:
            return None

    def _stake_cache_key(self, coldkey, hotkey, netuid):
        if coldkey is None or hotkey is None or netuid is None:
            return None
        try:
            from stake_tracker import _normalize_ss58

            ck = _normalize_ss58(coldkey) or str(coldkey)
            hk = _normalize_ss58(hotkey) or str(hotkey)
        except Exception:
            ck, hk = str(coldkey), str(hotkey)
        return (ck, hk, int(netuid))

    def _stake_rpc_substrate(self, substrate=None):
        """Substrate connection for StakeInfoRuntimeApi (needs bittensor type registry)."""
        if substrate is not None:
            return substrate
        if self.subtensor is not None and getattr(self.subtensor, "substrate", None) is not None:
            return self.subtensor.substrate
        return self.substrate

    def _stake_entries_for_coldkey(self, coldkey, substrate=None):
        """Cached ``get_stake_info_for_coldkey`` rows for one snapshot pass."""
        ck = str(coldkey)
        if ck in self._runtime_stake_entries_scratch:
            return self._runtime_stake_entries_scratch[ck]
        use_sub = self._stake_rpc_substrate(substrate)
        entries = []
        try:
            if hasattr(use_sub, "runtime_call"):
                result = use_sub.runtime_call(
                    "StakeInfoRuntimeApi",
                    "get_stake_info_for_coldkey",
                    [coldkey],
                )
                entries = result.value if hasattr(result, "value") else result
                entries = list(entries or [])
        except Exception:
            entries = []
        self._runtime_stake_entries_scratch[ck] = entries
        return entries

    def _query_stake_tao_for_position(
        self,
        coldkey,
        hotkey,
        netuid,
        substrate=None,
        subtensor=None,
    ):
        """Return TAO-equivalent stake for (coldkey, validator hotkey, netuid).

        ``remove_stake_full_limit`` extrinsics carry no amount; this uses
        ``StakeInfoRuntimeApi.get_stake_info_for_coldkey`` (same as the wallet
        panel) rather than the legacy ``SubtensorModule.Stake`` map keyed only
        by hotkey — which misses most dynamic-TAO positions.
        """
        cache_key = self._stake_cache_key(coldkey, hotkey, netuid)
        if cache_key is None:
            return None
        if cache_key in self.stake_cache:
            return self.stake_cache[cache_key]

        use_sub = self._stake_rpc_substrate(substrate)
        use_bt = subtensor if subtensor is not None else self.subtensor
        want_ck, want_hk, nid = cache_key[0], cache_key[1], cache_key[2]

        try:
            for entry in self._stake_entries_for_coldkey(want_ck, substrate=use_sub):
                try:
                    en = int(entry.get("netuid", -1))
                except (TypeError, ValueError):
                    continue
                if en != nid:
                    continue
                entry_hk = self._decode_stake_hotkey(entry.get("hotkey"))
                if not entry_hk or entry_hk != want_hk:
                    continue
                stake_rao = int(entry.get("stake", 0) or 0)
                if stake_rao <= 0:
                    self.stake_cache[cache_key] = 0.0
                    return 0.0
                tao = self._convert_alpha_to_tao(
                    stake_rao, nid, substrate=use_sub
                )
                if tao is not None and tao > 0:
                    val = float(tao)
                    self.stake_cache[cache_key] = val
                    return val
        except Exception:
            pass

        if use_bt is not None:
            try:
                infos = None
                if hasattr(use_bt, "get_stake_info_for_coldkey"):
                    infos = use_bt.get_stake_info_for_coldkey(coldkey_ss58=want_ck)
                elif hasattr(use_bt, "get_stake_for_coldkey"):
                    infos = use_bt.get_stake_for_coldkey(want_ck)
                if infos:
                    for si in infos:
                        info_netuid = getattr(si, "netuid", None)
                        if info_netuid is not None and int(info_netuid) != nid:
                            continue
                        info_hk_ss58 = getattr(si, "hotkey_ss58", None)
                        if not info_hk_ss58:
                            info_hk = getattr(si, "hotkey", None)
                            info_hk_ss58 = (
                                info_hk.ss58_address
                                if hasattr(info_hk, "ss58_address")
                                else str(info_hk or "")
                            )
                        if not info_hk_ss58 or str(info_hk_ss58) != want_hk:
                            continue
                        stake = getattr(si, "stake", None)
                        if not stake:
                            continue
                        try:
                            val = None
                            subnet = (
                                use_bt.subnet(netuid=nid)
                                if hasattr(use_bt, "subnet")
                                else None
                            )
                            if subnet and hasattr(subnet, "alpha_to_tao"):
                                try:
                                    converted = subnet.alpha_to_tao(stake)
                                    val = float(getattr(converted, "tao", converted))
                                except Exception:
                                    pass
                            if val is None:
                                val = float(
                                    getattr(stake, "tao", stake)
                                    if hasattr(stake, "tao")
                                    else stake
                                )
                            self.stake_cache[cache_key] = val
                            return val
                        except (TypeError, ValueError):
                            pass
            except Exception:
                pass

        return None

    def _get_stake_cached(self, coldkey, hotkey, netuid):
        """Cache-only stake lookup — queues miss for background fetch."""
        cache_key = self._stake_cache_key(coldkey, hotkey, netuid)
        if cache_key is None:
            return None
        if cache_key in self.stake_cache:
            return self.stake_cache[cache_key]
        self._bg_fetch_stake_queue.add(cache_key)
        return None

    def process_bg_fetch_queue(self, max_items=3, substrate=None, subtensor=None):
        """Process a limited batch of queued RPC lookups.

        When substrate/subtensor are provided, uses those connections
        instead of self.substrate/self.subtensor (for dedicated bg thread).
        """
        use_sub = substrate or self.substrate
        use_bt = subtensor if subtensor is not None else self.subtensor

        # Balances are drained in a single ``query_multi`` round trip instead of
        # one ``System.Account`` call per address. A busy block re-queues every
        # transacting address; fetching them one at a time hammered the shared
        # public endpoint and contended with block processing. Batching collapses
        # up to ``_balance_batch_size`` addresses into one RPC, so accuracy is
        # kept (every queued address is still refreshed) at a fraction of the
        # round trips.
        if self._bg_fetch_balance_queue:
            batch = []
            while self._bg_fetch_balance_queue and len(batch) < self._balance_batch_size:
                batch.append(self._bg_fetch_balance_queue.pop())
            if batch:
                try:
                    self._bg_fetch_balances_batch(batch, use_sub)
                except Exception:
                    pass

        done = 0
        while self._bg_fetch_stake_queue and done < max_items:
            cache_key = self._bg_fetch_stake_queue.pop()
            if cache_key not in self.stake_cache:
                try:
                    self._query_stake_tao_for_position(
                        cache_key[0],
                        cache_key[1],
                        cache_key[2],
                        substrate=use_sub,
                        subtensor=use_bt,
                    )
                except Exception:
                    pass
                done += 1

    def _bg_fetch_balance(self, address, substrate):
        """Fetch balance using provided substrate connection, update shared cache."""
        if not address:
            return
        try:
            result = substrate.query(
                module='System',
                storage_function='Account',
                params=[address]
            )
            balance_tao = None
            if result and getattr(result, 'value', None):
                data = result.value
                free_rao = 0
                if isinstance(data, dict):
                    if 'data' in data and isinstance(data['data'], dict):
                        free_rao = int(data['data'].get('free', 0) or 0)
                    elif 'free' in data:
                        free_rao = int(data.get('free', 0) or 0)
                balance_tao = free_rao / 1e9
            self.free_balance_cache[address] = {
                'balance': balance_tao,
                'time': time.time()
            }
        except Exception:
            pass

    @staticmethod
    def _free_tao_from_account_value(data):
        """Extract free balance (TAO) from a decoded System.Account value."""
        if not isinstance(data, dict):
            return None
        free_rao = 0
        if 'data' in data and isinstance(data['data'], dict):
            free_rao = int(data['data'].get('free', 0) or 0)
        elif 'free' in data:
            free_rao = int(data.get('free', 0) or 0)
        return free_rao / 1e9

    def _bg_fetch_balances_batch(self, addresses, substrate):
        """Fetch many free balances in ONE ``query_multi`` round trip.

        Builds a ``System.Account`` storage key per address and resolves them
        all in a single RPC call, then updates the shared cache. Falls back to
        per-address queries if batching is unavailable or fails, so behaviour is
        never worse than the original one-at-a-time path.
        """
        if not addresses:
            return
        storage_keys = []
        valid = []
        for addr in addresses:
            if not addr:
                continue
            try:
                sk = substrate.create_storage_key('System', 'Account', [addr])
            except Exception:
                continue
            storage_keys.append(sk)
            valid.append(addr)
        if not storage_keys:
            return
        try:
            results = substrate.query_multi(storage_keys)
        except Exception:
            # Batch path unavailable/failed — degrade to per-address fetches.
            for addr in valid:
                try:
                    self._bg_fetch_balance(addr, substrate)
                except Exception:
                    pass
            return
        now = time.time()
        for i, item in enumerate(results):
            try:
                value_obj = item[1]
            except (TypeError, IndexError, KeyError):
                value_obj = item
            addr = valid[i] if i < len(valid) else None
            if addr is None:
                continue
            balance_tao = self._free_tao_from_account_value(getattr(value_obj, 'value', None))
            self.free_balance_cache[addr] = {'balance': balance_tao, 'time': now}

    @staticmethod
    def _is_full_unstake_kind(kind: str | None) -> bool:
        k = (kind or "").lower()
        return "unstake_all" in k or "remove_stake_full" in k

    def _mempool_sell_needs_price_refresh(self, merged, prices) -> bool:
        """True when a partial sell has alpha RAO but TAO conversion failed."""
        import stake_tracker as st

        if st.stake_type(merged.get("kind")) != "REMOVE":
            return False
        if self._is_full_unstake_kind(merged.get("kind")):
            return False
        amt = self._tracker_merged_op_amount_tao(merged, prices)
        if amt is not None and amt > 0:
            return False
        v = merged.get("amount_rao") or merged.get("alpha_amount")
        return bool(v)

    def _maybe_refresh_tracker_prices(self, substrate=None, *, force: bool = False) -> None:
        """Refresh subnet price table for mempool sell amount (alpha→TAO) conversion."""
        now = time.monotonic()
        if (
            not force
            and self._tracker_prices
            and (now - self._last_mempool_prices_fetch) < self._price_refresh_sec
        ):
            return
        sub = substrate or self.substrate
        try:
            p = st_bridge.fetch_prices_sync(sub)
            if p:
                self._tracker_prices = p
            self._last_mempool_prices_fetch = now
        except Exception:
            pass


    def _is_dynamic_subnet(self, netuid):
        """Check if a subnet is a dynamic subnet (uses alpha tokens)
        
        Args:
            netuid: Subnet ID
            
        Returns:
            bool: True if dynamic subnet, False otherwise
        """
        if netuid is None:
            return False
        
        # Check cache first
        if netuid in self.subnet_type_cache:
            return self.subnet_type_cache[netuid]
        
        # Try to get price - if we get a price, it's a dynamic subnet
        price = self._get_subnet_price(netuid)
        is_dynamic = price is not None and price > 0
        
        self.subnet_type_cache[netuid] = is_dynamic
        return is_dynamic
    
    def _convert_alpha_to_tao(self, alpha_amount_rao, netuid, substrate=None):
        """Convert alpha token amount (in RAO) to TAO using latest subnet price."""
        if alpha_amount_rao is None or alpha_amount_rao <= 0:
            return None

        if netuid is None:
            return None

        price_alpha_per_tao = self._get_subnet_price(netuid, substrate=substrate)
        alpha_tokens = alpha_amount_rao / 1e9
        if price_alpha_per_tao is None or price_alpha_per_tao <= 0:
            return None
        return alpha_tokens / price_alpha_per_tao

    # Hard limits that filter out sentinel / junk `limit_price` values.
    # A legitimate tao_per_alpha price never exceeds a few hundred TAO/alpha, so
    # anything ≥ 1e6 TAO/alpha (1e15 rao) is almost certainly a "no-limit"
    # sentinel like u64::MAX submitted by some wallet clients.
    _SLIPPAGE_MAX_LIMIT_PRICE_RAO = 10 ** 15
    _SLIPPAGE_MIN_SPOT_TAO = 1e-9
    _SLIPPAGE_DISPLAY_CAP_PCT = 999.0

    @staticmethod
    def _normalize_limit_price_rao(limit_price_rao):
        if limit_price_rao is None:
            return None
        if isinstance(limit_price_rao, dict):
            for k in ("value", "Value", "rao", "amount", "tao"):
                if k in limit_price_rao and limit_price_rao[k] is not None:
                    limit_price_rao = limit_price_rao[k]
                    break
        try:
            lp = int(limit_price_rao)
        except (TypeError, ValueError):
            return None
        if lp <= 0:
            return None
        return lp

    @staticmethod
    def _tao_per_alpha_from_pool_row(tp) -> float | None:
        if not tp:
            return None
        try:
            ai = float(tp.get("alpha_in") or 0)
            ti = float(tp.get("tao_in") or 0)
        except (TypeError, ValueError):
            return None
        if ai > 0 and ti > 0:
            return ti / ai
        return None

    def _tao_per_alpha_spot(self, netuid, substrate=None) -> float | None:
        """Current spot τ per α for ``netuid``."""
        if netuid is None:
            return None
        nid = int(netuid[0] if isinstance(netuid, tuple) else netuid)
        tp = self._tracker_prices.get(nid) if self._tracker_prices else None
        spot = self._tao_per_alpha_from_pool_row(tp)
        if spot is not None:
            return spot
        alpha_per_tao = self._get_subnet_price(nid, substrate=substrate)
        if not alpha_per_tao or alpha_per_tao <= 0:
            return None
        return 1.0 / alpha_per_tao

    def _capture_block_spot_table(self, substrate=None) -> None:
        """Snapshot τ/α per netuid from the warm price table (+ RPC fill)."""
        table: dict[int, float] = {}
        for nid, tp in (self._tracker_prices or {}).items():
            spot = self._tao_per_alpha_from_pool_row(tp)
            if spot is not None:
                table[int(nid)] = spot
        self._last_block_tao_per_alpha = table

    def _remember_mempool_spots(self, tx_hash: str | None, ops: list) -> None:
        """Freeze spot τ/α for netuids touched by a newly seen pending extrinsic."""
        if not tx_hash or not ops:
            return
        for op in ops:
            kind = str(op.get("kind") or "")
            if "swap_stake" in kind:
                origin = op.get("origin_netuid")
                if origin is None and op.get("netuid") is not None:
                    nu = op.get("netuid")
                    origin = nu[0] if isinstance(nu, tuple) else nu
                dest = op.get("destination_netuid")
                if dest is None and isinstance(op.get("netuid"), tuple):
                    dest = op.get("netuid")[1]
                for nid in (origin, dest):
                    if nid is None:
                        continue
                    key = (tx_hash, int(nid))
                    if key not in self._mempool_spot_at_seen:
                        spot = self._tao_per_alpha_spot(int(nid))
                        if spot is not None:
                            self._mempool_spot_at_seen[key] = spot
                continue
            nu = op.get("netuid")
            if nu is None or isinstance(nu, tuple):
                continue
            key = (tx_hash, int(nu))
            if key not in self._mempool_spot_at_seen:
                spot = self._tao_per_alpha_spot(int(nu))
                if spot is not None:
                    self._mempool_spot_at_seen[key] = spot

    def _mempool_spot_for(self, tx_hash: str | None, netuid) -> float | None:
        if not tx_hash or netuid is None:
            return None
        nid = int(netuid[0] if isinstance(netuid, tuple) else netuid)
        return self._mempool_spot_at_seen.get((tx_hash, nid))

    def _slippage_pct(
        self,
        netuid,
        limit_price_rao,
        *,
        kind: str | None = None,
        spot_tao_per_alpha: float | None = None,
        spot_origin: float | None = None,
        spot_dest: float | None = None,
    ):
        """User-declared slippage tolerance for *_stake_limit extrinsics.

        Uses spot frozen at mempool first-seen (or block snapshot for Last block)
        rather than the live spot, which drifts while txs sit in the pool.

        ``swap_stake_limit`` encodes a cross-subnet α-price ratio, not τ/α on one
        netuid — those rows use a separate ratio formula.
        """
        lp_rao = self._normalize_limit_price_rao(limit_price_rao)
        if lp_rao is None or netuid is None:
            return None
        if lp_rao >= self._SLIPPAGE_MAX_LIMIT_PRICE_RAO:
            return None

        kind_s = (kind or "").lower()

        if "swap_stake" in kind_s:
            origin = None
            dest = None
            if isinstance(netuid, tuple) and len(netuid) == 2:
                origin, dest = netuid
            else:
                origin = netuid
            if origin is None or dest is None:
                return None
            o_spot = spot_origin or self._last_block_tao_per_alpha.get(int(origin))
            d_spot = spot_dest or self._last_block_tao_per_alpha.get(int(dest))
            if o_spot is None:
                o_spot = self._tao_per_alpha_spot(int(origin))
            if d_spot is None:
                d_spot = self._tao_per_alpha_spot(int(dest))
            if not o_spot or not d_spot:
                return None
            current_ratio = o_spot / d_spot
            limit_ratio = lp_rao / 1e9
            if current_ratio <= 0 or limit_ratio <= 0:
                return None
            try:
                pct = (1.0 - limit_ratio / current_ratio) * 100.0
            except (ZeroDivisionError, TypeError):
                return None
            if not (pct == pct) or pct < 0 or pct > self._SLIPPAGE_DISPLAY_CAP_PCT:
                return None
            return pct

        nid = int(netuid[0] if isinstance(netuid, tuple) else netuid)
        spot = spot_tao_per_alpha
        if spot is None:
            spot = self._last_block_tao_per_alpha.get(nid)
        if spot is None:
            spot = self._tao_per_alpha_spot(nid)
        if not spot or spot < self._SLIPPAGE_MIN_SPOT_TAO:
            return None

        limit_tao_per_alpha = lp_rao / 1e9
        import stake_tracker as st

        st_t = st.stake_type(kind) if kind else None
        try:
            if st_t == "REMOVE" or kind_s.endswith(":remove"):
                pct = (1.0 - limit_tao_per_alpha / spot) * 100.0
            elif st_t == "ADD" or kind_s.endswith(":add"):
                pct = (limit_tao_per_alpha / spot - 1.0) * 100.0
            else:
                pct = abs(limit_tao_per_alpha - spot) / spot * 100.0
        except (ZeroDivisionError, TypeError):
            return None
        if not (pct == pct) or pct < 0 or pct > self._SLIPPAGE_DISPLAY_CAP_PCT:
            return None
        return pct

    def _mempool_slippage_pct(self, merged: dict) -> float | None:
        tx_hash = merged.get("_tx_hash")
        netuid = merged.get("netuid")
        kind = str(merged.get("kind") or "")
        lp = merged.get("limit_price")

        if "swap_stake" in kind:
            origin = merged.get("origin_netuid")
            dest = merged.get("destination_netuid")
            if origin is None or dest is None:
                if isinstance(netuid, tuple) and len(netuid) == 2:
                    origin, dest = netuid
                elif netuid is not None and not isinstance(netuid, tuple):
                    if kind.endswith(":remove"):
                        origin = netuid
                    elif kind.endswith(":add"):
                        dest = netuid
            if origin is not None and dest is not None:
                o_spot = self._mempool_spot_for(tx_hash, origin)
                d_spot = self._mempool_spot_for(tx_hash, dest)
                return self._slippage_pct(
                    (int(origin), int(dest)),
                    lp,
                    kind=kind,
                    spot_origin=o_spot,
                    spot_dest=d_spot,
                )

        spot = self._mempool_spot_for(tx_hash, netuid)
        return self._slippage_pct(
            netuid,
            lp,
            kind=kind or None,
            spot_tao_per_alpha=spot,
        )

    @staticmethod
    def _last_block_slippage_kind(tx_type: str | None, method: str | None) -> str | None:
        m = (method or "").lower()
        if "swap" in m:
            return "swap_stake_limit:remove"
        if (tx_type or "").strip().lower() == "unstake":
            return "remove_stake_limit"
        if (tx_type or "").strip().lower() == "stake":
            return "add_stake_limit"
        return None

    def _last_block_slippage_pct(self, merged_tx: dict) -> float | None:
        netuid = merged_tx.get("netuid")
        lp = merged_tx.get("limit_price")
        tx_type = merged_tx.get("type")
        method = merged_tx.get("method")
        kind = self._last_block_slippage_kind(tx_type, method)
        if isinstance(netuid, tuple) and len(netuid) == 2 and "swap" in (method or "").lower():
            o, d = int(netuid[0]), int(netuid[1])
            return self._slippage_pct(
                netuid,
                lp,
                kind=kind,
                spot_origin=self._last_block_tao_per_alpha.get(o),
                spot_dest=self._last_block_tao_per_alpha.get(d),
            )
        if netuid is None:
            return None
        nid = int(netuid[0] if isinstance(netuid, tuple) else netuid)
        return self._slippage_pct(
            netuid,
            lp,
            kind=kind,
            spot_tao_per_alpha=self._last_block_tao_per_alpha.get(nid),
        )

    def _drop_mempool_spots_for_ext(self, ext_hex: str) -> None:
        ext = self._mempool_decoded.get(ext_hex) or {}
        tx_hash = str(ext.get("tx_hash") or "")
        if not tx_hash:
            return
        for key in list(self._mempool_spot_at_seen):
            if key[0] == tx_hash:
                del self._mempool_spot_at_seen[key]

    
    def get_pending_extrinsics(self):
        """Get all pending extrinsics from mempool.

        Returns an empty list on any failure or malformed response. The caller
        (``poll_mempool``) depends on this being a **list** — previously an
        RPC success with ``result['result'] is None`` (rare but observed when
        the WS layer is mid-handshake) leaked a ``None`` out and caused
        ``set(None)`` → TypeError in the poll, which in turn froze the UI
        mempool at the pre-error snapshot (cannot-broadcast-because-no-fp-
        change loop).
        """
        try:
            result = self.substrate.rpc_request(
                method="author_pendingExtrinsics",
                params=[]
            )
        except Exception:
            return []
        if not isinstance(result, dict):
            return []
        pending = result.get('result')
        if not isinstance(pending, list):
            return []
        return pending
    
    def _fast_extract_module_idx(self, ext_hex):
        """Return the top-level module index for a signed extrinsic, or None.

        Mirrors the parsing path in `_fast_is_interesting` but returns the raw
        module index instead of a boolean. Unsigned / malformed extrinsics → None.
        """
        try:
            raw = bytes.fromhex(ext_hex[2:] if ext_hex.startswith('0x') else ext_hex)
            _, off = self._read_compact(raw, 0)
            version = raw[off]; off += 1
            if not (version & 0x80):
                return None
            addr_type = raw[off]; off += 1
            if addr_type in (0x00, 0xff):
                off += 32
            else:
                return None
            sig_type = raw[off]; off += 1
            if sig_type in (0x00, 0x01):
                off += 64
            elif sig_type == 0x02:
                off += 65
            else:
                return None
            if raw[off] == 0x00:
                off += 1
            else:
                off += 2
            _, off = self._read_compact(raw, off)
            _, off = self._read_compact(raw, off)
            off += 1
            return raw[off]
        except Exception:
            return None

    def _orphan_failed_rows(self, block_hash, fail_set, agg_ext_idx_set, substrate=None):
        """Emit synthetic LastBlockRow entries for failed extrinsics with no events.

        Substrate extrinsics are atomic: when a stake-related extrinsic fails
        (slippage on `add_stake_limit`, insufficient balance on `add_stake`,
        encrypted MevShield inner-call revert, etc.), the chain emits only
        `ExtrinsicFailed` — no `StakeAdded`/`StakeRemoved`/`StakeMoved` events.
        The event-based pipeline therefore produces no row and the failure is
        invisible in the UI.

        For each ExtrinsicFailed index that has no matching aggregate event, we
        fetch the block body, fast-filter on module index (skip non-stake txs),
        and decode the extrinsic to recover signer + call args. Call args yield
        netuid / amount for direct stake calls; MevShield stays amount=netuid=
        None (payload is encrypted). One row per failed extrinsic — failure is
        atomic so we never emit per-leg rows.
        """
        orphans = sorted(i for i in fail_set if i not in agg_ext_idx_set)
        if not orphans:
            return []
        sub = substrate or self.substrate
        try:
            resp = sub.rpc_request("chain_getBlock", [block_hash])
        except Exception as exc:
            logger.debug("chain_getBlock failed for %s: %s", block_hash, exc)
            return []
        exts = (((resp or {}).get('result') or {}).get('block') or {}).get('extrinsics') or []

        import stake_tracker as st
        prices = self._tracker_prices or {}
        rows: list[dict] = []
        for idx in orphans:
            if idx < 0 or idx >= len(exts):
                continue
            ext_hex = exts[idx]
            if not self._fast_is_interesting(ext_hex):
                continue
            mod_idx = self._fast_extract_module_idx(ext_hex)
            try:
                decoded = self._extrinsic_hex_to_tracker_ext(ext_hex, substrate=sub)
            except Exception as exc:
                logger.debug("decode_scale failed idx=%d: %s", idx, exc)
                decoded = None
            if not decoded:
                # MevShield with decode failure: still emit a bare "MEV·Shield" row
                if mod_idx == 30:
                    rows.append(self._synthetic_failed_row(idx, "MEV", "Shield", ""))
                continue
            address = str(decoded.get("address") or "")
            try:
                ops = st.extract_stake_ops(decoded)
            except Exception as exc:
                logger.debug("extract_stake_ops failed idx=%d: %s", idx, exc)
                ops = []
            if not ops:
                if mod_idx == 30:
                    rows.append(self._synthetic_failed_row(idx, "MEV", "Shield", address))
                continue

            # Atomic failure: pick the first op (move/transfer return the :remove leg first).
            op = ops[0]
            kind = str(op.get("kind") or "")
            st_t = st.stake_type(kind)

            if st_t == "MEV" or kind == "mev_shield":
                row_type, method = "MEV", "Shield"
            elif kind.startswith(("move_stake", "transfer_stake", "swap_stake")):
                row_type = "Unstake"
                method = ("Move" if kind.startswith("move_stake")
                          else "Xfer" if kind.startswith("transfer_stake")
                          else "Swap")
            elif st_t == "REMOVE":
                row_type = "Unstake"
                method = self._kind_to_ui_method_core(kind)
            elif st_t == "EVM":
                row_type = "EVM"
                method = self._kind_to_ui_method_core(kind)
            else:
                row_type = "Stake"
                method = self._kind_to_ui_method_core(kind)

            if kind.startswith(("move_stake", "transfer_stake", "swap_stake")):
                origin = op.get("origin_netuid") or op.get("netuid")
                dest = op.get("destination_netuid")
                if origin is not None and dest is not None:
                    netuid = (origin, dest)
                else:
                    netuid = origin if origin is not None else dest
            else:
                netuid = op.get("netuid")

            try:
                v = self._tracker_merged_op_amount_tao(op, prices, substrate=sub)
                amt = float(v) if v is not None else None
            except Exception:
                amt = None
            if amt == 0.0:
                amt = None

            rows.append({
                "extrinsic_idx": idx,
                "type": row_type,
                "method": method,
                "address": address,
                "coldkey": address,
                "hotkey": str(op.get("hotkey") or address),
                "amount": amt,
                "netuid": netuid,
                "success": False,
                "limit_price": op.get("limit_price"),
            })
        if rows:
            logger.info("orphan failed rows: emitted %d for block", len(rows))
        return rows

    @staticmethod
    def _synthetic_failed_row(idx, row_type, method, address):
        return {
            "extrinsic_idx": idx,
            "type": row_type,
            "method": method,
            "address": address,
            "coldkey": address,
            "hotkey": address,
            "amount": None,
            "netuid": None,
            "success": False,
            "limit_price": None,
        }

    def parse_block_stake_transactions(self, block_number, block_hash=None, block=None, events=None, substrate=None):
        """Parse stake transactions: all StakeAdded/Removed/Moved events (no TAO amount filter)."""
        sub = substrate or self.substrate
        if block_hash is None:
            block_hash = sub.get_block_hash(block_number)
        if events is None:
            events = sub.get_events(block_hash)

        import stake_tracker as st

        raw = st_bridge.parse_stake_events_normalized(events)
        agg = st.aggregate_events(raw)
        succ_set, fail_set = st_bridge.extrinsic_success_sets(events)

        def _ext_ok(idx):
            if idx is None:
                return True
            if idx in fail_set:
                return False
            if idx in succ_set:
                return True
            return True

        rows = [self._tracker_event_to_last_block_row(ev, _ext_ok(ev.get("ext_idx")), substrate=substrate) for ev in agg]
        agg_ext_idx_set = {ev.get("ext_idx") for ev in agg if ev.get("ext_idx") is not None}
        rows.extend(self._orphan_failed_rows(block_hash, fail_set, agg_ext_idx_set, substrate=substrate))
        return rows
    
    def _netuid_to_json(self, netuid):
        if netuid is None:
            return None
        if isinstance(netuid, tuple):
            return [netuid[0], netuid[1]]
        return netuid
    
    @staticmethod
    def _method_label(method_full: str, max_width: int = 18) -> str:
        wrapper_prefix = ''
        lower_full = method_full.lower()
        if 'batch' in lower_full and 'proxy' in lower_full:
            wrapper_prefix = 'BATCH+PROXY.'
        elif 'batch' in lower_full:
            wrapper_prefix = 'BATCH.'
        elif 'proxy' in lower_full:
            wrapper_prefix = 'PROXY.'
        method_core = method_full.split('>')[-1]
        lower = method_core.lower()
        if method_core.startswith('StakeAdded') or method_core == 'StakeAdded':
            base = 'ADD'
        elif method_core.startswith('StakeRemoved') or method_core == 'StakeRemoved':
            base = 'REMOVE'
        elif 'stakemoved' in lower.replace('_', '') or 'StakeMoved' in method_core:
            base = 'MOVE'
        elif 'add_stake' in lower or lower == 'stake':
            base = 'ADD'
        elif 'remove_stake' in lower or 'unstake_all' in lower or 'unstake' in lower or 'remove' in lower:
            base = 'REMOVE'
        elif 'move_stake' in lower:
            base = 'MOVE'
        elif 'transfer_stake' in lower:
            base = 'TRANSFER'
        elif 'swap_stake' in lower or 'swap' in lower:
            base = 'SWAP'
        elif method_core.startswith('EVM.'):
            evm_part = method_core.split('.', 1)[1].lower()
            if 'stake' in evm_part:
                base = 'EVM.ADD'
            elif 'remove' in evm_part or 'unstake' in evm_part:
                base = 'EVM.REMOVE'
            else:
                base = 'EVM'
        else:
            parts = method_core.split('.')
            base = parts[-1]
        _short = {
            "ADD": "Add",
            "REMOVE": "Remove",
            "MOVE": "Move",
            "TRANSFER": "Xfer",
            "SWAP": "Swap",
            "EVM.ADD": "EVM·Add",
            "EVM.REMOVE": "EVM·Remove",
            "EVM": "EVM",
        }
        base = _short.get(base, base)
        return (wrapper_prefix + base)[:max_width]

    def build_last_block_ui_rows(
        self,
        block_data=None,
        free_balance_scratch=None,
        method_col_width: int = 18,
    ) -> list:
        """Build Last-block panel rows from parsed ``last_block_data``."""
        block_data = self.last_block_data if block_data is None else block_data
        if free_balance_scratch is None:
            free_balance_scratch = {}
        _get_bal = self._free_tao_cached
        last_block_rows: list = []
        if not block_data:
            return last_block_rows

        merged_txs = {}
        for tx in block_data:
            method = (tx.get('method') or '').lower()
            netuid = tx.get('netuid')
            is_move_stake = 'move_stake' in method or method == 'move'
            is_transfer_stake = 'transfer_stake' in method or method == 'xfer'
            if (is_move_stake or is_transfer_stake) and isinstance(netuid, tuple) and len(netuid) == 2:
                origin, dest = netuid
                if origin == dest:
                    continue
            ext_idx = tx['extrinsic_idx']
            tx_type = tx['type']
            tx_method = tx['method']
            tx_address = tx['address']
            key = (ext_idx, tx_type, tx_method, tx_address, netuid)
            if key not in merged_txs:
                merged_txs[key] = {
                    'extrinsic_idx': ext_idx,
                    'type': tx_type,
                    'method': tx_method,
                    'address': tx_address,
                    'coldkey': tx.get('coldkey'),
                    'netuid': netuid,
                    'amount': tx['amount'],
                    'success': tx.get('success', True),
                    'count': 0,
                    'limit_price': tx.get('limit_price'),
                }
            else:
                a = merged_txs[key]['amount']
                b = tx['amount']
                if a is None or b is None:
                    merged_txs[key]['amount'] = None
                else:
                    merged_txs[key]['amount'] = a + b
            merged_txs[key]['success'] = merged_txs[key]['success'] and tx.get('success', True)
            merged_txs[key]['count'] += 1

        merged_items = sorted(
            merged_txs.items(),
            key=lambda x: (x[1].get('extrinsic_idx') or 0, str(x[0])),
        )
        for _key, merged_tx in merged_items:
            method = (merged_tx.get('method') or '').lower()
            netuid = merged_tx.get('netuid')
            is_move_stake = 'move_stake' in method or method == 'move'
            is_transfer_stake = 'transfer_stake' in method or method == 'xfer'
            if (is_move_stake or is_transfer_stake) and isinstance(netuid, tuple) and len(netuid) == 2:
                origin, dest = netuid
                if origin == dest:
                    continue
            full_address = merged_tx['address']
            display_name = full_address
            netuid = merged_tx.get('netuid')
            if netuid is None:
                netuid_str = None
            elif isinstance(netuid, tuple) and len(netuid) == 2:
                netuid_str = f"{netuid[0]}→{netuid[1]}"
            else:
                netuid_str = str(netuid)
            amount = merged_tx.get('amount', 0.0)
            ext_idx = merged_tx['extrinsic_idx']
            method_label = self._ui_last_block_method(
                merged_tx.get('method', ''), method_col_width
            )
            stake_addr = merged_tx.get('coldkey') or full_address
            free_tao = _get_bal(stake_addr, free_balance_scratch)
            last_block_rows.append({
                'section': 'lastBlock',
                'extrinsicIdx': ext_idx,
                'type': merged_tx['type'],
                'method': method_label,
                'address': full_address,
                'addressLabel': display_name,
                'amount': None if (amount is None or amount == 0.0) else float(amount),
                'netuid': netuid_str,
                'netuidJson': self._netuid_to_json(netuid),
                'freeTao': free_tao,
                'success': merged_tx['success'],
                'limitPrice': merged_tx.get('limit_price'),
                'slippagePct': self._last_block_slippage_pct(merged_tx),
            })
        return last_block_rows

    def build_ui_snapshot(self, current_block):
        """Structured mempool + last-block rows for WebSocket JSON (cache-only + bg RPC fill)."""
        free_balance_scratch = {}
        method_col_width = 18
        mempool_rows = []
        last_block_rows = []
        _get_bal = self._free_tao_cached

        if self._mempool_stake:
            import stake_tracker as st

            agg = st.aggregate_ops(self._mempool_stake, self._mempool_pool, self._mempool_seen_at)

            prices = self._tracker_prices or {}
            if not prices or any(
                self._mempool_sell_needs_price_refresh(m, prices) for m in agg
            ):
                self._maybe_refresh_tracker_prices(force=not bool(prices))
                prices = self._tracker_prices or {}

            fp = frozenset(self._mempool_stake.keys())
            if fp != self._mempool_keys_fp:
                self._mempool_keys_fp = fp
                for merged in agg:
                    coldkey = merged.get("_address")
                    hotkey = merged.get("hotkey")
                    nu = merged.get("netuid")
                    if coldkey and hotkey and nu is not None:
                        if isinstance(nu, tuple) and len(nu) == 2:
                            for n in nu:
                                self.stake_cache.pop(
                                    self._stake_cache_key(coldkey, hotkey, n), None
                                )
                        else:
                            self.stake_cache.pop(
                                self._stake_cache_key(coldkey, hotkey, nu), None
                            )
            else:
                self._mempool_keys_fp = fp

            sync_stake_budget = self._mempool_stake_sync_max
            self._runtime_stake_entries_scratch.clear()

            for merged in agg:
                age = int(merged.get("_age_s") or 0)
                signer = self._resolve_signer_for_tracker_merged(merged)
                full_address = signer or merged.get("_address") or "Unknown"
                display_name = full_address
                amount = self._tracker_merged_op_amount_tao(merged, prices)
                netuid = merged.get("netuid")
                method_label = self._ui_mempool_method(merged)
                balance_address = full_address
                free_tao = _get_bal(balance_address, free_balance_scratch)
                tx_type = self._ui_mempool_type(merged)

                if netuid is None:
                    netuid_str = None
                elif isinstance(netuid, tuple) and len(netuid) == 2:
                    netuid_str = f"{netuid[0]}→{netuid[1]}"
                else:
                    netuid_str = str(netuid)
                netuid_for_balance = (
                    netuid[0] if isinstance(netuid, tuple) and len(netuid) == 2 else netuid
                )
                # Full-unstake (Rem·F) extrinsics carry no alpha amount in the call.
                # Look up stake by (coldkey, validator hotkey, netuid) via runtime API.
                coldkey = merged.get("_address") or full_address
                validator_hk = merged.get("hotkey")
                stake_tao = None
                if (
                    validator_hk
                    and netuid_for_balance is not None
                    and coldkey
                ):
                    stake_tao = self._get_stake_cached(
                        coldkey, validator_hk, netuid_for_balance
                    )
                    if (
                        (stake_tao is None or stake_tao <= 0)
                        and self._is_full_unstake_kind(merged.get("kind"))
                        and sync_stake_budget > 0
                    ):
                        stake_tao = self._query_stake_tao_for_position(
                            coldkey,
                            validator_hk,
                            netuid_for_balance,
                            subtensor=self.subtensor,
                        )
                        sync_stake_budget -= 1
                display_amount = amount
                if (display_amount is None or display_amount == 0.0) and stake_tao and stake_tao > 0:
                    if self._is_full_unstake_kind(merged.get("kind")):
                        display_amount = stake_tao
                # ``addressKey`` is the per-op identity used by
                # ``stake_tracker.aggregate_ops`` — i.e. ``proxy_real`` when a
                # Proxy.proxy wraps the call, otherwise the signer. The public
                # ``address`` field we expose to the UI was already resolved to
                # the tx signer by ``_resolve_signer_for_tracker_merged`` so the
                # Address column keeps displaying who actually submitted the
                # extrinsic (unchanged UX). The problem with only shipping the
                # signer was that a single ``Utility.batch([Proxy.proxy(real=A,
                # …), Proxy.proxy(real=B, …)])`` legitimately produces two
                # distinct merged rows (A and B), but both rows ended up with
                # the same ``(tx_hash, type, method, netuid, address=signer)``
                # tuple — which collapses to ONE React row key on the frontend.
                # React keeps the first ``<tr>`` and silently orphans the
                # duplicate, so the duplicate never gets unmounted when the tx
                # later drops from the pool. That orphan is what leaves stale
                # rows glued to the top of the mempool table even when the
                # panel is supposed to be empty (the "No pending…" empty-state
                # row then renders next to the ghost). Shipping the
                # ``addressKey`` alongside ``address`` gives the frontend a
                # stable, genuinely-unique disambiguator for the React key
                # without changing the displayed column.
                mempool_rows.append({
                    "section": "mempool",
                    "age": age,
                    "type": tx_type,
                    "method": method_label,
                    "address": full_address,
                    "addressKey": merged.get("_address") or full_address,
                    "addressLabel": display_name,
                    "amount": None if (display_amount is None or display_amount == 0.0) else float(display_amount),
                    "netuid": netuid_str,
                    "netuidJson": self._netuid_to_json(netuid),
                    "freeTao": free_tao,
                    "limitPrice": merged.get("limit_price"),
                    "slippagePct": self._mempool_slippage_pct(merged),
                    # Canonical extrinsic hash (blake2b-256 of SCALE bytes) —
                    # consumed by the UI as a stable React row key so sort /
                    # insert ops don't remount every row.
                    "txHash": merged.get("_tx_hash"),
                })
        else:
            self._mempool_keys_fp = frozenset()

        last_block_rows = self.build_last_block_ui_rows(
            self.last_block_data,
            free_balance_scratch,
            method_col_width,
        )
        
        transfer_rows = [
            r
            for r in list(self._transfer_history)
            if not self._transfer_event_excluded(
                r.get("from") or "",
                r.get("to") or "",
                r.get("amountTao"),
            )
        ]

        return {
            'currentBlock': current_block,
            'time': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'mempoolCount': len(self._mempool_stake),
            'mempool': mempool_rows,
            'lastBlockTxCount': len(self.last_block_data),
            'lastBlock': last_block_rows,
            'transferHistory': transfer_rows,
        }
    

    
    def _fetch_block_extrinsics(self, block_hash, substrate=None) -> list:
        """Return the list of hex-encoded extrinsics in ``block_hash``.

        Used by ``process_new_block`` to proactively evict confirmed txs from
        the mempool caches. Returns ``[]`` on any RPC / shape error so the
        caller can fall back to the slower implicit eviction path.
        """
        try:
            resp = (substrate or self.substrate).rpc_request("chain_getBlock", [block_hash])
        except Exception as exc:
            logger.debug("chain_getBlock(%s) failed: %s", block_hash, exc)
            return []
        if not isinstance(resp, dict):
            return []
        result = resp.get('result') or {}
        block = result.get('block') or {}
        exts = block.get('extrinsics') or []
        return exts if isinstance(exts, list) else []

    def process_new_block(self, block_number, substrate=None):
        """Process a new block: fetch events, parse stake transactions,
        and proactively purge confirmed txs from the mempool caches so they
        disappear from the UI within one block instead of waiting for the
        chain's txpool eviction timer (~30 s on Finney).

        ``substrate`` lets a dedicated block-processing thread pass its own
        ``SubstrateInterface`` so every RPC here runs on a connection separate
        from the main loop's mempool poller — block processing then no longer
        serializes behind ``poll_mempool`` / ``build_ui_snapshot``.
        """
        sub = substrate or self.substrate
        try:
            block_hash = sub.get_block_hash(block_number)
        except Exception as exc:
            logger.error("get_block_hash failed for block %s: %s", block_number, exc)
            return

        # Throttled full price-table refresh (see ``_price_refresh_sec``). The
        # heavy ``query_map`` pair only runs roughly once per block-time, not on
        # every processed block, which matters most during catch-up when many
        # blocks are processed back-to-back.
        now_mono = time.monotonic()
        if now_mono - self._last_price_fetch >= self._price_refresh_sec:
            try:
                p = st_bridge.fetch_prices_sync(sub)
                if p:
                    self._tracker_prices = p
                self._last_price_fetch = now_mono
            except Exception:
                pass
        self._capture_block_spot_table(substrate=sub)

        events: list | None = None
        last_err: Exception | None = None
        for attempt in range(2):
            try:
                events = sub.get_events(block_hash)
                break
            except Exception as exc:
                last_err = exc
                if attempt == 0:
                    time.sleep(0.05)
                    continue
                logger.warning(
                    "get_events failed block=%s hash=%s: %s",
                    block_number,
                    block_hash,
                    last_err,
                )
                events = []
                break
        if events is None:
            events = []

        # --- Proactive mempool prune on confirmation. ---------------------
        # The chain node's txpool can keep a confirmed tx in
        # ``author_pendingExtrinsics`` for tens of seconds (observed ~30 s on
        # Finney) after it has been included in a block. Without this step,
        # ``poll_mempool`` would keep re-admitting the tx and the UI would
        # show a phantom "pending" row long after the user has already seen
        # it in the Last block panel. We fetch the block body once here and
        # evict matching ``ext_hex`` from every mempool cache, then remember
        # the hashes in ``_confirmed_exts`` so the next poll cycle does not
        # re-insert them while the chain pool is still advertising them.
        # NOTE: this runs on the dedicated block-processing thread while the
        # main loop concurrently rebuilds the mempool dicts in ``poll_mempool``
        # and iterates them in ``build_ui_snapshot``. To stay thread-safe we do
        # NOT mutate the shared ``_mempool_pool`` / ``_mempool_stake`` /
        # ``_mempool_seen_at`` dicts here (mutating a dict another thread is
        # iterating raises ``RuntimeError``). Instead we only record the
        # confirmed extrinsic hashes in ``_confirmed_exts`` (set ``.add`` is
        # atomic under the GIL). ``poll_mempool`` already short-circuits any
        # hash in ``_confirmed_exts`` and rebuilds ``_mempool_pool`` /
        # ``_mempool_stake`` from scratch each cycle, so the confirmed tx drops
        # out of the UI on the very next poll (≤ a few hundred ms) — the same
        # visible result as the old in-place purge, without the cross-thread
        # mutation hazard.
        block_exts = self._fetch_block_extrinsics(block_hash, substrate=sub)
        if block_exts:
            newly_confirmed = 0
            for ext_hex in block_exts:
                if ext_hex not in self._confirmed_exts:
                    newly_confirmed += 1
                self._confirmed_exts.add(ext_hex)
            if newly_confirmed:
                # Invalidate the snapshot fingerprint so the next
                # ``build_ui_snapshot`` observes the shrunk mempool even if
                # nothing else changed between polls.
                self._mempool_keys_fp = None
                logger.info(
                    "mempool: %d confirmed tx(s) from block %s flagged for eviction",
                    newly_confirmed, block_number,
                )

        try:
            self.last_block_data = self.parse_block_stake_transactions(
                block_number, block_hash=block_hash, events=events, substrate=substrate
            )
        except Exception:
            logger.exception("parse_block_stake_transactions failed block=%s", block_number)
            self.last_block_data = []

        try:
            self.last_block_transfers = st_bridge.parse_balance_transfer_events_normalized(
                events
            )
        except Exception:
            logger.exception("parse_balance_transfer_events failed block=%s", block_number)
            self.last_block_transfers = []

        self._append_block_transfers(block_number)

        # Targeted balance refresh for addresses that actually transacted in
        # this block. Their on-chain free balance just changed, so the cached
        # value is now stale and ``freeTao`` would show the pre-trade amount.
        #
        # We deliberately do NOT use the old blanket ``free_balance_cache.pop``:
        # that both blanked the cell and forced a cache *miss* (hence a
        # ``System.Account`` RPC) for every transacting address every block.
        # Instead we mark only these entries stale (timestamp 0) so
        # ``_free_tao_cached`` re-queues exactly them for a background refresh on
        # the next snapshot, while still returning the last known value in the
        # meantime (no blank). Non-transacting addresses keep refreshing on the
        # gentle ``_balance_refresh_sec`` cadence. This restores post-trade
        # accuracy without reintroducing the per-block RPC storm (direction 2).
        affected_addrs = set()
        for tx in self.last_block_data:
            for key in ('address', 'coldkey', 'hotkey'):
                addr = tx.get(key)
                if addr:
                    affected_addrs.add(addr)
        for addr in affected_addrs:
            cached = self.free_balance_cache.get(addr)
            if cached:
                cached['time'] = 0.0
        self._last_seen_block = block_number

    def poll_mempool(self):
        """Mempool poll: decode pending extrinsics and extract all stake ops (no amount filter)."""
        import time as time_mod

        pending = self.get_pending_extrinsics()
        current_pending = set(pending)
        st_bridge.ensure_tracker_loaded()
        import stake_tracker as st

        now = time_mod.monotonic()
        seen_at = self._mempool_seen_at
        pool = {}
        stake = {}
        newly_seen: set[str] = set()

        for ext_hex in pending:
            if ext_hex in self._non_stake_exts:
                continue
            # Short-circuit txs we've already observed in a confirmed block —
            # the chain pool keeps re-advertising them for tens of seconds,
            # but as far as the user is concerned they've already moved to
            # Last block and must not linger in the mempool panel.
            if ext_hex in self._confirmed_exts:
                continue
            ext = self._mempool_decoded.get(ext_hex)
            if ext is None:
                if not self._fast_is_interesting(ext_hex):
                    self._non_stake_exts.add(ext_hex)
                    continue
                try:
                    ext = self._extrinsic_hex_to_tracker_ext(ext_hex)
                    if not ext:
                        self._non_stake_exts.add(ext_hex)
                        continue
                    self._mempool_decoded[ext_hex] = ext
                except Exception:
                    self._non_stake_exts.add(ext_hex)
                    continue
            pool[ext_hex] = ext
            if ext_hex not in seen_at:
                seen_at[ext_hex] = now
                newly_seen.add(ext_hex)

        # Expire pending extrinsics that have been tracked for longer than the
        # configured age cap. The chain node's local txpool can hold stuck or
        # invalid txs across many blocks (typical after long uptime or a brief
        # WS / network hiccup — the exact window in which this bug surfaces),
        # and without this guard such txs keep flowing into every snapshot and
        # appear "glued" to the top of the mempool table while new rows stack
        # up below them. Drop them from every collection at once: ``pool`` so
        # the current snapshot doesn't render them, ``seen_at`` so memory
        # doesn't grow, ``_mempool_decoded`` so we reclaim the decoded dict,
        # and add them to ``_non_stake_exts`` so future polls short-circuit
        # before ever touching the decoder again. The final
        # ``_non_stake_exts &= current_pending`` step keeps the "expired" mark
        # only while the tx is still being re-advertised by the node; once the
        # node finally drops it, the mark is discarded automatically.
        max_age = self._mempool_max_age_s
        if max_age > 0:
            expired = [
                h for h in pool
                if (now - seen_at.get(h, now)) > max_age
            ]
            if expired:
                logger.info(
                    "mempool: expiring %d stuck pending tx(s) older than %.0fs",
                    len(expired),
                    max_age,
                )
                for h in expired:
                    pool.pop(h, None)
                    seen_at.pop(h, None)
                    self._drop_mempool_spots_for_ext(h)
                    self._mempool_decoded.pop(h, None)
                    self._non_stake_exts.add(h)

        for k in list(seen_at):
            if k not in pool:
                seen_at.pop(k, None)
                self._drop_mempool_spots_for_ext(k)
        # ``.pop(k, None)`` (not ``del``) — the dedicated block thread may have
        # popped the same key first; ``del`` would raise ``KeyError``.
        for k in list(self._mempool_decoded):
            if k not in current_pending:
                self._mempool_decoded.pop(k, None)

        for h, ext in pool.items():
            try:
                ops = st.extract_stake_ops(ext)
            except Exception as exc:
                # One malformed extrinsic must not freeze the entire mempool
                # view. Without this, a decoder raise here would short-circuit
                # the function before the ``self._mempool_pool/stake = ...``
                # assignments below — leaving the *previous* poll's state in
                # place permanently (stale rows in the UI, ``last_snapshot_json``
                # never refreshes because the fingerprint doesn't change).
                logger.debug("extract_stake_ops failed for %s: %s", h, exc)
                continue
            if ops:
                stake[h] = ops
                if h in newly_seen:
                    tx_hash = str(ext.get("tx_hash") or "") or None
                    self._remember_mempool_spots(tx_hash, ops)

        self._mempool_pool = pool
        self._mempool_stake = stake
        self._non_stake_exts &= current_pending
        # Drop the "confirmed" flag once the chain pool finally stops
        # re-advertising the tx — keeps the set bounded and makes a genuine
        # reorg that re-pends the tx heal itself on the next poll.
        self._confirmed_exts &= current_pending

