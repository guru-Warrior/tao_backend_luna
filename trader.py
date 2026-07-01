"""
Trader for Bittensor stake operations via direct SDK substrate calls.

Uses a pre-warmed Subtensor connection so extrinsics are composed, signed,
and submitted without spawning a subprocess or re-importing bittensor.
Submits with wait_for_inclusion=False for minimal latency.
"""

from __future__ import annotations

import os
import time
import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# On-chain ``DefaultMinStake`` floor (500_000 RAO) + headroom for swap fees.
_MIN_SWAP_TAO = 0.0006

# Lazy-loaded on first MEV trade so non-MEV paths never import mev_shield_submit.
_mev_shield_submit_fns: Optional[
    Tuple[
        Callable[..., Any],
        Callable[..., Any],
        Callable[..., Any],
        Callable[..., Any],
    ]
] = None


def _get_mev_shield_submit_fns() -> Tuple[
    Callable[..., Any],
    Callable[..., Any],
    Callable[..., Any],
    Callable[..., Any],
]:
    """(add_stake, unstake_all, unstake, submit_calls_batch) — lazy-loaded once."""
    global _mev_shield_submit_fns
    if _mev_shield_submit_fns is None:
        from mev_shield_submit import (
            add_stake_mev_shield,
            submit_calls_mev_shield,
            unstake_all_mev_shield,
            unstake_mev_shield,
        )

        _mev_shield_submit_fns = (
            add_stake_mev_shield,
            unstake_all_mev_shield,
            unstake_mev_shield,
            submit_calls_mev_shield,
        )
    return _mev_shield_submit_fns


def _extrinsic_hash_from_mev_response(resp: Any) -> Optional[str]:
    """Best-effort hash: prefer inner revealed execution (mev_extrinsic), then outer receipt."""
    try:
        for attr in ("mev_extrinsic", "extrinsic_receipt", "mev_extrinsic_receipt"):
            er = getattr(resp, attr, None)
            if er is None:
                continue
            h = getattr(er, "extrinsic_hash", None)
            if h is None:
                continue
            if hasattr(h, "hex"):
                return str(h.hex())
            return str(h)
        return None
    except Exception:
        return None


def _extrinsic_response_error_message(resp: Any) -> str:
    """Human-readable failure from ExtrinsicResponse (dedupe message + error when identical)."""
    seen: list[str] = []
    for raw in (getattr(resp, "message", None), getattr(resp, "error", None)):
        if raw is None:
            continue
        s = str(raw).strip()
        if s and s not in seen:
            seen.append(s)
    return " — ".join(seen) if seen else "Extrinsic failed"


def _mev_shield_period_kw() -> Dict[str, Any]:
    """MEV Shield transaction era (passed to ``submit_encrypted_extrinsic``; bittensor 10.2+ resolves via ``resolve_mev_shield_period``).

    Set ``MEV_SHIELD_TX_PERIOD`` to a block count (e.g. 2048), or ``immortal`` / ``none`` for ``period=None``.
    """
    raw = os.environ.get("MEV_SHIELD_TX_PERIOD")
    if raw is None or not raw.strip():
        return {"period": None}
    s = raw.strip().lower()
    if s in ("immortal", "none", "default"):
        return {"period": None}
    try:
        p = int(raw.strip())
        return {"period": max(8, min(p, 65536))}
    except ValueError:
        return {"period": None}


def _is_tx_outdated_error(exc: BaseException) -> bool:
    s = str(exc).lower()
    return "outdated" in s


# ─────────────────────────────────────────────────────────────────────────────
# Non-MEV submit hardening (idle nonce invalidation + in-request retry)
# ─────────────────────────────────────────────────────────────────────────────
# The helpers below are used exclusively by ``Trader._submit_fast`` /
# ``Trader._reserve_nonce`` — i.e. the non-MEV path. The MEV Shield branch has
# its own (already working) retry loop via ``_prepare_mev_retry`` +
# ``_maybe_fresh_subtensor_for_mev`` and is intentionally untouched.
#
# Design invariants:
#   * Happy path overhead must be ~0 (a ``time.monotonic()`` call + a compare).
#   * Must not change observable behaviour when every submit succeeds first try.
#   * Must never cause a double-submit for a successfully-submitted extrinsic.
#     → We only auto-retry on errors that unambiguously mean "the chain rejected
#        the tx before it entered the tx pool" (stale nonce, bad proof, etc.).
#     → Connection-lost errors get one retry *only after* we recreate the
#        Subtensor, on the pragmatic basis that a dropped WS typically means
#        the tx never left the client. This is the same risk profile as a user
#        re-clicking, which is what happens today when the first submit fails.

# Default: 5s. Faster consecutive submits keep the optimistic cache (no extra
# RPC). Longer idle forces a one-time ``author_nextIndex`` refresh on the next
# submit so an out-of-band nonce bump (e.g. the buy_bot worker firing from its
# own subprocess) can't leave the UI trader with a stale cached value.
_DEFAULT_NONCE_STALENESS_SEC = 5.0


def _nonce_staleness_sec() -> float:
    raw = os.environ.get(
        "TRADER_NONCE_STALENESS_SEC", str(_DEFAULT_NONCE_STALENESS_SEC)
    )
    try:
        return max(0.0, float(raw))
    except ValueError:
        return _DEFAULT_NONCE_STALENESS_SEC


def _trader_auto_retry_enabled() -> bool:
    raw = os.environ.get("TRADER_AUTO_RETRY", "true").strip().lower()
    return raw not in ("0", "false", "no", "off")


# Errors that prove the chain rejected the extrinsic *before* inclusion. Safe
# to re-sign with a fresh nonce and retry — no double-submit risk.
_CHAIN_REJECTED_PATTERNS = (
    "outdated",
    "stale",
    "invalid transaction",
    "1010",             # substrate JSON-RPC error code for Invalid Transaction
    "bad signature",
    "badproof",
    "bad proof",
    "transaction has a bad",
    "future",
    "ancient birth",
    "nonce too low",
)

# Errors that indicate the local connection is probably dead. Ambiguous whether
# the tx made it to the node, but in practice a broken pipe means it did not —
# the write would have flushed first if it had. We recreate the Subtensor so
# the retry uses a fresh WS.
_CONNECTION_LOST_PATTERNS = (
    "broken pipe",
    "connection closed",
    "connection reset",
    "websocket",
    "ws connection",
    "timed out",
    "timeout",
    "eof",
    "connection lost",
    "connectionerror",
)


def _is_chain_rejected_error(exc: BaseException) -> bool:
    s = str(exc).lower()
    return any(p in s for p in _CHAIN_REJECTED_PATTERNS)


def _is_connection_lost_error(exc: BaseException) -> bool:
    s = str(exc).lower()
    return any(p in s for p in _CONNECTION_LOST_PATTERNS)


def _mev_retry_delay_sec() -> float:
    raw = os.environ.get("MEV_SHIELD_RETRY_DELAY_SEC", "0.5")
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.5


def _mev_max_retries() -> int:
    raw = os.environ.get("MEV_SHIELD_MAX_RETRIES", "5")
    try:
        return max(1, min(12, int(raw)))
    except ValueError:
        return 5


def _mev_fresh_subtensor_mode() -> str:
    """off | retry (default) | always — recreate Subtensor to avoid stale nonce/RPC cache (Grok checklist)."""
    raw = os.environ.get("MEV_SHIELD_FRESH_SUBTENSOR", "retry").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return "off"
    if raw in ("always", "every"):
        return "always"
    return "retry"


def _mev_raise_error_kw() -> Dict[str, Any]:
    raw = os.environ.get("MEV_SHIELD_RAISE_ERROR", "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return {"raise_error": True}
    return {}


def _mev_block_until_reveal() -> bool:
    """When True, hold the trader lock through MEV Shield inclusion + inner reveal.

    Default **False**: the encrypted outer extrinsic is submitted and the call
    returns as soon as it is accepted into the pool, so ``self._lock`` is released
    immediately and the next trade can proceed without waiting many blocks for
    inclusion/reveal. Set ``MEV_SHIELD_BLOCK_UNTIL_REVEAL=1`` to restore the legacy
    blocking behavior.
    """
    raw = os.environ.get("MEV_SHIELD_BLOCK_UNTIL_REVEAL", "").strip().lower()
    if raw:
        return raw not in ("0", "false", "no", "off")
    # Back-compat: the older batch-only knob still works (default off = non-blocking).
    raw_batch = os.environ.get("MEV_SHIELD_BATCH_WAIT_REVEAL", "0").strip().lower()
    return raw_batch not in ("0", "false", "no", "off")


def _mev_submit_wait_kwargs() -> Dict[str, bool]:
    """Wait flags for MEV Shield submits. Non-blocking by default so the trader
    lock is freed right after the encrypted extrinsic is submitted."""
    block = _mev_block_until_reveal()
    return {
        "wait_for_inclusion": block,
        "wait_for_finalization": False,
        "wait_for_revealed_execution": block,
    }


def _extrinsic_response_suggests_outdated(resp: Any) -> bool:
    err = _extrinsic_response_error_message(resp)
    return "outdated" in err.lower()


@dataclass
class PositionTradeResult:
    """Per-position outcome for batch MEV Shield sells."""

    netuid: int
    hotkey: str
    success: bool
    message: str
    extrinsic_hash: Optional[str] = None


@dataclass
class RoundTripReservation:
    """Accumulated round-trip buys on one (netuid, hotkey); sell uses ``sell_nonce``."""

    round_trip_id: str
    netuid: int
    hotkey: str
    buy_nonce: int
    sell_nonce: int
    buy_amount_tao: float
    slippage_pct: float
    no_slippage: bool
    created_at: float


@dataclass
class TradeResult:
    success: bool
    message: str
    extrinsic_hash: Optional[str] = None
    elapsed_ms: Optional[float] = None
    positions: Optional[list[PositionTradeResult]] = None
    round_trip_id: Optional[str] = None
    buy_nonce: Optional[int] = None
    sell_nonce: Optional[int] = None
    hotkey: Optional[str] = None
    round_trip_buy_tao: Optional[float] = None


class Trader:
    """Stake / unstake via direct SDK substrate calls."""

    def __init__(
        self,
        wallet_name: str,
        hotkey_name: str,
        network: str = "finney",
        delegate_hotkey_ss58: Optional[str] = None,
    ):
        self.wallet_name = wallet_name
        self.hotkey_name = hotkey_name
        self.network = network
        # One trade at a time: same coldkey nonce sequence; parallel submits cause 1010 / outdated.
        self._lock = threading.Lock()

        # Local optimistic nonce cache: fetched once, incremented per submit,
        # reset to chain on error. Saves ~1 RPC per trade (author_nextIndex).
        # ``_last_nonce_use_ts`` (monotonic seconds) lets ``_reserve_nonce``
        # self-invalidate the cache after a period of inactivity so an
        # out-of-band consumer of the same coldkey (e.g. the buy_bot worker,
        # which is a separate subprocess) cannot leave us signing with a stale
        # nonce. Fast consecutive trades stay on the optimistic path.
        self._nonce_lock = threading.Lock()
        self._next_nonce: Optional[int] = None
        self._last_nonce_use_ts: float = 0.0
        # One-block buy→sell round trips: sell nonce reserved at buy time,
        # consumed when the user presses Remove.
        self._round_trips: Dict[str, RoundTripReservation] = {}
        self._round_trip_by_pos: Dict[tuple[int, str], str] = {}

        # Immutable block-hash memoization (genesis + era-birth blocks). Scoped
        # to this Trader's own substrate instance. See
        # :meth:`_install_block_hash_memoization` for the safety argument.
        self._block_hash_cache: Dict[int, str] = {}

        # Secondary Subtensor used only by the background era-birth prefetch
        # thread. Lazy-initialised on first need so Trader startup is unchanged.
        self._bg_subtensor: Optional[Any] = None
        self._bg_sub_lock = threading.Lock()
        self._bg_prefetch_inflight = threading.Event()

        import bittensor as bt
        from bittensor_wallet import Wallet

        t0 = time.time()
        self._subtensor = bt.Subtensor(network=network)
        self._wallet = Wallet(name=wallet_name, hotkey=hotkey_name)
        self._hotkey_ss58 = delegate_hotkey_ss58 or self._wallet.hotkey.ss58_address
        # Force decryption + keypair cache now so the first trade doesn't pay
        # the wallet-unlock cost. Relies on WALLET_PASSWORD being in env.
        try:
            _ = self._wallet.coldkey
        except Exception as ex:
            logger.debug("Eager wallet unlock skipped: %s", ex)
        # Pre-warm chain metadata: composing a dummy call populates the SDK's
        # metadata cache so the first real stake call doesn't re-download it.
        try:
            from bittensor.core.extrinsics.pallets import SubtensorModule
            _ = SubtensorModule(self._subtensor).add_stake(
                netuid=1, hotkey=self._hotkey_ss58, amount_staked=1
            )
        except Exception as ex:
            logger.debug("Metadata pre-warm skipped: %s", ex)
        # Install block-hash memoization + prewarm genesis (block 0). Genesis
        # is absolutely immutable, and ``generate_signature_payload`` fetches it
        # on every extrinsic sign. Saves ~30–80ms per trade.
        try:
            self._install_block_hash_memoization()
        except Exception as ex:
            logger.debug("Block-hash memoization skipped: %s", ex)
        elapsed = (time.time() - t0) * 1000
        logger.info(
            "Trader ready (%.0fms) — wallet=%s delegate_hotkey=%s network=%s",
            elapsed, wallet_name, self._hotkey_ss58, network,
        )

    # ---- nonce cache -----------------------------------------------------

    def _maybe_refresh_nonce_from_chain(self) -> None:
        """Invalidate cached nonce when idle too long (see ``_reserve_nonce``)."""
        now = time.monotonic()
        if (
            self._next_nonce is not None
            and self._last_nonce_use_ts > 0.0
            and (now - self._last_nonce_use_ts) > _nonce_staleness_sec()
        ):
            self._next_nonce = None

    def _fetch_next_nonce_from_chain(self) -> int:
        return int(
            self._subtensor.substrate.get_account_next_index(
                self._wallet.coldkeypub.ss58_address
            )
        )

    def _reserve_nonce(self) -> int:
        """Return next nonce, refreshing from chain when unknown or stale.

        "Stale" = we have a cached ``_next_nonce`` but haven't used it within
        ``TRADER_NONCE_STALENESS_SEC`` seconds. Other coldkey consumers
        (buy_bot worker in another process, external tools, etc.) may have
        advanced the on-chain nonce in the meantime; re-fetching once after an
        idle window costs ~20–40 ms of RPC and prevents a "first submit
        fails-stale, second succeeds" round trip that easily exceeds 1 s.
        """
        with self._nonce_lock:
            self._maybe_refresh_nonce_from_chain()
            if self._next_nonce is None:
                self._next_nonce = self._fetch_next_nonce_from_chain()
            n = self._next_nonce
            self._next_nonce = n + 1
            self._last_nonce_use_ts = time.monotonic()
            return n

    def _reserve_nonce_pair(self) -> tuple[int, int]:
        """Reserve consecutive nonces ``(N, N+1)`` for buy then sell."""
        with self._nonce_lock:
            self._maybe_refresh_nonce_from_chain()
            if self._next_nonce is None:
                self._next_nonce = self._fetch_next_nonce_from_chain()
            buy_n = self._next_nonce
            sell_n = buy_n + 1
            self._next_nonce = sell_n + 1
            self._last_nonce_use_ts = time.monotonic()
            return buy_n, sell_n

    def _sync_nonce_after_explicit(self, used_nonce: int) -> None:
        """Keep the optimistic cache aligned after a caller-chosen nonce."""
        with self._nonce_lock:
            if self._next_nonce is None or self._next_nonce <= used_nonce:
                self._next_nonce = used_nonce + 1
            self._last_nonce_use_ts = time.monotonic()

    def _reset_nonce(self) -> None:
        with self._nonce_lock:
            self._next_nonce = None
            # Intentionally leave ``_last_nonce_use_ts`` alone: the next
            # ``_reserve_nonce`` sees ``_next_nonce is None`` and fetches
            # fresh regardless of the timestamp, so clearing it would be
            # redundant and only risk a test-seam regression.

    # ---- immutable block-hash memoization --------------------------------

    def _install_block_hash_memoization(self) -> None:
        """Memoize ``substrate.get_block_hash(block_id=N)`` on this Trader's
        substrate instance for positive integer ``block_id``.

        **Why this is safe.**

        * Patched attribute is ``self._subtensor.substrate.get_block_hash``,
          i.e. a bound method on *this* ``SubstrateInterface`` instance. Every
          other consumer of substrate-interface in the process (mempool
          monitor, balance checker, etc.) builds its own ``SubstrateInterface``
          and is therefore untouched.
        * ``block_id=None`` (chain-head lookup) always bypasses the cache and
          calls the original — the chain head is mutable, caching it would be
          wrong.
        * ``block_id=0`` is genesis: immutable forever.
        * Positive ``block_id`` values are cached after first resolution. On
          Bittensor/Substrate, a block hash for a given block number is only
          allowed to change via a chain reorg, and we only request hashes for
          blocks that are referenced by signing (genesis + era birth). Era
          birth blocks are well past finalization in steady state; even in the
          adversarial case of a reorg, a stale cached hash simply causes the
          signed extrinsic to be rejected (``BadProof``), which
          :meth:`_submit_fast` already handles by resetting the nonce. No
          silent correctness regression is possible.
        * Idempotent: calling twice on the same substrate shares the same
          cache dict.

        **MEV Shield safety.** The MEV shield submission path uses
        ``self._subtensor.substrate`` too, so it gets the cache hit for free —
        but only when it calls ``get_block_hash(block_id=<int>)``. Its own
        flows still reach through to live RPCs for everything else (nonce,
        submit, revealed-execution polling). No semantic change.
        """
        sub = self._subtensor.substrate
        cache = self._block_hash_cache

        if getattr(sub, "_trader_bhc_installed", False):
            existing = getattr(sub, "_trader_bhc_cache", None)
            if isinstance(existing, dict):
                self._block_hash_cache = existing
            return

        orig_get_block_hash = sub.get_block_hash

        def cached_get_block_hash(block_id=None):  # type: ignore[override]
            if block_id is None:
                return orig_get_block_hash(block_id=None)
            if isinstance(block_id, int) and block_id >= 0:
                hit = cache.get(block_id)
                if hit is not None:
                    return hit
                result = orig_get_block_hash(block_id=block_id)
                if isinstance(result, str) and result:
                    cache[block_id] = result
                return result
            return orig_get_block_hash(block_id=block_id)

        sub.get_block_hash = cached_get_block_hash  # type: ignore[assignment]
        sub._trader_bhc_installed = True
        sub._trader_bhc_cache = cache

        try:
            cached_get_block_hash(block_id=0)
        except Exception as ex:
            logger.debug("Genesis hash prewarm skipped: %s", ex)

    # ---- background era-birth prefetch (proposal 3) ----------------------

    _ERA_PERIOD = 64  # Must match the ``period`` used in :meth:`_submit_fast`.

    def _ensure_bg_subtensor(self) -> Optional[Any]:
        """Lazy-init the secondary Subtensor used for background prefetch.

        Runs on its own WebSocket connection so it never contends with the
        main ``self._subtensor`` (which is serialised behind ``self._lock``
        during a trade). Failure to construct it is non-fatal — the prefetch
        simply skips and the next trade pays the usual RPC cost.
        """
        if self._bg_subtensor is not None:
            return self._bg_subtensor
        with self._bg_sub_lock:
            if self._bg_subtensor is not None:
                return self._bg_subtensor
            try:
                import bittensor as bt
                self._bg_subtensor = bt.Subtensor(network=self.network)
            except Exception as ex:
                logger.debug("Background Subtensor init skipped: %s", ex)
                return None
        return self._bg_subtensor

    def _prefetch_era_birth_async(self) -> None:
        """Warm ``self._block_hash_cache`` with the current and next era-birth
        block hashes, using the secondary substrate, on a daemon thread.

        This is pure optimisation. The foreground :meth:`_submit_fast` does
        not depend on it — if the prefetch hasn't landed yet, the main path
        falls back to ``substrate.get_block_hash(block_id=…)`` exactly as
        before. If it has landed, the main path takes a dict lookup instead
        of a WS round trip.

        The call is strictly non-blocking: one in-flight prefetch at a time,
        and all exceptions are swallowed.
        """
        if self._bg_prefetch_inflight.is_set():
            return

        head = None
        try:
            import chain_head_cache as _head_cache
            head = _head_cache.get_fresh_head_number()
        except Exception:
            head = None
        if head is None:
            return

        period = self._ERA_PERIOD
        current_birth = head - (head % period)
        next_birth = current_birth + period
        wanted = [b for b in (current_birth, next_birth)
                  if b > 0 and b not in self._block_hash_cache]
        if not wanted:
            return

        self._bg_prefetch_inflight.set()

        def _run() -> None:
            try:
                bg = self._ensure_bg_subtensor()
                if bg is None:
                    return
                bg_sub = bg.substrate
                for bid in wanted:
                    if bid in self._block_hash_cache:
                        continue
                    try:
                        h = bg_sub.get_block_hash(block_id=bid)
                    except Exception as ex:
                        logger.debug("Era-birth prefetch (%d) skipped: %s", bid, ex)
                        continue
                    if isinstance(h, str) and h:
                        # Single writer (this thread) + dict __setitem__ is
                        # atomic in CPython — no lock required for readers.
                        self._block_hash_cache[bid] = h
            finally:
                self._bg_prefetch_inflight.clear()

        threading.Thread(
            target=_run,
            name="trader-era-prefetch",
            daemon=True,
        ).start()

    # ---- fast submit (bypass SDK wrapper's get_extrinsic_fee RPC) --------

    def _submit_fast(self, call: Any, *, nonce: Optional[int] = None) -> None:
        """Sign + submit with minimal RPC overhead.

        Skips ``Subtensor.sign_and_send_extrinsic`` because that helper, even
        with both waits disabled, still issues a ``get_payment_info`` RPC to
        populate ``extrinsic_fee`` on the response object — ~50–150 ms of
        latency we never consume. We instead drive ``substrate`` directly.

        When ``nonce`` is given the extrinsic is signed with that exact account
        nonce (used by one-block round-trip buy/sell). Otherwise the next
        nonce is taken from the optimistic cache via ``_reserve_nonce``.

        Extra optimisation: pre-fill ``era['current']`` with the head block
        number that the mempool monitor already tracks (via
        ``chain_head_cache``). Without this, ``create_signed_extrinsic``
        issues ``chain_getFinalisedHead`` + ``chain_getHeader`` every call to
        resolve ``current`` — two avoidable WS round trips per trade. On a
        stale/empty cache we simply omit ``current`` and the library falls
        back to its default RPC path (zero behaviour change).
        """
        import chain_head_cache as _head_cache

        keypair = self._wallet.coldkey
        max_attempts = 2 if _trader_auto_retry_enabled() else 1
        last_exc: Optional[BaseException] = None

        for attempt in range(max_attempts):
            # Re-bind every loop so that, after ``_recreate_subtensor`` runs on
            # a connection-lost retry, the fresh substrate is used to sign and
            # submit. On the happy (attempt=0) path this is the same object
            # reference we had before — zero extra work.
            sub = self._subtensor.substrate
            use_nonce = nonce if nonce is not None else self._reserve_nonce()
            era: Dict[str, Any] = {"period": 64}
            cached_bn = _head_cache.get_fresh_head_number()
            if cached_bn is not None:
                era["current"] = cached_bn
            try:
                ext = sub.create_signed_extrinsic(
                    call=call,
                    keypair=keypair,
                    nonce=use_nonce,
                    era=era,
                )
                sub.submit_extrinsic(
                    extrinsic=ext,
                    wait_for_inclusion=False,
                    wait_for_finalization=False,
                )
                if nonce is not None:
                    self._sync_nonce_after_explicit(use_nonce)
                # Success: warm the next era-birth block hash on the background
                # connection so the next trade after an era rollover isn't the
                # one paying the ``chain_getBlockHash`` round trip. Pure
                # optimisation — wrapped in a catch-all so this cannot affect
                # the trade outcome.
                try:
                    self._prefetch_era_birth_async()
                except Exception as ex:
                    logger.debug("Era-birth prefetch dispatch skipped: %s", ex)
                return
            except Exception as exc:
                last_exc = exc
                # Nonce state is uncertain after any submit failure; re-fetch
                # on the next ``_reserve_nonce`` call regardless of outcome.
                self._reset_nonce()

                retry_left = attempt + 1 < max_attempts
                rejected = _is_chain_rejected_error(exc)
                conn_lost = _is_connection_lost_error(exc)
                if not retry_left or not (rejected or conn_lost):
                    raise

                # Connection-lost → swap in a fresh WS before retrying. The
                # ``call`` object is a chain-wide scale-encoded ``GenericCall``
                # and is safe to re-sign against the new substrate (same
                # runtime metadata).
                if conn_lost:
                    try:
                        self._recreate_subtensor()
                    except Exception as reconnect_exc:
                        logger.debug(
                            "Subtensor recreate during retry failed: %s",
                            reconnect_exc,
                        )
                logger.warning(
                    "Fast submit transient failure [retry=%d/%d] (%s): %s",
                    attempt + 1,
                    max_attempts - 1,
                    "conn-lost" if conn_lost else "chain-rejected",
                    exc,
                )
                # Loop continues: fresh nonce + (possibly) fresh substrate.

        # Loop exhausted without returning — re-raise the last seen error so
        # callers (``buy`` / ``sell``) can surface it unchanged.
        assert last_exc is not None
        raise last_exc

    def _recreate_subtensor(self) -> None:
        """New Subtensor connection to reduce stale nonce / block cache after 1010 outdated (SDK is stateful over WS)."""
        import bittensor as bt

        t0 = time.time()
        self._subtensor = bt.Subtensor(network=self.network)
        logger.debug(
            "MEV Shield: recreated Subtensor in %.0fms (network=%s)",
            (time.time() - t0) * 1000,
            self.network,
        )

    def _maybe_fresh_subtensor_for_mev(self, attempt: int) -> None:
        mode = _mev_fresh_subtensor_mode()
        if mode == "off":
            return
        if mode == "always" or (mode == "retry" and attempt > 0):
            self._recreate_subtensor()

    def _log_mev_coldkey_nonce(self, attempt: int) -> None:
        if not logger.isEnabledFor(logging.DEBUG):
            return
        try:
            idx = self._subtensor.substrate.get_account_next_index(
                self._wallet.coldkeypub.ss58_address
            )
            blk = self._subtensor.get_current_block()
            logger.debug(
                "MEV Shield attempt %d: head block=%s coldkey next_index=%s",
                attempt,
                blk,
                idx,
            )
        except Exception as ex:
            logger.debug("MEV Shield nonce debug skipped: %s", ex)

    def _prepare_mev_retry(self, attempt: int) -> None:
        """After outdated: advance head (~2 blocks) + exponential backoff (Grok / Substrate stale-era pattern)."""
        try:
            cur = self._subtensor.get_current_block()
            self._subtensor.wait_for_block(block=cur + 2)
        except Exception:
            try:
                self._subtensor.wait_for_block()
                self._subtensor.wait_for_block()
            except Exception as ex:
                logger.debug("wait_for_block before MEV retry skipped: %s", ex)
        base = _mev_retry_delay_sec()
        if base > 0:
            time.sleep(base * float(attempt + 1))

    def _get_limit_price(
        self,
        netuid: int,
        tolerance: float,
        direction: str,
        subnet: Optional[Any] = None,
    ) -> int:
        """Query on-chain price and apply tolerance. direction='buy' or 'sell'.

        Order of preference:
          1. ``subnet`` object passed in by the caller (already fetched).
          2. Shared price cache populated by the mempool monitor (~1 block TTL).
          3. Fresh ``subtensor.subnet(netuid=…)`` RPC (slow fallback).
        """
        from bittensor.utils.balance import Balance
        import price_cache as _price_cache

        tao_per_alpha: Optional[float] = None
        if subnet is not None:
            try:
                tao_per_alpha = float(subnet.price.tao)
            except Exception:
                tao_per_alpha = None
        if tao_per_alpha is None:
            tao_per_alpha = _price_cache.get_fresh_tao_per_alpha(netuid)
        if tao_per_alpha is None:
            pool = self._subtensor.subnet(netuid=netuid)
            tao_per_alpha = float(pool.price.tao)

        if direction == "buy":
            price = tao_per_alpha * (1 + tolerance)
        else:
            price = tao_per_alpha * (1 - tolerance)
        return Balance.from_tao(price).rao

    def _tao_per_alpha(self, netuid: int, subnet: Optional[Any] = None) -> Optional[float]:
        """Spot τ per α for ``netuid`` (same sources as ``_get_limit_price``)."""
        import price_cache as _price_cache

        tao_per_alpha: Optional[float] = None
        if subnet is not None:
            try:
                tao_per_alpha = float(subnet.price.tao)
            except Exception:
                tao_per_alpha = None
        if tao_per_alpha is None:
            tao_per_alpha = _price_cache.get_fresh_tao_per_alpha(netuid)
        if tao_per_alpha is None:
            try:
                pool = self._subtensor.subnet(netuid=netuid)
                tao_per_alpha = float(pool.price.tao)
            except Exception:
                tao_per_alpha = None
        return tao_per_alpha if tao_per_alpha and tao_per_alpha > 0 else None

    def _get_swap_cross_limit_price_rao(
        self,
        origin_netuid: int,
        dest_netuid: int,
        tolerance: float,
        origin_subnet: Optional[Any] = None,
        dest_subnet: Optional[Any] = None,
    ) -> int:
        """Worst acceptable origin/dest α-price ratio for ``swap_stake_limit`` (RAO).

        Subtensor compares ``limit_price / 1e9`` to the current cross-subnet
        ratio (origin τ/α ÷ dest τ/α). It errors with ``ZeroMaxStakeAmount``
        when the limit is above spot. Passing ``0`` disables the cap.
        """
        from bittensor.utils.balance import Balance

        if tolerance >= 0.999999:
            return 0

        o_p = self._tao_per_alpha(origin_netuid, subnet=origin_subnet)
        d_p = self._tao_per_alpha(dest_netuid, subnet=dest_subnet)
        if o_p is None or d_p is None:
            return 0

        current_ratio = o_p / d_p
        limit_ratio = current_ratio * max(0.0, 1.0 - float(tolerance))
        if limit_ratio <= 0:
            return 0
        return int(Balance.from_tao(limit_ratio).rao)

    @staticmethod
    def _swap_batch_failure_message(
        skipped: list[PositionTradeResult],
        *,
        prefix: str = "No valid stake positions to swap",
    ) -> str:
        if not skipped:
            return prefix
        details = "; ".join(
            f"n{p.netuid}: {p.message}" for p in skipped[:4]
        )
        if len(skipped) > 4:
            details += f"; +{len(skipped) - 4} more"
        return f"{prefix} ({details})"

    def _store_round_trip(
        self,
        *,
        netuid: int,
        hotkey: str,
        buy_nonce: int,
        sell_nonce: int,
        buy_amount_tao: float,
        slippage_pct: float,
        no_slippage: bool,
    ) -> RoundTripReservation:
        import uuid

        pos_key = (netuid, hotkey)
        existing_id = self._round_trip_by_pos.get(pos_key)
        if existing_id:
            existing = self._round_trips.get(existing_id)
            if existing is not None:
                existing.buy_nonce = buy_nonce
                existing.sell_nonce = sell_nonce
                existing.buy_amount_tao += buy_amount_tao
                existing.slippage_pct = slippage_pct
                existing.no_slippage = no_slippage
                return existing

        rt_id = f"rt-{netuid}-{buy_nonce}-{uuid.uuid4().hex[:8]}"
        reservation = RoundTripReservation(
            round_trip_id=rt_id,
            netuid=netuid,
            hotkey=hotkey,
            buy_nonce=buy_nonce,
            sell_nonce=sell_nonce,
            buy_amount_tao=buy_amount_tao,
            slippage_pct=slippage_pct,
            no_slippage=no_slippage,
            created_at=time.time(),
        )
        self._round_trips[rt_id] = reservation
        self._round_trip_by_pos[pos_key] = rt_id
        return reservation

    def sell_round_trip(self, round_trip_id: str) -> TradeResult:
        """Submit the reserved sell extrinsic (nonce N+1) for a round-trip buy."""
        from bittensor.core.extrinsics.pallets import SubtensorModule

        reservation = self._round_trips.pop(round_trip_id, None)
        if reservation is None:
            return TradeResult(
                False,
                f"Unknown or expired round-trip id: {round_trip_id}",
            )
        self._round_trip_by_pos.pop((reservation.netuid, reservation.hotkey), None)

        t0 = time.time()
        tolerance = reservation.slippage_pct / 100.0
        validator_hk = reservation.hotkey
        netuid = reservation.netuid

        try:
            with self._lock:
                if reservation.no_slippage:
                    call = SubtensorModule(self._subtensor).remove_stake_full_limit(
                        netuid=netuid,
                        hotkey=validator_hk,
                        limit_price=None,
                    )
                else:
                    limit_price = self._get_limit_price(netuid, tolerance, "sell")
                    call = SubtensorModule(self._subtensor).remove_stake_full_limit(
                        netuid=netuid,
                        hotkey=validator_hk,
                        limit_price=limit_price,
                    )
                self._submit_fast(call, nonce=reservation.sell_nonce)
        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            logger.error(
                "round-trip sell failed (%.0fms) id=%s: %s",
                elapsed,
                round_trip_id,
                e,
            )
            return TradeResult(False, str(e), elapsed_ms=elapsed)

        elapsed = (time.time() - t0) * 1000
        msg = (
            f"Submitted round-trip sell n{netuid} full position "
            f"(nonce {reservation.sell_nonce}, {elapsed:.0f}ms)"
        )
        logger.info(
            "round-trip sell submitted id=%s buy_nonce=%d sell_nonce=%d n%d",
            round_trip_id,
            reservation.buy_nonce,
            reservation.sell_nonce,
            netuid,
        )
        return TradeResult(success=True, message=msg, elapsed_ms=elapsed)

    def buy(
        self,
        netuid: int,
        amount_tao: float,
        slippage_pct: float = 1.0,
        hotkey: Optional[str] = None,
        allow_partial: bool = True,
        no_slippage: bool = False,
        mev_shield: bool = False,
        decoy_tao: float = 0.0,
        round_trip: bool = False,
    ) -> TradeResult:
        from bittensor.core.extrinsics.pallets import SubtensorModule
        from bittensor.utils.balance import Balance

        t0 = time.time()
        amount_rao = Balance.from_tao(amount_tao).rao
        tolerance = slippage_pct / 100.0
        validator_hk = (hotkey or "").strip() or self._hotkey_ss58

        if round_trip and mev_shield:
            return TradeResult(
                False,
                "One-block round-trip is not supported with MEV Shield",
                elapsed_ms=0.0,
            )

        # Serialize all Subtensor RPC on this instance (shared WS is not thread-safe).
        try:
            with self._lock:
                buy_nonce: Optional[int] = None
                sell_nonce: Optional[int] = None
                if round_trip:
                    # One nonce per buy (no pair skip). Sell nonce is buy+1 locally
                    # so consecutive stakes stay ready in the mempool; Remove submits
                    # the reserved sell nonce after the last buy.
                    buy_nonce = self._reserve_nonce()
                    sell_nonce = buy_nonce + 1
                # Non-MEV: SubtensorModule + sign_and_send_extrinsic. MEV: mev_shield_submit (lazy-imported).
                if mev_shield:
                    # submit_encrypted_extrinsic + blocks_for_revealed_execution (see mev_shield_submit).
                    add_stake_mev_shield, _, _, _ = _get_mev_shield_submit_fns()
                    mev_kw = _mev_shield_period_kw()
                    raise_kw = _mev_raise_error_kw()
                    max_mev = _mev_max_retries()
                    resp = None
                    for attempt in range(max_mev):
                        self._maybe_fresh_subtensor_for_mev(attempt)
                        self._log_mev_coldkey_nonce(attempt)
                        try:
                            resp = add_stake_mev_shield(
                                self._subtensor,
                                self._wallet,
                                netuid,
                                self._hotkey_ss58,
                                Balance.from_tao(amount_tao),
                                safe_staking=not no_slippage,
                                allow_partial_stake=allow_partial,
                                rate_tolerance=tolerance,
                                **mev_kw,
                                **raise_kw,
                                **_mev_submit_wait_kwargs(),
                            )
                        except Exception as e:
                            if (
                                attempt < max_mev - 1
                                and _is_tx_outdated_error(e)
                            ):
                                logger.warning(
                                    "buy MEV Shield retry after outdated: %s", e
                                )
                                self._prepare_mev_retry(attempt)
                                continue
                            raise
                        if not getattr(resp, "success", False):
                            err = _extrinsic_response_error_message(resp)
                            if (
                                attempt < max_mev - 1
                                and _extrinsic_response_suggests_outdated(resp)
                            ):
                                logger.warning(
                                    "buy MEV Shield retry after outdated response: %s",
                                    err,
                                )
                                self._prepare_mev_retry(attempt)
                                continue
                            elapsed = (time.time() - t0) * 1000
                            logger.error("buy MEV Shield failed (%.0fms): %s", elapsed, err)
                            return TradeResult(False, err, elapsed_ms=elapsed)
                        break
                    assert resp is not None
                    elapsed = (time.time() - t0) * 1000
                    logger.info(
                        "buy MEV Shield submitted (%.0fms) netuid=%d amount=%.4f",
                        elapsed,
                        netuid,
                        amount_tao,
                    )
                    return TradeResult(
                        success=True,
                        message=f"Submitted buy n{netuid} {amount_tao}τ ({elapsed:.0f}ms) [MEV Shield]",
                        extrinsic_hash=_extrinsic_hash_from_mev_response(resp),
                        elapsed_ms=elapsed,
                    )

                limit_price_rao: Optional[int] = None
                if not no_slippage:
                    limit_price_rao = self._get_limit_price(netuid, tolerance, "buy")

                if no_slippage:
                    call = SubtensorModule(self._subtensor).add_stake(
                        netuid=netuid,
                        hotkey=self._hotkey_ss58,
                        amount_staked=amount_rao,
                    )
                else:
                    assert limit_price_rao is not None
                    call = SubtensorModule(self._subtensor).add_stake_limit(
                        hotkey=self._hotkey_ss58,
                        netuid=netuid,
                        amount_staked=amount_rao,
                        limit_price=limit_price_rao,
                        allow_partial=allow_partial,
                    )

                if round_trip:
                    assert buy_nonce is not None
                    self._submit_fast(call, nonce=buy_nonce)
                else:
                    self._submit_fast(call)
        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            logger.error("buy failed (%.0fms): %s", elapsed, e)
            if round_trip:
                self._reset_nonce()
            return TradeResult(False, str(e), elapsed_ms=elapsed)

        reservation: Optional[RoundTripReservation] = None
        if round_trip:
            assert buy_nonce is not None and sell_nonce is not None
            reservation = self._store_round_trip(
                netuid=netuid,
                hotkey=validator_hk,
                buy_nonce=buy_nonce,
                sell_nonce=sell_nonce,
                buy_amount_tao=amount_tao,
                slippage_pct=slippage_pct,
                no_slippage=no_slippage,
            )

        # Emergency decoy: fire a never-matching remove_stake_limit on a daemon
        # thread so the HTTP response isn't delayed by the second submit. The
        # decoy lands in the mempool ~1 RPC round-trip after the real buy.
        # Suppressed when mev_shield=True (the encrypted-extrinsic flow has its
        # own privacy guarantees) and obviously a no-op when decoy_tao<=0.
        # Also suppressed for round-trip buys — the decoy would consume the
        # reserved sell nonce.
        if not mev_shield and not round_trip and decoy_tao and decoy_tao > 0:
            self._fire_decoy_sell_async(netuid, float(decoy_tao))

        elapsed = (time.time() - t0) * 1000
        logger.info("buy submitted (%.0fms) netuid=%d amount=%.4f", elapsed, netuid, amount_tao)
        rt_note = ""
        if reservation is not None:
            rt_note = (
                f" [round-trip total {reservation.buy_amount_tao:.4f}τ"
                f", sell nonce {reservation.sell_nonce}]"
            )
        return TradeResult(
            success=True,
            message=f"Submitted buy n{netuid} {amount_tao}τ ({elapsed:.0f}ms){rt_note}",
            elapsed_ms=elapsed,
            round_trip_id=reservation.round_trip_id if reservation else None,
            buy_nonce=reservation.buy_nonce if reservation else None,
            sell_nonce=reservation.sell_nonce if reservation else None,
            hotkey=validator_hk if reservation else None,
            round_trip_buy_tao=reservation.buy_amount_tao if reservation else None,
        )

    # ---- emergency decoy sell --------------------------------------------

    def _fire_decoy_sell_async(self, netuid: int, decoy_tao: float) -> None:
        """Spawn a daemon thread that submits a decoy ``remove_stake_limit``.

        The decoy uses the real coldkey + delegate hotkey (so it looks
        identical to a legitimate unstake to a mempool observer) but with a
        ``limit_price`` set ~1000× above the current spot and
        ``allow_partial=False`` — so the dispatch always reverts on chain and
        no stake is actually removed. The TAO-equivalent amount comes from
        the user (``decoy_tao``); the alpha amount is derived from the cached
        spot price.
        """
        def _run() -> None:
            try:
                with self._lock:
                    self._fire_decoy_sell(netuid, decoy_tao)
            except Exception as ex:  # noqa: BLE001
                logger.warning(
                    "decoy sell failed (n%d, %.4fτ): %s", netuid, decoy_tao, ex
                )

        threading.Thread(
            target=_run,
            name=f"trader-decoy-n{netuid}",
            daemon=True,
        ).start()

    def _fire_decoy_sell(self, netuid: int, decoy_tao: float) -> None:
        """Compose + submit the decoy unstake. Must be called holding ``self._lock``."""
        from bittensor.core.extrinsics.pallets import SubtensorModule
        from bittensor.utils.balance import Balance
        import price_cache as _price_cache

        t0 = time.time()

        # alpha amount = decoy_tao TAO ÷ spot price (TAO per alpha).
        # Try the shared price cache first (populated by the mempool monitor);
        # fall back to a single subnet RPC if the cache is cold.
        tao_per_alpha = _price_cache.get_fresh_tao_per_alpha(netuid)
        if tao_per_alpha is None or tao_per_alpha <= 0:
            try:
                sn = self._subtensor.subnet(netuid=netuid)
                tao_per_alpha = float(sn.price.tao)
            except Exception as ex:  # noqa: BLE001
                logger.warning(
                    "decoy: cannot resolve price for n%d (%s); skipping", netuid, ex
                )
                return
        if tao_per_alpha <= 0:
            logger.warning("decoy: non-positive price for n%d; skipping", netuid)
            return

        alpha_tao = decoy_tao / tao_per_alpha
        alpha_rao = max(1, int(Balance.from_tao(alpha_tao).rao))

        # Sell limit_price is the *minimum* TAO/alpha the user will accept.
        # Picking ~1000× current spot guarantees the chain reverts the
        # dispatch (with allow_partial=False the whole call aborts). No stake
        # is ever removed even if the user has alpha on this hotkey.
        unreachable_limit_rao = int(
            Balance.from_tao(tao_per_alpha * 1000.0).rao
        )

        call = SubtensorModule(self._subtensor).remove_stake_limit(
            hotkey=self._hotkey_ss58,
            netuid=netuid,
            amount_unstaked=alpha_rao,
            limit_price=unreachable_limit_rao,
            allow_partial=False,
        )
        self._submit_fast(call)
        elapsed = (time.time() - t0) * 1000
        logger.info(
            "decoy sell submitted (%.0fms) netuid=%d alpha_rao=%d (~%.4fα @ %.4fτ/α, decoy=%.4fτ)",
            elapsed,
            netuid,
            alpha_rao,
            alpha_tao,
            tao_per_alpha,
            decoy_tao,
        )

    def _unstake_alpha_rao_for_tao_amount(
        self,
        netuid: int,
        validator_hotkey: str,
        amount_tao: float,
        subnet: Optional[Any] = None,
    ) -> tuple[int, int, float, Optional[Any]]:
        """Map requested unstake (TAO-equivalent, same basis as wallet UI) to alpha RAO.

        Returns (unstake_rao, total_rao, requested_tao_capped, subnet_or_none).
        ``subnet_or_none`` is the subnet object used for ``alpha_to_tao`` so callers can reuse it
        for limit-price queries without a second ``.subnet(netuid=…)`` RPC.
        """
        from bittensor.utils.balance import Balance
        import price_cache as _price_cache

        stake_bal = self._subtensor.get_stake(
            coldkey_ss58=self._wallet.coldkeypub.ss58_address,
            hotkey_ss58=validator_hotkey,
            netuid=netuid,
        )
        total_rao = int(stake_bal.rao)
        if total_rao <= 0:
            return 0, 0, 0.0, None

        total_tao = 0.0
        sn: Optional[Any] = None

        # Fast path: caller already has the subnet object.
        if subnet is not None:
            try:
                sn = subnet
                total_tao = float(sn.alpha_to_tao(stake_bal).tao)
            except Exception:
                total_tao = 0.0

        # Fast path: shared price cache (populated by the mempool monitor
        # each block). Linear approximation is sufficient here — the ratio is
        # only used to derive ``unstake_rao`` for partial sells, and the chain
        # does the real AMM swap under our slippage tolerance.
        if total_tao <= 0:
            tao_per_alpha = _price_cache.get_fresh_tao_per_alpha(netuid)
            if tao_per_alpha is not None and tao_per_alpha > 0:
                total_tao = (total_rao / 1e9) * tao_per_alpha

        # Slow fallback: RPC for full DynamicInfo to get the real AMM output.
        if total_tao <= 0:
            try:
                sn = self._subtensor.subnet(netuid=netuid)
                total_tao = float(sn.alpha_to_tao(stake_bal).tao)
            except Exception:
                try:
                    total_tao = float(stake_bal.tao)
                except Exception:
                    total_tao = float(Balance.from_rao(total_rao).tao)

        if total_tao <= 0:
            total_tao = float(Balance.from_rao(total_rao).tao)

        req_tao = min(float(amount_tao), total_tao)
        if req_tao <= 0:
            return 0, total_rao, 0.0, sn

        ratio = req_tao / total_tao
        if ratio >= 0.999999:
            unstake_rao = total_rao
        else:
            unstake_rao = int(total_rao * ratio)
            unstake_rao = min(total_rao, max(1, unstake_rao))

        return unstake_rao, total_rao, req_tao, sn

    @staticmethod
    def _unstake_is_full_position(unstake_rao: int, total_rao: int) -> bool:
        """True when the sell consumes essentially all stake (incl. dust tail)."""
        if total_rao <= 0:
            return False
        dust_rao = 1_000_000  # ~0.001 α — matches wallet dust filter
        return unstake_rao >= total_rao or (total_rao - unstake_rao) <= dust_rao

    def _build_sell_batch_inner_calls(
        self,
        positions: list[tuple[int, str, float]],
        tolerance: float,
        no_slippage: bool,
    ) -> tuple[list[Any], list[tuple[int, str]], list[PositionTradeResult]]:
        """Build ``Utility.force_batch`` inner calls for batch sell (MEV and non-MEV)."""
        from bittensor.core.extrinsics.pallets import SubtensorModule

        inner_calls: list[Any] = []
        sold_positions: list[tuple[int, str]] = []
        skipped: list[PositionTradeResult] = []

        for netuid, hotkey, amount_tao in positions:
            validator_hk = (hotkey or "").strip() or self._hotkey_ss58
            unstake_rao, total_rao, _req_tao, subnet = (
                self._unstake_alpha_rao_for_tao_amount(
                    netuid, validator_hk, amount_tao
                )
            )
            if total_rao <= 0:
                skipped.append(
                    PositionTradeResult(
                        netuid=netuid,
                        hotkey=validator_hk,
                        success=False,
                        message=f"No stake on netuid {netuid} for this hotkey",
                    )
                )
                continue
            if unstake_rao <= 0:
                skipped.append(
                    PositionTradeResult(
                        netuid=netuid,
                        hotkey=validator_hk,
                        success=False,
                        message="Unstake amount too small or invalid",
                    )
                )
                continue

            is_full = self._unstake_is_full_position(unstake_rao, total_rao)

            if no_slippage:
                inner_calls.append(
                    SubtensorModule(self._subtensor).remove_stake(
                        netuid=netuid,
                        hotkey=validator_hk,
                        amount_unstaked=unstake_rao,
                    )
                )
            else:
                limit_price = self._get_limit_price(
                    netuid, tolerance, "sell", subnet=subnet
                )
                if is_full:
                    inner_calls.append(
                        SubtensorModule(self._subtensor).remove_stake_full_limit(
                            netuid=netuid,
                            hotkey=validator_hk,
                            limit_price=limit_price,
                        )
                    )
                else:
                    inner_calls.append(
                        SubtensorModule(self._subtensor).remove_stake_limit(
                            netuid=netuid,
                            hotkey=validator_hk,
                            amount_unstaked=unstake_rao,
                            limit_price=limit_price,
                            allow_partial=True,
                        )
                    )
            sold_positions.append((netuid, validator_hk))

        return inner_calls, sold_positions, skipped

    def _full_stake_alpha_rao(
        self,
        netuid: int,
        validator_hotkey: str,
    ) -> tuple[int, Optional[Any]]:
        """Return total alpha RAO staked on ``netuid`` for ``validator_hotkey``."""
        stake_bal = self._subtensor.get_stake(
            coldkey_ss58=self._wallet.coldkeypub.ss58_address,
            hotkey_ss58=validator_hotkey,
            netuid=netuid,
        )
        total_rao = int(stake_bal.rao)
        if total_rao <= 0:
            return 0, None
        sn: Optional[Any] = None
        try:
            sn = self._subtensor.subnet(netuid=netuid)
        except Exception:
            pass
        return total_rao, sn

    def _build_swap_batch_inner_calls(
        self,
        positions: list[tuple[int, str, float]],
        dest_netuid: int,
        tolerance: float,
        no_slippage: bool,
    ) -> tuple[list[Any], list[tuple[int, str]], list[PositionTradeResult]]:
        """Build ``Utility.force_batch`` inner calls for batch swap (MEV and non-MEV).

        ``positions`` entries are ``(origin_netuid, hotkey_ss58, amount_tao)``.
        """
        from bittensor.core.extrinsics.pallets import SubtensorModule

        inner_calls: list[Any] = []
        swapped_positions: list[tuple[int, str]] = []
        skipped: list[PositionTradeResult] = []

        for origin_netuid, hotkey, amount_tao in positions:
            validator_hk = (hotkey or "").strip() or self._hotkey_ss58
            if origin_netuid == dest_netuid:
                skipped.append(
                    PositionTradeResult(
                        netuid=origin_netuid,
                        hotkey=validator_hk,
                        success=False,
                        message=f"Origin netuid {origin_netuid} equals destination",
                    )
                )
                continue

            if float(amount_tao) < _MIN_SWAP_TAO:
                skipped.append(
                    PositionTradeResult(
                        netuid=origin_netuid,
                        hotkey=validator_hk,
                        success=False,
                        message=(
                            f"Swap amount {amount_tao:.4f}τ below minimum "
                            f"{_MIN_SWAP_TAO:.4f}τ"
                        ),
                    )
                )
                continue

            unstake_rao, total_rao, req_tao, origin_sn = (
                self._unstake_alpha_rao_for_tao_amount(
                    origin_netuid, validator_hk, amount_tao, subnet=None
                )
            )
            if total_rao <= 0:
                skipped.append(
                    PositionTradeResult(
                        netuid=origin_netuid,
                        hotkey=validator_hk,
                        success=False,
                        message=f"No stake on netuid {origin_netuid} for this hotkey",
                    )
                )
                continue
            if unstake_rao <= 0:
                skipped.append(
                    PositionTradeResult(
                        netuid=origin_netuid,
                        hotkey=validator_hk,
                        success=False,
                        message=(
                            f"Swap amount too small (~{amount_tao:.4f}τ "
                            f"→ {req_tao:.4f}τ requested)"
                        ),
                    )
                )
                continue

            try:
                if no_slippage:
                    inner_calls.append(
                        SubtensorModule(self._subtensor).swap_stake(
                            hotkey=validator_hk,
                            origin_netuid=origin_netuid,
                            destination_netuid=dest_netuid,
                            alpha_amount=unstake_rao,
                        )
                    )
                else:
                    dest_sn = None
                    try:
                        dest_sn = self._subtensor.subnet(netuid=dest_netuid)
                    except Exception:
                        pass
                    limit_price_rao = self._get_swap_cross_limit_price_rao(
                        origin_netuid,
                        dest_netuid,
                        tolerance,
                        origin_subnet=origin_sn,
                        dest_subnet=dest_sn,
                    )
                    if limit_price_rao == 0:
                        inner_calls.append(
                            SubtensorModule(self._subtensor).swap_stake(
                                hotkey=validator_hk,
                                origin_netuid=origin_netuid,
                                destination_netuid=dest_netuid,
                                alpha_amount=unstake_rao,
                            )
                        )
                    else:
                        inner_calls.append(
                            SubtensorModule(self._subtensor).swap_stake_limit(
                                hotkey=validator_hk,
                                origin_netuid=origin_netuid,
                                destination_netuid=dest_netuid,
                                alpha_amount=unstake_rao,
                                limit_price=limit_price_rao,
                                allow_partial=True,
                            )
                        )
                swapped_positions.append((origin_netuid, validator_hk))
            except Exception as exc:
                skipped.append(
                    PositionTradeResult(
                        netuid=origin_netuid,
                        hotkey=validator_hk,
                        success=False,
                        message=str(exc),
                    )
                )

        return inner_calls, swapped_positions, skipped

    def _submit_mev_shield_calls(
        self,
        inner_calls: list[Any],
    ) -> TradeResult:
        """Submit one or more calls via MEV Shield (``force_batch`` when len > 1).

        Non-blocking by default (see ``_mev_submit_wait_kwargs``): returns as soon
        as the encrypted extrinsic is submitted so the caller holds the lock only
        briefly instead of across inclusion + reveal.
        """
        _, _, _, submit_calls_mev_shield = _get_mev_shield_submit_fns()
        mev_kw = _mev_shield_period_kw()
        raise_kw = _mev_raise_error_kw()
        max_mev = _mev_max_retries()
        t0 = time.time()
        resp = None
        for attempt in range(max_mev):
            self._maybe_fresh_subtensor_for_mev(attempt)
            self._log_mev_coldkey_nonce(attempt)
            try:
                resp = submit_calls_mev_shield(
                    self._subtensor,
                    self._wallet,
                    inner_calls,
                    **mev_kw,
                    **raise_kw,
                    **_mev_submit_wait_kwargs(),
                )
            except Exception as e:
                if attempt < max_mev - 1 and _is_tx_outdated_error(e):
                    logger.warning(
                        "MEV Shield batch retry after outdated: %s", e
                    )
                    self._prepare_mev_retry(attempt)
                    continue
                raise
            if not getattr(resp, "success", False):
                err = _extrinsic_response_error_message(resp)
                if (
                    attempt < max_mev - 1
                    and _extrinsic_response_suggests_outdated(resp)
                ):
                    logger.warning(
                        "MEV Shield batch retry after outdated response: %s", err
                    )
                    self._prepare_mev_retry(attempt)
                    continue
                elapsed = (time.time() - t0) * 1000
                logger.error("MEV Shield batch failed (%.0fms): %s", elapsed, err)
                return TradeResult(False, err, elapsed_ms=elapsed)
            break
        assert resp is not None
        elapsed = (time.time() - t0) * 1000
        n = len(inner_calls)
        msg = (
            f"Submitted MEV Shield batch sell: {n} position(s) in one tx ({elapsed:.0f}ms)"
            if n > 1
            else f"Submitted MEV Shield sell ({elapsed:.0f}ms)"
        )
        return TradeResult(
            success=True,
            message=msg,
            extrinsic_hash=_extrinsic_hash_from_mev_response(resp),
            elapsed_ms=elapsed,
        )

    def _sell_mev_shield_position(
        self,
        netuid: int,
        validator_hk: str,
        unstake_rao: int,
        total_rao: int,
        req_tao: float,
        tolerance: float,
        no_slippage: bool,
        *,
        allow_partial: bool = True,
    ) -> TradeResult:
        """Submit one MEV Shield unstake. Caller must hold ``self._lock`` briefly.

        Non-blocking by default (see ``_mev_submit_wait_kwargs``)."""
        from bittensor.utils.balance import Balance

        t0 = time.time()
        is_full = self._unstake_is_full_position(unstake_rao, total_rao)
        _, unstake_all_mev_shield, unstake_mev_shield, _ = _get_mev_shield_submit_fns()
        mev_kw = _mev_shield_period_kw()
        raise_kw = _mev_raise_error_kw()
        max_mev = _mev_max_retries()
        resp = None
        for attempt in range(max_mev):
            self._maybe_fresh_subtensor_for_mev(attempt)
            self._log_mev_coldkey_nonce(attempt)
            try:
                if is_full:
                    resp = unstake_all_mev_shield(
                        self._subtensor,
                        self._wallet,
                        netuid,
                        validator_hk,
                        None if no_slippage else tolerance,
                        **mev_kw,
                        **raise_kw,
                        **_mev_submit_wait_kwargs(),
                    )
                else:
                    alpha_amt = Balance.from_rao(unstake_rao, netuid=netuid)
                    resp = unstake_mev_shield(
                        self._subtensor,
                        self._wallet,
                        netuid,
                        validator_hk,
                        alpha_amt,
                        allow_partial_stake=allow_partial,
                        rate_tolerance=tolerance,
                        safe_unstaking=not no_slippage,
                        **mev_kw,
                        **raise_kw,
                        **_mev_submit_wait_kwargs(),
                    )
            except Exception as e:
                if attempt < max_mev - 1 and _is_tx_outdated_error(e):
                    logger.warning("sell MEV Shield retry after outdated: %s", e)
                    self._prepare_mev_retry(attempt)
                    continue
                raise
            if not getattr(resp, "success", False):
                err = _extrinsic_response_error_message(resp)
                if (
                    attempt < max_mev - 1
                    and _extrinsic_response_suggests_outdated(resp)
                ):
                    logger.warning(
                        "sell MEV Shield retry after outdated response: %s", err
                    )
                    self._prepare_mev_retry(attempt)
                    continue
                elapsed = (time.time() - t0) * 1000
                logger.error(
                    "sell MEV Shield failed (%.0fms) n%d: %s", elapsed, netuid, err
                )
                return TradeResult(False, err, elapsed_ms=elapsed)
            break
        assert resp is not None
        elapsed = (time.time() - t0) * 1000
        pct = 100.0 * unstake_rao / total_rao if total_rao else 0.0
        logger.info(
            "sell MEV Shield submitted (%.0fms) netuid=%d unstake_rao=%d (~%.2f%%)",
            elapsed,
            netuid,
            unstake_rao,
            pct,
        )
        base = (
            f"Submitted sell n{netuid} ~{req_tao:.4f}τ ({pct:.1f}% of stake) ({elapsed:.0f}ms)"
            if not is_full
            else f"Submitted sell n{netuid} full position ({elapsed:.0f}ms)"
        )
        return TradeResult(
            success=True,
            message=f"{base} [MEV Shield]",
            extrinsic_hash=_extrinsic_hash_from_mev_response(resp),
            elapsed_ms=elapsed,
        )

    def sell(
        self,
        netuid: int,
        amount_tao: float,
        slippage_pct: float = 1.0,
        hotkey: Optional[str] = None,
        allow_partial: bool = True,
        no_slippage: bool = False,
        mev_shield: bool = False,
    ) -> TradeResult:
        from bittensor.core.extrinsics.pallets import SubtensorModule

        t0 = time.time()
        tolerance = slippage_pct / 100.0

        validator_hk = (hotkey or "").strip() or self._hotkey_ss58

        try:
            with self._lock:
                unstake_rao, total_rao, req_tao, subnet = self._unstake_alpha_rao_for_tao_amount(
                    netuid, validator_hk, amount_tao
                )
                if total_rao <= 0:
                    return TradeResult(
                        False,
                        f"No stake on netuid {netuid} for this hotkey",
                        elapsed_ms=(time.time() - t0) * 1000,
                    )
                if unstake_rao <= 0:
                    return TradeResult(
                        False,
                        "Unstake amount too small or invalid",
                        elapsed_ms=(time.time() - t0) * 1000,
                    )

                is_full = self._unstake_is_full_position(unstake_rao, total_rao)

                sell_limit_price: Optional[float] = None
                if not no_slippage:
                    sell_limit_price = float(
                        self._get_limit_price(netuid, tolerance, "sell", subnet=subnet)
                    )

                if mev_shield:
                    return self._sell_mev_shield_position(
                        netuid,
                        validator_hk,
                        unstake_rao,
                        total_rao,
                        req_tao,
                        tolerance,
                        no_slippage,
                        allow_partial=allow_partial,
                    )

                if no_slippage:
                    call = SubtensorModule(self._subtensor).remove_stake(
                        netuid=netuid,
                        hotkey=validator_hk,
                        amount_unstaked=unstake_rao,
                    )
                else:
                    assert sell_limit_price is not None
                    if is_full:
                        call = SubtensorModule(self._subtensor).remove_stake_full_limit(
                            netuid=netuid,
                            hotkey=validator_hk,
                            limit_price=sell_limit_price,
                        )
                    else:
                        call = SubtensorModule(self._subtensor).remove_stake_limit(
                            netuid=netuid,
                            hotkey=validator_hk,
                            amount_unstaked=unstake_rao,
                            limit_price=sell_limit_price,
                            allow_partial=allow_partial,
                        )

                self._submit_fast(call)
        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            logger.error("sell failed (%.0fms): %s", elapsed, e)
            return TradeResult(False, str(e), elapsed_ms=elapsed)

        elapsed = (time.time() - t0) * 1000
        pct = 100.0 * unstake_rao / total_rao if total_rao else 0.0
        logger.info(
            "sell submitted (%.0fms) netuid=%d unstake_rao=%d (~%.2f%% of position, ~%.4f TAO)",
            elapsed,
            netuid,
            unstake_rao,
            pct,
            req_tao,
        )
        msg = (
            f"Submitted sell n{netuid} ~{req_tao:.4f}τ ({pct:.1f}% of stake) ({elapsed:.0f}ms)"
            if not is_full
            else f"Submitted sell n{netuid} full position ({elapsed:.0f}ms)"
        )
        return TradeResult(success=True, message=msg, elapsed_ms=elapsed)

    def _build_buy_batch_inner_calls(
        self,
        netuids: list[int],
        amount_tao: float,
        tolerance: float,
        no_slippage: bool,
        allow_partial: bool = True,
    ) -> tuple[list[Any], list[int], list[PositionTradeResult]]:
        """Build ``Utility.force_batch`` inner calls for batch buy (MEV and non-MEV)."""
        from bittensor.core.extrinsics.pallets import SubtensorModule
        from bittensor.utils.balance import Balance

        inner_calls: list[Any] = []
        bought_netuids: list[int] = []
        skipped: list[PositionTradeResult] = []
        amount_rao = Balance.from_tao(amount_tao).rao

        for netuid in netuids:
            try:
                if no_slippage:
                    inner_calls.append(
                        SubtensorModule(self._subtensor).add_stake(
                            netuid=netuid,
                            hotkey=self._hotkey_ss58,
                            amount_staked=amount_rao,
                        )
                    )
                else:
                    limit_price_rao = self._get_limit_price(
                        netuid, tolerance, "buy"
                    )
                    inner_calls.append(
                        SubtensorModule(self._subtensor).add_stake_limit(
                            hotkey=self._hotkey_ss58,
                            netuid=netuid,
                            amount_staked=amount_rao,
                            limit_price=limit_price_rao,
                            allow_partial=allow_partial,
                        )
                    )
                bought_netuids.append(netuid)
            except Exception as exc:
                skipped.append(
                    PositionTradeResult(
                        netuid=netuid,
                        hotkey=self._hotkey_ss58,
                        success=False,
                        message=str(exc),
                    )
                )

        return inner_calls, bought_netuids, skipped

    def buy_batch(
        self,
        netuids: list[int],
        amount_tao: float,
        slippage_pct: float = 1.0,
        no_slippage: bool = False,
        mev_shield: bool = False,
        decoy_tao: float = 0.0,
        allow_partial: bool = True,
    ) -> TradeResult:
        """Stake the same TAO amount on many netuids in one ``force_batch`` tx."""
        if not netuids:
            return TradeResult(False, "No netuids to buy", elapsed_ms=0.0)
        if len(netuids) == 1:
            return self.buy(
                netuids[0],
                amount_tao,
                slippage_pct,
                no_slippage=no_slippage,
                mev_shield=mev_shield,
                decoy_tao=decoy_tao,
                allow_partial=allow_partial,
            )

        t0 = time.time()
        tolerance = slippage_pct / 100.0
        inner_calls: list[Any] = []
        bought_netuids: list[int] = []
        bought_labels: list[str] = []

        if mev_shield:
            try:
                with self._lock:
                    inner_calls, bought_netuids, skipped = (
                        self._build_buy_batch_inner_calls(
                            netuids,
                            amount_tao,
                            tolerance,
                            no_slippage,
                            allow_partial=allow_partial,
                        )
                    )
                    if not inner_calls:
                        elapsed = (time.time() - t0) * 1000
                        return TradeResult(
                            False,
                            "No valid netuids to buy",
                            elapsed_ms=elapsed,
                            positions=skipped,
                        )
                    r = self._submit_mev_shield_calls(inner_calls)
            except Exception as e:
                elapsed = (time.time() - t0) * 1000
                logger.error("buy_batch MEV failed (%.0fms): %s", elapsed, e)
                return TradeResult(False, str(e), elapsed_ms=elapsed)

            elapsed = (time.time() - t0) * 1000
            position_results: list[PositionTradeResult] = list(skipped)
            batch_note = (
                r.message
                if len(bought_netuids) == 1
                else "included in encrypted force_batch"
            )
            for netuid in bought_netuids:
                position_results.append(
                    PositionTradeResult(
                        netuid=netuid,
                        hotkey=self._hotkey_ss58,
                        success=r.success,
                        message=batch_note,
                        extrinsic_hash=r.extrinsic_hash,
                    )
                )
            if not r.success:
                return TradeResult(
                    False,
                    r.message,
                    elapsed_ms=elapsed,
                    positions=position_results,
                )
            n_ok = len(bought_netuids)
            return TradeResult(
                True,
                f"MEV Shield: submitted buy on {n_ok} subnet(s) in one encrypted batch ({elapsed:.0f}ms)",
                extrinsic_hash=r.extrinsic_hash,
                elapsed_ms=elapsed,
                positions=position_results,
            )

        try:
            with self._lock:
                inner_calls, bought_netuids, _skipped = (
                    self._build_buy_batch_inner_calls(
                        netuids,
                        amount_tao,
                        tolerance,
                        no_slippage,
                        allow_partial=allow_partial,
                    )
                )
                bought_labels = [f"n{n}" for n in bought_netuids]

                if not inner_calls:
                    return TradeResult(
                        False,
                        "No valid netuids to buy",
                        elapsed_ms=(time.time() - t0) * 1000,
                    )

                if len(inner_calls) == 1:
                    self._submit_fast(inner_calls[0])
                else:
                    batch_call = self._subtensor.substrate.compose_call(
                        call_module="Utility",
                        call_function="force_batch",
                        call_params={"calls": inner_calls},
                    )
                    self._submit_fast(batch_call)
        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            logger.error("buy_batch failed (%.0fms): %s", elapsed, e)
            return TradeResult(False, str(e), elapsed_ms=elapsed)

        if not mev_shield and decoy_tao and decoy_tao > 0 and bought_netuids:
            self._fire_decoy_sell_async(bought_netuids[0], float(decoy_tao))

        elapsed = (time.time() - t0) * 1000
        n = len(inner_calls)
        msg = (
            f"Submitted batch buy: {amount_tao}τ on {n} subnet(s) in one tx ({elapsed:.0f}ms)"
            if n > 1
            else f"Submitted buy {bought_labels[0]} ({elapsed:.0f}ms)"
        )
        logger.info("buy_batch %s", msg)
        return TradeResult(success=True, message=msg, elapsed_ms=elapsed)

    def sell_all_batch(
        self,
        positions: list[tuple[int, str, float]],
        slippage_pct: float = 1.0,
        no_slippage: bool = False,
        mev_shield: bool = False,
    ) -> TradeResult:
        """Unstake many (netuid, hotkey, amount_tao) positions.

        ``positions`` entries are ``(netuid, hotkey_ss58, amount_tao)``.
        Without MEV Shield: one ``Utility.force_batch`` extrinsic.
        With MEV Shield: one encrypted extrinsic wrapping the same ``force_batch``
        so all selected positions unstake together on reveal.
        """
        if not positions:
            return TradeResult(
                False,
                "No positions to sell",
                elapsed_ms=0.0,
            )

        t0 = time.time()
        tolerance = slippage_pct / 100.0

        if mev_shield:
            try:
                with self._lock:
                    inner_calls, sold_positions, skipped = (
                        self._build_sell_batch_inner_calls(
                            positions, tolerance, no_slippage
                        )
                    )
                    if not inner_calls:
                        elapsed = (time.time() - t0) * 1000
                        return TradeResult(
                            False,
                            "No valid stake positions to sell",
                            elapsed_ms=elapsed,
                            positions=skipped,
                        )
                    r = self._submit_mev_shield_calls(inner_calls)
            except Exception as e:
                elapsed = (time.time() - t0) * 1000
                logger.error("sell_all_batch MEV failed (%.0fms): %s", elapsed, e)
                return TradeResult(False, str(e), elapsed_ms=elapsed)

            elapsed = (time.time() - t0) * 1000
            position_results: list[PositionTradeResult] = list(skipped)
            batch_note = (
                r.message
                if len(sold_positions) == 1
                else "included in encrypted force_batch"
            )
            for netuid, hk in sold_positions:
                position_results.append(
                    PositionTradeResult(
                        netuid=netuid,
                        hotkey=hk,
                        success=r.success,
                        message=batch_note,
                        extrinsic_hash=r.extrinsic_hash,
                    )
                )
            if not r.success:
                return TradeResult(
                    False,
                    r.message,
                    elapsed_ms=elapsed,
                    positions=position_results,
                )
            n_ok = len(sold_positions)
            return TradeResult(
                True,
                f"MEV Shield: submitted {n_ok} position(s) in one encrypted batch ({elapsed:.0f}ms)",
                extrinsic_hash=r.extrinsic_hash,
                elapsed_ms=elapsed,
                positions=position_results,
            )

        try:
            with self._lock:
                substrate = self._subtensor.substrate
                inner_calls, sold_positions, _skipped = (
                    self._build_sell_batch_inner_calls(
                        positions, tolerance, no_slippage
                    )
                )
                sold_labels = [f"n{n}" for n, _ in sold_positions]

                if not inner_calls:
                    return TradeResult(
                        False,
                        "No valid stake positions to sell",
                        elapsed_ms=(time.time() - t0) * 1000,
                    )

                if len(inner_calls) == 1:
                    self._submit_fast(inner_calls[0])
                else:
                    batch_call = substrate.compose_call(
                        call_module="Utility",
                        call_function="force_batch",
                        call_params={"calls": inner_calls},
                    )
                    self._submit_fast(batch_call)
        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            logger.error("sell_all_batch failed (%.0fms): %s", elapsed, e)
            return TradeResult(False, str(e), elapsed_ms=elapsed)

        elapsed = (time.time() - t0) * 1000
        n = len(inner_calls)
        msg = (
            f"Submitted batch sell: {n} position(s) in one tx ({elapsed:.0f}ms)"
            if n > 1
            else f"Submitted sell {sold_labels[0]} ({elapsed:.0f}ms)"
        )
        logger.info("sell_all_batch %s", msg)
        return TradeResult(success=True, message=msg, elapsed_ms=elapsed)

    def swap_batch(
        self,
        positions: list[tuple[int, str, float]],
        dest_netuid: int,
        slippage_pct: float = 5.0,
        no_slippage: bool = False,
        mev_shield: bool = False,
    ) -> TradeResult:
        """Swap stake from many origin netuids into one destination netuid.

        ``positions`` entries are ``(origin_netuid, hotkey_ss58, amount_tao)``.
        Without MEV Shield: one ``Utility.force_batch`` extrinsic.
        With MEV Shield: one encrypted extrinsic wrapping the same ``force_batch``.
        """
        if not positions:
            return TradeResult(
                False,
                "No positions to swap",
                elapsed_ms=0.0,
            )
        if dest_netuid < 0 or dest_netuid > 65535:
            return TradeResult(
                False,
                f"Invalid destination netuid: {dest_netuid}",
                elapsed_ms=0.0,
            )

        t0 = time.time()
        tolerance = slippage_pct / 100.0

        if mev_shield:
            try:
                with self._lock:
                    inner_calls, swapped_positions, skipped = (
                        self._build_swap_batch_inner_calls(
                            positions, dest_netuid, tolerance, no_slippage
                        )
                    )
                    if not inner_calls:
                        elapsed = (time.time() - t0) * 1000
                        return TradeResult(
                            False,
                            self._swap_batch_failure_message(skipped),
                            elapsed_ms=elapsed,
                            positions=skipped,
                        )
                    r = self._submit_mev_shield_calls(inner_calls)
            except Exception as e:
                elapsed = (time.time() - t0) * 1000
                logger.error("swap_batch MEV failed (%.0fms): %s", elapsed, e)
                return TradeResult(False, str(e), elapsed_ms=elapsed)

            elapsed = (time.time() - t0) * 1000
            position_results: list[PositionTradeResult] = list(skipped)
            batch_note = (
                r.message
                if len(swapped_positions) == 1
                else "included in encrypted force_batch"
            )
            for netuid, hk in swapped_positions:
                position_results.append(
                    PositionTradeResult(
                        netuid=netuid,
                        hotkey=hk,
                        success=r.success,
                        message=batch_note,
                        extrinsic_hash=r.extrinsic_hash,
                    )
                )
            if not r.success:
                return TradeResult(
                    False,
                    r.message,
                    elapsed_ms=elapsed,
                    positions=position_results,
                )
            n_ok = len(swapped_positions)
            return TradeResult(
                True,
                f"MEV Shield: submitted swap of {n_ok} position(s) → n{dest_netuid} in one encrypted batch ({elapsed:.0f}ms)",
                extrinsic_hash=r.extrinsic_hash,
                elapsed_ms=elapsed,
                positions=position_results,
            )

        try:
            with self._lock:
                substrate = self._subtensor.substrate
                inner_calls, swapped_positions, _skipped = (
                    self._build_swap_batch_inner_calls(
                        positions, dest_netuid, tolerance, no_slippage
                    )
                )
                swapped_labels = [f"n{n}" for n, _ in swapped_positions]

                if not inner_calls:
                    skipped_local = _skipped if _skipped else []
                    return TradeResult(
                        False,
                        self._swap_batch_failure_message(skipped_local),
                        elapsed_ms=(time.time() - t0) * 1000,
                        positions=skipped_local,
                    )

                if len(inner_calls) == 1:
                    self._submit_fast(inner_calls[0])
                else:
                    batch_call = substrate.compose_call(
                        call_module="Utility",
                        call_function="force_batch",
                        call_params={"calls": inner_calls},
                    )
                    self._submit_fast(batch_call)
        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            logger.error("swap_batch failed (%.0fms): %s", elapsed, e)
            return TradeResult(False, str(e), elapsed_ms=elapsed)

        elapsed = (time.time() - t0) * 1000
        n = len(inner_calls)
        msg = (
            f"Submitted batch swap: {n} position(s) → n{dest_netuid} in one tx ({elapsed:.0f}ms)"
            if n > 1
            else f"Submitted swap {swapped_labels[0]} → n{dest_netuid} ({elapsed:.0f}ms)"
        )
        logger.info("swap_batch %s", msg)
        return TradeResult(success=True, message=msg, elapsed_ms=elapsed)


_trader_1: Optional[Trader] = None
_trader_init_error: Optional[str] = None
_trader_singleton_lock = threading.Lock()


def trader_last_init_error() -> Optional[str]:
    """Set when ``get_trader()`` failed to construct ``Trader`` (see server logs)."""
    return _trader_init_error


def get_trader(substrate: Optional[Any] = None) -> Optional[Trader]:
    """Lazy-init singleton using env vars. Returns None if construction fails (error in logs)."""
    global _trader_1, _trader_init_error
    if _trader_1 is not None:
        return _trader_1
    if _trader_init_error is not None:
        return None

    with _trader_singleton_lock:
        if _trader_1 is not None:
            return _trader_1
        if _trader_init_error is not None:
            return None

        wallet_name = os.getenv("BTCLI_WALLET", "trading")
        hotkey_name = os.getenv("BTCLI_HOTKEY", "m-1")
        network = os.getenv("NETWORK", "finney")
        delegate_hk = os.getenv("TRADE_HOTKEY", "").strip() or None

        try:
            _trader_1 = Trader(wallet_name, hotkey_name, network, delegate_hotkey_ss58=delegate_hk)
            _trader_init_error = None
            return _trader_1
        except Exception as e:
            _trader_init_error = str(e)
            logger.error(
                "Trader init failed (wallet=%s hotkey=%s): %s",
                wallet_name,
                hotkey_name,
                e,
                exc_info=True,
            )
            return None
