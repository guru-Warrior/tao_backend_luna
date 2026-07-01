#!/usr/bin/env python3
"""
WebSocket server: mempool + last block snapshots; My balance is pushed on the same `/ws` when the chain head advances (see monitor_thread + fetch_wallet).
Run: python -m uvicorn mempool_server:app --host 0.0.0.0 --port 8001
Requires one of: WALLET_ADDRESS / COLDKEY_ADDRESS, SEED_PHRASE_1, or WALLET_NAME (+ wallet files) for live wallet panel (optional).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, Set

logger = logging.getLogger(__name__)

import urllib.request

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from pydantic import BaseModel

from app_auth import (
    COOKIE_NAME,
    MAX_AGE_SEC,
    AppAuthMiddleware,
    LOGIN_HTML,
    auth_enabled,
    create_session_token,
    is_authenticated_request,
    password_matches,
    websocket_authenticated,
)
from check_balance import WalletChecker, resolve_wallet_address_from_env
from config import (
    BACKPROP_SUBNETS_URL,
    BOT_AUTOSTART,
    BOT_ENV_PATH,
    BOT_STATE_PATH,
    BOT_WORKER_SCRIPT,
    FINNEY_WS,
    FRONTEND_DIST,
    MEMPOOL_HEAD_POLL_SEC,
    MEMPOOL_MAX_CATCHUP_BLOCKS,
    MEMPOOL_POLL_BUSY_SEC,
    MEMPOOL_POLL_IDLE_SEC,
    POOL_POLL_INTERVAL_SEC,
    cors_allow_origins_and_credentials,
)
from buy_bot import BotManager, BotRule
from faker import (
    faker_last_init_error,
    get_faker,
    read_env_defaults as _faker_env_defaults,
)
from mempool.chain_utils import block_number_from_chain_get_header, decode_block_number_field
from mempool.monitor import MempoolMonitor
import stake_tracker_bridge as st_bridge
from trader import get_trader, trader_last_init_error

ASYNC_QUEUE: asyncio.Queue[Optional[Dict[str, Any]]] = asyncio.Queue()
STOP = threading.Event()

last_snapshot_json: Optional[str] = None
last_wallet_json: Optional[str] = None
_prev_snapshot_fp: Optional[tuple] = None
# Monitor thread only: skip queue/async wake when snapshot unchanged (same fingerprint as consumer).
_monitor_thread_last_fp: Optional[tuple] = None
# Set by ``ws_mempool`` when a client (re)connects so that the next
# ``monitor_thread`` poll guarantees a fresh broadcast even if the snapshot
# fingerprint hasn't drifted. Without this, a reconnecting client can receive
# only ``last_snapshot_json`` and then wait an entire mempool cycle for the
# next fingerprint change — which in a quiet pool never arrives, and stale
# rows from before the disconnect linger on screen.
_force_broadcast_event: threading.Event = threading.Event()

checker: Optional[WalletChecker] = None
wallet_fetch_lock: Optional[asyncio.Lock] = None
_monitor: Optional[MempoolMonitor] = None

# Buy-bot manager — owns the worker subprocess, persisted rules, and the env
# file the worker polls. Created lazily in lifespan (needs the running event
# loop so `asyncio.Lock` is safe to instantiate).
bot_manager: Optional[BotManager] = None

# Pool info cache: netuid -> {taoInPool, name}; filled from BACKPROP_SUBNETS_URL (full list, one request)
_pool_cache: Dict[int, Dict[str, Any]] = {}


def _json_dumps_compact(obj: Any) -> str:
    """Fast path: orjson when installed; stdlib json fallback."""
    try:
        import orjson

        return orjson.dumps(obj).decode("utf-8")
    except (ImportError, TypeError, ValueError):
        return json.dumps(obj, separators=(",", ":"))


def _snapshot_fingerprint(snap: Dict[str, Any]) -> tuple:
    """Fingerprint including balance/stake data so bg-fetched updates get broadcast.

    `txHash` is included per row so that brand-new mempool entries always
    change the fingerprint — otherwise two different txs that happen to share
    (address, netuid, amount, balances) could be deduped silently.

    ``type``/``method``/``slippagePct`` are also included: an extrinsic can
    be re-decoded into a different UI type/method (e.g. when ``proxy_real`` is
    resolved on a later tick) without any of the balance fields moving, and
    the old fingerprint would suppress that refresh.
    """
    return (
        snap.get("currentBlock"),
        snap.get("mempoolCount"),
        tuple(
            (
                r.get("txHash"),
                r.get("type"),
                r.get("method"),
                r.get("address"),
                r.get("netuid"),
                r.get("amount"),
                r.get("freeTao"),
                r.get("alphaTao"),
                r.get("slippagePct"),
            )
            for r in snap.get("mempool", [])
        ),
        tuple(
            (
                r.get("extrinsicIdx"),
                r.get("address"),
                r.get("netuid"),
                r.get("amount"),
                r.get("type"),
                r.get("method"),
                r.get("success"),
                r.get("freeTao"),
                r.get("alphaTao"),
            )
            for r in snap.get("lastBlock", [])
        ),
        tuple(
            (
                r.get("block"),
                r.get("from"),
                r.get("to"),
                r.get("amountTao"),
                r.get("extrinsicIdx"),
            )
            for r in snap.get("transferHistory", [])
        ),
    )


def _put(msg: Dict[str, Any]) -> None:
    """Thread-safe enqueue into the asyncio Queue."""
    try:
        ASYNC_QUEUE.put_nowait(msg)
    except asyncio.QueueFull:
        pass


def _catch_up_blocks_to_target(mon: MempoolMonitor, target_bn: int, substrate=None) -> tuple[int, bool]:
    """Process ``_last_seen_block + 1 .. target_bn`` (capped). Returns ``(last_bn, advanced)``.

    ``substrate`` (when given) is forwarded to ``process_new_block`` so the
    dedicated block-processing thread runs every RPC on its own connection.
    """
    prev = mon._last_seen_block or 0
    if target_bn <= prev:
        return prev, False
    gap = target_bn - prev
    if gap > MEMPOOL_MAX_CATCHUP_BLOCKS:
        logger.warning(
            "Catch-up gap %s blocks exceeds max %s; processing first chunk only",
            gap,
            MEMPOOL_MAX_CATCHUP_BLOCKS,
        )
        target_bn = prev + MEMPOOL_MAX_CATCHUP_BLOCKS
    for b in range(prev + 1, target_bn + 1):
        mon.process_new_block(b, substrate=substrate)
    return target_bn, True


def _poll_chain_head_catchup(mon: MempoolMonitor, current_block: int, substrate=None) -> tuple[int, bool]:
    """Read best head via RPC; catch up to tip (same block-number parsing as bootstrap)."""
    import chain_head_cache as _head_cache
    sub = substrate or mon.substrate
    try:
        header = sub.rpc_request("chain_getHeader", [])
        head_bn = block_number_from_chain_get_header(header)
        if head_bn is None:
            logger.warning("chain_getHeader returned no parsable block number")
            return current_block, False
        # Keep trader's era fast-path warm even when the subscription WS is
        # flaky — this branch polls the head itself so we know the number.
        _head_cache.put(head_bn)
        last_bn, advanced = _catch_up_blocks_to_target(mon, head_bn, substrate=substrate)
        if advanced:
            return last_bn, True
        return current_block, False
    except Exception as exc:
        logger.warning("chain head poll failed: %s", exc, exc_info=True)
        return current_block, False


def _block_subscription_thread(mon: MempoolMonitor, new_block_event: threading.Event,
                               new_block_number: list) -> None:
    """Subscribe to new block headers on a dedicated WebSocket.

    Signals the main loop immediately when a new block arrives, so it
    can skip the polling delay for block detection. Also publishes the
    head block number to :mod:`chain_head_cache` so the Trader can
    pre-fill ``era['current']`` and skip two RPCs per trade submit.
    """
    from substrateinterface import SubstrateInterface
    import chain_head_cache as _head_cache

    while not STOP.is_set():
        try:
            sub = SubstrateInterface(url=FINNEY_WS)

            def on_head(obj, update_nr, subscription_id):
                if STOP.is_set():
                    return {'result': None, 'subscription_id': subscription_id}
                header = obj.get('header', obj)
                bn = decode_block_number_field(header.get('number'))
                if bn is None:
                    return {'result': None, 'subscription_id': subscription_id}
                new_block_number[0] = bn
                # Publish to the shared head cache for the trader fast-path.
                # Hash is intentionally omitted here — the subscription
                # payload only guarantees the number; the trader only needs
                # ``current`` (block number) anyway.
                _head_cache.put(bn)
                new_block_event.set()

            sub.subscribe_block_headers(on_head, finalized_only=False)
        except Exception as exc:
            logger.warning("Block subscription thread error: %s", exc, exc_info=True)
            time.sleep(2)


def _bg_fetch_thread(mon: MempoolMonitor) -> None:
    """Dedicated thread for background balance/stake RPC lookups.

    Uses its own SubstrateInterface + bt.Subtensor so it never blocks
    the main monitor loop. Cache dicts are shared (CPython GIL makes
    dict reads/writes atomic for simple key-value ops).
    """
    from substrateinterface import SubstrateInterface
    try:
        bg_substrate = SubstrateInterface(url=FINNEY_WS)
    except Exception as exc:
        logger.error("BG-FETCH: substrate connect failed: %s", exc)
        return
    bg_subtensor = None
    try:
        import bittensor as bt
        bg_subtensor = bt.Subtensor(network='finney')
    except Exception:
        pass

    while not STOP.is_set():
        try:
            if not mon._bg_fetch_balance_queue and not mon._bg_fetch_stake_queue:
                time.sleep(0.05)
                continue
            mon.process_bg_fetch_queue(
                max_items=5, substrate=bg_substrate, subtensor=bg_subtensor
            )
            # Throttle: yield briefly between batches even while the queue is
            # busy so these refresh RPCs never saturate the shared public
            # endpoint and starve the block-processing thread. Each iteration
            # already drains up to BALANCE_BATCH_SIZE balances in one query_multi
            # round trip, so this small pause bounds balance RPC rate without
            # meaningfully delaying convergence.
            time.sleep(float(os.environ.get("BG_FETCH_THROTTLE_SEC", "0.05")))
        except Exception as exc:
            logger.warning("BG-FETCH err: %s", exc, exc_info=True)
            time.sleep(1)


def _block_processing_thread(
    mon: MempoolMonitor,
    new_block_event: threading.Event,
    new_block_number: list,
    current_block_holder: list,
    block_processed_event: threading.Event,
    wallet_addr: str | None,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Dedicated thread that runs all block processing on its own WS connection.

    Block processing (``process_new_block``) measured at 230–860 ms per block —
    almost entirely I/O (full price-table refresh + ``get_events`` + block body)
    rather than CPU. Running it on the main monitor loop serialized it behind
    ``poll_mempool`` + ``build_ui_snapshot``, so a single slow block (or a brief
    RPC stall) stalled the whole loop and the UI fell behind the chain head and
    then "jumped" once it caught up.

    By moving it here with its own ``SubstrateInterface`` it runs concurrently
    with the main loop's mempool polling and never blocks UI updates. The thread
    detects new blocks via the subscription event (instant) with a periodic
    ``chain_getHeader`` poll as a fallback, publishes the latest processed block
    number through ``current_block_holder`` and nudges the main loop to push a
    fresh snapshot + wallet refresh via ``block_processed_event``.
    """
    from substrateinterface import SubstrateInterface

    block_sub = None
    while not STOP.is_set() and block_sub is None:
        try:
            block_sub = SubstrateInterface(url=FINNEY_WS)
        except Exception as exc:
            logger.error("block-proc: substrate connect failed (retrying): %s", exc)
            time.sleep(2)
    if block_sub is None:
        return

    last_head_poll = 0.0
    while not STOP.is_set():
        try:
            advanced = False

            # Periodic best-head poll — independent of subscription WS health.
            now = time.monotonic()
            if now - last_head_poll >= MEMPOOL_HEAD_POLL_SEC:
                last_head_poll = now
                new_bn, adv = _poll_chain_head_catchup(
                    mon, current_block_holder[0], substrate=block_sub
                )
                if adv:
                    current_block_holder[0] = new_bn
                    advanced = True

            # Subscription-signaled new block (instant path).
            if new_block_event.is_set():
                new_block_event.clear()
                bn = new_block_number[0]
                if bn > (mon._last_seen_block or 0):
                    last_bn, adv = _catch_up_blocks_to_target(
                        mon, bn, substrate=block_sub
                    )
                    if adv:
                        current_block_holder[0] = last_bn
                        advanced = True

            if advanced:
                # Instant, lightweight block-number push — decoupled from the
                # heavier snapshot build on the main loop. The UI anchors its
                # block timer to the moment ``currentBlock`` changes, so
                # delivering the number the instant the block is detected
                # (subscription latency only) makes the counter tick smoothly
                # every ~12 s regardless of snapshot cost. The full snapshot
                # (mempool / last-block tables) still follows via the main loop.
                loop.call_soon_threadsafe(
                    _put,
                    {"type": "block", "currentBlock": current_block_holder[0]},
                )
                # Nudge the main loop to broadcast the refreshed Last-block data
                # immediately (even if the mempool fingerprint is unchanged) and
                # refresh the wallet panel on the new block.
                block_processed_event.set()
                if wallet_addr:
                    loop.call_soon_threadsafe(
                        _put,
                        {
                            "type": "fetch_wallet",
                            "address": wallet_addr,
                            "block": current_block_holder[0],
                        },
                    )

            # Block until the next subscription signal (or short timeout so the
            # head-poll fallback keeps running when the subscription is quiet).
            new_block_event.wait(timeout=0.2)
        except Exception as exc:
            logger.exception("block-proc thread failed: %s", exc)
            time.sleep(1)


def monitor_thread(loop: asyncio.AbstractEventLoop) -> None:
    global _monitor_thread_last_fp, _monitor
    mon = MempoolMonitor()
    _monitor = mon
    wallet_addr = resolve_wallet_address_from_env()
    if not wallet_addr:
        logger.info(
            "No coldkey in env (WALLET_ADDRESS / SEED_PHRASE_1 / WALLET_NAME) — My wallet panel disabled."
        )

    # Spawn dedicated bg-fetch thread with its own connections
    bg_t = threading.Thread(target=_bg_fetch_thread, args=(mon,), daemon=True)
    bg_t.start()

    # Spawn block subscription thread for instant new-block detection
    new_block_event = threading.Event()
    new_block_number = [0]  # mutable container shared with subscription thread
    sub_t = threading.Thread(
        target=_block_subscription_thread,
        args=(mon, new_block_event, new_block_number),
        daemon=True,
    )
    sub_t.start()

    # Bootstrap: fetch initial block via RPC (subscription hasn't fired yet).
    # Runs single-threaded on ``mon.substrate`` *before* the block-processing
    # thread starts, so there is no concurrent use of that connection.
    try:
        header = mon.substrate.rpc_request('chain_getHeader', [])
        bn_boot = block_number_from_chain_get_header(header)
        if bn_boot is None:
            raise ValueError("chain_getHeader: could not parse block number")
        current_block = bn_boot
        try:
            p = st_bridge.fetch_prices_sync(mon.substrate)
            if p:
                mon._tracker_prices = p
                mon._last_price_fetch = time.monotonic()
        except Exception:
            pass
        mon.last_block_data = mon.parse_block_stake_transactions(current_block)
        mon._last_seen_block = current_block
        # Head may have moved during bootstrap RPC; subscription may not have fired yet.
        current_block, _ = _poll_chain_head_catchup(mon, current_block)
    except Exception as exc:
        logger.error("Mempool monitor bootstrap failed: %s", exc, exc_info=True)
        current_block = 0

    # Block processing now lives on its own thread + connection so it never
    # serializes behind the mempool poll / snapshot build below. The main loop
    # only polls the mempool, builds the snapshot for the latest processed
    # block, and broadcasts it.
    current_block_holder = [current_block]
    block_processed_event = threading.Event()
    blk_t = threading.Thread(
        target=_block_processing_thread,
        args=(
            mon,
            new_block_event,
            new_block_number,
            current_block_holder,
            block_processed_event,
            wallet_addr,
            loop,
        ),
        daemon=True,
    )
    blk_t.start()

    while not STOP.is_set():
        try:
            # Fast mempool-only poll
            mon.poll_mempool()

            current_block = current_block_holder[0]
            snap = mon.build_ui_snapshot(current_block)
            _fp = _snapshot_fingerprint(snap)
            new_block = block_processed_event.is_set()
            if new_block:
                block_processed_event.clear()
            forced = _force_broadcast_event.is_set()
            if forced:
                _force_broadcast_event.clear()
            if forced or new_block or _fp != _monitor_thread_last_fp:
                _monitor_thread_last_fp = _fp
                loop.call_soon_threadsafe(
                    _put,
                    {"type": "snapshot", "data": snap, "fp": _fp, "force": forced or new_block},
                )
            if mon._mempool_stake or new_block:
                time.sleep(MEMPOOL_POLL_BUSY_SEC)
            else:
                time.sleep(MEMPOOL_POLL_IDLE_SEC)
        except Exception as e:
            # Previously only ``str(e)`` went to the UI and nothing to the log,
            # which masked root causes (stake_tracker decode regressions,
            # substrate WS hiccups, …). Log full stack once per error tick;
            # the UI still gets a short message.
            logger.exception("monitor_thread poll failed: %s", e)
            loop.call_soon_threadsafe(_put, {"type": "error", "message": str(e)})
            time.sleep(1)


def _trader_prewarm_thread() -> None:
    try:
        get_trader()
        logger.info("Trader pre-warmed (Subtensor + wallet ready for /api/trade)")
    except Exception as exc:
        logger.warning("Trader pre-warm failed (first trade may be slower): %s", exc)


def _faker_prewarm_thread() -> None:
    """Same pattern as :func:`_trader_prewarm_thread`: build the Faker
    singleton off the request path so the first confirm click doesn't pay
    the Subtensor connect + coldkey unlock + metadata download cost."""
    try:
        get_faker()
        logger.info("Faker pre-warmed (Subtensor + wallet ready for /api/faker/submit)")
    except Exception as exc:
        logger.warning("Faker pre-warm failed (first faker submit may be slower): %s", exc)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global checker, wallet_fetch_lock, bot_manager
    wallet_fetch_lock = asyncio.Lock()
    checker = WalletChecker(network="finney")
    # Do not block HTTP/WebSocket startup on Subtensor init (can take tens of seconds).
    threading.Thread(target=_trader_prewarm_thread, daemon=True).start()
    threading.Thread(target=_faker_prewarm_thread, daemon=True).start()
    loop = asyncio.get_event_loop()
    threading.Thread(target=monitor_thread, args=(loop,), daemon=True).start()
    threading.Thread(target=_pool_fetcher_thread, daemon=True).start()
    asyncio.create_task(queue_consumer())

    # Create the buy-bot manager. The worker subprocess is NOT started here by
    # default — the UI decides (via /api/bot/start) so bots never run silently
    # in the background on a fresh deploy. Override with BOT_AUTOSTART=true.
    bot_manager = BotManager(
        worker_script=BOT_WORKER_SCRIPT,
        env_file=BOT_ENV_PATH,
        state_file=BOT_STATE_PATH,
    )
    if BOT_AUTOSTART:
        try:
            await bot_manager.start()
        except Exception as exc:
            logger.warning("[bot] autostart failed: %s", exc)

    try:
        yield
    finally:
        STOP.set()
        if bot_manager is not None:
            try:
                await bot_manager.stop()
            except Exception as exc:
                logger.warning("[bot] shutdown stop failed: %s", exc)


app = FastAPI(title="Mempool monitor API", lifespan=_lifespan)
_cors_origins, _cors_creds = cors_allow_origins_and_credentials()
if _cors_origins == ["*"] and not _cors_creds:
    logger.warning(
        "CORS_ORIGINS=* — credentials disabled for CORS. Set CORS_ORIGINS to your frontend "
        "origin(s) (comma-separated) if the UI is on another host and uses cookie auth."
    )
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=_cors_creds,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(AppAuthMiddleware)


def _cookie_secure(request: Request) -> bool:
    if request.url.scheme == "https":
        return True
    return (request.headers.get("x-forwarded-proto") or "").lower() == "https"


class AuthLoginReq(BaseModel):
    password: str


@app.get("/login")
async def login_page() -> Any:
    if not auth_enabled():
        return RedirectResponse(url="/", status_code=302)
    return HTMLResponse(content=LOGIN_HTML)


@app.get("/api/auth/status")
def api_auth_status(request: Request) -> Dict[str, Any]:
    en = auth_enabled()
    ok = is_authenticated_request(request.cookies) if en else True
    return {"authRequired": en, "authenticated": ok}


@app.post("/api/auth/login")
async def api_auth_login(req: AuthLoginReq, response: Response, request: Request) -> Any:
    if not auth_enabled():
        return {"status": "ok", "message": "Auth disabled"}
    if password_matches(req.password):
        token = create_session_token()
        response.set_cookie(
            key=COOKIE_NAME,
            value=token,
            max_age=MAX_AGE_SEC,
            httponly=True,
            samesite="lax",
            secure=_cookie_secure(request),
            path="/",
        )
        return {"status": "ok"}
    return JSONResponse(
        {"status": "error", "message": "Invalid password"}, status_code=401
    )


@app.post("/api/auth/logout")
async def api_auth_logout(response: Response) -> Dict[str, str]:
    response.delete_cookie(COOKIE_NAME, path="/")
    return {"status": "ok"}


class Hub:
    def __init__(self) -> None:
        self.clients: Set[WebSocket] = set()

    async def register(self, ws: WebSocket) -> None:
        await ws.accept()
        self.clients.add(ws)

    def unregister(self, ws: WebSocket) -> None:
        self.clients.discard(ws)

    async def broadcast_raw(self, raw_json: str) -> None:
        """Send pre-serialized JSON to all clients (avoids per-client json.dumps)."""
        stale = []
        for ws in self.clients:
            try:
                await ws.send_text(raw_json)
            except Exception:
                stale.append(ws)
        for ws in stale:
            self.clients.discard(ws)


hub = Hub()


async def queue_consumer() -> None:
    global last_snapshot_json, last_wallet_json, _prev_snapshot_fp
    while True:
        msg = await ASYNC_QUEUE.get()
        if msg is None:
            continue
        mtype = msg.get("type")
        if mtype == "snapshot":
            fp = msg.get("fp")
            if fp is None:
                fp = _snapshot_fingerprint(msg["data"])
            # ``force`` bypasses fp-dedupe for reconnect-triggered refreshes
            # (see `_force_broadcast_event`); we still cache fp so subsequent
            # unchanged snapshots get skipped normally.
            if not msg.get("force") and fp == _prev_snapshot_fp:
                continue
            _prev_snapshot_fp = fp
            raw = _json_dumps_compact({"type": "snapshot", "data": msg["data"]})
            last_snapshot_json = raw
            await hub.broadcast_raw(raw)
        elif mtype == "fetch_wallet" and checker and wallet_fetch_lock:
            async with wallet_fetch_lock:
                # Invalidate only this address's stake cache (not full clear) + parallel free/stake in check_wallet.
                checker.invalidate_stake_cache_for_address(msg["address"])
                try:
                    data = await checker.check_wallet(
                        msg["address"], show_header=False, silent=True
                    )
                    payload = {
                        **data,
                        "block": msg["block"],
                        "address": msg["address"],
                    }
                    raw = _json_dumps_compact({"type": "wallet", "data": payload})
                    last_wallet_json = raw
                    await hub.broadcast_raw(raw)
                except Exception as e:
                    raw = _json_dumps_compact({"type": "wallet_error", "message": str(e)})
                    await hub.broadcast_raw(raw)
        elif mtype == "block":
            # Lightweight block-number tick, broadcast immediately (no fp
            # dedupe, no snapshot build) so the UI block counter/timer updates
            # the instant the chain advances.
            await hub.broadcast_raw(
                _json_dumps_compact(
                    {"type": "block", "currentBlock": msg.get("currentBlock")}
                )
            )
        elif mtype == "error":
            await hub.broadcast_raw(_json_dumps_compact(msg))


def _parse_backprop_tao_in_pool(raw: Any) -> float:
    """Subnet list API uses string or int rao; normalize to TAO float."""
    try:
        if raw is None:
            return 0.0
        if isinstance(raw, str):
            return int(raw.strip() or "0") / 1e9
        return int(raw) / 1e9
    except (TypeError, ValueError):
        return 0.0


def _pool_fetcher_thread() -> None:
    """Background thread: refresh pool cache from BACKPROP_SUBNETS_URL (full subnet list, one GET)."""
    global _pool_cache
    url = BACKPROP_SUBNETS_URL
    while not STOP.is_set():
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "mempool-server/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                payload = json.loads(resp.read())
            if not payload.get("success"):
                raise RuntimeError("missing success")
            body = payload.get("body")
            if not isinstance(body, list):
                raise RuntimeError("body is not a list")
            new_cache: Dict[int, Dict[str, Any]] = {}
            for item in body:
                if not isinstance(item, dict):
                    continue
                uid = item.get("id")
                try:
                    netuid = int(uid)
                except (TypeError, ValueError):
                    continue
                new_cache[netuid] = {
                    "taoInPool": _parse_backprop_tao_in_pool(item.get("taoInPool")),
                    "name": str(item.get("name") or ""),
                }
            _pool_cache = new_cache
        except Exception as exc:
            logger.debug("pool fetch failed: %s", exc)
        time.sleep(POOL_POLL_INTERVAL_SEC)


@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/trading-coldkey")
def api_trading_coldkey() -> Dict[str, str]:
    """SS58 resolved from .env for the live WebSocket wallet (My wallet)."""
    a = resolve_wallet_address_from_env()
    return {"address": a or ""}


class WalletBalanceReq(BaseModel):
    address: str
    # When True, skip stake cache clear (faster periodic refresh on new block for same address).
    light: bool = False


# Batch: whitelist addresses that have stake on a given netuid (uses stake cache TTL like light refresh).
_MAX_WHITELIST_SUBNET_ADDRESSES = int(os.environ.get("WHITELIST_SUBNET_MAX_ADDRESSES", "200"))


class WhitelistSubnetHoldersReq(BaseModel):
    netuid: int
    addresses: list[str]


class WhitelistAddressesReq(BaseModel):
    addresses: list[str]


@app.post("/api/whitelist-addresses")
async def api_whitelist_addresses(req: WhitelistAddressesReq) -> Dict[str, Any]:
    """Register normalized whitelist coldkeys for ``Balances.Transfer`` history."""
    import stake_tracker as st

    if _monitor is None:
        return {"status": "error", "message": "Monitor not ready"}
    addrs: Set[str] = set()
    for raw in req.addresses:
        a = (raw or "").strip()
        if not a:
            continue
        norm = st._normalize_ss58(a) or a
        if norm.startswith("5") and len(norm) >= 40:
            addrs.add(norm)
    _monitor.set_whitelist_addresses(addrs)
    return {"status": "ok", "count": len(addrs)}


@app.post("/api/whitelist-subnet-holders")
async def api_whitelist_subnet_holders(req: WhitelistSubnetHoldersReq) -> Dict[str, Any]:
    """Stake-only rows for whitelisted coldkeys on one subnet (batch RPC; no free balance)."""
    global checker, wallet_fetch_lock
    if not checker or not wallet_fetch_lock:
        return {"status": "error", "message": "Wallet checker unavailable"}
    if req.netuid < 0 or req.netuid > 65535:
        return {"status": "error", "message": "Invalid netuid"}
    netuid = int(req.netuid)

    seen: Set[str] = set()
    unique: list[str] = []
    for raw in req.addresses:
        a = (raw or "").strip()
        if not a.startswith("5") or len(a) < 40:
            continue
        if a in seen:
            continue
        seen.add(a)
        unique.append(a)
    if not unique:
        return {"status": "error", "message": "No valid addresses"}
    if len(unique) > _MAX_WHITELIST_SUBNET_ADDRESSES:
        return {
            "status": "error",
            "message": f"Too many addresses (max {_MAX_WHITELIST_SUBNET_ADDRESSES})",
        }

    holders: list[Dict[str, Any]] = []
    errors: Dict[str, str] = {}
    async with wallet_fetch_lock:
        for addr in unique:
            try:
                rows = await checker.get_subnet_stake_rows_async(addr)
                for row in rows:
                    try:
                        rid = int(row.get("netuid", -1))
                    except (TypeError, ValueError):
                        continue
                    if rid != netuid:
                        continue
                    holders.append(
                        {
                            "address": addr,
                            "hotkey": str(row.get("hotkey", "")),
                            "stake_tao": float(row.get("stake_tao", 0) or 0),
                        }
                    )
            except Exception as e:
                errors[addr] = str(e)

    out: Dict[str, Any] = {
        "status": "ok",
        "netuid": netuid,
        "holders": holders,
    }
    if errors:
        out["errors"] = errors
    return out


@app.post("/api/wallet-balance")
async def api_wallet_balance(req: WalletBalanceReq) -> Dict[str, Any]:
    """Read-only balance for any SS58 address."""
    global checker, wallet_fetch_lock
    addr = (req.address or "").strip()
    if not addr.startswith("5") or len(addr) < 40:
        return {"status": "error", "message": "Invalid SS58 address"}
    if not checker or not wallet_fetch_lock:
        return {"status": "error", "message": "Wallet checker unavailable"}
    async with wallet_fetch_lock:
        if not req.light:
            checker.clear_cache()
        try:
            data = await checker.check_wallet(addr, show_header=False, silent=True)
            payload: Dict[str, Any] = {**data, "address": addr, "block": None}
            return {"status": "ok", "data": payload}
        except Exception as e:
            return {"status": "error", "message": str(e)}


class RestartReq(BaseModel):
    password: str


class TradeReq(BaseModel):
    action: str  # "buy" or "sell"
    netuid: str
    amount: float = 0
    slippage: float = 1.0
    hotkey: Optional[str] = None  # validator hotkey (required for sell from a specific subnet stake)
    # When True: plain add_stake/unstake (no limit price). When False: safe_staking with rate_tolerance.
    no_slippage: bool = False
    # When True: encrypt via MevShield pallet (front-running protection).
    mev_shield: bool = False
    # Emergency decoy mode (buy only, ignored when mev_shield=True):
    # fire a remove_stake_limit decoy on the background after the real buy lands
    # in the mempool. ``emergency_tao`` is the TAO-equivalent amount the decoy
    # should pretend to remove (alpha = emergency_tao / spot_price).
    emergency: bool = False
    emergency_tao: float = 0.0
    # Buy only: reserve consecutive nonces N/N+1 so Remove can submit the sell
    # with the pre-planned nonce N+1 (one-block round-trip, no transfer).
    round_trip: bool = False
    # Sell only: consume a round-trip reservation from an earlier buy.
    round_trip_id: Optional[str] = None


class SellAllBatchPosition(BaseModel):
    netuid: int
    amount: float
    hotkey: str


class SellAllBatchReq(BaseModel):
    """Deposit panel batch remove — selected positions; MEV or force_batch."""

    positions: list[SellAllBatchPosition]
    slippage: float = 5.0
    no_slippage: bool = False
    mev_shield: bool = False


class BuyBatchReq(BaseModel):
    """Deposit panel batch buy — same TAO amount per netuid; MEV or force_batch."""

    netuids: list[int]
    amount: float
    slippage: float = 1.0
    no_slippage: bool = False
    mev_shield: bool = False
    emergency: bool = False
    emergency_tao: float = 0.0


class SwapBatchPosition(BaseModel):
    netuid: int
    hotkey: str
    amount: float


class SwapBatchReq(BaseModel):
    """Deposit panel batch swap — partial/full stake from origins to one destination."""

    positions: list[SwapBatchPosition]
    destination_netuid: int
    slippage: float = 5.0
    no_slippage: bool = False
    mev_shield: bool = False


_MAX_BUY_BATCH_NETUIDS = int(os.environ.get("BUY_BATCH_MAX_NETUIDS", "32"))
_MAX_SWAP_BATCH_POSITIONS = int(os.environ.get("SWAP_BATCH_MAX_POSITIONS", "32"))


def _trade_async_submit_ms() -> int:
    """Opt-in early-return timeout for :func:`trade` (milliseconds).

    When set to a positive integer, ``/api/trade`` awaits the trader in the
    executor for at most this many milliseconds; if the trader hasn't
    returned yet, the HTTP call responds with an interim ``submitting``
    status and the background thread finishes to completion with its
    result logged.

    **Default (unset / ``0`` / invalid): disabled.** In that case the
    endpoint behaves exactly as before — waits for the full trader result
    and returns the concrete success/error message. This env var is the
    only gate for proposal 4 so the current toast UX is preserved by
    default.
    """
    raw = os.environ.get("TRADE_ASYNC_SUBMIT_MS", "").strip()
    if not raw:
        return 0
    try:
        n = int(raw)
    except ValueError:
        return 0
    return max(0, n)


@app.post("/api/restart")
async def restart_with_password(req: RestartReq) -> Dict[str, str]:
    """Store wallet password and signal monitor to restart."""
    os.environ["WALLET_PASSWORD"] = req.password
    return {"status": "Password applied. Backend will use it on next wallet operation."}


@app.post("/api/trade")
async def trade(req: TradeReq) -> Dict[str, Any]:
    """Execute buy/sell via bittensor SDK (wallet from BTCLI_WALLET / BTCLI_HOTKEY in .env)."""
    t_start = time.time()

    if req.action not in ("buy", "sell"):
        return {"status": "error", "message": "action must be 'buy' or 'sell'"}
    if req.amount <= 0 and not (req.action == "sell" and req.round_trip_id):
        return {"status": "error", "message": "amount must be > 0"}

    try:
        netuid = int(req.netuid)
    except ValueError:
        return {"status": "error", "message": f"Invalid netuid: {req.netuid}"}

    trader = get_trader()
    if trader is None:
        hint = trader_last_init_error() or ""
        base = (
            "Trader unavailable: check BTCLI_WALLET, BTCLI_HOTKEY, TRADE_HOTKEY in .env "
            "and that ~/.bittensor/wallets/ exists. "
        )
        return {"status": "error", "message": (base + hint).strip()}

    hk = (req.hotkey or "").strip() or None
    t_pre = time.time()
    logger.info("TRADE timing: validation=%.0fms, dispatching %s …",
                (t_pre - t_start) * 1000, req.action)

    # Emergency decoy is buy-only and intentionally suppressed when MEV Shield
    # is active (the encrypted-extrinsic path is its own privacy mechanism).
    decoy_tao = 0.0
    if (
        req.action == "buy"
        and req.emergency
        and not req.mev_shield
        and req.emergency_tao > 0
    ):
        decoy_tao = float(req.emergency_tao)

    try:
        loop = asyncio.get_event_loop()
        if req.action == "buy":
            fut = loop.run_in_executor(
                None,
                lambda: trader.buy(
                    netuid,
                    req.amount,
                    req.slippage,
                    hotkey=hk,
                    no_slippage=req.no_slippage,
                    mev_shield=req.mev_shield,
                    decoy_tao=decoy_tao,
                    round_trip=req.round_trip,
                ),
            )
        elif req.round_trip_id:
            fut = loop.run_in_executor(
                None,
                lambda: trader.sell_round_trip(req.round_trip_id),
            )
        else:
            fut = loop.run_in_executor(
                None,
                lambda: trader.sell(
                    netuid,
                    req.amount,
                    req.slippage,
                    hotkey=hk,
                    no_slippage=req.no_slippage,
                    mev_shield=req.mev_shield,
                ),
            )

        async_ms = _trade_async_submit_ms()
        if async_ms > 0:
            # Opt-in early return. ``shield`` prevents ``wait_for`` from
            # cancelling the awaitable we still want to keep (the executor
            # thread is not actually cancellable either way, but this keeps
            # asyncio state tidy). On timeout we attach a completion
            # callback that logs the eventual result and return an interim
            # response so the HTTP client isn't held on the socket.
            shielded = asyncio.shield(fut)
            try:
                result = await asyncio.wait_for(shielded, timeout=async_ms / 1000.0)
            except asyncio.TimeoutError:
                def _log_async_result(f: "asyncio.Future[Any]") -> None:
                    try:
                        r = f.result()
                    except Exception as ex:  # noqa: BLE001
                        logger.exception("TRADE async-submit failed post-response: %s", ex)
                        return
                    logger.info(
                        "TRADE async-submit result post-response: success=%s msg=%s hash=%s elapsed=%sms",
                        getattr(r, "success", None),
                        getattr(r, "message", None),
                        getattr(r, "extrinsic_hash", None),
                        getattr(r, "elapsed_ms", None),
                    )
                fut.add_done_callback(_log_async_result)
                elapsed_early = (time.time() - t_start) * 1000
                logger.info(
                    "TRADE async early-return after %.0fms (cap=%dms); trader still running",
                    elapsed_early, async_ms,
                )
                return {
                    "status": f"Submitting {req.action} n{netuid} …",
                    "async": True,
                    "elapsed": f"{elapsed_early:.0f}ms",
                }
        else:
            result = await fut
    except Exception as e:
        logger.exception("trade executor failed")
        return {"status": "error", "message": str(e)}

    t_done = time.time()
    logger.info("TRADE timing: total=%.0fms (btcli=%.0fms)",
                (t_done - t_start) * 1000, (t_done - t_pre) * 1000)

    if not result.success:
        return {"status": "error", "message": result.message}

    resp: Dict[str, Any] = {"status": result.message}
    if result.extrinsic_hash:
        resp["hash"] = result.extrinsic_hash
    if result.elapsed_ms is not None:
        resp["elapsed"] = f"{result.elapsed_ms:.0f}ms"
    if result.round_trip_id:
        resp["roundTripId"] = result.round_trip_id
    if result.buy_nonce is not None:
        resp["buyNonce"] = result.buy_nonce
    if result.sell_nonce is not None:
        resp["sellNonce"] = result.sell_nonce
    if result.hotkey:
        resp["hotkey"] = result.hotkey
    if result.round_trip_buy_tao is not None:
        resp["roundTripBuyTao"] = result.round_trip_buy_tao
    return resp


def _position_trade_results_payload(
    positions: Optional[list[Any]],
) -> Optional[list[Dict[str, Any]]]:
    if not positions:
        return None
    out: list[Dict[str, Any]] = []
    for p in positions:
        row: Dict[str, Any] = {
            "netuid": p.netuid,
            "hotkey": p.hotkey,
            "success": p.success,
            "message": p.message,
        }
        if p.extrinsic_hash:
            row["hash"] = p.extrinsic_hash
        out.append(row)
    return out


@app.post("/api/trade/buy-batch")
async def trade_buy_batch(req: BuyBatchReq) -> Dict[str, Any]:
    """Batch stake the same TAO amount on many netuids.

    Without MEV Shield: one ``Utility.force_batch`` extrinsic.
    With MEV Shield: one encrypted extrinsic wrapping the same ``force_batch``.
    """
    t_start = time.time()
    if not req.netuids:
        return {"status": "error", "message": "netuids must not be empty"}
    if req.amount <= 0:
        return {"status": "error", "message": "amount must be > 0"}

    seen: Set[int] = set()
    netuids: list[int] = []
    for raw in req.netuids:
        nid = int(raw)
        if nid < 0 or nid > 65535:
            return {"status": "error", "message": f"netuid out of range: {nid}"}
        if nid in seen:
            continue
        seen.add(nid)
        netuids.append(nid)

    if not netuids:
        return {"status": "error", "message": "no valid netuids"}
    if len(netuids) > _MAX_BUY_BATCH_NETUIDS:
        return {
            "status": "error",
            "message": f"too many netuids (max {_MAX_BUY_BATCH_NETUIDS})",
        }

    trader = get_trader()
    if trader is None:
        hint = trader_last_init_error() or ""
        base = (
            "Trader unavailable: check BTCLI_WALLET, BTCLI_HOTKEY, TRADE_HOTKEY in .env "
            "and that ~/.bittensor/wallets/ exists. "
        )
        return {"status": "error", "message": (base + hint).strip()}

    decoy_tao = 0.0
    if req.emergency and not req.mev_shield and req.emergency_tao > 0:
        decoy_tao = float(req.emergency_tao)

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: trader.buy_batch(
                netuids,
                req.amount,
                req.slippage,
                no_slippage=req.no_slippage,
                mev_shield=req.mev_shield,
                decoy_tao=decoy_tao,
            ),
        )
    except Exception as e:
        logger.exception("buy-batch executor failed")
        return {"status": "error", "message": str(e)}

    logger.info(
        "BUY-BATCH timing: total=%.0fms netuids=%d",
        (time.time() - t_start) * 1000,
        len(netuids),
    )
    if not result.success:
        err_resp: Dict[str, Any] = {"status": "error", "message": result.message}
        per_pos = _position_trade_results_payload(result.positions)
        if per_pos is not None:
            err_resp["results"] = per_pos
            err_resp["ok"] = sum(1 for r in per_pos if r.get("success"))
            err_resp["count"] = len(per_pos)
        return err_resp

    resp: Dict[str, Any] = {
        "status": result.message,
        "count": len(netuids),
        "netuids": netuids,
    }
    per_pos = _position_trade_results_payload(result.positions)
    if per_pos is not None:
        resp["results"] = per_pos
        resp["ok"] = sum(1 for r in per_pos if r.get("success"))
        resp["mev_shield"] = True
    if result.extrinsic_hash:
        resp["hash"] = result.extrinsic_hash
    if result.elapsed_ms is not None:
        resp["elapsed"] = f"{result.elapsed_ms:.0f}ms"
    return resp


@app.post("/api/trade/sell-all-batch")
async def trade_sell_all_batch(req: SellAllBatchReq) -> Dict[str, Any]:
    """Batch unstake selected positions.

    Without MEV Shield: one ``Utility.force_batch`` extrinsic.
    With MEV Shield: one encrypted extrinsic wrapping the same ``force_batch``
    (all positions unstake together; response includes per-position ``results``).
    """
    t_start = time.time()
    if not req.positions:
        return {"status": "error", "message": "positions must not be empty"}

    trader = get_trader()
    if trader is None:
        hint = trader_last_init_error() or ""
        base = (
            "Trader unavailable: check BTCLI_WALLET, BTCLI_HOTKEY, TRADE_HOTKEY in .env "
            "and that ~/.bittensor/wallets/ exists. "
        )
        return {"status": "error", "message": (base + hint).strip()}

    tuples: list[tuple[int, str, float]] = []
    for p in req.positions:
        if p.netuid < 0 or p.netuid > 65535:
            return {"status": "error", "message": f"netuid out of range: {p.netuid}"}
        if p.amount <= 0:
            continue
        hk = (p.hotkey or "").strip()
        if not hk:
            return {"status": "error", "message": f"netuid {p.netuid}: hotkey required"}
        tuples.append((int(p.netuid), hk, float(p.amount)))

    if not tuples:
        return {"status": "error", "message": "no positions with amount > 0"}

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: trader.sell_all_batch(
                tuples,
                req.slippage,
                no_slippage=req.no_slippage,
                mev_shield=req.mev_shield,
            ),
        )
    except Exception as e:
        logger.exception("sell-all-batch executor failed")
        return {"status": "error", "message": str(e)}

    logger.info(
        "SELL-ALL-BATCH timing: total=%.0fms positions=%d",
        (time.time() - t_start) * 1000,
        len(tuples),
    )
    if not result.success:
        err_resp: Dict[str, Any] = {"status": "error", "message": result.message}
        per_pos = _position_trade_results_payload(result.positions)
        if per_pos is not None:
            err_resp["results"] = per_pos
            err_resp["ok"] = sum(1 for r in per_pos if r.get("success"))
            err_resp["count"] = len(per_pos)
        return err_resp

    resp: Dict[str, Any] = {"status": result.message, "count": len(tuples)}
    per_pos = _position_trade_results_payload(result.positions)
    if per_pos is not None:
        resp["results"] = per_pos
        resp["ok"] = sum(1 for r in per_pos if r.get("success"))
        resp["mev_shield"] = True
    if result.extrinsic_hash:
        resp["hash"] = result.extrinsic_hash
    if result.elapsed_ms is not None:
        resp["elapsed"] = f"{result.elapsed_ms:.0f}ms"
    return resp


@app.post("/api/trade/swap-batch")
async def trade_swap_batch(req: SwapBatchReq) -> Dict[str, Any]:
    """Batch swap stake from selected origins into one destination netuid.

    Each position carries a TAO-equivalent ``amount`` (same basis as sell batch).
    """
    t_start = time.time()
    if not req.positions:
        return {"status": "error", "message": "positions must not be empty"}

    dest = int(req.destination_netuid)
    if dest < 0 or dest > 65535:
        return {"status": "error", "message": f"destination_netuid out of range: {dest}"}

    trader = get_trader()
    if trader is None:
        hint = trader_last_init_error() or ""
        base = (
            "Trader unavailable: check BTCLI_WALLET, BTCLI_HOTKEY, TRADE_HOTKEY in .env "
            "and that ~/.bittensor/wallets/ exists. "
        )
        return {"status": "error", "message": (base + hint).strip()}

    tuples: list[tuple[int, str, float]] = []
    for p in req.positions:
        if p.netuid < 0 or p.netuid > 65535:
            return {"status": "error", "message": f"netuid out of range: {p.netuid}"}
        if p.amount <= 0:
            continue
        hk = (p.hotkey or "").strip()
        if not hk:
            return {"status": "error", "message": f"netuid {p.netuid}: hotkey required"}
        tuples.append((int(p.netuid), hk, float(p.amount)))

    if not tuples:
        return {"status": "error", "message": "no positions with amount > 0"}
    if len(tuples) > _MAX_SWAP_BATCH_POSITIONS:
        return {
            "status": "error",
            "message": f"at most {_MAX_SWAP_BATCH_POSITIONS} positions per swap batch",
        }

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: trader.swap_batch(
                tuples,
                dest,
                req.slippage,
                no_slippage=req.no_slippage,
                mev_shield=req.mev_shield,
            ),
        )
    except Exception as e:
        logger.exception("swap-batch executor failed")
        return {"status": "error", "message": str(e)}

    logger.info(
        "SWAP-BATCH timing: total=%.0fms positions=%d dest=%d",
        (time.time() - t_start) * 1000,
        len(tuples),
        dest,
    )
    if not result.success:
        err_resp: Dict[str, Any] = {"status": "error", "message": result.message}
        per_pos = _position_trade_results_payload(result.positions)
        if per_pos is not None:
            err_resp["results"] = per_pos
            err_resp["ok"] = sum(1 for r in per_pos if r.get("success"))
            err_resp["count"] = len(per_pos)
        return err_resp

    resp: Dict[str, Any] = {
        "status": result.message,
        "count": len(tuples),
        "destination_netuid": dest,
    }
    per_pos = _position_trade_results_payload(result.positions)
    if per_pos is not None:
        resp["results"] = per_pos
        resp["ok"] = sum(1 for r in per_pos if r.get("success"))
        resp["mev_shield"] = True
    if result.extrinsic_hash:
        resp["hash"] = result.extrinsic_hash
    if result.elapsed_ms is not None:
        resp["elapsed"] = f"{result.elapsed_ms:.0f}ms"
    return resp


@app.get("/api/pools")
def api_pools() -> Dict[str, Any]:
    """JSON map netuid -> {taoInPool, name}; data from subnet list API (see BACKPROP_SUBNETS_URL)."""
    return {str(k): v for k, v in _pool_cache.items()}


# ── Buy-bot REST surface ─────────────────────────────────────────────────────
#
# The buy-bot worker is a plain subprocess; the UI drives it through this API.
# A PUT to /api/bot/state regenerates the env file the worker polls, so every
# mutation propagates within ~50ms without any IPC plumbing.

_BOT_MAX_RULES = 200


def _normalize_ss58(raw: str) -> Optional[str]:
    """Cheap SS58 sanity check (matches the frontend's normalizeSs58)."""
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if len(s) < 40 or len(s) > 52 or not s.startswith("5"):
        return None
    allowed = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    if any(ch not in allowed for ch in s):
        return None
    return s


def _validate_bot_rule(entry: Any) -> Optional[BotRule]:
    """Coerce a user-supplied rule dict into a validated :class:`BotRule`."""
    if not isinstance(entry, dict):
        return None
    mode = str(entry.get("mode", "")).strip()
    if mode == "cross_subnet_sell":
        mode = "mirror_sell"
    if mode not in {"copy_buy", "copy_sell", "mirror_sell", "mirror_buy"}:
        return None
    addr = _normalize_ss58(str(entry.get("address", "")))
    if not addr:
        return None
    enabled = bool(entry.get("enabled", True))
    rid = str(entry.get("id") or "").strip() or (
        f"{int(time.time() * 1000)}-"
        f"{abs(hash(addr + mode)) % 0xFFFFFF:06x}"
    )
    created_at = entry.get("createdAt") or entry.get("created_at")
    try:
        created_at_f = float(created_at) if created_at is not None else time.time()
    except (TypeError, ValueError):
        created_at_f = time.time()
    return BotRule(
        id=rid,
        mode=mode,
        address=addr,
        enabled=enabled,
        created_at=created_at_f,
    )


class BotSettingsPatch(BaseModel):
    # UI-editable global knobs. Tip, tolerances, instant-buy amount, and
    # copy-on-sell are hardcoded in ``buy_bot.manager``.
    copy_trade_amount: Optional[float] = None
    home_netuids: Optional[str] = None
    home_netuid: Optional[int] = None  # legacy single-netuid → home_netuids


class BotStateReq(BaseModel):
    rules: Optional[list[Dict[str, Any]]] = None
    settings: Optional[BotSettingsPatch] = None


class BotTriggerSellReq(BaseModel):
    netuids: list[int]


def _bot_unavailable() -> Dict[str, Any]:
    return {"status": "error", "message": "bot manager not initialised"}


@app.get("/api/bot/state")
def api_bot_state() -> Dict[str, Any]:
    """Full snapshot: rules, settings, wallet/network, subprocess status."""
    if bot_manager is None:
        return _bot_unavailable()
    return {"status": "ok", **bot_manager.snapshot()}


@app.put("/api/bot/state")
async def api_bot_state_put(req: BotStateReq) -> Dict[str, Any]:
    """Update rules and/or global settings. Worker picks up changes via env file poll."""
    if bot_manager is None:
        return _bot_unavailable()

    rejected: list[Dict[str, Any]] = []
    if req.rules is not None:
        if not isinstance(req.rules, list):
            return {"status": "error", "message": "rules must be a list"}
        if len(req.rules) > _BOT_MAX_RULES:
            return {
                "status": "error",
                "message": f"Too many rules (max {_BOT_MAX_RULES})",
            }
        accepted: list[BotRule] = []
        for raw in req.rules:
            rule = _validate_bot_rule(raw)
            if rule is None:
                rejected.append(raw if isinstance(raw, dict) else {"value": str(raw)})
                continue
            accepted.append(rule)
        try:
            await bot_manager.replace_rules(accepted)
        except Exception as exc:
            logger.exception("[bot] replace_rules failed")
            return {"status": "error", "message": f"rule update failed: {exc}"}

    if req.settings is not None:
        patch = {
            k: v
            for k, v in req.settings.model_dump(exclude_none=True).items()
        }
        if patch:
            try:
                await bot_manager.update_settings(**patch)
            except Exception as exc:
                logger.exception("[bot] update_settings failed")
                return {"status": "error", "message": f"settings update failed: {exc}"}

    return {"status": "ok", "rejected": rejected, **bot_manager.snapshot()}


@app.post("/api/bot/trigger/sell")
async def api_bot_trigger_sell(req: BotTriggerSellReq) -> Dict[str, Any]:
    """Queue an instant-sell of all alpha on the given netuid(s)."""
    if bot_manager is None:
        return _bot_unavailable()
    for n in req.netuids:
        if n < 0 or n > 65535:
            return {"status": "error", "message": f"netuid out of range: {n}"}
    try:
        await bot_manager.trigger_sell(req.netuids)
    except Exception as exc:
        logger.exception("[bot] trigger sell failed")
        return {"status": "error", "message": str(exc)}
    return {"status": "ok", **bot_manager.snapshot()}


@app.post("/api/bot/start")
async def api_bot_start() -> Dict[str, Any]:
    if bot_manager is None:
        return _bot_unavailable()
    try:
        await bot_manager.start()
    except Exception as exc:
        logger.exception("[bot] start failed")
        return {"status": "error", "message": str(exc)}
    return {"status": "ok", **bot_manager.snapshot()}


@app.post("/api/bot/stop")
async def api_bot_stop() -> Dict[str, Any]:
    if bot_manager is None:
        return _bot_unavailable()
    try:
        await bot_manager.stop()
    except Exception as exc:
        logger.exception("[bot] stop failed")
        return {"status": "error", "message": str(exc)}
    return {"status": "ok", **bot_manager.snapshot()}


@app.post("/api/bot/restart")
async def api_bot_restart() -> Dict[str, Any]:
    if bot_manager is None:
        return _bot_unavailable()
    try:
        await bot_manager.restart()
    except Exception as exc:
        logger.exception("[bot] restart failed")
        return {"status": "error", "message": str(exc)}
    return {"status": "ok", **bot_manager.snapshot()}


@app.get("/api/bot/positions")
async def api_bot_positions() -> Dict[str, Any]:
    """Wallet balances + staked alpha positions with TAO valuations.

    Ported from the standalone buy_bot dashboard so the UI can show what the
    worker is actually holding without duplicating balance logic.
    """
    if bot_manager is None:
        return _bot_unavailable()

    wallet_name = os.environ.get("WALLET_NAME", "").strip()
    wallet_hotkey = os.environ.get("WALLET_HOTKEY", "default").strip() or "default"
    wallet_path = os.environ.get("WALLET_PATH", "~/.bittensor/wallets")
    network = os.environ.get("NETWORK", "finney").strip() or "finney"

    if not wallet_name:
        return {
            "status": "error",
            "message": "WALLET_NAME not set in backend .env — cannot query positions",
        }

    try:
        from bittensor_wallet import Wallet
        from bittensor.core.async_subtensor import AsyncSubtensor
        from substrateinterface.utils.ss58 import ss58_encode
    except Exception as exc:
        return {
            "status": "error",
            "message": f"Bittensor SDK unavailable: {exc}",
        }

    try:
        wallet = await asyncio.to_thread(
            Wallet, name=wallet_name, hotkey=wallet_hotkey, path=wallet_path
        )
        coldkey_ss58 = wallet.coldkeypub.ss58_address
    except Exception as exc:
        return {"status": "error", "message": f"failed to load wallet: {exc}"}

    try:
        async with AsyncSubtensor(network=network) as subtensor:
            stake_result, free_balance = await asyncio.gather(
                subtensor.substrate.runtime_call(
                    "StakeInfoRuntimeApi",
                    "get_stake_info_for_coldkey",
                    [coldkey_ss58],
                ),
                subtensor.get_balance(coldkey_ss58),
            )
            # Dust threshold: `remove_stake_full_limit` can leave a few rao of
            # residual due to on-chain pool-math rounding, which then appears
            # as a permanently "empty" (rounds to 0.00 τ) row in the UI. Anything
            # below ~0.001 α is visually indistinguishable from zero and cannot
            # be sold anyway (the chain will no-op), so we drop it server-side.
            # Keep in sync with `DUST_ALPHA_RAO` documented elsewhere if raised.
            DUST_STAKE_RAO = 1_000_000  # 0.001 α
            # AsyncSubtensor's runtime_call already unwraps the ScaleType and
            # returns a plain list; the older sync substrate-interface returns
            # a wrapper with ``.value``. Support both shapes.
            stake_entries = (
                stake_result.value
                if hasattr(stake_result, "value")
                else stake_result
            ) or []
            positions: list[Dict[str, Any]] = []
            for entry in stake_entries:
                stake_rao = int(entry["stake"])
                if stake_rao < DUST_STAKE_RAO:
                    continue
                raw = entry["hotkey"]
                # AsyncSubtensor already decodes hotkey to an ss58 string;
                # sync substrate-interface returns raw AccountId bytes (or a
                # nested ((b1..b32),) / (b1..b32) tuple). Handle every shape.
                if isinstance(raw, str):
                    hotkey_ss58 = raw
                elif isinstance(raw, (tuple, list)) and raw and isinstance(raw[0], (tuple, list)):
                    hotkey_ss58 = ss58_encode(bytes(raw[0]), ss58_format=42)
                else:
                    hotkey_ss58 = ss58_encode(bytes(raw), ss58_format=42)
                positions.append(
                    {
                        "netuid": int(entry["netuid"]),
                        "alpha": round(stake_rao / 1e9, 4),
                        "stake_rao": stake_rao,
                        "hotkey_ss58": hotkey_ss58,
                    }
                )

            async def _value(pos: Dict[str, Any]) -> Dict[str, Any]:
                try:
                    info = await subtensor.subnet(netuid=pos["netuid"])
                    price = float(info.price.tao)
                    pos["tao"] = round(pos["alpha"] * price, 2)
                except Exception:
                    pos["tao"] = None
                return pos

            positions = list(await asyncio.gather(*[_value(p) for p in positions]))
            positions.sort(key=lambda p: p["netuid"])

            free_tao = round(float(free_balance.tao), 4)
            staked_tao = round(
                sum(p["tao"] for p in positions if p["tao"] is not None), 2
            )
            return {
                "status": "ok",
                "positions": positions,
                "free_tao": free_tao,
                "staked_tao": staked_tao,
                "total_tao": round(free_tao + staked_tao, 2),
                "coldkey_ss58": coldkey_ss58,
            }
    except Exception as exc:
        logger.exception("[bot] positions fetch failed")
        return {"status": "error", "message": f"failed to fetch positions: {exc}"}


# ── Faker (decoy self-proxy extrinsic) ──────────────────────────────────────

class FakerSubmitReq(BaseModel):
    netuid: int
    amount: float
    action: str  # "add" | "remove" | "swap"
    dest_netuid: Optional[int] = None
    real: Optional[str] = None  # ss58; empty falls back to coldkey


_FAKER_SS58_RE = re.compile(r"^5[1-9A-HJ-NP-Za-km-z]{40,50}$")


@app.get("/api/faker/defaults")
def api_faker_defaults() -> Dict[str, Any]:
    """Expose the static Faker config (from backend .env) so the UI can
    surface which wallet/hotkey will sign. Secrets are never returned."""
    d = _faker_env_defaults()
    return {"status": "ok", **d}


@app.post("/api/faker/submit")
async def api_faker_submit(req: FakerSubmitReq) -> Dict[str, Any]:
    """Submit a single decoy ``Proxy.proxy`` staking extrinsic.

    Uses the warm :class:`faker.Faker` singleton so timing matches
    ``/api/trade`` (compose → sign → submit only — no block subscription,
    no intentional delay, no repeat loop).
    """
    t_start = time.time()

    action = (req.action or "").strip().lower()
    if action not in {"add", "remove", "swap"}:
        return {"status": "error", "message": f"invalid action {req.action!r}"}
    if req.netuid is None or req.netuid < 0 or req.netuid > 65535:
        return {"status": "error", "message": "netuid out of range"}
    if action == "swap":
        if req.dest_netuid is None or req.dest_netuid < 0 or req.dest_netuid > 65535:
            return {"status": "error", "message": "dest_netuid required for swap"}
    if action != "remove" and (req.amount is None or req.amount <= 0):
        return {"status": "error", "message": "amount must be > 0"}
    if req.real and not _FAKER_SS58_RE.match(req.real.strip()):
        return {"status": "error", "message": "real address is not a valid ss58"}

    faker = get_faker()
    if faker is None:
        hint = faker_last_init_error() or ""
        base = (
            "Faker unavailable: check FAKE_WALLET, FAKE_HOTKEY, WALLET_PASSWORD "
            "in backend .env and that ~/.bittensor/wallets/<FAKE_WALLET>/ exists. "
        )
        return {"status": "error", "message": (base + hint).strip()}

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            None,
            lambda: faker.submit(
                action=action,
                netuid=int(req.netuid),
                amount_tao=float(req.amount or 0.0),
                dest_netuid=int(req.dest_netuid) if req.dest_netuid is not None else None,
                real=(req.real or "").strip() or None,
            ),
        )
    except Exception as exc:
        logger.exception("[faker] submit failed")
        return {"status": "error", "message": str(exc)}

    t_done = time.time()
    logger.info("FAKER timing: total=%.0fms", (t_done - t_start) * 1000)

    if not result.success:
        return {"status": "error", "message": result.message}

    resp: Dict[str, Any] = {"status": "ok"}
    if result.extrinsic_hash:
        resp["hash"] = result.extrinsic_hash
        resp["hashes"] = [result.extrinsic_hash]  # back-compat with current UI
    else:
        resp["hashes"] = []
    if result.elapsed_ms is not None:
        resp["elapsed"] = f"{result.elapsed_ms:.0f}ms"
    return resp


@app.websocket("/ws")
async def ws_mempool(websocket: WebSocket) -> None:
    if auth_enabled() and not websocket_authenticated(websocket.cookies):
        await websocket.close(code=1008)
        return
    await hub.register(websocket)
    try:
        if last_snapshot_json is not None:
            await websocket.send_text(last_snapshot_json)
        if last_wallet_json is not None:
            await websocket.send_text(last_wallet_json)
        # Request the next monitor cycle to rebroadcast even if the fingerprint
        # hasn't moved. Poll cadence is ``MEMPOOL_POLL_BUSY_SEC`` (~20ms) so
        # the reconnected client picks up a truly fresh snapshot within one
        # tick — regardless of whether ``last_snapshot_json`` has drifted from
        # the live mempool during the outage.
        _force_broadcast_event.set()
        while True:
            await websocket.receive()
    except WebSocketDisconnect:
        hub.unregister(websocket)


# Serve Vite production build if present (register API routes before this)
if FRONTEND_DIST is not None and FRONTEND_DIST.is_dir():
    from starlette.staticfiles import StaticFiles as StarletteStaticFiles

    app.mount(
        "/",
        StarletteStaticFiles(directory=str(FRONTEND_DIST), html=True),
        name="spa",
    )
