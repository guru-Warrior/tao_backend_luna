#!/usr/bin/env python3
"""
Bittensor Alpha Token Buy Bot

Two operating modes:
  1. NETUID watcher  — watches .env for NETUID changes and buys immediately.
  2. Copy-trade      — scans mempool + blocks; when a watched address
                       (WATCH_ADDRESSES) buys, this bot buys too.
                       When a SELL_ADDRESSES address buys, this bot SELLS
                       its own alpha on that subnet (full tolerance). If
                       HOME_NETUID is set (comma-separated), the bot also SELLS
                       every home subnet whenever the target buys any subnet
                       (all homes + bought subnet in one force_batch).

Staking target hotkey = WALLET_HOTKEY from .env (the validator you stake TO).
Signing key           = WALLET_NAME coldkey.
"""

import asyncio
import argparse
import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv, set_key
from bittensor_wallet import Wallet
from bittensor.core.async_subtensor import AsyncSubtensor

load_dotenv()

ENV_FILE          = Path(os.getenv("ENV_FILE", ".env"))
ENV_POLL_INTERVAL = 0.05   # 50 ms — fast enough to catch a file save within one frame

DEFAULT_NETWORK       = os.getenv("NETWORK", "finney")
DEFAULT_WALLET_NAME   = os.getenv("WALLET_NAME", "default")
DEFAULT_WALLET_HOTKEY = os.getenv("WALLET_HOTKEY", "default")
DEFAULT_WALLET_PATH   = os.getenv("WALLET_PATH", "~/.bittensor/wallets")
WALLET_PASSWORD       = os.getenv("WALLET_PASSWORD", "")
DEFAULT_TIP_RAO            = int(os.getenv("TIP_RAO", "5100000"))
PRICE_TOLERANCE_PCT        = float(os.getenv("PRICE_TOLERANCE_PCT", "2.0"))
COPY_TRADE_TOLERANCE_PCT   = float(os.getenv("COPY_TRADE_TOLERANCE_PCT",
                                              str(PRICE_TOLERANCE_PCT)))

TX_FEE_BUFFER_TAO = 0.005

# Dust threshold for stake positions. ``remove_stake_full_limit`` can leave a
# few rao of residual on a (hotkey, netuid) slot due to on-chain pool-math
# rounding; re-submitting a sell for those leftovers is always a no-op and only
# costs us an extra compose_call + sign + submit every block boundary. Skip
# anything under this threshold so the pre-sign + sell paths ignore dust
# entirely. Kept in sync with the backend's /api/bot/positions filter.
DUST_STAKE_RAO = int(os.getenv("DUST_STAKE_RAO", "1000000"))  # 0.001 α

# Copy-trade: ss58 addresses to mirror (comma-separated in .env)
WATCH_ADDRESSES: list[str] = [
    a.strip() for a in os.getenv("WATCH_ADDRESSES", "").split(",") if a.strip()
]
# Only mirror buys (add_stake), or also trigger on sells (remove_stake)?
COPY_ON_SELL = os.getenv("COPY_ON_SELL", "false").lower() in ("1", "true", "yes")

# Sell-trigger addresses: when these addresses BUY alpha on a subnet,
# we immediately SELL (unstake) our own alpha on that subnet.
SELL_ADDRESSES: list[str] = [
    a.strip() for a in os.getenv("SELL_ADDRESSES", "").split(",") if a.strip()
]

# Mirror-buy addresses: when these addresses SELL (remove_stake*) alpha on a
# subnet, we immediately BUY (stake) the same subnet ourselves. Independent
# from WATCH_ADDRESSES + COPY_ON_SELL — kept as a separate list so that pure
# `copy_buy` rules don't get forced into "buy on their sells too".
MIRROR_BUY_ADDRESSES: list[str] = [
    a.strip() for a in os.getenv("MIRROR_BUY_ADDRESSES", "").split(",") if a.strip()
]

# Copy-sell addresses: when these addresses SELL (remove_stake*) on subnet N,
# we SELL our alpha on the same subnet (``force_batch`` when multiple netuids).
COPY_SELL_ADDRESSES: list[str] = [
    a.strip() for a in os.getenv("COPY_SELL_ADDRESSES", "").split(",") if a.strip()
]

# Mirror sell + HOME_NETUID: comma-separated netuids (e.g. ``28,118``). When a
# SELL_ADDRESSES address buys subnet N, we SELL alpha on every listed home
# netuid and on N (only where we hold stake) in one ``force_batch`` tx.


def parse_home_netuids(raw: str | int | None) -> list[int]:
    """Parse ``HOME_NETUID`` env value: single int or comma-separated netuids."""
    if raw is None:
        return []
    if isinstance(raw, int):
        return [raw] if raw > 0 else []
    out: list[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if part.isdigit():
            n = int(part)
            if 0 < n <= 65535 and n not in out:
                out.append(n)
    return sorted(out)


# ── .env helpers ───────────────────────────────────────────────────────────────

def reload_env() -> dict:
    load_dotenv(dotenv_path=ENV_FILE, override=True)
    try:
        netuid = int(os.getenv("NETUID", "0"))
    except (ValueError, TypeError):
        netuid = 0
    try:
        amount = float(os.getenv("AMOUNT", "0"))
    except (ValueError, TypeError):
        amount = 0.0
    try:
        copy_trade_amount = float(os.getenv("COPY_TRADE_AMOUNT", str(amount)))
    except (ValueError, TypeError):
        copy_trade_amount = amount
    try:
        price_tolerance_pct = float(os.getenv("PRICE_TOLERANCE_PCT",
                                               str(PRICE_TOLERANCE_PCT)))
    except (ValueError, TypeError):
        price_tolerance_pct = PRICE_TOLERANCE_PCT
    try:
        copy_trade_tolerance_pct = float(os.getenv("COPY_TRADE_TOLERANCE_PCT",
                                                    str(price_tolerance_pct)))
    except (ValueError, TypeError):
        copy_trade_tolerance_pct = price_tolerance_pct
    watch_addresses = [
        a.strip() for a in os.getenv("WATCH_ADDRESSES", "").split(",") if a.strip()
    ]
    copy_on_sell = os.getenv("COPY_ON_SELL", "false").lower() in ("1", "true", "yes")
    sell_addresses = [
        a.strip() for a in os.getenv("SELL_ADDRESSES", "").split(",") if a.strip()
    ]
    mirror_buy_addresses = [
        a.strip() for a in os.getenv("MIRROR_BUY_ADDRESSES", "").split(",") if a.strip()
    ]
    copy_sell_addresses = [
        a.strip() for a in os.getenv("COPY_SELL_ADDRESSES", "").split(",") if a.strip()
    ]
    home_netuids = parse_home_netuids(os.getenv("HOME_NETUID", ""))
    sell_netuids: list[int] = []
    for x in os.getenv("SELL_NETUIDS", "").split(","):
        x = x.strip()
        if x.isdigit():
            sell_netuids.append(int(x))
    return {
        "netuid":                  netuid,
        "amount":                  amount,
        "copy_trade_amount":       copy_trade_amount,
        "price_tolerance_pct":     price_tolerance_pct,
        "copy_trade_tolerance_pct": copy_trade_tolerance_pct,
        "wallet_hotkey":           os.getenv("WALLET_HOTKEY", DEFAULT_WALLET_HOTKEY).strip(),
        "watch_addresses":         watch_addresses,
        "copy_on_sell":            copy_on_sell,
        "sell_addresses":          sell_addresses,
        "mirror_buy_addresses": mirror_buy_addresses,
        "copy_sell_addresses":  copy_sell_addresses,
        "home_netuids":         home_netuids,
        "sell_netuids":         sell_netuids,
    }


# ── Blockchain helpers ─────────────────────────────────────────────────────────

def limit_price_rao(current_price_tao: float, tolerance_pct: float) -> int:
    return int(current_price_tao * (1.0 + tolerance_pct / 100.0) * 1e9)


def is_ss58_address(value: str) -> bool:
    return bool(value) and len(value) >= 47 and value[0].isdigit()


async def resolve_hotkey(subtensor: AsyncSubtensor, netuid: int,
                         wallet: Wallet, wallet_hotkey_name: str | None) -> str:
    if wallet_hotkey_name and wallet_hotkey_name.lower() != "default":
        if is_ss58_address(wallet_hotkey_name):
            print(f"[bot] Staking TO ss58: {wallet_hotkey_name}")
            return wallet_hotkey_name
        try:
            hw  = Wallet(name=wallet.name, hotkey=wallet_hotkey_name, path=wallet.path)
            ss58 = hw.hotkey.ss58_address
            print(f"[bot] Staking TO hotkey '{wallet_hotkey_name}' → {ss58}")
            return ss58
        except Exception as exc:
            print(f"[bot] WARNING: could not load hotkey '{wallet_hotkey_name}': {exc}")
            print(f"[bot] Falling back to top validator …")

    metagraph = await subtensor.metagraph(netuid)
    ranked = sorted(
        ((uid, metagraph.hotkeys[uid], metagraph.stake[uid])
         for uid in range(len(metagraph.hotkeys))
         if metagraph.stake[uid] > 0),
        key=lambda x: x[2], reverse=True,
    )
    if not ranked:
        raise RuntimeError(f"No validators found on subnet {netuid}")
    uid, hotkey, stake = ranked[0]
    print(f"[bot] Auto top validator  uid={uid}  hotkey={hotkey}  stake={stake:.4f}")
    return hotkey


# ── Core buy ───────────────────────────────────────────────────────────────────

async def buy_now(subtensor: AsyncSubtensor, wallet,
                  netuid: int, tao_amount: float,
                  wallet_hotkey_name: str, tip: int,
                  tolerance_pct: float | None = None) -> bool:
    """
    Build, sign, and submit the buy extrinsic immediately.
    No waiting for any block boundary.
    """
    tol = tolerance_pct if tolerance_pct is not None else PRICE_TOLERANCE_PCT
    coldkey_ss58 = wallet.coldkeypub.ss58_address
    t0 = time.time()

    # Fetch balance, block, subnet price, and hotkey all in parallel
    balance, current_block, subnet_info, hotkey = await asyncio.gather(
        subtensor.get_balance(coldkey_ss58),
        subtensor.get_current_block(),
        subtensor.subnet(netuid=netuid),
        resolve_hotkey(subtensor, netuid, wallet, wallet_hotkey_name),
    )
    print(f"[bot] Pre-flight fetched in {time.time()-t0:.3f}s  block=#{current_block}")

    required = tao_amount + TX_FEE_BUFFER_TAO
    if balance.tao < required:
        print(f"[bot] ERROR: insufficient balance — need {required:.6f}, "
              f"have {balance.tao:.6f} TAO")
        return False
    print(f"[bot] Balance: {balance.tao:.6f} TAO")

    cur_price    = float(subnet_info.price.tao)
    lp_rao       = limit_price_rao(cur_price, tol)
    approx_alpha = tao_amount / cur_price if cur_price else 0
    print(f"[bot] Subnet {netuid}  price={cur_price:.9f} TAO/α  "
          f"lp_rao={lp_rao}  ~{approx_alpha:.4f} α  tol={tol}%")
    print(f"[bot] Staking TO hotkey: {hotkey}")

    # Build & sign
    amount_rao = int(tao_amount * 1e9)
    t_sign = time.time()
    stake_call = await subtensor.substrate.compose_call(
        call_module="SubtensorModule",
        call_function="add_stake_limit",
        call_params={
            "hotkey":        hotkey,
            "netuid":        netuid,
            "amount_staked": amount_rao,
            "limit_price":   lp_rao,
            "allow_partial": False,
        },
    )
    batch_call = await subtensor.substrate.compose_call(
        call_module="Utility",
        call_function="force_batch",
        call_params={"calls": [stake_call]},
    )
    extrinsic = await subtensor.substrate.create_signed_extrinsic(
        call=batch_call,
        keypair=wallet.coldkey,
        era={"period": 64, "current": current_block},
        tip=tip,
    )
    print(f"[bot] Signed in {time.time() - t_sign:.3f}s")

    # Submit immediately
    t_submit = time.time()
    print(f"[bot] Submitting now …  t={t_submit:.3f}")
    try:
        receipt = await subtensor.substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=False,
            wait_for_finalization=False,
        )
        elapsed = time.time() - t_submit
        print(f"[bot] ✅  Submitted in {elapsed:.3f}s  "
              f"(total: {time.time() - t0:.3f}s)")
        print(f"[bot]    Netuid  : {netuid}")
        print(f"[bot]    TAO     : {tao_amount}")
        print(f"[bot]    Hotkey  : {hotkey}")
        print(f"[bot]    Hash    : {receipt.extrinsic_hash}")
        return True
    except Exception as exc:
        print(f"[bot] ❌  Submit error: {exc}")
        return False


# ── Core sell ──────────────────────────────────────────────────────────────────

async def _get_stake_positions(subtensor: AsyncSubtensor,
                               coldkey_ss58: str) -> list[dict]:
    """
    Query StakeInfoRuntimeApi to get all actual stake positions for a coldkey.
    Returns list of {hotkey_ss58, netuid, stake_rao} for positions with stake
    at or above ``DUST_STAKE_RAO``. Positions below the dust threshold are the
    un-sellable residue of a previous ``remove_stake_full_limit`` and are
    skipped so the pre-sign loop + sell paths don't waste work on them.
    Falls back to empty list on error.
    """
    from substrateinterface.utils.ss58 import ss58_encode
    try:
        result = await subtensor.substrate.runtime_call(
            "StakeInfoRuntimeApi", "get_stake_info_for_coldkey", [coldkey_ss58]
        )
        positions = []
        for entry in result.value:
            stake_rao = int(entry["stake"])
            # Skip dust (see DUST_STAKE_RAO): these rows are un-sellable
            # leftovers from a full unstake and only waste presign/sign work.
            if stake_rao < DUST_STAKE_RAO:
                continue
            raw = entry["hotkey"]
            # Decode nested tuple format: ((b1,...,b32),) or (b1,...,b32)
            if isinstance(raw, (tuple, list)) and isinstance(raw[0], (tuple, list)):
                raw = bytes(raw[0])
            else:
                raw = bytes(raw)
            positions.append({
                "hotkey_ss58": ss58_encode(raw, ss58_format=42),
                "netuid":      int(entry["netuid"]),
                "stake_rao":   stake_rao,
            })
        return positions
    except Exception as exc:
        print(f"[bot] Warning: get_stake_positions failed: {exc}")
        return []


def _calc_sell_era(current_block: int, period: int = 64) -> dict:
    """
    Compute a mortal era for sell transactions.
    Tx is valid for `period` blocks starting from current_block (~12.8 min with period=64).
    Using a large period prevents "Invalid Transaction" errors caused by the
    block advancing during the ~1-3s it takes to fetch stake positions and sign.
    """
    return {"period": period, "current": current_block}


async def sell_all_positions(subtensor: AsyncSubtensor, wallet,
                             netuids: list[int], tip: int,
                             current_block: int | None = None) -> bool:
    """
    Auto-discover actual (hotkey, netuid) stake positions via StakeInfoRuntimeApi,
    then unstake ALL of them in a single force_batch transaction.

    Uses remove_stake_full_limit (no amount_unstaked parameter needed):
      - limit_price = None  →  no price floor, accepts any execution price
    Era: next block is the LAST valid block in an 8-block window (~96 s).
    """
    if not netuids:
        return False

    t0 = time.time()
    coldkey_ss58 = wallet.coldkeypub.ss58_address
    netuid_set   = set(netuids)

    # Fetch block, nonce, and stake positions in parallel
    if current_block is None:
        current_block, nonce, positions = await asyncio.gather(
            subtensor.get_current_block(),
            subtensor.substrate.get_account_nonce(coldkey_ss58),
            _get_stake_positions(subtensor, coldkey_ss58),
        )
    else:
        nonce, positions = await asyncio.gather(
            subtensor.substrate.get_account_nonce(coldkey_ss58),
            _get_stake_positions(subtensor, coldkey_ss58),
        )

    targets = [
        (p["hotkey_ss58"], p["netuid"], p["stake_rao"])
        for p in positions
        if p["netuid"] in netuid_set
    ]

    if not targets:
        print(f"[bot] ⚠️  No stake found on netuids={netuids} — nothing to sell.")
        print(f"[bot]    All positions: {[(p['netuid'], round(p['stake_rao']/1e9, 4)) for p in positions]}")
        return False

    era = _calc_sell_era(current_block)  # period=64 default → ~12.8 min validity window
    print(f"[bot] SELL {len(targets)} position(s)  block=#{current_block}  era={era}")
    for hotkey, netuid, rao in targets:
        print(f"[bot]    netuid={netuid}  hotkey={hotkey}  alpha≈{rao/1e9:.4f}")

    t_sign = time.time()
    # remove_stake_full_limit: no amount_unstaked, limit_price=None means any price
    sell_calls = await asyncio.gather(*[
        subtensor.substrate.compose_call(
            call_module="SubtensorModule",
            call_function="remove_stake_full_limit",
            call_params={
                "hotkey":      hotkey,
                "netuid":      netuid,
                "limit_price": None,
            },
        )
        for hotkey, netuid, _ in targets
    ])

    batch_call = await subtensor.substrate.compose_call(
        call_module="Utility",
        call_function="force_batch",
        call_params={"calls": list(sell_calls)},
    )
    current_tip = tip
    for attempt in range(1, 4):  # up to 3 attempts with escalating tip
        extrinsic = await subtensor.substrate.create_signed_extrinsic(
            call=batch_call,
            keypair=wallet.coldkey,
            era=era,
            tip=current_tip,
            nonce=str(nonce),
        )
        print(f"[bot] Signed in {time.time() - t_sign:.3f}s  (attempt {attempt}, tip={current_tip} RAO)")

        t_submit = time.time()
        print(f"[bot] Submitting SELL now …  t={t_submit:.3f}")
        try:
            receipt = await subtensor.substrate.submit_extrinsic(
                extrinsic,
                wait_for_inclusion=False,
                wait_for_finalization=False,
            )
            elapsed = time.time() - t_submit
            print(f"[bot] ✅  SELL submitted in {elapsed:.3f}s  "
                  f"(total: {time.time() - t0:.3f}s)")
            print(f"[bot]    Hash: {receipt.extrinsic_hash}")
            return True
        except Exception as exc:
            err = str(exc)
            print(f"[bot] ❌  SELL submit error (attempt {attempt}): {err}")
            if "priority is too low" in err.lower() and attempt < 3:
                current_tip = current_tip * 10
                print(f"[bot] ⚡  Retrying with 10× tip = {current_tip} RAO …")
                t_sign = time.time()
                continue
            return False


async def sell_now(subtensor: AsyncSubtensor, wallet,
                   netuid: int, tip: int,
                   hotkey_ss58: str | None = None,
                   current_block: int | None = None) -> bool:
    """Unstake all alpha on a single netuid. Auto-discovers the correct hotkey."""
    return await sell_all_positions(
        subtensor=subtensor, wallet=wallet,
        netuids=[netuid], tip=tip, current_block=current_block,
    )


async def sell_netuids_batch(subtensor: AsyncSubtensor, wallet,
                              netuids: list[int], tip: int,
                              hotkey_ss58: str | None = None,
                              current_block: int | None = None) -> bool:
    """Unstake all alpha across multiple netuids. Auto-discovers correct hotkeys."""
    return await sell_all_positions(
        subtensor=subtensor, wallet=wallet,
        netuids=netuids, tip=tip, current_block=current_block,
    )


# ── Watcher (pm2 / always-on mode) ────────────────────────────────────────────

class EnvWatcher:
    def __init__(self, wallet, network: str, tip: int):
        self.wallet         = wallet
        self.network        = network
        self.tip            = tip
        self.last_netuid: int | None = None
        self.last_sell_netuids: set[int] = set()
        self.env_mtime: float            = 0.0

    def _env_changed(self) -> bool:
        if not ENV_FILE.exists():
            return False
        mtime = ENV_FILE.stat().st_mtime
        if mtime != self.env_mtime:
            self.env_mtime = mtime
            return True
        return False

    async def run(self):
        print(f"[watcher] Monitoring {ENV_FILE.absolute()} for NETUID / SELL_NETUIDS changes")
        print(f"[watcher] Poll interval: {ENV_POLL_INTERVAL}s — Ctrl+C to stop\n")

        # On startup: always start from 0 so any NETUID the user saves from
        # the dashboard fires a buy regardless of what was in .env previously.
        env = reload_env()
        self.last_netuid: int = 0
        self.last_sell_netuids = set(env["sell_netuids"])
        self._env_changed()   # consume current mtime
        if env["netuid"] > 0:
            print(f"[watcher] Startup — NETUID={env['netuid']} in .env (will buy on next save).")
        else:
            print(f"[watcher] Startup — NETUID not set. Enter a NETUID in .env to buy.")
        if self.last_sell_netuids:
            print(f"[watcher] Startup — SELL_NETUIDS={self.last_sell_netuids} recorded (no sell). "
                  f"Change SELL_NETUIDS to trigger a sell.")

        # Keep a persistent connection so there is zero connection overhead
        # when a NETUID change is detected.  If the connection drops, reconnect
        # immediately and keep watching.
        while True:
            try:
                async with AsyncSubtensor(network=self.network) as subtensor:
                    print(f"[watcher] Connected to {self.network} — ready.\n")
                    while True:
                        await asyncio.sleep(ENV_POLL_INTERVAL)

                        if not self._env_changed():
                            continue

                        env = reload_env()

                        # ── Mode switch: WATCH/SELL/MIRROR_BUY added → restart ──
                        if (
                            env["watch_addresses"]
                            or env["sell_addresses"]
                            or env.get("mirror_buy_addresses")
                            or env.get("copy_sell_addresses")
                        ):
                            print(f"\n[watcher] WATCH / SELL / MIRROR_BUY / COPY_SELL "
                                  f"addresses detected in .env — "
                                  f"switching to copy-trade mode (restarting) …")
                            sys.exit(0)  # pm2 restarts; main() will enter copy-trade mode

                        # ── SELL_NETUIDS: any newly added netuid triggers immediate sell ──
                        new_sell = set(env["sell_netuids"]) - self.last_sell_netuids
                        if new_sell:
                            self.last_sell_netuids = set()
                            netuids_to_sell = sorted(new_sell)
                            print(f"\n[watcher] SELL_NETUIDS — {netuids_to_sell} detected, selling now …")
                            # Clear SELL_NETUIDS immediately so a future re-buy of the same
                            # subnet won't auto-sell again.
                            set_key(str(ENV_FILE), "SELL_NETUIDS", "", quote_mode="never")
                            await self._do_sell(subtensor, env, netuids_to_sell)
                        else:
                            self.last_sell_netuids = set(env["sell_netuids"])

                        # ── NETUID buy watcher ────────────────────────────────────────────
                        if env["netuid"] <= 0 or env["amount"] <= 0:
                            print(f"[watcher] .env changed but NETUID={env['netuid']} or "
                                  f"AMOUNT={env['amount']} not valid, skipping.")
                            self.last_netuid = env["netuid"]
                            continue

                        if env["netuid"] == self.last_netuid:
                            if not new_sell:
                                print(f"[watcher] .env changed but NETUID unchanged "
                                      f"({env['netuid']}), skipping.")
                            continue

                        print(f"\n[watcher] NETUID changed: {self.last_netuid} → {env['netuid']}  "
                              f"AMOUNT={env['amount']}  WALLET_HOTKEY={env['wallet_hotkey']}")
                        # Reset to 0 so the same NETUID can trigger again on the next save
                        self.last_netuid = 0
                        # Clear NETUID from .env so bot restarts cleanly and dashboard
                        # shows the field as empty (buy was consumed)
                        set_key(str(ENV_FILE), "NETUID", "", quote_mode="never")
                        await self._do_buy(subtensor, env)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[watcher] Connection lost: {exc} — reconnecting in 2s …")
                await asyncio.sleep(2)

    async def _do_buy(self, subtensor: AsyncSubtensor, env: dict):
        print(f"\n{'='*60}")
        print(f"  BUY  netuid={env['netuid']}  amount={env['amount']} TAO  "
              f"hotkey={env['wallet_hotkey']}  tol={env['price_tolerance_pct']}%")
        print(f"{'='*60}")
        try:
            await buy_now(
                subtensor=subtensor,
                wallet=self.wallet,
                netuid=env["netuid"],
                tao_amount=env["amount"],
                wallet_hotkey_name=env["wallet_hotkey"],
                tip=self.tip,
                tolerance_pct=env["price_tolerance_pct"],
            )
        except Exception as exc:
            print(f"[watcher] Error during buy: {exc}")
            import traceback
            traceback.print_exc()

    async def _do_sell(self, subtensor: AsyncSubtensor, env: dict, netuids: list[int]):
        print(f"\n{'='*60}")
        print(f"  SELL_NETUIDS  netuids={netuids}  (auto-discover hotkeys, full tolerance)")
        print(f"{'='*60}")
        try:
            await sell_all_positions(subtensor=subtensor, wallet=self.wallet,
                                     netuids=netuids, tip=self.tip)
        except Exception as exc:
            print(f"[watcher] Error during sell netuids={netuids}: {exc}")
            import traceback
            traceback.print_exc()



# ── Copy-trade watcher ─────────────────────────────────────────────────────────

# Extrinsic call names (lowercase) that signal a buy or sell of alpha tokens.
_BUY_FNS  = {"add_stake", "add_stake_limit"}
_SELL_FNS = {"remove_stake", "remove_stake_limit"}


def _extrinsic_signer(ext) -> str | None:
    """Return the ss58 address that signed the extrinsic, or None."""
    try:
        # ext.value is a dict for decoded extrinsics
        v = ext.value if isinstance(ext.value, dict) else {}
        addr = v.get("address")
        if not addr:
            # fallback: some versions nest under "signed_by" or "signature"
            sig = v.get("signature") or {}
            addr = sig.get("address") or sig.get("signer")
        return addr
    except Exception:
        return None


def _extract_ss58(value) -> str | None:
    """
    Normalise a hotkey/address value that may come in several forms:
      - plain ss58 string: "5Abc..."
      - AccountId hex dict: {"AccountId": "0xdeadbeef..."}
      - raw hex string: "0xdeadbeef..."
    Returns a clean ss58 string, or None if it can't be resolved.
    """
    if not value:
        return None
    if isinstance(value, str):
        if value.startswith("0x"):
            # raw hex AccountId — convert to ss58
            try:
                from scalecodec.utils.ss58 import ss58_encode
                return ss58_encode(bytes.fromhex(value[2:]), ss58_format=42)
            except Exception:
                return None
        return value  # already ss58
    if isinstance(value, dict):
        # e.g. {"AccountId": "0x..."} or {"Id": "5Abc..."}
        inner = next(iter(value.values()), None)
        return _extract_ss58(inner)
    return None


def _extrinsic_call_info(ext) -> tuple[str, str, dict] | None:
    """
    Return (module, function, params) for a decoded extrinsic, or None.
    Handles both plain calls and force_batch / batch wrappers.
    """
    try:
        call = ext.value.get("call") or {}
        module   = call.get("call_module", "")
        function = call.get("call_function", "")
        params_list = call.get("call_args") or []
        params = {p["name"]: p["value"] for p in params_list if isinstance(p, dict)}
        return module, function, params
    except Exception:
        return None


def _unwrap_batch_calls(ext) -> list[tuple[str, str, dict]]:
    """
    Return a flat list of (module, function, params) tuples from an extrinsic.
    Unwraps force_batch / batch / batch_all wrappers one level deep.
    """
    info = _extrinsic_call_info(ext)
    if info is None:
        return []
    module, function, params = info
    if function in ("force_batch", "batch", "batch_all"):
        calls = params.get("calls") or []
        results = []
        for c in calls:
            try:
                if not isinstance(c, dict):
                    continue
                # Inner batch calls may be raw dicts with "call_module" / "call_function"
                # directly, or nested under a "call" key — handle both.
                inner = c.get("call") or c
                m = inner.get("call_module", "")
                f = inner.get("call_function", "")
                p_list = inner.get("call_args") or []
                p = {x["name"]: x["value"] for x in p_list if isinstance(x, dict) and "name" in x}
                results.append((m, f, p))
            except Exception:
                pass
        return results
    return [(module, function, params)]


class CopyTradeWatcher:
    """
    Subscribes to every new block and scans extrinsics.

    Triggers:
      * WATCH_ADDRESSES        — they BUY  → we BUY (also on their SELL if
        COPY_ON_SELL=true).
      * COPY_SELL_ADDRESSES    — they SELL → we SELL the same subnet(s).
      * SELL_ADDRESSES (+ optional HOME_NETUID) — they BUY on subnet N  → we
        SELL on N; if HOME_NETUID lists homes we also SELL every home (one batch).
      * MIRROR_BUY_ADDRESSES   — they SELL → we BUY the same subnet. Independent
        of COPY_ON_SELL so pure copy_buy rules don't get forced into mirroring
        the target's sells.
    """

    def __init__(self, wallet: Wallet, network: str, tip: int,
                 amount: float, copy_on_sell: bool, watch_addresses: list[str],
                 sell_addresses: list[str],
                 mirror_buy_addresses: list[str] | None = None,
                 copy_sell_addresses: list[str] | None = None,
                 home_netuids: list[int] | None = None):
        self.wallet          = wallet
        self.network         = network
        self.tip             = tip
        self.amount          = amount
        self.copy_trade_amount = float(os.getenv("COPY_TRADE_AMOUNT", str(amount)))
        self.copy_on_sell    = copy_on_sell
        self.watch_addresses = set(addr.lower() for addr in watch_addresses)
        self.sell_addresses  = set(addr.lower() for addr in sell_addresses)
        # mirror_buy: their SELL → our BUY (independent of copy_on_sell).
        self.mirror_buy_addresses = set(
            addr.lower() for addr in (mirror_buy_addresses or [])
        )
        self.copy_sell_addresses = set(
            addr.lower() for addr in (copy_sell_addresses or [])
        )
        self.home_netuids = list(home_netuids or [])
        # Track tx hashes already acted on — prevents double-firing if the same
        # pending tx is seen across multiple mempool polls before it's mined.
        self._seen_mempool: set[str] = set()
        # Block-level cooldown per netuid for buys and sells separately
        self._recent: dict[int, int] = {}        # netuid → last_block_bought
        self._recent_sell: dict[int, int] = {}   # netuid → last_block_sold
        self._cooldown_blocks = 2
        # Permanent per-session buy guard: once we've bought a netuid, never
        # buy it again until the bot restarts.  Prevents triple-spending when
        # the same (or multiple) watched addresses buy the same subnet more
        # than once within a session.
        self._bought_netuids: set[int] = set()
        # Per-block-window tracking of what was already triggered via mempool.
        # The block-scanner fallback checks these first and skips if the
        # mempool already handled the trigger.  Cleared on every new block.
        # This ensures: if MEV-protection hides a tx from the mempool,
        # the block scanner still fires; if the mempool saw it first,
        # the block scanner silently skips.
        self._mempool_done_buys: set[int] = set()   # netuids bought via mempool
        self._mempool_done_sells: set[int] = set()  # netuids sold  via mempool
        # Manual sell-netuid tracking
        env0 = reload_env()
        self._last_sell_netuids: set[int] = set(env0["sell_netuids"])
        # Manual buy-netuid tracking (NETUID watcher in copy-trade mode)
        # Always start at 0 — the .env poll fires on mtime change only, so any
        # NETUID written from the dashboard will always be treated as a new trigger.
        self._last_manual_netuid: int = 0
        # .env mtime — check every loop iteration (0.1s) for SELL_NETUIDS / SELL_ADDRESSES
        self._env_mtime: float = ENV_FILE.stat().st_mtime if ENV_FILE.exists() else 0.0
        # ── Pre-sign cache ──────────────────────────────────────────────────
        # Every block we background-refresh stake positions and pre-sign the
        # sell extrinsic.  When a SELL trigger fires we just submit() the
        # pre-built tx — no extra RPC round-trips in the critical path.
        self._presigned_sells: dict[int, object] = {}   # netuid → signed extrinsic
        self._pos_cache: list[dict] = []                # last known stake positions
        self._pos_cache_block: int  = 0
        self._presign_lock = asyncio.Lock()

    async def _held_netuids(self, subtensor: AsyncSubtensor) -> list[int]:
        """All netuids where this coldkey holds stake at or above the dust threshold."""
        positions = self._pos_cache
        if not positions:
            coldkey_ss58 = self.wallet.coldkeypub.ss58_address
            positions = await _get_stake_positions(subtensor, coldkey_ss58)
        return sorted({
            int(p["netuid"])
            for p in positions
            if int(p["stake_rao"]) >= DUST_STAKE_RAO
        })

    def _mirror_sell_blocked(self, netuids: list[int], block_number: int) -> bool:
        """Cooldown / mempool dedup for a mirror-sell target netuid set."""
        for netuid in netuids:
            if netuid in self._mempool_done_sells:
                return True
            if block_number - self._recent_sell.get(netuid, 0) < self._cooldown_blocks:
                return True
        return False

    def _mark_mirror_sell_done(self, netuids: list[int], block_number: int) -> None:
        for netuid in netuids:
            self._recent_sell[netuid] = block_number
            self._mempool_done_sells.add(netuid)

    async def _resolve_mirror_sell_netuids(
        self, subtensor: AsyncSubtensor, their_buy_netuid: int,
    ) -> list[int]:
        """
        Mirror sell targets for one of the target's buys:
          - HOME_NETUID unset → sell only the subnet they bought.
          - HOME_NETUID set (comma-separated) → sell all home netuids + bought
            (deduped), limited to subnets where we hold stake. Multiple homes
            are sold together via ``force_batch`` in ``_do_sell_batch``.
        Example: home=28,118, they buy 119 → sell [28, 118, 119] when held.
        """
        if their_buy_netuid == 0:
            return []
        if self.home_netuids:
            candidates = sorted(set(self.home_netuids) | {their_buy_netuid})
        else:
            candidates = [their_buy_netuid]
        held = set(await self._held_netuids(subtensor))
        if held:
            return [n for n in candidates if n in held]
        return candidates

    async def _refresh_and_presign(self, subtensor: AsyncSubtensor, current_block: int):
        """
        Background task: fetch stake positions + nonce, then pre-sign a sell
        extrinsic for every netuid where we hold stake.

        Runs once on startup and is re-scheduled on every new block boundary
        (~12 s cadence).  The resulting pre-signed extrinsics are stored in
        self._presigned_sells so that _do_sell can submit without ANY extra
        RPC calls (sub-100 ms critical path vs. 400–700 ms without caching).
        """
        coldkey_ss58 = self.wallet.coldkeypub.ss58_address
        async with self._presign_lock:
            if current_block <= self._pos_cache_block:
                return
            try:
                nonce, positions = await asyncio.gather(
                    subtensor.substrate.get_account_nonce(coldkey_ss58),
                    _get_stake_positions(subtensor, coldkey_ss58),
                )
                self._pos_cache       = positions
                self._pos_cache_block = current_block

                # Group positions by netuid and pre-sign one extrinsic per
                # netuid. ``_get_stake_positions`` already drops dust, this
                # secondary gate is belt-and-suspenders so a future change to
                # the fetcher can't silently reintroduce wasted presigns.
                netuid_to_targets: dict[int, list[tuple[str, int]]] = {}
                for p in positions:
                    if p["stake_rao"] >= DUST_STAKE_RAO:
                        netuid_to_targets.setdefault(p["netuid"], []).append(
                            (p["hotkey_ss58"], p["netuid"])
                        )

                new_presigned: dict[int, object] = {}
                for netuid, targets in netuid_to_targets.items():
                    try:
                        sell_calls = await asyncio.gather(*[
                            subtensor.substrate.compose_call(
                                call_module="SubtensorModule",
                                call_function="remove_stake_full_limit",
                                call_params={"hotkey": hk, "netuid": nu, "limit_price": None},
                            )
                            for hk, nu in targets
                        ])
                        batch_call = await subtensor.substrate.compose_call(
                            call_module="Utility",
                            call_function="force_batch",
                            call_params={"calls": list(sell_calls)},
                        )
                        extrinsic = await subtensor.substrate.create_signed_extrinsic(
                            call=batch_call,
                            keypair=self.wallet.coldkey,
                            era={"period": 64, "current": current_block},
                            tip=self.tip,
                            nonce=str(nonce),
                        )
                        new_presigned[netuid] = extrinsic
                    except Exception as e:
                        print(f"[presign] netuid={netuid} sign failed: {e}")

                self._presigned_sells = new_presigned
                if new_presigned:
                    print(f"[presign] ✓ block=#{current_block}  "
                          f"netuids={sorted(new_presigned)}  nonce={nonce}")
            except Exception as exc:
                print(f"[presign] refresh failed: {exc}")

    def _refresh_config(self) -> tuple[set[int], int | None]:
        """
        Reload hot config from .env without stopping the watcher.
        Returns (new_sell_netuids, new_manual_netuid_or_None).
        new_manual_netuid is set when NETUID changes to a positive value.
        """
        load_dotenv(dotenv_path=ENV_FILE, override=True)
        try:
            self.amount = float(os.getenv("AMOUNT", str(self.amount)))
        except (ValueError, TypeError):
            pass
        try:
            self.copy_trade_amount = float(os.getenv("COPY_TRADE_AMOUNT", str(self.copy_trade_amount)))
        except (ValueError, TypeError):
            pass
        self.copy_on_sell = os.getenv("COPY_ON_SELL", "false").lower() in ("1", "true", "yes")
        self.watch_addresses = set(
            a.strip().lower()
            for a in os.getenv("WATCH_ADDRESSES", "").split(",")
            if a.strip()
        )
        self.sell_addresses = set(
            a.strip().lower()
            for a in os.getenv("SELL_ADDRESSES", "").split(",")
            if a.strip()
        )
        self.mirror_buy_addresses = set(
            a.strip().lower()
            for a in os.getenv("MIRROR_BUY_ADDRESSES", "").split(",")
            if a.strip()
        )
        self.copy_sell_addresses = set(
            a.strip().lower()
            for a in os.getenv("COPY_SELL_ADDRESSES", "").split(",")
            if a.strip()
        )
        self.home_netuids = parse_home_netuids(os.getenv("HOME_NETUID", ""))
        current_sell_netuids: set[int] = set()
        for x in os.getenv("SELL_NETUIDS", "").split(","):
            x = x.strip()
            if x.isdigit():
                current_sell_netuids.add(int(x))
        new_netuids = current_sell_netuids - self._last_sell_netuids
        self._last_sell_netuids = current_sell_netuids

        # Manual buy: detect NETUID change
        try:
            current_netuid = int(os.getenv("NETUID", "0"))
        except (ValueError, TypeError):
            current_netuid = 0
        new_manual_netuid: int | None = None
        if current_netuid > 0 and current_netuid != self._last_manual_netuid:
            new_manual_netuid = current_netuid
        self._last_manual_netuid = current_netuid

        return new_netuids, new_manual_netuid

    async def run(self):
        print(f"[copy] Copy-trade watcher starting on network={self.network}")
        print(f"[copy] Watching {len(self.watch_addresses)} BUY address(es):")
        for a in self.watch_addresses:
            print(f"[copy]   {a}")
        if self.sell_addresses:
            print(f"[copy] Watching {len(self.sell_addresses)} SELL-trigger address(es):")
            for a in self.sell_addresses:
                print(f"[copy]   {a}")
        if self.mirror_buy_addresses:
            print(f"[copy] Watching {len(self.mirror_buy_addresses)} MIRROR-BUY address(es) "
                  f"(their SELL → our BUY):")
            for a in self.mirror_buy_addresses:
                print(f"[copy]   {a}")
        if self.copy_sell_addresses:
            print(f"[copy] Watching {len(self.copy_sell_addresses)} COPY-SELL address(es) "
                  f"(their SELL → our SELL):")
            for a in self.copy_sell_addresses:
                print(f"[copy]   {a}")
        if self.sell_addresses and self.home_netuids:
            print(f"[copy] Mirror-sell HOME_NETUID={','.join(str(n) for n in self.home_netuids)} "
                  f"(target buys any subnet → force_batch sell home(s) + that subnet)")
        print(f"[copy] AMOUNT={self.amount} TAO  COPY_ON_SELL={self.copy_on_sell}")
        print(f"[copy] Mode: MEMPOOL (same-block) + block fallback\n")

        async with AsyncSubtensor(network=self.network) as subtensor:
            last_block = await subtensor.get_current_block()
            # Pre-sign sell extrinsics immediately so the first trigger is fast
            await self._refresh_and_presign(subtensor, last_block)
            while True:
                try:
                    # ── Parallel fetch: mempool + block head ──────────────────
                    # Runs both RPC calls concurrently instead of sequentially,
                    # cutting per-iteration overhead from ~500ms to ~200ms.
                    fetch_results = await asyncio.gather(
                        subtensor.substrate.retrieve_pending_extrinsics(),
                        subtensor.substrate.get_chain_head(),
                        return_exceptions=True,
                    )
                    pending   = fetch_results[0] if not isinstance(fetch_results[0], Exception) else []
                    head_hash = fetch_results[1] if not isinstance(fetch_results[1], Exception) else None

                    if head_hash:
                        current_block = await subtensor.substrate.get_block_number(head_hash)
                    else:
                        current_block = last_block

                    # ── Scan mempool with known block (no extra RPC) ──────────
                    await self._scan_mempool(subtensor, pending, current_block)

                    await asyncio.sleep(0.05)

                    # ── Fast .env poll (no RPC calls) ─────────────────────────
                    # Detect SELL_NETUIDS / SELL_ADDRESSES / NETUID changes
                    # immediately, independent of block boundaries (~12s).
                    if ENV_FILE.exists():
                        mtime = ENV_FILE.stat().st_mtime
                        if mtime != self._env_mtime:
                            self._env_mtime = mtime
                            print(f"[copy] .env changed — path={ENV_FILE.absolute()}")
                            prev_sell_addresses = set(self.sell_addresses)
                            new_sell_netuids, new_manual_netuid = self._refresh_config()
                            print(f"[copy] .env poll — NETUID raw={os.getenv('NETUID')!r}  "
                                  f"new_manual_netuid={new_manual_netuid}  "
                                  f"_last={self._last_manual_netuid}")
                            new_sell_addresses = self.sell_addresses - prev_sell_addresses

                            # New NETUID → manual buy immediately
                            if new_manual_netuid is not None:
                                env = reload_env()
                                if env["amount"] > 0:
                                    print(f"\n[copy] Manual BUY triggered via .env — "
                                          f"netuid={new_manual_netuid}  amount={env['amount']} TAO")
                                    # Reset + clear .env so same NETUID fires again next time
                                    self._last_manual_netuid = 0
                                    set_key(str(ENV_FILE), "NETUID", "", quote_mode="never")
                                    await self._do_manual_buy(subtensor, new_manual_netuid,
                                                              current_block, env)
                                else:
                                    print(f"[copy] NETUID={new_manual_netuid} set but AMOUNT=0, skipping.")

                            # New SELL_NETUIDS → sell immediately (reuse current_block)
                            if new_sell_netuids:
                                netuids_list = sorted(new_sell_netuids)
                                print(f"\n[copy] SELL_NETUIDS — {netuids_list} triggered via .env, selling now …")
                                # Clear SELL_NETUIDS immediately so a future re-buy of the same
                                # subnet won't auto-sell again.
                                self._last_sell_netuids = set()
                                set_key(str(ENV_FILE), "SELL_NETUIDS", "", quote_mode="never")
                                if len(netuids_list) == 1:
                                    await self._do_sell(subtensor, netuids_list[0], current_block)
                                else:
                                    await self._do_sell_batch(subtensor, netuids_list, current_block)

                            # New SELL_ADDRESSES → re-scan with current pending list
                            if new_sell_addresses:
                                print(f"\n[copy] SELL_ADDRESSES changed — {len(new_sell_addresses)} new "
                                      f"address(es) added. Scanning mempool now …")
                                await self._scan_mempool(subtensor, pending, current_block)

                    # ── Block fallback: catch anything missed in mempool ───────
                    if current_block > last_block:
                        for blk in range(last_block + 1, current_block + 1):
                            await self._process_block(subtensor, blk)
                        last_block = current_block
                        # Prune old seen hashes — txs confirmed in blocks won't
                        # reappear in the mempool, so the set can be cleared.
                        self._seen_mempool.clear()
                        self._mempool_done_buys.clear()
                        self._mempool_done_sells.clear()
                        # Re-sign sell extrinsics for the new block in background.
                        # This ensures fresh nonce + era for the next SELL trigger.
                        asyncio.create_task(
                            self._refresh_and_presign(subtensor, current_block)
                        )

                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    print(f"[copy] Loop error: {exc}")
                    await asyncio.sleep(2)

    async def _scan_mempool(self, subtensor: AsyncSubtensor,
                            pending: list, current_block: int):
        """
        Process already-fetched pending extrinsics and fire immediately on trigger:
        - WATCH_ADDRESSES buying  → we buy the same subnet
        - SELL_ADDRESSES buying   → we sell our alpha on that subnet
        - COPY_SELL_ADDRESSES selling → we sell the same subnet(s)

        `pending` and `current_block` are passed in from the main loop so we
        don't make extra RPC calls here.
        """
        for ext in pending:
            try:
                tx_hash = ext.value.get("extrinsic_hash", "")
            except Exception:
                tx_hash = ""
            if tx_hash and tx_hash in self._seen_mempool:
                continue

            signer = _extrinsic_signer(ext)
            if not signer:
                continue

            signer_lower = signer.lower()
            is_watch = signer_lower in self.watch_addresses
            is_sell_trigger = signer_lower in self.sell_addresses
            is_mirror_buy = signer_lower in self.mirror_buy_addresses
            is_copy_sell = signer_lower in self.copy_sell_addresses
            if not is_watch and not is_sell_trigger and not is_mirror_buy and not is_copy_sell:
                continue

            calls = _unwrap_batch_calls(ext)

            # ── Collect mirror-sell / copy-sell netuids from this extrinsic ─
            # Batch into ONE sell tx per trigger type.
            pending_mirror_buys: list[int] = []
            pending_copy_sells: list[int] = []

            for module, function, params in calls:
                if module.lower() != "subtensormodule":
                    continue

                fn_lower = function.lower()
                is_buy_fn  = fn_lower in _BUY_FNS
                is_sell_fn = fn_lower in _SELL_FNS

                netuid = params.get("netuid")
                if netuid is None:
                    continue
                try:
                    netuid = int(netuid)
                except (ValueError, TypeError):
                    continue

                # ── SELL_ADDRESSES (mirror sell): they BUY ────────────────
                if is_sell_trigger and is_buy_fn:
                    if netuid not in pending_mirror_buys:
                        pending_mirror_buys.append(netuid)
                    continue

                # ── COPY_SELL_ADDRESSES: they SELL → we SELL same subnet ──
                if is_copy_sell and is_sell_fn:
                    if netuid not in pending_copy_sells:
                        pending_copy_sells.append(netuid)
                    continue

                # ── WATCH_ADDRESSES: they BUY (or SELL) → we BUY ──────────
                # ── MIRROR_BUY_ADDRESSES: they SELL → we BUY (per-addr opt-in)
                trigger_via_watch = is_watch and (
                    is_buy_fn or (is_sell_fn and self.copy_on_sell)
                )
                trigger_via_mirror_buy = is_mirror_buy and is_sell_fn
                if trigger_via_watch or trigger_via_mirror_buy:
                    if (
                        is_watch
                        and not is_buy_fn
                        and not (is_sell_fn and self.copy_on_sell)
                    ):
                        # Defensive — covered by the boolean above, kept for
                        # parity with the previous skip log.
                        if is_sell_fn and not self.copy_on_sell:
                            print(f"[mempool] {signer[:16]}… SELL pending "
                                  f"netuid={netuid}  (skipping)")
                        continue

                    if tx_hash:
                        self._seen_mempool.add(tx_hash)
                    last_bought = self._recent.get(netuid, 0)
                    if current_block - last_bought < self._cooldown_blocks:
                        print(f"[mempool] Skipping BUY netuid={netuid} — cooldown")
                        continue
                    if netuid in self._bought_netuids:
                        print(f"[mempool] Skipping BUY netuid={netuid} — already bought this session")
                        continue
                    if netuid == 0:
                        print(f"[mempool] Skipping BUY netuid=0 — subnet 0 ignored")
                        continue

                    their_hotkey = _extract_ss58(params.get("hotkey"))
                    if trigger_via_mirror_buy:
                        trigger = "MIRROR-BUY (their SELL)"
                    else:
                        trigger = "BUY" if is_buy_fn else "SELL"
                    print(f"\n[mempool] {signer[:16]}… {trigger} PENDING "
                          f"netuid={netuid}  hotkey={str(their_hotkey)[:16]}…"
                          f"  → firing OUR buy NOW (same block target)")
                    self._recent[netuid] = current_block
                    self._mempool_done_buys.add(netuid)
                    await self._do_buy(subtensor, netuid, current_block, their_hotkey)

            # ── Mirror sell: one batch tx per extrinsic ───────────────────
            sell_netuids: list[int] = []
            if pending_mirror_buys:
                merged: list[int] = []
                for bought in pending_mirror_buys:
                    for nu in await self._resolve_mirror_sell_netuids(subtensor, bought):
                        if nu not in merged:
                            merged.append(nu)
                sell_netuids = merged

            if sell_netuids:
                if self._mirror_sell_blocked(sell_netuids, current_block):
                    print(f"[mempool] Skipping mirror SELL {sell_netuids} — cooldown/done")
                    continue
                if tx_hash:
                    self._seen_mempool.add(tx_hash)
                self._mark_mirror_sell_done(sell_netuids, current_block)
                trigger = f"bought {pending_mirror_buys} → sell {sell_netuids}"
                if self.home_netuids:
                    trigger += f"  (HOME_NETUID={','.join(str(n) for n in self.home_netuids)})"
                print(f"\n[mempool] SELL-trigger {signer[:16]}… {trigger}"
                      f"  → firing OUR BATCH SELL NOW")
                if len(sell_netuids) == 1:
                    await self._do_sell(subtensor, sell_netuids[0], current_block)
                else:
                    await self._do_sell_batch(subtensor, sell_netuids, current_block)

            if pending_copy_sells:
                copy_sell_netuids = sorted(set(pending_copy_sells))
                if self._mirror_sell_blocked(copy_sell_netuids, current_block):
                    print(f"[mempool] Skipping copy SELL {copy_sell_netuids} — cooldown/done")
                else:
                    if tx_hash:
                        self._seen_mempool.add(tx_hash)
                    self._mark_mirror_sell_done(copy_sell_netuids, current_block)
                    print(f"\n[mempool] COPY-SELL {signer[:16]}… sold {pending_copy_sells} "
                          f"→ firing OUR SELL {copy_sell_netuids} NOW")
                    if len(copy_sell_netuids) == 1:
                        await self._do_sell(subtensor, copy_sell_netuids[0], current_block)
                    else:
                        await self._do_sell_batch(
                            subtensor, copy_sell_netuids, current_block
                        )

    async def _process_block(self, subtensor: AsyncSubtensor, block_number: int):
        """
        Block fallback using on-chain EVENTS (StakeAdded / StakeRemoved).

        Events are always visible on confirmed blocks even when MEV protection
        hides the extrinsic content from the mempool.  No extrinsic parsing
        needed — the event attributes give us coldkey, hotkey, and netuid
        directly from the chain result.
        """
        try:
            block_hash = await subtensor.substrate.get_block_hash(block_number)
            events     = await subtensor.substrate.get_events(block_hash)
        except Exception as exc:
            print(f"[copy] Could not fetch events for block #{block_number}: {exc}")
            return

        # Collect ALL sell-netuids from this block before firing any sell.
        # Multiple StakeAdded events from the same trigger address (batch buy)
        # must be batched into ONE sell tx to avoid nonce collisions.
        block_sell_netuids: list[int] = []
        block_copy_sell_netuids: list[int] = []

        for event in events:
            try:
                ev       = event.value if hasattr(event, "value") else event
                module   = ev.get("module_id", "")
                event_id = ev.get("event_id", "")

                if module != "SubtensorModule":
                    continue
                if event_id not in ("StakeAdded", "StakeRemoved"):
                    continue

                attrs = ev.get("attributes", [])
                # Attributes can be a list/tuple or a dict depending on SDK version.
                if isinstance(attrs, dict):
                    coldkey = str(attrs.get("coldkey", ""))
                    hotkey  = str(attrs.get("hotkey", ""))
                    netuid  = attrs.get("netuid") or attrs.get("netuid_id")
                    try:
                        netuid = int(netuid) if netuid is not None else None
                    except (ValueError, TypeError):
                        netuid = None
                elif isinstance(attrs, (list, tuple)) and len(attrs) >= 3:
                    coldkey = str(attrs[0])
                    hotkey  = str(attrs[1])
                    # Event format: (coldkey, hotkey, tao_amount, alpha_amount, netuid)
                    netuid = None
                    if len(attrs) >= 5:
                        try:
                            v = int(attrs[4])
                            if 0 <= v < 65536:
                                netuid = v
                        except (ValueError, TypeError):
                            pass
                else:
                    continue

                if not coldkey or netuid is None:
                    continue

                signer_lower    = coldkey.lower()
                is_watch        = signer_lower in self.watch_addresses
                is_sell_trigger = signer_lower in self.sell_addresses
                is_mirror_buy   = signer_lower in self.mirror_buy_addresses
                is_copy_sell    = signer_lower in self.copy_sell_addresses
                if not is_watch and not is_sell_trigger and not is_mirror_buy and not is_copy_sell:
                    continue

                is_added   = event_id == "StakeAdded"    # they staked (bought)
                is_removed = event_id == "StakeRemoved"  # they unstaked (sold)

                # ── SELL_ADDRESSES (mirror sell): they BOUGHT ─────────────
                if is_sell_trigger and is_added:
                    resolved = await self._resolve_mirror_sell_netuids(
                        subtensor, netuid
                    )
                    for nu in resolved:
                        if nu not in block_sell_netuids:
                            block_sell_netuids.append(nu)
                    continue

                # ── COPY_SELL_ADDRESSES: they SOLD → we SELL same subnet ─
                if is_copy_sell and is_removed:
                    if netuid not in block_copy_sell_netuids:
                        block_copy_sell_netuids.append(netuid)
                    continue

                # ── WATCH_ADDRESSES: they BOUGHT (or SOLD) → we BUY ───────
                # ── MIRROR_BUY_ADDRESSES: they SOLD → we BUY (per-addr) ───
                trigger_via_watch = is_watch and (
                    is_added or (is_removed and self.copy_on_sell)
                )
                trigger_via_mirror_buy = is_mirror_buy and is_removed
                if trigger_via_watch or trigger_via_mirror_buy:
                    if (
                        is_watch
                        and is_removed
                        and not self.copy_on_sell
                        and not is_mirror_buy
                    ):
                        print(f"[copy] Block #{block_number} — {coldkey[:16]}… SOLD "
                              f"netuid={netuid}  (COPY_ON_SELL=false → skipping)")
                        continue

                    if netuid in self._mempool_done_buys:
                        print(f"[copy] Block #{block_number} — {coldkey[:16]}… "
                              f"netuid={netuid}  already handled via mempool")
                        continue
                    if netuid in self._bought_netuids:
                        print(f"[copy] Block #{block_number} — {coldkey[:16]}… "
                              f"netuid={netuid}  already bought this session")
                        continue
                    if netuid == 0:
                        print(f"[copy] Block #{block_number} — {coldkey[:16]}… "
                              f"netuid=0 ignored")
                        continue

                    if trigger_via_mirror_buy:
                        action = "SOLD (mirror-buy → we BUY)"
                    else:
                        action = "BOUGHT" if is_added else "SOLD"
                    print(f"\n[copy] Block #{block_number} — {coldkey[:16]}… {action} "
                          f"netuid={netuid}  (event-based, mempool missed → block fallback)")
                    self._recent[netuid] = block_number
                    self._mempool_done_buys.add(netuid)
                    await self._do_buy(subtensor, netuid, block_number, hotkey)

            except Exception as exc:
                print(f"[copy] Block #{block_number} event parse error: {exc}")

        # ── Mirror sell batch (block fallback) ────────────────────────────
        if block_sell_netuids:
            if self._mirror_sell_blocked(block_sell_netuids, block_number):
                print(f"[copy] Block #{block_number} — mirror SELL {block_sell_netuids} "
                      f"skipped (cooldown/done)")
            else:
                self._mark_mirror_sell_done(block_sell_netuids, block_number)
                label = f"sell {block_sell_netuids}"
                if self.home_netuids:
                    label += f"  (HOME_NETUID={','.join(str(n) for n in self.home_netuids)})"
                print(f"\n[copy] Block #{block_number} — SELL-trigger {label} "
                      f"(event-based fallback)")
                if len(block_sell_netuids) == 1:
                    await self._do_sell(subtensor, block_sell_netuids[0], block_number)
                else:
                    await self._do_sell_batch(subtensor, block_sell_netuids, block_number)

        if block_copy_sell_netuids:
            if self._mirror_sell_blocked(block_copy_sell_netuids, block_number):
                print(f"[copy] Block #{block_number} — copy SELL {block_copy_sell_netuids} "
                      f"skipped (cooldown/done)")
            else:
                self._mark_mirror_sell_done(block_copy_sell_netuids, block_number)
                print(f"\n[copy] Block #{block_number} — COPY-SELL "
                      f"sell {block_copy_sell_netuids} (event-based fallback)")
                if len(block_copy_sell_netuids) == 1:
                    await self._do_sell(subtensor, block_copy_sell_netuids[0], block_number)
                else:
                    await self._do_sell_batch(
                        subtensor, block_copy_sell_netuids, block_number
                    )

    async def _do_buy(self, subtensor: AsyncSubtensor, netuid: int,
                      block_number: int, their_hotkey: str | None = None):
        if netuid == 0:
            print(f"[copy] _do_buy: skipping netuid=0")
            return
        env = reload_env()
        amount = env["copy_trade_amount"] if env["copy_trade_amount"] > 0 else self.copy_trade_amount
        copy_tol = env["copy_trade_tolerance_pct"]

        # Always stake to the user's own configured WALLET_HOTKEY.
        target_hotkey = env["wallet_hotkey"]

        # Mark immediately so concurrent triggers for the same netuid are blocked.
        self._bought_netuids.add(netuid)

        print(f"\n{'='*60}")
        print(f"  COPY-TRADE BUY  netuid={netuid}  amount={amount} TAO  tol={copy_tol}%")
        print(f"  stake TO hotkey : {target_hotkey}  (WALLET_HOTKEY)")
        print(f"  triggered at block #{block_number}")
        print(f"{'='*60}")
        try:
            await buy_now(
                subtensor=subtensor,
                wallet=self.wallet,
                netuid=netuid,
                tao_amount=amount,
                wallet_hotkey_name=target_hotkey,
                tip=self.tip,
                tolerance_pct=copy_tol,
            )
        except Exception as exc:
            print(f"[copy] Error during buy: {exc}")
            import traceback
            traceback.print_exc()


    async def _do_manual_buy(self, subtensor: AsyncSubtensor, netuid: int,
                              block_number: int, env: dict):
        """
        Manual buy triggered by NETUID change in .env (dashboard Buy button).
        Bypasses copy-trade bought-guard so it always fires regardless of
        whether this netuid was already copy-traded this session.
        Uses PRICE_TOLERANCE_PCT (manual tolerance, separate from copy-trade).
        """
        print(f"\n{'='*60}")
        print(f"  MANUAL BUY  netuid={netuid}  amount={env['amount']} TAO  "
              f"tol={env['price_tolerance_pct']}%")
        print(f"  hotkey={env['wallet_hotkey']}  block=#{block_number}")
        print(f"{'='*60}")
        try:
            await buy_now(
                subtensor=subtensor,
                wallet=self.wallet,
                netuid=netuid,
                tao_amount=env["amount"],
                wallet_hotkey_name=env["wallet_hotkey"],
                tip=self.tip,
                tolerance_pct=env["price_tolerance_pct"],
            )
        except Exception as exc:
            print(f"[copy] Error during manual buy: {exc}")
            import traceback
            traceback.print_exc()

    async def _do_sell(self, subtensor: AsyncSubtensor, netuid: int,
                       block_number: int):
        """Sell (unstake) ALL our alpha on netuid using full price tolerance.

        Fast path: if a pre-signed extrinsic is cached from the last block
        boundary refresh, submit it immediately (no extra RPC calls → sub-100 ms).
        Slow path fallback: auto-discover hotkey via StakeInfoRuntimeApi and
        build/sign/submit fresh (~400–700 ms).
        """
        print(f"\n{'='*60}")
        print(f"  SELL (unstake)  netuid={netuid}")
        print(f"  triggered at block #{block_number}")
        print(f"{'='*60}")

        # ── Fast path: pre-signed extrinsic ──────────────────────────────────
        async with self._presign_lock:
            presigned = self._presigned_sells.pop(netuid, None)
            cache_age = block_number - self._pos_cache_block

        if presigned is not None and cache_age <= 2:
            print(f"[bot] ⚡ Using pre-signed SELL (cache age={cache_age} block(s))")
            t0 = time.time()
            current_tip = self.tip
            for attempt in range(1, 4):
                try:
                    receipt = await subtensor.substrate.submit_extrinsic(
                        presigned,
                        wait_for_inclusion=False,
                        wait_for_finalization=False,
                    )
                    print(f"[bot] ✅  SELL (pre-signed) submitted in "
                          f"{time.time()-t0:.3f}s  hash={receipt.extrinsic_hash}")
                    return
                except Exception as exc:
                    err = str(exc)
                    print(f"[bot] ❌  Pre-signed submit error (attempt {attempt}): {err}")
                    if "priority is too low" in err.lower() and attempt < 3:
                        # Re-sign with escalated tip and try again
                        current_tip *= 10
                        print(f"[bot] ⚡  Retrying with 10× tip = {current_tip} RAO …")
                        try:
                            # Re-sign only the tip changed; we must rebuild from cache
                            targets = [
                                (p["hotkey_ss58"], p["netuid"])
                                for p in self._pos_cache
                                if p["netuid"] == netuid
                                and p["stake_rao"] >= DUST_STAKE_RAO
                            ]
                            if not targets:
                                break
                            sell_calls = await asyncio.gather(*[
                                subtensor.substrate.compose_call(
                                    call_module="SubtensorModule",
                                    call_function="remove_stake_full_limit",
                                    call_params={"hotkey": hk, "netuid": nu, "limit_price": None},
                                )
                                for hk, nu in targets
                            ])
                            batch_call = await subtensor.substrate.compose_call(
                                call_module="Utility",
                                call_function="force_batch",
                                call_params={"calls": list(sell_calls)},
                            )
                            coldkey_ss58 = self.wallet.coldkeypub.ss58_address
                            nonce = await subtensor.substrate.get_account_nonce(coldkey_ss58)
                            presigned = await subtensor.substrate.create_signed_extrinsic(
                                call=batch_call,
                                keypair=self.wallet.coldkey,
                                era={"period": 64, "current": block_number},
                                tip=current_tip,
                                nonce=str(nonce),
                            )
                        except Exception as sign_exc:
                            print(f"[bot] Re-sign failed: {sign_exc}")
                            break
                        continue
                    # Non-retryable error → fall through to slow path
                    print(f"[bot] Non-retryable error, falling back to fresh sign …")
                    break
            else:
                return  # all attempts exhausted via loop (shouldn't reach here)

        # ── Slow path fallback ────────────────────────────────────────────────
        print(f"[bot] Using slow-path SELL (cache miss or stale, age={cache_age})")
        try:
            await sell_now(
                subtensor=subtensor,
                wallet=self.wallet,
                netuid=netuid,
                tip=self.tip,
                current_block=block_number,
            )
        except Exception as exc:
            print(f"[copy] Error during sell: {exc}")
            import traceback
            traceback.print_exc()

    async def _do_sell_batch(self, subtensor: AsyncSubtensor,
                              netuids: list[int], block_number: int):
        """Batch-sell ALL alpha across multiple netuids in a single tx.

        Auto-discovers the actual hotkey(s) where the coldkey has stake.
        """
        print(f"\n{'='*60}")
        print(f"  SELL BATCH  netuids={netuids}")
        print(f"  triggered at block #{block_number}  (auto-discover hotkeys, full tol)")
        print(f"{'='*60}")
        try:
            await sell_netuids_batch(
                subtensor=subtensor,
                wallet=self.wallet,
                netuids=netuids,
                tip=self.tip,
                current_block=block_number,
            )
        except Exception as exc:
            print(f"[copy] Error during sell batch: {exc}")
            import traceback
            traceback.print_exc()


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Buy Bittensor subnet alpha tokens immediately on NETUID change or copy-trade"
    )
    parser.add_argument("--netuid",          type=int,   default=None)
    parser.add_argument("--amount",          type=float, default=None)
    parser.add_argument("--network",         type=str,   default=DEFAULT_NETWORK)
    parser.add_argument("--wallet-name",     type=str,   default=DEFAULT_WALLET_NAME)
    parser.add_argument("--wallet-hotkey",   type=str,   default=DEFAULT_WALLET_HOTKEY)
    parser.add_argument("--wallet-path",     type=str,   default=DEFAULT_WALLET_PATH)
    parser.add_argument("--tip",             type=int,   default=DEFAULT_TIP_RAO)
    parser.add_argument(
        "--watch-addresses",
        type=str,
        default=None,
        help="Comma-separated ss58 addresses to mirror (overrides .env WATCH_ADDRESSES)",
    )
    parser.add_argument(
        "--copy-on-sell",
        action="store_true",
        default=False,
        help="Also trigger a buy when a watched address sells (overrides .env COPY_ON_SELL)",
    )
    return parser.parse_args()


async def main():
    args = parse_args()

    print("=" * 60)
    print("  Bittensor Alpha Token Buy Bot")
    print("=" * 60)
    print(f"  Network      : {args.network}")
    print(f"  Wallet       : {args.wallet_name} / {args.wallet_hotkey}")
    print(f"  Tip (RAO)    : {args.tip}")
    print(f"  Price tol.   : {PRICE_TOLERANCE_PCT}%")

    wallet = Wallet(
        name=args.wallet_name,
        hotkey=args.wallet_hotkey,
        path=args.wallet_path,
    )
    if WALLET_PASSWORD:
        wallet.coldkey_file.decrypt(WALLET_PASSWORD)
    else:
        wallet.unlock_coldkey()
    print(f"  Coldkey      : {wallet.coldkeypub.ss58_address}")
    print("=" * 60)

    # ── One-shot mode ──────────────────────────────────────────────────────────
    if args.netuid and args.amount:
        print(f"  Mode         : ONE-SHOT  netuid={args.netuid}  amount={args.amount}")
        print("=" * 60)
        async with AsyncSubtensor(network=args.network) as subtensor:
            success = await buy_now(
                subtensor=subtensor,
                wallet=wallet,
                netuid=args.netuid,
                tao_amount=args.amount,
                wallet_hotkey_name=args.wallet_hotkey,
                tip=args.tip,
                tolerance_pct=PRICE_TOLERANCE_PCT,
            )
        sys.exit(0 if success else 1)

    # ── Resolve watch addresses (CLI flag overrides .env) ──────────────────────
    env = reload_env()
    watch_addresses: list[str] = []
    if args.watch_addresses:
        watch_addresses = [a.strip() for a in args.watch_addresses.split(",") if a.strip()]
    else:
        watch_addresses = env["watch_addresses"]

    copy_on_sell: bool = env["copy_on_sell"] or args.copy_on_sell
    amount = args.amount if args.amount else env["amount"]
    sell_addresses: list[str] = env["sell_addresses"]
    mirror_buy_addresses: list[str] = env.get("mirror_buy_addresses", [])
    copy_sell_addresses: list[str] = env.get("copy_sell_addresses", [])
    home_netuids: list[int] = list(env.get("home_netuids") or [])

    # ── Copy-trade mode ────────────────────────────────────────────────────────
    if watch_addresses or sell_addresses or mirror_buy_addresses or copy_sell_addresses:
        print(f"  Mode         : COPY-TRADE")
        if watch_addresses:
            print(f"  BUY-trigger  : {len(watch_addresses)} address(es)")
            for a in watch_addresses:
                print(f"                 {a}")
        if sell_addresses:
            print(f"  SELL-trigger : {len(sell_addresses)} address(es)")
            for a in sell_addresses:
                print(f"                 {a}")
            if home_netuids:
                print(f"  HOME_NETUID  : {','.join(str(n) for n in home_netuids)}  "
                      f"(buy any subnet → force_batch sell home(s) + that)")
        if mirror_buy_addresses:
            print(f"  MIRROR-BUY   : {len(mirror_buy_addresses)} address(es) (their SELL → our BUY)")
            for a in mirror_buy_addresses:
                print(f"                 {a}")
        if copy_sell_addresses:
            print(f"  COPY-SELL    : {len(copy_sell_addresses)} address(es) (their SELL → our SELL)")
            for a in copy_sell_addresses:
                print(f"                 {a}")
        print(f"  Copy on sell : {copy_on_sell}")
        print(f"  Amount       : {amount} TAO")
        print("=" * 60)

        ct_watcher = CopyTradeWatcher(
            wallet=wallet,
            network=args.network,
            tip=args.tip,
            amount=amount,
            copy_on_sell=copy_on_sell,
            watch_addresses=watch_addresses,
            sell_addresses=sell_addresses,
            mirror_buy_addresses=mirror_buy_addresses,
            copy_sell_addresses=copy_sell_addresses,
            home_netuids=home_netuids,
        )
        await ct_watcher.run()
        return

    # ── NETUID watcher mode (pm2 / always-on) ─────────────────────────────────
    print(f"  Mode         : NETUID WATCHER  ({ENV_FILE.absolute()})")
    print("=" * 60)
    watcher = EnvWatcher(wallet=wallet, network=args.network, tip=args.tip)
    await watcher.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[bot] Interrupted.")
        sys.exit(0)
    except Exception as exc:
        print(f"\n[bot] Fatal error: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
