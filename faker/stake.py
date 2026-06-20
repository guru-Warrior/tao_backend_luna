"""Submit decoy ``Proxy.proxy`` staking extrinsics with deposit-like latency.

Design mirrors :class:`trader.Trader` exactly so Faker submits have the same
speed characteristics as ``/api/trade``:

* One pre-warmed ``bittensor.Subtensor`` connection for the process lifetime.
* Coldkey decrypted eagerly using ``WALLET_PASSWORD`` from the backend env.
* Chain metadata pre-warmed at construction (a dummy compose_call).
* Nonce cached optimistically and incremented per submit; reset on RPC error.
* Direct ``substrate.create_signed_extrinsic`` + ``substrate.submit_extrinsic``
  (skip ``sign_and_send_extrinsic`` which costs an extra RPC per call).

The per-submit path contains **no** block subscription, **no** intentional
delay, and **no** repeat loop — those belonged to the original
``/tao/faker/fake_stake.py`` CLI and are not compatible with an interactive
UI. A single confirm click therefore completes in the same ~500 ms – 1.5 s
window as a stake/unstake deposit.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class FakerResult:
    success: bool
    message: str
    extrinsic_hash: Optional[str] = None
    elapsed_ms: Optional[float] = None


VALID_ACTIONS = {"add", "remove", "swap"}
_SS58_RE = re.compile(r"^5[1-9A-HJ-NP-Za-km-z]{40,50}$")


class Faker:
    """Compose + sign + submit a self-proxy staking extrinsic synchronously.

    The submit path is the minimal cost equivalent of :meth:`trader.Trader`'s
    ``_submit_fast``, wrapped around a ``Proxy.proxy(real=self)`` call so the
    extrinsic is accepted into the mempool but cannot settle on a block.
    """

    def __init__(
        self,
        wallet_name: str,
        signing_hotkey_ss58: str,
        network: str = "finney",
        password: Optional[str] = None,
    ) -> None:
        self.wallet_name = wallet_name
        self.network = network
        self._signing_hotkey_ss58 = signing_hotkey_ss58

        # Serialise all substrate calls on this instance (shared WS is not
        # thread-safe) + nonce protection, same pattern as Trader.
        self._lock = threading.Lock()
        self._nonce_lock = threading.Lock()
        self._next_nonce: Optional[int] = None

        import bittensor as bt
        from bittensor_wallet import Wallet

        t0 = time.time()
        self._subtensor = bt.Subtensor(network=network)
        self._wallet = Wallet(name=wallet_name)

        # Eager coldkey unlock. ``wallet.unlock_coldkey()`` is interactive and
        # would block a server process, so we use ``get_coldkey(password=...)``
        # which decrypts the keyfile directly when a password is available.
        try:
            if password:
                self._coldkey = self._wallet.get_coldkey(password=password)
            else:
                # No password env var — last-resort fallback. Will raise if the
                # keyfile is encrypted.
                self._coldkey = self._wallet.coldkey
        except Exception as ex:
            raise RuntimeError(
                f"Faker coldkey unlock failed for wallet {wallet_name!r} "
                f"(wrong WALLET_PASSWORD?): {ex}"
            ) from ex

        # Pre-warm chain metadata so the first real submit doesn't pay the
        # metadata-download cost. Compose a dummy inner call + Proxy wrapper;
        # both pallets' metadata get cached on the substrate connection.
        try:
            inner = self._subtensor.substrate.compose_call(
                call_module="SubtensorModule",
                call_function="add_stake",
                call_params={
                    "hotkey": signing_hotkey_ss58,
                    "netuid": 1,
                    "amount_staked": 1,
                },
            )
            _ = self._subtensor.substrate.compose_call(
                call_module="Proxy",
                call_function="proxy",
                call_params={
                    "real": self._coldkey.ss58_address,
                    "force_proxy_type": None,
                    "call": inner,
                },
            )
        except Exception as ex:
            logger.debug("Faker metadata pre-warm skipped: %s", ex)

        elapsed = (time.time() - t0) * 1000
        logger.info(
            "Faker ready (%.0fms) — wallet=%s signing_hotkey=%s network=%s",
            elapsed, wallet_name, signing_hotkey_ss58, network,
        )

    # ---- nonce cache (same pattern as trader.Trader) --------------------

    def _reserve_nonce(self) -> int:
        with self._nonce_lock:
            if self._next_nonce is None:
                self._next_nonce = int(
                    self._subtensor.substrate.get_account_next_index(
                        self._coldkey.ss58_address
                    )
                )
            n = self._next_nonce
            self._next_nonce = n + 1
            return n

    def _reset_nonce(self) -> None:
        with self._nonce_lock:
            self._next_nonce = None

    # ---- public: one-shot submit ----------------------------------------

    def submit(
        self,
        *,
        action: str,
        netuid: int,
        amount_tao: float,
        dest_netuid: Optional[int] = None,
        alpha: Optional[float] = None,
        real: Optional[str] = None,
    ) -> FakerResult:
        t0 = time.time()

        act = (action or "").strip().lower()
        if act not in VALID_ACTIONS:
            return FakerResult(False, f"invalid action: {action!r}")
        if act == "swap" and dest_netuid is None:
            return FakerResult(False, "dest_netuid is required for swap")
        real_clean = (real or "").strip() or None
        if real_clean and not _SS58_RE.match(real_clean):
            return FakerResult(False, "real address is not a valid ss58")

        real_addr = real_clean or self._coldkey.ss58_address
        amount_rao = int(max(0.0, float(amount_tao or 0.0)) * 1_000_000_000)

        try:
            with self._lock:
                sub = self._subtensor.substrate
                if act == "add":
                    inner = sub.compose_call(
                        call_module="SubtensorModule",
                        call_function="add_stake",
                        call_params={
                            "hotkey": self._signing_hotkey_ss58,
                            "netuid": int(netuid),
                            "amount_staked": amount_rao,
                        },
                    )
                elif act == "swap":
                    swap_alpha = int(float(alpha if alpha else amount_tao) * 1e9)
                    inner = sub.compose_call(
                        call_module="SubtensorModule",
                        call_function="swap_stake_limit",
                        call_params={
                            "hotkey": self._signing_hotkey_ss58,
                            "origin_netuid": int(netuid),
                            "destination_netuid": int(
                                dest_netuid if dest_netuid is not None else netuid
                            ),
                            "alpha_amount": swap_alpha,
                            "limit_price": 0,
                            "allow_partial": True,
                        },
                    )
                else:  # remove
                    inner = sub.compose_call(
                        call_module="SubtensorModule",
                        call_function="remove_stake",
                        call_params={
                            "hotkey": self._signing_hotkey_ss58,
                            "netuid": int(netuid),
                            "amount_unstaked": amount_rao,
                        },
                    )
                call = sub.compose_call(
                    call_module="Proxy",
                    call_function="proxy",
                    call_params={
                        "real": real_addr,
                        "force_proxy_type": None,
                        "call": inner,
                    },
                )

                nonce = self._reserve_nonce()
                try:
                    ext = sub.create_signed_extrinsic(
                        call=call,
                        keypair=self._coldkey,
                        nonce=nonce,
                        era={"period": 64},
                    )
                    sub.submit_extrinsic(
                        extrinsic=ext,
                        wait_for_inclusion=False,
                        wait_for_finalization=False,
                    )
                except Exception:
                    # Nonce state is uncertain after any RPC failure.
                    self._reset_nonce()
                    raise

                raw_hash = getattr(ext, "extrinsic_hash", None)
                if isinstance(raw_hash, bytes):
                    ext_hash: Optional[str] = "0x" + raw_hash.hex()
                else:
                    ext_hash = str(raw_hash) if raw_hash else None
        except Exception as exc:
            elapsed = (time.time() - t0) * 1000
            logger.exception("Faker submit failed (%.0fms)", elapsed)
            return FakerResult(False, str(exc), elapsed_ms=elapsed)

        elapsed = (time.time() - t0) * 1000
        # Log the exact values we embed so we can trace mempool "from" column
        # back to the Proxy.proxy(real=...) we actually signed. Useful when the
        # UI shows a coldkey instead of the provided `real` address.
        logger.info(
            "Faker submitted (%.0fms) action=%s netuid=%d amount=%.4f "
            "inner_hotkey=%s proxy_real=%s signer=%s hash=%s",
            elapsed, act, netuid, float(amount_tao or 0.0),
            self._signing_hotkey_ss58, real_addr, self._coldkey.ss58_address,
            ext_hash,
        )
        return FakerResult(True, "submitted", extrinsic_hash=ext_hash, elapsed_ms=elapsed)


# ---- singleton (mirror of trader.get_trader) ----------------------------

_faker: Optional[Faker] = None
_faker_init_error: Optional[str] = None
_faker_singleton_lock = threading.Lock()


def faker_last_init_error() -> Optional[str]:
    return _faker_init_error


def get_faker() -> Optional[Faker]:
    """Lazy-init singleton using env vars. Returns None if construction fails."""
    global _faker, _faker_init_error
    if _faker is not None:
        return _faker
    if _faker_init_error is not None:
        return None

    with _faker_singleton_lock:
        if _faker is not None:
            return _faker
        if _faker_init_error is not None:
            return None

        wallet_name = os.getenv("FAKE_WALLET", "").strip()
        hotkey_ss58 = os.getenv("FAKE_HOTKEY", "").strip()
        network = os.getenv("NETWORK", "finney").strip() or "finney"
        password = os.getenv("WALLET_PASSWORD", "").strip() or None

        if not wallet_name:
            _faker_init_error = "FAKE_WALLET not set in backend .env"
            return None
        if not hotkey_ss58:
            _faker_init_error = "FAKE_HOTKEY not set in backend .env"
            return None

        try:
            _faker = Faker(wallet_name, hotkey_ss58, network, password=password)
            _faker_init_error = None
            return _faker
        except Exception as e:
            _faker_init_error = str(e)
            logger.error(
                "Faker init failed (wallet=%s): %s", wallet_name, e, exc_info=True
            )
            return None


def read_env_defaults() -> Dict[str, Any]:
    """Static Faker config for the UI (``GET /api/faker/defaults``)."""
    return {
        "wallet_name": os.getenv("FAKE_WALLET", "").strip(),
        "hotkey_ss58": os.getenv("FAKE_HOTKEY", "").strip(),
        "network": os.getenv("NETWORK", "finney").strip() or "finney",
        "password_set": bool(os.getenv("WALLET_PASSWORD", "").strip()),
    }
