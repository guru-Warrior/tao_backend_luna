"""Subprocess manager for the standalone buy_bot worker.

Design
------
* `buy_bot_main.py` is copied verbatim from `tao/buy_bot/buy_bot.py`.
* The manager owns a *private* env file (``bot.env``) that the worker consumes.
  Wallet/network settings come from the backend's process env (backend ``.env``
  is already loaded by ``mempool_server.py`` on import). Dynamic settings (rules,
  amounts, tolerances, instant-buy/sell triggers) are supplied through the UI
  and serialized into this env file on every change.
* The worker polls its env file every 50 ms (``ENV_POLL_INTERVAL`` in
  ``buy_bot_main.py``) so mutations propagate with zero IPC cost.
* Rules + global state are persisted to a separate JSON blob so the UI can
  restore them across restarts.

Rule model
----------
Rules are intentionally thin to mirror buy_bot's native capabilities:

* ``copy_buy``    — address goes into ``WATCH_ADDRESSES`` (we buy when they buy).
* ``copy_sell``   — address goes into ``COPY_SELL_ADDRESSES`` (we sell when they sell).
* ``mirror_sell`` — address goes into ``SELL_ADDRESSES`` (we sell when they buy).
* ``mirror_buy``  — address goes into ``MIRROR_BUY_ADDRESSES`` (we buy when they
  sell). Independent of the global ``COPY_ON_SELL`` flag so that ``copy_buy``
  rules are *not* forced to also fire on the target's sells.
* ``HOME_NETUID`` (global) — comma-separated netuids. With mirror sell, when the
  target buys subnet N we ``force_batch`` sell alpha on every listed home netuid
  and on N (where we hold stake); with HOME unset we only sell N.

``COPY_ON_SELL`` stays a single global toggle because buy_bot treats it that
way for ``WATCH_ADDRESSES``; ``mirror_buy`` is the per-address opt-in for the
"target sells → we buy" semantic.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

VALID_MODES = {"copy_buy", "copy_sell", "mirror_sell", "mirror_buy"}

# Hardcoded constants — mirrored verbatim from tao/buy_bot/.env. These are
# intentionally NOT exposed through the UI: the user asked for only
# copy_trade_amount to be editable.
CONST_TIP_RAO = 5_100_000
CONST_PRICE_TOLERANCE_PCT = 0.5
CONST_COPY_TRADE_TOLERANCE_PCT = 1.6
# Instant-buy is disabled in the current UI; AMOUNT is only consumed when
# NETUID is non-empty, which we never set from the backend.
CONST_AMOUNT = 0.0
CONST_COPY_ON_SELL = False


@dataclass
class BotRule:
    """One rule supplied through the UI."""

    id: str
    mode: str
    address: str
    enabled: bool = True
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["BotRule"]:
        rid = str(data.get("id") or "").strip()
        mode = str(data.get("mode") or "").strip()
        addr = str(data.get("address") or "").strip()
        if mode == "cross_subnet_sell":
            mode = "mirror_sell"
        if not rid or mode not in VALID_MODES or not _looks_like_ss58(addr):
            return None
        return cls(
            id=rid,
            mode=mode,
            address=addr,
            enabled=bool(data.get("enabled", True)),
            created_at=float(data.get("createdAt") or data.get("created_at") or time.time()),
        )


@dataclass
class BotSettings:
    """Global, UI-tunable knobs written into the bot env file.

    All tuning knobs except ``copy_trade_amount`` were moved into hardcoded
    constants above (see module docstring). ``sell_netuids`` is kept because the
    per-position "Sell" button writes it into the env file as a one-shot
    trigger; the worker clears it after firing.
    """

    copy_trade_amount: float = 0.0
    home_netuids: str = ""
    sell_netuids: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _normalize_home_netuids(raw: Any) -> str:
    """Comma-separated unique netuids for ``HOME_NETUID`` env (e.g. ``28,118``)."""
    if raw is None:
        return ""
    if isinstance(raw, int):
        return str(raw) if raw > 0 else ""
    out: list[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if part.isdigit():
            n = int(part)
            if 0 < n <= 65535 and n not in out:
                out.append(n)
    return ",".join(str(n) for n in sorted(out))


def _looks_like_ss58(addr: str) -> bool:
    if not addr or len(addr) < 40 or len(addr) > 52 or not addr.startswith("5"):
        return False
    allowed = set(
        "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    )
    return all(ch in allowed for ch in addr)


def compile_rules_to_env(
    rules: List[BotRule], settings: BotSettings, base_env: Dict[str, str]
) -> Dict[str, str]:
    """Flatten rules + settings into the env dict the worker consumes."""
    watch: List[str] = []
    copy_sell: List[str] = []
    sell: List[str] = []
    mirror_buy: List[str] = []
    for r in rules:
        if not r.enabled:
            continue
        if r.mode == "copy_buy":
            watch.append(r.address)
        elif r.mode == "copy_sell":
            copy_sell.append(r.address)
        elif r.mode == "mirror_sell":
            sell.append(r.address)
        elif r.mode == "mirror_buy":
            mirror_buy.append(r.address)

    env: Dict[str, str] = {}
    env.update(base_env)  # wallet/network/password etc.
    env["WATCH_ADDRESSES"] = ",".join(watch)
    env["COPY_SELL_ADDRESSES"] = ",".join(copy_sell)
    env["SELL_ADDRESSES"] = ",".join(sell)
    env["MIRROR_BUY_ADDRESSES"] = ",".join(mirror_buy)
    env["CROSS_SUBNET_SELL_ADDRESSES"] = ""
    env["HOME_NETUID"] = _normalize_home_netuids(settings.home_netuids)
    env["COPY_ON_SELL"] = "true" if CONST_COPY_ON_SELL else "false"
    env["AMOUNT"] = _fmt_num(CONST_AMOUNT)
    env["COPY_TRADE_AMOUNT"] = _fmt_num(settings.copy_trade_amount)
    env["PRICE_TOLERANCE_PCT"] = _fmt_num(CONST_PRICE_TOLERANCE_PCT)
    env["COPY_TRADE_TOLERANCE_PCT"] = _fmt_num(CONST_COPY_TRADE_TOLERANCE_PCT)
    env["TIP_RAO"] = str(int(CONST_TIP_RAO))
    env["NETUID"] = ""
    env["SELL_NETUIDS"] = settings.sell_netuids or ""
    return env


def _fmt_num(v: float) -> str:
    if v is None:
        return ""
    if float(v).is_integer():
        return str(int(v))
    return f"{float(v):g}"


def _write_env_file(path: Path, env: Dict[str, str]) -> None:
    """Write ``env`` atomically so the worker never reads a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        fh.write("# AUTO-GENERATED by backend.buy_bot.manager — do NOT edit.\n")
        for k, v in env.items():
            # Escape newlines only — single-line values, no quoting needed
            # because python-dotenv handles bare values fine.
            safe = (v or "").replace("\n", " ")
            fh.write(f"{k}={safe}\n")
    os.replace(tmp, path)


class BotManager:
    """Owns the worker subprocess + persisted rules/settings."""

    def __init__(
        self,
        *,
        worker_script: Path,
        env_file: Path,
        state_file: Path,
        base_env_keys: Optional[List[str]] = None,
        python_executable: Optional[str] = None,
    ) -> None:
        self._worker_script = Path(worker_script).resolve()
        self._env_file = Path(env_file).resolve()
        self._state_file = Path(state_file).resolve()
        self._python = python_executable or sys.executable
        self._base_env_keys = base_env_keys or [
            "NETWORK",
            "WALLET_NAME",
            "WALLET_HOTKEY",
            "WALLET_PATH",
            "WALLET_PASSWORD",
        ]

        self.rules: List[BotRule] = []
        self.settings: BotSettings = BotSettings()
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._last_error: Optional[str] = None
        self._started_at: Optional[float] = None
        self._lock = asyncio.Lock()

        self._load_state()
        # Persist a fresh env file so the worker can start cleanly even if the
        # backend is restarted with no rules update.
        with contextlib.suppress(Exception):
            _write_env_file(self._env_file, self._current_env())

    # ── Persistence ─────────────────────────────────────────────────────────

    def _load_state(self) -> None:
        if not self._state_file.exists():
            return
        try:
            data = json.loads(self._state_file.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("[bot] failed to read %s: %s", self._state_file, exc)
            return
        rules_raw = data.get("rules") or []
        rules: List[BotRule] = []
        for entry in rules_raw:
            if not isinstance(entry, dict):
                continue
            r = BotRule.from_dict(entry)
            if r is not None:
                rules.append(r)
        self.rules = rules

        settings_raw = data.get("settings") or {}
        if isinstance(settings_raw, dict):
            # Older state blobs may still have extra fields (amount_tao,
            # price_tolerance_pct, …) — we silently drop them.
            home_raw = settings_raw.get("home_netuids")
            if home_raw is None and "home_netuid" in settings_raw:
                home_raw = settings_raw.get("home_netuid")
            self.settings = BotSettings(
                copy_trade_amount=float(
                    settings_raw.get("copy_trade_amount") or 0.0
                ),
                home_netuids=_normalize_home_netuids(home_raw),
                sell_netuids=str(settings_raw.get("sell_netuids") or ""),
            )

    def _save_state(self) -> None:
        payload = {
            "rules": [r.to_dict() for r in self.rules],
            "settings": self.settings.to_dict(),
        }
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._state_file.with_suffix(self._state_file.suffix + ".tmp")
        tmp.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        os.replace(tmp, self._state_file)

    def _base_env(self) -> Dict[str, str]:
        """Wallet/network/password from the backend process env (backend .env)."""
        return {k: os.environ.get(k, "") for k in self._base_env_keys}

    def _current_env(self) -> Dict[str, str]:
        return compile_rules_to_env(self.rules, self.settings, self._base_env())

    # ── Public state API ────────────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        base = self._base_env()
        return {
            "rules": [r.to_dict() for r in self.rules],
            "settings": self.settings.to_dict(),
            "wallet": {
                "network": base.get("NETWORK", ""),
                "wallet_name": base.get("WALLET_NAME", ""),
                "wallet_hotkey": base.get("WALLET_HOTKEY", ""),
                "wallet_path": base.get("WALLET_PATH", ""),
                "password_set": bool(base.get("WALLET_PASSWORD", "")),
            },
            "process": self.process_status(),
            "env_file": str(self._env_file),
            "state_file": str(self._state_file),
        }

    def process_status(self) -> Dict[str, Any]:
        running = self._proc is not None and self._proc.returncode is None
        return {
            "running": running,
            "pid": self._proc.pid if self._proc and running else None,
            "started_at": self._started_at if running else None,
            "last_error": self._last_error,
        }

    # ── Mutations ───────────────────────────────────────────────────────────

    async def replace_rules(self, rules: List[BotRule]) -> List[BotRule]:
        """Replace the rule set. Deduplicates (mode, address)."""
        async with self._lock:
            seen: set[tuple[str, str]] = set()
            accepted: List[BotRule] = []
            for r in rules:
                key = (r.mode, r.address)
                if key in seen:
                    continue
                seen.add(key)
                accepted.append(r)
            self.rules = accepted
            self._save_state()
            _write_env_file(self._env_file, self._current_env())
            return accepted

    async def update_settings(self, **fields: Any) -> BotSettings:
        """Patch the mutable global settings + rewrite env file."""
        async with self._lock:
            if "home_netuid" in fields and "home_netuids" not in fields:
                fields["home_netuids"] = fields.pop("home_netuid")
            for k, v in fields.items():
                if not hasattr(self.settings, k):
                    continue
                current = getattr(self.settings, k)
                if isinstance(current, bool):
                    setattr(self.settings, k, bool(v))
                elif isinstance(current, int):
                    try:
                        setattr(self.settings, k, int(v))
                    except (TypeError, ValueError):
                        pass
                elif isinstance(current, float):
                    try:
                        setattr(self.settings, k, float(v))
                    except (TypeError, ValueError):
                        pass
                elif k == "home_netuids":
                    setattr(self.settings, k, _normalize_home_netuids(v))
                else:
                    setattr(self.settings, k, "" if v is None else str(v))
            self._save_state()
            _write_env_file(self._env_file, self._current_env())
            return self.settings

    async def trigger_sell(self, netuids: List[int | str]) -> None:
        joined = ",".join(str(n) for n in netuids if str(n).strip())
        await self.update_settings(sell_netuids=joined)

    # ── Subprocess lifecycle ────────────────────────────────────────────────

    async def start(self) -> Dict[str, Any]:
        """Launch the worker subprocess. No-op if already running."""
        async with self._lock:
            if self._proc is not None and self._proc.returncode is None:
                return self.process_status()

            if not self._worker_script.exists():
                self._last_error = f"worker script missing: {self._worker_script}"
                return self.process_status()

            # Make sure the env file reflects the latest settings before launch.
            _write_env_file(self._env_file, self._current_env())

            env = os.environ.copy()
            env["ENV_FILE"] = str(self._env_file)
            # The worker prints to stdout/stderr; we keep those inherited so the
            # pm2/systemd logs capture them next to the backend's own logs.
            try:
                self._proc = await asyncio.create_subprocess_exec(
                    self._python,
                    "-u",
                    str(self._worker_script),
                    cwd=str(self._worker_script.parent),
                    env=env,
                    stdout=None,
                    stderr=None,
                )
            except Exception as exc:
                self._last_error = f"spawn failed: {exc}"
                logger.exception("[bot] failed to spawn worker")
                return self.process_status()

            self._started_at = time.time()
            self._last_error = None
            logger.info(
                "[bot] worker started pid=%s env=%s",
                self._proc.pid,
                self._env_file,
            )
            # Detach a reaper so zombies/non-zero exits are logged.
            asyncio.create_task(self._reap())
            return self.process_status()

    async def stop(self, *, timeout: float = 5.0) -> Dict[str, Any]:
        """Terminate the subprocess (SIGTERM → SIGKILL on timeout)."""
        async with self._lock:
            proc = self._proc
            if proc is None or proc.returncode is not None:
                self._proc = None
                self._started_at = None
                return self.process_status()

            try:
                proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass

            try:
                await asyncio.wait_for(proc.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("[bot] worker did not exit within %.1fs — killing", timeout)
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(proc.wait(), timeout=2.0)

            self._proc = None
            self._started_at = None
            return self.process_status()

    async def restart(self) -> Dict[str, Any]:
        await self.stop()
        return await self.start()

    async def _reap(self) -> None:
        proc = self._proc
        if proc is None:
            return
        rc = await proc.wait()
        # Avoid clobbering state if a newer proc has already been started.
        if self._proc is proc:
            self._proc = None
            self._started_at = None
            if rc != 0:
                self._last_error = f"worker exited with code {rc}"
                logger.warning("[bot] worker exited rc=%s", rc)
            else:
                logger.info("[bot] worker exited cleanly")
