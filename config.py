"""
Shared paths and defaults for the backend (single source of truth).
Override Finney RPC with env: SUBSTRATE_WS_URL=wss://...
"""
from __future__ import annotations

import os
from pathlib import Path

BACKEND_DIR: Path = Path(__file__).resolve().parent

# Default Bittensor Finney public endpoint
FINNEY_WS: str = os.environ.get(
    "SUBSTRATE_WS_URL",
    "wss://entrypoint-finney.opentensor.ai:443",
)

# Vite production build: monorepo ../frontend/dist or legacy backend/frontend/dist
FRONTEND_DIST: Path | None = None
for _cand in (BACKEND_DIR.parent / "frontend" / "dist", BACKEND_DIR / "frontend" / "dist"):
    if _cand.is_dir():
        FRONTEND_DIST = _cand
        break

# Mempool server: subnet list for pool liquidity (one GET returns all subnets; override for tests/mirrors)
BACKPROP_SUBNETS_URL: str = os.environ.get(
    "BACKPROP_SUBNETS_URL",
    "https://backprop.finance/api/subnets",
)
# How often the pool thread refetches BACKPROP_SUBNETS_URL
POOL_POLL_INTERVAL_SEC: float = 0.5

# Buy-bot integration — the worker (buy_bot/buy_bot_main.py) is a subprocess
# configured through a private env file regenerated on every UI mutation. Rules
# + global settings are persisted to ``BOT_STATE_PATH`` so the UI survives
# backend restarts.
BOT_DATA_DIR: Path = Path(os.environ.get("BOT_DATA_DIR", str(BACKEND_DIR / "data")))
BOT_ENV_PATH: Path = Path(
    os.environ.get("BOT_ENV_PATH", str(BOT_DATA_DIR / "buy_bot.env"))
)
BOT_STATE_PATH: Path = Path(
    os.environ.get("BOT_STATE_PATH", str(BOT_DATA_DIR / "buy_bot_state.json"))
)
BOT_WORKER_SCRIPT: Path = Path(
    os.environ.get(
        "BOT_WORKER_SCRIPT", str(BACKEND_DIR / "buy_bot" / "buy_bot_main.py")
    )
)
# Auto-start the subprocess when the FastAPI lifespan boots. Set to "false" (or
# 0) to leave the worker in stopped state until the UI calls /api/bot/start.
BOT_AUTOSTART: bool = os.environ.get("BOT_AUTOSTART", "false").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Mempool monitor loop sleep (defaults match pre–adaptive behavior; use env to tune).
MEMPOOL_POLL_BUSY_SEC: float = float(os.environ.get("MEMPOOL_POLL_BUSY_SEC", "0.02"))
MEMPOOL_POLL_IDLE_SEC: float = float(os.environ.get("MEMPOOL_POLL_IDLE_SEC", "0.02"))
# Poll best head on this interval so we still advance if block subscription connects late or drops.
MEMPOOL_HEAD_POLL_SEC: float = float(os.environ.get("MEMPOOL_HEAD_POLL_SEC", "0.4"))
# Max blocks processed in one catch-up burst (avoids long stalls if RPC returns a bogus gap).
MEMPOOL_MAX_CATCHUP_BLOCKS: int = int(os.environ.get("MEMPOOL_MAX_CATCHUP_BLOCKS", "512"))
# How long a pending extrinsic may remain in our mempool view before it is
# forcibly hidden. The chain node's local txpool can hold stuck / dead txs
# across many blocks (especially after brief WS or network interruptions on
# long-running backends), which makes ``author_pendingExtrinsics`` keep
# returning the same extrinsic every tick. Without this cap those rows would
# "never leave" the mempool panel. 60 s is ~5 blocks on Finney — more than
# enough for a genuinely live tx to land, while still hiding truly stuck ones.
# Set to 0 to disable the cap (legacy behavior).
MEMPOOL_MAX_AGE_S: float = float(os.environ.get("MEMPOOL_MAX_AGE_S", "60"))


def cors_allow_origins_and_credentials() -> tuple[list[str], bool]:
    """Starlette forbids allow_origins=['*'] with allow_credentials=True (browser CORS spec).

    Set CORS_ORIGINS to a comma-separated list of frontend origins (e.g. https://app.vercel.app).
    Use CORS_ORIGINS=* only for open public APIs without cookie credentials (not typical for this app).
    """
    raw = os.environ.get("CORS_ORIGINS", "").strip()
    if raw == "*":
        return ["*"], False
    if raw:
        return [x.strip() for x in raw.split(",") if x.strip()], True
    return [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ], True
