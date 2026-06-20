# Backend — TAO trading / mempool UI

FastAPI serves the React build (if present) and a WebSocket feed of mempool and last-block data. Optional trading uses the Bittensor SDK (`trader.py`). Run with uvicorn or PM2.

**Working directory:** run Python commands from `backend/` (so `mempool_server`, `check_balance`, etc. resolve correctly).

## Prerequisites

- Python 3.11+
- Node.js + npm (only if you build the frontend in `../frontend/`)

## Install

```bash
cd backend
python3 -m venv venv
source venv/bin/activate
# Windows: venv\Scripts\activate
# Linux/macOS: source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then edit with your keys
```

Build the frontend (static files served from `../frontend/dist/` when the server runs):

```bash
cd ../frontend && npm ci && npm run build
```

## Run the API + Web UI

From `backend/`:

```bash
source venv/bin/activate
python3 -m uvicorn mempool_server:app --host 0.0.0.0 --port 8001
```

Open `http://<host>:8001/`. With `npm run dev` in `frontend/`, Vite proxies `/ws` and `/api` to port 8001 — start uvicorn first.

### Endpoints

- `GET /api/health` — liveness
- `GET /api/pools` — per–netuid pool liquidity (`taoInPool`, `name`); server refreshes from `https://backprop.finance/api/subnets` (full list in one request). Optional env: `BACKPROP_SUBNETS_URL`
- `WebSocket /ws` — mempool snapshots and, on each new block, wallet balance when a coldkey is configured
- `POST /api/trade` — stake / unstake via SDK + MEV Shield (`mev_protection=True`); requires `SEED_PHRASE_1`, `TRADE_HOTKEY` in `.env`. Optional: `BT_MEV_PROTECTION=1` for SDK-wide defaults ([MEV Shield](https://docs.learnbittensor.org/sdk/mev-protection)).
- `POST /api/wallet-balance` — read-only balance for an arbitrary address (used by the UI Watch list)

Implementation details: `mempool_server.py`, `trader.py`.

### Wallet / balance panel

The server resolves **one** coldkey from `.env` (then restart to pick up changes):

- `WALLET_ADDRESS` or `COLDKEY_ADDRESS` — ss58 coldkey
- `SEED_PHRASE_1` — mnemonic; address derived for balance only
- `WALLET_NAME` — wallet under `~/.bittensor/wallets/`; use `WALLET_PASSWORD` if encrypted

The React **Watch** list and address labels use **browser localStorage only**; the backend does not ship nickname maps.

## Wallet balance (`check_balance.py`)

`WalletChecker` is used by `mempool_server` for `/api/wallet-balance` and WebSocket wallet pushes. There is no standalone CLI; use the API or import `WalletChecker` in your own script.

## PM2 (optional)

`ecosystem.config.js` sets `cwd` to this `backend/` folder. From the **repository root**:

```bash
pm2 start backend/ecosystem.config.js
```

Logs default to `backend/logs/`. For reboot persistence: `pm2 save` and `pm2 startup` (follow PM2’s printed instructions).

## systemd (production)

Template: [`deploy/systemd/mempool-server.service.example`](deploy/systemd/mempool-server.service.example). Replace `INSTALL_ROOT` with the absolute path to your clone, copy into `/etc/systemd/system/`, then `daemon-reload`, `enable`, `start`. Use `journalctl -u mempool-server.service -f` for logs.

`EnvironmentFile=` expects `KEY=value` (no `export`). Fix any lines systemd rejects or inject secrets another way.

## Stack modules (imported by the server)

| Module | Role |
|--------|------|
| `mempool/monitor.py` | `MempoolMonitor` (mempool + last block) |
| `stake_tracker.py` | Stake op parsing (`extract_stake_ops`, `aggregate_ops`, block events); `stake_tracker_bridge.py` adapts Substrate events |

## Docs

- [docs/stake-parsing.md](docs/stake-parsing.md) — how netuid, methods, and wrapped calls appear in monitors

## Restart after `.env` changes

Stop uvicorn (Ctrl+C) or `systemctl restart` / `pm2 restart`, then start again unless your process manager reloads env automatically.
