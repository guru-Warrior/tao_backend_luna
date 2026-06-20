# Stake monitoring: events, extrinsics, netuid, methods

This document describes how the block/mempool monitors interpret Bittensor stake operations. It replaces the older split notes (`EVENT_VS_EXTRINSIC`, `NETUID_EXPLANATION`, `STAKING_METHODS`, `WRAPPED_CALLS`, `TRANSFER_STAKE`, `MOVE_SWAP_NETUID`).

Reference implementation: `mempool/monitor.py` (`MempoolMonitor`) + `stake_tracker.py` (used by the FastAPI server).

## 1. Events vs extrinsics

`StakeAdded` / `StakeRemoved` events carry **hotkey**, **coldkey**, and **amount** (RAO). They do **not** carry **netuid**.

Subnet context comes from the **extrinsic** that caused the event: call function name and call arguments (including `netuid` when present).

Typical flow:

1. Detect stake-related **events** to know that something happened and to read amount and accounts.
2. Use the event’s `extrinsic_idx` (when present) to load the matching **extrinsic** and parse `netuid`, wrappers, and movement ops.

If the extrinsic has no `netuid` argument (legacy root-style calls), parsers may infer root (`0`), query registrations, or show ambiguous / unknown — see below.

## 2. Netuid resolution and “Unknown”

Rough priority:

1. **Explicit** `netuid` / `net_uid` in extrinsic call args.
2. **Legacy root** patterns: some `add_stake` / `remove_stake` shapes without netuid map to root network (`0`).
3. **Hotkey registration query**: which subnets the hotkey is registered on — if exactly one, use it; if several, display a list; if none, **Unknown** is possible.

**Unknown** can appear for new hotkeys, parsing edge cases, or internal/system events without a useful extrinsic link.

**Ambiguous** multi-netuid display (e.g. `[1, 3, 21]`) means the hotkey is registered on multiple subnets and the extrinsic alone does not pin a single subnet.

## 3. Common staking methods (direct)

| Method | Role |
|--------|------|
| `add_stake` | Add stake (with or without netuid in args depending on era) |
| `add_stake_limit` | Stake with price / limit protection on dynamic subnets |
| `remove_stake` | Remove stake |
| `remove_stake_full_limit` | Unstake with limit protection |
| `move_stake` / `move_stake_limit` | Move stake between hotkeys and/or subnets |
| `swap_stake` / `swap_stake_limit` | Swap stake between positions |
| `transfer_stake` / `transfer_stake_limit` | Transfer stake to another coldkey |

Exact parameter names follow the on-chain metadata; monitors unwrap nested calls to show the inner `SubtensorModule` dispatch.

## 4. Wrapped calls

Batch, proxy, utility, and derivative wrappers can nest the real call. The UI / logs may show **wrapper > inner**, for example:

- `batch > add_stake`
- `batch_all > remove_stake`
- `proxy > add_stake_limit`
- `force_batch > move_stake`

Netuid and amounts are taken from the **inner** call when parsing succeeds. If the wrapper hides the inner call or decoding fails, method may show as `unknown`.

## 5. Move, swap, transfer — origin and destination

For operations that touch two subnets, monitors extract **origin_netuid** and **destination_netuid** and may render them as **`origin→dest`** (e.g. `67→73`, or `35→35` when only hotkeys change).

Supported families include `move_stake`, `swap_stake`, `transfer_stake` (and `*_limit` variants). Two logical rows (unstake / stake legs) may appear for the same extrinsic depending on how events are expanded.

## 6. Why a method is still “unknown”

1. `extrinsic_idx` is missing on the event.
2. Wrapper chain without a decoded `SubtensorModule` inner call.
3. Rare decoding or metadata mismatches.

---

For operational setup (server, PM2, balance checker), see [../README.md](../README.md).
