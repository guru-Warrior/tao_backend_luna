"""
MEV Shield via ``submit_encrypted_extrinsic`` with explicit ``blocks_for_revealed_execution``.

High-level ``add_stake`` / ``unstake`` / ``unstake_all`` (bittensor 10.2.x) do not forward this
parameter; this module mirrors the SDK extrinsic builders and post-success handling so behavior
stays aligned with ``bittensor.core.extrinsics.staking`` / ``unstaking``.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Optional

from bittensor.core.extrinsics.mev_shield import submit_encrypted_extrinsic
from bittensor.core.extrinsics.pallets import SubtensorModule
from bittensor.core.types import ExtrinsicResponse
from bittensor.utils.balance import Balance
from bittensor.utils.btlogging import logging

if TYPE_CHECKING:
    from bittensor_wallet import Wallet
    from bittensor.core.subtensor import Subtensor


def mev_blocks_for_revealed_execution() -> int:
    """Blocks to poll for inner extrinsic after outer inclusion (SDK default in submit_encrypted_extrinsic is 3)."""
    raw = os.environ.get("MEV_SHIELD_BLOCKS_FOR_REVEALED", "10")
    try:
        return max(3, min(64, int(raw)))
    except ValueError:
        return 10


def add_stake_mev_shield(
    subtensor: "Subtensor",
    wallet: "Wallet",
    netuid: int,
    hotkey_ss58: str,
    amount: Balance,
    safe_staking: bool,
    allow_partial_stake: bool,
    rate_tolerance: float,
    *,
    period: Optional[int] = None,
    raise_error: bool = False,
    wait_for_inclusion: bool = True,
    wait_for_finalization: bool = False,
    wait_for_revealed_execution: bool = True,
    blocks_for_revealed_execution: Optional[int] = None,
) -> ExtrinsicResponse:
    """Aligned with ``add_stake_extrinsic`` MEV path + explicit reveal polling window."""
    try:
        if not (
            unlocked := ExtrinsicResponse.unlock_wallet(wallet, raise_error)
        ).success:
            return unlocked

        old_balance = subtensor.get_balance(wallet.coldkeypub.ss58_address)
        block = subtensor.get_current_block()

        old_stake = subtensor.get_stake(
            hotkey_ss58=hotkey_ss58,
            coldkey_ss58=wallet.coldkeypub.ss58_address,
            netuid=netuid,
            block=block,
        )
        existential_deposit = subtensor.get_existential_deposit(block=block)

        if old_balance <= existential_deposit:
            return ExtrinsicResponse(
                False,
                f"Balance ({old_balance}) is not enough to cover existential deposit `{existential_deposit}`.",
            ).with_log()

        if amount > old_balance - existential_deposit:
            amount = old_balance - existential_deposit

        if amount > old_balance:
            message = "Not enough stake"
            logging.debug(
                f":cross_mark: [red]{message}:[/red] balance:{old_balance} amount: {amount} wallet: {wallet.name}"
            )
            return ExtrinsicResponse(False, f"{message}.").with_log()

        if safe_staking:
            pool = subtensor.subnet(netuid=netuid)
            price_with_tolerance = (
                pool.price.tao
                if pool.netuid == 0
                else pool.price.tao * (1 + rate_tolerance)
            )
            limit_price = Balance.from_tao(price_with_tolerance).rao
            logging.debug(
                f"Safe Staking to: netuid: {netuid}, amount: {amount}, "
                f"tolerance: {rate_tolerance * 100}%, limit_price: {Balance.from_tao(limit_price)}, "
                f"original price: {pool.price}, partial: {allow_partial_stake} on {subtensor.network}."
            )
            call = SubtensorModule(subtensor).add_stake_limit(
                hotkey=hotkey_ss58,
                netuid=netuid,
                amount_staked=amount.rao,
                limit_price=limit_price,
                allow_partial=allow_partial_stake,
            )
        else:
            logging.debug(
                f"Staking to: netuid: {netuid}, amount: {amount} on {subtensor.network}."
            )
            call = SubtensorModule(subtensor).add_stake(
                netuid=netuid,
                hotkey=hotkey_ss58,
                amount_staked=amount.rao,
            )

        block_before = subtensor.block
        br = (
            blocks_for_revealed_execution
            if blocks_for_revealed_execution is not None
            else mev_blocks_for_revealed_execution()
        )
        response = submit_encrypted_extrinsic(
            subtensor=subtensor,
            wallet=wallet,
            call=call,
            period=period,
            raise_error=raise_error,
            wait_for_inclusion=wait_for_inclusion,
            wait_for_finalization=wait_for_finalization,
            wait_for_revealed_execution=wait_for_revealed_execution,
            blocks_for_revealed_execution=br,
        )

        if response.success:
            sim_swap = subtensor.sim_swap(
                origin_netuid=0,
                destination_netuid=netuid,
                amount=amount,
                block=block_before,
            )
            response.transaction_tao_fee = sim_swap.tao_fee
            response.transaction_alpha_fee = sim_swap.alpha_fee.set_unit(netuid)

            if not wait_for_finalization and not wait_for_inclusion:
                return response
            logging.debug("[green]Finalized.[/green]")

            new_block = subtensor.get_current_block()
            new_balance = subtensor.get_balance(
                wallet.coldkeypub.ss58_address, block=new_block
            )
            new_stake = subtensor.get_stake(
                coldkey_ss58=wallet.coldkeypub.ss58_address,
                hotkey_ss58=hotkey_ss58,
                netuid=netuid,
                block=new_block,
            )
            response.data = {
                "balance_before": old_balance,
                "balance_after": new_balance,
                "stake_before": old_stake,
                "stake_after": new_stake,
            }
            return response

        if safe_staking and "Custom error: 8" in response.message:
            response.message = (
                "Price exceeded tolerance limit. Either increase price tolerance or enable partial staking."
            )

        logging.error(f"[red]{response.message}[/red]")
        return response

    except Exception as error:
        return ExtrinsicResponse.from_exception(raise_error=raise_error, error=error)


def unstake_mev_shield(
    subtensor: "Subtensor",
    wallet: "Wallet",
    netuid: int,
    hotkey_ss58: str,
    amount: Balance,
    allow_partial_stake: bool,
    rate_tolerance: float,
    safe_unstaking: bool,
    *,
    period: Optional[int] = None,
    raise_error: bool = False,
    wait_for_inclusion: bool = True,
    wait_for_finalization: bool = False,
    wait_for_revealed_execution: bool = True,
    blocks_for_revealed_execution: Optional[int] = None,
) -> ExtrinsicResponse:
    """Aligned with ``unstake_extrinsic`` MEV path + explicit reveal polling window."""
    try:
        if not (
            unlocked := ExtrinsicResponse.unlock_wallet(wallet, raise_error)
        ).success:
            return unlocked

        block = subtensor.get_current_block()
        old_balance = subtensor.get_balance(
            address=wallet.coldkeypub.ss58_address, block=block
        )
        old_stake = subtensor.get_stake(
            coldkey_ss58=wallet.coldkeypub.ss58_address,
            hotkey_ss58=hotkey_ss58,
            netuid=netuid,
            block=block,
        )

        amount.set_unit(netuid)

        if amount > old_stake:
            return ExtrinsicResponse(
                False,
                f"Not enough stake: {old_stake} to unstake: {amount} from hotkey: {hotkey_ss58}",
            ).with_log()

        if safe_unstaking:
            pool = subtensor.subnet(netuid=netuid)
            if pool.netuid == 0:
                price_with_tolerance = pool.price.tao
            else:
                price_with_tolerance = pool.price.tao * (1 - rate_tolerance)
            limit_price = Balance.from_tao(price_with_tolerance).rao
            call = SubtensorModule(subtensor).remove_stake_limit(
                netuid=netuid,
                hotkey=hotkey_ss58,
                amount_unstaked=amount.rao,
                limit_price=limit_price,
                allow_partial=allow_partial_stake,
            )
        else:
            call = SubtensorModule(subtensor).remove_stake(
                netuid=netuid,
                hotkey=hotkey_ss58,
                amount_unstaked=amount.rao,
            )

        block_before = subtensor.block
        br = (
            blocks_for_revealed_execution
            if blocks_for_revealed_execution is not None
            else mev_blocks_for_revealed_execution()
        )
        response = submit_encrypted_extrinsic(
            subtensor=subtensor,
            wallet=wallet,
            call=call,
            period=period,
            raise_error=raise_error,
            wait_for_inclusion=wait_for_inclusion,
            wait_for_finalization=wait_for_finalization,
            wait_for_revealed_execution=wait_for_revealed_execution,
            blocks_for_revealed_execution=br,
        )

        if response.success:
            sim_swap = subtensor.sim_swap(
                origin_netuid=netuid,
                destination_netuid=0,
                amount=amount,
                block=block_before,
            )
            response.transaction_tao_fee = sim_swap.tao_fee
            response.transaction_alpha_fee = sim_swap.alpha_fee.set_unit(netuid)

            if not wait_for_finalization and not wait_for_inclusion:
                return response

            logging.debug("[green]Finalized[/green]")
            new_block = subtensor.get_current_block()
            new_balance = subtensor.get_balance(
                wallet.coldkeypub.ss58_address, block=new_block
            )
            new_stake = subtensor.get_stake(
                coldkey_ss58=wallet.coldkeypub.ss58_address,
                hotkey_ss58=hotkey_ss58,
                netuid=netuid,
                block=new_block,
            )
            response.data = {
                "balance_before": old_balance,
                "balance_after": new_balance,
                "stake_before": old_stake,
                "stake_after": new_stake,
            }
            return response

        if safe_unstaking and "Custom error: 8" in response.message:
            response.message = (
                "Price exceeded tolerance limit. Either increase price tolerance or enable partial staking."
            )

        logging.error(f"[red]{response.message}[/red]")
        return response

    except Exception as error:
        return ExtrinsicResponse.from_exception(raise_error=raise_error, error=error)


def submit_calls_mev_shield(
    subtensor: "Subtensor",
    wallet: "Wallet",
    inner_calls: list[Any],
    *,
    period: Optional[int] = None,
    raise_error: bool = False,
    wait_for_inclusion: bool = True,
    wait_for_finalization: bool = False,
    wait_for_revealed_execution: bool = True,
    blocks_for_revealed_execution: Optional[int] = None,
) -> ExtrinsicResponse:
    """Encrypt and submit one or many unstake calls atomically via ``Utility.force_batch``.

    When ``len(inner_calls) > 1``, wraps calls in a single ``force_batch`` so all
    positions unstake in one MEV Shield extrinsic (same block on reveal).
    """
    try:
        if not inner_calls:
            return ExtrinsicResponse(False, "No calls to submit").with_log()

        if not (
            unlocked := ExtrinsicResponse.unlock_wallet(wallet, raise_error)
        ).success:
            return unlocked

        if len(inner_calls) == 1:
            call = inner_calls[0]
        else:
            call = subtensor.substrate.compose_call(
                call_module="Utility",
                call_function="force_batch",
                call_params={"calls": inner_calls},
            )

        br = (
            blocks_for_revealed_execution
            if blocks_for_revealed_execution is not None
            else mev_blocks_for_revealed_execution()
        )
        return submit_encrypted_extrinsic(
            subtensor=subtensor,
            wallet=wallet,
            call=call,
            period=period,
            raise_error=raise_error,
            wait_for_inclusion=wait_for_inclusion,
            wait_for_finalization=wait_for_finalization,
            wait_for_revealed_execution=wait_for_revealed_execution,
            blocks_for_revealed_execution=br,
        )

    except Exception as error:
        return ExtrinsicResponse.from_exception(raise_error=raise_error, error=error)


def unstake_all_mev_shield(
    subtensor: "Subtensor",
    wallet: "Wallet",
    netuid: int,
    hotkey_ss58: str,
    rate_tolerance: Optional[float],
    *,
    period: Optional[int] = None,
    raise_error: bool = False,
    wait_for_inclusion: bool = True,
    wait_for_finalization: bool = False,
    wait_for_revealed_execution: bool = True,
    blocks_for_revealed_execution: Optional[int] = None,
) -> ExtrinsicResponse:
    """Aligned with ``unstake_all_extrinsic`` MEV path + explicit reveal polling window."""
    try:
        if not (
            unlocked := ExtrinsicResponse.unlock_wallet(wallet, raise_error)
        ).success:
            return unlocked

        pool = subtensor.subnet(netuid=netuid) if rate_tolerance else None
        limit_price = pool.price * (1 - rate_tolerance) if rate_tolerance else None

        call = SubtensorModule(subtensor).remove_stake_full_limit(
            netuid=netuid,
            hotkey=hotkey_ss58,
            limit_price=limit_price,
        )

        br = (
            blocks_for_revealed_execution
            if blocks_for_revealed_execution is not None
            else mev_blocks_for_revealed_execution()
        )
        return submit_encrypted_extrinsic(
            subtensor=subtensor,
            wallet=wallet,
            call=call,
            period=period,
            raise_error=raise_error,
            wait_for_inclusion=wait_for_inclusion,
            wait_for_finalization=wait_for_finalization,
            wait_for_revealed_execution=wait_for_revealed_execution,
            blocks_for_revealed_execution=br,
        )

    except Exception as error:
        return ExtrinsicResponse.from_exception(raise_error=raise_error, error=error)
