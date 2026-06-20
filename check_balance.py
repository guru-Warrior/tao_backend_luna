#!/usr/bin/env python3
"""
Bittensor Wallet Balance Checker
Check free, staked, and total TAO balance for your wallet
"""

from __future__ import annotations

import concurrent.futures
from substrateinterface import SubstrateInterface
import sys
import os
import json
import asyncio
from typing import Any, Dict, List, Optional

# Try to load .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    HAS_DOTENV = True
except ImportError:
    HAS_DOTENV = False

try:
    import bittensor as bt
    HAS_BITTENSOR = True
except ImportError:
    HAS_BITTENSOR = False
    print("Warning: bittensor SDK not found. Stake balance may not work correctly.")
    print("Install with: pip install bittensor")

# Subnet positions at or below this TAO are treated as dust (omitted from API / UI).
# Chain/SDK can still return tiny residuals after a full unstake.
MIN_STAKE_TAO_UI = 1e-6

class _NoColors:
    """Strip ANSI when printing from this module (keeps f-string templates unchanged)."""

    def __getattr__(self, _name: str) -> str:
        return ""


Colors = _NoColors()

class WalletChecker:
    def __init__(self, network='finney'):
        try:
            from config import FINNEY_WS

            default_url = FINNEY_WS
        except ImportError:
            default_url = 'wss://entrypoint-finney.opentensor.ai:443'
        self._rpc_ws_url = self._get_url(network, default_url)
        self.substrate = SubstrateInterface(url=self._rpc_ws_url)
        self.network = network
        self._subtensor = None  # Cache subtensor instance
        self._stake_cache = {}  # Cache stake data: {coldkey: {'data': dict, 'timestamp': float}}
        self._cache_ttl = float(os.environ.get("WALLET_STAKE_CACHE_TTL", "3.0"))
        # Second Substrate client + single-thread pool: safe parallel free-balance vs Subtensor stake RPCs.
        self._free_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="wallet_free"
        )
        self._substrate_free_dedicated: Optional[SubstrateInterface] = None
    
    def _get_url(self, network, finney_default: str) -> str:
        urls = {
            'finney': finney_default,
            'test': 'wss://test.finney.opentensor.ai:443',
            'local': 'ws://127.0.0.1:9944'
        }
        return urls.get(network, urls['finney'])
    
    def set_cache_ttl(self, ttl_seconds):
        """Set the cache TTL for stake data
        
        Args:
            ttl_seconds: Time-to-live in seconds (default: 3.0)
        """
        self._cache_ttl = max(0.0, float(ttl_seconds))
    
    def clear_cache(self):
        """Clear the stake data cache"""
        self._stake_cache.clear()

    def invalidate_stake_cache_for_address(self, coldkey: str) -> None:
        """Drop cached stake rows for one coldkey (cheaper than clear_cache on new-block refresh)."""
        if coldkey:
            self._stake_cache.pop(coldkey, None)

    @staticmethod
    def _query_free_balance_substrate(substrate: SubstrateInterface, address: str) -> float:
        """System.Account free balance (TAO). Pure query — no shared WalletChecker state."""
        try:
            result = substrate.query(
                module='System',
                storage_function='Account',
                params=[address]
            )
            if result and result.value:
                free_balance = result.value['data']['free']
                return free_balance / 1e9  # Convert from Rao to TAO
            return 0.0
        except Exception as e:
            print(f"Error getting free balance: {e}")
            return 0.0

    def get_free_balance(self, address):
        """Get free (unstaked) TAO balance via main Substrate connection."""
        return self._query_free_balance_substrate(self.substrate, address)

    def _free_balance_worker(self, address: str) -> float:
        """Runs only on ``_free_executor`` — dedicated Substrate connection (no cross-thread sharing)."""
        if self._substrate_free_dedicated is None:
            self._substrate_free_dedicated = SubstrateInterface(url=self._rpc_ws_url)
        return self._query_free_balance_substrate(self._substrate_free_dedicated, address)
    
    async def get_staked_balance_async(self, coldkey):
        """Get staked TAO across all subnets using bittensor SDK
        
        Returns:
            dict: {
                'total': float,
                'by_subnet': [
                    {'netuid': int, 'hotkey': str, 'stake_tao': float},
                    ...
                ]
            }
        """
        if not HAS_BITTENSOR:
            print("⚠️ Bittensor SDK not available. Cannot retrieve stake balance.")
            return {'total': 0.0, 'by_subnet': []}
        
        # Check cache first
        import time
        current_time = time.time()
        if coldkey in self._stake_cache:
            cached = self._stake_cache[coldkey]
            if current_time - cached['timestamp'] < self._cache_ttl:
                return cached['data']
        
        try:
            # Bittensor 10.0.0+ uses synchronous API, wrap it in async
            # Bittensor 9.x uses async_subtensor
            if hasattr(bt, 'async_subtensor'):
                # Version 9.x - use sync API wrapped in executor
                result = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self._get_staked_balance_sync_v9,
                    coldkey
                )
            else:
                # Version 10.0.0+ uses synchronous subtensor API
                # Run the synchronous calls in a thread pool to avoid blocking
                result = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self._get_staked_balance_sync_v10,
                    coldkey
                )
            
            # Cache the result
            self._stake_cache[coldkey] = {
                'data': result,
                'timestamp': current_time
            }
            
            return result
                
        except Exception as e:
            print(f"Error getting staked balance: {e}")
            import traceback
            traceback.print_exc()
            return {'total': 0.0, 'by_subnet': []}
    
    def _get_staked_balance_sync_v9(self, coldkey):
        """Get staked balance using bittensor 9.x synchronous API"""
        try:
            # Reuse cached subtensor instance or create new one
            if self._subtensor is None:
                self._subtensor = bt.subtensor(network=self.network)
            subtensor = self._subtensor
            
            # Get all stakes for this coldkey
            all_stakes = subtensor.get_stake_for_coldkey(coldkey)
            
            if all_stakes is None:
                return {'total': 0.0, 'by_subnet': []}
            
            total_staked_tao = 0.0
            subnet_stakes = []
            
            # Sum up stakes across all subnets
            # Cache subnet info to avoid repeated queries
            subnet_cache = {}
            for stake_info in all_stakes:
                if stake_info and stake_info.stake:
                    try:
                        # Try direct conversion first (faster)
                        stake_tao_float = float(stake_info.stake.tao)
                        # Alpha subnets: stake.tao can be 0 until alpha_to_tao — always try conversion when netuid is set
                        if hasattr(stake_info, 'netuid') and stake_info.netuid is not None:
                            netuid = stake_info.netuid
                            if netuid not in subnet_cache:
                                try:
                                    subnet_cache[netuid] = subtensor.subnet(netuid=netuid)
                                except Exception:
                                    subnet_cache[netuid] = None
                            subnet_info = subnet_cache[netuid]
                            if subnet_info and hasattr(subnet_info, 'alpha_to_tao'):
                                try:
                                    stake_tao = subnet_info.alpha_to_tao(stake_info.stake)
                                    stake_tao_float = float(stake_tao.tao)
                                except Exception:
                                    pass
                        
                        total_staked_tao += stake_tao_float
                        
                        subnet_stakes.append({
                            'netuid': getattr(stake_info, 'netuid', 0),
                            'hotkey': getattr(stake_info, 'hotkey_ss58', 'unknown'),
                            'stake_tao': stake_tao_float
                        })
                    except Exception:
                        pass
            
            return {'total': total_staked_tao, 'by_subnet': subnet_stakes}
            
        except Exception as e:
            print(f"Error in sync v9 stake retrieval: {e}")
            import traceback
            traceback.print_exc()
            return {'total': 0.0, 'by_subnet': []}

    def _get_staked_balance_sync_v10(self, coldkey):
        """Get staked balance using bittensor 10.0.0+ synchronous API"""
        try:
            # Reuse cached subtensor instance or create new one
            if self._subtensor is None:
                self._subtensor = bt.Subtensor(network=self.network)
            subtensor = self._subtensor
            
            # Get stake info for this coldkey
            # Returns list of StakeInfo objects
            stake_infos = subtensor.get_stake_info_for_coldkey(coldkey_ss58=coldkey)
            
            if not stake_infos:
                return {'total': 0.0, 'by_subnet': []}
            
            total_staked_tao = 0.0
            subnet_stakes = []
            
            # Sum up all stakes
            # Cache subnet info to avoid repeated queries
            subnet_cache = {}
            for stake_info in stake_infos:
                if stake_info and stake_info.stake:
                    # In v10, stake_info.stake is already a Balance object
                    # Try direct conversion first (faster)
                    try:
                        stake_tao_float = float(stake_info.stake.tao)
                        
                        # Only query subnet if we need alpha conversion
                        if hasattr(stake_info, 'netuid') and stake_info.netuid is not None:
                            netuid = stake_info.netuid
                            # Check if we need alpha conversion (only for certain subnets)
                            # Most subnets use TAO directly, so skip subnet query if possible
                            if netuid not in subnet_cache:
                                try:
                                    subnet_cache[netuid] = subtensor.subnet(netuid=netuid)
                                except:
                                    subnet_cache[netuid] = None
                            
                            subnet_info = subnet_cache[netuid]
                            if subnet_info and hasattr(subnet_info, 'alpha_to_tao'):
                                try:
                                    stake_tao = subnet_info.alpha_to_tao(stake_info.stake)
                                    stake_tao_float = float(stake_tao.tao)
                                except:
                                    pass  # Use direct conversion if alpha conversion fails
                        
                        total_staked_tao += stake_tao_float
                        
                        subnet_stakes.append({
                            'netuid': getattr(stake_info, 'netuid', 0),
                            'hotkey': getattr(stake_info, 'hotkey_ss58', 'unknown'),
                            'stake_tao': stake_tao_float
                        })
                    except Exception:
                        pass
            
            return {'total': total_staked_tao, 'by_subnet': subnet_stakes}
            
        except Exception as e:
            print(f"Error in sync v10 stake retrieval: {e}")
            import traceback
            traceback.print_exc()
            return {'total': 0.0, 'by_subnet': []}
    
    async def get_subnet_stake_rows_async(self, address: str) -> List[Dict[str, Any]]:
        """Stake rows only (TAO), same filtering as check_wallet subnet_stakes; no free balance."""
        stake_data = await self.get_staked_balance_async(address)
        if not isinstance(stake_data, dict):
            return []
        raw_stakes = stake_data.get("by_subnet") or []
        return [
            s
            for s in raw_stakes
            if float(s.get("stake_tao", 0) or 0) > MIN_STAKE_TAO_UI
        ]
    
    async def check_wallet(self, address, show_header=True, silent=False):
        """Check wallet balance and display
        
        Args:
            silent: If True, do not print (for API / WebSocket use).
        
        Returns:
            dict: Balance information including total, free, staked, and breakdown
        """
        if show_header and not silent:
            print("="*60)
            print(f"{Colors.BOLD}BITTENSOR WALLET BALANCE{Colors.RESET}")
            print(f"Network: {self.network}")
            print("="*60)
            print()
            
            # Display wallet address
            print(f"{Colors.CYAN}Wallet Address:{Colors.RESET}")
            print(f"  {address}")
            print()
            
            print("Fetching balances...")
        
        # Parallel free (dedicated Substrate + single-thread pool) + staked (Subtensor) — no shared
        # substrate client between threads (avoids the slowdown from unsafe parallel use).
        loop = asyncio.get_running_loop()
        free_balance, stake_data = await asyncio.gather(
            loop.run_in_executor(self._free_executor, self._free_balance_worker, address),
            self.get_staked_balance_async(address),
        )
        
        # Handle both old float format and new dict format
        if isinstance(stake_data, dict):
            raw_stakes = stake_data['by_subnet']
            subnet_stakes = [
                s
                for s in raw_stakes
                if float(s.get('stake_tao', 0) or 0) > MIN_STAKE_TAO_UI
            ]
            staked_balance = sum(float(s['stake_tao']) for s in subnet_stakes)
        else:
            # Fallback for old format
            staked_balance = stake_data
            subnet_stakes = []
        
        total_balance = free_balance + staked_balance
        
        if not silent:
            # Display balances
            if show_header:
                print()
            print(f"{Colors.GREEN}Free Balance:    {free_balance:>20,.9f} TAO{Colors.RESET}")
            
            # Display staked balance breakdown by subnet (only show stakes > 0.01 TAO)
            if subnet_stakes:
                print(f"{Colors.YELLOW}Staked Balance:  {staked_balance:>20,.9f} TAO{Colors.RESET}")
                # Filter stakes to only show those over 0.01 TAO
                significant_stakes = [stake for stake in subnet_stakes if stake['stake_tao'] > 0.01]
                if significant_stakes:
                    print(f"  {Colors.CYAN}Breakdown by Subnet (stakes > 0.01 TAO):{Colors.RESET}")
                    for stake in significant_stakes:
                        netuid = stake['netuid']
                        hotkey_short = stake['hotkey'][:10] + "..." + stake['hotkey'][-8:] if len(stake['hotkey']) > 20 else stake['hotkey']
                        stake_tao = stake['stake_tao']
                        print(f"    Subnet {netuid:>3}: {stake_tao:>18,.9f} TAO  (Validator: {hotkey_short})")
            else:
                print(f"{Colors.YELLOW}Staked Balance:  {staked_balance:>20,.9f} TAO{Colors.RESET}")
            
            print("-"*60)
            print(f"{Colors.BOLD}{Colors.RED}Total Balance:   {total_balance:>20,.9f} TAO{Colors.RESET}")
            print()
        
        return {
            'total': total_balance,
            'free': free_balance,
            'staked': staked_balance,
            'subnet_stakes': subnet_stakes
        }


def get_address_from_wallet_name(wallet_name, wallet_password=None, wallet_path=None):
    """Get coldkey address from wallet name and password"""
    
    # Default wallet path
    if not wallet_path:
        wallet_path = os.path.expanduser("~/.bittensor/wallets")
    
    wallet_dir = os.path.join(wallet_path, wallet_name)
    
    print(f"Loading wallet: {wallet_name}")
    print(f"Wallet directory: {wallet_dir}")
    
    if not os.path.exists(wallet_dir):
        print(f"{Colors.RED}✗ Wallet directory not found{Colors.RESET}")
        
        # List available wallets
        if os.path.exists(wallet_path):
            available = [d for d in os.listdir(wallet_path) 
                       if os.path.isdir(os.path.join(wallet_path, d))]
            if available:
                print(f"\nAvailable wallets in {wallet_path}:")
                for w in available:
                    print(f"  - {w}")
        return None
    
    # Method 1: Try reading from public key text file
    possible_pub_files = [
        os.path.join(wallet_dir, "coldkeypub.txt"),
        os.path.join(wallet_dir, "coldkeypub"),
        os.path.join(wallet_dir, "coldkey.pub"),
    ]
    
    for pub_file in possible_pub_files:
        if os.path.exists(pub_file):
            try:
                with open(pub_file, 'r') as f:
                    address = f.read().strip()
                    if address and address.startswith('5') and len(address) > 40:
                        print(f"✓ Loaded address from {os.path.basename(pub_file)}")
                        return address
            except Exception as e:
                pass
    
    # Method 2: Try reading from coldkey JSON file
    coldkey_file = os.path.join(wallet_dir, "coldkey")
    if not os.path.exists(coldkey_file):
        coldkey_file = os.path.join(wallet_dir, "coldkey.json")
    
    if os.path.exists(coldkey_file):
        try:
            with open(coldkey_file, 'r') as f:
                keyfile_data = json.load(f)
            
            # Check if ss58Address is in the JSON (available without decryption)
            if 'ss58Address' in keyfile_data:
                address = keyfile_data['ss58Address']
                if address and address.startswith('5'):
                    print(f"✓ Loaded address from coldkey file")
                    return address
            
            # If we need to decrypt
            if wallet_password:
                try:
                    from substrateinterface import Keypair
                    keypair = Keypair.create_from_encrypted_json(keyfile_data, wallet_password)
                    address = keypair.ss58_address
                    print(f"✓ Decrypted coldkey and loaded address")
                    return address
                except Exception as e:
                    print(f"{Colors.RED}✗ Failed to decrypt wallet: {e}{Colors.RESET}")
                    print("Check that WALLET_PASSWORD is correct")
            else:
                # Try to get public key without decryption
                if 'publicKey' in keyfile_data:
                    try:
                        from substrateinterface import Keypair
                        public_key = keyfile_data['publicKey']
                        if public_key.startswith('0x'):
                            public_key = public_key[2:]
                        keypair = Keypair(public_key=bytes.fromhex(public_key), ss58_format=42)
                        address = keypair.ss58_address
                        print(f"✓ Derived address from public key")
                        return address
                    except Exception as e:
                        pass
                
                print(f"{Colors.YELLOW}⚠ Wallet may be encrypted but no password provided{Colors.RESET}")
                
        except json.JSONDecodeError as e:
            print(f"Error reading coldkey file: {e}")
        except Exception as e:
            print(f"Error processing coldkey file: {e}")
    
    # Method 3: Try using bittensor SDK if available
    if HAS_BITTENSOR:
        try:
            print("\nTrying bittensor SDK...")
            wallet = bt.wallet(name=wallet_name, path=wallet_path)
            
            # Try to access coldkey
            if wallet_password:
                try:
                    coldkey = wallet.coldkey
                    address = coldkey.ss58_address
                    print(f"✓ Loaded via bittensor SDK")
                    return address
                except:
                    pass
            
            # Try coldkeypub
            if hasattr(wallet, 'coldkeypub') and wallet.coldkeypub:
                address = wallet.coldkeypub.ss58_address
                print(f"✓ Loaded via bittensor SDK (coldkeypub)")
                return address
                
        except Exception as e:
            print(f"Bittensor SDK failed: {e}")
    
    print(f"{Colors.RED}✗ Could not load wallet address{Colors.RESET}")
    return None


def resolve_wallet_address_from_env():
    """Resolve coldkey ss58 for the mempool UI balance panel (no console output).

    Tries in order:
    ``WALLET_ADDRESS`` / ``COLDKEY_ADDRESS``, ``SEED_PHRASE_1`` (mnemonic),
    then ``WALLET_NAME`` (+ ``WALLET_PASSWORD`` / local wallet files).
    """
    addr = os.getenv("WALLET_ADDRESS") or os.getenv("COLDKEY_ADDRESS")
    if addr and str(addr).strip():
        return str(addr).strip()
    phrase = os.getenv("SEED_PHRASE_1", "").strip()
    if phrase:
        try:
            from substrateinterface import Keypair
            return Keypair.create_from_mnemonic(phrase).ss58_address
        except Exception:
            return None
    name = os.getenv("WALLET_NAME") or os.getenv("BITTENSOR_WALLET_NAME")
    if not name:
        return None
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        return get_address_from_wallet_name(
            name,
            os.getenv("WALLET_PASSWORD"),
            os.getenv("WALLET_PATH"),
        )


if __name__ == "__main__":
    sys.stderr.write(
        "Use the FastAPI app: POST /api/wallet-balance, or import WalletChecker from "
        "check_balance in your own script.\n"
    )
    sys.exit(2)

