"""Buy-bot integration.

The standalone buy_bot logic lives in `buy_bot_main.py` (ported verbatim from
`tao/buy_bot/buy_bot.py`). It is launched as a subprocess by :class:`BotManager`
and configured exclusively through a dedicated env file whose dynamic values
are produced from UI-supplied rules/settings.
"""

from .manager import BotManager, BotRule, compile_rules_to_env

__all__ = ["BotManager", "BotRule", "compile_rules_to_env"]
