"""Self-contained faker (decoy self-proxy extrinsic) integration.

Design mirrors :mod:`trader` so that the faker's submit latency matches the
deposit path exactly. All logic lives inside the backend package; there is
no dependency on the standalone ``/tao/faker`` directory.
"""

from .stake import (
    Faker,
    FakerResult,
    faker_last_init_error,
    get_faker,
    read_env_defaults,
)

__all__ = [
    "Faker",
    "FakerResult",
    "faker_last_init_error",
    "get_faker",
    "read_env_defaults",
]
