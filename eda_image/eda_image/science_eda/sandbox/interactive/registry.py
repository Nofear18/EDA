"""SQLite durable registry for trusted-local interactive workspace sessions.

The public registry is composed from focused persistence domains.  Connection
ownership and transaction primitives live in :mod:`registry_storage`; the
mixins contain create, execution, and lifecycle state-machine operations.
"""

from science_eda.sandbox.interactive.registry_codec import (
    decode_history_cursor,
    encode_history_cursor,
)
from science_eda.sandbox.interactive.registry_constants import (
    ACTIVE_EXECUTION_STATES as _ACTIVE_EXECUTION_STATES,
)
from science_eda.sandbox.interactive.registry_create import RegistryCreateMixin
from science_eda.sandbox.interactive.registry_execution import RegistryExecutionMixin
from science_eda.sandbox.interactive.registry_lifecycle import RegistryLifecycleMixin
from science_eda.sandbox.interactive.registry_schema import REGISTRY_USER_VERSION
from science_eda.sandbox.interactive.registry_startup import RegistryStartupMixin
from science_eda.sandbox.interactive.registry_storage import RegistryStorage


class InteractiveRegistry(
    RegistryCreateMixin,
    RegistryStartupMixin,
    RegistryExecutionMixin,
    RegistryLifecycleMixin,
    RegistryStorage,
):
    """Thread-safe single-process owner of the v1 interactive registry."""


__all__ = [
    "InteractiveRegistry",
    "REGISTRY_USER_VERSION",
    "decode_history_cursor",
    "encode_history_cursor",
]
