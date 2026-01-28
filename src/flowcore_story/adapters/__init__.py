"""Public adapter helpers and backwards compatibility shims."""

from flowcore.adapters.factory import available_site_keys, get_adapter
from flowcore.adapters.registry import adapter_registry


def get_adapter_for_site(site_key: str):
    """
    Backwards-compatible alias for ``get_adapter``.

    The health-checker worker and some docs still import ``get_adapter_for_site``,
    so keep this thin wrapper to avoid import errors.
    """

    return get_adapter(site_key)


__all__ = [
    "available_site_keys",
    "adapter_registry",
    "get_adapter",
    "get_adapter_for_site",
]
