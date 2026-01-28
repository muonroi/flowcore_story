"""Factory helpers for retrieving site adapters."""
from flowcore.adapters.registry import adapter_registry


def get_adapter(site_key: str):
    """Return an adapter instance registered for ``site_key``."""

    return adapter_registry.get(site_key)


def available_site_keys():
    """Return the discovered site keys that can be instantiated."""

    return list(adapter_registry.available_site_keys())
