from .base import BaseDestination

_DESTINATION_MODULE_MAP = {
    "slack": ("src.destinations.slack", "SlackDestination"),
    "email": ("src.destinations.email", "EmailDestination"),
    "salesforce": ("src.destinations.salesforce", "SalesforceDestination"),
    "hubspot": ("src.destinations.hubspot", "HubSpotDestination"),
}


class _LazyRegistry(dict):
    def __missing__(self, key):
        entry = _DESTINATION_MODULE_MAP.get(key)
        if entry is None:
            return None
        import importlib
        module = importlib.import_module(entry[0])
        cls = getattr(module, entry[1])
        self[key] = cls
        return cls


DESTINATION_REGISTRY: dict[str, type[BaseDestination]] = _LazyRegistry()

__all__ = ["BaseDestination", "DESTINATION_REGISTRY"]
