from .base import BaseSource

# Adapters are imported lazily inside each class; the registry maps type strings to
# module paths so we can import on demand without pulling optional SDKs at startup.
_SOURCE_MODULE_MAP = {
    "postgres": ("src.sources.postgres", "PostgresSource"),
    "bigquery": ("src.sources.bigquery", "BigQuerySource"),
    "snowflake": ("src.sources.snowflake", "SnowflakeSource"),
}


class _LazyRegistry(dict):
    def __missing__(self, key):
        entry = _SOURCE_MODULE_MAP.get(key)
        if entry is None:
            return None
        import importlib
        module = importlib.import_module(entry[0])
        cls = getattr(module, entry[1])
        self[key] = cls
        return cls


SOURCE_REGISTRY: dict[str, type[BaseSource]] = _LazyRegistry()

__all__ = ["BaseSource", "SOURCE_REGISTRY"]
