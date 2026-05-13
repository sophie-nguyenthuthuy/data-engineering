"""multi-source-collector — HTTP / CSV / Excel / FTP / GSheet → staging zone."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from msc.manifest import Manifest, ManifestEntry
    from msc.naming import NamingConvention, StagedKey
    from msc.runner import IngestionResult, Runner
    from msc.sources.base import Record, Source, SourceError
    from msc.sources.csv_src import CSVSource
    from msc.sources.excel import ExcelSource
    from msc.sources.ftp import FTPSource
    from msc.sources.gsheet import GoogleSheetSource
    from msc.sources.http_api import HTTPAPISource
    from msc.staging.zone import StagingZone


_LAZY: dict[str, tuple[str, str]] = {
    "NamingConvention": ("msc.naming", "NamingConvention"),
    "StagedKey": ("msc.naming", "StagedKey"),
    "Manifest": ("msc.manifest", "Manifest"),
    "ManifestEntry": ("msc.manifest", "ManifestEntry"),
    "Source": ("msc.sources.base", "Source"),
    "SourceError": ("msc.sources.base", "SourceError"),
    "Record": ("msc.sources.base", "Record"),
    "CSVSource": ("msc.sources.csv_src", "CSVSource"),
    "ExcelSource": ("msc.sources.excel", "ExcelSource"),
    "HTTPAPISource": ("msc.sources.http_api", "HTTPAPISource"),
    "FTPSource": ("msc.sources.ftp", "FTPSource"),
    "GoogleSheetSource": ("msc.sources.gsheet", "GoogleSheetSource"),
    "StagingZone": ("msc.staging.zone", "StagingZone"),
    "Runner": ("msc.runner", "Runner"),
    "IngestionResult": ("msc.runner", "IngestionResult"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        module, attr = _LAZY[name]
        return getattr(import_module(module), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CSVSource",
    "ExcelSource",
    "FTPSource",
    "GoogleSheetSource",
    "HTTPAPISource",
    "IngestionResult",
    "Manifest",
    "ManifestEntry",
    "NamingConvention",
    "Record",
    "Runner",
    "Source",
    "SourceError",
    "StagedKey",
    "StagingZone",
    "__version__",
]
