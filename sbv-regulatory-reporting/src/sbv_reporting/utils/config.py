"""Load and expose pipeline configuration."""
import os
from pathlib import Path

import yaml


def _find_default_config() -> Path:
    # Walk up from CWD looking for config/sbv_config.yaml
    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents]:
        candidate = parent / "config" / "sbv_config.yaml"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "sbv_config.yaml not found. Set SBV_CONFIG env var or run from the project root."
    )


def load_config(path: str | Path | None = None) -> dict:
    if path:
        cfg_path = Path(path)
    elif "SBV_CONFIG" in os.environ:
        cfg_path = Path(os.environ["SBV_CONFIG"])
    else:
        cfg_path = _find_default_config()
    with cfg_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


_config: dict | None = None


def get_config() -> dict:
    global _config
    if _config is None:
        _config = load_config()
    return _config
