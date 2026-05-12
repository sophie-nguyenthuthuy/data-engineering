"""Learned index structures for LSM-tree storage engines."""
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("lsm-learned")
except PackageNotFoundError:
    __version__ = "0.0.0"
