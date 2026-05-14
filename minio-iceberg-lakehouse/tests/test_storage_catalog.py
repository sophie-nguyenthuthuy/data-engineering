"""Storage backends + catalog tests."""

from __future__ import annotations

import pytest

from lake.catalog import Catalog, CatalogError
from lake.storage.base import CASMismatch
from lake.storage.inmemory import InMemoryStorage
from lake.storage.local_fs import LocalFSStorage

# -------------------------------------------------- InMemoryStorage


def test_inmemory_put_get_round_trip():
    s = InMemoryStorage()
    etag = s.put("foo/bar.json", b"hello")
    assert s.get("foo/bar.json") == b"hello"
    assert s.head_etag("foo/bar.json") == etag


def test_inmemory_rejects_empty_path():
    with pytest.raises(ValueError):
        InMemoryStorage().put("", b"x")


def test_inmemory_atomic_put_requires_correct_etag():
    s = InMemoryStorage()
    etag1 = s.atomic_put("k", b"v1", expected_etag=None)
    with pytest.raises(CASMismatch):
        s.atomic_put("k", b"v2", expected_etag="bogus")
    s.atomic_put("k", b"v2", expected_etag=etag1)


def test_inmemory_atomic_put_first_write_requires_none():
    s = InMemoryStorage()
    with pytest.raises(CASMismatch):
        s.atomic_put("k", b"v", expected_etag="any")
    s.atomic_put("k", b"v", expected_etag=None)


def test_inmemory_get_missing_raises():
    with pytest.raises(KeyError):
        InMemoryStorage().get("nope")


def test_inmemory_exists_and_head_for_missing():
    s = InMemoryStorage()
    assert not s.exists("k")
    assert s.head_etag("k") is None


# ---------------------------------------------------- LocalFSStorage


def test_localfs_put_round_trip(tmp_path):
    s = LocalFSStorage(root=tmp_path)
    s.put("a/b/c.bin", b"data")
    assert s.get("a/b/c.bin") == b"data"


def test_localfs_rejects_absolute_path(tmp_path):
    s = LocalFSStorage(root=tmp_path)
    with pytest.raises(ValueError):
        s.put("/abs", b"x")


def test_localfs_atomic_put_cas(tmp_path):
    s = LocalFSStorage(root=tmp_path)
    etag1 = s.atomic_put("k", b"v1", expected_etag=None)
    with pytest.raises(CASMismatch):
        s.atomic_put("k", b"v2", expected_etag="wrong")
    s.atomic_put("k", b"v2", expected_etag=etag1)


# ---------------------------------------------------------- Catalog


def test_catalog_register_lookup_round_trip():
    c = Catalog()
    c.register(namespace="db", name="orders", metadata_path="meta/v1.json")
    assert c.lookup("db", "orders") == "meta/v1.json"


def test_catalog_rejects_duplicate_registration():
    c = Catalog()
    c.register(namespace="db", name="orders", metadata_path="m")
    with pytest.raises(CatalogError):
        c.register(namespace="db", name="orders", metadata_path="m2")


def test_catalog_update_pointer():
    c = Catalog()
    c.register(namespace="db", name="orders", metadata_path="m1")
    c.update_pointer(namespace="db", name="orders", metadata_path="m2")
    assert c.lookup("db", "orders") == "m2"


def test_catalog_update_unknown_raises():
    c = Catalog()
    with pytest.raises(CatalogError):
        c.update_pointer(namespace="db", name="ghost", metadata_path="x")


def test_catalog_list_tables_and_namespaces():
    c = Catalog()
    c.register(namespace="db", name="orders", metadata_path="a")
    c.register(namespace="db", name="customers", metadata_path="b")
    c.register(namespace="lake", name="events", metadata_path="c")
    assert c.list_tables("db") == ["customers", "orders"]
    assert c.list_namespaces() == ["db", "lake"]


def test_catalog_drop_table():
    c = Catalog()
    c.register(namespace="db", name="x", metadata_path="m")
    c.drop("db", "x")
    with pytest.raises(CatalogError):
        c.lookup("db", "x")


def test_catalog_rejects_empty_names():
    c = Catalog()
    with pytest.raises(ValueError):
        c.register(namespace="", name="x", metadata_path="m")
    with pytest.raises(ValueError):
        c.register(namespace="db", name="", metadata_path="m")
    with pytest.raises(ValueError):
        c.register(namespace="db", name="x", metadata_path="")
