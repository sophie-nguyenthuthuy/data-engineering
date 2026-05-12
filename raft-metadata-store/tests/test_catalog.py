"""Integration tests for the data catalog API."""

import asyncio
import pytest
from catalog.api import CatalogAPI
from catalog.models import Column, DataLineage, Dataset, Table
from .conftest import wait_for_leader


pytestmark = pytest.mark.asyncio


@pytest.fixture
def catalog_fixture(three_node_cluster):
    return three_node_cluster


async def test_create_and_get_dataset(catalog_fixture):
    nodes, stores = catalog_fixture
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    store_idx = [n.node_id for n in nodes].index(leader.node_id)
    kv = stores[store_idx]
    catalog = CatalogAPI(leader, kv)

    ds = Dataset(name="raw_events", description="Raw clickstream events",
                 owner="data-team", location="s3://bucket/raw/events")
    await catalog.create_dataset(ds)

    result = await catalog.get_dataset("raw_events")
    assert result is not None
    assert result.name == "raw_events"
    assert result.owner == "data-team"


async def test_list_datasets(catalog_fixture):
    nodes, stores = catalog_fixture
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    kv = stores[[n.node_id for n in nodes].index(leader.node_id)]
    catalog = CatalogAPI(leader, kv)

    for name in ["ds_a", "ds_b", "ds_c"]:
        await catalog.create_dataset(Dataset(name=name))

    datasets = await catalog.list_datasets()
    names = {d.name for d in datasets}
    assert {"ds_a", "ds_b", "ds_c"} <= names


async def test_create_table_with_columns(catalog_fixture):
    nodes, stores = catalog_fixture
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    kv = stores[[n.node_id for n in nodes].index(leader.node_id)]
    catalog = CatalogAPI(leader, kv)

    await catalog.create_dataset(Dataset(name="warehouse"))
    table = Table(
        name="orders",
        dataset_name="warehouse",
        description="Order transactions",
        owner="analytics",
        columns=[
            Column("id", "BIGINT", is_primary_key=True),
            Column("user_id", "BIGINT"),
            Column("amount", "DECIMAL(10,2)"),
            Column("created_at", "TIMESTAMP", nullable=False),
        ],
        tags=["finance", "core"],
    )
    await catalog.create_table(table)

    result = await catalog.get_table("warehouse", "orders")
    assert result is not None
    assert len(result.columns) == 4
    assert result.columns[0].name == "id"
    assert result.columns[0].is_primary_key


async def test_add_column(catalog_fixture):
    nodes, stores = catalog_fixture
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    kv = stores[[n.node_id for n in nodes].index(leader.node_id)]
    catalog = CatalogAPI(leader, kv)

    await catalog.create_dataset(Dataset(name="mydb"))
    await catalog.create_table(Table(name="users", dataset_name="mydb",
                                     columns=[Column("id", "INT")]))
    updated = await catalog.add_column(
        "mydb", "users", Column("email", "VARCHAR(255)")
    )
    assert any(c.name == "email" for c in updated.columns)


async def test_lineage_tracking(catalog_fixture):
    nodes, stores = catalog_fixture
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    kv = stores[[n.node_id for n in nodes].index(leader.node_id)]
    catalog = CatalogAPI(leader, kv)

    # raw → enriched → agg
    await catalog.add_lineage(DataLineage(
        source="raw.events", target="enriched.events",
        job="enrich_pipeline", description="Adds user metadata"
    ))
    await catalog.add_lineage(DataLineage(
        source="enriched.events", target="agg.daily_counts",
        job="aggregate_daily", description="Daily aggregation"
    ))

    downstream = await catalog.get_lineage_downstream("raw.events")
    assert any(e.target == "enriched.events" for e in downstream)

    upstream = await catalog.get_lineage_upstream("agg.daily_counts")
    assert any(e.source == "enriched.events" for e in upstream)


async def test_lineage_impact_analysis(catalog_fixture):
    nodes, stores = catalog_fixture
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    kv = stores[[n.node_id for n in nodes].index(leader.node_id)]
    catalog = CatalogAPI(leader, kv)

    # Build a DAG: A → B → C → D
    for src, tgt in [("A", "B"), ("B", "C"), ("C", "D")]:
        await catalog.add_lineage(DataLineage(source=src, target=tgt, job="etl"))

    impacted = await catalog.get_lineage_impact("A", depth=5)
    assert "B" in impacted
    assert "C" in impacted
    assert "D" in impacted


async def test_tag_search(catalog_fixture):
    nodes, stores = catalog_fixture
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    kv = stores[[n.node_id for n in nodes].index(leader.node_id)]
    catalog = CatalogAPI(leader, kv)

    await catalog.create_dataset(Dataset(name="tagged_db"))
    await catalog.create_table(Table(
        name="sales", dataset_name="tagged_db", tags=["pii", "finance"]
    ))
    await catalog.create_table(Table(
        name="customers", dataset_name="tagged_db", tags=["pii"]
    ))

    pii_tables = await catalog.find_by_tag("pii")
    assert "tagged_db.sales" in pii_tables
    assert "tagged_db.customers" in pii_tables

    finance_tables = await catalog.find_by_tag("finance")
    assert "tagged_db.sales" in finance_tables
    assert "tagged_db.customers" not in finance_tables
