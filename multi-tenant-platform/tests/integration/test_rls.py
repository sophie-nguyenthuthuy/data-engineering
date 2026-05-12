"""
Integration tests for Row-Level Security.
Require a running PostgreSQL with migrations applied.
Run with: pytest tests/integration/ -m integration

These tests verify the core invariant: tenant A cannot read tenant B's data,
even when using the same database role.
"""
import uuid
import pytest
import pytest_asyncio

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text

pytestmark = pytest.mark.integration

DB_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/platform"


@pytest_asyncio.fixture
async def raw_session():
    engine = create_async_engine(DB_URL, echo=False)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            yield session
    await engine.dispose()


@pytest.mark.asyncio
async def test_tenant_cannot_read_other_tenants_records(raw_session: AsyncSession) -> None:
    tenant_a = uuid.uuid4()
    tenant_b = uuid.uuid4()
    dataset_id = uuid.uuid4()
    record_id_a = uuid.uuid4()

    # Insert without RLS (as admin)
    await raw_session.execute(text("""
        INSERT INTO platform.tenants (id, name, slug, tier) VALUES
        (:a, 'Tenant A', 'tenant-a', 'free'),
        (:b, 'Tenant B', 'tenant-b', 'free')
        ON CONFLICT DO NOTHING
    """), {"a": tenant_a, "b": tenant_b})

    await raw_session.execute(text("""
        INSERT INTO platform.datasets (id, tenant_id, name, schema_definition)
        VALUES (:did, :tid, 'Test Dataset', '{}')
        ON CONFLICT DO NOTHING
    """), {"did": dataset_id, "tid": tenant_a})

    await raw_session.execute(text("""
        INSERT INTO platform.data_records (id, tenant_id, dataset_id, data)
        VALUES (:rid, :tid, :did, '{"secret": "tenant-a-value"}')
        ON CONFLICT DO NOTHING
    """), {"rid": record_id_a, "tid": tenant_a, "did": dataset_id})

    # Now query as tenant B — should see no rows
    await raw_session.execute(
        text("SET LOCAL app.tenant_id = :tid"), {"tid": str(tenant_b)}
    )
    result = await raw_session.execute(
        text("SELECT * FROM platform.data_records WHERE id = :rid"),
        {"rid": record_id_a},
    )
    rows = result.fetchall()
    assert len(rows) == 0, "RLS must prevent tenant B from reading tenant A's records"

    # Tenant A can read their own record
    await raw_session.execute(
        text("SET LOCAL app.tenant_id = :tid"), {"tid": str(tenant_a)}
    )
    result = await raw_session.execute(
        text("SELECT * FROM platform.data_records WHERE id = :rid"),
        {"rid": record_id_a},
    )
    rows = result.fetchall()
    assert len(rows) == 1, "Tenant A must be able to read their own records"
