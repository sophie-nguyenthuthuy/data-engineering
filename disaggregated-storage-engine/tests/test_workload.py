"""Workload generator tests."""

from __future__ import annotations

import collections

from disagg.workload import scan_workload, tpcc_workload, zipf_workload


def test_scan_workload_yields_sequential():
    pages = list(scan_workload(n_pages=10, n_passes=2))
    assert len(pages) == 20
    assert [p.page_no for p in pages[:10]] == list(range(10))


def test_zipf_workload_count():
    pages = list(zipf_workload(n_pages=100, n_ops=1000))
    assert len(pages) == 1000


def test_zipf_workload_concentrates_on_low_ranks():
    """Page 0 (rank 0) should be hit more than mid-range pages."""
    pages = list(zipf_workload(n_pages=100, n_ops=10_000, alpha=1.5, seed=0))
    counts = collections.Counter(p.page_no for p in pages)
    # Page 0 should be in the top-3 hottest pages
    top_pages = [pn for pn, _ in counts.most_common(3)]
    assert 0 in top_pages


def test_tpcc_workload_respects_warehouse_locality():
    pages = list(tpcc_workload(n_warehouses=4, n_transactions=200))
    # Warehouse pages 0..3 should be heavily hit
    warehouse_count = sum(1 for p in pages if p.page_no < 4)
    # Each transaction touches one warehouse page; we have 200 tx
    assert warehouse_count == 200


def test_tpcc_mix_includes_multiple_transaction_types():
    """Across many tx, page distribution should hit warehouse, customer, stock pages."""
    pages = list(tpcc_workload(n_warehouses=4, n_transactions=500))
    page_nos = [p.page_no for p in pages]
    warehouse_hit = any(pn < 4 for pn in page_nos)
    customer_hit = any(1000 <= pn < 2000 for pn in page_nos)
    stock_hit = any(pn >= 10_000 for pn in page_nos)
    assert warehouse_hit
    assert customer_hit
    assert stock_hit
