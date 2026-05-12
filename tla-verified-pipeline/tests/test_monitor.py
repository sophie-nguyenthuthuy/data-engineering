from src import Monitor


def _ok_run():
    """A well-behaved pipeline run."""
    return [
        {"action": "pg_insert",         "record": "r1"},
        {"action": "pg_insert",         "record": "r2"},
        {"action": "debezium_publish",  "record": "r1"},
        {"action": "flink_consume"},
        {"action": "warehouse_load",    "record": "r1"},
        {"action": "reverse_etl",       "record": "r1"},
        {"action": "debezium_publish",  "record": "r2"},
        {"action": "flink_consume"},
        {"action": "warehouse_load",    "record": "r2"},
        {"action": "reverse_etl",       "record": "r2"},
    ]


def test_clean_run_has_no_incidents():
    m = Monitor()
    incidents = m.replay(_ok_run())
    assert incidents == []
    assert m.state.rev_etl == {"r1", "r2"}


def test_warehouse_load_without_pg_caught():
    """Bug: someone wrote a record to warehouse that was never in PG.
    Replay should flag warehouse_subset_of_pg."""
    events = [
        # Force a record into warehouse without first inserting into PG by
        # simulating a "phantom" state: directly inserting into the state.
        # Our state machine refuses warehouse_load unless r is in pg, so we
        # have to bypass — emulate by writing the bug as a manual state
        # corruption.
    ]
    m = Monitor()
    # Insert a record properly
    m.state.pg.add("r1")
    # BUG: phantom warehouse entry not in pg
    m.state.warehouse.add("r_phantom")
    m.state.rev_etl.add("r_phantom")
    from src import check_all
    violations = check_all(m.state)
    assert "WarehouseSubsetOfPg" in violations


def test_bounded_lag_violation_caught():
    """Push more events than max_lag; expect violation."""
    m = Monitor(max_lag=2)
    events = []
    for i in range(5):
        rid = f"r{i}"
        events.append({"action": "pg_insert", "record": rid})
        events.append({"action": "debezium_publish", "record": rid})
    incidents = m.replay(events)
    # Should violate BoundedLag once kafka has 3+ items
    assert any("BoundedLag" in inc.violations for inc in incidents)


def test_replay_against_violation_emits_state_snapshot():
    m = Monitor(max_lag=1)
    events = [
        {"action": "pg_insert", "record": "a"},
        {"action": "pg_insert", "record": "b"},
        {"action": "debezium_publish", "record": "a"},
        {"action": "debezium_publish", "record": "b"},  # lag now = 2 > 1
    ]
    incidents = m.replay(events)
    assert incidents, "expected an incident"
    inc = incidents[-1]
    assert "BoundedLag" in inc.violations
    assert "a" in inc.state_snapshot["kafka"]
    assert "b" in inc.state_snapshot["kafka"]


def test_eventual_delivery_in_clean_run():
    """All inserted records end up in rev_etl at the end of a clean run."""
    m = Monitor()
    m.replay(_ok_run())
    assert m.state.pg == m.state.rev_etl


def test_full_pipeline_with_many_records():
    m = Monitor(max_lag=5)
    events = []
    for i in range(10):
        rid = f"r{i}"
        events.append({"action": "pg_insert", "record": rid})
        events.append({"action": "debezium_publish", "record": rid})
        events.append({"action": "flink_consume"})
        events.append({"action": "warehouse_load", "record": rid})
        events.append({"action": "reverse_etl", "record": rid})
    incidents = m.replay(events)
    assert incidents == []
    assert len(m.state.rev_etl) == 10
