"""Demo: schedule a small DAG; compare FCFS, CP-first list, and B&B."""
from __future__ import annotations

from src import Task, DAG, list_schedule, baseline_fcfs_schedule, branch_and_bound, makespan, regret


def main():
    d = DAG()
    # An ETL DAG:
    #   ingest_users (3) ─┐
    #   ingest_orders (5) ┼─▶ join (4) ─▶ aggregate (3) ─▶ write_dw (2)
    #   ingest_items (2) ─┘                                       │
    #                                                              ├─▶ rev_etl_slack (1)
    #                                                              └─▶ rev_etl_email (2)
    d.add(Task("ingest_users",   3.0))
    d.add(Task("ingest_orders",  5.0))
    d.add(Task("ingest_items",   2.0))
    d.add(Task("join",           4.0, deps=["ingest_users", "ingest_orders", "ingest_items"]))
    d.add(Task("aggregate",      3.0, deps=["join"]))
    d.add(Task("write_dw",       2.0, deps=["aggregate"]))
    d.add(Task("rev_etl_slack",  1.0, deps=["write_dw"]))
    d.add(Task("rev_etl_email",  2.0, deps=["write_dw"]))

    cp, _ = d.critical_path_length()
    print(f"Critical path length: {cp:.1f}")

    for sched_name, fn in [("FCFS", baseline_fcfs_schedule),
                            ("CP-first", list_schedule),
                            ("B&B",      branch_and_bound)]:
        sched = fn(d, num_workers=3)
        ms = makespan(sched)
        print(f"\n{sched_name}: makespan = {ms:.1f}")
        for tid, (s, f, w) in sorted(sched.items(), key=lambda x: x[1][0]):
            print(f"  worker={w}  [{s:>4.1f} → {f:>4.1f}]  {tid}")

    r = regret(d, num_workers=3)
    print(f"\nRegret report: baseline={r.baseline_makespan:.1f}  "
          f"ours={r.our_makespan:.1f}  saved={r.regret:.1f} ({(r.speedup - 1) * 100:.1f}%)")


if __name__ == "__main__":
    main()
