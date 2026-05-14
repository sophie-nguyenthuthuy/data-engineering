"""``mtipctl`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from mtip import __version__

    print(f"multi-tenant-ingestion-platform {__version__}")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from mtip.platform import Platform
    from mtip.quota import ResourceQuota
    from mtip.registry.tenant import Tenant

    plat = Platform()
    for i in range(args.tenants):
        plat.register_tenant(
            Tenant(
                id=f"team-{i}",
                display_name=f"Team {i}",
                quota=ResourceQuota(cpu_cores=2.0, storage_gb=10.0, ingestion_qps=100.0),
            )
        )
    # Each tenant submits N jobs.
    for i in range(args.tenants):
        for j in range(args.jobs):
            plat.submit_job(f"team-{i}", f"job-{j}", cpu=0.5)
    sched = plat.scheduler.schedule(args.tenants * args.jobs)
    print(f"tenants={args.tenants} jobs/tenant={args.jobs}")
    print(f"scheduled = {len(sched)}")
    seen = {s.job.tenant_id for s in sched}
    print(f"distinct_tenants_in_order = {len(seen)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="mtipctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)
    d = sub.add_parser("demo", help="register N tenants, submit M jobs each, schedule")
    d.add_argument("--tenants", type=int, default=3)
    d.add_argument("--jobs", type=int, default=4)
    d.set_defaults(func=cmd_demo)
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
