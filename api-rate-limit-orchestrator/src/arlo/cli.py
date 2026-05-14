"""``arlctl`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from arlo import __version__

    print(f"api-rate-limit-orchestrator {__version__}")
    return 0


def cmd_lua(_args: argparse.Namespace) -> int:
    from arlo.storage.redis_lua import render_redis_lua

    print(render_redis_lua())
    return 0


def cmd_sim(args: argparse.Namespace) -> int:
    """Simulate N workers acquiring against the in-memory backend."""
    import threading

    from arlo.bucket import TokenBucket
    from arlo.orchestrator import AcquireTimeout, Orchestrator
    from arlo.quota import Quota
    from arlo.storage.inmemory import InMemoryStorage

    storage = InMemoryStorage()
    bucket = TokenBucket(key="demo", quota=Quota.per_second(args.rps), storage=storage)
    counts: list[int] = [0] * args.workers
    locks_taken: list[int] = [0]

    def worker(i: int) -> None:
        orch = Orchestrator(bucket=bucket, max_wait=args.duration + 1.0, max_attempts=1_000_000)
        end = __import__("time").monotonic() + args.duration
        while __import__("time").monotonic() < end:
            try:
                orch.wait_and_acquire(1.0)
                counts[i] += 1
                locks_taken[0] += 1
            except AcquireTimeout:
                return

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"workers={args.workers}  rps={args.rps}  duration={args.duration}s")
    print(f"total_acquired = {locks_taken[0]}")
    print(f"per_worker     = {counts}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="arlctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)
    sub.add_parser("lua", help="emit the atomic-take Lua script").set_defaults(func=cmd_lua)

    s = sub.add_parser("sim", help="simulate N workers sharing one bucket")
    s.add_argument("--workers", type=int, default=4)
    s.add_argument("--rps", type=int, default=10)
    s.add_argument("--duration", type=float, default=2.0)
    s.set_defaults(func=cmd_sim)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
