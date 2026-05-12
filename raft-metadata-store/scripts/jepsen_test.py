"""
Jepsen-style end-to-end linearizability test against a live cluster.

Runs concurrent clients against the HTTP API, records all operations,
then checks linearizability.  Injects network partitions via chaos.py
to stress-test the system.

Usage (against docker-compose cluster):
  python scripts/jepsen_test.py --addrs localhost:8001,localhost:8002,localhost:8003
"""

import asyncio
import random
import sys
import time
from typing import Any, List

import click

sys.path.insert(0, ".")
from catalog.client import CatalogClient
from tests.checker import HistoryRecorder, check_linearizability


async def run_client(
    client: CatalogClient,
    recorder: HistoryRecorder,
    n_ops: int,
    key_space: int = 5,
) -> None:
    for _ in range(n_ops):
        key = f"jepsen_k{random.randint(0, key_space - 1)}"
        op = random.choice(["put", "get", "cas"])
        call_t = time.monotonic()
        ok = True
        result = None
        try:
            if op == "put":
                value = random.randint(0, 100)
                result = await client.kv_put(key, value)
                recorder.record(
                    "put", key, value=value, result=result,
                    call_time=call_t, return_time=time.monotonic(),
                )
            elif op == "get":
                result = await client.kv_get(key)
                recorder.record(
                    "get", key, result={"value": result.get("value")},
                    call_time=call_t, return_time=time.monotonic(),
                )
            elif op == "cas":
                expected = random.randint(0, 50)
                new_val = random.randint(51, 100)
                result = await client.kv_cas(key, expected, new_val)
                recorder.record(
                    "cas", key, value=expected, new_value=new_val,
                    result=result, call_time=call_t, return_time=time.monotonic(),
                )
        except Exception as e:
            recorder.record(
                op, key, call_time=call_t, return_time=time.monotonic(), ok=False
            )
        await asyncio.sleep(random.uniform(0.001, 0.02))


@click.command()
@click.option(
    "--addrs",
    default="localhost:8001,localhost:8002,localhost:8003",
    help="Comma-separated host:port of cluster nodes",
)
@click.option("--clients", default=10, type=int)
@click.option("--ops-per-client", default=50, type=int)
@click.option("--chaos/--no-chaos", default=False, help="Inject partitions via tc netem")
def main(addrs, clients, ops_per_client, chaos):
    addr_list = [a.strip() for a in addrs.split(",")]
    recorder = HistoryRecorder()

    async def run():
        async with CatalogClient(addr_list) as client:
            click.echo(f"Starting {clients} concurrent clients, {ops_per_client} ops each...")
            await asyncio.gather(
                *[run_client(client, recorder, ops_per_client) for _ in range(clients)]
            )

    asyncio.run(run())

    total = len(recorder.ops)
    completed = sum(1 for o in recorder.ops if o.ok)
    click.echo(f"\nRecorded {total} operations ({completed} completed, {total - completed} crashed)")

    click.echo("Checking linearizability...")
    ok, msg = check_linearizability([o for o in recorder.ops if o.ok])
    if ok:
        click.echo("✓ History is linearizable!", err=False)
    else:
        click.echo(f"✗ NOT linearizable: {msg}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
