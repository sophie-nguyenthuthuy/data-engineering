"""
Network chaos tool using tc netem.

Requires root / sudo and the iproute2 `tc` command.
Designed for use with Docker containers in the docker-compose cluster.

Usage:
  # Partition node1 from node2
  python scripts/chaos.py partition --src node1 --dst node2

  # Add 50ms latency to node3
  python scripts/chaos.py delay --node node3 --ms 50

  # Drop 20% of packets from node2
  python scripts/chaos.py loss --node node2 --pct 20

  # Restore all rules
  python scripts/chaos.py heal
"""

import subprocess
import sys
import click

# Map container names to their IPs in the compose network
NODE_IPS = {
    "node1": "172.20.0.11",
    "node2": "172.20.0.12",
    "node3": "172.20.0.13",
    "node4": "172.20.0.14",
    "node5": "172.20.0.15",
}

IFACE = "eth0"


def _tc(container: str, *args) -> None:
    cmd = ["docker", "exec", "--privileged", container, "tc"] + list(args)
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 and "RTNETLINK" not in result.stderr:
        print(f"  WARN: {result.stderr.strip()}", file=sys.stderr)


def _add_netem(container: str, **params) -> None:
    """Set up a netem qdisc on IFACE of container."""
    # Delete any existing qdisc first
    _tc(container, "qdisc", "del", "dev", IFACE, "root")
    netem_args = ["qdisc", "add", "dev", IFACE, "root", "netem"]
    for k, v in params.items():
        netem_args += [k, str(v)]
    _tc(container, *netem_args)


def _add_iptables_drop(src_container: str, dst_ip: str) -> None:
    cmd = [
        "docker", "exec", "--privileged", src_container,
        "iptables", "-A", "OUTPUT", "-d", dst_ip, "-j", "DROP"
    ]
    print(f"  $ {' '.join(cmd)}")
    subprocess.run(cmd)


def _del_iptables_drop(src_container: str, dst_ip: str) -> None:
    cmd = [
        "docker", "exec", "--privileged", src_container,
        "iptables", "-D", "OUTPUT", "-d", dst_ip, "-j", "DROP"
    ]
    print(f"  $ {' '.join(cmd)}")
    subprocess.run(cmd, capture_output=True)


@click.group()
def cli():
    """Network chaos injection for the Raft cluster."""


@cli.command()
@click.option("--src", required=True, help="Source node (e.g. node1)")
@click.option("--dst", required=True, help="Destination node (e.g. node2)")
def partition(src, dst):
    """Bidirectional network partition between two nodes."""
    src_ip = NODE_IPS.get(src)
    dst_ip = NODE_IPS.get(dst)
    if not src_ip or not dst_ip:
        click.echo(f"Unknown node: {src} or {dst}", err=True)
        sys.exit(1)

    click.echo(f"Partitioning {src} ↔ {dst}")
    _add_iptables_drop(src, dst_ip)
    _add_iptables_drop(dst, src_ip)
    click.echo("Done. Run 'python scripts/chaos.py heal' to restore.")


@cli.command()
@click.option("--src", required=True)
@click.option("--dst", required=True)
def heal_partition(src, dst):
    """Heal a specific partition."""
    src_ip = NODE_IPS.get(src)
    dst_ip = NODE_IPS.get(dst)
    click.echo(f"Healing partition {src} ↔ {dst}")
    _del_iptables_drop(src, dst_ip)
    _del_iptables_drop(dst, src_ip)


@cli.command()
@click.option("--node", required=True)
@click.option("--ms", default=100, type=int, help="Delay in milliseconds")
@click.option("--jitter", default=10, type=int, help="Jitter in milliseconds")
def delay(node, ms, jitter):
    """Inject network latency on a node."""
    click.echo(f"Adding {ms}ms ±{jitter}ms delay to {node}")
    _add_netem(node, delay=f"{ms}ms", jitter=f"{jitter}ms", distribution="normal")


@cli.command()
@click.option("--node", required=True)
@click.option("--pct", default=10, type=int, help="Packet loss percentage")
def loss(node, pct):
    """Inject packet loss on a node."""
    click.echo(f"Adding {pct}% packet loss to {node}")
    _add_netem(node, loss=f"{pct}%")


@cli.command()
def heal():
    """Remove all tc qdiscs and iptables DROP rules."""
    click.echo("Healing all nodes...")
    for name, ip in NODE_IPS.items():
        _tc(name, "qdisc", "del", "dev", IFACE, "root")
        for other_ip in NODE_IPS.values():
            if other_ip != ip:
                _del_iptables_drop(name, other_ip)
    click.echo("All rules removed.")


@cli.command()
def status():
    """Show current tc and iptables rules on all nodes."""
    for name in NODE_IPS:
        click.echo(f"\n=== {name} ===")
        subprocess.run(["docker", "exec", name, "tc", "qdisc", "show", "dev", IFACE])
        subprocess.run(["docker", "exec", name, "iptables", "-L", "OUTPUT", "-n"])


if __name__ == "__main__":
    cli()
