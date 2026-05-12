"""
Raft metadata server entry point.

Usage:
  python server.py --id node1 --port 8001 --peers node2=localhost:8002,node3=localhost:8003
"""

import asyncio
import json
import logging
import os
import sys
from typing import Dict

import click
from aiohttp import web

from catalog.api import CatalogAPI
from catalog.models import Column, DataLineage, Dataset, Table
from raft.node import RaftNode
from store.kv_store import KVStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def build_app(node: RaftNode, kv: KVStore, catalog: CatalogAPI) -> web.Application:
    app = web.Application()

    # ── Raft RPC endpoints ─────────────────────────────────────────────

    async def handle_request_vote(request):
        msg = await request.json()
        result = await node.handle_request_vote(msg)
        return web.json_response(result)

    async def handle_append_entries(request):
        msg = await request.json()
        result = await node.handle_append_entries(msg)
        return web.json_response(result)

    async def handle_install_snapshot(request):
        msg = await request.json()
        result = await node.handle_install_snapshot(msg)
        return web.json_response(result)

    async def handle_status(request):
        return web.json_response(node.status())

    app.router.add_post("/raft/request_vote", handle_request_vote)
    app.router.add_post("/raft/append_entries", handle_append_entries)
    app.router.add_post("/raft/install_snapshot", handle_install_snapshot)
    app.router.add_get("/raft/status", handle_status)

    # ── KV endpoints ───────────────────────────────────────────────────

    def redirect_to_leader():
        leader = node.leader_id
        leader_addr = node.peers.get(leader) if leader else None
        body = {"error": "not leader", "leader_id": leader, "leader_addr": leader_addr}
        return web.Response(
            status=307,
            content_type="application/json",
            text=json.dumps(body),
        )

    async def kv_put(request):
        if node.state.value != "leader":
            return redirect_to_leader()
        body = await request.json()
        result = await node.submit(
            {"op": "put", "key": body["key"], "value": body["value"],
             "version": body.get("version")}
        )
        return web.json_response(result)

    async def kv_get(request):
        key = request.match_info["key"]
        vv = await kv.get(key)
        if vv is None:
            return web.json_response({"ok": False, "error": "not found"}, status=404)
        return web.json_response({"ok": True, "key": key, **vv.to_dict()})

    async def kv_cas(request):
        if node.state.value != "leader":
            return redirect_to_leader()
        body = await request.json()
        result = await node.submit(
            {"op": "cas", "key": body["key"], "expected": body["expected"],
             "new_value": body["new_value"]}
        )
        return web.json_response(result)

    async def kv_delete(request):
        if node.state.value != "leader":
            return redirect_to_leader()
        body = await request.json()
        result = await node.submit({"op": "delete", "key": body["key"]})
        return web.json_response(result)

    async def kv_list(request):
        prefix = request.rel_url.query.get("prefix", "")
        pairs = await kv.list_prefix(prefix)
        return web.json_response(
            {"keys": [{"key": k, **v.to_dict()} for k, v in pairs]}
        )

    app.router.add_post("/kv/put", kv_put)
    app.router.add_get("/kv/get/{key}", kv_get)
    app.router.add_post("/kv/cas", kv_cas)
    app.router.add_post("/kv/delete", kv_delete)
    app.router.add_get("/kv/list", kv_list)

    # ── Catalog endpoints ──────────────────────────────────────────────

    async def create_dataset(request):
        if node.state.value != "leader":
            return redirect_to_leader()
        body = await request.json()
        ds = Dataset.from_dict(body)
        result = await catalog.create_dataset(ds)
        return web.json_response(result.to_dict(), status=201)

    async def list_datasets(request):
        datasets = await catalog.list_datasets()
        return web.json_response({"datasets": [d.to_dict() for d in datasets]})

    async def get_dataset(request):
        name = request.match_info["name"]
        ds = await catalog.get_dataset(name)
        if ds is None:
            return web.json_response({"error": "not found"}, status=404)
        return web.json_response(ds.to_dict())

    async def delete_dataset(request):
        if node.state.value != "leader":
            return redirect_to_leader()
        name = request.match_info["name"]
        await catalog.delete_dataset(name)
        return web.json_response({"ok": True})

    async def create_table(request):
        if node.state.value != "leader":
            return redirect_to_leader()
        dataset = request.match_info["dataset"]
        body = await request.json()
        body.setdefault("dataset_name", dataset)
        tbl = Table.from_dict(body)
        result = await catalog.create_table(tbl)
        return web.json_response(result.to_dict(), status=201)

    async def list_tables(request):
        dataset = request.match_info["dataset"]
        tables = await catalog.list_tables(dataset)
        return web.json_response({"tables": [t.to_dict() for t in tables]})

    async def get_table(request):
        dataset = request.match_info["dataset"]
        table = request.match_info["table"]
        tbl = await catalog.get_table(dataset, table)
        if tbl is None:
            return web.json_response({"error": "not found"}, status=404)
        return web.json_response(tbl.to_dict())

    async def add_column(request):
        if node.state.value != "leader":
            return redirect_to_leader()
        dataset = request.match_info["dataset"]
        table = request.match_info["table"]
        body = await request.json()
        col = Column.from_dict(body)
        result = await catalog.add_column(dataset, table, col)
        return web.json_response(result.to_dict())

    async def add_lineage(request):
        if node.state.value != "leader":
            return redirect_to_leader()
        body = await request.json()
        lineage = DataLineage.from_dict(body)
        result = await catalog.add_lineage(lineage)
        return web.json_response(result.to_dict(), status=201)

    async def lineage_upstream(request):
        table = request.match_info["table"]
        edges = await catalog.get_lineage_upstream(table)
        return web.json_response({"lineage": [e.to_dict() for e in edges]})

    async def lineage_downstream(request):
        table = request.match_info["table"]
        edges = await catalog.get_lineage_downstream(table)
        return web.json_response({"lineage": [e.to_dict() for e in edges]})

    async def lineage_impact(request):
        table = request.match_info["table"]
        depth = int(request.rel_url.query.get("depth", "5"))
        impacted = await catalog.get_lineage_impact(table, depth)
        return web.json_response({"impacted": impacted})

    async def search_by_tag(request):
        tag = request.match_info["tag"]
        results = await catalog.find_by_tag(tag)
        return web.json_response({"results": results})

    async def membership_add(request):
        if node.state.value != "leader":
            return redirect_to_leader()
        body = await request.json()
        await node.add_peer(body["node_id"], body["addr"])
        return web.json_response({"ok": True})

    async def membership_remove(request):
        if node.state.value != "leader":
            return redirect_to_leader()
        body = await request.json()
        await node.remove_peer(body["node_id"])
        return web.json_response({"ok": True})

    app.router.add_post("/catalog/datasets", create_dataset)
    app.router.add_get("/catalog/datasets", list_datasets)
    app.router.add_get("/catalog/datasets/{name}", get_dataset)
    app.router.add_delete("/catalog/datasets/{name}", delete_dataset)
    app.router.add_post("/catalog/datasets/{dataset}/tables", create_table)
    app.router.add_get("/catalog/datasets/{dataset}/tables", list_tables)
    app.router.add_get("/catalog/datasets/{dataset}/tables/{table}", get_table)
    app.router.add_post(
        "/catalog/datasets/{dataset}/tables/{table}/columns", add_column
    )
    app.router.add_post("/catalog/lineage", add_lineage)
    app.router.add_get("/catalog/lineage/upstream/{table}", lineage_upstream)
    app.router.add_get("/catalog/lineage/downstream/{table}", lineage_downstream)
    app.router.add_get("/catalog/lineage/impact/{table}", lineage_impact)
    app.router.add_get("/catalog/search/tag/{tag}", search_by_tag)
    app.router.add_post("/cluster/members/add", membership_add)
    app.router.add_post("/cluster/members/remove", membership_remove)

    return app


@click.command()
@click.option("--id", "node_id", required=True, help="Unique node ID")
@click.option("--port", default=8001, type=int, help="HTTP port to listen on")
@click.option(
    "--peers",
    default="",
    help="Comma-separated peer=host:port pairs, e.g. node2=localhost:8002",
)
@click.option("--data-dir", default="/tmp/raft", help="Persistent storage directory")
def main(node_id: str, port: int, peers: str, data_dir: str):
    peer_map: Dict[str, str] = {}
    if peers:
        for item in peers.split(","):
            pid, addr = item.strip().split("=")
            peer_map[pid.strip()] = addr.strip()

    kv = KVStore()
    node = RaftNode(
        node_id=node_id,
        peers=peer_map,
        state_machine_apply=kv.apply,
        state_machine_snapshot=kv.snapshot,
        state_machine_restore=kv.restore,
        data_dir=os.path.join(data_dir, node_id),
    )
    catalog = CatalogAPI(node, kv)
    app = build_app(node, kv, catalog)

    async def startup(app):
        await node.start()
        logger.info("Node %s listening on :%d", node_id, port)

    async def shutdown(app):
        await node.stop()

    app.on_startup.append(startup)
    app.on_shutdown.append(shutdown)

    web.run_app(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
