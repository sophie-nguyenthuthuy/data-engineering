"""
Core Raft consensus algorithm implementation.

Implements:
  - Leader election (§5.2)
  - Log replication (§5.3)
  - Safety (§5.4)
  - Log compaction / snapshotting (§7)
  - Joint-consensus cluster membership changes (§6)
"""

import asyncio
import logging
import random
import time
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set

from .log import LogEntry, RaftLog, Snapshot
from .rpc import RaftRPC

logger = logging.getLogger(__name__)

ELECTION_TIMEOUT_MIN = 0.150  # 150 ms
ELECTION_TIMEOUT_MAX = 0.300  # 300 ms
HEARTBEAT_INTERVAL = 0.050   # 50 ms
SNAPSHOT_THRESHOLD = 1000    # take snapshot every N committed entries


class RaftState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    """
    Single Raft node.

    The caller must supply:
      - node_id: unique string identifier (used as peer address key)
      - peers: {node_id: "host:port"} mapping for all OTHER nodes
      - state_machine_apply: async callable(command) → result
      - state_machine_snapshot: async callable() → dict
      - state_machine_restore: async callable(snapshot_data) → None
      - data_dir: directory for persistent storage
    """

    def __init__(
        self,
        node_id: str,
        peers: Dict[str, str],
        state_machine_apply: Callable[[Dict], Coroutine],
        state_machine_snapshot: Callable[[], Coroutine],
        state_machine_restore: Callable[[Dict], Coroutine],
        data_dir: str = "/tmp/raft",
    ):
        self.node_id = node_id
        self.peers = dict(peers)  # mutable — membership changes update it
        self._apply_fn = state_machine_apply
        self._snapshot_fn = state_machine_snapshot
        self._restore_fn = state_machine_restore

        self.log = RaftLog(data_dir, node_id)
        self.rpc = RaftRPC(node_id)

        # Volatile state
        self.state = RaftState.FOLLOWER
        self.leader_id: Optional[str] = None
        self.commit_index: int = 0
        self.last_applied: int = 0

        # Leader volatile state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Election timer
        self._election_deadline: float = self._new_election_deadline()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._apply_task: Optional[asyncio.Task] = None
        self._timer_task: Optional[asyncio.Task] = None

        # Pending client futures: log_index → Future
        self._pending: Dict[int, asyncio.Future] = {}

        # Config change in progress (joint consensus)
        self._config_changing: bool = False

        # Restore snapshot if present
        if self.log.snapshot:
            # Restore happens in start() after event loop is running
            pass

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start background tasks."""
        if self.log.snapshot:
            await self._restore_fn(self.log.snapshot.data)
            self.commit_index = self.log.snapshot.last_included_index
            self.last_applied = self.log.snapshot.last_included_index

        self._timer_task = asyncio.create_task(self._election_timer_loop())
        self._apply_task = asyncio.create_task(self._apply_loop())
        logger.info("[%s] started as %s", self.node_id, self.state.value)

    async def stop(self) -> None:
        for task in [self._timer_task, self._heartbeat_task, self._apply_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await self.rpc.close()

    # ── Client API ────────────────────────────────────────────────────────

    async def submit(self, command: Dict[str, Any]) -> Any:
        """
        Submit a command to the cluster.
        Only succeeds on the leader; raises RuntimeError otherwise.
        Blocks until the entry is committed and applied.
        """
        if self.state != RaftState.LEADER:
            raise RuntimeError(
                f"not leader; current leader={self.leader_id}"
            )

        entry = LogEntry(
            term=self.log.current_term,
            index=self.log.last_index + 1,
            command=command,
        )
        await self.log.append(entry)

        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[entry.index] = fut

        # Immediately try to replicate
        asyncio.create_task(self._replicate_once())

        try:
            result = await asyncio.wait_for(fut, timeout=5.0)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(entry.index, None)
            raise RuntimeError("command timed out waiting for commit")

    # ── RPC handlers (called by HTTP server) ──────────────────────────────

    async def handle_request_vote(self, msg: Dict) -> Dict:
        term = msg["term"]
        candidate_id = msg["candidate_id"]
        last_log_index = msg["last_log_index"]
        last_log_term = msg["last_log_term"]

        await self._maybe_update_term(term)

        vote_granted = False
        if term < self.log.current_term:
            pass  # stale term
        elif (
            self.log.voted_for is None or self.log.voted_for == candidate_id
        ) and self._candidate_log_up_to_date(last_log_index, last_log_term):
            await self.log.save_term(self.log.current_term, candidate_id)
            vote_granted = True
            self._reset_election_timer()

        logger.debug(
            "[%s] RequestVote from %s term=%d granted=%s",
            self.node_id, candidate_id, term, vote_granted,
        )
        return {"term": self.log.current_term, "vote_granted": vote_granted}

    async def handle_append_entries(self, msg: Dict) -> Dict:
        term = msg["term"]
        leader_id = msg["leader_id"]
        prev_log_index = msg["prev_log_index"]
        prev_log_term = msg["prev_log_term"]
        entries_raw = msg["entries"]
        leader_commit = msg["leader_commit"]

        await self._maybe_update_term(term)

        if term < self.log.current_term:
            return {"term": self.log.current_term, "success": False}

        # Valid AppendEntries → reset election timer, record leader
        self.state = RaftState.FOLLOWER
        self.leader_id = leader_id
        self._reset_election_timer()

        entries = [LogEntry.from_dict(e) for e in entries_raw]
        ok = await self.log.append_entries(prev_log_index, prev_log_term, entries)

        if ok and leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.log.last_index)

        return {"term": self.log.current_term, "success": ok}

    async def handle_install_snapshot(self, msg: Dict) -> Dict:
        term = msg["term"]
        leader_id = msg["leader_id"]

        await self._maybe_update_term(term)

        if term < self.log.current_term:
            return {"term": self.log.current_term}

        self.state = RaftState.FOLLOWER
        self.leader_id = leader_id
        self._reset_election_timer()

        snap = Snapshot(
            last_included_index=msg["last_included_index"],
            last_included_term=msg["last_included_term"],
            data=msg["data"],
            cluster_config=msg["cluster_config"],
        )
        await self.log.install_snapshot(snap)
        await self._restore_fn(snap.data)

        self.commit_index = max(self.commit_index, snap.last_included_index)
        self.last_applied = max(self.last_applied, snap.last_included_index)

        # Update peers from snapshot config
        if snap.cluster_config:
            await self._apply_cluster_config(snap.cluster_config)

        return {"term": self.log.current_term}

    # ── Cluster membership change (joint consensus) ────────────────────────

    async def add_peer(self, new_peer_id: str, new_peer_addr: str) -> None:
        """
        Add a new peer using joint consensus.
        Submits a config-change entry; once committed, the new config is active.
        """
        if self.state != RaftState.LEADER:
            raise RuntimeError("not leader")
        if self._config_changing:
            raise RuntimeError("config change already in progress")

        new_config = list(self.peers.keys()) + [new_peer_id, self.node_id]
        new_config = sorted(set(new_config))

        self._config_changing = True
        try:
            # Phase 1: joint consensus (C_old,new)
            joint_cmd = {
                "type": "_config_joint",
                "new_peer_id": new_peer_id,
                "new_peer_addr": new_peer_addr,
                "new_config": new_config,
            }
            entry = LogEntry(
                term=self.log.current_term,
                index=self.log.last_index + 1,
                command=joint_cmd,
                entry_type="config",
            )
            self.peers[new_peer_id] = new_peer_addr
            self._init_peer_indices()
            await self.log.append(entry)

            fut: asyncio.Future = asyncio.get_event_loop().create_future()
            self._pending[entry.index] = fut
            asyncio.create_task(self._replicate_once())
            await asyncio.wait_for(fut, timeout=10.0)

            # Phase 2: new config only (C_new)
            final_cmd = {
                "type": "_config_final",
                "new_config": new_config,
            }
            await self.submit(final_cmd)
        finally:
            self._config_changing = False

    async def remove_peer(self, peer_id: str) -> None:
        if self.state != RaftState.LEADER:
            raise RuntimeError("not leader")

        new_config = [p for p in list(self.peers.keys()) + [self.node_id] if p != peer_id]
        new_config = sorted(set(new_config))

        cmd = {"type": "_config_remove", "remove_id": peer_id, "new_config": new_config}
        await self.submit(cmd)

    # ── Internal: elections ───────────────────────────────────────────────

    async def _election_timer_loop(self) -> None:
        while True:
            await asyncio.sleep(0.010)  # check every 10ms
            if self.state != RaftState.LEADER:
                if time.monotonic() >= self._election_deadline:
                    await self._start_election()

    def _new_election_deadline(self) -> float:
        return time.monotonic() + random.uniform(
            ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX
        )

    def _reset_election_timer(self) -> None:
        self._election_deadline = self._new_election_deadline()

    async def _start_election(self) -> None:
        new_term = self.log.current_term + 1
        await self.log.save_term(new_term, self.node_id)
        self.state = RaftState.CANDIDATE
        self.leader_id = None
        self._reset_election_timer()

        logger.info("[%s] starting election for term %d", self.node_id, new_term)

        votes: Set[str] = {self.node_id}
        all_nodes = list(self.peers.keys()) + [self.node_id]
        quorum = len(all_nodes) // 2 + 1

        async def request_vote_from(peer_id: str) -> None:
            addr = self.peers[peer_id]
            resp = await self.rpc.request_vote(
                addr,
                new_term,
                self.node_id,
                self.log.last_index,
                self.log.last_term,
            )
            if resp is None:
                return
            if resp["term"] > self.log.current_term:
                await self._maybe_update_term(resp["term"])
                return
            if resp.get("vote_granted") and self.state == RaftState.CANDIDATE:
                votes.add(peer_id)
                if len(votes) >= quorum and self.state == RaftState.CANDIDATE:
                    await self._become_leader()

        await asyncio.gather(
            *[request_vote_from(p) for p in self.peers], return_exceptions=True
        )

    async def _become_leader(self) -> None:
        if self.state != RaftState.CANDIDATE:
            return
        self.state = RaftState.LEADER
        self.leader_id = self.node_id
        logger.info("[%s] became leader for term %d", self.node_id, self.log.current_term)

        self._init_peer_indices()

        # Append a no-op to commit previous entries (§8 leader completeness)
        noop = LogEntry(
            term=self.log.current_term,
            index=self.log.last_index + 1,
            command={"type": "_noop"},
            entry_type="noop",
        )
        await self.log.append(noop)

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def _init_peer_indices(self) -> None:
        for p in self.peers:
            self.next_index[p] = self.log.last_index + 1
            self.match_index[p] = 0

    # ── Internal: replication ─────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self.state == RaftState.LEADER:
            await self._replicate_once()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _replicate_once(self) -> None:
        if self.state != RaftState.LEADER:
            return
        tasks = [self._replicate_to(p) for p in list(self.peers.keys())]
        await asyncio.gather(*tasks, return_exceptions=True)
        self._maybe_advance_commit()

    async def _replicate_to(self, peer_id: str) -> None:
        addr = self.peers.get(peer_id)
        if not addr:
            return

        # Send snapshot if peer is behind the snapshot point
        if self.log.snapshot and self.next_index.get(peer_id, 1) <= self.log.snapshot.last_included_index:
            await self._send_snapshot(peer_id, addr)
            return

        next_idx = self.next_index.get(peer_id, self.log.last_index + 1)
        prev_index = next_idx - 1
        prev_term = self.log.get_term(prev_index)
        entries = self.log.get_entries_from(next_idx)

        resp = await self.rpc.append_entries(
            addr,
            self.log.current_term,
            self.node_id,
            prev_index,
            prev_term,
            [e.to_dict() for e in entries],
            self.commit_index,
        )
        if resp is None:
            return
        if resp["term"] > self.log.current_term:
            await self._maybe_update_term(resp["term"])
            return
        if resp["success"]:
            if entries:
                self.match_index[peer_id] = entries[-1].index
                self.next_index[peer_id] = entries[-1].index + 1
        else:
            # Decrement and retry (simple back-off)
            self.next_index[peer_id] = max(1, next_idx - 1)

    async def _send_snapshot(self, peer_id: str, addr: str) -> None:
        snap = self.log.snapshot
        if snap is None:
            return
        resp = await self.rpc.install_snapshot(
            addr,
            self.log.current_term,
            self.node_id,
            snap.last_included_index,
            snap.last_included_term,
            snap.data,
            snap.cluster_config,
        )
        if resp and resp["term"] <= self.log.current_term:
            self.match_index[peer_id] = snap.last_included_index
            self.next_index[peer_id] = snap.last_included_index + 1

    def _maybe_advance_commit(self) -> None:
        """
        Advance commit_index to the highest N such that:
          - N > commitIndex
          - log[N].term == currentTerm
          - a majority of matchIndex[i] >= N
        """
        if self.state != RaftState.LEADER:
            return
        all_nodes = list(self.peers.keys()) + [self.node_id]
        quorum = len(all_nodes) // 2 + 1

        for n in range(self.log.last_index, self.commit_index, -1):
            if self.log.get_term(n) != self.log.current_term:
                continue
            count = 1  # self
            for p in self.peers:
                if self.match_index.get(p, 0) >= n:
                    count += 1
            if count >= quorum:
                self.commit_index = n
                break

    # ── Internal: apply loop ──────────────────────────────────────────────

    async def _apply_loop(self) -> None:
        while True:
            if self.last_applied < self.commit_index:
                for idx in range(self.last_applied + 1, self.commit_index + 1):
                    entry = self.log.get_entry(idx)
                    if entry is None:
                        # Covered by snapshot
                        self.last_applied = idx
                        continue
                    result = await self._apply_entry(entry)
                    self.last_applied = idx

                    # Resolve pending client futures
                    fut = self._pending.pop(idx, None)
                    if fut and not fut.done():
                        fut.set_result(result)

                # Maybe take snapshot
                if self.last_applied - (
                    self.log.snapshot.last_included_index if self.log.snapshot else 0
                ) >= SNAPSHOT_THRESHOLD:
                    await self._take_snapshot()

            await asyncio.sleep(0.005)

    async def _apply_entry(self, entry: LogEntry) -> Any:
        if entry.entry_type == "noop":
            return None
        if entry.entry_type == "config":
            await self._apply_config_entry(entry.command)
            return None
        return await self._apply_fn(entry.command)

    async def _apply_config_entry(self, cmd: Dict) -> None:
        t = cmd.get("type")
        if t == "_config_joint":
            self.peers[cmd["new_peer_id"]] = cmd["new_peer_addr"]
        elif t in ("_config_final", "_config_remove"):
            await self._apply_cluster_config(cmd["new_config"])

    async def _apply_cluster_config(self, config: List[str]) -> None:
        to_remove = [p for p in self.peers if p not in config]
        for p in to_remove:
            self.peers.pop(p, None)
            self.next_index.pop(p, None)
            self.match_index.pop(p, None)

    async def _take_snapshot(self) -> None:
        state = await self._snapshot_fn()
        config = list(self.peers.keys()) + [self.node_id]
        await self.log.take_snapshot(
            self.last_applied,
            self.log.get_term(self.last_applied),
            state,
            config,
        )
        logger.info("[%s] snapshot taken at index %d", self.node_id, self.last_applied)

    # ── Internal: term management ─────────────────────────────────────────

    async def _maybe_update_term(self, term: int) -> None:
        if term > self.log.current_term:
            await self.log.save_term(term, None)
            self.state = RaftState.FOLLOWER
            self.leader_id = None
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                self._heartbeat_task = None
            self._reset_election_timer()

    def _candidate_log_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        """§5.4.1: candidate log must be at least as up-to-date as ours."""
        if last_log_term != self.log.last_term:
            return last_log_term > self.log.last_term
        return last_log_index >= self.log.last_index

    # ── Status ────────────────────────────────────────────────────────────

    def status(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.log.current_term,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "last_log_index": self.log.last_index,
            "last_log_term": self.log.last_term,
            "peers": list(self.peers.keys()),
        }
