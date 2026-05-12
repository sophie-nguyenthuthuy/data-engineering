"""
Synthetic interbank transaction generator.

Models a realistic small-world interbank network where:
- Tier-1 banks (big) lend to many counterparties
- Tier-2 banks cluster around specific Tier-1 banks
- Occasional large overnight/repo transactions create temporary circular exposures
"""

import asyncio
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import AsyncIterator

import numpy as np

from src.config import settings


INSTITUTION_TIERS = {
    "tier1": {
        "prefix": "BANK",
        "count": 5,
        "balance_range": (50_000, 200_000),  # $M
        "lending_rate_range": (0.02, 0.08),
    },
    "tier2": {
        "prefix": "REG",
        "count": 10,
        "balance_range": (5_000, 50_000),
        "lending_rate_range": (0.005, 0.03),
    },
    "tier3": {
        "prefix": "COMM",
        "count": 5,
        "balance_range": (500, 5_000),
        "lending_rate_range": (0.001, 0.01),
    },
}

TX_TYPES = ["overnight_loan", "repo", "fx_swap", "bond_purchase", "credit_line_draw"]


@dataclass
class Institution:
    id: str
    name: str
    tier: str
    balance: float           # $M
    lending_capacity: float  # $M available to lend
    borrow_rate: float       # willingness to borrow (0-1)


@dataclass
class Transaction:
    tx_id: str
    sender_id: str
    receiver_id: str
    amount: float      # $M
    tx_type: str
    timestamp: float
    metadata: dict = field(default_factory=dict)


class InstitutionRegistry:
    def __init__(self, n: int = settings.num_institutions):
        self.institutions: dict[str, Institution] = {}
        self._build(n)

    def _build(self, n: int) -> None:
        idx = 0
        for tier, spec in INSTITUTION_TIERS.items():
            count = min(spec["count"], n - idx)
            if count <= 0:
                break
            for i in range(count):
                inst_id = f"{spec['prefix']}-{i+1:02d}"
                balance = random.uniform(*spec["balance_range"])
                self.institutions[inst_id] = Institution(
                    id=inst_id,
                    name=f"{spec['prefix']} Institution {i+1}",
                    tier=tier,
                    balance=balance,
                    lending_capacity=balance * random.uniform(0.05, 0.20),
                    borrow_rate=random.uniform(0.3, 0.9),
                )
                idx += 1
                if idx >= n:
                    return

    def all_ids(self) -> list[str]:
        return list(self.institutions.keys())

    def get(self, inst_id: str) -> Institution:
        return self.institutions[inst_id]


class TransactionGenerator:
    """
    Generates a continuous stream of synthetic interbank transactions.

    The network topology skews toward:
    - Hub-and-spoke: Tier-1 banks transact heavily with everyone
    - Cluster: Tier-2 banks mostly transact within their cluster
    - Occasional circular chains introduced deliberately for stress testing
    """

    def __init__(self, registry: InstitutionRegistry):
        self.registry = registry
        self._ids = registry.all_ids()
        self._tier1_ids = [
            i for i, inst in registry.institutions.items() if inst.tier == "tier1"
        ]
        self._seq = 0

    def _pick_sender(self) -> str:
        # Tier-1 are 3x more likely to be senders (major lenders)
        weights = [
            3.0 if self.registry.get(i).tier == "tier1" else 1.0
            for i in self._ids
        ]
        return random.choices(self._ids, weights=weights, k=1)[0]

    def _pick_receiver(self, sender_id: str) -> str:
        candidates = [i for i in self._ids if i != sender_id]
        sender = self.registry.get(sender_id)
        if sender.tier == "tier1":
            # Tier-1 lends broadly
            weights = [1.0] * len(candidates)
        else:
            # Lower tiers prefer borrowing from Tier-1
            weights = [
                2.0 if self.registry.get(c).tier == "tier1" else 0.5
                for c in candidates
            ]
        return random.choices(candidates, weights=weights, k=1)[0]

    def _amount(self, sender_id: str) -> float:
        inst = self.registry.get(sender_id)
        # Amount as fraction of lending capacity, log-normal spread
        base = inst.lending_capacity * random.uniform(0.05, 0.40)
        noise = np.random.lognormal(mean=0, sigma=0.5)
        return round(base * noise, 2)

    def next_transaction(self) -> Transaction:
        self._seq += 1
        sender = self._pick_sender()
        receiver = self._pick_receiver(sender)
        return Transaction(
            tx_id=str(uuid.uuid4()),
            sender_id=sender,
            receiver_id=receiver,
            amount=self._amount(sender),
            tx_type=random.choice(TX_TYPES),
            timestamp=time.time(),
            metadata={"seq": self._seq},
        )

    def inject_circular_chain(self, length: int = 3) -> list[Transaction]:
        """
        Deliberately inject a circular transaction chain for stress testing.
        e.g., A→B→C→A each for the same notional amount.
        """
        nodes = random.sample(self._ids, min(length, len(self._ids)))
        amount = random.uniform(500, 5000)
        chain = []
        for i in range(len(nodes)):
            src = nodes[i]
            dst = nodes[(i + 1) % len(nodes)]
            chain.append(
                Transaction(
                    tx_id=str(uuid.uuid4()),
                    sender_id=src,
                    receiver_id=dst,
                    amount=amount,
                    tx_type="credit_line_draw",
                    timestamp=time.time(),
                    metadata={"seq": self._seq + i, "injected_cycle": True, "cycle_length": length},
                )
            )
        return chain

    async def stream(self, interval_ms: int = settings.transaction_interval_ms) -> AsyncIterator[Transaction]:
        """Yields transactions at a configurable rate; occasionally injects cycles."""
        cycle_countdown = random.randint(30, 80)
        while True:
            tx = self.next_transaction()
            yield tx

            cycle_countdown -= 1
            if cycle_countdown <= 0:
                length = random.choice([3, 4, 5])
                for cycle_tx in self.inject_circular_chain(length):
                    yield cycle_tx
                cycle_countdown = random.randint(30, 80)

            await asyncio.sleep(interval_ms / 1000)
