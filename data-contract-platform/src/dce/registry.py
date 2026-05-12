"""In-memory contract registry with version resolution."""

from __future__ import annotations

from pathlib import Path

from .contract import DataContract, load_contracts_dir


class ContractRegistry:
    """
    Load contracts from a directory and expose lookup / diff helpers.

    Layout expected:
        <contracts_dir>/
            <contract-id>/
                v1.0.0.yaml
                v1.1.0.yaml
                v2.0.0.yaml
    """

    def __init__(self, contracts_dir: Path | str):
        self.contracts_dir = Path(contracts_dir)
        self._registry: dict[str, list[DataContract]] = load_contracts_dir(contracts_dir)

    def reload(self) -> None:
        self._registry = load_contracts_dir(self.contracts_dir)

    # ---------------------------------------------------------------- #

    def ids(self) -> list[str]:
        return sorted(self._registry.keys())

    def versions(self, contract_id: str) -> list[str]:
        return [c.version for c in self._registry.get(contract_id, [])]

    def get(self, contract_id: str, version: str | None = None) -> DataContract:
        """Return a contract by id and optional version (latest if omitted)."""
        versions = self._registry.get(contract_id)
        if not versions:
            raise KeyError(f"Contract '{contract_id}' not found")
        if version is None:
            return versions[-1]
        for c in versions:
            if c.version == version:
                return c
        raise KeyError(f"Contract '{contract_id}' version '{version}' not found")

    def latest(self, contract_id: str) -> DataContract:
        return self.get(contract_id)

    def previous(self, contract_id: str, version: str) -> DataContract | None:
        versions = self._registry.get(contract_id, [])
        for i, c in enumerate(versions):
            if c.version == version and i > 0:
                return versions[i - 1]
        return None

    def all_latest(self) -> list[DataContract]:
        return [versions[-1] for versions in self._registry.values()]

    def by_producer(self, producer: str) -> list[DataContract]:
        return [
            versions[-1]
            for versions in self._registry.values()
            if versions and versions[-1].producer == producer
        ]
