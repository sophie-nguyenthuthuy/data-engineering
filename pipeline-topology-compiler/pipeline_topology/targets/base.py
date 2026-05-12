from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from ..dsl.ir import PipelineSpec
from ..compiler.selector import Target


@dataclass
class CompiledArtifact:
    """Output of a code generator, with metadata for the equivalence checker."""

    target: Target
    spec_name: str
    files: dict[str, str] = field(default_factory=dict)
    # Structured record of what was compiled — used by the equivalence checker
    compiled_nodes: list[dict] = field(default_factory=list)

    def write_to(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        for rel_path, content in self.files.items():
            dest = output_dir / rel_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(content)

    def summary(self) -> str:
        lines = [f"Target: {self.target.value}", f"Files: {list(self.files.keys())}"]
        return "\n".join(lines)


class BaseTarget(ABC):
    """Abstract code generator backend."""

    @abstractmethod
    def generate(self, spec: PipelineSpec) -> CompiledArtifact:
        ...
