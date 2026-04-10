"""DAG dependency linker — resolves and validates inter-DAG task references."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG


class LinkError(Exception):
    """Raised when a cross-DAG link cannot be resolved."""


@dataclass
class LinkEntry:
    source_dag: str
    source_task: str
    target_dag: str
    target_task: str

    def __str__(self) -> str:
        return (
            f"{self.source_dag}.{self.source_task} "
            f"-> {self.target_dag}.{self.target_task}"
        )


@dataclass
class LinkResult:
    resolved: List[LinkEntry] = field(default_factory=list)
    unresolved: List[LinkEntry] = field(default_factory=list)

    @property
    def has_unresolved(self) -> bool:
        return len(self.unresolved) > 0

    def __str__(self) -> str:
        lines = [f"Resolved: {len(self.resolved)}, Unresolved: {len(self.unresolved)}"]
        for entry in self.unresolved:
            lines.append(f"  MISSING {entry}")
        return "\n".join(lines)


class DAGLinker:
    """Resolves cross-DAG task references given a registry of DAGs."""

    def __init__(self, registry: Optional[Dict[str, DAG]] = None) -> None:
        self._registry: Dict[str, DAG] = registry or {}

    def register(self, dag: DAG) -> None:
        """Add a DAG to the registry under its name."""
        self._registry[dag.name] = dag

    def link(self, links: List[LinkEntry]) -> LinkResult:
        """Attempt to resolve each link entry against the registry."""
        result = LinkResult()
        for entry in links:
            dag = self._registry.get(entry.target_dag)
            if dag is None:
                result.unresolved.append(entry)
                continue
            task_ids = {t.task_id for t in dag.tasks}
            if entry.target_task not in task_ids:
                result.unresolved.append(entry)
            else:
                result.resolved.append(entry)
        return result
