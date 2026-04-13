"""DAG task cataloger — builds a searchable catalog of all tasks across a DAG."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG, Task


class CatalogError(Exception):
    """Raised when cataloging fails."""


@dataclass
class CatalogEntry:
    task_id: str
    description: Optional[str]
    tags: List[str]
    upstream: List[str]
    downstream: List[str]

    def __str__(self) -> str:
        parts = [f"[{self.task_id}]"]
        if self.description:
            parts.append(f"  desc      : {self.description}")
        parts.append(f"  tags      : {', '.join(self.tags) if self.tags else '(none)'}")
        parts.append(f"  upstream  : {', '.join(self.upstream) if self.upstream else '(none)'}")
        parts.append(f"  downstream: {', '.join(self.downstream) if self.downstream else '(none)'}")
        return "\n".join(parts)


@dataclass
class CatalogResult:
    dag_name: str
    entries: Dict[str, CatalogEntry] = field(default_factory=dict)

    def get(self, task_id: str) -> Optional[CatalogEntry]:
        return self.entries.get(task_id)

    @property
    def count(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        lines = [f"Catalog for DAG '{self.dag_name}' ({self.count} tasks)"]
        for entry in sorted(self.entries.values(), key=lambda e: e.task_id):
            lines.append(str(entry))
        return "\n".join(lines)


class DAGCataloger:
    """Builds a CatalogResult from a DAG."""

    def catalog(self, dag: DAG) -> CatalogResult:
        if dag is None:
            raise CatalogError("DAG must not be None")

        result = CatalogResult(dag_name=dag.name)

        adjacency: Dict[str, List[str]] = {t: [] for t in dag.tasks}
        for src, dst in dag.edges:
            adjacency[src].append(dst)

        reverse: Dict[str, List[str]] = {t: [] for t in dag.tasks}
        for src, dst in dag.edges:
            reverse[dst].append(src)

        for task_id, task in dag.tasks.items():
            entry = CatalogEntry(
                task_id=task_id,
                description=task.metadata.get("description") if task.metadata else None,
                tags=sorted(task.metadata.get("tags", [])) if task.metadata else [],
                upstream=sorted(reverse[task_id]),
                downstream=sorted(adjacency[task_id]),
            )
            result.entries[task_id] = entry

        return result
