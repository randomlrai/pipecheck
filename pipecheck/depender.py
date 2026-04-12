"""Dependency analysis for DAG tasks."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Set
from pipecheck.dag import DAG


class DependError(Exception):
    """Raised when dependency analysis fails."""


@dataclass
class DependResult:
    """Result of a dependency lookup for a single task."""

    task_id: str
    dag_name: str
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)
    all_ancestors: List[str] = field(default_factory=list)
    all_descendants: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [f"Task: {self.task_id} (DAG: {self.dag_name})"]
        lines.append(f"  Direct upstream   : {', '.join(self.upstream) or 'none'}")
        lines.append(f"  Direct downstream : {', '.join(self.downstream) or 'none'}")
        lines.append(f"  All ancestors     : {', '.join(self.all_ancestors) or 'none'}")
        lines.append(f"  All descendants   : {', '.join(self.all_descendants) or 'none'}")
        return "\n".join(lines)


class DAGDepender:
    """Analyses upstream/downstream dependencies of a task in a DAG."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def _ancestors(self, task_id: str, visited: Set[str] | None = None) -> Set[str]:
        if visited is None:
            visited = set()
        for parent in self._dag.dependencies.get(task_id, []):
            if parent not in visited:
                visited.add(parent)
                self._ancestors(parent, visited)
        return visited

    def _descendants(self, task_id: str, visited: Set[str] | None = None) -> Set[str]:
        if visited is None:
            visited = set()
        for child in self._dag.dependents.get(task_id, []):
            if child not in visited:
                visited.add(child)
                self._descendants(child, visited)
        return visited

    def analyze(self, task_id: str) -> DependResult:
        if task_id not in {t.task_id for t in self._dag.tasks}:
            raise DependError(f"Task '{task_id}' not found in DAG '{self._dag.name}'")
        upstream = sorted(self._dag.dependencies.get(task_id, []))
        downstream = sorted(self._dag.dependents.get(task_id, []))
        ancestors = sorted(self._ancestors(task_id))
        descendants = sorted(self._descendants(task_id))
        return DependResult(
            task_id=task_id,
            dag_name=self._dag.name,
            upstream=upstream,
            downstream=downstream,
            all_ancestors=ancestors,
            all_descendants=descendants,
        )
