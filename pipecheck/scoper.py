"""Scope analysis for DAG tasks — identify tasks within a named execution scope."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG, Task


class ScopeError(Exception):
    """Raised when a scoping operation fails."""


@dataclass
class ScopeResult:
    dag_name: str
    scope_name: str
    tasks: List[Task] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.tasks)

    @property
    def has_tasks(self) -> bool:
        return bool(self.tasks)

    def __str__(self) -> str:
        if not self.tasks:
            return f"Scope '{self.scope_name}' in DAG '{self.dag_name}': no tasks matched."
        ids = ", ".join(t.task_id for t in sorted(self.tasks, key=lambda t: t.task_id))
        return (
            f"Scope '{self.scope_name}' in DAG '{self.dag_name}': "
            f"{self.count} task(s) — {ids}"
        )


class DAGScoper:
    """Resolves tasks that belong to a named scope.

    A scope is defined by a string label stored in ``task.metadata['scope']``.
    """

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def scope(self, scope_name: str) -> ScopeResult:
        """Return all tasks whose metadata 'scope' field matches *scope_name*."""
        if not scope_name or not scope_name.strip():
            raise ScopeError("scope_name must be a non-empty string.")
        matched = [
            task
            for task in self._dag.tasks.values()
            if task.metadata.get("scope") == scope_name
        ]
        return ScopeResult(
            dag_name=self._dag.name,
            scope_name=scope_name,
            tasks=matched,
        )

    def all_scopes(self) -> List[str]:
        """Return a sorted list of every distinct scope label present in the DAG."""
        scopes = set()
        for task in self._dag.tasks.values():
            s = task.metadata.get("scope")
            if s:
                scopes.add(s)
        return sorted(scopes)
