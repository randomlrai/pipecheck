"""Isolate a subgraph of a DAG reachable from a given set of root tasks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Set

from pipecheck.dag import DAG, Task


class IsolateError(Exception):
    """Raised when isolation cannot be performed."""


@dataclass
class IsolateResult:
    """Result of a DAG isolation operation."""

    original_name: str
    roots: List[str]
    subgraph: DAG
    included_task_ids: List[str] = field(default_factory=list)

    def __str__(self) -> str:  # pragma: no cover
        task_list = ", ".join(sorted(self.included_task_ids))
        return (
            f"IsolateResult(dag={self.original_name!r}, "
            f"roots={self.roots}, tasks=[{task_list}])"
        )

    @property
    def count(self) -> int:
        return len(self.included_task_ids)


class DAGIsolator:
    """Extracts the subgraph reachable from the given root task IDs."""

    def isolate(self, dag: DAG, root_ids: List[str]) -> IsolateResult:
        """Return a new DAG containing only tasks reachable from *root_ids*.

        Reachability is defined as the root tasks themselves plus all
        downstream tasks (following dependency edges forward).
        """
        missing = [tid for tid in root_ids if tid not in dag.tasks]
        if missing:
            raise IsolateError(
                f"Root task(s) not found in DAG: {missing}"
            )

        reachable: Set[str] = set()
        stack = list(root_ids)
        while stack:
            tid = stack.pop()
            if tid in reachable:
                continue
            reachable.add(tid)
            for dep_id in dag.tasks[tid].dependencies:
                stack.append(dep_id)

        sub = DAG(name=f"{dag.name}_isolated")
        for tid in reachable:
            original: Task = dag.tasks[tid]
            kept_deps = [
                d for d in original.dependencies if d in reachable
            ]
            sub.add_task(
                Task(
                    task_id=original.task_id,
                    name=original.name,
                    dependencies=kept_deps,
                    timeout=original.timeout,
                    metadata=dict(original.metadata),
                )
            )

        return IsolateResult(
            original_name=dag.name,
            roots=list(root_ids),
            subgraph=sub,
            included_task_ids=sorted(reachable),
        )
