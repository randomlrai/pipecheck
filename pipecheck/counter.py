"""DAG element counter — counts tasks, edges, roots, leaves, and isolated nodes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipecheck.dag import DAG


class CountError(Exception):
    """Raised when counting fails."""


@dataclass
class CountResult:
    dag_name: str
    task_count: int = 0
    edge_count: int = 0
    root_count: int = 0
    leaf_count: int = 0
    isolated_count: int = 0
    tag_count: int = 0

    def __str__(self) -> str:
        lines = [
            f"DAG: {self.dag_name}",
            f"  tasks     : {self.task_count}",
            f"  edges     : {self.edge_count}",
            f"  roots     : {self.root_count}",
            f"  leaves    : {self.leaf_count}",
            f"  isolated  : {self.isolated_count}",
            f"  unique tags: {self.tag_count}",
        ]
        return "\n".join(lines)


class DAGCounter:
    """Counts structural elements of a DAG."""

    def count(self, dag: "DAG") -> CountResult:
        if dag is None:
            raise CountError("DAG must not be None")

        tasks = list(dag.tasks.values())
        task_ids = {t.task_id for t in tasks}

        # Build adjacency from dependencies
        all_deps: dict[str, set[str]] = {t.task_id: set(t.dependencies) for t in tasks}
        dependents: dict[str, set[str]] = {tid: set() for tid in task_ids}
        for tid, deps in all_deps.items():
            for dep in deps:
                if dep in dependents:
                    dependents[dep].add(tid)

        edge_count = sum(len(deps) for deps in all_deps.values())
        roots = [t for t in tasks if not all_deps[t.task_id]]
        leaves = [t for t in tasks if not dependents[t.task_id]]
        isolated = [t for t in tasks if not all_deps[t.task_id] and not dependents[t.task_id]]

        all_tags: set[str] = set()
        for t in tasks:
            all_tags.update(getattr(t, "tags", []) or [])

        return CountResult(
            dag_name=dag.name,
            task_count=len(tasks),
            edge_count=edge_count,
            root_count=len(roots),
            leaf_count=len(leaves),
            isolated_count=len(isolated),
            tag_count=len(all_tags),
        )
