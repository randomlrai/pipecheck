"""DAG cutter: remove a subgraph by cutting at a given task boundary."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Set

from pipecheck.dag import DAG, Task


class CutError(Exception):
    """Raised when a cut operation cannot be completed."""


@dataclass
class CutResult:
    """Result of a DAG cut operation."""

    dag_name: str
    cut_task_id: str
    upstream: List[Task] = field(default_factory=list)
    downstream: List[Task] = field(default_factory=list)

    @property
    def has_upstream(self) -> bool:
        return len(self.upstream) > 0

    @property
    def has_downstream(self) -> bool:
        return len(self.downstream) > 0

    def __str__(self) -> str:  # noqa: D401
        lines = [f"Cut at '{self.cut_task_id}' in DAG '{self.dag_name}'"]
        lines.append(f"  Upstream tasks  : {len(self.upstream)}")
        lines.append(f"  Downstream tasks: {len(self.downstream)}")
        return "\n".join(lines)


class DAGCutter:
    """Splits a DAG into upstream and downstream halves at a task boundary."""

    def cut(self, dag: DAG, task_id: str) -> CutResult:
        """Return tasks strictly upstream and strictly downstream of *task_id*."""
        task_ids: Set[str] = {t.task_id for t in dag.tasks}
        if task_id not in task_ids:
            raise CutError(f"Task '{task_id}' not found in DAG '{dag.name}'")

        upstream = self._ancestors(dag, task_id)
        downstream = self._descendants(dag, task_id)

        upstream_tasks = [t for t in dag.tasks if t.task_id in upstream]
        downstream_tasks = [t for t in dag.tasks if t.task_id in downstream]

        return CutResult(
            dag_name=dag.name,
            cut_task_id=task_id,
            upstream=upstream_tasks,
            downstream=downstream_tasks,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ancestors(self, dag: DAG, task_id: str) -> Set[str]:
        visited: Set[str] = set()
        queue = list(dag.dependencies.get(task_id, []))
        while queue:
            node = queue.pop()
            if node in visited:
                continue
            visited.add(node)
            queue.extend(dag.dependencies.get(node, []))
        return visited

    def _descendants(self, dag: DAG, task_id: str) -> Set[str]:
        # Build reverse adjacency (child -> parents) then walk forward
        children: dict[str, List[str]] = {t.task_id: [] for t in dag.tasks}
        for child, parents in dag.dependencies.items():
            for parent in parents:
                children[parent].append(child)

        visited: Set[str] = set()
        queue = list(children.get(task_id, []))
        while queue:
            node = queue.pop()
            if node in visited:
                continue
            visited.add(node)
            queue.extend(children.get(node, []))
        return visited
