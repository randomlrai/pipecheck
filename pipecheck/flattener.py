"""DAG flattener: collapses a multi-level DAG into a single-level task list
with inlined dependency metadata."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict

from pipecheck.dag import DAG, Task


class FlattenError(Exception):
    """Raised when flattening cannot be completed."""


@dataclass
class FlatTask:
    """A task entry in a flattened DAG representation."""

    task_id: str
    depth: int
    all_upstream: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)

    def __str__(self) -> str:  # noqa: D105
        upstream = ", ".join(self.all_upstream) if self.all_upstream else "none"
        return f"[depth={self.depth}] {self.task_id} (upstream: {upstream})"


@dataclass
class FlattenResult:
    """Result produced by DAGFlattener."""

    dag_name: str
    tasks: List[FlatTask] = field(default_factory=list)

    def __str__(self) -> str:  # noqa: D105
        lines = [f"Flattened DAG: {self.dag_name} ({len(self.tasks)} tasks)"]
        for ft in self.tasks:
            lines.append(f"  {ft}")
        return "\n".join(lines)

    @property
    def task_ids(self) -> List[str]:
        """Return ordered list of task IDs."""
        return [ft.task_id for ft in self.tasks]


class DAGFlattener:
    """Flattens a DAG into a depth-annotated, single-level task list."""

    def flatten(self, dag: DAG) -> FlattenResult:
        """Return a FlattenResult for *dag*.

        Raises FlattenError if the DAG has no tasks.
        """
        if not dag.tasks:
            raise FlattenError(f"DAG '{dag.name}' has no tasks to flatten.")

        depths = self._compute_depths(dag)
        result = FlattenResult(dag_name=dag.name)
        for task_id in sorted(depths, key=lambda t: (depths[t], t)):
            task: Task = dag.tasks[task_id]
            upstream = self._all_upstream(dag, task_id)
            result.tasks.append(
                FlatTask(
                    task_id=task_id,
                    depth=depths[task_id],
                    all_upstream=sorted(upstream),
                    metadata=dict(task.metadata),
                )
            )
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_depths(self, dag: DAG) -> Dict[str, int]:
        depths: Dict[str, int] = {}
        for task_id in dag.tasks:
            depths[task_id] = self._depth(dag, task_id, set())
        return depths

    def _depth(self, dag: DAG, task_id: str, visiting: set) -> int:
        if task_id in visiting:
            raise FlattenError(f"Cycle detected at task '{task_id}'.")
        deps = dag.dependencies.get(task_id, [])
        if not deps:
            return 0
        visiting = visiting | {task_id}
        return 1 + max(self._depth(dag, dep, visiting) for dep in deps)

    def _all_upstream(self, dag: DAG, task_id: str) -> List[str]:
        visited: set = set()
        stack = list(dag.dependencies.get(task_id, []))
        while stack:
            current = stack.pop()
            if current not in visited:
                visited.add(current)
                stack.extend(dag.dependencies.get(current, []))
        return list(visited)
