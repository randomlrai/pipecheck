"""DAG padding: insert no-op placeholder tasks to fill structural gaps."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List

from pipecheck.dag import DAG, Task


class PadError(Exception):
    """Raised when padding cannot be applied."""


@dataclass
class PadEntry:
    task_id: str
    inserted_between: tuple  # (upstream_id, downstream_id)

    def __str__(self) -> str:
        up, down = self.inserted_between
        return f"[pad] {self.task_id} inserted between {up!r} and {down!r}"


@dataclass
class PadResult:
    dag_name: str
    entries: List[PadEntry] = field(default_factory=list)
    padded_dag: DAG = field(default=None)

    def has_pads(self) -> bool:
        return len(self.entries) > 0

    def count(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        if not self.entries:
            return f"PadResult({self.dag_name}): no pads inserted"
        lines = [f"PadResult({self.dag_name}): {self.count()} pad(s) inserted"]
        for e in self.entries:
            lines.append(f"  {e}")
        return "\n".join(lines)


class DAGPadder:
    """Insert no-op placeholder tasks between pairs of tasks that span
    more than *max_depth* levels in topological order."""

    def __init__(self, dag: DAG, max_depth: int = 1) -> None:
        if max_depth < 1:
            raise PadError("max_depth must be >= 1")
        self._dag = dag
        self._max_depth = max_depth

    # ------------------------------------------------------------------
    def _topo_levels(self) -> dict:
        """Return {task_id: level} via BFS from roots."""
        levels = {}
        roots = [
            t.task_id
            for t in self._dag.tasks.values()
            if not self._dag.get_dependencies(t.task_id)
        ]
        queue = list(roots)
        for r in roots:
            levels[r] = 0
        while queue:
            current = queue.pop(0)
            for dep in self._dag.get_dependents(current):
                new_level = levels[current] + 1
                if dep not in levels or levels[dep] < new_level:
                    levels[dep] = new_level
                    queue.append(dep)
        return levels

    def pad(self) -> PadResult:
        result = PadResult(dag_name=self._dag.name)
        levels = self._topo_levels()
        new_dag = DAG(name=self._dag.name)
        for task in self._dag.tasks.values():
            new_dag.add_task(task)

        pad_counter = 0
        edges_to_add = []
        edges_to_remove = []

        for task in list(self._dag.tasks.values()):
            for dep_id in self._dag.get_dependencies(task.task_id):
                gap = levels.get(task.task_id, 0) - levels.get(dep_id, 0)
                if gap > self._max_depth:
                    pad_counter += 1
                    pad_id = f"_                    pad_task = Task(task_id=pad_id, description="[no-op placeholder]")
                    new_dag.add_task(pad_task)
                    edges_to_remove.append((task.task_id, dep_id))
                    edges_to_add.append((pad_id, dep_id))
                    edges_to_add.append((task.task_id, pad_id))
                    result.entries.append(
                        PadEntry(task_id=pad_id, inserted_between=(dep_id, task.task_id))
                    )
                else:
                    edges_to_add.append((task.task_id, dep_id))

        for downstream, upstream in edges_to_add:
            try:
                new_dag.add_edge(upstream, downstream)
            except Exception:
                pass

        result.padded_dag = new_dag
        return result
