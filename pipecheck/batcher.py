"""Batch task grouping for parallel execution planning."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict
from pipecheck.dag import DAG


class BatchError(Exception):
    """Raised when batching fails."""


@dataclass
class Batch:
    """A group of tasks that can run concurrently."""

    index: int
    task_ids: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        tasks = ", ".join(self.task_ids) if self.task_ids else "(empty)"
        return f"Batch {self.index}: [{tasks}]"

    def __len__(self) -> int:
        return len(self.task_ids)


@dataclass
class BatchResult:
    """Result of batching a DAG."""

    dag_name: str
    batches: List[Batch] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.batches)

    @property
    def total_tasks(self) -> int:
        return sum(len(b) for b in self.batches)

    def __str__(self) -> str:
        lines = [f"BatchResult for '{self.dag_name}' ({self.count} batches)"]
        for batch in self.batches:
            lines.append(f"  {batch}")
        return "\n".join(lines)


class DAGBatcher:
    """Groups DAG tasks into parallel execution batches."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def batch(self) -> BatchResult:
        """Return tasks grouped into waves of parallel-safe batches."""
        dag = self._dag
        if not dag.tasks:
            return BatchResult(dag_name=dag.name)

        in_degree: Dict[str, int] = {t.task_id: 0 for t in dag.tasks.values()}
        for task in dag.tasks.values():
            for dep in task.dependencies:
                if dep not in in_degree:
                    raise BatchError(f"Unknown dependency '{dep}' on task '{task.task_id}'")
                in_degree[task.task_id] += 1

        result = BatchResult(dag_name=dag.name)
        completed = set()
        batch_index = 0

        while len(completed) < len(dag.tasks):
            ready = [
                tid for tid, deg in in_degree.items()
                if deg == 0 and tid not in completed
            ]
            if not ready:
                raise BatchError("Cycle detected; cannot produce batches.")
            ready.sort()
            result.batches.append(Batch(index=batch_index, task_ids=ready))
            completed.update(ready)
            batch_index += 1
            for task in dag.tasks.values():
                if task.task_id not in completed:
                    resolved = sum(1 for d in task.dependencies if d in completed)
                    in_degree[task.task_id] = len(task.dependencies) - resolved

        return result
