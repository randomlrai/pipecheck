"""DAG task weight analysis — assigns and aggregates cost weights across tasks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG, Task


class WeighError(Exception):
    """Raised when weight analysis cannot be completed."""


@dataclass
class TaskWeight:
    task_id: str
    weight: float
    cumulative_weight: float  # sum of weights along the critical path to this task
    is_heaviest: bool = False

    def __str__(self) -> str:
        marker = " [heaviest]" if self.is_heaviest else ""
        return (
            f"{self.task_id}: weight={self.weight:.2f}, "
            f"cumulative={self.cumulative_weight:.2f}{marker}"
        )


@dataclass
class WeighResult:
    dag_name: str
    weights: List[TaskWeight] = field(default_factory=list)
    total_weight: float = 0.0

    @property
    def heaviest_task(self) -> Optional[TaskWeight]:
        return next((w for w in self.weights if w.is_heaviest), None)

    def has_weights(self) -> bool:
        return bool(self.weights)

    def __str__(self) -> str:
        lines = [f"WeighResult(dag={self.dag_name}, total={self.total_weight:.2f})"]
        for w in self.weights:
            lines.append(f"  {w}")
        return "\n".join(lines)


class DAGWeigher:
    """Assigns weights to tasks and computes cumulative path weights."""

    DEFAULT_WEIGHT_KEY = "weight"

    def __init__(self, weight_key: str = DEFAULT_WEIGHT_KEY) -> None:
        self.weight_key = weight_key

    def weigh(self, dag: DAG) -> WeighResult:
        if not dag.tasks:
            return WeighResult(dag_name=dag.name)

        raw: Dict[str, float] = {
            t.task_id: float(t.metadata.get(self.weight_key, 1.0))
            for t in dag.tasks.values()
        }

        # Topological order via Kahn's algorithm
        in_degree: Dict[str, int] = {tid: 0 for tid in dag.tasks}
        for tid in dag.tasks:
            for dep in dag.tasks[tid].dependencies:
                in_degree[tid] = in_degree.get(tid, 0) + 1

        queue = [tid for tid, deg in in_degree.items() if deg == 0]
        cumulative: Dict[str, float] = {tid: raw[tid] for tid in queue}
        order: List[str] = []

        while queue:
            current = queue.pop(0)
            order.append(current)
            for tid, task in dag.tasks.items():
                if current in task.dependencies:
                    candidate = cumulative[current] + raw[tid]
                    cumulative[tid] = max(cumulative.get(tid, 0.0), candidate)
                    in_degree[tid] -= 1
                    if in_degree[tid] == 0:
                        queue.append(tid)

        max_cum = max(cumulative.values(), default=0.0)
        task_weights = [
            TaskWeight(
                task_id=tid,
                weight=raw[tid],
                cumulative_weight=cumulative.get(tid, raw[tid]),
                is_heaviest=(cumulative.get(tid, raw[tid]) == max_cum),
            )
            for tid in order
        ]
        return WeighResult(
            dag_name=dag.name,
            weights=task_weights,
            total_weight=sum(raw.values()),
        )
