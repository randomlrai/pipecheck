"""DAG walker: traverses a DAG in topological order, yielding tasks level by level."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, List

from pipecheck.dag import DAG, Task


class WalkError(Exception):
    """Raised when a walk cannot be completed."""


@dataclass
class WalkStep:
    """A single step in a DAG walk."""

    depth: int
    task: Task

    def __str__(self) -> str:
        indent = "  " * self.depth
        return f"{indent}[{self.depth}] {self.task.task_id}"


@dataclass
class WalkResult:
    """Result of walking an entire DAG."""

    dag_name: str
    steps: List[WalkStep] = field(default_factory=list)

    def task_ids(self) -> List[str]:
        """Return task IDs in walk order."""
        return [s.task.task_id for s in self.steps]

    def __len__(self) -> int:
        return len(self.steps)

    def __str__(self) -> str:
        lines = [f"Walk of '{self.dag_name}' ({len(self.steps)} steps):"]
        for step in self.steps:
            lines.append(str(step))
        return "\n".join(lines)


class DAGWalker:
    """Walks a DAG in topological (BFS) order, tracking depth per task."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def walk(self) -> WalkResult:
        """Return a WalkResult with every task visited in topological order."""
        dag = self._dag
        result = WalkResult(dag_name=dag.name)

        # Build in-degree map and adjacency list
        in_degree: dict[str, int] = {t.task_id: 0 for t in dag.tasks}
        children: dict[str, list[str]] = {t.task_id: [] for t in dag.tasks}

        for task in dag.tasks:
            for dep in task.dependencies:
                if dep not in in_degree:
                    raise WalkError(f"Unknown dependency '{dep}' for task '{task.task_id}'")
                in_degree[task.task_id] += 1
                children[dep].append(task.task_id)

        task_map = {t.task_id: t for t in dag.tasks}
        depth_map: dict[str, int] = {}

        queue: list[str] = [tid for tid, deg in in_degree.items() if deg == 0]
        for tid in queue:
            depth_map[tid] = 0

        visited_count = 0
        while queue:
            current_id = queue.pop(0)
            current_depth = depth_map[current_id]
            result.steps.append(WalkStep(depth=current_depth, task=task_map[current_id]))
            visited_count += 1
            for child_id in children[current_id]:
                in_degree[child_id] -= 1
                depth_map[child_id] = max(depth_map.get(child_id, 0), current_depth + 1)
                if in_degree[child_id] == 0:
                    queue.append(child_id)

        if visited_count != len(dag.tasks):
            raise WalkError("DAG contains a cycle; walk aborted.")

        return result

    def iter_steps(self) -> Iterator[WalkStep]:
        """Yield WalkStep objects one at a time."""
        yield from self.walk().steps
