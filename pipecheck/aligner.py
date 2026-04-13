"""DAG task alignment — groups tasks by their topological depth level."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipecheck.dag import DAG


class AlignError(Exception):
    """Raised when alignment cannot be computed."""


@dataclass
class AlignLevel:
    """A single depth level containing tasks at the same topological distance from roots."""

    depth: int
    task_ids: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        tasks = ", ".join(self.task_ids) if self.task_ids else "(empty)"
        return f"Level {self.depth}: [{tasks}]"

    def __len__(self) -> int:
        return len(self.task_ids)


@dataclass
class AlignResult:
    """Result of aligning a DAG into depth levels."""

    dag_name: str
    levels: List[AlignLevel] = field(default_factory=list)

    @property
    def depth(self) -> int:
        """Total number of levels."""
        return len(self.levels)

    @property
    def max_width(self) -> int:
        """Maximum number of tasks in any single level."""
        return max((len(lvl) for lvl in self.levels), default=0)

    def __str__(self) -> str:
        header = f"Alignment for '{self.dag_name}' ({self.depth} levels, max width {self.max_width})"
        rows = [str(lvl) for lvl in self.levels]
        return "\n".join([header] + rows)


class DAGAligner:
    """Computes topological depth levels for all tasks in a DAG."""

    def align(self, dag: DAG) -> AlignResult:
        """Return an AlignResult grouping tasks by their topological depth."""
        tasks = list(dag.tasks.values())
        if not tasks:
            return AlignResult(dag_name=dag.name)

        # Build in-degree map and adjacency list
        in_degree: Dict[str, int] = {t.task_id: 0 for t in tasks}
        children: Dict[str, List[str]] = {t.task_id: [] for t in tasks}

        for task in tasks:
            for dep in task.dependencies:
                if dep not in in_degree:
                    raise AlignError(
                        f"Task '{task.task_id}' depends on unknown task '{dep}'"
                    )
                in_degree[task.task_id] += 1
                children[dep].append(task.task_id)

        levels: List[AlignLevel] = []
        current_frontier = [tid for tid, deg in in_degree.items() if deg == 0]

        if not current_frontier:
            raise AlignError("No root tasks found — DAG may contain a cycle.")

        depth = 0
        remaining = dict(in_degree)

        while current_frontier:
            level = AlignLevel(depth=depth, task_ids=sorted(current_frontier))
            levels.append(level)
            next_frontier: List[str] = []
            for tid in current_frontier:
                for child in children[tid]:
                    remaining[child] -= 1
                    if remaining[child] == 0:
                        next_frontier.append(child)
            current_frontier = next_frontier
            depth += 1

        return AlignResult(dag_name=dag.name, levels=levels)
