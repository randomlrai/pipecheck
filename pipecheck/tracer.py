"""Execution path tracer for DAG task chains."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG, Task


@dataclass
class TraceStep:
    """A single step in an execution trace."""
    task: Task
    depth: int

    def __str__(self) -> str:
        indent = "  " * self.depth
        return f"{indent}-> {self.task.task_id}"


@dataclass
class ExecutionTrace:
    """Represents a traced path through the DAG."""
    start_id: str
    end_id: Optional[str]
    steps: List[TraceStep] = field(default_factory=list)

    @property
    def task_ids(self) -> List[str]:
        return [s.task.task_id for s in self.steps]

    def __str__(self) -> str:
        lines = [f"Trace from '{self.start_id}':"]
        lines.extend(str(step) for step in self.steps)
        return "\n".join(lines)


class DAGTracer:
    """Traces execution paths through a DAG."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def _build_adjacency(self) -> dict:
        adj: dict = {t: [] for t in self._dag.tasks}
        for parent, child in self._dag.edges:
            adj[parent].append(child)
        return adj

    def trace_from(self, start_id: str, end_id: Optional[str] = None) -> ExecutionTrace:
        """Trace all tasks reachable from start_id, optionally stopping at end_id."""
        task_map = {t.task_id: t for t in self._dag.tasks}
        if start_id not in task_map:
            raise KeyError(f"Task '{start_id}' not found in DAG.")
        if end_id is not None and end_id not in task_map:
            raise KeyError(f"Task '{end_id}' not found in DAG.")

        adj = self._build_adjacency()
        trace = ExecutionTrace(start_id=start_id, end_id=end_id)
        visited = set()

        def dfs(task_id: str, depth: int) -> None:
            if task_id in visited:
                return
            visited.add(task_id)
            task = task_map[task_id]
            trace.steps.append(TraceStep(task=task, depth=depth))
            if end_id and task_id == end_id:
                return
            for neighbour in adj.get(task, []):
                dfs(neighbour.task_id, depth + 1)

        dfs(start_id, 0)
        return trace

    def critical_path(self) -> List[str]:
        """Return the longest dependency chain (by task count) in the DAG."""
        task_map = {t.task_id: t for t in self._dag.tasks}
        adj = self._build_adjacency()
        memo: dict = {}

        def longest(task: Task) -> List[str]:
            if task in memo:
                return memo[task]
            children = adj.get(task, [])
            if not children:
                memo[task] = [task.task_id]
                return memo[task]
            best = max((longest(c) for c in children), key=len)
            memo[task] = [task.task_id] + best
            return memo[task]

        if not task_map:
            return []
        return max((longest(t) for t in self._dag.tasks), key=len)
