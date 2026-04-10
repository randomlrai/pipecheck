"""Task scheduling order utilities for DAG execution planning."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pipecheck.dag import DAG, Task


@dataclass
class ScheduleWave:
    """A group of tasks that can run in parallel."""
    index: int
    tasks: List[Task] = field(default_factory=list)

    def __str__(self) -> str:
        ids = ", ".join(t.task_id for t in self.tasks)
        return f"Wave {self.index}: [{ids}]"

    def __len__(self) -> int:
        return len(self.tasks)


@dataclass
class ExecutionSchedule:
    """Ordered list of waves representing a full execution plan."""
    dag_name: str
    waves: List[ScheduleWave] = field(default_factory=list)

    @property
    def total_waves(self) -> int:
        return len(self.waves)

    @property
    def total_tasks(self) -> int:
        return sum(len(w) for w in self.waves)

    def __str__(self) -> str:
        lines = [f"Schedule for '{self.dag_name}' ({self.total_waves} waves):"]
        for wave in self.waves:
            lines.append(f"  {wave}")
        return "\n".join(lines)


class DAGScheduler:
    """Computes a topological execution schedule (Kahn's algorithm)."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def build_schedule(self) -> ExecutionSchedule:
        """Return an ExecutionSchedule with parallel waves."""
        in_degree: Dict[str, int] = {t.task_id: 0 for t in self._dag.tasks}
        dependents: Dict[str, List[str]] = {t.task_id: [] for t in self._dag.tasks}

        for src, dst in self._dag.edges:
            in_degree[dst] += 1
            dependents[src].append(dst)

        schedule = ExecutionSchedule(dag_name=self._dag.name)
        ready = [tid for tid, deg in in_degree.items() if deg == 0]
        task_map = {t.task_id: t for t in self._dag.tasks}
        wave_index = 0

        while ready:
            wave = ScheduleWave(index=wave_index, tasks=[task_map[tid] for tid in sorted(ready)])
            schedule.waves.append(wave)
            next_ready: List[str] = []
            for tid in ready:
                for dep in dependents[tid]:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0:
                        next_ready.append(dep)
            ready = next_ready
            wave_index += 1

        return schedule

    def critical_path(self) -> List[str]:
        """Return task IDs along the longest dependency chain."""
        task_map = {t.task_id: t for t in self._dag.tasks}
        timeout_map = {t.task_id: (t.timeout or 0) for t in self._dag.tasks}
        dependents: Dict[str, List[str]] = {t.task_id: [] for t in self._dag.tasks}
        in_degree: Dict[str, int] = {t.task_id: 0 for t in self._dag.tasks}

        for src, dst in self._dag.edges:
            dependents[src].append(dst)
            in_degree[dst] += 1

        dist: Dict[str, float] = {tid: 0.0 for tid in task_map}
        prev: Dict[str, Optional[str]] = {tid: None for tid in task_map}
        order = []
        queue = [tid for tid, d in in_degree.items() if d == 0]

        while queue:
            node = queue.pop(0)
            order.append(node)
            for nxt in dependents[node]:
                candidate = dist[node] + timeout_map[node]
                if candidate > dist[nxt]:
                    dist[nxt] = candidate
                    prev[nxt] = node
                in_degree[nxt] -= 1
                if in_degree[nxt] == 0:
                    queue.append(nxt)

        end = max(dist, key=lambda k: dist[k])
        path: List[str] = []
        cur: Optional[str] = end
        while cur is not None:
            path.append(cur)
            cur = prev[cur]
        return list(reversed(path))
