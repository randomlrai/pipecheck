"""DAG layer stacker: groups tasks into dependency layers (topological levels)."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict

from pipecheck.dag import DAG


class StackError(Exception):
    """Raised when stacking fails."""


@dataclass
class StackLayer:
    index: int
    task_ids: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        tasks = ", ".join(self.task_ids) if self.task_ids else "(empty)"
        return f"Layer {self.index}: [{tasks}]"

    def __len__(self) -> int:
        return len(self.task_ids)


@dataclass
class StackResult:
    dag_name: str
    layers: List[StackLayer] = field(default_factory=list)

    @property
    def depth(self) -> int:
        return len(self.layers)

    @property
    def widest_layer(self) -> StackLayer | None:
        if not self.layers:
            return None
        return max(self.layers, key=lambda l: len(l))

    def __str__(self) -> str:
        lines = [f"Stack for '{self.dag_name}' ({self.depth} layer(s)):"]
        for layer in self.layers:
            lines.append(f"  {layer}")
        return "\n".join(lines)


class DAGStacker:
    """Assigns each task to a topological layer (0-indexed)."""

    def stack(self, dag: DAG) -> StackResult:
        tasks = {t.task_id: t for t in dag.tasks}
        if not tasks:
            return StackResult(dag_name=dag.name)

        in_degree: Dict[str, int] = {tid: 0 for tid in tasks}
        for task in dag.tasks:
            for dep in task.dependencies:
                if dep not in tasks:
                    raise StackError(
                        f"Task '{task.task_id}' depends on unknown task '{dep}'"
                    )
                in_degree[task.task_id] += 1

        layer_map: Dict[str, int] = {}
        queue = [tid for tid, deg in in_degree.items() if deg == 0]
        remaining = dict(in_degree)

        current_layer = 0
        while queue:
            next_queue = []
            for tid in queue:
                layer_map[tid] = current_layer
            # find successors
            for task in dag.tasks:
                if task.task_id in queue:
                    continue
                if all(dep in layer_map for dep in task.dependencies) and \
                        task.task_id not in layer_map:
                    if task.dependencies and \
                            max(layer_map[d] for d in task.dependencies) == current_layer:
                        next_queue.append(task.task_id)
            current_layer += 1
            queue = next_queue

        if len(layer_map) != len(tasks):
            raise StackError("Cycle detected; cannot stack DAG.")

        max_layer = max(layer_map.values(), default=0)
        layers = [StackLayer(index=i) for i in range(max_layer + 1)]
        for tid, idx in layer_map.items():
            layers[idx].task_ids.append(tid)
        for layer in layers:
            layer.task_ids.sort()

        return StackResult(dag_name=dag.name, layers=layers)
