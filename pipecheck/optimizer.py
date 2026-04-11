"""DAG optimization: suggest parallelism improvements and redundant edge removal."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from pipecheck.dag import DAG


@dataclass
class OptimizeHint:
    kind: str  # 'redundant_edge' | 'parallelism'
    description: str

    def __str__(self) -> str:
        return f"[{self.kind}] {self.description}"


@dataclass
class OptimizeResult:
    dag_name: str
    hints: List[OptimizeHint] = field(default_factory=list)

    @property
    def has_hints(self) -> bool:
        return len(self.hints) > 0

    @property
    def count(self) -> int:
        return len(self.hints)

    def __str__(self) -> str:
        if not self.hints:
            return f"DAG '{self.dag_name}': no optimizations suggested."
        lines = [f"DAG '{self.dag_name}': {self.count} optimization hint(s)"]
        for h in self.hints:
            lines.append(f"  - {h}")
        return "\n".join(lines)


class DAGOptimizer:
    """Analyse a DAG and produce optimization hints."""

    def optimize(self, dag: DAG) -> OptimizeResult:
        result = OptimizeResult(dag_name=dag.name)
        self._check_redundant_edges(dag, result)
        self._check_parallelism(dag, result)
        return result

    # ------------------------------------------------------------------
    def _check_redundant_edges(self, dag: DAG, result: OptimizeResult) -> None:
        """Flag edges that are implied by a longer path (transitive reduction)."""
        for task in dag.tasks.values():
            for dep_id in list(task.dependencies):
                # Check if dep_id is reachable via another dependency path
                other_deps = [d for d in task.dependencies if d != dep_id]
                if self._reachable_from_any(dep_id, other_deps, dag):
                    result.hints.append(
                        OptimizeHint(
                            kind="redundant_edge",
                            description=(
                                f"Edge '{dep_id}' -> '{task.task_id}' is implied "
                                "by a longer dependency path and can be removed."
                            ),
                        )
                    )

    def _reachable_from_any(self, target: str, starts: List[str], dag: DAG) -> bool:
        visited: set = set()
        stack = list(starts)
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            if node == target:
                return True
            task = dag.tasks.get(node)
            if task:
                stack.extend(task.dependencies)
        return False

    def _check_parallelism(self, dag: DAG, result: OptimizeResult) -> None:
        """Warn when all tasks are sequential (no parallelism opportunity)."""
        roots = [
            t for t in dag.tasks.values() if not t.dependencies
        ]
        if len(dag.tasks) > 1 and len(roots) == 1:
            result.hints.append(
                OptimizeHint(
                    kind="parallelism",
                    description=(
                        "Only one root task found; consider splitting independent "
                        "tasks to increase parallelism."
                    ),
                )
            )
