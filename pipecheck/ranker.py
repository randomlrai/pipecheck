"""Rank tasks in a DAG by a given metric (degree, depth, or weight)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.dag import DAG


class RankError(Exception):
    """Raised when ranking cannot be performed."""


@dataclass
class RankEntry:
    task_id: str
    rank: int
    score: float
    metric: str

    def __str__(self) -> str:
        return f"#{self.rank:>3}  {self.task_id}  ({self.metric}={self.score:.2f})"


@dataclass
class RankResult:
    dag_name: str
    metric: str
    entries: List[RankEntry] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.entries)

    def top(self, n: int = 5) -> List[RankEntry]:
        return self.entries[:n]

    def __str__(self) -> str:
        lines = [f"Rank ({self.metric}) for DAG '{self.dag_name}':",
                 f"  Total tasks: {self.count}"]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGRanker:
    """Rank tasks within a DAG by degree, depth, or timeout."""

    SUPPORTED_METRICS = ("degree", "depth", "timeout")

    def rank(self, dag: DAG, metric: str = "degree") -> RankResult:
        if metric not in self.SUPPORTED_METRICS:
            raise RankError(
                f"Unsupported metric '{metric}'. "
                f"Choose from: {', '.join(self.SUPPORTED_METRICS)}"
            )
        scores = self._compute_scores(dag, metric)
        sorted_tasks = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        entries = [
            RankEntry(task_id=tid, rank=i + 1, score=score, metric=metric)
            for i, (tid, score) in enumerate(sorted_tasks)
        ]
        return RankResult(dag_name=dag.name, metric=metric, entries=entries)

    def _compute_scores(self, dag: DAG, metric: str):
        if metric == "degree":
            return {
                t.task_id: float(
                    len(dag.dependencies.get(t.task_id, set()))
                    + sum(1 for deps in dag.dependencies.values() if t.task_id in deps)
                )
                for t in dag.tasks.values()
            }
        if metric == "depth":
            return {t.task_id: float(self._depth(dag, t.task_id)) for t in dag.tasks.values()}
        # timeout
        return {
            t.task_id: float(t.timeout or 0)
            for t in dag.tasks.values()
        }

    def _depth(self, dag: DAG, task_id: str, memo: Optional[dict] = None) -> int:
        if memo is None:
            memo = {}
        if task_id in memo:
            return memo[task_id]
        parents = dag.dependencies.get(task_id, set())
        if not parents:
            memo[task_id] = 0
        else:
            memo[task_id] = 1 + max(self._depth(dag, p, memo) for p in parents)
        return memo[task_id]
