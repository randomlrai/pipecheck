"""DAG streak detection: find the longest consecutive chain of tasks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.dag import DAG


class StreakError(Exception):
    """Raised when streak detection fails."""


@dataclass
class Streak:
    """A consecutive linear chain of tasks (no branching)."""

    task_ids: List[str] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.task_ids)

    def __str__(self) -> str:
        if not self.task_ids:
            return "Streak(empty)"
        return " -> ".join(self.task_ids)


@dataclass
class StreakResult:
    """Result of streak analysis on a DAG."""

    dag_name: str
    streaks: List[Streak] = field(default_factory=list)

    @property
    def longest(self) -> Streak:
        if not self.streaks:
            return Streak()
        return max(self.streaks, key=len)

    @property
    def count(self) -> int:
        return len(self.streaks)

    @property
    def total_tasks(self) -> int:
        """Return the total number of tasks covered across all streaks."""
        return sum(len(s) for s in self.streaks)

    def __str__(self) -> str:
        longest = self.longest
        return (
            f"StreakResult(dag={self.dag_name!r}, "
            f"streaks={self.count}, longest={len(longest)})"
        )


class DAGStreaker:
    """Finds all maximal linear streaks in a DAG."""

    def find(self, dag: DAG) -> StreakResult:
        """Return all maximal linear chains (no branching in or out)."""
        if not dag.tasks:
            return StreakResult(dag_name=dag.name)

        # Build adjacency structures
        children: dict[str, list[str]] = {t: [] for t in dag.tasks}
        parents: dict[str, list[str]] = {t: [] for t in dag.tasks}
        for src, dst in dag.edges:
            children[src].append(dst)
            parents[dst].append(src)

        visited: set[str] = set()
        streaks: list[Streak] = []

        # A streak starts at a node that is NOT a "pass-through"
        # (i.e. not exactly 1 parent with 1 child on the parent side)
        for task_id in dag.tasks:
            if task_id in visited:
                continue
            # Only start a streak from a node that isn't mid-chain
            if len(parents[task_id]) == 1 and len(children[parents[task_id][0]]) == 1:
                continue

            # Walk forward while chain is linear
            chain = [task_id]
            visited.add(task_id)
            current = task_id
            while len(children[current]) == 1:
                nxt = children[current][0]
                if len(parents[nxt]) != 1 or nxt in visited:
                    break
                chain.append(nxt)
                visited.add(nxt)
                current = nxt

            streaks.append(Streak(task_ids=chain))

        return StreakResult(dag_name=dag.name, streaks=streaks)
