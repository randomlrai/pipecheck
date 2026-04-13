"""Pattern-based task matching for DAGs."""
from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.dag import DAG, Task


class MatchError(Exception):
    """Raised when a match operation fails."""


@dataclass
class MatchEntry:
    task: Task
    pattern: str
    match_type: str  # 'glob', 'regex', or 'exact'

    def __str__(self) -> str:
        return f"{self.task.task_id} [{self.match_type}] <- {self.pattern!r}"


@dataclass
class MatchResult:
    dag_name: str
    pattern: str
    entries: List[MatchEntry] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.entries)

    @property
    def has_matches(self) -> bool:
        return bool(self.entries)

    @property
    def task_ids(self) -> List[str]:
        return [e.task.task_id for e in self.entries]

    def __str__(self) -> str:
        lines = [f"MatchResult(dag={self.dag_name!r}, pattern={self.pattern!r})"]
        if not self.entries:
            lines.append("  (no matches)")
        else:
            for entry in self.entries:
                lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGMatcher:
    """Match tasks in a DAG by glob, regex, or exact patterns."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def match(self, pattern: str, mode: str = "glob") -> MatchResult:
        if mode not in ("glob", "regex", "exact"):
            raise MatchError(f"Unknown match mode: {mode!r}")

        result = MatchResult(dag_name=self._dag.name, pattern=pattern)
        for task in self._dag.tasks.values():
            if self._matches(task.task_id, pattern, mode):
                result.entries.append(MatchEntry(task=task, pattern=pattern, match_type=mode))
        result.entries.sort(key=lambda e: e.task.task_id)
        return result

    def _matches(self, task_id: str, pattern: str, mode: str) -> bool:
        if mode == "exact":
            return task_id == pattern
        if mode == "glob":
            return fnmatch.fnmatch(task_id, pattern)
        if mode == "regex":
            try:
                return bool(re.search(pattern, task_id))
            except re.error as exc:
                raise MatchError(f"Invalid regex pattern {pattern!r}: {exc}") from exc
        return False
