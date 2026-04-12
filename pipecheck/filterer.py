"""Filter DAG tasks by various criteria."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG, Task


class FilterError(Exception):
    """Raised when a filter operation fails."""


@dataclass
class FilterResult:
    dag_name: str
    matched: List[Task] = field(default_factory=list)
    excluded: List[Task] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.matched)

    @property
    def has_matches(self) -> bool:
        return bool(self.matched)

    def __str__(self) -> str:
        lines = [f"Filter results for DAG '{self.dag_name}':",
                 f"  Matched : {len(self.matched)}",
                 f"  Excluded: {len(self.excluded)}"]
        if self.matched:
            lines.append("  Tasks:")
            for t in self.matched:
                lines.append(f"    - {t.task_id}")
        return "\n".join(lines)


class DAGFilterer:
    """Filter tasks in a DAG by tag, prefix, or timeout threshold."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def by_tag(self, tag: str) -> FilterResult:
        matched = [t for t in self._dag.tasks.values()
                   if tag in t.metadata.get("tags", [])]
        excluded = [t for t in self._dag.tasks.values() if t not in matched]
        return FilterResult(dag_name=self._dag.name,
                            matched=matched, excluded=excluded)

    def by_prefix(self, prefix: str) -> FilterResult:
        matched = [t for t in self._dag.tasks.values()
                   if t.task_id.startswith(prefix)]
        excluded = [t for t in self._dag.tasks.values() if t not in matched]
        return FilterResult(dag_name=self._dag.name,
                            matched=matched, excluded=excluded)

    def by_max_timeout(self, max_timeout: int) -> FilterResult:
        matched = [t for t in self._dag.tasks.values()
                   if t.timeout is not None and t.timeout <= max_timeout]
        excluded = [t for t in self._dag.tasks.values() if t not in matched]
        return FilterResult(dag_name=self._dag.name,
                            matched=matched, excluded=excluded)

    def by_no_timeout(self) -> FilterResult:
        matched = [t for t in self._dag.tasks.values() if t.timeout is None]
        excluded = [t for t in self._dag.tasks.values() if t not in matched]
        return FilterResult(dag_name=self._dag.name,
                            matched=matched, excluded=excluded)
