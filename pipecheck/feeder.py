"""Feed external data sources into a DAG as task metadata."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipecheck.dag import DAG


class FeedError(Exception):
    pass


@dataclass
class FeedEntry:
    task_id: str
    source: str
    fields: Dict[str, str] = field(default_factory=dict)

    def __str__(self) -> str:
        parts = [f"{self.task_id} <- {self.source}"]
        for k, v in self.fields.items():
            parts.append(f"  {k}: {v}")
        return "\n".join(parts)


@dataclass
class FeedResult:
    dag_name: str
    entries: List[FeedEntry] = field(default_factory=list)

    def has_feeds(self) -> bool:
        return len(self.entries) > 0

    def count(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        if not self.entries:
            return f"FeedResult({self.dag_name}): no feeds applied"
        lines = [f"FeedResult({self.dag_name}): {self.count()} feed(s)"]
        for e in self.entries:
            lines.append(str(e))
        return "\n".join(lines)


class DAGFeeder:
    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def feed(self, source: str, data: Dict[str, Dict[str, str]]) -> FeedResult:
        """Apply external data to matching tasks. data maps task_id -> fields."""
        result = FeedResult(dag_name=self._dag.name)
        task_ids = {t.task_id for t in self._dag.tasks}
        for task_id, fields in data.items():
            if task_id not in task_ids:
                raise FeedError(f"Task '{task_id}' not found in DAG '{self._dag.name}'")
            entry = FeedEntry(task_id=task_id, source=source, fields=fields)
            result.entries.append(entry)
        return result
