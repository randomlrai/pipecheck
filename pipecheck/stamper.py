"""DAG stamping: attach version stamps to tasks for release tracking."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipecheck.dag import DAG


class StampError(Exception):
    """Raised when a stamping operation fails."""


@dataclass
class StampEntry:
    task_id: str
    version: str
    stamped_at: str
    author: Optional[str] = None

    def __str__(self) -> str:
        base = f"{self.task_id} @ {self.version} ({self.stamped_at})"
        if self.author:
            base += f" by {self.author}"
        return base

    def to_dict(self) -> dict:
        d = {
            "task_id": self.task_id,
            "version": self.version,
            "stamped_at": self.stamped_at,
        }
        if self.author:
            d["author"] = self.author
        return d


@dataclass
class StampResult:
    dag_name: str
    entries: List[StampEntry] = field(default_factory=list)

    @property
    def has_stamps(self) -> bool:
        return len(self.entries) > 0

    @property
    def count(self) -> int:
        return len(self.entries)

    def get(self, task_id: str) -> Optional[StampEntry]:
        for e in self.entries:
            if e.task_id == task_id:
                return e
        return None

    def __str__(self) -> str:
        if not self.entries:
            return f"StampResult({self.dag_name}): no stamps"
        lines = [f"StampResult({self.dag_name}): {self.count} stamp(s)"]
        for e in self.entries:
            lines.append(f"  {e}")
        return "\n".join(lines)


class DAGStamper:
    """Stamps tasks in a DAG with version strings."""

    def stamp(
        self,
        dag: DAG,
        versions: Dict[str, str],
        author: Optional[str] = None,
    ) -> StampResult:
        task_ids = {t.task_id for t in dag.tasks}
        unknown = set(versions) - task_ids
        if unknown:
            raise StampError(
                f"Unknown task(s) for stamping: {', '.join(sorted(unknown))}"
            )
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        result = StampResult(dag_name=dag.name)
        for task_id, version in sorted(versions.items()):
            result.entries.append(
                StampEntry(
                    task_id=task_id,
                    version=version,
                    stamped_at=now,
                    author=author,
                )
            )
        return result
