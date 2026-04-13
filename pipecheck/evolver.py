"""DAG evolution tracker: compares a DAG against a baseline snapshot to describe structural changes over time."""
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG


class EvolveError(Exception):
    """Raised when evolution analysis fails."""


@dataclass
class EvolveEntry:
    kind: str  # 'added_task', 'removed_task', 'added_edge', 'removed_edge'
    subject: str
    detail: Optional[str] = None

    def __str__(self) -> str:
        if self.detail:
            return f"[{self.kind}] {self.subject} -> {self.detail}"
        return f"[{self.kind}] {self.subject}"


@dataclass
class EvolveResult:
    baseline_name: str
    current_name: str
    entries: List[EvolveEntry] = field(default_factory=list)

    def has_changes(self) -> bool:
        return len(self.entries) > 0

    def count(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        if not self.entries:
            return f"No changes detected between '{self.baseline_name}' and '{self.current_name}'."
        lines = [f"Evolution: '{self.baseline_name}' -> '{self.current_name}' ({self.count()} change(s))"]
        for e in self.entries:
            lines.append(f"  {e}")
        return "\n".join(lines)


class DAGEvolver:
    """Compares two DAG instances and produces an EvolveResult describing structural differences."""

    def evolve(self, baseline: DAG, current: DAG) -> EvolveResult:
        if not isinstance(baseline, DAG) or not isinstance(current, DAG):
            raise EvolveError("Both baseline and current must be DAG instances.")

        result = EvolveResult(
            baseline_name=baseline.name,
            current_name=current.name,
        )

        baseline_ids = {t.task_id for t in baseline.tasks.values()}
        current_ids = {t.task_id for t in current.tasks.values()}

        for tid in sorted(current_ids - baseline_ids):
            result.entries.append(EvolveEntry(kind="added_task", subject=tid))

        for tid in sorted(baseline_ids - current_ids):
            result.entries.append(EvolveEntry(kind="removed_task", subject=tid))

        baseline_edges = set(baseline.edges)
        current_edges = set(current.edges)

        for src, dst in sorted(current_edges - baseline_edges):
            result.entries.append(EvolveEntry(kind="added_edge", subject=src, detail=dst))

        for src, dst in sorted(baseline_edges - current_edges):
            result.entries.append(EvolveEntry(kind="removed_edge", subject=src, detail=dst))

        return result
