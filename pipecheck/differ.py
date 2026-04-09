"""DAG diff module: compare two DAGs and report structural changes."""
from dataclasses import dataclass, field
from typing import List, Set
from pipecheck.dag import DAG


@dataclass
class DiffEntry:
    kind: str  # 'added_task', 'removed_task', 'added_edge', 'removed_edge'
    detail: str

    def __str__(self) -> str:
        symbol = {"added_task": "+", "removed_task": "-",
                  "added_edge": "~+", "removed_edge": "~-"}.get(self.kind, "?")
        return f"[{symbol}] {self.detail}"


@dataclass
class DAGDiff:
    entries: List[DiffEntry] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return len(self.entries) > 0

    def added_tasks(self) -> List[DiffEntry]:
        return [e for e in self.entries if e.kind == "added_task"]

    def removed_tasks(self) -> List[DiffEntry]:
        return [e for e in self.entries if e.kind == "removed_task"]

    def added_edges(self) -> List[DiffEntry]:
        return [e for e in self.entries if e.kind == "added_edge"]

    def removed_edges(self) -> List[DiffEntry]:
        return [e for e in self.entries if e.kind == "removed_edge"]

    def summary(self) -> str:
        lines = [f"DAG diff: {len(self.entries)} change(s)"]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGDiffer:
    """Compares two DAG instances and produces a DAGDiff."""

    def compare(self, old: DAG, new: DAG) -> DAGDiff:
        diff = DAGDiff()

        old_ids: Set[str] = {t.task_id for t in old.tasks.values()}
        new_ids: Set[str] = {t.task_id for t in new.tasks.values()}

        for tid in sorted(new_ids - old_ids):
            diff.entries.append(DiffEntry("added_task", f"task '{tid}' added"))

        for tid in sorted(old_ids - new_ids):
            diff.entries.append(DiffEntry("removed_task", f"task '{tid}' removed"))

        old_edges = self._edges(old)
        new_edges = self._edges(new)

        for src, dst in sorted(new_edges - old_edges):
            diff.entries.append(DiffEntry("added_edge", f"edge '{src}' -> '{dst}' added"))

        for src, dst in sorted(old_edges - new_edges):
            diff.entries.append(DiffEntry("removed_edge", f"edge '{src}' -> '{dst}' removed"))

        return diff

    @staticmethod
    def _edges(dag: DAG) -> Set[tuple]:
        edges = set()
        for task in dag.tasks.values():
            for dep in task.dependencies:
                edges.add((dep, task.task_id))
        return edges
