"""DAG comparator: produces a structured comparison report between two DAGs."""

from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG


@dataclass
class CompareEntry:
    """A single comparison finding between two DAGs."""

    category: str  # 'task', 'edge', 'metadata'
    key: str
    left_value: Optional[str]
    right_value: Optional[str]

    def __str__(self) -> str:
        lv = self.left_value or "<none>"
        rv = self.right_value or "<none>"
        return f"[{self.category}] {self.key}: {lv!r} -> {rv!r}"


@dataclass
class CompareResult:
    """Full comparison result between a left and right DAG."""

    left_name: str
    right_name: str
    entries: List[CompareEntry] = field(default_factory=list)

    @property
    def has_differences(self) -> bool:
        return len(self.entries) > 0

    def __str__(self) -> str:
        header = f"Compare: {self.left_name!r} vs {self.right_name!r}"
        if not self.entries:
            return f"{header}\n  No differences found."
        lines = [header]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


class CompareError(Exception):
    """Raised when comparison cannot be performed."""


class DAGComparator:
    """Compares two DAG objects and returns a CompareResult."""

    def compare(self, left: DAG, right: DAG) -> CompareResult:
        if not isinstance(left, DAG) or not isinstance(right, DAG):
            raise CompareError("Both arguments must be DAG instances.")

        result = CompareResult(left_name=left.name, right_name=right.name)

        left_ids = {t.task_id for t in left.tasks}
        right_ids = {t.task_id for t in right.tasks}

        for tid in sorted(left_ids - right_ids):
            result.entries.append(
                CompareEntry("task", tid, left_value=tid, right_value=None)
            )
        for tid in sorted(right_ids - left_ids):
            result.entries.append(
                CompareEntry("task", tid, left_value=None, right_value=tid)
            )

        common_ids = left_ids & right_ids
        left_map = {t.task_id: t for t in left.tasks}
        right_map = {t.task_id: t for t in right.tasks}
        for tid in sorted(common_ids):
            lt = left_map[tid]
            rt = right_map[tid]
            if lt.timeout != rt.timeout:
                result.entries.append(
                    CompareEntry(
                        "metadata",
                        f"{tid}.timeout",
                        left_value=str(lt.timeout),
                        right_value=str(rt.timeout),
                    )
                )

        left_edges = set(left.edges)
        right_edges = set(right.edges)
        for edge in sorted(left_edges - right_edges):
            result.entries.append(
                CompareEntry("edge", f"{edge[0]}->{edge[1]}", left_value="exists", right_value=None)
            )
        for edge in sorted(right_edges - left_edges):
            result.entries.append(
                CompareEntry("edge", f"{edge[0]}->{edge[1]}", left_value=None, right_value="exists")
            )

        return result
