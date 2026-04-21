"""DAG segmentation: split a DAG into named segments based on task depth ranges."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipecheck.dag import DAG, Task


class SegmentError(Exception):
    """Raised when segmentation fails."""


@dataclass
class Segment:
    """A named group of tasks occupying a contiguous depth range."""

    name: str
    tasks: List[Task] = field(default_factory=list)
    depth_start: int = 0
    depth_end: int = 0

    def __len__(self) -> int:
        return len(self.tasks)

    def __str__(self) -> str:
        ids = ", ".join(t.task_id for t in self.tasks)
        return (
            f"Segment({self.name!r}, depth={self.depth_start}-{self.depth_end}, "
            f"tasks=[{ids}])"
        )


@dataclass
class SegmentResult:
    """Result of a segmentation run."""

    dag_name: str
    segments: List[Segment] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.segments)

    def get(self, name: str) -> Segment | None:
        for seg in self.segments:
            if seg.name == name:
                return seg
        return None

    def __str__(self) -> str:
        lines = [f"SegmentResult for '{self.dag_name}' ({self.count} segment(s)):"]
        for seg in self.segments:
            lines.append(f"  {seg}")
        return "\n".join(lines)


class DAGSegmenter:
    """Segments a DAG into depth-based slices."""

    def segment(self, dag: DAG, size: int = 1) -> SegmentResult:
        """Group tasks into segments of *size* depth levels each."""
        if size < 1:
            raise SegmentError("Segment size must be >= 1")

        depths = self._compute_depths(dag)
        if not depths:
            return SegmentResult(dag_name=dag.name)

        max_depth = max(depths.values())
        segments: List[Segment] = []

        for start in range(0, max_depth + 1, size):
            end = start + size - 1
            tasks_in_range = [
                dag.tasks[tid]
                for tid, d in sorted(depths.items())
                if start <= d <= end
            ]
            if tasks_in_range:
                seg = Segment(
                    name=f"seg_{start}_{end}",
                    tasks=tasks_in_range,
                    depth_start=start,
                    depth_end=end,
                )
                segments.append(seg)

        return SegmentResult(dag_name=dag.name, segments=segments)

    def _compute_depths(self, dag: DAG) -> Dict[str, int]:
        depths: Dict[str, int] = {}
        visited: set = set()

        def visit(task_id: str, depth: int) -> None:
            if task_id in visited:
                return
            visited.add(task_id)
            depths[task_id] = max(depths.get(task_id, 0), depth)
            for dep in dag.tasks[task_id].dependencies:
                visit(dep, depth - 1)

        # Assign depth 0 to roots, propagate downward
        for tid in dag.tasks:
            if not dag.tasks[tid].dependencies:
                depths[tid] = 0

        changed = True
        while changed:
            changed = False
            for tid, task in dag.tasks.items():
                for dep in task.dependencies:
                    new_depth = depths.get(dep, 0) + 1
                    if new_depth > depths.get(tid, 0):
                        depths[tid] = new_depth
                        changed = True

        return depths
