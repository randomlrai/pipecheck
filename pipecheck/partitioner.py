"""Partition a DAG into subgraphs based on a task attribute or tag."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG, Task


class PartitionError(Exception):
    """Raised when partitioning fails."""


@dataclass
class Partition:
    """A named subset of tasks from a DAG."""

    name: str
    tasks: List[Task] = field(default_factory=list)

    def __str__(self) -> str:
        ids = ", ".join(t.task_id for t in sorted(self.tasks, key=lambda t: t.task_id))
        return f"Partition({self.name!r}, tasks=[{ids}])"

    def __len__(self) -> int:
        return len(self.tasks)


@dataclass
class PartitionResult:
    """Result of partitioning a DAG."""

    dag_name: str
    partitions: Dict[str, Partition] = field(default_factory=dict)
    unassigned: List[Task] = field(default_factory=list)

    def partition_names(self) -> List[str]:
        return sorted(self.partitions.keys())

    def has_unassigned(self) -> bool:
        return len(self.unassigned) > 0

    def __str__(self) -> str:
        lines = [f"PartitionResult for '{self.dag_name}':"]
        for name in self.partition_names():
            p = self.partitions[name]
            lines.append(f"  {name}: {len(p)} task(s)")
        if self.unassigned:
            ids = ", ".join(t.task_id for t in self.unassigned)
            lines.append(f"  (unassigned): {ids}")
        return "\n".join(lines)


class DAGPartitioner:
    """Partitions a DAG by tag or by a metadata key."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def by_tag(self) -> PartitionResult:
        """Group tasks into partitions, one per tag. Untagged tasks go to 'unassigned'."""
        result = PartitionResult(dag_name=self._dag.name)
        for task in self._dag.tasks.values():
            tags = task.metadata.get("tags", [])
            if not tags:
                result.unassigned.append(task)
                continue
            for tag in tags:
                if tag not in result.partitions:
                    result.partitions[tag] = Partition(name=tag)
                result.partitions[tag].tasks.append(task)
        return result

    def by_metadata_key(self, key: str) -> PartitionResult:
        """Group tasks by a string value stored in task.metadata[key]."""
        result = PartitionResult(dag_name=self._dag.name)
        for task in self._dag.tasks.values():
            value: Optional[str] = task.metadata.get(key)
            if value is None:
                result.unassigned.append(task)
                continue
            bucket = str(value)
            if bucket not in result.partitions:
                result.partitions[bucket] = Partition(name=bucket)
            result.partitions[bucket].tasks.append(task)
        return result
