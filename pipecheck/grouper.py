"""DAG task grouper: partition tasks into named groups by tag or prefix."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipecheck.dag import DAG, Task


class GroupError(Exception):
    """Raised when grouping cannot be performed."""


@dataclass
class TaskGroup:
    name: str
    tasks: List[Task] = field(default_factory=list)

    def __str__(self) -> str:
        ids = ", ".join(t.task_id for t in self.tasks)
        return f"Group({self.name!r}): [{ids}]"

    def __len__(self) -> int:
        return len(self.tasks)


@dataclass
class GroupResult:
    groups: Dict[str, TaskGroup] = field(default_factory=dict)
    ungrouped: List[Task] = field(default_factory=list)

    @property
    def group_names(self) -> List[str]:
        return sorted(self.groups.keys())

    def __str__(self) -> str:
        lines = [f"GroupResult: {len(self.groups)} group(s), {len(self.ungrouped)} ungrouped"]
        for name in self.group_names:
            lines.append(f"  {self.groups[name]}")
        if self.ungrouped:
            ids = ", ".join(t.task_id for t in self.ungrouped)
            lines.append(f"  Ungrouped: [{ids}]")
        return "\n".join(lines)


class DAGGrouper:
    """Partition DAG tasks into named groups."""

    def by_tag(self, dag: DAG) -> GroupResult:
        """Group tasks by their first tag (metadata key 'tags')."""
        result = GroupResult()
        for task in dag.tasks.values():
            tags = task.metadata.get("tags", [])
            if not tags:
                result.ungrouped.append(task)
                continue
            group_name = tags[0]
            if group_name not in result.groups:
                result.groups[group_name] = TaskGroup(name=group_name)
            result.groups[group_name].tasks.append(task)
        return result

    def by_prefix(self, dag: DAG, separator: str = "_") -> GroupResult:
        """Group tasks by the prefix before the first separator in task_id."""
        if not separator:
            raise GroupError("separator must not be empty")
        result = GroupResult()
        for task in dag.tasks.values():
            if separator in task.task_id:
                prefix = task.task_id.split(separator, 1)[0]
            else:
                result.ungrouped.append(task)
                continue
            if prefix not in result.groups:
                result.groups[prefix] = TaskGroup(name=prefix)
            result.groups[prefix].tasks.append(task)
        return result
