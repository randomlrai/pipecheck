"""Tag-based filtering and grouping for DAG tasks."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set
from pipecheck.dag import DAG, Task


@dataclass
class TagIndex:
    """Maps tags to the tasks that carry them."""
    _index: Dict[str, Set[str]] = field(default_factory=dict)

    def add(self, tag: str, task_id: str) -> None:
        self._index.setdefault(tag, set()).add(task_id)

    def tasks_for_tag(self, tag: str) -> Set[str]:
        return frozenset(self._index.get(tag, set()))

    def all_tags(self) -> List[str]:
        return sorted(self._index.keys())

    def __len__(self) -> int:
        return len(self._index)


class DAGTagger:
    """Builds a TagIndex from a DAG and provides filtered sub-views."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag
        self._index = self._build_index(dag)

    # ------------------------------------------------------------------
    def _build_index(self, dag: DAG) -> TagIndex:
        idx = TagIndex()
        for task in dag.tasks.values():
            for tag in task.metadata.get("tags", []):
                idx.add(str(tag), task.task_id)
        return idx

    # ------------------------------------------------------------------
    @property
    def index(self) -> TagIndex:
        return self._index

    def tasks_by_tag(self, tag: str) -> List[Task]:
        """Return Task objects whose metadata contains *tag*."""
        ids = self._index.tasks_for_tag(tag)
        return [self._dag.tasks[tid] for tid in sorted(ids) if tid in self._dag.tasks]

    def filter_dag(self, tag: str) -> List[str]:
        """Return task IDs that belong to *tag* (sorted)."""
        return sorted(self._index.tasks_for_tag(tag))

    def summary(self) -> Dict[str, int]:
        """Return mapping of tag -> task count."""
        return {tag: len(self._index.tasks_for_tag(tag)) for tag in self._index.all_tags()}
