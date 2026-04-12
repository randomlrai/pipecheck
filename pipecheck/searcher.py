"""Search tasks in a DAG by various criteria."""
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG, Task


class SearchError(Exception):
    """Raised when a search operation fails."""


@dataclass
class SearchResult:
    dag_name: str
    query: str
    matches: List[Task] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.matches)

    @property
    def has_matches(self) -> bool:
        return len(self.matches) > 0

    def __str__(self) -> str:
        if not self.matches:
            return f"No tasks matched '{self.query}' in DAG '{self.dag_name}'."
        lines = [f"Search results for '{self.query}' in DAG '{self.dag_name}' ({self.count} match(es)):"]
        for task in self.matches:
            desc = f" — {task.description}" if task.description else ""
            lines.append(f"  - {task.task_id}{desc}")
        return "\n".join(lines)


class DAGSearcher:
    """Search tasks within a DAG by id substring, tag, or description keyword."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def search(
        self,
        query: str,
        by: str = "id",
    ) -> SearchResult:
        """Search tasks by 'id', 'tag', or 'description'."""
        valid_modes = {"id", "tag", "description"}
        if by not in valid_modes:
            raise SearchError(f"Invalid search mode '{by}'. Choose from: {valid_modes}")

        q = query.lower()
        matches: List[Task] = []

        for task in self._dag.tasks.values():
            if by == "id":
                if q in task.task_id.lower():
                    matches.append(task)
            elif by == "tag":
                tags = task.metadata.get("tags", [])
                if isinstance(tags, list) and any(q == t.lower() for t in tags):
                    matches.append(task)
            elif by == "description":
                desc = task.description or ""
                if q in desc.lower():
                    matches.append(task)

        matches.sort(key=lambda t: t.task_id)
        return SearchResult(dag_name=self._dag.name, query=query, matches=matches)
