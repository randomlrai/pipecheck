"""DAG task cap enforcement — limits the number of tasks allowed in a DAG."""

from dataclasses import dataclass, field
from typing import List

from pipecheck.dag import DAG


class CapError(Exception):
    """Raised when cap enforcement fails."""


@dataclass
class CapResult:
    dag_name: str
    limit: int
    actual: int
    exceeded: bool
    over_by: int
    offending_tasks: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        status = "EXCEEDED" if self.exceeded else "OK"
        lines = [
            f"DAG: {self.dag_name}",
            f"Cap: {self.limit} | Actual: {self.actual} | Status: {status}",
        ]
        if self.exceeded:
            lines.append(f"Over by: {self.over_by} task(s)")
            if self.offending_tasks:
                task_list = ", ".join(self.offending_tasks)
                lines.append(f"Extra tasks: {task_list}")
        return "\n".join(lines)


class DAGCapper:
    """Enforces a maximum task count on a DAG."""

    def cap(self, dag: DAG, limit: int) -> CapResult:
        if limit < 0:
            raise CapError(f"Limit must be non-negative, got {limit}")

        task_ids = list(dag.tasks.keys())
        actual = len(task_ids)
        exceeded = actual > limit
        over_by = max(0, actual - limit)
        offending = task_ids[limit:] if exceeded else []

        return CapResult(
            dag_name=dag.name,
            limit=limit,
            actual=actual,
            exceeded=exceeded,
            over_by=over_by,
            offending_tasks=offending,
        )
