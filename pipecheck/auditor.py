"""DAG audit trail: records structural facts about a DAG for compliance review."""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Any

from pipecheck.dag import DAG


@dataclass
class AuditEntry:
    """A single auditable fact about a DAG."""
    category: str
    key: str
    value: Any

    def __str__(self) -> str:
        return f"[{self.category}] {self.key}: {self.value}"


@dataclass
class AuditReport:
    """Collection of audit entries for a DAG."""
    dag_name: str
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    entries: List[AuditEntry] = field(default_factory=list)

    def add(self, category: str, key: str, value: Any) -> None:
        self.entries.append(AuditEntry(category=category, key=key, value=value))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dag_name": self.dag_name,
            "generated_at": self.generated_at,
            "entries": [
                {"category": e.category, "key": e.key, "value": e.value}
                for e in self.entries
            ],
        }

    def __len__(self) -> int:
        return len(self.entries)


class DAGAuditor:
    """Inspects a DAG and produces a structured AuditReport."""

    def audit(self, dag: DAG) -> AuditReport:
        report = AuditReport(dag_name=dag.name)

        # Structure facts
        report.add("structure", "task_count", len(dag.tasks))
        report.add("structure", "edge_count", sum(
            len(deps) for deps in dag.dependencies.values()
        ))
        report.add("structure", "task_ids", sorted(t.task_id for t in dag.tasks))

        # Root tasks (no incoming dependencies)
        all_ids = {t.task_id for t in dag.tasks}
        dependent_ids = set(dag.dependencies.keys())
        roots = sorted(all_ids - dependent_ids)
        report.add("structure", "root_tasks", roots)

        # Leaf tasks (no outgoing dependencies)
        tasks_with_deps = set()
        for deps in dag.dependencies.values():
            tasks_with_deps.update(deps)
        leaves = sorted(all_ids - tasks_with_deps)
        report.add("structure", "leaf_tasks", leaves)

        # Timeout coverage
        tasks_with_timeout = [
            t.task_id for t in dag.tasks if t.timeout is not None
        ]
        report.add("coverage", "tasks_with_timeout", sorted(tasks_with_timeout))
        report.add("coverage", "timeout_coverage_pct",
                   round(len(tasks_with_timeout) / max(len(dag.tasks), 1) * 100, 1))

        # Metadata coverage
        tasks_with_meta = [
            t.task_id for t in dag.tasks if t.metadata
        ]
        report.add("coverage", "tasks_with_metadata", sorted(tasks_with_meta))

        return report
