"""DAG summary generator for pipecheck."""
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG


@dataclass
class DAGSummary:
    """Human-readable summary of a DAG's structure and metadata."""

    dag_name: str
    task_count: int
    edge_count: int
    root_tasks: List[str]
    leaf_tasks: List[str]
    max_depth: int
    tags_used: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [
            f"DAG: {self.dag_name}",
            f"  Tasks      : {self.task_count}",
            f"  Edges      : {self.edge_count}",
            f"  Max Depth  : {self.max_depth}",
            f"  Root Tasks : {', '.join(self.root_tasks) or 'none'}",
            f"  Leaf Tasks : {', '.join(self.leaf_tasks) or 'none'}",
        ]
        if self.tags_used:
            lines.append(f"  Tags       : {', '.join(sorted(self.tags_used))}")
        return "\n".join(lines)


class DAGSummarizer:
    """Generates a DAGSummary from a DAG instance."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def summarize(self) -> DAGSummary:
        dag = self._dag
        task_ids = list(dag.tasks.keys())

        # Compute in-degree and out-degree
        in_degree = {tid: 0 for tid in task_ids}
        out_degree = {tid: 0 for tid in task_ids}
        edge_count = 0
        for tid in task_ids:
            for dep in dag.tasks[tid].dependencies:
                in_degree[tid] += 1
                out_degree[dep] += 1
                edge_count += 1

        roots = [tid for tid in task_ids if in_degree[tid] == 0]
        leaves = [tid for tid in task_ids if out_degree[tid] == 0]

        max_depth = self._compute_max_depth(roots, dag)

        all_tags: List[str] = []
        for task in dag.tasks.values():
            all_tags.extend(task.metadata.get("tags", []))
        unique_tags = list(set(all_tags))

        return DAGSummary(
            dag_name=dag.name,
            task_count=len(task_ids),
            edge_count=edge_count,
            root_tasks=sorted(roots),
            leaf_tasks=sorted(leaves),
            max_depth=max_depth,
            tags_used=unique_tags,
        )

    def _compute_max_depth(self, roots: List[str], dag: DAG) -> int:
        if not roots:
            return 0
        depths = {tid: 0 for tid in dag.tasks}
        visited = set()

        def dfs(tid: str, depth: int) -> None:
            if depth > depths[tid]:
                depths[tid] = depth
            visited.add(tid)
            for other_id, task in dag.tasks.items():
                if tid in task.dependencies and other_id not in visited:
                    dfs(other_id, depth + 1)

        for root in roots:
            dfs(root, 0)
        return max(depths.values(), default=0)
