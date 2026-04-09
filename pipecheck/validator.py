"""Validation logic for DAG structures."""

from typing import List, Dict, Set, Tuple
from .dag import DAG, Task


class ValidationError(Exception):
    """Base exception for validation errors."""
    pass


class CyclicDependencyError(ValidationError):
    """Raised when a cycle is detected in the DAG."""
    pass


class OrphanTaskError(ValidationError):
    """Raised when a task has no dependencies and no dependents."""
    pass


class DAGValidator:
    """Validates DAG structures for common issues."""

    def __init__(self, dag: DAG):
        self.dag = dag
        self.errors: List[ValidationError] = []
        self.warnings: List[str] = []

    def validate(self) -> Tuple[bool, List[ValidationError], List[str]]:
        """Run all validation checks.
        
        Returns:
            Tuple of (is_valid, errors, warnings)
        """
        self.errors = []
        self.warnings = []

        self._check_cycles()
        self._check_orphan_tasks()
        self._check_task_metadata()

        return len(self.errors) == 0, self.errors, self.warnings

    def _check_cycles(self) -> None:
        """Detect cycles using DFS."""
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def visit(task_id: str) -> bool:
            visited.add(task_id)
            rec_stack.add(task_id)

            for dep_id in self.dag.tasks[task_id].dependencies:
                if dep_id not in visited:
                    if visit(dep_id):
                        return True
                elif dep_id in rec_stack:
                    self.errors.append(
                        CyclicDependencyError(f"Cycle detected involving task '{dep_id}'")
                    )
                    return True

            rec_stack.remove(task_id)
            return False

        for task_id in self.dag.tasks:
            if task_id not in visited:
                visit(task_id)

    def _check_orphan_tasks(self) -> None:
        """Check for tasks with no dependencies and no dependents."""
        if len(self.dag.tasks) <= 1:
            return

        has_dependents = set()
        for task in self.dag.tasks.values():
            has_dependents.update(task.dependencies)

        for task_id, task in self.dag.tasks.items():
            if not task.dependencies and task_id not in has_dependents:
                self.warnings.append(f"Task '{task_id}' appears to be orphaned (no dependencies or dependents)")

    def _check_task_metadata(self) -> None:
        """Validate task metadata for common issues."""
        for task_id, task in self.dag.tasks.items():
            if task.metadata.get('timeout', 0) > 86400:
                self.warnings.append(f"Task '{task_id}' has unusually long timeout (>24h)")
            
            if task.metadata.get('retries', 0) > 10:
                self.warnings.append(f"Task '{task_id}' has high retry count (>10)")
