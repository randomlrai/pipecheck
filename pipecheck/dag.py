"""Core DAG (Directed Acyclic Graph) data structures and validation."""

from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field


@dataclass
class Task:
    """Represents a single task in the pipeline."""
    name: str
    dependencies: List[str] = field(default_factory=list)
    metadata: Dict[str, any] = field(default_factory=dict)

    def __hash__(self):
        return hash(self.name)


class DAG:
    """Directed Acyclic Graph for representing data pipelines."""

    def __init__(self, name: str = "pipeline"):
        self.name = name
        self.tasks: Dict[str, Task] = {}

    def add_task(self, task: Task) -> None:
        """Add a task to the DAG."""
        if task.name in self.tasks:
            raise ValueError(f"Task '{task.name}' already exists in DAG")
        self.tasks[task.name] = task

    def get_task(self, name: str) -> Optional[Task]:
        """Retrieve a task by name."""
        return self.tasks.get(name)

    def validate(self) -> List[str]:
        """Validate the DAG structure and return list of errors."""
        errors = []

        # Check for missing dependencies
        for task in self.tasks.values():
            for dep in task.dependencies:
                if dep not in self.tasks:
                    errors.append(f"Task '{task.name}' depends on non-existent task '{dep}'")

        # Check for cycles
        if self._has_cycle():
            errors.append("DAG contains cycles")

        return errors

    def _has_cycle(self) -> bool:
        """Detect cycles using DFS."""
        visited = set()
        rec_stack = set()

        def visit(task_name: str) -> bool:
            visited.add(task_name)
            rec_stack.add(task_name)

            task = self.tasks.get(task_name)
            if task:
                for dep in task.dependencies:
                    if dep not in visited:
                        if visit(dep):
                            return True
                    elif dep in rec_stack:
                        return True

            rec_stack.remove(task_name)
            return False

        for task_name in self.tasks:
            if task_name not in visited:
                if visit(task_name):
                    return True
        return False

    def get_execution_order(self) -> List[str]:
        """Return topologically sorted task execution order."""
        in_degree = {name: 0 for name in self.tasks}
        
        for task in self.tasks.values():
            for dep in task.dependencies:
                if dep in in_degree:
                    in_degree[task.name] += 1

        queue = [name for name, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            current = queue.pop(0)
            result.append(current)

            for task in self.tasks.values():
                if current in task.dependencies:
                    in_degree[task.name] -= 1
                    if in_degree[task.name] == 0:
                        queue.append(task.name)

        return result if len(result) == len(self.tasks) else []
