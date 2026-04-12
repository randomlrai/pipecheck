"""Classify DAG tasks into categories based on their properties."""

from dataclasses import dataclass, field
from typing import Dict, List

from pipecheck.dag import DAG, Task


class ClassifyError(Exception):
    """Raised when classification fails."""


@dataclass
class TaskClass:
    """A named category containing a set of tasks."""

    name: str
    tasks: List[Task] = field(default_factory=list)

    def __str__(self) -> str:
        task_ids = ", ".join(t.task_id for t in self.tasks)
        return f"{self.name}: [{task_ids}]"

    def __len__(self) -> int:
        return len(self.tasks)


@dataclass
class ClassifyResult:
    """Result of classifying all tasks in a DAG."""

    dag_name: str
    classes: Dict[str, TaskClass] = field(default_factory=dict)

    def __str__(self) -> str:
        lines = [f"Classification for '{self.dag_name}':"]
        for cls in self.classes.values():
            lines.append(f"  {cls}")
        return "\n".join(lines)

    def get(self, name: str) -> TaskClass:
        return self.classes.get(name, TaskClass(name=name))

    def has_class(self, name: str) -> bool:
        return name in self.classes and len(self.classes[name]) > 0


class DAGClassifier:
    """Classify tasks in a DAG by role: source, sink, intermediate, isolated."""

    def classify(self, dag: DAG) -> ClassifyResult:
        if not dag.tasks:
            return ClassifyResult(dag_name=dag.name)

        result = ClassifyResult(dag_name=dag.name)
        categories = ["source", "sink", "intermediate", "isolated"]
        for cat in categories:
            result.classes[cat] = TaskClass(name=cat)

        all_ids = set(dag.tasks.keys())
        has_upstream: set = set()
        has_downstream: set = set()

        for task in dag.tasks.values():
            for dep in task.dependencies:
                has_upstream.add(task.task_id)
                has_downstream.add(dep)

        for task_id in all_ids:
            task = dag.tasks[task_id]
            is_up = task_id in has_upstream
            is_down = task_id in has_downstream

            if not is_up and not is_down:
                result.classes["isolated"].tasks.append(task)
            elif not is_down and is_up:
                result.classes["source"].tasks.append(task)
            elif is_down and not is_up:
                result.classes["sink"].tasks.append(task)
            else:
                result.classes["intermediate"].tasks.append(task)

        return result
