"""Annotator module for attaching inline notes to DAG tasks."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipecheck.dag import DAG


@dataclass
class Annotation:
    task_id: str
    note: str
    author: Optional[str] = None

    def __str__(self) -> str:
        author_part = f" [{self.author}]" if self.author else ""
        return f"{self.task_id}{author_part}: {self.note}"


class AnnotationError(Exception):
    """Raised when an annotation references a non-existent task."""


class DAGAnnotator:
    """Attaches and retrieves annotations for tasks in a DAG."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag
        self._annotations: Dict[str, List[Annotation]] = {}

    def annotate(self, task_id: str, note: str, author: Optional[str] = None) -> Annotation:
        """Add a note to a task. Raises AnnotationError if task not found."""
        if task_id not in {t.task_id for t in self._dag.tasks}:
            raise AnnotationError(f"Task '{task_id}' not found in DAG '{self._dag.name}'.")
        annotation = Annotation(task_id=task_id, note=note, author=author)
        self._annotations.setdefault(task_id, []).append(annotation)
        return annotation

    def get(self, task_id: str) -> List[Annotation]:
        """Return all annotations for a given task."""
        return list(self._annotations.get(task_id, []))

    def all_annotations(self) -> List[Annotation]:
        """Return every annotation across all tasks, in insertion order."""
        result: List[Annotation] = []
        for notes in self._annotations.values():
            result.extend(notes)
        return result

    def to_dict(self) -> Dict[str, List[Dict]]:
        """Serialise annotations to a plain dictionary."""
        return {
            task_id: [
                {"note": a.note, "author": a.author}
                for a in notes
            ]
            for task_id, notes in self._annotations.items()
        }

    def clear(self, task_id: Optional[str] = None) -> None:
        """Remove annotations for a specific task, or all annotations."""
        if task_id is None:
            self._annotations.clear()
        else:
            self._annotations.pop(task_id, None)
