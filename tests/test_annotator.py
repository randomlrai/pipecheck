"""Tests for pipecheck.annotator."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.annotator import Annotation, AnnotationError, DAGAnnotator


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task(task_id="extract", command="run extract"))
    dag.add_task(Task(task_id="transform", command="run transform"))
    dag.add_dependency("extract", "transform")
    return dag


class TestAnnotation:
    def test_str_without_author(self):
        ann = Annotation(task_id="extract", note="needs review")
        assert str(ann) == "extract: needs review"

    def test_str_with_author(self):
        ann = Annotation(task_id="extract", note="needs review", author="alice")
        assert str(ann) == "extract [alice]: needs review"


class TestDAGAnnotator:
    def setup_method(self):
        self.dag = _build_dag()
        self.annotator = DAGAnnotator(self.dag)

    def test_annotate_valid_task(self):
        ann = self.annotator.annotate("extract", "check source schema")
        assert ann.task_id == "extract"
        assert ann.note == "check source schema"
        assert ann.author is None

    def test_annotate_with_author(self):
        ann = self.annotator.annotate("transform", "heavy compute", author="bob")
        assert ann.author == "bob"

    def test_annotate_unknown_task_raises(self):
        with pytest.raises(AnnotationError, match="'missing_task'"):
            self.annotator.annotate("missing_task", "some note")

    def test_get_returns_annotations_for_task(self):
        self.annotator.annotate("extract", "note one")
        self.annotator.annotate("extract", "note two")
        notes = self.annotator.get("extract")
        assert len(notes) == 2
        assert notes[0].note == "note one"

    def test_get_returns_empty_for_unannotated_task(self):
        assert self.annotator.get("transform") == []

    def test_all_annotations_returns_all(self):
        self.annotator.annotate("extract", "a")
        self.annotator.annotate("transform", "b")
        all_ann = self.annotator.all_annotations()
        assert len(all_ann) == 2

    def test_to_dict_structure(self):
        self.annotator.annotate("extract", "doc note", author="carol")
        d = self.annotator.to_dict()
        assert "extract" in d
        assert d["extract"][0] == {"note": "doc note", "author": "carol"}

    def test_clear_specific_task(self):
        self.annotator.annotate("extract", "temp")
        self.annotator.clear("extract")
        assert self.annotator.get("extract") == []

    def test_clear_all(self):
        self.annotator.annotate("extract", "a")
        self.annotator.annotate("transform", "b")
        self.annotator.clear()
        assert self.annotator.all_annotations() == []
