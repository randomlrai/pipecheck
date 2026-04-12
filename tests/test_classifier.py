"""Tests for pipecheck.classifier."""

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.classifier import ClassifyError, TaskClass, ClassifyResult, DAGClassifier


def _build_dag():
    dag = DAG(name="pipeline")
    dag.add_task(Task(task_id="ingest"))
    dag.add_task(Task(task_id="transform", dependencies=["ingest"]))
    dag.add_task(Task(task_id="load", dependencies=["transform"]))
    return dag


class TestTaskClass:
    def test_str_format(self):
        tc = TaskClass(name="source", tasks=[Task(task_id="ingest")])
        assert str(tc) == "source: [ingest]"

    def test_str_empty(self):
        tc = TaskClass(name="sink")
        assert str(tc) == "sink: []"

    def test_len(self):
        tc = TaskClass(name="source", tasks=[Task(task_id="a"), Task(task_id="b")])
        assert len(tc) == 2

    def test_len_empty(self):
        tc = TaskClass(name="isolated")
        assert len(tc) == 0


class TestClassifyResult:
    def test_str_contains_dag_name(self):
        result = ClassifyResult(dag_name="mypipe")
        assert "mypipe" in str(result)

    def test_get_missing_class_returns_empty(self):
        result = ClassifyResult(dag_name="x")
        cls = result.get("nonexistent")
        assert cls.name == "nonexistent"
        assert len(cls) == 0

    def test_has_class_false_when_empty(self):
        result = ClassifyResult(dag_name="x")
        assert not result.has_class("source")

    def test_has_class_true_when_populated(self):
        result = ClassifyResult(dag_name="x")
        result.classes["source"] = TaskClass(
            name="source", tasks=[Task(task_id="a")]
        )
        assert result.has_class("source")


class TestDAGClassifier:
    def setup_method(self):
        self.classifier = DAGClassifier()

    def test_empty_dag_returns_empty_result(self):
        dag = DAG(name="empty")
        result = self.classifier.classify(dag)
        assert result.dag_name == "empty"
        assert not result.classes

    def test_chain_dag_source_identified(self):
        dag = _build_dag()
        result = self.classifier.classify(dag)
        sources = [t.task_id for t in result.get("source").tasks]
        assert "ingest" in sources

    def test_chain_dag_sink_identified(self):
        dag = _build_dag()
        result = self.classifier.classify(dag)
        sinks = [t.task_id for t in result.get("sink").tasks]
        assert "load" in sinks

    def test_chain_dag_intermediate_identified(self):
        dag = _build_dag()
        result = self.classifier.classify(dag)
        intermediates = [t.task_id for t in result.get("intermediate").tasks]
        assert "transform" in intermediates

    def test_isolated_task_classified(self):
        dag = DAG(name="test")
        dag.add_task(Task(task_id="alone"))
        result = self.classifier.classify(dag)
        isolated = [t.task_id for t in result.get("isolated").tasks]
        assert "alone" in isolated

    def test_str_output_includes_classes(self):
        dag = _build_dag()
        result = self.classifier.classify(dag)
        text = str(result)
        assert "pipeline" in text
        assert "source" in text
        assert "sink" in text
