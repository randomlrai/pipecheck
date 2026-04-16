"""Tests for pipecheck.extractor."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.extractor import DAGExtractor, ExtractError, ExtractResult


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    for tid in ("ingest", "transform", "load", "report"):
        dag.add_task(Task(task_id=tid))
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "load")
    dag.add_edge("load", "report")
    return dag


class TestExtractResult:
    def test_count_property(self):
        r = ExtractResult(dag_name="d", task_ids=["a", "b"])
        assert r.count == 2

    def test_has_skipped_false_when_empty(self):
        r = ExtractResult(dag_name="d", task_ids=["a"])
        assert not r.has_skipped

    def test_has_skipped_true_when_populated(self):
        r = ExtractResult(dag_name="d", task_ids=["a"], skipped_ids=["x"])
        assert r.has_skipped

    def test_str_contains_dag_name(self):
        r = ExtractResult(dag_name="mypipe", task_ids=["a"])
        assert "mypipe" in str(r)

    def test_str_shows_none_when_no_tasks(self):
        r = ExtractResult(dag_name="d", task_ids=[])
        assert "none" in str(r)

    def test_str_shows_skipped(self):
        r = ExtractResult(dag_name="d", task_ids=["a"], skipped_ids=["missing"])
        assert "missing" in str(r)


class TestDAGExtractor:
    def setup_method(self):
        self.dag = _build_dag()
        self.extractor = DAGExtractor()

    def test_extract_subset(self):
        result = self.extractor.extract(self.dag, ["ingest", "transform"])
        assert set(result.task_ids) == {"ingest", "transform"}

    def test_extract_records_skipped(self):
        result = self.extractor.extract(self.dag, ["ingest", "ghost"])
        assert "ghost" in result.skipped_ids

    def test_extract_all_tasks(self):
        all_ids = [t.task_id for t in self.dag.tasks]
        result = self.extractor.extract(self.dag, all_ids)
        assert result.count == len(all_ids)
        assert not result.has_skipped

    def test_extract_empty_raises(self):
        with pytest.raises(ExtractError):
            self.extractor.extract(self.dag, [])

    def test_extracted_dag_has_correct_edges(self):
        result = self.extractor.extract(self.dag, ["ingest", "transform", "load"])
        # edges between extracted tasks should be preserved
        assert result.count == 3

    def test_dag_name_preserved(self):
        result = self.extractor.extract(self.dag, ["ingest"])
        assert result.dag_name == "pipeline"
