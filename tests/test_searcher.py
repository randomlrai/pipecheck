"""Tests for pipecheck.searcher."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.searcher import DAGSearcher, SearchResult, SearchError


def _build_dag() -> DAG:
    dag = DAG(name="test_pipeline")
    dag.add_task(Task(task_id="extract_orders", description="Pull orders from DB", metadata={"tags": ["io", "extract"]}))
    dag.add_task(Task(task_id="transform_orders", description="Clean and reshape orders", metadata={"tags": ["transform"]}))
    dag.add_task(Task(task_id="load_warehouse", description="Load data into warehouse", metadata={"tags": ["io", "load"]}))
    dag.add_task(Task(task_id="notify_slack", description="Send Slack notification", metadata={"tags": ["notify"]}))
    dag.add_edge("extract_orders", "transform_orders")
    dag.add_edge("transform_orders", "load_warehouse")
    dag.add_edge("load_warehouse", "notify_slack")
    return dag


class TestSearchResult:
    def test_count_property(self):
        dag = _build_dag()
        searcher = DAGSearcher(dag)
        result = searcher.search("orders", by="id")
        assert result.count == 2

    def test_has_matches_true(self):
        dag = _build_dag()
        result = DAGSearcher(dag).search("load", by="id")
        assert result.has_matches is True

    def test_has_matches_false_when_empty(self):
        dag = _build_dag()
        result = DAGSearcher(dag).search("zzznomatch", by="id")
        assert result.has_matches is False

    def test_str_no_matches(self):
        dag = _build_dag()
        result = DAGSearcher(dag).search("zzz", by="id")
        assert "No tasks matched" in str(result)
        assert "zzz" in str(result)

    def test_str_with_matches(self):
        dag = _build_dag()
        result = DAGSearcher(dag).search("orders", by="id")
        text = str(result)
        assert "extract_orders" in text
        assert "transform_orders" in text
        assert "2 match" in text


class TestDAGSearcher:
    def test_search_by_id_substring(self):
        dag = _build_dag()
        result = DAGSearcher(dag).search("order", by="id")
        ids = [t.task_id for t in result.matches]
        assert "extract_orders" in ids
        assert "transform_orders" in ids
        assert "load_warehouse" not in ids

    def test_search_by_tag(self):
        dag = _build_dag()
        result = DAGSearcher(dag).search("io", by="tag")
        ids = [t.task_id for t in result.matches]
        assert "extract_orders" in ids
        assert "load_warehouse" in ids
        assert len(ids) == 2

    def test_search_by_description(self):
        dag = _build_dag()
        result = DAGSearcher(dag).search("slack", by="description")
        assert result.count == 1
        assert result.matches[0].task_id == "notify_slack"

    def test_search_case_insensitive(self):
        dag = _build_dag()
        result = DAGSearcher(dag).search("ORDERS", by="id")
        assert result.count == 2

    def test_search_invalid_mode_raises(self):
        dag = _build_dag()
        with pytest.raises(SearchError, match="Invalid search mode"):
            DAGSearcher(dag).search("x", by="unknown")

    def test_results_sorted_by_task_id(self):
        dag = _build_dag()
        result = DAGSearcher(dag).search("o", by="id")
        ids = [t.task_id for t in result.matches]
        assert ids == sorted(ids)

    def test_dag_name_stored_in_result(self):
        dag = _build_dag()
        result = DAGSearcher(dag).search("x", by="id")
        assert result.dag_name == "test_pipeline"
