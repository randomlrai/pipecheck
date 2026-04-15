"""Tests for pipecheck.rewirer."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.rewirer import DAGRewirer, RewireEntry, RewireResult, RewireError


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    for tid in ("ingest", "transform", "load", "report"):
        dag.add_task(Task(task_id=tid))
    dag.add_dependency("ingest", "transform")
    dag.add_dependency("transform", "load")
    dag.add_dependency("load", "report")
    return dag


class TestRewireEntry:
    def test_str_format(self):
        e = RewireEntry(
            old_source="a", old_target="b",
            new_source="a", new_target="c"
        )
        result = str(e)
        assert "a -> b" in result
        assert "a -> c" in result
        assert "rewired to" in result


class TestRewireResult:
    def test_has_changes_false_when_empty(self):
        dag = DAG(name="empty")
        r = RewireResult(dag=dag)
        assert r.has_changes() is False

    def test_has_changes_true_when_populated(self):
        dag = DAG(name="x")
        e = RewireEntry("a", "b", "a", "c")
        r = RewireResult(dag=dag, entries=[e])
        assert r.has_changes() is True

    def test_count_property(self):
        dag = DAG(name="x")
        entries = [
            RewireEntry("a", "b", "a", "c"),
            RewireEntry("c", "d", "e", "d"),
        ]
        r = RewireResult(dag=dag, entries=entries)
        assert r.count() == 2

    def test_str_no_changes(self):
        dag = DAG(name="mypipe")
        r = RewireResult(dag=dag)
        assert "no changes" in str(r)
        assert "mypipe" in str(r)

    def test_str_with_changes(self):
        dag = DAG(name="mypipe")
        e = RewireEntry("a", "b", "a", "c")
        r = RewireResult(dag=dag, entries=[e])
        text = str(r)
        assert "1 rewire" in text
        assert "a -> b" in text


class TestDAGRewirer:
    def setup_method(self):
        self.dag = _build_dag()
        self.rewirer = DAGRewirer()

    def test_rewire_existing_edge_returns_result(self):
        result = self.rewirer.rewire(
            self.dag, "ingest", "transform", "ingest", "load"
        )
        assert isinstance(result, RewireResult)
        assert result.has_changes()

    def test_rewire_entry_records_old_and_new(self):
        result = self.rewirer.rewire(
            self.dag, "transform", "load", "ingest", "load"
        )
        entry = result.entries[0]
        assert entry.old_source == "transform"
        assert entry.old_target == "load"
        assert entry.new_source == "ingest"
        assert entry.new_target == "load"

    def test_rewire_missing_old_source_raises(self):
        with pytest.raises(RewireError):
            self.rewirer.rewire(self.dag, "ghost", "transform", "ingest", "transform")

    def test_rewire_missing_new_target_raises(self):
        with pytest.raises(RewireError):
            self.rewirer.rewire(self.dag, "ingest", "transform", "ingest", "nowhere")

    def test_rewire_preserves_other_edges(self):
        result = self.rewirer.rewire(
            self.dag, "ingest", "transform", "ingest", "load"
        )
        deps = result.dag.dependencies
        # transform -> load and load -> report should still exist
        assert "load" in deps.get("transform", [])
        assert "report" in deps.get("load", [])

    def test_rewire_dag_name_preserved(self):
        result = self.rewirer.rewire(
            self.dag, "ingest", "transform", "ingest", "load"
        )
        assert result.dag.name == "pipeline"
