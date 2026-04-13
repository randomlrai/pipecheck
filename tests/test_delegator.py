"""Tests for pipecheck.delegator."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.delegator import (
    DAGDelegator,
    DelegateEntry,
    DelegateError,
    DelegateResult,
)


def _build_dag() -> DAG:
    dag = DAG(name="test_pipeline")
    for tid in ("ingest", "transform", "load", "report"):
        dag.add_task(Task(task_id=tid))
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "load")
    dag.add_edge("load", "report")
    return dag


class TestDelegateEntry:
    def test_str_without_reason(self):
        e = DelegateEntry(task_id="ingest", owner="alice")
        assert str(e) == "ingest -> alice"

    def test_str_with_reason(self):
        e = DelegateEntry(task_id="load", owner="bob", reason="owns infra")
        assert str(e) == "load -> bob (owns infra)"


class TestDelegateResult:
    def test_has_assignments_false_when_empty(self):
        r = DelegateResult(dag_name="pipe")
        assert r.has_assignments is False

    def test_has_assignments_true_when_populated(self):
        r = DelegateResult(dag_name="pipe")
        r.entries.append(DelegateEntry(task_id="ingest", owner="alice"))
        assert r.has_assignments is True

    def test_count_property(self):
        r = DelegateResult(dag_name="pipe")
        r.entries.append(DelegateEntry(task_id="ingest", owner="alice"))
        r.entries.append(DelegateEntry(task_id="load", owner="bob"))
        assert r.count == 2

    def test_owners_sorted_unique(self):
        r = DelegateResult(dag_name="pipe")
        r.entries.append(DelegateEntry(task_id="ingest", owner="charlie"))
        r.entries.append(DelegateEntry(task_id="load", owner="alice"))
        r.entries.append(DelegateEntry(task_id="transform", owner="alice"))
        assert r.owners() == ["alice", "charlie"]

    def test_tasks_for_owner(self):
        r = DelegateResult(dag_name="pipe")
        r.entries.append(DelegateEntry(task_id="ingest", owner="alice"))
        r.entries.append(DelegateEntry(task_id="transform", owner="alice"))
        r.entries.append(DelegateEntry(task_id="load", owner="bob"))
        assert r.tasks_for_owner("alice") == ["ingest", "transform"]
        assert r.tasks_for_owner("bob") == ["load"]
        assert r.tasks_for_owner("unknown") == []

    def test_str_empty(self):
        r = DelegateResult(dag_name="pipe")
        assert "no assignments" in str(r)

    def test_str_non_empty(self):
        r = DelegateResult(dag_name="pipe")
        r.entries.append(DelegateEntry(task_id="ingest", owner="alice"))
        out = str(r)
        assert "DelegateResult(pipe)" in out
        assert "ingest -> alice" in out


class TestDAGDelegator:
    def test_assign_valid_tasks(self):
        dag = _build_dag()
        delegator = DAGDelegator(dag)
        result = delegator.assign({"ingest": "alice", "load": "bob"})
        assert result.dag_name == "test_pipeline"
        assert result.count == 2

    def test_assign_with_reason(self):
        dag = _build_dag()
        delegator = DAGDelegator(dag)
        result = delegator.assign({"ingest": "alice"}, reason="data owner")
        assert result.entries[0].reason == "data owner"

    def test_assign_unknown_task_raises(self):
        dag = _build_dag()
        delegator = DAGDelegator(dag)
        with pytest.raises(DelegateError, match="Unknown task"):
            delegator.assign({"nonexistent": "alice"})

    def test_assign_entries_sorted_by_task_id(self):
        dag = _build_dag()
        delegator = DAGDelegator(dag)
        result = delegator.assign({"transform": "bob", "ingest": "alice"})
        ids = [e.task_id for e in result.entries]
        assert ids == sorted(ids)
