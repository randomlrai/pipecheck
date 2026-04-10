"""Tests for pipecheck.comparator."""

import pytest
from pipecheck.dag import DAG, Task
from pipecheck.comparator import (
    CompareEntry,
    CompareResult,
    CompareError,
    DAGComparator,
)


def _build_dag(name: str, tasks, edges=None) -> DAG:
    dag = DAG(name=name)
    for t in tasks:
        dag.add_task(t)
    for src, dst in (edges or []):
        dag.add_edge(src, dst)
    return dag


class TestCompareEntry:
    def test_str_both_values(self):
        entry = CompareEntry("task", "load", left_value="load", right_value=None)
        assert "task" in str(entry)
        assert "load" in str(entry)
        assert "<none>" in str(entry)

    def test_str_right_only(self):
        entry = CompareEntry("edge", "a->b", left_value=None, right_value="exists")
        assert "edge" in str(entry)
        assert "a->b" in str(entry)


class TestCompareResult:
    def test_has_differences_false_when_empty(self):
        r = CompareResult(left_name="a", right_name="b")
        assert not r.has_differences

    def test_has_differences_true_when_entries_present(self):
        r = CompareResult(left_name="a", right_name="b")
        r.entries.append(CompareEntry("task", "x", "x", None))
        assert r.has_differences

    def test_str_no_differences(self):
        r = CompareResult(left_name="dag1", right_name="dag2")
        output = str(r)
        assert "No differences" in output
        assert "dag1" in output

    def test_str_with_differences(self):
        r = CompareResult(left_name="dag1", right_name="dag2")
        r.entries.append(CompareEntry("task", "load", "load", None))
        output = str(r)
        assert "load" in output


class TestDAGComparator:
    def setup_method(self):
        self.comparator = DAGComparator()

    def test_identical_dags_no_differences(self):
        t = Task(task_id="load", operator="PythonOperator")
        left = _build_dag("dag", [t])
        right = _build_dag("dag", [Task(task_id="load", operator="PythonOperator")])
        result = self.comparator.compare(left, right)
        assert not result.has_differences

    def test_detects_added_task(self):
        left = _build_dag("dag", [Task(task_id="load", operator="PythonOperator")])
        right = _build_dag(
            "dag",
            [
                Task(task_id="load", operator="PythonOperator"),
                Task(task_id="transform", operator="PythonOperator"),
            ],
        )
        result = self.comparator.compare(left, right)
        keys = [e.key for e in result.entries]
        assert "transform" in keys

    def test_detects_removed_task(self):
        left = _build_dag(
            "dag",
            [
                Task(task_id="load", operator="PythonOperator"),
                Task(task_id="old", operator="PythonOperator"),
            ],
        )
        right = _build_dag("dag", [Task(task_id="load", operator="PythonOperator")])
        result = self.comparator.compare(left, right)
        keys = [e.key for e in result.entries]
        assert "old" in keys

    def test_detects_timeout_change(self):
        left = _build_dag("dag", [Task(task_id="t", operator="Op", timeout=30)])
        right = _build_dag("dag", [Task(task_id="t", operator="Op", timeout=60)])
        result = self.comparator.compare(left, right)
        keys = [e.key for e in result.entries]
        assert "t.timeout" in keys

    def test_detects_added_edge(self):
        t1 = Task(task_id="a", operator="Op")
        t2 = Task(task_id="b", operator="Op")
        left = _build_dag("dag", [t1, t2])
        right = _build_dag(
            "dag",
            [Task(task_id="a", operator="Op"), Task(task_id="b", operator="Op")],
            edges=[("a", "b")],
        )
        result = self.comparator.compare(left, right)
        categories = [e.category for e in result.entries]
        assert "edge" in categories

    def test_raises_on_invalid_input(self):
        with pytest.raises(CompareError):
            self.comparator.compare("not_a_dag", DAG(name="x"))
