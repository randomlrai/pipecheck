"""Tests for pipecheck.differ module."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.differ import DAGDiffer, DAGDiff, DiffEntry


def _build_dag(*task_specs) -> DAG:
    """Helper: task_specs is list of (task_id, dependencies)."""
    dag = DAG()
    for tid, deps in task_specs:
        dag.add_task(Task(task_id=tid, dependencies=deps))
    return dag


class TestDiffEntry:
    def test_str_added_task(self):
        e = DiffEntry("added_task", "task 'foo' added")
        assert str(e) == "[+] task 'foo' added"

    def test_str_removed_task(self):
        e = DiffEntry("removed_task", "task 'foo' removed")
        assert str(e) == "[-] task 'foo' removed"

    def test_str_added_edge(self):
        e = DiffEntry("added_edge", "edge 'a' -> 'b' added")
        assert str(e) == "[~+] edge 'a' -> 'b' added"

    def test_str_removed_edge(self):
        e = DiffEntry("removed_edge", "edge 'a' -> 'b' removed")
        assert str(e) == "[~-] edge 'a' -> 'b' removed"


class TestDAGDiff:
    def test_no_changes(self):
        diff = DAGDiff()
        assert not diff.has_changes

    def test_has_changes(self):
        diff = DAGDiff(entries=[DiffEntry("added_task", "task 'x' added")])
        assert diff.has_changes

    def test_filter_methods(self):
        diff = DAGDiff(entries=[
            DiffEntry("added_task", "task 'a' added"),
            DiffEntry("removed_task", "task 'b' removed"),
            DiffEntry("added_edge", "edge 'a' -> 'c' added"),
            DiffEntry("removed_edge", "edge 'b' -> 'c' removed"),
        ])
        assert len(diff.added_tasks()) == 1
        assert len(diff.removed_tasks()) == 1
        assert len(diff.added_edges()) == 1
        assert len(diff.removed_edges()) == 1

    def test_summary_contains_count(self):
        diff = DAGDiff(entries=[DiffEntry("added_task", "task 'x' added")])
        assert "1 change" in diff.summary()


class TestDAGDiffer:
    def setup_method(self):
        self.differ = DAGDiffer()

    def test_identical_dags_no_diff(self):
        old = _build_dag(("a", []), ("b", ["a"]))
        new = _build_dag(("a", []), ("b", ["a"]))
        diff = self.differ.compare(old, new)
        assert not diff.has_changes

    def test_added_task_detected(self):
        old = _build_dag(("a", []))
        new = _build_dag(("a", []), ("b", []))
        diff = self.differ.compare(old, new)
        assert len(diff.added_tasks()) == 1
        assert "'b'" in diff.added_tasks()[0].detail

    def test_removed_task_detected(self):
        old = _build_dag(("a", []), ("b", []))
        new = _build_dag(("a", []))
        diff = self.differ.compare(old, new)
        assert len(diff.removed_tasks()) == 1
        assert "'b'" in diff.removed_tasks()[0].detail

    def test_added_edge_detected(self):
        old = _build_dag(("a", []), ("b", []))
        new = _build_dag(("a", []), ("b", ["a"]))
        diff = self.differ.compare(old, new)
        assert len(diff.added_edges()) == 1

    def test_removed_edge_detected(self):
        old = _build_dag(("a", []), ("b", ["a"]))
        new = _build_dag(("a", []), ("b", []))
        diff = self.differ.compare(old, new)
        assert len(diff.removed_edges()) == 1

    def test_empty_dags(self):
        diff = self.differ.compare(DAG(), DAG())
        assert not diff.has_changes
