"""Tests for pipecheck.evolver."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.evolver import DAGEvolver, EvolveEntry, EvolveResult, EvolveError


def _build_dag(name: str, task_ids, edges=None) -> DAG:
    dag = DAG(name=name)
    for tid in task_ids:
        dag.add_task(Task(task_id=tid))
    for src, dst in (edges or []):
        dag.add_edge(src, dst)
    return dag


class TestEvolveEntry:
    def test_str_without_detail(self):
        e = EvolveEntry(kind="added_task", subject="ingest")
        assert str(e) == "[added_task] ingest"

    def test_str_with_detail(self):
        e = EvolveEntry(kind="added_edge", subject="ingest", detail="transform")
        assert str(e) == "[added_edge] ingest -> transform"


class TestEvolveResult:
    def test_has_changes_false_when_empty(self):
        r = EvolveResult(baseline_name="a", current_name="b")
        assert not r.has_changes()

    def test_has_changes_true_when_entries_present(self):
        r = EvolveResult(baseline_name="a", current_name="b",
                         entries=[EvolveEntry(kind="added_task", subject="x")])
        assert r.has_changes()

    def test_count_property(self):
        r = EvolveResult(baseline_name="a", current_name="b",
                         entries=[EvolveEntry(kind="added_task", subject="x"),
                                  EvolveEntry(kind="removed_task", subject="y")])
        assert r.count() == 2

    def test_str_no_changes(self):
        r = EvolveResult(baseline_name="v1", current_name="v2")
        assert "No changes" in str(r)
        assert "v1" in str(r)
        assert "v2" in str(r)

    def test_str_with_changes(self):
        r = EvolveResult(baseline_name="v1", current_name="v2",
                         entries=[EvolveEntry(kind="added_task", subject="load")])
        out = str(r)
        assert "Evolution" in out
        assert "added_task" in out
        assert "load" in out


class TestDAGEvolver:
    def setup_method(self):
        self.evolver = DAGEvolver()

    def test_identical_dags_no_changes(self):
        a = _build_dag("pipe", ["a", "b"], [("a", "b")])
        b = _build_dag("pipe", ["a", "b"], [("a", "b")])
        result = self.evolver.evolve(a, b)
        assert not result.has_changes()

    def test_added_task_detected(self):
        a = _build_dag("pipe", ["a"])
        b = _build_dag("pipe", ["a", "b"])
        result = self.evolver.evolve(a, b)
        kinds = [e.kind for e in result.entries]
        assert "added_task" in kinds
        subjects = [e.subject for e in result.entries]
        assert "b" in subjects

    def test_removed_task_detected(self):
        a = _build_dag("pipe", ["a", "b"])
        b = _build_dag("pipe", ["a"])
        result = self.evolver.evolve(a, b)
        kinds = [e.kind for e in result.entries]
        assert "removed_task" in kinds

    def test_added_edge_detected(self):
        a = _build_dag("pipe", ["a", "b"])
        b = _build_dag("pipe", ["a", "b"], [("a", "b")])
        result = self.evolver.evolve(a, b)
        kinds = [e.kind for e in result.entries]
        assert "added_edge" in kinds

    def test_removed_edge_detected(self):
        a = _build_dag("pipe", ["a", "b"], [("a", "b")])
        b = _build_dag("pipe", ["a", "b"])
        result = self.evolver.evolve(a, b)
        kinds = [e.kind for e in result.entries]
        assert "removed_edge" in kinds

    def test_invalid_input_raises(self):
        with pytest.raises(EvolveError):
            self.evolver.evolve("not_a_dag", _build_dag("x", []))
