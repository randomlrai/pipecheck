"""Tests for pipecheck.freezer."""

from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.freezer import DAGFreezer, FreezeError, FrozenDAG


def _build_dag(name: str = "pipeline") -> DAG:
    dag = DAG(name=name)
    dag.add_task(Task(task_id="extract", command="run extract"))
    dag.add_task(Task(task_id="load", command="run load", dependencies=["extract"]))
    return dag


class TestFrozenDAG:
    def test_str_contains_dag_name(self):
        dag = _build_dag()
        frozen = FrozenDAG(dag=dag)
        assert "pipeline" in str(frozen)

    def test_str_contains_frozen_at(self):
        dag = _build_dag()
        frozen = FrozenDAG(dag=dag)
        assert "frozen_at" in str(frozen)

    def test_str_contains_frozen_by_when_set(self):
        dag = _build_dag()
        frozen = FrozenDAG(dag=dag, frozen_by="alice")
        assert "alice" in str(frozen)

    def test_str_omits_frozen_by_when_none(self):
        dag = _build_dag()
        frozen = FrozenDAG(dag=dag)
        assert "frozen_by" not in str(frozen)

    def test_to_dict_structure(self):
        dag = _build_dag()
        frozen = FrozenDAG(dag=dag, reason="release", frozen_by="bob")
        d = frozen.to_dict()
        assert d["dag_name"] == "pipeline"
        assert d["task_count"] == 2
        assert d["reason"] == "release"
        assert d["frozen_by"] == "bob"
        assert "frozen_at" in d


class TestDAGFreezer:
    def setup_method(self):
        self.freezer = DAGFreezer()
        self.dag = _build_dag()

    def test_freeze_returns_frozen_dag(self):
        result = self.freezer.freeze(self.dag)
        assert isinstance(result, FrozenDAG)
        assert result.dag is self.dag

    def test_is_frozen_after_freeze(self):
        self.freezer.freeze(self.dag)
        assert self.freezer.is_frozen("pipeline") is True

    def test_is_not_frozen_before_freeze(self):
        assert self.freezer.is_frozen("pipeline") is False

    def test_freeze_same_dag_twice_raises(self):
        self.freezer.freeze(self.dag)
        with pytest.raises(FreezeError, match="already frozen"):
            self.freezer.freeze(self.dag)

    def test_unfreeze_removes_record(self):
        self.freezer.freeze(self.dag)
        self.freezer.unfreeze("pipeline")
        assert self.freezer.is_frozen("pipeline") is False

    def test_unfreeze_unknown_dag_raises(self):
        with pytest.raises(FreezeError, match="not frozen"):
            self.freezer.unfreeze("ghost")

    def test_get_returns_none_for_unknown(self):
        assert self.freezer.get("missing") is None

    def test_get_returns_frozen_dag(self):
        self.freezer.freeze(self.dag, reason="hotfix")
        record = self.freezer.get("pipeline")
        assert record is not None
        assert record.reason == "hotfix"

    def test_all_frozen_sorted_by_name(self):
        dag_a = _build_dag("alpha")
        dag_b = _build_dag("beta")
        self.freezer.freeze(dag_b)
        self.freezer.freeze(dag_a)
        names = [f.dag.name for f in self.freezer.all_frozen()]
        assert names == ["alpha", "beta"]
