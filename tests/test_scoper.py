"""Tests for pipecheck.scoper."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.scoper import DAGScoper, ScopeError, ScopeResult


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task("extract", metadata={"scope": "ingestion"}))
    dag.add_task(Task("transform", metadata={"scope": "processing"}))
    dag.add_task(Task("load", metadata={"scope": "ingestion"}))
    dag.add_task(Task("report", metadata={}))
    dag.add_dependency("extract", "transform")
    dag.add_dependency("transform", "load")
    return dag


class TestScopeResult:
    def test_count_property(self):
        r = ScopeResult(dag_name="d", scope_name="s", tasks=[])
        assert r.count == 0

    def test_has_tasks_false_when_empty(self):
        r = ScopeResult(dag_name="d", scope_name="s", tasks=[])
        assert not r.has_tasks

    def test_has_tasks_true_when_populated(self):
        t = Task("a", metadata={"scope": "s"})
        r = ScopeResult(dag_name="d", scope_name="s", tasks=[t])
        assert r.has_tasks

    def test_str_no_tasks(self):
        r = ScopeResult(dag_name="my_dag", scope_name="alpha")
        assert "alpha" in str(r)
        assert "no tasks matched" in str(r)

    def test_str_with_tasks(self):
        tasks = [Task("a", metadata={}), Task("b", metadata={})]
        r = ScopeResult(dag_name="my_dag", scope_name="beta", tasks=tasks)
        s = str(r)
        assert "beta" in s
        assert "2 task(s)" in s
        assert "a" in s and "b" in s


class TestDAGScoper:
    def test_scope_returns_matching_tasks(self):
        dag = _build_dag()
        scoper = DAGScoper(dag)
        result = scoper.scope("ingestion")
        ids = {t.task_id for t in result.tasks}
        assert ids == {"extract", "load"}

    def test_scope_returns_empty_for_unknown_scope(self):
        dag = _build_dag()
        scoper = DAGScoper(dag)
        result = scoper.scope("nonexistent")
        assert result.count == 0
        assert not result.has_tasks

    def test_scope_stores_dag_name(self):
        dag = _build_dag()
        scoper = DAGScoper(dag)
        result = scoper.scope("processing")
        assert result.dag_name == "test_dag"

    def test_scope_raises_on_empty_name(self):
        dag = _build_dag()
        scoper = DAGScoper(dag)
        with pytest.raises(ScopeError):
            scoper.scope("")

    def test_scope_raises_on_whitespace_name(self):
        dag = _build_dag()
        scoper = DAGScoper(dag)
        with pytest.raises(ScopeError):
            scoper.scope("   ")

    def test_all_scopes_returns_sorted(self):
        dag = _build_dag()
        scoper = DAGScoper(dag)
        scopes = scoper.all_scopes()
        assert scopes == ["ingestion", "processing"]

    def test_all_scopes_excludes_tasks_without_scope(self):
        dag = _build_dag()
        scoper = DAGScoper(dag)
        scopes = scoper.all_scopes()
        assert "" not in scopes
        assert len(scopes) == 2

    def test_all_scopes_empty_dag(self):
        dag = DAG(name="empty")
        scoper = DAGScoper(dag)
        assert scoper.all_scopes() == []
