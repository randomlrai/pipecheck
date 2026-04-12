"""Tests for pipecheck.bouncer."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.bouncer import BounceEntry, BounceResult, BounceError, DAGBouncer


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task("extract", metadata={"tags": ["io", "source"]}))
    dag.add_task(Task("transform", metadata={"tags": ["compute"]}))
    dag.add_task(Task("load", metadata={"tags": ["io", "sink"]}))
    dag.add_task(Task("notify", metadata={"tags": ["ops"]}))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    dag.add_edge("load", "notify")
    return dag


class TestBounceEntry:
    def test_str_allowed_no_reason(self):
        e = BounceEntry("my_task", allowed=True)
        assert str(e) == "[ALLOW] my_task"

    def test_str_denied_no_reason(self):
        e = BounceEntry("my_task", allowed=False)
        assert str(e) == "[DENY] my_task"

    def test_str_with_reason(self):
        e = BounceEntry("my_task", allowed=False, reason="tag in deny list")
        assert "tag in deny list" in str(e)
        assert "[DENY]" in str(e)


class TestBounceResult:
    def test_has_denials_false_when_all_allowed(self):
        r = BounceResult(dag_name="d")
        r.entries.append(BounceEntry("a", allowed=True))
        assert not r.has_denials

    def test_has_denials_true_when_any_denied(self):
        r = BounceResult(dag_name="d")
        r.entries.append(BounceEntry("a", allowed=True))
        r.entries.append(BounceEntry("b", allowed=False))
        assert r.has_denials

    def test_denied_property_filters_correctly(self):
        r = BounceResult(dag_name="d")
        r.entries.append(BounceEntry("a", allowed=True))
        r.entries.append(BounceEntry("b", allowed=False))
        assert len(r.denied) == 1
        assert r.denied[0].task_id == "b"

    def test_allowed_property_filters_correctly(self):
        r = BounceResult(dag_name="d")
        r.entries.append(BounceEntry("a", allowed=True))
        r.entries.append(BounceEntry("b", allowed=False))
        assert len(r.allowed) == 1
        assert r.allowed[0].task_id == "a"

    def test_str_contains_dag_name(self):
        r = BounceResult(dag_name="my_dag")
        assert "my_dag" in str(r)


class TestDAGBouncer:
    def setup_method(self):
        self.dag = _build_dag()
        self.bouncer = DAGBouncer(self.dag)

    def test_no_rules_allows_all(self):
        result = self.bouncer.evaluate()
        assert not result.has_denials
        assert len(result.entries) == 4

    def test_deny_tag_blocks_matching_tasks(self):
        result = self.bouncer.evaluate(deny_tags=["io"])
        denied_ids = {e.task_id for e in result.denied}
        assert "extract" in denied_ids
        assert "load" in denied_ids
        assert "transform" not in denied_ids

    def test_allow_tag_restricts_to_matching_tasks(self):
        result = self.bouncer.evaluate(allow_tags=["compute"])
        allowed_ids = {e.task_id for e in result.allowed}
        assert allowed_ids == {"transform"}

    def test_deny_prefix_blocks_matching_tasks(self):
        result = self.bouncer.evaluate(deny_prefixes=["not"])
        denied_ids = {e.task_id for e in result.denied}
        assert "notify" in denied_ids

    def test_allow_prefix_restricts_to_matching_tasks(self):
        result = self.bouncer.evaluate(allow_prefixes=["ext", "load"])
        allowed_ids = {e.task_id for e in result.allowed}
        assert "extract" in allowed_ids
        assert "load" in allowed_ids
        assert "transform" not in allowed_ids

    def test_dag_name_stored_in_result(self):
        result = self.bouncer.evaluate()
        assert result.dag_name == "test_dag"
