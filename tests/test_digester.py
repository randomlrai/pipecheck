"""Tests for pipecheck.digester."""
from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.digester import DAGDigester, DigestError, DigestResult


def _build_dag(name: str = "pipeline") -> DAG:
    dag = DAG(name=name)
    t1 = Task(task_id="ingest", retries=1)
    t2 = Task(task_id="transform", dependencies=["ingest"])
    t3 = Task(task_id="load", dependencies=["transform"])
    dag.add_task(t1)
    dag.add_task(t2)
    dag.add_task(t3)
    return dag


class TestDigestResult:
    def test_str_contains_dag_name(self):
        r = DigestResult(dag_name="my_dag", fingerprint="abc123", task_count=3, edge_count=2)
        assert "my_dag" in str(r)

    def test_str_contains_fingerprint(self):
        r = DigestResult(dag_name="d", fingerprint="deadbeef", task_count=1, edge_count=0)
        assert "deadbeef" in str(r)

    def test_short_returns_prefix(self):
        r = DigestResult(dag_name="d", fingerprint="abcdef123456789", task_count=1, edge_count=0)
        assert r.short(8) == "abcdef12"

    def test_short_default_length_is_12(self):
        r = DigestResult(dag_name="d", fingerprint="a" * 64, task_count=1, edge_count=0)
        assert len(r.short()) == 12

    def test_matches_same_fingerprint(self):
        r1 = DigestResult(dag_name="d", fingerprint="abc", task_count=1, edge_count=0)
        r2 = DigestResult(dag_name="other", fingerprint="abc", task_count=1, edge_count=0)
        assert r1.matches(r2) is True

    def test_matches_different_fingerprint(self):
        r1 = DigestResult(dag_name="d", fingerprint="abc", task_count=1, edge_count=0)
        r2 = DigestResult(dag_name="d", fingerprint="xyz", task_count=1, edge_count=0)
        assert r1.matches(r2) is False


class TestDAGDigester:
    def test_digest_returns_result(self):
        dag = _build_dag()
        result = DAGDigester().digest(dag)
        assert isinstance(result, DigestResult)

    def test_digest_stores_dag_name(self):
        dag = _build_dag("my_pipeline")
        result = DAGDigester().digest(dag)
        assert result.dag_name == "my_pipeline"

    def test_digest_task_and_edge_counts(self):
        dag = _build_dag()
        result = DAGDigester().digest(dag)
        assert result.task_count == 3
        assert result.edge_count == 2

    def test_digest_is_deterministic(self):
        dag = _build_dag()
        r1 = DAGDigester().digest(dag)
        r2 = DAGDigester().digest(dag)
        assert r1.fingerprint == r2.fingerprint

    def test_different_dags_produce_different_fingerprints(self):
        dag1 = _build_dag("pipeline_a")
        dag2 = _build_dag("pipeline_b")
        r1 = DAGDigester().digest(dag1)
        r2 = DAGDigester().digest(dag2)
        assert r1.fingerprint != r2.fingerprint

    def test_adding_task_changes_fingerprint(self):
        dag = _build_dag()
        r1 = DAGDigester().digest(dag)
        dag.add_task(Task(task_id="extra"))
        r2 = DAGDigester().digest(dag)
        assert r1.fingerprint != r2.fingerprint

    def test_algorithm_stored_in_result(self):
        dag = _build_dag()
        result = DAGDigester(algorithm="md5").digest(dag)
        assert result.algorithm == "md5"

    def test_invalid_algorithm_raises(self):
        with pytest.raises(DigestError):
            DAGDigester(algorithm="not_a_real_algo")
