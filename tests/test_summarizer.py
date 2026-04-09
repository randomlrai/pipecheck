"""Tests for pipecheck.summarizer and summarize_cmd."""
import json
import types
from unittest.mock import MagicMock, patch

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.summarizer import DAGSummary, DAGSummarizer


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task(task_id="ingest", name="Ingest", dependencies=[]))
    dag.add_task(Task(task_id="clean", name="Clean", dependencies=["ingest"]))
    dag.add_task(Task(task_id="transform", name="Transform", dependencies=["clean"]))
    dag.add_task(Task(task_id="load", name="Load", dependencies=["transform"]))
    return dag


class TestDAGSummary:
    def test_str_contains_dag_name(self):
        s = DAGSummary(
            dag_name="my_dag", task_count=2, edge_count=1,
            root_tasks=["a"], leaf_tasks=["b"], max_depth=1,
        )
        assert "my_dag" in str(s)

    def test_str_contains_counts(self):
        s = DAGSummary(
            dag_name="d", task_count=4, edge_count=3,
            root_tasks=["a"], leaf_tasks=["d"], max_depth=3,
        )
        assert "4" in str(s)
        assert "3" in str(s)

    def test_str_shows_tags_when_present(self):
        s = DAGSummary(
            dag_name="d", task_count=1, edge_count=0,
            root_tasks=["a"], leaf_tasks=["a"], max_depth=0,
            tags_used=["etl", "prod"],
        )
        assert "etl" in str(s)

    def test_str_omits_tags_when_empty(self):
        s = DAGSummary(
            dag_name="d", task_count=1, edge_count=0,
            root_tasks=["a"], leaf_tasks=["a"], max_depth=0,
        )
        assert "Tags" not in str(s)


class TestDAGSummarizer:
    def test_task_count(self):
        dag = _build_dag()
        summary = DAGSummarizer(dag).summarize()
        assert summary.task_count == 4

    def test_edge_count(self):
        dag = _build_dag()
        summary = DAGSummarizer(dag).summarize()
        assert summary.edge_count == 3

    def test_root_tasks(self):
        dag = _build_dag()
        summary = DAGSummarizer(dag).summarize()
        assert summary.root_tasks == ["ingest"]

    def test_leaf_tasks(self):
        dag = _build_dag()
        summary = DAGSummarizer(dag).summarize()
        assert summary.leaf_tasks == ["load"]

    def test_max_depth(self):
        dag = _build_dag()
        summary = DAGSummarizer(dag).summarize()
        assert summary.max_depth == 3

    def test_tags_collected_from_metadata(self):
        dag = DAG(name="tagged")
        dag.add_task(Task(task_id="a", name="A", dependencies=[], metadata={"tags": ["etl"]}))
        dag.add_task(Task(task_id="b", name="B", dependencies=["a"], metadata={"tags": ["prod"]}))
        summary = DAGSummarizer(dag).summarize()
        assert set(summary.tags_used) == {"etl", "prod"}

    def test_empty_dag(self):
        dag = DAG(name="empty")
        summary = DAGSummarizer(dag).summarize()
        assert summary.task_count == 0
        assert summary.edge_count == 0
        assert summary.max_depth == 0
        assert summary.root_tasks == []
        assert summary.leaf_tasks == []
