"""Tests for pipecheck.cataloger and the catalog CLI command."""
from __future__ import annotations

import json
import os
import tempfile

import pytest

from pipecheck.cataloger import CatalogEntry, CatalogResult, DAGCataloger, CatalogError
from pipecheck.dag import DAG, Task


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task(task_id="ingest", metadata={"description": "Load raw data", "tags": ["io", "extract"]}))
    dag.add_task(Task(task_id="transform", metadata={"description": "Clean data", "tags": ["compute"]}))
    dag.add_task(Task(task_id="export", metadata={"tags": ["io", "load"]}))
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "export")
    return dag


class TestCatalogEntry:
    def test_str_no_description(self):
        entry = CatalogEntry(task_id="t1", description=None, tags=[], upstream=[], downstream=[])
        text = str(entry)
        assert "[t1]" in text
        assert "(none)" in text

    def test_str_with_description(self):
        entry = CatalogEntry(task_id="t1", description="My task", tags=["io"], upstream=["t0"], downstream=[])
        text = str(entry)
        assert "My task" in text
        assert "io" in text
        assert "t0" in text


class TestCatalogResult:
    def test_count_property(self):
        result = CatalogResult(dag_name="d")
        assert result.count == 0
        result.entries["a"] = CatalogEntry("a", None, [], [], [])
        assert result.count == 1

    def test_get_existing(self):
        result = CatalogResult(dag_name="d")
        entry = CatalogEntry("a", None, [], [], [])
        result.entries["a"] = entry
        assert result.get("a") is entry

    def test_get_missing_returns_none(self):
        result = CatalogResult(dag_name="d")
        assert result.get("missing") is None

    def test_str_contains_dag_name(self):
        result = CatalogResult(dag_name="my_pipeline")
        assert "my_pipeline" in str(result)

    def test_str_contains_task_ids(self):
        dag = _build_dag()
        result = DAGCataloger().catalog(dag)
        text = str(result)
        assert "ingest" in text
        assert "transform" in text
        assert "export" in text


class TestDAGCataloger:
    def test_catalog_none_raises(self):
        with pytest.raises(CatalogError):
            DAGCataloger().catalog(None)  # type: ignore

    def test_catalog_populates_entries(self):
        dag = _build_dag()
        result = DAGCataloger().catalog(dag)
        assert result.count == 3

    def test_upstream_downstream_correct(self):
        dag = _build_dag()
        result = DAGCataloger().catalog(dag)
        transform_entry = result.get("transform")
        assert transform_entry is not None
        assert "ingest" in transform_entry.upstream
        assert "export" in transform_entry.downstream

    def test_tags_sorted(self):
        dag = _build_dag()
        result = DAGCataloger().catalog(dag)
        ingest_entry = result.get("ingest")
        assert ingest_entry is not None
        assert ingest_entry.tags == sorted(ingest_entry.tags)

    def test_task_with_no_metadata(self):
        dag = DAG(name="bare")
        dag.add_task(Task(task_id="solo"))
        result = DAGCataloger().catalog(dag)
        entry = result.get("solo")
        assert entry is not None
        assert entry.description is None
        assert entry.tags == []
