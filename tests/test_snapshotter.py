"""Tests for pipecheck.snapshotter and the snapshot CLI command."""

from __future__ import annotations

import json
import os
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.snapshotter import DAGSnapshot, SnapshotError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    t1 = Task(task_id="ingest", description="Ingest raw data", timeout=30)
    t2 = Task(task_id="transform", dependencies={"ingest"}, timeout=60)
    t3 = Task(task_id="export", dependencies={"transform"})
    for t in (t1, t2, t3):
        dag.add_task(t)
    return dag


# ---------------------------------------------------------------------------
# DAGSnapshot unit tests
# ---------------------------------------------------------------------------

class TestDAGSnapshot:
    def test_creation_sets_label_from_dag_name(self):
        dag = _build_dag()
        snap = DAGSnapshot(dag)
        assert snap.label == "test_dag"

    def test_creation_accepts_custom_label(self):
        dag = _build_dag()
        snap = DAGSnapshot(dag, label="release-1.0")
        assert snap.label == "release-1.0"

    def test_to_dict_structure(self):
        dag = _build_dag()
        snap = DAGSnapshot(dag)
        d = snap.to_dict()
        assert d["version"] == DAGSnapshot.VERSION
        assert d["dag"]["name"] == "test_dag"
        assert len(d["dag"]["tasks"]) == 3

    def test_to_dict_task_fields(self):
        dag = _build_dag()
        snap = DAGSnapshot(dag)
        task_map = {t["id"]: t for t in snap.to_dict()["dag"]["tasks"]}
        assert task_map["ingest"]["timeout"] == 30
        assert task_map["transform"]["dependencies"] == ["ingest"]

    def test_save_and_load_roundtrip(self, tmp_path):
        dag = _build_dag()
        snap = DAGSnapshot(dag, label="v1")
        path = str(tmp_path / "snap.json")
        snap.save(path)

        loaded = DAGSnapshot.load(path)
        assert loaded.label == "v1"
        assert loaded.dag.name == "test_dag"
        assert set(loaded.dag.tasks.keys()) == {"ingest", "transform", "export"}

    def test_load_preserves_dependencies(self, tmp_path):
        dag = _build_dag()
        path = str(tmp_path / "snap.json")
        DAGSnapshot(dag).save(path)
        loaded = DAGSnapshot.load(path)
        assert "ingest" in loaded.dag.tasks["transform"].dependencies

    def test_save_raises_snapshot_error_on_bad_path(self):
        dag = _build_dag()
        snap = DAGSnapshot(dag)
        with pytest.raises(SnapshotError):
            snap.save("/nonexistent_dir/snap.json")

    def test_load_raises_snapshot_error_if_missing(self):
        with pytest.raises(SnapshotError, match="not found"):
            DAGSnapshot.load("/no/such/file.json")

    def test_load_raises_snapshot_error_on_invalid_json(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json", encoding="utf-8")
        with pytest.raises(SnapshotError):
            DAGSnapshot.load(str(bad))

    def test_created_at_is_iso_format(self):
        dag = _build_dag()
        snap = DAGSnapshot(dag)
        # Should not raise
        from datetime import datetime
        datetime.fromisoformat(snap.created_at)
