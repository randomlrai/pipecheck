"""Tests for pipecheck.tagger and pipecheck.commands.tag_cmd."""
from __future__ import annotations
import argparse
import json
import os
import tempfile
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.tagger import TagIndex, DAGTagger
from pipecheck.commands.tag_cmd import add_tag_subparser, tag_command


def _build_dag() -> DAG:
    dag = DAG(name="sample")
    dag.add_task(Task("ingest", metadata={"tags": ["etl", "source"]}))
    dag.add_task(Task("transform", metadata={"tags": ["etl"]}))
    dag.add_task(Task("report", metadata={"tags": ["output"]}))
    dag.add_task(Task("untagged"))
    dag.add_dependency("transform", "ingest")
    dag.add_dependency("report", "transform")
    return dag


# ---------------------------------------------------------------------------
class TestTagIndex:
    def test_add_and_retrieve(self):
        idx = TagIndex()
        idx.add("etl", "ingest")
        idx.add("etl", "transform")
        assert idx.tasks_for_tag("etl") == {"ingest", "transform"}

    def test_missing_tag_returns_empty(self):
        idx = TagIndex()
        assert idx.tasks_for_tag("missing") == frozenset()

    def test_all_tags_sorted(self):
        idx = TagIndex()
        idx.add("z", "t1")
        idx.add("a", "t2")
        assert idx.all_tags() == ["a", "z"]

    def test_len(self):
        idx = TagIndex()
        idx.add("x", "t")
        idx.add("y", "t")
        assert len(idx) == 2


# ---------------------------------------------------------------------------
class TestDAGTagger:
    def setup_method(self):
        self.dag = _build_dag()
        self.tagger = DAGTagger(self.dag)

    def test_index_contains_expected_tags(self):
        assert set(self.tagger.index.all_tags()) == {"etl", "source", "output"}

    def test_tasks_by_tag_returns_task_objects(self):
        tasks = self.tagger.tasks_by_tag("etl")
        ids = {t.task_id for t in tasks}
        assert ids == {"ingest", "transform"}

    def test_filter_dag_sorted(self):
        result = self.tagger.filter_dag("etl")
        assert result == ["ingest", "transform"]

    def test_summary_counts(self):
        s = self.tagger.summary()
        assert s["etl"] == 2
        assert s["source"] == 1
        assert s["output"] == 1

    def test_untagged_task_not_in_index(self):
        for tag in self.tagger.index.all_tags():
            assert "untagged" not in self.tagger.index.tasks_for_tag(tag)


# ---------------------------------------------------------------------------
class TestTagCommand:
    def _write_dag(self, tmp_path: str) -> str:
        data = {
            "name": "test",
            "tasks": [
                {"task_id": "a", "metadata": {"tags": ["etl"]}},
                {"task_id": "b", "metadata": {"tags": ["etl", "output"]}},
            ],
            "dependencies": [],
        }
        path = os.path.join(tmp_path, "dag.json")
        with open(path, "w") as fh:
            json.dump(data, fh)
        return path

    def test_summary_output(self, tmp_path, capsys):
        path = self._write_dag(str(tmp_path))
        args = argparse.Namespace(file=path, filter_tag=None, summary=True)
        rc = tag_command(args)
        out = capsys.readouterr().out
        assert rc == 0
        assert "etl: 2" in out

    def test_filter_output(self, tmp_path, capsys):
        path = self._write_dag(str(tmp_path))
        args = argparse.Namespace(file=path, filter_tag="output", summary=False)
        rc = tag_command(args)
        out = capsys.readouterr().out
        assert rc == 0
        assert "b" in out

    def test_missing_file_returns_1(self, capsys):
        args = argparse.Namespace(file="/no/such/file.json", filter_tag=None, summary=False)
        rc = tag_command(args)
        assert rc == 1
