"""Integration tests: DAGGrouper round-trips through the CLI subcommand path."""
import json
import os
import tempfile

from pipecheck.commands.group_cmd import group_command
from pipecheck.dag import DAG, Task
from pipecheck.grouper import DAGGrouper

import argparse


def _write_complex_dag(path: str) -> None:
    data = {
        "name": "complex",
        "tasks": [
            {"task_id": "ingest_raw", "metadata": {"tags": ["ingest"]}},
            {"task_id": "ingest_clean", "metadata": {"tags": ["ingest"]}},
            {"task_id": "transform_norm", "metadata": {"tags": ["transform"]}},
            {"task_id": "load_db", "metadata": {"tags": ["load"]}},
            {"task_id": "standalone", "metadata": {}},
        ],
        "edges": [
            {"from": "ingest_raw", "to": "transform_norm"},
            {"from": "ingest_clean", "to": "transform_norm"},
            {"from": "transform_norm", "to": "load_db"},
        ],
    }
    with open(path, "w") as f:
        json.dump(data, f)


class TestGrouperIntegration:
    def test_by_tag_groups_match_direct_grouper(self):
        dag = DAG(name="direct")
        dag.add_task(Task("ingest_raw", metadata={"tags": ["ingest"]}))
        dag.add_task(Task("ingest_clean", metadata={"tags": ["ingest"]}))
        dag.add_task(Task("transform_norm", metadata={"tags": ["transform"]}))
        grouper = DAGGrouper()
        result = grouper.by_tag(dag)
        assert len(result.groups["ingest"]) == 2
        assert len(result.groups["transform"]) == 1

    def test_by_prefix_groups_match_direct_grouper(self):
        dag = DAG(name="direct")
        dag.add_task(Task("ingest_raw"))
        dag.add_task(Task("ingest_clean"))
        dag.add_task(Task("transform_norm"))
        grouper = DAGGrouper()
        result = grouper.by_prefix(dag)
        assert "ingest" in result.groups
        assert len(result.groups["ingest"]) == 2

    def test_cli_command_by_tag_exit_zero(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            _write_complex_dag(f.name)
            name = f.name
        try:
            args = argparse.Namespace(dag_file=name, by="tag", separator="_", func=group_command)
            rc = group_command(args)
            assert rc == 0
        finally:
            os.unlink(name)

    def test_cli_command_by_prefix_exit_zero(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            _write_complex_dag(f.name)
            name = f.name
        try:
            args = argparse.Namespace(dag_file=name, by="prefix", separator="_", func=group_command)
            rc = group_command(args)
            assert rc == 0
        finally:
            os.unlink(name)

    def test_ungrouped_tasks_present_in_result(self):
        dag = DAG(name="u")
        dag.add_task(Task("standalone", metadata={}))
        grouper = DAGGrouper()
        result = grouper.by_tag(dag)
        assert any(t.task_id == "standalone" for t in result.ungrouped)
