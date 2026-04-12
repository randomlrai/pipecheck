"""Tests for pipecheck.blocker and pipecheck.commands.block_cmd."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from unittest.mock import patch

import pytest

from pipecheck.blocker import BlockEntry, BlockError, BlockResult, DAGBlocker
from pipecheck.dag import DAG, Task


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    for tid in ["extract", "transform", "load", "notify"]:
        dag.add_task(Task(task_id=tid))
    dag.add_edge("transform", "extract")
    dag.add_edge("load", "transform")
    dag.add_edge("notify", "load")
    return dag


class TestBlockEntry:
    def test_str_without_optional_fields(self):
        entry = BlockEntry(task_id="extract")
        assert str(entry) == "BLOCKED: extract"

    def test_str_with_reason(self):
        entry = BlockEntry(task_id="extract", reason="upstream failure")
        assert "upstream failure" in str(entry)
        assert "BLOCKED: extract" in str(entry)

    def test_str_with_blocked_by(self):
        entry = BlockEntry(task_id="transform", blocked_by="extract")
        assert "cascades from extract" in str(entry)

    def test_str_with_all_fields(self):
        entry = BlockEntry(task_id="load", reason="frozen", blocked_by="transform")
        result = str(entry)
        assert "BLOCKED: load" in result
        assert "cascades from transform" in result
        assert "frozen" in result


class TestBlockResult:
    def test_has_blocks_false_when_empty(self):
        result = BlockResult(dag_name="pipe")
        assert result.has_blocks() is False

    def test_has_blocks_true_when_populated(self):
        result = BlockResult(dag_name="pipe", entries=[BlockEntry(task_id="t1")])
        assert result.has_blocks() is True

    def test_count_property(self):
        result = BlockResult(
            dag_name="pipe",
            entries=[BlockEntry(task_id="t1"), BlockEntry(task_id="t2")],
        )
        assert result.count == 2

    def test_str_no_blocks(self):
        result = BlockResult(dag_name="pipe")
        assert "No blocked tasks" in str(result)

    def test_str_with_blocks(self):
        result = BlockResult(
            dag_name="pipe",
            entries=[BlockEntry(task_id="extract")],
        )
        text = str(result)
        assert "Blocked tasks" in text
        assert "extract" in text


class TestDAGBlocker:
    def test_block_single_task(self):
        dag = _build_dag()
        blocker = DAGBlocker(dag)
        result = blocker.block(["extract"])
        assert result.count == 1
        assert result.entries[0].task_id == "extract"

    def test_block_unknown_task_raises(self):
        dag = _build_dag()
        blocker = DAGBlocker(dag)
        with pytest.raises(BlockError):
            blocker.block(["nonexistent"])

    def test_block_with_reason(self):
        dag = _build_dag()
        blocker = DAGBlocker(dag)
        result = blocker.block(["extract"], reason="maintenance")
        assert result.entries[0].reason == "maintenance"

    def test_cascade_blocks_downstream(self):
        dag = _build_dag()
        blocker = DAGBlocker(dag)
        result = blocker.block(["extract"], cascade=True)
        task_ids = {e.task_id for e in result.entries}
        # extract -> transform -> load -> notify should all be blocked
        assert "transform" in task_ids
        assert "load" in task_ids
        assert "notify" in task_ids

    def test_cascade_sets_blocked_by(self):
        dag = _build_dag()
        blocker = DAGBlocker(dag)
        result = blocker.block(["extract"], cascade=True)
        cascaded = [e for e in result.entries if e.blocked_by is not None]
        assert len(cascaded) > 0

    def test_no_cascade_only_direct(self):
        dag = _build_dag()
        blocker = DAGBlocker(dag)
        result = blocker.block(["extract"], cascade=False)
        assert result.count == 1
