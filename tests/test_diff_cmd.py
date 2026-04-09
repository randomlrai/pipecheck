"""Tests for pipecheck.commands.diff_cmd."""
import argparse
from unittest.mock import patch, MagicMock
import pytest
from pipecheck.commands.diff_cmd import add_diff_subparser, diff_command
from pipecheck.dag import DAG, Task
from pipecheck.differ import DAGDiff, DiffEntry


def _make_args(**kwargs) -> argparse.Namespace:
    defaults = {"old": "old.yaml", "new": "new.yaml", "exit_code": False}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddDiffSubparser:
    def _make_subparsers(self):
        parser = argparse.ArgumentParser()
        return parser, parser.add_subparsers()

    def test_subparser_registered(self):
        parser, subparsers = self._make_subparsers()
        add_diff_subparser(subparsers)
        args = parser.parse_args(["diff", "a.yaml", "b.yaml"])
        assert args.old == "a.yaml"
        assert args.new == "b.yaml"

    def test_exit_code_flag_default_false(self):
        parser, subparsers = self._make_subparsers()
        add_diff_subparser(subparsers)
        args = parser.parse_args(["diff", "a.yaml", "b.yaml"])
        assert args.exit_code is False

    def test_exit_code_flag_set(self):
        parser, subparsers = self._make_subparsers()
        add_diff_subparser(subparsers)
        args = parser.parse_args(["diff", "a.yaml", "b.yaml", "--exit-code"])
        assert args.exit_code is True

    def test_func_is_diff_command(self):
        parser, subparsers = self._make_subparsers()
        add_diff_subparser(subparsers)
        args = parser.parse_args(["diff", "a.yaml", "b.yaml"])
        assert args.func is diff_command


class TestDiffCommand:
    def _dag_with_tasks(self, *task_ids):
        dag = DAG()
        for tid in task_ids:
            dag.add_task(Task(task_id=tid, dependencies=[]))
        return dag

    def test_no_differences_returns_zero(self, capsys):
        dag = self._dag_with_tasks("a", "b")
        with patch("pipecheck.commands.diff_cmd.DAGLoader") as MockLoader:
            MockLoader.return_value.load_from_file.return_value = dag
            code = diff_command(_make_args())
        assert code == 0
        out = capsys.readouterr().out
        assert "No differences" in out

    def test_differences_printed(self, capsys):
        old = self._dag_with_tasks("a")
        new = self._dag_with_tasks("a", "b")
        with patch("pipecheck.commands.diff_cmd.DAGLoader") as MockLoader:
            MockLoader.return_value.load_from_file.side_effect = [old, new]
            code = diff_command(_make_args())
        out = capsys.readouterr().out
        assert "change" in out

    def test_exit_code_flag_returns_one_on_diff(self):
        old = self._dag_with_tasks("a")
        new = self._dag_with_tasks("a", "b")
        with patch("pipecheck.commands.diff_cmd.DAGLoader") as MockLoader:
            MockLoader.return_value.load_from_file.side_effect = [old, new]
            code = diff_command(_make_args(exit_code=True))
        assert code == 1

    def test_format_error_returns_two(self, capsys):
        from pipecheck.formats import FormatError
        with patch("pipecheck.commands.diff_cmd.DAGLoader") as MockLoader:
            MockLoader.return_value.load_from_file.side_effect = FormatError("bad")
            code = diff_command(_make_args())
        assert code == 2

    def test_file_not_found_returns_two(self, capsys):
        with patch("pipecheck.commands.diff_cmd.DAGLoader") as MockLoader:
            MockLoader.return_value.load_from_file.side_effect = FileNotFoundError("missing")
            code = diff_command(_make_args())
        assert code == 2
