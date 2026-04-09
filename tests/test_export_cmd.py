"""Integration tests for the export CLI sub-command."""
from __future__ import annotations

import json
import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipecheck.commands.export_cmd import add_export_subparser, export_command
from pipecheck.reporter import ValidationReport


def _make_args(**kwargs) -> argparse.Namespace:
    defaults = {
        "file": "dag.yaml",
        "fmt": "txt",
        "output": None,
        "strict": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddExportSubparser:
    def test_subparser_registered(self):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        add_export_subparser(subparsers)
        args = parser.parse_args(["export", "mydag.yaml"])
        assert args.file == "mydag.yaml"

    def test_default_format_is_txt(self):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        add_export_subparser(subparsers)
        args = parser.parse_args(["export", "dag.json"])
        assert args.fmt == "txt"

    def test_format_flag(self):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        add_export_subparser(subparsers)
        args = parser.parse_args(["export", "dag.json", "--format", "json"])
        assert args.fmt == "json"

    def test_strict_flag_default_false(self):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        add_export_subparser(subparsers)
        args = parser.parse_args(["export", "dag.json"])
        assert args.strict is False


class TestExportCommand:
    def _mock_dag(self, name="test_dag"):
        dag = MagicMock()
        dag.name = name
        return dag

    def _mock_validator(self, errors=None, warnings=None):
        validator = MagicMock()
        validator.validate.return_value = (errors or [], warnings or [])
        return validator

    @patch("pipecheck.commands.export_cmd.DAGLoader.load_from_file")
    @patch("pipecheck.commands.export_cmd.DAGValidator")
    def test_returns_0_on_clean_dag(self, MockValidator, mock_load):
        mock_load.return_value = self._mock_dag()
        MockValidator.return_value = self._mock_validator()
        args = _make_args(fmt="txt")
        code = export_command(args)
        assert code == 0

    @patch("pipecheck.commands.export_cmd.DAGLoader.load_from_file")
    @patch("pipecheck.commands.export_cmd.DAGValidator")
    def test_returns_1_on_errors(self, MockValidator, mock_load):
        mock_load.return_value = self._mock_dag()
        MockValidator.return_value = self._mock_validator(
            errors=[("E001", "bad dep", "task_a")]
        )
        args = _make_args(fmt="txt")
        code = export_command(args)
        assert code == 1

    @patch("pipecheck.commands.export_cmd.DAGLoader.load_from_file")
    def test_returns_1_on_load_failure(self, mock_load):
        mock_load.side_effect = FileNotFoundError("not found")
        args = _make_args()
        code = export_command(args)
        assert code == 1

    @patch("pipecheck.commands.export_cmd.DAGLoader.load_from_file")
    @patch("pipecheck.commands.export_cmd.DAGValidator")
    def test_strict_returns_1_on_warnings(self, MockValidator, mock_load):
        mock_load.return_value = self._mock_dag()
        MockValidator.return_value = self._mock_validator(
            warnings=[("W001", "high timeout", "task_b")]
        )
        args = _make_args(fmt="txt", strict=True)
        code = export_command(args)
        assert code == 1

    @patch("pipecheck.commands.export_cmd.DAGLoader.load_from_file")
    @patch("pipecheck.commands.export_cmd.DAGValidator")
    def test_json_output_to_file(self, MockValidator, mock_load, tmp_path):
        mock_load.return_value = self._mock_dag()
        MockValidator.return_value = self._mock_validator()
        dest = tmp_path / "out.json"
        args = _make_args(fmt="json", output=str(dest))
        code = export_command(args)
        assert code == 0
        assert dest.exists()
        data = json.loads(dest.read_text())
        assert data["dag"] == "test_dag"
