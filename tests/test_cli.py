"""Tests for CLI interface."""

import pytest
from pathlib import Path
from pipecheck.cli import create_parser, main, validate_command


class TestCLIParser:
    """Test suite for CLI argument parsing."""

    def test_parser_creation(self):
        """Test that parser is created successfully."""
        parser = create_parser()
        assert parser is not None
        assert parser.prog == "pipecheck"

    def test_validate_command_parsing(self):
        """Test parsing of validate command."""
        parser = create_parser()
        args = parser.parse_args(["validate", "test.yaml"])
        
        assert args.command == "validate"
        assert args.dag_file == "test.yaml"
        assert args.strict is False

    def test_validate_command_strict_flag(self):
        """Test parsing of validate command with strict flag."""
        parser = create_parser()
        args = parser.parse_args(["validate", "test.yaml", "--strict"])
        
        assert args.command == "validate"
        assert args.strict is True

    def test_no_command(self):
        """Test parsing with no command."""
        parser = create_parser()
        args = parser.parse_args([])
        
        assert args.command is None


class TestValidateCommand:
    """Test suite for validate command execution."""

    def test_validate_nonexistent_file(self):
        """Test validation with non-existent file."""
        result = validate_command("nonexistent.yaml")
        assert result == 1

    def test_validate_existing_file(self, tmp_path):
        """Test validation with existing file."""
        dag_file = tmp_path / "test_dag.yaml"
        dag_file.write_text("# Empty DAG file")
        
        result = validate_command(str(dag_file))
        assert result == 0

    def test_validate_strict_mode(self, tmp_path):
        """Test validation in strict mode."""
        dag_file = tmp_path / "test_dag.yaml"
        dag_file.write_text("# Empty DAG file")
        
        result = validate_command(str(dag_file), strict=True)
        assert result in [0, 1]


class TestMainFunction:
    """Test suite for main entry point."""

    def test_main_no_args(self):
        """Test main function with no arguments."""
        result = main([])
        assert result == 0

    def test_main_with_validate(self, tmp_path):
        """Test main function with validate command."""
        dag_file = tmp_path / "test_dag.yaml"
        dag_file.write_text("# Empty DAG file")
        
        result = main(["validate", str(dag_file)])
        assert result == 0

    def test_main_with_nonexistent_file(self):
        """Test main function with non-existent file."""
        result = main(["validate", "nonexistent.yaml"])
        assert result == 1
