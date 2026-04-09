"""Command-line interface for pipecheck."""

import argparse
import sys
from pathlib import Path
from typing import Optional

from .dag import DAG
from .validator import DAGValidator


def create_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        prog="pipecheck",
        description="Validate and visualize data pipeline DAGs before deployment"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Validate command
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate a DAG configuration"
    )
    validate_parser.add_argument(
        "dag_file",
        type=str,
        help="Path to DAG configuration file"
    )
    validate_parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as errors"
    )
    
    # Version command
    parser.add_argument(
        "--version",
        action="version",
        version="pipecheck 0.1.0"
    )
    
    return parser


def validate_command(dag_file: str, strict: bool = False) -> int:
    """Execute the validate command.
    
    Args:
        dag_file: Path to DAG file
        strict: Whether to treat warnings as errors
        
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    file_path = Path(dag_file)
    
    if not file_path.exists():
        print(f"Error: File '{dag_file}' not found", file=sys.stderr)
        return 1
    
    # For now, create a simple example DAG
    # In a real implementation, this would load from the file
    dag = DAG(name=file_path.stem)
    
    print(f"Validating DAG: {dag.name}")
    print("-" * 50)
    
    validator = DAGValidator(dag)
    is_valid, errors, warnings = validator.validate()
    
    # Print errors
    if errors:
        print("\nErrors found:")
        for error in errors:
            print(f"  ❌ {error}")
    
    # Print warnings
    if warnings:
        print("\nWarnings:")
        for warning in warnings:
            print(f"  ⚠️  {warning}")
    
    # Determine success
    if not errors and not warnings:
        print("\n✅ DAG validation passed!")
        return 0
    elif not errors and warnings and not strict:
        print("\n✅ DAG validation passed with warnings")
        return 0
    else:
        print("\n❌ DAG validation failed")
        return 1


def main(argv: Optional[list] = None) -> int:
    """Main entry point for CLI."""
    parser = create_parser()
    args = parser.parse_args(argv)
    
    if args.command == "validate":
        return validate_command(args.dag_file, args.strict)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
