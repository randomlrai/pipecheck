"""CLI sub-command: diff — compare two pipeline DAG definition files."""
import sys
from argparse import ArgumentParser, Namespace
from pipecheck.formats import DAGLoader, FormatError
from pipecheck.differ import DAGDiffer


def add_diff_subparser(subparsers) -> None:
    """Register the 'diff' sub-command onto an argparse subparsers object."""
    parser: ArgumentParser = subparsers.add_parser(
        "diff",
        help="Compare two DAG definition files and show structural changes.",
    )
    parser.add_argument("old", metavar="OLD_FILE", help="Path to the baseline DAG file.")
    parser.add_argument("new", metavar="NEW_FILE", help="Path to the updated DAG file.")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if any differences are found.",
    )
    parser.set_defaults(func=diff_command)


def diff_command(args: Namespace) -> int:
    """Execute the diff sub-command. Returns an integer exit code."""
    loader = DAGLoader()
    try:
        old_dag = loader.load_from_file(args.old)
        new_dag = loader.load_from_file(args.new)
    except FormatError as exc:
        print(f"Error loading DAG files: {exc}", file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"File not found: {exc}", file=sys.stderr)
        return 2

    differ = DAGDiffer()
    diff = differ.compare(old_dag, new_dag)

    if not diff.has_changes:
        print("No differences found.")
        return 0

    print(diff.summary())

    if args.exit_code and diff.has_changes:
        return 1
    return 0
