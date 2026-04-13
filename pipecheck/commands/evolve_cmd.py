"""CLI sub-command: evolve — compare a DAG against a baseline to report structural evolution."""
import argparse
from pipecheck.formats import DAGLoader
from pipecheck.evolver import DAGEvolver


def add_evolve_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "evolve",
        help="Compare a current DAG against a baseline DAG and report structural changes.",
    )
    parser.add_argument("baseline", help="Path to the baseline DAG file (JSON or YAML).")
    parser.add_argument("current", help="Path to the current DAG file (JSON or YAML).")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if any changes are detected.",
    )
    parser.set_defaults(func=evolve_command)


def evolve_command(args: argparse.Namespace) -> int:
    loader = DAGLoader()
    try:
        baseline_dag = loader.load_from_file(args.baseline)
        current_dag = loader.load_from_file(args.current)
    except Exception as exc:  # pragma: no cover
        print(f"Error loading DAG files: {exc}")
        return 1

    evolver = DAGEvolver()
    result = evolver.evolve(baseline_dag, current_dag)

    print(str(result))

    if args.exit_code and result.has_changes():
        return 1
    return 0
