"""CLI subcommand: pipecheck search."""
import argparse
from pipecheck.formats import DAGLoader
from pipecheck.searcher import DAGSearcher, SearchError


def add_search_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "search",
        help="Search tasks in a DAG by id, tag, or description.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON or YAML).")
    parser.add_argument("query", help="Search query string.")
    parser.add_argument(
        "--by",
        choices=["id", "tag", "description"],
        default="id",
        help="Field to search against (default: id).",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if no matches are found.",
    )
    parser.set_defaults(func=search_command)


def search_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:
        print(f"Error loading DAG: {exc}")
        return 1

    try:
        searcher = DAGSearcher(dag)
        result = searcher.search(args.query, by=args.by)
    except SearchError as exc:
        print(f"Search error: {exc}")
        return 1

    print(result)

    if args.exit_code and not result.has_matches:
        return 1
    return 0
