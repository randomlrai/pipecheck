"""CLI sub-command: annotate — attach notes to DAG tasks."""
import argparse
from pipecheck.formats import DAGLoader
from pipecheck.annotator import DAGAnnotator, AnnotationError


def add_annotate_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "annotate",
        help="Attach an inline note to a task in a DAG file.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument("task_id", help="ID of the task to annotate.")
    parser.add_argument("note", help="Annotation text.")
    parser.add_argument(
        "--author", "-a",
        default=None,
        help="Optional author name to attach to the annotation.",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        dest="list_all",
        help="List all annotations for the specified task instead of adding one.",
    )
    parser.set_defaults(func=annotate_command)


def annotate_command(args: argparse.Namespace) -> int:
    """Execute the annotate sub-command. Returns exit code."""
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}")
        return 1

    annotator = DAGAnnotator(dag)

    if args.list_all:
        annotations = annotator.get(args.task_id)
        if not annotations:
            print(f"No annotations found for task '{args.task_id}'.")
        else:
            for ann in annotations:
                print(ann)
        return 0

    try:
        annotation = annotator.annotate(args.task_id, args.note, author=args.author)
        print(f"Annotation added: {annotation}")
        return 0
    except AnnotationError as exc:
        print(f"Error: {exc}")
        return 1
