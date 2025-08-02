from argparse import SUPPRESS, ArgumentParser
from multiprocessing import cpu_count
from pathlib import Path

from mc_trimmer.main import Trim, Command, process_world

from . import Paths
from .__version__ import __version__


def run():
    parser = ArgumentParser(
        prog="mctrimmer",
        description=f"Trim a minecraft dimension based on per-chunk criteria. v{__version__}",
        add_help=False,
    )
    parser.add_argument(
        "-h",
        "--help",
        help="Show this help message and exit.",  # Default implementation is not capitalized
        action="help",
        default=SUPPRESS,
    )

    action = parser.add_subparsers(
        title="action",
        description="Which action you would like to perform. Each with their own arguments.",
        dest="action",
    )
    trim = action.add_parser(
        name="trim",
        description="Delete/Export select regions",
    )
    trim.add_argument(
        "-i",
        "--input-region",
        dest="input_dir",
        help="Directory to source the dimension files from. If no output directory is specified, in-place editing will be performed.",
        required=True,
        type=str,
    )
    trim.add_argument(
        "-p",
        "--parallel",
        dest="threads",
        help="Parallelize the task. If no thread count is specified, the number of cpu cores -1 is taken instead.",
        nargs="?",
        type=int,
        default=None,
        const=cpu_count() - 1,
    )
    trim.add_argument(
        "-c",
        "--criteria",
        dest="trimming_criteria",
        choices=[k for k in Trim.CRITERIA_MAPPING.keys()],
        help="Pre-defined criteria by which to determine if a chunk should be trimmed or not.",
        required=True,
    )
    trim.add_argument(
        "-b",
        "--backup",
        dest="backup_dir",
        help="Backup regions affected by trimming to this directory. Defaults to './backup'",
        nargs="?",
        default=None,
        const="./backup",
    )
    trim.add_argument(
        "-o",
        "--output-region",
        dest="output_dir",
        help="Directory to store the dimension files to. If unspecified, in-place editing will be performed by taking the input directory instead.",
        nargs="?",
        default=None,
    )

    # Parse
    args, _ = parser.parse_known_args()

    command: Command = None  # type: ignore
    threads: int | None = args.threads
    paths = Paths(
        inp=Path(args.input_dir),
        outp=Path(args.output_dir) if getattr(args, "output_dir", None) is not None else Path(args.input_dir),
        backup=Path(args.backup_dir) if getattr(args, "backup_dir", None) else None,
    )

    match args.action:
        case "trim":
            command = Trim(args.trimming_criteria)
        case _:
            raise Exception(f"Unknown option: '{args.action}'")

    assert command
    process_world(threads=threads, paths=paths, command=command)
