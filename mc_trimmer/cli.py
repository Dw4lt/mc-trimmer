from argparse import SUPPRESS, ArgumentParser
from multiprocessing import cpu_count
from pathlib import Path

from mc_trimmer.main import CRITERIA_MAPPING, main

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
    parser.add_argument(
        "-b",
        "--backup",
        dest="backup_dir",
        help="Backup regions affected by trimming to this directory. Defaults to './backup'",
        nargs="?",
        default=None,
        const="./backup",
    )
    parser.add_argument(
        "-i",
        "--input-region",
        dest="input_dir",
        help="Directory to source the dimension files from. If no output directory is specified, in-place editing will be performed.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-o",
        "--output-region",
        dest="output_dir",
        help="Directory to store the dimension files to. If unspecified, in-place editing will be performed by taking the input directory instead.",
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "-p",
        "--parallel",
        dest="threads",
        help="Parallelize the task. If no thread count is specified, the number of cpu cores -1 is taken instead.",
        nargs="?",
        type=int,
        default=None,
        const=cpu_count() - 1,
    )

    parser.add_argument(
        "-c",
        "--criteria",
        dest="trimming_criteria",
        choices=[k for k in CRITERIA_MAPPING.keys()],
        help="Pre-defined criteria by which to determmine if a chunk should be trimmed or not.",
        required=True,
    )

    # Parse
    args, _ = parser.parse_known_args()

    inp = Path(args.input_dir)
    outp = Path(args.output_dir) if args.output_dir is not None else inp
    backup = Path(args.backup_dir) if args.backup_dir else None
    threads: int | None = args.threads

    paths = Paths(inp, outp, backup)

    main(threads=threads, paths=paths, trimming_criteria=args.trimming_criteria)
