from argparse import SUPPRESS, ArgumentParser
from functools import partial
from multiprocessing import cpu_count
from pathlib import Path

from multiprocess.pool import Pool

from mc_trimmer.main import CRITERIA_MAPPING, process_batch

from . import Paths, get_regions
from .__version__ import __version__


def run():
    parser = ArgumentParser(
        prog="mctrimmer",
        description=f"Query NetBox via GraphQL and deploy the documented services. v{__version__}",
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
        dest="region_input_dir",
        help="Directory to source the dimension files from. If no output directory is specified, in-place editing will be performed.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-o",
        "--output-region",
        dest="region_output_dir",
        help="Directory to store the dimension files to. If unspecified, in-place editing will be performed by taking the input directory instead.",
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "-p",
        "--parallel",
        dest="parallel",
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

    inp = Path(args.region_input_dir)
    outp = Path(args.region_output_dir) if args.region_output_dir is not None else inp
    backup = Path(args.backup_dir) if args.backup_dir else None
    parallel: int | None = args.parallel

    paths = Paths(inp, outp, backup)

    if parallel is None:
        process_batch(args.trimming_criteria, paths=paths, regions=list(get_regions(paths.inp_region)))
    else:
        work: list[list[Path]] = [[] for _ in range(parallel)]
        for i, r in enumerate(get_regions(paths.inp_region)):
            work[i % parallel].append(r)

        foo = partial(process_batch, args.trimming_criteria, paths)
        with Pool(parallel) as p:
            res = p.map(func=foo, iterable=(a for a in work))
            pass
        pass
