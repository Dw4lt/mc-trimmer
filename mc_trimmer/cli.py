import logging
import logging.handlers
import multiprocess
import shutil
from argparse import SUPPRESS, ArgumentParser
from pathlib import Path
from typing import Callable, Iterable
from multiprocessing import cpu_count
from multiprocess.pool import Pool
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import asyncio

from . import Chunk, Region, get_regions
from .__version__ import __version__


CRITERIA_MAPPING: dict[str, Callable[[Chunk], bool]] = {
    "inhabited_time<15s": lambda chunk: chunk.InhabitedTime <= 1200 * 0.25,
    "inhabited_time<30s": lambda chunk: chunk.InhabitedTime <= 1200 * 0.5,
    "inhabited_time<1m": lambda chunk: chunk.InhabitedTime <= 1200,
    "inhabited_time<2m": lambda chunk: chunk.InhabitedTime <= 1200 * 2,
    "inhabited_time<3m": lambda chunk: chunk.InhabitedTime <= 1200 * 3,
    "inhabited_time<5m": lambda chunk: chunk.InhabitedTime <= 1200 * 5,
    "inhabited_time<10m": lambda chunk: chunk.InhabitedTime <= 1200 * 10,
}


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
        help="Directory to source the region files from. If no output region directory is specified, in-place editing will be performed.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-o",
        "--output-region",
        dest="region_output_dir",
        help="Directory to store the region files to. If unspecified, in-place editing will be performed by taking the input directory instead.",
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

    assert backup != inp
    assert backup != outp

    if parallel is None:
        process_batch(args.trimming_criteria, backup=backup, outp=outp, regions=list(get_regions(Path(inp))))
    else:
        work: list[list[Path]] = [[] for _ in range(parallel)]
        for i, r in enumerate(get_regions(Path(inp))):
            work[i % parallel].append(r)

        foo = partial(process_batch, args.trimming_criteria, backup, outp)
        with Pool(parallel) as p:
            res = p.map(func=foo, iterable=(a for a in work))
            pass


def process_region(criteria: Callable[[Chunk], bool], region_path: Path, backup: Path | None, outp: Path):
    region = Region.from_file(region_path)
    region.trim(criteria)
    if region.dirty:
        if backup:
            shutil.copy2(region_path, backup / region_path.name)
        region.save_to_file(outp / region_path.name)
    else:
        print(f"Region unchanged: {region_path}")
        if region_path != outp / region_path.name:
            shutil.copy2(region_path, outp / region_path.name)


def process_batch(criteria: str, backup: Path | None, outp: Path, regions: Iterable[Path]):
    l = len(regions)
    for i, r in enumerate(regions):
        print(f"Processing region {r} ({i}/{l})")
        process_region(CRITERIA_MAPPING[criteria], r, backup, outp)
