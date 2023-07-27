import logging
import shutil
from argparse import SUPPRESS, ArgumentParser
from pathlib import Path
from typing import Callable

from . import Chunk, Region, get_regions
from .__version__ import __version__

logging.basicConfig(
    level=logging.DEBUG,
)


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

    criteria_mapping: dict[str, Callable[[Chunk], bool]] = {
        "inhabited_time<1m": lambda chunk: chunk.InhabitedTime <= 1200,
        "inhabited_time<5m": lambda chunk: chunk.InhabitedTime <= 1200 * 5,
        "inhabited_time<10m": lambda chunk: chunk.InhabitedTime <= 1200 * 10,
    }

    parser.add_argument(
        "-c",
        "--criteria",
        dest="trimming_criteria",
        choices=[k for k in criteria_mapping.keys()],
        help="Pre-defined criteria by which to determmine if a chunk should be trimmed or not.",
        required=True,
    )

    # Parse
    args, _ = parser.parse_known_args()

    inp = Path(args.region_input_dir)
    outp = Path(args.region_output_dir) if args.region_output_dir is not None else inp
    backup = Path(args.backup_dir) if args.backup_dir else None

    assert backup != inp
    assert backup != outp

    criteria = criteria_mapping[args.trimming_criteria]

    for r in get_regions(Path(inp)):
        region = Region.from_file(r)
        region.trim(criteria)
        if region.dirty:
            if backup:
                shutil.copy2(r, backup / r.name)
            region.save_to_file(outp / r.name)
    pass


run()
