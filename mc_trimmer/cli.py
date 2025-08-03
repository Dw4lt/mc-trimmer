from argparse import SUPPRESS, ArgumentParser
from multiprocessing import cpu_count
from pathlib import Path

import rich

from .pipeline import Config
from .commands import Trim, process_world, RegionManager
from .primitives import RegionLike

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
        "-s",
        "--generate-schema",
        dest="schema_destination",
        help="Generate a JSON-Schema for the pipeline configuration and exit. This can be used by various text editors to provide type hints.",
        type=Path,
    )

    action = parser.add_subparsers(
        title="action",
        description="Which action you would like to perform. Each with their own arguments.",
        dest="action",
    )

    pipeline = action.add_parser(
        "pipeline",
        description="A selection of commands relating to a pipeline. Effectively, this is a data-driven command interface.",
    )
    pipeline.add_argument(
        "--validate",
        dest="validate_pipeline",
        type=Path,
        help="Validate a pipeline to ensure the contents are valid and exit.",
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
    args = parser.parse_args()

    if schema_destination := getattr(args, "schema_destination", None):
        Config.to_schema(schema_destination)
        rich.print(f"Schema saved to '{schema_destination}'")
        rich.print("Hint: if you are using VScode, you can add the following to your settings:")
        rich.print_json(
            """{"json.schemas": [{"fileMatch": ["*.test.json"],"url": """ + f'"{schema_destination}"' + """}]}"""
        )
        return

    # Run
    match args.action:
        case "pipeline":
            if pipeline := getattr(args, "validate_pipeline", None):
                Config.load(pipeline)
                rich.print("Pipeline deemed valid. Exiting.")
                return
        case "trim":
            threads: int = getattr(args, "threads", 1)
            paths = Paths(
                inp=Path(args.input_dir),
                outp=Path(args.output_dir) if getattr(args, "output_dir", None) is not None else Path(args.input_dir),
                backup=Path(args.backup_dir) if getattr(args, "backup_dir", None) else None,
            )
            region_manager = RegionManager(paths=paths)
            region_file_names: list[str] = list(RegionLike.get_regions(paths.inp_region))
            for _ in process_world(
                threads=threads,
                command=Trim(args.trimming_criteria),
                region_manager=region_manager,
                region_file_names=region_file_names,
            ):
                pass
        case _:
            raise Exception(f"Unknown option: '{args.action}'")
