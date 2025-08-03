from collections.abc import Iterable
import csv
from pathlib import Path
from rich.progress import Progress, TaskID


def write_mca_selection(output_csv: Path, selection: Iterable[tuple[int, int]]):
    with open(output_csv, "+w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=("region_x", "region_y", "chunk_x", "chunk_y"),
            lineterminator="\n",
            delimiter=";",
        )
        for coord in selection:
            writer.writerow(
                {
                    "region_x": coord[0] // 32,
                    "region_y": coord[1] // 32,
                    "chunk_x": coord[0],
                    "chunk_y": coord[1],
                }
            )
