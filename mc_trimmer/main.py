from dataclasses import dataclass
from pathlib import Path
import shutil
from typing import Callable, Iterable
from mc_trimmer.entities import EntitiesFile
from mc_trimmer.primitives import Paths
from mc_trimmer.regions import Chunk, RegionFile


@dataclass
class Region:
    region: RegionFile
    entities: EntitiesFile


CRITERIA_MAPPING: dict[str, Callable[["Chunk"], bool]] = {
    "inhabited_time<15s": lambda chunk: chunk.InhabitedTime <= 1200 * 0.25,
    "inhabited_time<30s": lambda chunk: chunk.InhabitedTime <= 1200 * 0.5,
    "inhabited_time<1m": lambda chunk: chunk.InhabitedTime <= 1200,
    "inhabited_time<2m": lambda chunk: chunk.InhabitedTime <= 1200 * 2,
    "inhabited_time<3m": lambda chunk: chunk.InhabitedTime <= 1200 * 3,
    "inhabited_time<5m": lambda chunk: chunk.InhabitedTime <= 1200 * 5,
    "inhabited_time<10m": lambda chunk: chunk.InhabitedTime <= 1200 * 10,
}


def process_region(criteria: Callable[[Chunk], bool], region_path: Path, paths: Paths):
    region = RegionFile.from_file(region_path)
    region.trim(criteria)
    if region.dirty:
        if paths.backup_region is not None:
            shutil.copy2(region_path, paths.backup_region / region_path.name)
        region.save_to_file(paths.outp_region / region_path.name)
    else:
        print(f"Region unchanged: {region_path}")
        if region_path != paths.outp_region / region_path.name:
            shutil.copy2(region_path, paths.outp_region / region_path.name)


def process_batch(criteria: str, paths: Paths, regions: Iterable[Path]):
    l = len(regions)
    for i, r in enumerate(regions, start=1):
        print(f"Processing region {r} ({i}/{l})")
        process_region(CRITERIA_MAPPING[criteria], r, paths)
