import shutil
from dataclasses import dataclass
from functools import partial
import sys
import traceback
from typing import Callable, Iterable

from multiprocess.pool import Pool

from mc_trimmer.entities import EntitiesFile, Entity
from mc_trimmer.primitives import Paths, RegionLike
from mc_trimmer.regions import Chunk, RegionFile


@dataclass
class Region:
    file_name: str
    region: RegionFile
    entities: EntitiesFile

    def trim(self, condition: Callable[[Chunk, Entity], bool]):
        indexes_to_delete: list[int] = []
        for i, cd in self.region.chunk_data.items():
            condition_met: bool = False
            if self.entities is not None:
                ed = self.entities.entity_data.get(i, None)
                if ed is None:
                    condition_met = condition(cd.data, Entity())
                else:
                    condition_met = condition(cd.data, ed.data)
            if condition_met:
                indexes_to_delete.append(i)
        for i in indexes_to_delete:
            self.region.reset_chunk(i)
            if self.entities is not None:
                self.entities.reset_chunk(i)

        pass

    def iterate(self) -> Iterable[tuple[int, Chunk, Entity]]:
        full_outer_join = set(self.region.chunk_data.keys()) | set(self.entities.entity_data.keys())
        for i in full_outer_join:
            c = self.region.chunk_data.get(i, None)
            c = c.data if c is not None else Chunk()

            e = self.entities.entity_data.get(i, None)
            e = e.data if e is not None else Entity()
            yield (i, c, e)

    def reset_chunk(self, index: int):
        self.region.reset_chunk(index)
        self.entities.reset_chunk(index)


class RegionManager:
    def __init__(self, paths: Paths) -> None:
        self._paths: Paths = paths

    def open_file(self, file_name: str) -> Region:
        region = RegionFile.from_file(self._paths.inp_region / file_name)

        if (self._paths.inp_entities / file_name).exists():
            entities = EntitiesFile.from_file(self._paths.inp_entities / file_name)
        else:
            entities = EntitiesFile(b"", b"", b"")

        return Region(region=region, entities=entities, file_name=file_name)

    def trim(self, region: Region, condition: Callable[[Chunk, Entity], bool]) -> None:
        for i, c, e in region.iterate():
            if condition(c, e):
                region.reset_chunk(i)

    def save_to_file(self, region: Region, file_name: str) -> None:
        if region.region.dirty:
            if self._paths.backup_region is not None:
                shutil.copy2(self._paths.inp_region / file_name, self._paths.backup_region / file_name)
            region.region.save_to_file(self._paths.outp_region / file_name)
        else:
            print(f"Region unchanged: {file_name}")
            if self._paths.inp_region != self._paths.outp_region:
                shutil.copy2(self._paths.inp_region / file_name, self._paths.outp_region / file_name)

        if region.entities.dirty:
            if self._paths.backup_entities is not None:
                shutil.copy2(self._paths.inp_entities / file_name, self._paths.backup_entities / file_name)
            region.entities.save_to_file(self._paths.outp_entities / file_name)
        else:
            print(f"Entities unchanged: {file_name}")
            if (
                self._paths.inp_entities != self._paths.outp_entities
                and (self._paths.inp_entities / file_name).exists()
            ):
                shutil.copy2(self._paths.inp_entities / file_name, self._paths.outp_entities / file_name)


CRITERIA_MAPPING: dict[str, Callable[["Chunk", "Entity"], bool]] = {
    "inhabited_time<15s": lambda chunk, _: chunk.InhabitedTime <= 1200 * 0.25,
    "inhabited_time<30s": lambda chunk, _: chunk.InhabitedTime <= 1200 * 0.5,
    "inhabited_time<1m": lambda chunk, _: chunk.InhabitedTime <= 1200,
    "inhabited_time<2m": lambda chunk, _: chunk.InhabitedTime <= 1200 * 2,
    "inhabited_time<3m": lambda chunk, _: chunk.InhabitedTime <= 1200 * 3,
    "inhabited_time<5m": lambda chunk, _: chunk.InhabitedTime <= 1200 * 5,
    "inhabited_time<10m": lambda chunk, _: chunk.InhabitedTime <= 1200 * 10,
}


def process_region(manager: RegionManager, criteria: Callable[[Chunk, Entity], bool], file_name: str):
    region: Region = manager.open_file(file_name=file_name)
    manager.trim(region=region, condition=criteria)
    manager.save_to_file(region=region, file_name=file_name)


def process_batch(manager: RegionManager, criteria: str, file_names: list[str]) -> list[tuple[Exception, str]]:
    l = len(file_names)
    exceptions: list[tuple[Exception, str]] = []
    for i, r in enumerate(file_names, start=1):
        print(f"Processing region {r} ({i}/{l})")
        try:
            process_region(manager, CRITERIA_MAPPING[criteria], r)
        except AssertionError as e:
            e.add_note(f"[E]: AssertionError while processing {r}")
            tb = str(traceback.extract_tb(sys.exc_info()[2]))
            exceptions.append((e, tb))
        except Exception as e:
            e.add_note(f"[E]: Exception while processing {r}")
            tb = str(traceback.extract_tb(sys.exc_info()[2]))
            exceptions.append((e, tb))
    return exceptions


def main(*, threads: int | None, paths: Paths, trimming_criteria: str) -> None:
    rm = RegionManager(paths=paths)
    region_file_names: Iterable[str] = RegionLike.get_regions(paths.inp_region)

    if threads is None:
        res = process_batch(
            manager=rm,
            criteria=trimming_criteria,
            file_names=list(region_file_names),
        )
        for e, traceback in res:
            print("\n".join(e.__notes__), e, traceback)
    else:
        work: list[list[str]] = [[] for _ in range(threads)]
        for i, r in enumerate(region_file_names):
            work[i % threads].append(r)

        foo = partial(process_batch, rm, trimming_criteria)
        with Pool(threads) as p:
            res = p.map(func=foo, iterable=work)
            pass
        for combo in res:
            for e, traceback in combo:
                print("\n".join(e.__notes__), e, traceback)
