from dataclasses import dataclass
import shutil
import traceback

from multiprocess.pool import Pool
from functools import partial
import rich

from .entities import EntitiesFile
from .primitives import *
from .entities import Entity
from .regions import Chunk, Region, RegionFile, override

from abc import ABC, abstractmethod
from typing import Callable, Generic, Iterable


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

    def save_to_file(self, region: Region, file_name: str) -> None:
        if region.region.dirty:
            if self._paths.backup_region is not None:
                shutil.copy2(self._paths.inp_region / file_name, self._paths.backup_region / file_name)
            region.region.save_to_file(self._paths.outp_region / file_name)
        else:
            rich.print(f"Region unchanged: {file_name}")
            if self._paths.inp_region != self._paths.outp_region:
                shutil.copy2(self._paths.inp_region / file_name, self._paths.outp_region / file_name)

        if region.entities.dirty:
            if self._paths.backup_entities is not None:
                shutil.copy2(self._paths.inp_entities / file_name, self._paths.backup_entities / file_name)
            region.entities.save_to_file(self._paths.outp_entities / file_name)
        else:
            rich.print(f"Entities unchanged: {file_name}")
            if (
                self._paths.inp_entities != self._paths.outp_entities
                and (self._paths.inp_entities / file_name).exists()
            ):
                shutil.copy2(self._paths.inp_entities / file_name, self._paths.outp_entities / file_name)


@dataclass
class CommandError:
    exception: Exception
    traceback: str

    def __str__(self) -> str:
        return "\n".join(
            (
                "\n".join(self.exception.__notes__),
                str(self.exception),
                self.traceback,
            )
        )

    def __repr__(self) -> str:
        return str(self)


class Command(ABC, Generic[T]):
    @abstractmethod
    def run(self, manager: RegionManager, region_name: str) -> T: ...


class Trim(Command[None]):
    CRITERIA_MAPPING: dict[str, Callable[["Chunk", "Entity"], bool]] = {
        "inhabited_time<15s": lambda chunk, _: chunk.InhabitedTime <= 1200 * 0.25,
        "inhabited_time<30s": lambda chunk, _: chunk.InhabitedTime <= 1200 * 0.5,
        "inhabited_time<1m": lambda chunk, _: chunk.InhabitedTime <= 1200,
        "inhabited_time<2m": lambda chunk, _: chunk.InhabitedTime <= 1200 * 2,
        "inhabited_time<3m": lambda chunk, _: chunk.InhabitedTime <= 1200 * 3,
        "inhabited_time<5m": lambda chunk, _: chunk.InhabitedTime <= 1200 * 5,
        "inhabited_time<10m": lambda chunk, _: chunk.InhabitedTime <= 1200 * 10,
    }

    def __init__(self, criteria: str) -> None:
        self._criteria: Callable[[Chunk, Entity], bool] = Trim.CRITERIA_MAPPING[criteria]

    @staticmethod
    def trim(region: Region, elimination_condition: Callable[[Chunk, Entity], bool]) -> None:
        for i, c, e in region.iterate():
            if elimination_condition(c, e):
                region.reset_chunk(i)

    @override
    def run(self, manager: RegionManager, region_name: str) -> None:
        region: Region = manager.open_file(file_name=region_name)
        self.trim(region=region, elimination_condition=self._criteria)
        manager.save_to_file(region=region, file_name=region_name)


def error_handling_wrapper[T](
    manager: RegionManager,
    command: Command[T],
    region_name: str,
) -> CommandError | T:
    try:
        return command.run(manager=manager, region_name=region_name)
    except AssertionError as e:
        e.add_note(f"[E]: AssertionError while processing {region_name}")
        tb = traceback.format_exc()
        return CommandError(exception=e, traceback=tb)
    except Exception as e:
        e.add_note(f"[E]: Exception while processing {region_name}")
        tb = traceback.format_exc()
        return CommandError(exception=e, traceback=tb)


def process_world(
    *,
    region_manager: RegionManager,
    threads: int,
    region_file_names: list[str],
    command: Command[T],
) -> Iterable[CommandError | T]:

    if threads is None:
        threads = 1

    foo = partial(error_handling_wrapper, region_manager, command)
    with Pool(threads) as p:
        for x in p.imap_unordered(func=foo, iterable=region_file_names, chunksize=10):
            yield x
