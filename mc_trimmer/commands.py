from cmath import log
from dataclasses import dataclass, field
import itertools
from math import ceil, floor
import shutil
import traceback

from multiprocess.pool import Pool
from functools import partial
import rich
import rich.progress as progress

from mc_trimmer.mca_selector import write_mca_selection

from .pipeline import Extend, Pipeline, SaveSelection, Start, Filter, Condition, RadiallyExpandSelection
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


@dataclass(unsafe_hash=True)
class ChunkMetadata:
    x: int = field(hash=True, compare=True)
    y: int = field(hash=True, compare=True)
    inhabited_time: int = field(hash=False, compare=False)

    @property
    def region_coordinate(self) -> tuple[int, int]:
        return self.x // 32, self.y // 32


class GatherMetadata(Command[list[ChunkMetadata]]):
    def __init__(self) -> None:
        super().__init__()

    @staticmethod
    def _gather_metadata(region: Region) -> Iterable[ChunkMetadata]:
        for i, chunk, entity in region.iterate():
            try:
                yield ChunkMetadata(x=chunk.xPos, y=chunk.zPos, inhabited_time=chunk.InhabitedTime)
            except Exception:
                pass

    @override
    def run(self, manager: RegionManager, region_name: str) -> list[ChunkMetadata]:
        region: Region = manager.open_file(file_name=region_name)
        return list(self._gather_metadata(region))


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


class PipelineExecutor:
    def __init__(self, pipeline: Pipeline) -> None:
        self.__pipeline = pipeline
        self.__available_chunks: set[ChunkMetadata] = set()
        self.__selected_chunks: set[ChunkMetadata] = set()
        self.__regions: list[str] = []
        self.__paths = Paths(inp=pipeline.input_folder)
        self.__region_manager = RegionManager(self.__paths)
        pass

    def setup(self, prog: progress.Progress):
        self.__regions.extend(self._gather_all_regions())
        prog.log(f"Found {len(self.__regions)} regions.")

        self._gather_all_chunks(prog)
        if self.__pipeline.start_with == Start.ALL_CHUNKS_SELECTED:
            self.__selected_chunks = self.__available_chunks
        prog.log(f"Starting selection: {len(self.__selected_chunks)}/{len(self.__available_chunks)} chunks")

    def execute(self):
        with progress.Progress(
            progress.SpinnerColumn(finished_text="✅"),
            progress.TextColumn("[progress.description]{task.description}"),
            progress.BarColumn(),
            progress.MofNCompleteColumn(),
            progress.TaskProgressColumn(show_speed=True),
            progress.TimeElapsedColumn(),
            progress.TimeRemainingColumn(),
        ) as prog:
            self.setup(prog)

            pipeline_length = len(self.__pipeline.command_chain)
            for step_nr, step in enumerate(self.__pipeline.command_chain, 1):
                start_cnt = len(self.__selected_chunks)
                task: progress.TaskID
                match step.root:
                    case Filter() as filter:
                        task = prog.add_task(f"Step {step_nr}/{pipeline_length}: Fiter selection")
                        foo = self._make_filter(filter.condition)
                        self.__selected_chunks = set(self._run_filter(prog, task, foo, self.__selected_chunks))
                    case Extend() as extend:
                        task = prog.add_task(f"Step {step_nr}/{pipeline_length}: Extend selection")
                        foo = self._make_filter(extend.condition)
                        self.__selected_chunks.update(self._run_filter(prog, task, foo, self.__available_chunks))
                    case RadiallyExpandSelection() as expand:
                        task = prog.add_task(
                            f"Step {step_nr}/{pipeline_length}: Radially extend selection (r={expand.radius})",
                            total=len(self.__selected_chunks),
                        )
                        self.__selected_chunks.update(
                            self._radially_expand_selection(
                                prog=prog,
                                task=task,
                                threads=self.__pipeline.threads,
                                select_radius=expand.radius,
                                available_chunks=self.__available_chunks,
                                selection=self.__selected_chunks,
                            )
                        )
                    case SaveSelection() as save:
                        task = prog.add_task(
                            f"Step {step_nr}/{pipeline_length}: Save selection in MCASelector format to '{save.MCASelector_csv_file}'",
                            total=len(self.__selected_chunks),
                        )
                        write_mca_selection(
                            output_csv=save.MCASelector_csv_file,
                            selection=prog.track(((c.x, c.y) for c in self.__selected_chunks), task_id=task),
                        )
                    case _:
                        raise Exception(f"Unimplemented command: {step.root.command}")
                end_cnt = len(self.__selected_chunks)
                delta = end_cnt - start_cnt
                delta_text = f"[red]{delta:+}[/red]" if delta < 0 else f"[green]{delta:+}[/green]"
                prog.log(
                    f"Step {step_nr}/{pipeline_length} '{step.root.command}': Chunks in selection: {len(self.__selected_chunks)} [{delta_text}]"
                )

    @staticmethod
    def _make_circular_kernel(radius: int) -> set[tuple[int, int]]:
        ret = set()
        for x in range(radius + 1):
            for y in range(radius + 1):
                if (x) ** 2 + (y) ** 2 <= radius**2:
                    ret.add((x, y))
                    ret.add((-x, y))
                    ret.add((x, -y))
                    ret.add((-x, -y))
        ret.remove((0, 0))  # Uninteresting chunk
        return ret

    @staticmethod
    def _radially_expand_selection(
        *,
        prog: progress.Progress,
        task: progress.TaskID,
        threads: int,
        select_radius: int,
        available_chunks: set[ChunkMetadata],
        selection: set[ChunkMetadata],
    ) -> set[ChunkMetadata]:
        """
        Steps:
        - gather all unselected chunk coordinates
        - for each selected chunk, mark its neighbour coordinates
        - collect all marks and pick those chunks (if they exist) from the available chunks list

        Note: the batch size is important, as it allows each thread in the pool to filter out some duplicates,
        rather than handing that task back to the main thread.
        """

        kernel = PipelineExecutor._make_circular_kernel(select_radius)

        # Switch to raw coordinate tuples to minimize inter-process IO
        selected_coords: set[tuple[int, int]] = {(c.x, c.y) for c in selection}
        currently_unselected_coords: set[tuple[int, int]] = {(c.x, c.y) for c in available_chunks.difference(selection)}

        def filter_for_neighbors(inputs: Iterable[tuple[int, int]]) -> set[tuple[int, int]]:
            ret = set()
            for coord in inputs:
                for offset in kernel:
                    neighbor = coord[0] + offset[0], coord[1] + offset[1]
                    ret.add(neighbor)
            return ret

        coords_to_include: set[tuple[int, int]] = set()

        with Pool(threads) as pool:
            batch_size = 500
            for i, res in enumerate(
                pool.imap_unordered(
                    filter_for_neighbors,
                    itertools.batched(selected_coords, n=batch_size),
                )
            ):
                prog.advance(task, advance=min(batch_size, len(selection) - i * batch_size))
                coords_to_include.update(res & currently_unselected_coords)
        expanded_selection = selection | set(filter(lambda c: (c.x, c.y) in coords_to_include, available_chunks))
        return expanded_selection

    @staticmethod
    def _make_filter(condition: Condition) -> Callable[[ChunkMetadata], bool]:
        criteria: list[Callable[[ChunkMetadata], bool]] = []
        if condition.minimum_inhabited_minutes is not None:
            ticks = floor(condition.minimum_inhabited_minutes * 1200)
            criteria.append(lambda x: x.inhabited_time >= ticks)
        if condition.maximum_inhabited_minutes is not None:
            ticks = ceil(condition.maximum_inhabited_minutes * 1200)
            criteria.append(lambda x: x.inhabited_time <= ticks)

        def foo(filter: list[Callable[[ChunkMetadata], bool]], chunk: ChunkMetadata) -> bool:
            for f in filter:
                if not f(chunk):
                    return False
            return True

        return partial(foo, criteria)

    @staticmethod
    def _run_filter(
        prog: progress.Progress,
        task: progress.TaskID,
        condition: Callable[[ChunkMetadata], bool],
        input: set[ChunkMetadata],
    ) -> Iterable[ChunkMetadata]:
        return filter(condition, prog.track(input, total=len(input), task_id=task))

    def _gather_all_regions(self) -> set[str]:
        return set(RegionLike.get_regions(self.__paths.inp_region))

    def _gather_all_chunks(self, prog: progress.Progress):
        gather_metadata = prog.add_task(
            f"Gathering metadata on all {len(self.__regions)} regions in '{self.__pipeline.input_folder}'",
            start=True,
            total=len(self.__regions),
        )
        for result in process_world(
            region_manager=self.__region_manager,
            threads=self.__pipeline.threads,
            region_file_names=self.__regions,
            command=GatherMetadata(),
        ):
            match result:
                case [*content] if len(content) == 0 or isinstance(content[0], ChunkMetadata):
                    self.__available_chunks.update(content)
                case CommandError() as err:
                    rich.print(err)
                case _:
                    raise Exception(f"Unknown scenario: {result}")
            prog.advance(gather_metadata)
        prog.log(f"Found {len(self.__available_chunks)} chunks.")
