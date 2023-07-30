import os
import tempfile
from pathlib import Path
from typing import Callable

import pytest

from mc_trimmer import *
from mc_trimmer.entities import Entity

current_dir = Path(os.path.dirname(__file__))
input_dir = current_dir / "in"
output_dir = current_dir / "out"
# test_dir = current_dir / "debug"
# test_dir.mkdir(exist_ok=True)


@pytest.hookimpl(tryfirst=True)
def pytest_exception_interact(call):
    raise call.excinfo.value


@pytest.hookimpl(tryfirst=True)
def pytest_internalerror(excinfo):
    raise excinfo.value


@pytest.mark.parametrize(
    "file,filter",
    [
        ("region/simple.mca", None),
        ("region/remove_one.mca", lambda chunk: chunk.xPos == 1 and chunk.zPos == 288),
        ("region/r.0.0.mca", lambda chunk: chunk.xPos == 0 and chunk.zPos == 0),
        ("region/checkerboard.mca", lambda chunk: (chunk.xPos + chunk.zPos) % 2),
        ("region/complex_checkerboard.mca", lambda chunk: (chunk.xPos + chunk.zPos) % 2),
    ],
)
def test_RegionFile(file: str, filter: Callable[[Chunk], bool] | None):
    input_file = input_dir / file
    output_file = output_dir / file

    region = RegionFile.from_file(input_file)
    if filter is not None:
        region.trim(filter)

    b = bytes(region)

    # region.save_to_file(test_dir / file)

    with open(output_file, "+rb") as f:
        a = f.read()

    t = a == b
    if not t:
        assert False


@pytest.mark.parametrize(
    "file,filter",
    [
        ("entities/simple.mca", None),
        ("entities/remove_one.mca", lambda entity: entity.contains_id("minecraft:chicken")),
    ],
)
def test_EntityFile(file: str, filter: Callable[[Entity], bool] | None):
    input_file = input_dir / file
    output_file = output_dir / file

    entities = EntitiesFile.from_file(input_file)
    if filter is not None:
        entities.trim(filter)

    b = bytes(entities)

    with open(output_file, "+rb") as f:
        a = f.read()

    t = a == b

    if not t:
        assert False


@pytest.mark.parametrize(
    "file,filter",
    [
        ("simple.mca", None),
    ],
)
def test_all(file: str, filter: Callable[[Chunk, Entity], bool]):
    expected_paths = Paths(
        inp=Path(input_dir),
        outp=Path(output_dir),
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        paths = Paths(
            inp=Path(input_dir),
            outp=Path(tmp_dir),
        )
        manager = RegionManager(paths)
        region: Region = manager.open_file(file)
        if filter is not None:
            manager.trim(region, filter)
        manager.save_to_file(region, file)

        with open(expected_paths.outp_region / file, "rb") as correct, open(paths.outp_region / file, "rb") as got:
            a = correct.read()
            b = got.read()
            t = a == b
            assert t

        with open(expected_paths.outp_entities / file, "rb") as correct, open(paths.outp_entities / file, "rb") as got:
            a = correct.read()
            b = got.read()
            t = a == b
            assert t
