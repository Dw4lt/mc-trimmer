import os
from pathlib import Path
from typing import Callable

import pytest
from mc_trimmer import *

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
        ("simple.mca", None),
        ("remove_one.mca", lambda nbt: nbt["xPos"].value == 1 and nbt["zPos"].value == 288),
        ("r.0.0.mca", lambda nbt: nbt["xPos"].value == 0 and nbt["zPos"].value == 0),
        ("checkerboard.mca", lambda nbt: (nbt["xPos"].value + nbt["zPos"].value) % 2),
        ("complex_checkerboard.mca", lambda nbt: (nbt["xPos"].value + nbt["zPos"].value) % 2),
    ],
)
def test_wtf(file: str, filter: Callable | None):
    input_file = input_dir / file
    output_file = output_dir / file

    region = Region.from_file(input_file)
    if filter is not None:
        region.trim(filter)

    b = bytes(region)

    #region.save_to_file(test_dir / file)

    with open(output_file, "+rb") as f:
        a = f.read()

    t = a == b
    if not t:
        assert False
