from collections.abc import Iterator
from enum import Enum
import json
import os
from pathlib import Path
from typing import Literal
from pydantic import BaseModel, Field, RootModel


class Condition(BaseModel):
    minimum_inhabited_minutes: float | None = Field(
        default=None, description="Chunk must have been loaded for at least N minutes."
    )
    maximum_inhabited_minutes: float | None = Field(
        default=None, description="Chunk must have been loaded for at most N minutes."
    )


class BackupVariant(str, Enum):
    BACKUP_ENTIRE_REGION = "entire_region"
    BACKUP_ONLY_AFFECTED_CHUNKS = "only_affected_chunks"


class Backup(BaseModel):
    destination: Path = Field(description="Path to directory in which to save a backup of the region.")
    mode: BackupVariant = Field(default=BackupVariant.BACKUP_ENTIRE_REGION)


class Filter(BaseModel):
    command: Literal["filter_selection"] = Field(description="Restrict selection to chunks matching criteria.")
    condition: Condition


class Extend(BaseModel):
    command: Literal["extend_selection"] = Field(
        description="Extend selection to also include chunks matching criteria."
    )
    condition: Condition


class RadiallyExpandSelection(BaseModel):
    command: Literal["radially_expand_selection"] = Field(
        description="For each chunk in the selection, include all neighbours within a radius of N chunks around it.\nWarning: a large radius combined with an already large selection may take a long time."
    )
    radius: int = Field(gt=0)


class DeleteSelected(BaseModel):
    command: Literal["delete_selected_chunks"] = Field(description="Delete all chunks in selection.")
    backup: Backup = Field(description="Before deletion, back up the relevant chunks.")


class SaveSelection(BaseModel):
    command: Literal["save_selection"] = Field(
        description="Save the current selection as a MCASelector compatible .csv file."
    )
    MCASelector_csv_file: Path


class ExtendToRegion(BaseModel):
    command: Literal["select_affected_regions"] = Field(
        description="If a region contains a chunk which is part of the selection, select the entire region."
    )


class MoveSelected(BaseModel):
    command: Literal["move_selected"]
    entire_region: bool = Field(
        default=False,
        description="If false (default), only the relevant chunks are moved. If true, the entire ",
    )
    destination: Path


class InvertSelection(BaseModel):
    command: Literal["invert_selection"]


class Start(str, Enum):
    ALL_CHUNKS_SELECTED = "all_chunks_selected"
    NO_CHUNKS_SELECTED = "no_chunks_selected"


class PipelineStep(RootModel):
    root: (
        Filter
        | Extend
        | MoveSelected
        | RadiallyExpandSelection
        | SaveSelection
        | DeleteSelected
        | ExtendToRegion
        | InvertSelection
    ) = Field(discriminator="command")


class Pipeline(BaseModel):
    input_folder: Path
    start_with: Start
    threads: int = Field(
        description="Number of threads to use at once. If unspecified, all available threads will be used.",
        default_factory=lambda: (os.cpu_count() or 2) - 1,
    )
    command_chain: list[PipelineStep] = Field(description="Sequence of commands to execute.")


class Config(RootModel):
    root: list[Pipeline]

    def __iter__(self) -> Iterator[Pipeline]:  # type: ignore
        return iter(self.root)

    def __getitem__(self, item: int):
        return self.root[item]

    @staticmethod
    def to_schema(file: Path):
        with open(file, "w") as f:
            f.write(json.dumps(Config.model_json_schema(), indent=4))

    @staticmethod
    def load(file: Path) -> "Config":
        with open(file, "r") as f:
            model = Config.model_validate_json(f.read())
            return model
