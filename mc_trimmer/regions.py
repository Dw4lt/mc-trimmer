from dataclasses import dataclass
import struct
from pathlib import Path
from typing import Callable, Iterable, Self, override

from .entities import EntitiesFile, Entity
from .primitives import (
    INT_STRATEGY,
    LONG_STRATEGY,
    ChunkDataBase,
    ChunkDataDict,
    Compression,
    SerializableLocation,
    Timestamp,
    decompress,
    LocationData,
    RegionLike,
    Serializable,
    Sizes,
    TimestampData,
    fast_get_property,
)

# LOG = logging.getLogger(__name__)


class Chunk(Serializable):
    def __init__(
        self,
        compression: Compression = Compression.ZLIB,
        compressed_data: bytes = b"",
        decompressed_data: bytes = b"",
    ) -> None:
        self._compression: Compression = compression
        self._compressed_data: bytes = compressed_data
        self.decompressed_data: bytes = decompressed_data

    @property
    def InhabitedTime(self) -> int:
        velue = fast_get_property(self.decompressed_data, b"InhabitedTime", LONG_STRATEGY)
        assert velue >= 0
        return velue

    @property
    def xPos(self) -> int:
        return fast_get_property(self.decompressed_data, b"xPos", INT_STRATEGY)

    @property
    def yPos(self) -> int:
        return fast_get_property(self.decompressed_data, b"yPos", INT_STRATEGY)

    @property
    def zPos(self) -> int:
        return fast_get_property(self.decompressed_data, b"zPos", INT_STRATEGY)

    @classmethod
    def from_bytes(cls: type[Self], data: bytes) -> Self | None:
        assert len(data) >= Sizes.CHUNK_HEADER_SIZE
        length, compression = struct.unpack(">IB", data[: Sizes.CHUNK_HEADER_SIZE])
        if length == 0:
            return None

        if not compression in Compression:
            raise Exception(f"'{compression}' is not a known compression scheme.")
        compression = Compression(compression)

        # Compression is part of the length
        nbt_data = data[Sizes.CHUNK_HEADER_SIZE : Sizes.CHUNK_HEADER_SIZE - 1 + length]

        if decompressed := decompress(nbt_data, compression):
            return cls(compression=compression, compressed_data=data, decompressed_data=decompressed)
        return None

    def conditional_reset(self, condition: Callable[[Self], bool]) -> bool:
        if len(self._compressed_data) > 0:
            if condition(self):
                self._compressed_data = b""
                return True
        return False

    def __bytes__(self) -> bytes:
        return bytes(self._compressed_data)

    @override
    def SIZE(self) -> int:
        return len(self._compressed_data)


class RegionFile(RegionLike):
    def __init__(
        self,
        chunks: ChunkDataDict[Chunk],
    ) -> None:
        self.chunk_data: ChunkDataDict[Chunk] = chunks
        self.dirty: bool = False

    @staticmethod
    def _extract_chunks(
        data: memoryview,
        locations: Iterable[SerializableLocation],
        timestamps: Iterable[Timestamp],
    ) -> Iterable[ChunkDataBase[Chunk]]:
        for i, (loc, ts) in enumerate(zip(locations, timestamps, strict=False)):
            if loc.size > 0 and loc.offset >= 2:
                start = loc.offset * Sizes.CHUNK_SIZE_MULTIPLIER
                data_slice = data[start : start + loc.size * Sizes.CHUNK_SIZE_MULTIPLIER]
                if len(data_slice) < loc.size * Sizes.CHUNK_SIZE_MULTIPLIER:
                    continue

                if chunk := Chunk.from_bytes(data_slice):
                    yield ChunkDataBase(data=chunk, location=loc, timestamp=ts, index=i)

    def __bytes__(self) -> bytes:
        return RegionFile.to_bytes(data=self.chunk_data)

    def trim(self, condition: Callable[[Chunk], bool]):
        for _, cd in self.chunk_data.items():
            self.dirty |= cd.data.conditional_reset(condition)

    @classmethod
    def from_file(cls, region: Path) -> "RegionFile | None":
        with open(region, "rb") as f:
            data = memoryview(f.read()).toreadonly()
            if len(data) < Sizes.LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE:
                return None

            chunk_location_data: memoryview = data[: Sizes.LOCATION_DATA_SIZE]
            timestamps_data: memoryview = data[
                Sizes.LOCATION_DATA_SIZE : Sizes.LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE
            ]

            locations = LocationData().from_bytes(chunk_location_data)
            timestamps = TimestampData().from_bytes(timestamps_data)

            chunks = ChunkDataDict[Chunk]()
            for chunk in RegionFile._extract_chunks(data, locations, timestamps):
                chunks.append(chunk)

            return RegionFile(chunks)

    def reset_chunk(self, index: int) -> None:
        popped = self.chunk_data.pop(index, None)
        self.dirty |= popped is not None


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
