#!/usr/bin/env python


import logging
import os
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Self
from .entities import EntitiesFile

from mc_trimmer.primitives import (
    INT_STRATEGY,
    LONG_STRATEGY,
    LocationData,
    Serializable,
    SerializableLocation,
    Sizes,
    Timestamp,
    TimestampData,
    fast_get_property,
)


class Chunk(Serializable):
    def __init__(
        self,
        length: int = 0,
        compression: int = 2,
        data: bytes = b"",
        compressed_data=b"",
    ) -> None:
        self._compression: int = compression
        self._compressed_data: bytes = compressed_data

        if length > 0:
            self.decompressed_data = zlib.decompress(data)[3:]  # 3 bytes removes root tag opening
            pass

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
    def from_bytes(cls: type[Self], data: bytes) -> Self:
        length, compression = struct.unpack(">IB", data[: Sizes.CHUNK_HEADER_SIZE])
        nbt_data = data[Sizes.CHUNK_HEADER_SIZE :]  # Sizes.CHUNK_HEADER_SIZE + length - 1]
        assert compression == 2
        post_chunk_data = data[Sizes.CHUNK_HEADER_SIZE + length :]
        if len(post_chunk_data) > 0:
            if post_chunk_data[0] != 0:
                pass
                # print(f"Warning: post-chunk data was padded with non-zero values: {bytes(post_chunk_data[:100])}")
        return cls(length=length, compression=compression, data=nbt_data, compressed_data=data)

    def conditional_reset(self, condition: Callable[[Self], bool]) -> bool:
        if self._compressed_data != b"":
            if condition(self):
                self._compressed_data = b""
                return True
        return False

    def __bytes__(self) -> bytes:
        return bytes(self._compressed_data)

    @property
    def SIZE(self) -> int:
        return len(self._compressed_data)


TimestampData = Timestamp * 1024
ChunkLocationData = ChunkLocation * 1024


def get_regions(path: str | Path) -> Iterable[Path]:
    p: Path = Path(path)
    if p.exists() and p.is_dir():
        return (f for f in p.glob("*.mca") if f.is_file())
    raise Exception(f"Invalid input <{p}>")


@dataclass
class ChunkData:
    chunk: Chunk
    location: SerializableLocation
    timestamp: Timestamp
    index: int

    def __lt__(self, other: Self) -> bool:
        return self.index < other.index


class RegionFile:
    def __init__(self, chunk_location_data: bytes, timestamps_data: bytes, data: bytes) -> None:
        self.chunk_data: list[ChunkData] = []
        self.dirty: bool = False
        self.__data: bytes = data

        locations = LocationData().from_bytes(chunk_location_data)
        timestamps = TimestampData().from_bytes(timestamps_data)

        for i, (loc, ts) in enumerate(zip(locations, timestamps, strict=False)):
            chunk: Chunk
            if loc.size > 0:
                assert loc.offset >= 2
                start = loc.offset * Sizes.CHUNK_SIZE_MULTIPLIER
                data_slice = self.__data[start : start + loc.size * Sizes.CHUNK_SIZE_MULTIPLIER]
                chunk = Chunk.from_bytes(data_slice)

                # Tests:
                b = bytes(chunk)
                a = bytes(data_slice)
                assert a == b
            else:
                chunk = Chunk()
            self.chunk_data.append(ChunkData(chunk=chunk, location=loc, timestamp=ts, index=i))

        t1, t2 = bytes(data), bytes(self)
        l1, l2 = len(t1), len(t2)
        w1, w2 = t1[:4096], t2[:4096]
        assert len(self.chunk_data) == 1024
        # assert w1 == w2  # Only true if no chunks were modified since inception

        return

    def trim(self, condition: Callable[[Chunk], bool]):
        for cd in self.chunk_data:
            self.dirty |= cd.chunk.conditional_reset(condition)

    @classmethod
    def from_file(cls, region: Path) -> Self:
        with open(region, "+rb") as f:
            data = memoryview(f.read()).toreadonly()
            chunk_location_data: bytes = data[: Sizes.LOCATION_DATA_SIZE]
            timestamps_data: bytes = data[
                Sizes.LOCATION_DATA_SIZE : Sizes.LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE
            ]
            return RegionFile(chunk_location_data, timestamps_data, data)

    def __bytes__(self) -> bytes:
        offset: int = 2
        locations: bytes = b""
        timestamps: bytes = b""
        chunks: bytes = b""

        # Adjust offsets and sizes, store chunk data
        for cd in sorted(self.chunk_data, key=lambda combo: combo.location.offset):
            chunk_data = bytes(cd.chunk)
            length = len(chunk_data)

            assert length % 4096 == 0
            if length == 0 and cd.location.size != 0:
                pass
            cd.location.size = length // 4096
            if cd.location.size == 0:
                cd.location.offset = 0
                cd.timestamp.timestamp = 0
            else:
                chunks += chunk_data
                cd.location.offset = offset
                offset += cd.location.size

        # Convert tables to binary
        for cd in sorted(self.chunk_data, key=lambda combo: combo.index):
            locations += bytes(cd.location)
            timestamps += bytes(cd.timestamp)

        return locations + timestamps + chunks

    def save_to_file(self, region: Path) -> None:
        data = bytes(self)
        if len(data) > Sizes.LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE:
            with open(region, "wb") as f:
                f.write(data)
                LOG.info(f"Written {region}")
        else:
            LOG.info(f"Deleting {region}")
            if region.exists() and region.is_file():
                os.remove(region)
