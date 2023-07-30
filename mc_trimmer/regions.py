import os
import struct
import zlib
from pathlib import Path
from typing import Callable, Self

from mc_trimmer.primitives import (
    INT_STRATEGY,
    LONG_STRATEGY,
    ChunkDataBase,
    ChunkDataDict,
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
        length: int = 0,
        compression: int = 2,
        data: bytes = b"",
        compressed_data: bytes = b"",
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


class RegionFile(RegionLike):
    def __init__(self, chunk_location_data: bytes, timestamps_data: bytes, data: bytes) -> None:
        self.chunk_data: ChunkDataDict[Chunk] = ChunkDataDict[Chunk]()
        self.dirty: bool = False

        locations = LocationData().from_bytes(chunk_location_data)
        timestamps = TimestampData().from_bytes(timestamps_data)

        for i, (loc, ts) in enumerate(zip(locations, timestamps, strict=False)):
            chunk: Chunk
            if loc.size > 0:
                assert loc.offset >= 2
                start = loc.offset * Sizes.CHUNK_SIZE_MULTIPLIER
                data_slice = data[start : start + loc.size * Sizes.CHUNK_SIZE_MULTIPLIER]
                chunk = Chunk.from_bytes(data_slice)

                # Tests:
                # b = bytes(chunk)
                # a = bytes(data_slice)
                # assert a == b
                self.chunk_data.append(ChunkDataBase(data=chunk, location=loc, timestamp=ts, index=i))
        return

    def __bytes__(self) -> bytes:
        return RegionFile.to_bytes(data=self.chunk_data)

    def trim(self, condition: Callable[[Chunk], bool]):
        for i, cd in self.chunk_data.items():
            self.dirty |= cd.data.conditional_reset(condition)

    @classmethod
    def from_file(cls, region: Path) -> Self:
        with open(region, "+rb") as f:
            data = memoryview(f.read()).toreadonly()
            chunk_location_data: bytes = data[: Sizes.LOCATION_DATA_SIZE]
            timestamps_data: bytes = data[
                Sizes.LOCATION_DATA_SIZE : Sizes.LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE
            ]
            return RegionFile(chunk_location_data, timestamps_data, data)

    def reset_chunk(self, index: int) -> None:
        popped = self.chunk_data.pop(index, None)
        self.dirty |= popped is not None
