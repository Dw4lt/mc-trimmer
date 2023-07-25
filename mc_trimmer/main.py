#!/usr/bin/env python


from abc import abstractmethod
from dataclasses import dataclass
import io
from pathlib import Path
import struct
from typing import Callable, Iterable, Self, Type, TypeVar
from enum import IntEnum
import zlib
from nbt.nbt import TAG_Compound


PATH = "./data"
T = TypeVar("T", bound="Serializable")


class Sizes(IntEnum):
    CHUNK_LOCATION_DATA_SIZE = 4 * 1024
    TIMESTAMPS_DATA_SIZE = 4 * 1024
    CHUNK_SIZE_MULTIPLIER = 4 * 1024
    CHUNK_HEADER_SIZE = 4 + 1


class Meta(type):
    def __mul__(mcs: Type[T], i: int) -> Callable[[], "ArrayOfSerializable[T]"]:
        """With T = Type[Serializable], T * int = ArrayofSerializable[T] of size int"""
        assert Serializable in mcs.__mro__

        def curry():
            return ArrayOfSerializable[mcs](mcs, i)

        return curry


class Serializable(metaclass=Meta):
    @abstractmethod
    def __bytes__(self) -> bytes:
        ...

    @classmethod
    @abstractmethod
    def from_bytes(cls, data: bytes) -> Self:
        ...

    @classmethod
    @property
    @abstractmethod
    def SIZE(cls) -> int:
        ...


class ArrayOfSerializable(list[T]):
    def __init__(self, cls: type[T], len: int) -> None:
        self._len: int = len
        self._cls: Type[Serializable] = cls

    def from_bytes(self, data: bytes) -> Self:
        for i in range(self._len):
            obj: T = self._cls.from_bytes(data[i * self._cls.SIZE : (i + 1) * self._cls.SIZE])  # type: ignore
            self.append(obj)
        return self

    def __bytes__(self) -> bytes:
        ret = b""
        for data in self:
            ret += bytes(data)
        return ret


class ChunkLocation(Serializable):
    def __init__(self, offset: int, size: int) -> None:
        self.offset = offset
        self.size = size

    def __repr__(self) -> str:
        return str(f"{self.__class__}: {self.offset}+{self.size}")

    def __lt__(self, other: Self):
        return self.offset < other.offset

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        offset, size = struct.unpack(">IB", b"\x00" + data)  # 3 byte offset, 1 byte size
        return cls(offset=offset, size=size)

    def __bytes__(self) -> bytes:
        return struct.pack(">IB", self.offset, self.size)[1:]

    @classmethod
    @property
    def SIZE(cls) -> int:
        return 4


class Timestamp(Serializable):
    def __init__(self, timestamp: int) -> None:
        self.timestamp = timestamp

    def __repr__(self) -> str:
        return str(f"{self.__class__}: {self.timestamp}")

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        (timestamp,) = struct.unpack(">I", data)
        return cls(timestamp=timestamp)

    def __bytes__(self) -> bytes:
        return struct.pack(">I", self.timestamp)

    @classmethod
    @property
    def SIZE(cls) -> int:
        return 4


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
        self.nbt: TAG_Compound

        if length > 0:
            decompressed_data = zlib.decompress(data)
            member_data = decompressed_data[3:]  # 3 bytes removes root tag opening
            self.nbt = TAG_Compound(buffer=io.BytesIO(member_data))

            assert self.nbt["InhabitedTime"].value >= 0
        else:
            self.nbt = TAG_Compound()

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

    def conditional_reset(self, condition: Callable[[TAG_Compound], bool]) -> bool:
        if self._compressed_data != b"":
            if condition(self.nbt):
                self._compressed_data = b""
                self.nbt = TAG_Compound()
                return True
        return False

    def __bytes__(self) -> bytes:
        return bytes(self._compressed_data)
        if self._compressed_data != b"":
            l1 = len(self._compressed_data)
            data = struct.pack(">IB", l1 + 1, self._compression) + self._compressed_data  # +1 for compression scheme
            l = len(data)
            assert l1 + Sizes.CHUNK_HEADER_SIZE == l
            padding = (4096 - (l % 4096)) % 4096
            data = data + b"\x00" * padding
            return bytes(self._compressed_data)
        return b""

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
    location: ChunkLocation
    timestamp: Timestamp
    index: int

    def __lt__(self, other: Self) -> bool:
        return self.index < other.index


class Region:
    def __init__(self, chunk_location_data: bytes, timestamps_data: bytes, data: bytes) -> None:
        self.chunk_data: list[ChunkData] = []
        self.dirty: bool = False
        self.__data: bytes = data

        # TODO: Delete!
        locations = ChunkLocationData().from_bytes(chunk_location_data)
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

    def trim(self, condition: Callable[[TAG_Compound], bool]):
        for cd in self.chunk_data:
            self.dirty |= cd.chunk.conditional_reset(condition)

    @classmethod
    def from_file(cls, region: Path) -> Self:
        with open(region, "+rb") as f:
            data = memoryview(f.read()).toreadonly()
            chunk_location_data: bytes = data[: Sizes.CHUNK_LOCATION_DATA_SIZE]
            timestamps_data: bytes = data[
                Sizes.CHUNK_LOCATION_DATA_SIZE : Sizes.CHUNK_LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE
            ]
            return Region(chunk_location_data, timestamps_data, data)

    def __bytes__(self) -> bytes:
        offset: int = 2
        locations: bytes = b""
        timestamps: bytes = b""
        chunks: bytes = b""

        # Adjust offsets and sizes, store chunk data
        prev = ChunkData(Chunk(), ChunkLocation(offset, 0), Timestamp(0), 0)
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
        if len(data) > Sizes.CHUNK_LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE:
            with open(region, "wb") as f:
                f.write(data)
        else:
            print(f"Deleting {region}")
