#!/usr/bin/env python


from abc import abstractmethod
from pathlib import Path
import struct
from typing import Iterable, Self, Type, TypeVar
from enum import IntEnum


PATH = "./data"
T = TypeVar('T', bound='Serializable')

class Sizes(IntEnum):
    CHUNK_LOCATION_DATA_SIZE = 4 * 1024
    TIMESTAMPS_DATA_SIZE = 4 * 1024


class Meta(type):
    def __mul__(mcs: type[T], i: int) -> 'ArrayOfSerializable[T]':
        assert(Serializable in mcs.__mro__)
        return ArrayOfSerializable[mcs](mcs, int(i))


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
            obj: T = self._cls.from_bytes(data[i * self._cls.SIZE : (i + 1) * self._cls.SIZE]) # type: ignore
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

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        offset, size = struct.unpack(">IB", b'\x00' + data)  # 3 byte offset, 1 byte size
        if (size != 0):
            pass
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

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        timestamp, = struct.unpack(">I", data)
        return cls(timestamp=timestamp)

    def __bytes__(self) -> bytes:
        return struct.pack(">I", self.timestamp)

    @classmethod
    @property
    def SIZE(cls) -> int:
        return 4

print(Timestamp.__dict__)

TimestampData = Timestamp * 1024
ChunkLocationData = ChunkLocation * 1024



def get_regions(path: str | Path) -> Iterable[Path]:
    p: Path = Path(path)
    if p.exists() and p.is_dir():
        return (f for f in p.glob("*.mca") if f.is_file())
    raise Exception(f"Invalid input <{p}>")


class Region:
    def __init__(self, chunk_location_data: bytes, timestamps_data: bytes) -> None:
        self.__locations = (ChunkLocation * 1024).from_bytes(chunk_location_data)
        self.__timestamps = (Timestamp * 1024).from_bytes(timestamps_data)

        # Test:
        a = bytes(self.__locations)
        b = bytes(chunk_location_data)
        assert(b == a)

        a = bytes(self.__timestamps)
        b = bytes(timestamps_data)
        assert(b == a)


def open_region(region: Path):
    with open(region, "+rb") as f:
        chunk_location_data: bytes = memoryview(f.read(Sizes.CHUNK_LOCATION_DATA_SIZE))
        timestamps_data: bytes = memoryview(f.read(Sizes.TIMESTAMPS_DATA_SIZE))

        Region(chunk_location_data, timestamps_data)
        pass


def start():
    for r in get_regions(PATH):
        print(r)
        open_region(r)
    exit()


if __name__ == "__main__":
    start()
