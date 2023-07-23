#!/usr/bin/env python


from abc import abstractmethod
from pathlib import Path
import struct
from typing import Any, Callable, Iterable, Self, Type, TypeVar
from enum import IntEnum
import zlib


PATH = "./data"
T = TypeVar('T', bound='Serializable')

class Sizes(IntEnum):
    CHUNK_LOCATION_DATA_SIZE = 4 * 1024
    TIMESTAMPS_DATA_SIZE = 4 * 1024
    CHUNK_SIZE_MULTIPLIER = 4 * 1024
    CHUNK_HEADER_SIZE = 5


class Meta(type):
    def __mul__(mcs: Type[T], i: int) -> Callable[[], 'ArrayOfSerializable[T]']:
        '''With T = Type[Serializable], T * int = ArrayofSerializable[T] of size int'''
        assert(Serializable in mcs.__mro__)
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

    def __repr__(self) -> str:
        return str(f'{self.__class__}: {self.offset}+{self.size}')

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

    def __repr__(self) -> str:
        return str(f'{self.__class__}: {self.timestamp}')

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

class Chunk(Serializable):
    def __init__(self, len: int = 0, compression: int = 2, data: bytes = b'') -> None:
        self._len: int = len
        self._compression: int = compression
        self._unpacked_data: bytes = data


    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        len, compression = struct.unpack(">IB", data[:Sizes.CHUNK_HEADER_SIZE])
        assert compression == 2
        dd = zlib.decompress(data[Sizes.CHUNK_HEADER_SIZE : Sizes.CHUNK_HEADER_SIZE + len])
        return cls(len=len, compression=compression, data=dd)

    def conditional_reset(self, condition: Callable[[Self], bool]):
        if condition(self):
            self._len = 0
            self._unpacked_data = b''

    def __bytes__(self) -> bytes:
        if self._unpacked_data != b'':
            compressed_data = zlib.compress(self._unpacked_data)
            data = struct.pack(">IB", len(compressed_data), self._compression) + compressed_data
            self._len = len(data)
            return data
        return b''

    @property
    def SIZE(self) -> int:
        return Sizes.CHUNK_HEADER_SIZE + self._len


print(Timestamp.__dict__)

TimestampData = Timestamp * 1024
ChunkLocationData = ChunkLocation * 1024


def get_regions(path: str | Path) -> Iterable[Path]:
    p: Path = Path(path)
    if p.exists() and p.is_dir():
        return (f for f in p.glob("*.mca") if f.is_file())
    raise Exception(f"Invalid input <{p}>")


class Region:
    def __init__(self, chunk_location_data: bytes, timestamps_data: bytes, data: bytes) -> None:
        self.__locations = ChunkLocationData().from_bytes(chunk_location_data)
        self.__timestamps = TimestampData().from_bytes(timestamps_data)
        self.__data = data

        # Test:
        a = bytes(self.__locations)
        b = bytes(chunk_location_data)
        assert(b == a)

        a = bytes(self.__timestamps)
        b = bytes(timestamps_data)
        assert(b == a)

        self.__chunks: list[Chunk] = []
        for loc in self.__locations:
            if loc.size == 0:
                self.__chunks.append(Chunk())
            else:
                # location is relative to beginning of file, so timestamp and location table have to be subtracted.
                self.__chunks.append(Chunk.from_bytes(self.__data[loc.offset * Sizes.CHUNK_SIZE_MULTIPLIER - Sizes.CHUNK_LOCATION_DATA_SIZE - Sizes.TIMESTAMPS_DATA_SIZE: ]))
        pass

    def trim(self, condition: Callable[[Chunk], bool]):
        for c in self.__chunks:
            c.conditional_reset(condition)



def open_region(region: Path):
    with open(region, "+rb") as f:
        chunk_location_data: bytes = memoryview(f.read(Sizes.CHUNK_LOCATION_DATA_SIZE))
        timestamps_data: bytes = memoryview(f.read(Sizes.TIMESTAMPS_DATA_SIZE))

        a = Region(chunk_location_data, timestamps_data, memoryview(f.read()))
        pass


def start():
    for r in get_regions(PATH):
        print(r)
        open_region(r)
    exit()


if __name__ == "__main__":
    start()
