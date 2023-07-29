from abc import abstractmethod
from enum import IntEnum
import struct
from typing import Callable, Generic, Self, Type, TypeVar


PATH = "./data"
T = TypeVar("T")
S = TypeVar("S", bound="Serializable")


class Strategy(Generic[T]):
    def __init__(self, typ: bytes, payload_size: int, unpack: bytes, resulting_type: Type[T]) -> None:
        self.typ: bytes = typ
        self.payload_size: int = payload_size
        self.unpack: bytes = unpack
        self.resulting_type: Type[T] = resulting_type


BYTE_STRATEGY = Strategy(typ=b"\x01", payload_size=1, unpack=b">c", resulting_type=int)
INT_STRATEGY = Strategy(typ=b"\x03", payload_size=4, unpack=b">i", resulting_type=int)
LONG_STRATEGY = Strategy(typ=b"\x04", payload_size=8, unpack=b">Q", resulting_type=int)
FLOAT_STRATEGY = Strategy(typ=b"\x05", payload_size=4, unpack=b">f", resulting_type=float)
DOUBLE_STRATEGY = Strategy(typ=b"\x06", payload_size=8, unpack=b">d", resulting_type=float)


class Sizes(IntEnum):
    LOCATION_DATA_SIZE = 4 * 1024
    TIMESTAMPS_DATA_SIZE = 4 * 1024
    CHUNK_SIZE_MULTIPLIER = 4 * 1024
    CHUNK_HEADER_SIZE = 4 + 1


class Meta(type):
    def __mul__(mcs: Type[S], i: int) -> Callable[[], "ArrayOfSerializable[S]"]:
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


class ArrayOfSerializable(list[S]):
    def __init__(self, cls: type[S], len: int) -> None:
        self._len: int = len
        self._cls: Type[Serializable] = cls

    def from_bytes(self, data: bytes) -> Self:
        for i in range(self._len):
            obj: S = self._cls.from_bytes(data[i * self._cls.SIZE : (i + 1) * self._cls.SIZE])  # type: ignore
            self.append(obj)
        return self

    def __bytes__(self) -> bytes:
        ret = b""
        for data in self:
            ret += bytes(data)
        return ret


class SerializableLocation(Serializable):
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


LocationData = SerializableLocation * 1024
TimestampData = Timestamp * 1024


def fast_get_property(decompressed_data: bytes, name: bytes, strategy: Strategy[T]) -> T:
    """Quick-fetch property by seeking through the byte-stream.

    If a property can appear more than once, this will break!"""
    prop_sequence = strategy.typ + struct.pack(">H", len(name)) + name
    start = decompressed_data.find(prop_sequence)
    if start < 0:
        raise Exception(f"Prop '{name.decode()}' not found!")
    (value,) = struct.unpack(
        strategy.unpack,
        decompressed_data[start + len(prop_sequence) : start + len(prop_sequence) + strategy.payload_size],
    )
    return value
