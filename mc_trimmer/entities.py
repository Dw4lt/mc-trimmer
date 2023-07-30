import zlib
from .primitives import *


class Entity(Serializable):
    def __init__(
        self,
        length: int = 0,
        compression: int = 2,
        data: bytes = b"",
        compressed_data: bytes = b"",
    ) -> None:
        self._compression: int = compression
        self._compressed_data: bytes = compressed_data
        self.decompressed_data: bytes = b""

        if length > 0:
            self.decompressed_data = zlib.decompress(data)[3:]  # 3 bytes removes root tag opening

    def contains_id(self, id: str) -> bool:
        if len(self.decompressed_data) == 0:
            return False
        bytes_id: bytes = id.encode()
        size: bytes = struct.pack(">H", len(bytes_id))
        sub: bytes = b"\x08\x00\x02id" + size + bytes_id
        return sub in self.decompressed_data

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

    def __bytes__(self) -> bytes:
        return bytes(self._compressed_data)

    @property
    def SIZE(self) -> int:
        return len(self._compressed_data)


class EntitiesFile(RegionLike):
    def __init__(self, location_data: bytes, timestamp_data: bytes, data: bytes) -> None:
        self.entity_data: ChunkDataDict[Entity] = ChunkDataDict[Entity]()
        self.dirty: bool = False

        if len(location_data) > 0:
            locations: ArrayOfSerializable[SerializableLocation] = LocationData().from_bytes(location_data)
            timestamps: ArrayOfSerializable[Timestamp] = TimestampData().from_bytes(timestamp_data)

            for i, (loc, ts) in enumerate(zip(locations, timestamps, strict=False)):
                if loc.size > 0:
                    assert loc.offset >= 2
                    start = loc.offset * Sizes.CHUNK_SIZE_MULTIPLIER
                    entity_data = data[start : start + loc.size * Sizes.CHUNK_SIZE_MULTIPLIER]

                    d = ChunkDataBase(data=Entity.from_bytes(entity_data), location=loc, timestamp=ts, index=i)
                    self.entity_data.append(d)

    def __bytes__(self) -> bytes:
        return RegionLike.to_bytes(self.entity_data)

    @classmethod
    def from_file(cls, file: Path) -> Self:
        with open(file, "+rb") as f:
            data = memoryview(f.read()).toreadonly()
            chunk_location_data: bytes = data[: Sizes.LOCATION_DATA_SIZE]
            timestamps_data: bytes = data[
                Sizes.LOCATION_DATA_SIZE : Sizes.LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE
            ]
            return EntitiesFile(chunk_location_data, timestamps_data, data)

    def trim(self, condition: Callable[[Entity], bool]):
        to_delete: list[int] = []
        for i, cd in self.entity_data.items():
            if condition(cd.data):
                to_delete.append(i)
        for i in to_delete:
            self.reset_chunk(i)

    def reset_chunk(self, index: int) -> None:
        popped = self.entity_data.pop(index, None)
        self.dirty |= popped is not None
