from typing import override

from .primitives import *


class Entity(Serializable):
    def __init__(
        self,
        compression: Compression = Compression.ZLIB,
        compressed_data: bytes = b"",
        decompressed_data: bytes = b"",
    ) -> None:
        self._compression: Compression = compression
        self._compressed_data: bytes = compressed_data
        self.decompressed_data: bytes = decompressed_data

    def contains_id(self, id: str) -> bool:
        if len(self.decompressed_data) == 0:
            return False
        bytes_id: bytes = id.encode()
        size: bytes = struct.pack(">H", len(bytes_id))
        sub: bytes = b"\x08\x00\x02id" + size + bytes_id
        return sub in self.decompressed_data

    @classmethod
    def from_bytes(cls: type[Self], data: bytes) -> Self | None:
        length, compression = struct.unpack(">IB", data[: Sizes.CHUNK_HEADER_SIZE])
        if length == 0:
            return None
        nbt_data = data[Sizes.CHUNK_HEADER_SIZE : Sizes.CHUNK_HEADER_SIZE - 1 + length]

        if not compression in Compression:
            raise Exception(f"'{compression}' is not a known compression scheme.")
        compression = Compression(compression)

        decompressed = decompress(nbt_data, compression)
        if decompressed is None:
            return None
        return cls(compression=compression, compressed_data=data, decompressed_data=decompressed)

    def __bytes__(self) -> bytes:
        return bytes(self._compressed_data)

    @override
    def SIZE(self) -> int:
        return len(self._compressed_data)


class EntitiesFile(RegionLike):
    def __init__(self, entity_data: ChunkDataDict[Entity]) -> None:
        self.entity_data: ChunkDataDict[Entity] = entity_data
        self.dirty: bool = False

    @staticmethod
    def _extract_chunks(
        locations: Iterable[SerializableLocation],
        timestamps: Iterable[Timestamp],
        data: memoryview,
    ) -> Iterable[ChunkDataBase[Entity]]:

        for i, (loc, ts) in enumerate(zip(locations, timestamps, strict=False)):
            if loc.size > 0 and loc.offset >= 2:
                start = loc.offset * Sizes.CHUNK_SIZE_MULTIPLIER
                entity_data = data[start : start + loc.size * Sizes.CHUNK_SIZE_MULTIPLIER]
                if entity := Entity.from_bytes(entity_data):
                    yield ChunkDataBase[Entity](data=entity, location=loc, timestamp=ts, index=i)

    def __bytes__(self) -> bytes:
        return RegionLike.to_bytes(self.entity_data)

    @classmethod
    def from_file(cls, file: Path) -> "EntitiesFile | None":
        with open(file, "+rb") as f:
            data = memoryview(f.read()).toreadonly()
            if len(data) < Sizes.LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE:
                return None
            chunk_location_data: bytes = data[: Sizes.LOCATION_DATA_SIZE]
            timestamps_data: bytes = data[
                Sizes.LOCATION_DATA_SIZE : Sizes.LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE
            ]

            locations: ArrayOfSerializable[SerializableLocation] = LocationData().from_bytes(chunk_location_data)
            timestamps: ArrayOfSerializable[Timestamp] = TimestampData().from_bytes(timestamps_data)

            entity_data: ChunkDataDict[Entity] = ChunkDataDict[Entity]()
            if len(chunk_location_data) > 0:
                for chunk in EntitiesFile._extract_chunks(locations, timestamps, data):
                    entity_data.append(chunk)

            return EntitiesFile(entity_data)

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
