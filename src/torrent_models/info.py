import hashlib
from math import ceil
from typing import Annotated, Self, cast

import bencode_rs
from annotated_types import Gt, MinLen
from pydantic import Field, model_validator

from torrent_models.base import ConfiguredBase
from torrent_models.hashing.v2 import FileTree
from torrent_models.types.serdes import ByteStr
from torrent_models.types.v1 import FileItem, Pieces, V1PieceLength
from torrent_models.types.v2 import FileTreeItem, FileTreeType, V2PieceLength


class InfoDictRoot(ConfiguredBase):
    """Fields shared by v1 and v2 infodicts"""

    name: ByteStr | None = None
    source: ByteStr | None = None

    _total_length: int | None = None

    @property
    def v1_infohash(self) -> bytes | None:
        return None

    @property
    def v2_infohash(self) -> bytes | None:
        return None


class InfoDictV1Base(InfoDictRoot):
    pieces: Pieces | None = None
    length: Annotated[int, Gt(0)] | None = None
    files: Annotated[list[FileItem], MinLen(1)] | None = Field(None)
    piece_length: V1PieceLength | None = Field(alias="piece length")

    @property
    def v1_infohash(self) -> bytes:
        """SHA-1 hash of the infodict"""
        dumped = self.model_dump(exclude_none=True, by_alias=True)
        bencoded = bencode_rs.bencode(dumped)
        return hashlib.sha1(bencoded).digest()

    @property
    def total_length(self) -> int:
        """Total length of all files, in bytes"""
        if self._total_length is None:
            total = 0
            if not self.files:
                return total

            for f in self.files:
                total += f.length
            self._total_length = total
        return self._total_length

    @model_validator(mode="after")
    def disallowed_fields(self) -> Self:
        """
        We allow extra fields, but not those in v2 infodicts, in order to make them discriminable
        """
        if isinstance(self.__pydantic_extra__, dict):
            assert "file tree" not in self.__pydantic_extra__, "V1 Infodicts can't have file_trees"
        return self

    @model_validator(mode="after")
    def expected_n_pieces(self) -> Self:
        """We have the expected number of pieces given the sizes implied by our file dict"""
        if self.pieces is None or self.piece_length is None:
            return self
        n_pieces = ceil(self.total_length / self.piece_length)
        assert n_pieces == len(self.pieces), (
            f"Expected {n_pieces} pieces for torrent with "
            f"total length {self.total_length} and piece_length"
            f"{self.piece_length}"
            f"Got {len(self.pieces)}"
        )
        return self


class InfoDictV1(InfoDictV1Base):
    """An infodict from a valid V1 torrent"""

    name: ByteStr
    pieces: Pieces
    piece_length: V1PieceLength = Field(alias="piece length")

    @model_validator(mode="after")
    def length_xor_files(self) -> Self:
        """
        There is also a key length or a key files, but not both or neither.
        If length is present then the download represents a single file,
        otherwise it represents a set of files which go in a directory structure.
        """
        assert bool(self.length) != bool(
            self.files
        ), "V1 Torrents must have a `length` or `files`,  but not both."
        return self


class InfoDictV1Create(InfoDictV1Base):
    """v1 Infodict that may or may not have its pieces hashed yet"""

    pass


class InfoDictV2Base(InfoDictRoot):
    meta_version: int = Field(2, alias="meta version")
    file_tree: FileTreeType | None = Field(None, alias="file tree")
    piece_length: V2PieceLength | None = Field(alias="piece length")

    @model_validator(mode="after")
    def disallowed_fields(self) -> Self:
        """
        We allow extra fields, but not those in v1 infodicts, in order to make them discriminable
        """
        if isinstance(self.__pydantic_extra__, dict):
            assert "pieces" not in self.__pydantic_extra__, "V2 Infodicts can't have pieces"
        return self

    @property
    def v2_infohash(self) -> bytes:
        """SHA-256 hash of the infodict"""
        dumped = self.model_dump(exclude_none=True, by_alias=True)
        bencoded = bencode_rs.bencode(dumped)
        return hashlib.sha256(bencoded).digest()

    @property
    def flat_tree(self) -> dict[str, FileTreeItem]:
        """Flattened file tree! mapping full paths to tree items"""
        if self.file_tree is None:
            return {}
        else:
            return FileTree.flatten_tree(self.file_tree)

    @property
    def total_length(self) -> int:
        """
        Total length of all files, in bytes.
        """
        total_length = 0
        for file in self.flat_tree.values():
            total_length += file["length"]
        return total_length


class InfoDictV2(InfoDictV2Base):
    """An infodict from a valid V2 torrent"""

    name: ByteStr
    piece_length: V2PieceLength = Field(alias="piece length")
    file_tree: FileTreeType = Field(alias="file tree", exclude=False)


class InfoDictV2Create(InfoDictV2Base):
    pass


class InfoDictHybridCreate(InfoDictV1Create, InfoDictV2Create):
    """An infodict of a hybrid torrent that may or may not have its pieces hashed yet"""

    @model_validator(mode="after")
    def disallowed_fields(self) -> Self:
        """hybrids can have any additional fields"""
        return self

    name: ByteStr | None = None
    piece_length: V1PieceLength | V2PieceLength | None = Field(None, alias="piece length")


class InfoDictHybrid(InfoDictV2, InfoDictV1):
    """An infodict of a valid v1/v2 hybrid torrent"""

    piece_length: V2PieceLength = Field(alias="piece length")

    @model_validator(mode="after")
    def disallowed_fields(self) -> Self:
        """hybrids can have any additional fields"""
        return self

    @model_validator(mode="after")
    def expected_n_pieces(self) -> Self:
        """We have the expected number of pieces given the sizes implied by our file dict"""
        if self.pieces is None:
            return self
        if self.files is not None:
            n_pieces = ceil(sum([f.length for f in self.files]) / self.piece_length)
        else:
            self.length = cast(int, self.length)
            n_pieces = ceil(self.length / self.piece_length)

        assert n_pieces == len(self.pieces), (
            f"Expected {n_pieces} pieces for torrent with "
            f"total length {self.total_length} and piece_length"
            f"{self.piece_length}. "
            f"Got {len(self.pieces)}"
        )
        return self
