from math import ceil
from pathlib import Path
from typing import Any, Self, cast
from typing import Literal as L

import bencode_rs
from pydantic import Field, model_validator

from torrent_models.base import ConfiguredBase
from torrent_models.const import DEFAULT_TORRENT_CREATOR
from torrent_models.hashing.v2 import FileTree
from torrent_models.info import (
    InfoDictHybrid,
    InfoDictV1,
    InfoDictV1Base,
    InfoDictV2,
    InfoDictV2Base,
)
from torrent_models.types import (
    ByteStr,
    ByteUrl,
    FileTreeItem,
    ListOrValue,
    PieceLayersType,
    TorrentVersion,
    UnixDatetime,
    str_keys,
)


class TorrentBase(ConfiguredBase):
    announce: ByteUrl
    announce_list: list[list[ByteUrl]] | None = Field(default=None, alias="announce-list")
    comment: ByteStr | None = None
    created_by: ByteStr | None = Field(DEFAULT_TORRENT_CREATOR, alias="created by")
    creation_date: UnixDatetime | None = Field(default=None, alias="creation date")
    info: InfoDictV1 | InfoDictV2 | InfoDictHybrid = Field(..., union_mode="left_to_right")
    piece_layers: PieceLayersType | None = Field(None, alias="piece layers")
    url_list: ListOrValue[ByteUrl] | None = Field(
        None, alias="url-list", description="List of webseeds"
    )

    @property
    def webseeds(self) -> list[ByteUrl] | None:
        """alias to url_list"""
        return self.url_list

    @classmethod
    def read(cls, path: Path | str) -> Self:
        with open(path, "rb") as tfile:
            tdata = tfile.read()
        tdict = bencode_rs.bdecode(tdata)
        return cls.from_decoded(decoded=tdict)

    @classmethod
    def from_decoded(cls, decoded: dict[str | bytes, Any], **data: Any) -> Self:
        """Create from bdecoded dict"""
        if decoded is not None:
            # we fix these incompatible types in str_keys
            decoded.update(data)  # type: ignore
            data = decoded  # type: ignore

        if any([isinstance(k, bytes) for k in data]):
            data = str_keys(data)  # type: ignore
        return cls(**data)

    @property
    def torrent_version(self) -> TorrentVersion:
        if isinstance(self.info, InfoDictV1Base) and not isinstance(self.info, InfoDictV2Base):
            return TorrentVersion.v1
        elif isinstance(self.info, InfoDictV2Base) and not isinstance(self.info, InfoDictV1Base):
            return TorrentVersion.v2
        else:
            return TorrentVersion.hybrid

    @property
    def v1_infohash(self) -> bytes | None:
        return self.info.v1_infohash

    @property
    def v2_infohash(self) -> bytes | None:
        return self.info.v2_infohash

    @property
    def n_files(self) -> int:
        """
        Total number of files described by the torrent, excluding padfiles
        """

        if self.torrent_version in (TorrentVersion.v1, TorrentVersion.hybrid):
            self.info = cast(InfoDictV1 | InfoDictHybrid, self.info)
            if self.info.files is None:
                return 1
            return len([f for f in self.info.files if f.attr not in (b"p", "p")])
        else:
            self.info = cast(InfoDictV2, self.info)
            tree = FileTree.flatten_tree(self.info.file_tree)
            return len(tree)

    @property
    def total_size(self) -> int:
        """
        Total size of the torrent, excluding padfiles, in bytes
        """
        if self.torrent_version in (TorrentVersion.v1, TorrentVersion.hybrid):
            self.info = cast(InfoDictV1 | InfoDictHybrid, self.info)
            if self.info.files is None:
                self.info.length = cast(int, self.info.length)
                return self.info.length
            return sum([f.length for f in self.info.files if f.attr not in (b"p", "p")])
        else:
            self.info = cast(InfoDictV2, self.info)
            tree = FileTree.flatten_tree(self.info.file_tree)
            return sum([t["length"] for t in tree.values()])

    @property
    def flat_files(self) -> dict[str, FileTreeItem] | None:
        """A flattened version of the v2 file tree"""
        if self.torrent_version == TorrentVersion.v1:
            return None
        self.info = cast(InfoDictV2, self.info)
        return FileTree.flatten_tree(self.info.file_tree)

    def model_dump_torrent(self, mode: L["str", "binary"] = "binary", **kwargs: Any) -> dict:
        """
        Dump the model into a dictionary that can be bencoded into a torrent

        Args:
            mode ("str", "binary"): ``str`` returns as a 'python' version of the torrent,
                with string keys and serializers applied.
                ``binary`` roundtrips to and from bencoding.
            kwargs: forwarded to :meth:`pydantic.BaseModel.model_dump`
        """
        dumped = self.model_dump(exclude_none=True, by_alias=True, **kwargs)
        if mode == "binary":
            dumped = bencode_rs.bdecode(bencode_rs.bencode(dumped))
        return dumped


class Torrent(TorrentBase):
    """
    A valid torrent file, including hashes.
    """

    @model_validator(mode="after")
    def piece_layers_if_v2(self) -> Self:
        """If we are a v2 or hybrid torrent, we should have piece layers"""
        if self.torrent_version in (TorrentVersion.v2, TorrentVersion.hybrid):
            assert self.piece_layers is not None, "Hybrid and v2 torrents must have piece layers"
        return self

    @model_validator(mode="after")
    def pieces_layers_correct(self) -> Self:
        """
        All files with a length longer than the piece length should be in piece layers,
        Piece layers should have the correct number of hashes
        """
        if self.torrent_version == TorrentVersion.v1:
            return self
        self.piece_layers = cast(dict[bytes, bytes], self.piece_layers)
        self.info = cast(InfoDictV2 | InfoDictHybrid, self.info)
        for path, file_info in self.info.flat_tree.items():
            if file_info["length"] > self.info.piece_length:
                assert file_info["pieces root"] in self.piece_layers, (
                    f"file {path} does not have a matching piece root in the piece layers dict. "
                    f"Expected to find: {file_info['pieces root']}"  # type: ignore
                )
                expected_pieces = ceil(file_info["length"] / self.info.piece_length)
                assert len(self.piece_layers[file_info["pieces root"]]) == expected_pieces * 32, (
                    f"File {path} does not have the correct number of piece hashes. "
                    f"Expected {expected_pieces} hashes from file length {file_info['length']} "
                    f"and piece length {self.info.piece_length}. "
                    f"Got {len(self.piece_layers[file_info['pieces root']]) / 32}"
                )
        return self

    def bencode(self) -> bytes:
        dumped = self.model_dump(exclude_none=True, by_alias=True)
        return bencode_rs.bencode(dumped)
