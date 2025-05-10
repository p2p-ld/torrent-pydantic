"""
Types used only in v2 (and hybrid) torrents
"""

from typing import Annotated, NotRequired, TypeAlias, Union
from typing import Literal as L

from pydantic import AfterValidator, PlainSerializer
from pydantic_core.core_schema import SerializationInfo
from typing_extensions import TypeAliasType, TypedDict

from torrent_models.types.common import _divisible_by_16kib, _power_of_two

V2PieceLength = Annotated[int, AfterValidator(_divisible_by_16kib), AfterValidator(_power_of_two)]
"""
Per BEP 52: "must be a power of two and at least 16KiB"
"""


def _serialize_v2_hash(value: bytes, info: SerializationInfo) -> bytes | str | list[str]:
    if info.context and info.context.get("mode") == "print":
        ret: str = value.hex()

        if info.context.get("hash_truncate"):
            ret = ret[0:8]
        # split layers
        if len(ret) > 64:
            return [ret[i : i + 64] for i in range(0, len(ret), 64)]
        else:
            return ret

    return value


PieceLayerItem = Annotated[bytes, PlainSerializer(_serialize_v2_hash)]
PieceLayersType = dict[PieceLayerItem, PieceLayerItem]
FileTreeItem = TypedDict(
    "FileTreeItem", {"length": int, "pieces root": NotRequired[PieceLayerItem]}
)
FileTreeType: TypeAlias = TypeAliasType(  # type: ignore
    "FileTreeType", dict[bytes, Union[dict[L[""], FileTreeItem], "FileTreeType"]]  # type: ignore
)
