"""
Types used only in model serialization and deserialization:
AKA types that do not represent concrete types/fields in a torrent file
"""

from datetime import datetime
from typing import Annotated, TypeVar, cast

from pydantic import AnyUrl, BaseModel, BeforeValidator, PlainSerializer, WrapSerializer
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler


def _timestamp_to_datetime(val: int | datetime) -> datetime:
    if isinstance(val, int | float):
        val = datetime.fromtimestamp(val)
    return val


def _datetime_to_timestamp(val: datetime) -> int:
    return round(val.timestamp())


UnixDatetime = Annotated[
    datetime, BeforeValidator(_timestamp_to_datetime), PlainSerializer(_datetime_to_timestamp)
]
EXCLUDE_STRINGIFY = ("piece_layers", "piece layers", "path")


def str_keys(
    value: dict | list | BaseModel, _isdict: bool | None = None
) -> dict | list | BaseModel:
    """
    Convert the byte-encoded keys of a bencoded dictionary to strings,
    avoiding value of keys we know to be binary encoded like piece layers.

    .. note: Performance Notes

        AKA why this function looks so weird.

        doing isinstance check at the start is slightly faster than EAFP and allocating new_value,
        but use th internal bool _isdict to avoid doing it twice when we have already checked
        the argument's type in a parent call

        since we only call this with fresh output from bencode, we know that we only have
        python builtins, and therefore we can check with the classname rather than isinstance,
        which is slightly faster. this means that this function is *not* a general purpose function,
        and should only be used on freshly decoded bencoded data!

        We iterate over lists in the ``dict`` leg rather than in the function root to avoid the
        final double type check for leaf nodes, checking only if they are dictionaries.
    """

    if _isdict or value.__class__.__name__ == "dict":
        new_value = {}
        value = cast(dict, value)
        for k, v in value.items():
            try:
                if isinstance(k, bytes):
                    k = k.decode("utf-8")
            except UnicodeDecodeError:
                # fine, e.g. for piece maps whose keys are not utf-8 encoded
                # any invalid keys will get caught in validation
                pass

            if k in EXCLUDE_STRINGIFY:
                new_value[k] = v
                continue

            if v.__class__.__name__ == "dict":
                v = str_keys(v, _isdict=True)
            elif v.__class__.__name__ == "list":
                v = [str_keys(item) for item in v]
            new_value[k] = v
        return new_value
    else:
        return value


def str_keys_list(value: list[dict | BaseModel]) -> list[dict | list | BaseModel]:
    return [str_keys(v) for v in value]


def _to_str(value: str | bytes) -> str:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return value


def _to_bytes(value: str | bytes, info: SerializationInfo) -> bytes | str:
    if info.context and info.context.get("mode") == "print":
        return str(value)

    if isinstance(value, str):
        value = value.encode("utf-8")
    elif isinstance(value, AnyUrl):
        value = str(value).encode("utf-8")
    else:
        value = str(value).encode("utf-8")
    return value


ByteStr = Annotated[str, PlainSerializer(_to_bytes)]
ByteUrl = Annotated[AnyUrl, PlainSerializer(_to_bytes)]
_Inner = TypeVar("_Inner")


def _to_list(val: _Inner | list[_Inner]) -> list[_Inner]:
    if val and not isinstance(val, list):
        return [val]
    else:
        val = cast(list[_Inner], val)
        return val


def _from_list(
    val: list[_Inner] | None, handler: SerializerFunctionWrapHandler
) -> list[_Inner] | _Inner | None:
    partial = handler(val)
    if not partial:
        return None
    elif len(partial) == 1:
        return partial[0]
    else:
        return partial


ListOrValue = Annotated[list[_Inner], BeforeValidator(_to_list), WrapSerializer(_from_list)]
