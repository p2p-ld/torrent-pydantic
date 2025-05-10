"""
Routines that interaact with torrents using libtorrent

https://www.rasterbar.com/products/libtorrent/index.html

Libtorrent should be imported nowhere else in the library,
because libtorrent is optional.

When using functions from this module elsewhere in the library,
import them within the scope of the function or method,
not the module.
"""

from pathlib import Path
from typing import TYPE_CHECKING, cast

import bencode_rs
from tqdm import tqdm

from torrent_models.compat import get_size
from torrent_models.types.common import TorrentVersion

if TYPE_CHECKING:

    from torrent_models import TorrentCreate

try:
    import libtorrent
except ImportError as e:
    raise ImportError(
        "libtorrent is not installed."
        "install it with pip or with the optional dependency group torrent-models[libtorrent]"
    ) from e


def create_from_model(
    torrent: "TorrentCreate",
    version: TorrentVersion | str = TorrentVersion.hybrid,
    output: Path | None = None,
    bencode: bool = False,
    progress: bool = False,
) -> dict | bytes:
    """
    Create a .torrent file using libtorrent

    .. admonition:: Incomplete!
        :class: warning

        This function is not guaranteed to dump all fields set in your model.
        Libtorrent is an imperative function creator and can't set arbitrary metadata.
        This is primarily intended for ensuring backwards compatibility and testing

    Args:
        torrent (:class:`.TorrentCreate`): Torrent creation object
        version (:class:`.TorrentVersion`): Torrent version to create - strings or enum values
        output (Path | None): If present, write torrent to file
        bencode (bool): If True, bencode before returning
        progress (bool): If True, show progress bar while hashing
    """
    if isinstance(version, str):
        version = TorrentVersion.__members__[version]

    fs = libtorrent.file_storage()
    for path in torrent.get_paths():
        fs.add_file(str(path), get_size(torrent.path_root / path))

    flags = 0
    if version == TorrentVersion.v1:
        flags = libtorrent.create_torrent.v1_only
    elif version == TorrentVersion.v2:
        flags = libtorrent.create_torrent.v2_only

    torrent.piece_length = cast(int, torrent.piece_length)
    created = libtorrent.create_torrent(fs, piece_size=torrent.piece_length, flags=flags)

    trackers = torrent.get_trackers()
    flat_trackers = [trackers.get("announce", None)]

    for tier in trackers.get("announce-list", []):
        flat_trackers.extend(tier)

    flat_trackers = list(dict.fromkeys([str(t) for t in flat_trackers if t is not None]))

    for tier, tracker in enumerate(flat_trackers):
        created.add_tracker(tracker, tier)

    if torrent.webseeds:
        for webseed in torrent.webseeds:
            created.add_url_seed(str(webseed))

    if torrent.similar:
        for s in torrent.similar:
            created.add_similar_torrent(libtorrent.sha1_hash(s))

    if torrent.comment:
        created.set_comment(torrent.comment)

    if torrent.created_by is not None:
        created.set_creator(torrent.created_by)

    _pbar = None
    if progress:
        _pbar = tqdm(desc="hashing pieces...", total=created.num_pieces())

        def _pbar_callback(piece_index: int) -> None:
            _pbar.update()

        libtorrent.set_piece_hashes(created, str(torrent.path_root.resolve()), _pbar_callback)
        _pbar.close()
    else:
        libtorrent.set_piece_hashes(created, str(torrent.path_root.resolve()))

    ret = created.generate()
    if bencode or output:
        bencoded = bencode_rs.bencode(ret)
        if output:
            with open(output, "wb") as f:
                f.write(bencoded)
        if bencode:
            return bencoded

    return ret
