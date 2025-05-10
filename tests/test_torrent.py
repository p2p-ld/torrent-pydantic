from pathlib import Path

import libtorrent
import pytest

from torrent_models import Torrent
from torrent_models.types.common import TorrentVersion

from .conftest import DATA_DIR

ALL_TORRENTS = list(tf for tf in DATA_DIR.rglob("*.torrent") if "giant" not in tf.name)


def test_parse_hybrid():
    torrent = Torrent.read("tests/data/qbt_directory_hybrid.torrent")


@pytest.mark.parametrize(
    "tfile",
    [pytest.param(tf, id=str(tf.name)) for tf in ALL_TORRENTS],
)
def test_infohash(tfile: Path):
    """
    Test that the infohash that we get from a torrent is the same as what libtorrent would compute
    """
    lt_torrent = libtorrent.load_torrent_file(str(tfile))
    t = Torrent.read(tfile)

    if t.torrent_version in (TorrentVersion.v1, TorrentVersion.hybrid):
        assert t.v1_infohash is not None
        assert lt_torrent.info_hashes.has_v1()
        assert t.v1_infohash == lt_torrent.info_hashes.v1.to_bytes()
    if t.torrent_version in (TorrentVersion.v2, TorrentVersion.hybrid):
        assert t.v2_infohash is not None
        assert lt_torrent.info_hashes.has_v2()
        assert t.v2_infohash == lt_torrent.info_hashes.v2.to_bytes()
    if t.torrent_version not in TorrentVersion:
        raise ValueError("Torrent not detecting version")
