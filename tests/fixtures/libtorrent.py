import pytest

from torrent_models import testing
from torrent_models.torrent import Torrent

__all__ = [
    "libtorrent_pair",
]


@pytest.fixture(
    params=[
        pytest.param("v1", id="v1", marks=pytest.mark.v1),
        pytest.param("v2", id="v2", marks=pytest.mark.v2),
        pytest.param("hybrid", id="hybrid", marks=pytest.mark.hybrid),
    ],
)
def libtorrent_pair(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> tuple[dict, Torrent]:
    """
    A pair of basic torrents generated with libtorrent and our methods
    """
    tmp_path = tmp_path_factory.mktemp(request.param)
    version = request.param
    piece_length = 32 * (2**10)
    paths = testing.make_files(base=tmp_path, piece_length=piece_length)
    torrent = testing.default_torrent(
        paths=paths, base=tmp_path, piece_length=piece_length, version=version
    )
    libtorrent = testing.default_libtorrent(
        paths=paths, base=tmp_path, piece_length=piece_length, version=version
    )

    return libtorrent, torrent
