from pathlib import Path

import pytest
from torf import Torrent as TorfTorrent

from torrent_models import Torrent

DATA_DIR = (Path(__file__).parent / "data").resolve()

MULTI_HYBRID = DATA_DIR / "qbt_directory_hybrid.torrent"
MULTI_V1 = DATA_DIR / "qbt_directory_v1.torrent"
MULTI_V2 = DATA_DIR / "qbt_directory_v2.torrent"
GIANT_HYBRID = DATA_DIR / "qbt_giant_hybrid.torrent"
GIANT_V1 = DATA_DIR / "qbt_giant_v1.torrent"
GIANT_V2 = DATA_DIR / "qbt_giant_v@.torrent"

MULTI_FILE_HYBRID = Path(__file__).joinpath("../data/qbt_directory_hybrid.torrent").resolve()
GIANT_TORRENT = Path(__file__).joinpath("../data/qbt_giant_v1.torrent").resolve()


@pytest.fixture(
    params=[
        pytest.param(MULTI_HYBRID, id="multi-hybrid"),
        pytest.param(MULTI_V1, id="multi-v1"),
        pytest.param(MULTI_V2, id="multi-v2"),
        pytest.param(GIANT_HYBRID, id="giant-hybrid"),
        pytest.param(GIANT_V1, id="giant-v1"),
        pytest.param(GIANT_V2, id="giant-v2"),
    ]
)
def torrent_file(request) -> Path:
    if not request.param.exists():
        pytest.skip()
    return request.param


def test_benchmark_decode(benchmark, torrent_file):
    benchmark(Torrent.read, torrent_file)


def test_benchmark_decode_torf(benchmark, torrent_file, monkeypatch):
    monkeypatch.setattr(TorfTorrent, "MAX_TORRENT_FILE_SIZE", 100 * (2**20))
    if "v2" in str(torrent_file):
        pytest.xfail()
    benchmark(TorfTorrent.read, torrent_file)
