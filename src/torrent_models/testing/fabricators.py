import string
from itertools import product
from pathlib import Path
from random import randbytes, randint, random

from torrent_models import Torrent, TorrentCreate, TorrentVersion

try:
    import libtorrent
except ImportError:
    libtorrent = None  # type: ignore


def make_paths(n: int = 10, p_descend: float = 0.3, p_ascend: float = 0.1) -> list[Path]:
    if p_descend < p_ascend:
        raise ValueError("p_descend must be greater than p_ascend")

    paths = []
    letters = iter("".join([*letters]) for letters in product(string.ascii_letters, repeat=4))
    # ensure one root path
    paths.append(Path(next(letters)))
    if n == 1:
        return paths
    parent: Path | None = None
    while len(paths) < n:
        path = parent / next(letters) if parent else Path(next(letters))
        roll = random()
        if roll <= p_descend:
            parent = path
            continue
        elif roll <= p_ascend:
            parent = None
            continue
        paths.append(path)

    return paths


def make_files(
    base: Path, paths: list[Path] | None = None, piece_length: int = 32 * (2**10)
) -> list[Path]:
    if paths is None:
        paths = make_paths()

    paths = [base / path for path in paths]
    paths[0].parent.mkdir(exist_ok=True, parents=True)
    paths[0].write_bytes(randbytes(randint(0, piece_length - 1)))
    if len(paths) == 1:
        return paths
    paths[1].parent.mkdir(exist_ok=True, parents=True)
    paths[1].write_bytes(randbytes(randint(piece_length + 1, 3 * piece_length)))
    for path in paths[2:]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(randbytes(randint(0, 3 * piece_length)))
    return paths


def default_tcreate(
    paths: list[Path], base: Path, piece_length: int = 32 * (2**10)
) -> TorrentCreate:
    return TorrentCreate(
        piece_length=piece_length,
        paths=paths,
        path_root=base,
        trackers=["udp://example.com:6969/announce", "http://example.com/announce"],
        similar=[bytes(20)],
        webseeds=["https://example.com/data", "https://example.com/data2"],
    )


def default_torrent(
    paths: list[Path],
    base: Path,
    piece_length: int = 32 * (2**10),
    version: TorrentVersion = TorrentVersion.hybrid,
) -> Torrent:
    creator = default_tcreate(paths, base, piece_length)
    return creator.generate(version=version)


def default_libtorrent(
    paths: list[Path],
    base: Path,
    piece_length: int = 32 * (2**10),
    version: TorrentVersion = TorrentVersion.hybrid,
    output: Path | None = None,
    bencode: bool = False,
) -> dict | bytes:
    creator = default_tcreate(paths, base, piece_length)
    return creator.generate_libtorrent(version=version, output=output, bencode=bencode)
