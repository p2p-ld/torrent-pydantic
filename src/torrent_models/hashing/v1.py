import hashlib
import multiprocessing
import os
from collections import deque
from itertools import count
from math import ceil, floor
from pathlib import Path
from typing import TYPE_CHECKING, Generator, cast

from tqdm import tqdm

from torrent_models.compat import get_size
from torrent_models.types.v1 import Pieces, V1PieceLength

if TYPE_CHECKING:
    from multiprocessing.pool import AsyncResult


def hash_pieces(
    paths: list[Path] | Path,
    piece_length: V1PieceLength,
    path_root: Path | None = None,
    sort: bool = True,
    n_processes: int | None = None,
    max_memory_size: int | None = None,
    progress: bool = False,
) -> Pieces:
    """
    Given a list of files and piece length, return a concatenated series of SHA1 hashes.

    This does *not* check for correctness of files, e.g. that they have some common root directory.
    It will raise a FileNotFoundError if the file does not exist.

    Args:
        paths (list[Path]): List of files to hash
        piece_length (V1PieceLength): Length of each piece (in bytes).
        path_root (Path | None): If paths are relative, they are relative to this directory.
            (cwd if None)
        sort (bool): Sort the files, default True
        n_processes (int | None): Number of parallel processes to use.
            If `None` , use number of CPUs present
        max_memory_size (int | None): Maximum amount of memory to be used at once (in bytes).
            Not *really* a memory cap, but it prevents us from reading
            more pieces into memory until our outstanding hash queue
            is smaller than max_memory_size / piece_length.
            If None, go hogwild on em (no limit).
        progress (bool): If `True`, progress bar will be displayed
    """
    pieces: list[tuple[int, bytes]] = []
    if isinstance(paths, Path):
        paths = [paths]

    if sort:
        paths = sorted(paths)

    if path_root is None:
        path_root = Path.cwd()

    paths = [path_root / path if not path.is_absolute() else path for path in paths]

    read_pbar = None
    hash_pbar = None
    if progress:
        total_pieces = ceil(sum(get_size(file) for file in paths) / piece_length)
        read_pbar = tqdm(total=total_pieces, desc="Reading pieces", position=0)
        hash_pbar = tqdm(total=total_pieces, desc="Hashing pieces", position=1)

    if not n_processes:
        n_processes = multiprocessing.cpu_count()
    if max_memory_size is not None:
        max_outstanding_results = floor(max_memory_size / piece_length)
    else:
        max_outstanding_results = None

    pool = multiprocessing.Pool(n_processes)

    iterator = piece_iter(paths, piece_length)
    last_file = None
    results: deque[AsyncResult] = deque()
    for idx, file, piece in iterator:
        if progress:
            read_pbar = cast(tqdm, read_pbar)
            if file != last_file:
                read_pbar.set_description(f"Reading {file.name}")
            read_pbar.update(1)

        last_file = file
        results.append(pool.apply_async(_hash_piece, (idx, piece)))

        # try to get a piece hash
        if len(results) == 0:
            continue
        res = results.popleft()
        try:
            pieces.append(res.get(timeout=0))
            if progress:
                hash_pbar = cast(tqdm, hash_pbar)
                hash_pbar.update(1)
        except multiprocessing.TimeoutError:
            # she not done yet
            results.appendleft(res)

        # don't read until our hash queue settles down a bit
        if max_outstanding_results:
            while len(results) >= max_outstanding_results:
                pieces.append(results.popleft().get())
                if progress:
                    hash_pbar = cast(tqdm, hash_pbar)
                    hash_pbar.update(1)

    # drain the queue
    while len(results) > 0:
        res = results.popleft()
        pieces.append(res.get())
        if progress:
            hash_pbar = cast(tqdm, hash_pbar)
            hash_pbar.update(1)

    # sort pieces but don't concat - we handle concat, deconcat in the type
    pieces = sorted(pieces, key=lambda piece: piece[0])
    piece_hashes = [piece[1] for piece in pieces]

    if progress:
        read_pbar = cast(tqdm, read_pbar)
        hash_pbar = cast(tqdm, hash_pbar)

        read_pbar.close()
        hash_pbar.close()
    pool.close()

    return piece_hashes


def _hash_piece(idx: int, piece: bytes) -> tuple[int, bytes]:
    return idx, hashlib.sha1(piece).digest()


def piece_iter(
    files: list[Path], piece_length: int
) -> Generator[tuple[int, Path, bytes], None, None]:
    """yield pieces from a list of files"""
    piece = None
    if len(files) == 0:
        raise ValueError("No files in list to iterate!")
    file = None
    idx = count()

    for file in files:
        with open(file, "rb") as f:
            pos = f.tell()
            total_size = f.seek(0, os.SEEK_END)
            f.seek(pos)
            while f.tell() < total_size:
                if piece is not None:
                    piece += f.read(piece_length - len(piece))
                else:
                    piece = f.read(piece_length)

                if len(piece) == piece_length:
                    yield next(idx), file, piece
                    piece = None

    # yield the final, potentially incomplete piece, if any
    if file is not None and piece is not None:
        yield next(idx), file, piece
