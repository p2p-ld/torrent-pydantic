import multiprocessing as mp
from abc import abstractmethod
from collections import deque
from collections.abc import AsyncGenerator
from functools import cached_property
from itertools import count
from math import ceil
from multiprocessing.pool import ApplyResult, AsyncResult
from multiprocessing.pool import Pool as PoolType
from pathlib import Path
from typing import Any, Generic, TypeAlias, TypeVar, overload
from typing import Literal as L

from anyio import open_file, run
from pydantic import BaseModel, Field
from tqdm import tqdm

from torrent_models.compat import get_size
from torrent_models.types.v1 import V1PieceLength
from torrent_models.types.v2 import V2PieceLength

BLOCK_SIZE = 16 * (2**10)

T = TypeVar("T")
"""Type of completed result of hasher"""


class Chunk(BaseModel):
    """A single unit of data, usually a 16KiB block, but can be a whole piece e.g. in v1 hashing"""

    path: Path
    """Absolute path"""
    chunk: bytes
    idx: int


class Hash(BaseModel):
    """Hash of a block or piece"""

    type: L["block", "v1_piece", "v2_piece"]
    path: Path
    hash: bytes
    idx: int = Field(
        ...,
        description="""
    The index of the block for ordering.
    
    For v1 hashes, the absolute index of piece across all files.
    For v2 block and piece hashes, index within the given file
    """,
    )


async def iter_blocks(path: Path, read_size: int = BLOCK_SIZE) -> AsyncGenerator[Chunk, None]:
    """Iterate 16KiB blocks"""
    counter = count()
    last_size = read_size
    async with await open_file(path, "rb") as f:
        while last_size == read_size:
            read = await f.read(read_size)
            if len(read) > 0:
                yield Chunk.model_construct(idx=next(counter), path=path, chunk=read)
            last_size = len(read)


class HasherBase(BaseModel, Generic[T]):
    paths: list[Path]
    """
    Relative paths beneath the path base to hash.
    
    Paths should already be sorted in the order they are to appear in the torrent
    """
    path_base: Path
    """Directory containing paths to hash"""
    piece_length: V1PieceLength | V2PieceLength
    n_processes: int = Field(default_factory=mp.cpu_count)
    progress: bool = False
    """Show progress"""
    read_size: int = BLOCK_SIZE
    """
    How much of a file should be read in a single read call.
    """
    memory_limit: int | None = None
    """
    Rough cap on outstanding memory usage (in bytes) - pauses reading more data until
    the number of outstanding chunks to process are smaller than this size
    """

    @abstractmethod
    def update(self, chunk: Chunk, pool: PoolType) -> list[AsyncResult]:
        """
        Update hasher with a new chunk of data, returning a list of AsyncResults to fetch hashes
        """
        pass

    @abstractmethod
    def complete(self, hashes: list[Hash]) -> T:
        """After hashing, do any postprocessing to yield the desired output"""
        pass

    def _after_read(self, pool: PoolType) -> list[AsyncResult]:
        """Optional step after reading completes"""
        return []

    @cached_property
    def total_chunks(self) -> int:
        """Total read_size chunks in all files"""
        total_chunks = 0
        for path in self.paths:
            total_chunks += ceil(get_size(self.path_base / path) / self.read_size)
        return total_chunks

    @cached_property
    def total_hashes(self) -> int:
        """Total hashes that need to be computed"""
        return self.total_chunks

    @cached_property
    def max_outstanding_results(self) -> int | None:
        """Total number of async result objects that can be outstanding, to limit memory usage"""
        if self.memory_limit is None:
            return None
        else:
            return self.memory_limit // self.read_size

    async def process_async(self) -> T:
        hashes = await self.hash()
        return self.complete(hashes)

    def process(self) -> T:
        return run(self.process_async)

    async def hash(self) -> list[Hash]:
        """
        Hash all files
        """
        pool = mp.Pool(self.n_processes)
        pbars = self._pbars()

        hashes = []
        results: deque[ApplyResult] = deque()
        try:
            for path in self.paths:

                pbars.file.set_description(str(path))

                async for chunk in iter_blocks(self.path_base / path, read_size=self.read_size):
                    pbars.read.update()
                    res = self.update(chunk, pool)
                    results.extend(res)
                    results, hash = self._step_results(results)
                    if hash is not None:
                        hashes.append(hash)
                        pbars.hash.update()

                    if self.max_outstanding_results:
                        while len(results) > self.max_outstanding_results:
                            results, hash = self._step_results(results, block=True)
                            hashes.append(hash)
                            pbars.hash.update()

                pbars.file.update()

            results.extend(self._after_read(pool))
            while len(results) > 0:
                results, hash = self._step_results(results, block=True)
                hashes.append(hash)
                pbars.hash.update()

        finally:
            pool.close()
            pbars.close()

        return hashes

    @overload
    def _step_results(self, results: deque, block: L[True]) -> tuple[deque, Hash]: ...

    @overload
    def _step_results(self, results: deque, block: L[False]) -> tuple[deque, Hash | None]: ...

    @overload
    def _step_results(self, results: deque) -> tuple[deque, Hash | None]: ...

    def _step_results(self, results: deque, block: bool = False) -> tuple[deque, Hash | None]:
        """Step the outstanding results, yielding a single hash"""
        res = results.popleft()
        if block:
            return results, res.get()
        else:
            try:
                return results, res.get(timeout=0)
            except mp.TimeoutError:
                # she not done yet
                results.appendleft(res)
                return results, None

    def _pbars(self) -> "_PBars":
        return _PBars(
            dummy=not self.progress,
            file_total=len(self.paths),
            read_total=self.total_chunks,
            hash_total=self.total_hashes,
        )


class DummyPbar:
    """pbar that does nothing so we i don't get fined by mypy"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def update(self, n: int = 1) -> None:
        pass

    def close(self) -> None:
        pass

    def set_description(self, *args: Any, **kwargs: Any) -> None:
        pass


PbarLike: TypeAlias = DummyPbar | tqdm


class _PBars:
    """
    Wrapper around multiple pbars, including dummy pbars for when progress is disabled
    """

    def __init__(
        self,
        file_total: int | None = None,
        read_total: int | None = None,
        hash_total: int | None = None,
        dummy: bool = False,
    ):
        self.file: PbarLike
        self.read: PbarLike
        self.hash: PbarLike
        if dummy:
            self.file = DummyPbar()
            self.read = DummyPbar()
            self.hash = DummyPbar()
        else:
            self.file = tqdm(total=file_total, desc="File", position=0)
            self.read = tqdm(total=read_total, desc="Reading Chunk", position=1)
            self.hash = tqdm(total=hash_total, desc="Hashing Chunk", position=2)

    def close(self) -> None:
        self.file.close()
        self.read.close()
        self.hash.close()
