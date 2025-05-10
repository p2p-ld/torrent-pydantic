"""
Helpers for v2 torrents
"""

import concurrent.futures
import hashlib
import multiprocessing as mp
from collections import defaultdict, deque
from copy import deepcopy
from dataclasses import dataclass, field
from functools import cached_property
from math import ceil
from multiprocessing.pool import ApplyResult
from multiprocessing.pool import Pool as PoolType
from pathlib import Path
from typing import Union, cast, overload

from anyio import run
from pydantic import BaseModel
from tqdm.asyncio import tqdm

from torrent_models.compat import get_size
from torrent_models.hashing.base import BLOCK_SIZE, DummyPbar, Hash, PbarLike, iter_blocks
from torrent_models.types.v2 import FileTreeItem, FileTreeType


@dataclass
class MerkleTree:
    """
    Representation and computation of v2 merkle trees

    A v2 merkle tree is a branching factor 2 tree where each of the leaf nodes is a 16KiB block.

    Two layers of the tree are embedded in a torrent file:

    - the ``piece layer``: the hashes from ``piece length/16KiB`` layers from the leaves.
      or, the layer where each hash corresponds to a chunk of the file ``piece length`` long.
    - the tree root.

    Padding is performed in two steps:

    - For files whose size is not a multiple of ``piece length``,
      pad the *leaf hashes* with zeros
      (the hashes, not the leaf data, i.e. 32 bytes not 16KiB of zeros)
      such that there are enough blocks to complete a piece
    - For files there the number of pieces does not create a balanced merkle tree,
      pad the *pieces hashes* with identical piece hashes each ``piece length`` long
      s.t. their leaf hashes are all zeros, as above.

    These are separated because the padding added to the

    References:
        - https://www.bittorrent.org/beps/bep_0052_torrent_creator.py
    """

    path: Path
    """Absolute path to file on filesystem"""
    piece_length: int
    """Piece length, in bytes"""
    torrent_path: Path | None = None
    """Path within torrent file"""
    leaf_hashes: list[bytes] = field(default_factory=list)
    """SHA256 hashes of 16KiB leaf segments"""
    piece_hashes: list[bytes] = field(default_factory=list)
    """hashes of each piece (the nth later of the merkle tree, determined by piece length)"""
    root_hash: bytes | None = None
    """Root hash of the tree"""
    n_processes: int = mp.cpu_count()
    progress: bool = False
    """Show progress"""

    @classmethod
    def from_path(
        cls,
        path: Path,
        piece_length: int,
        n_processes: int = mp.cpu_count(),
        progress: bool = False,
        pool: PoolType | None = None,
    ) -> "MerkleTree":
        """
        Create a MerkleTree and return it with computed hashes
        """
        tree = MerkleTree(
            path=path,
            piece_length=piece_length,
            n_processes=n_processes,
            progress=progress,
        )
        _ = run(tree.hash_file, pool)
        return tree

    @classmethod
    def from_leaf_hashes(
        cls, hashes: list[Hash], base_path: Path, piece_length: int
    ) -> Union[list["MerkleTree"], "MerkleTree"]:
        """
        Create from a collection of leaf hashes.

        If leaf hashes from multiple paths are found, return a list of merkle trees.

        This method does *not* check that the trees are correct and complete -
        it assumes that the collection of leaf hashes passed to it is already complete.
        So e.g. it does not validate that the number of leaf hashes matches that which
        would be expected given the file size.

        Args:
            hashes (list[Hash]): collection of leaf hashes, from a single or multiple files
            base_path (Path): the base path that contains the relative paths in the leaf hashes
            piece_length (int): in bytes, you know what it is.
        """

        leaf_hashes = [h for h in hashes if h.type == "block"]
        leaf_hashes = sorted(leaf_hashes, key=lambda h: (h.path, h.idx))
        file_hashes = defaultdict(list)
        for h in leaf_hashes:
            file_hashes[h.path].append(h)

        trees = []
        for path, hashes in file_hashes.items():
            # this is increasingly disturbing, clean this up
            hash_bytes = [h.hash for h in hashes]
            tree = MerkleTree(
                path=base_path / path,
                torrent_path=path,
                piece_length=piece_length,
                progress=False,
                leaf_hashes=hash_bytes,
            )
            hash_bytes.extend([bytes(32)] * tree.n_pad_blocks)
            tree.leaf_hashes = hash_bytes
            tree.piece_hashes = tree.hash_pieces(hash_bytes)
            tree.root_hash = tree.get_root_hash(tree.piece_hashes)
            trees.append(tree)

        if len(trees) == 1:
            return trees[0]
        return trees

    async def hash_file(
        self, pool: PoolType | None = None
    ) -> tuple[list[bytes], list[bytes], bytes]:
        """
        Compute leaf hashes, then piece hashes, then root

        Returns:
            tuple: leaf_hashes, piece_hashes, root_hash
        """

        self.leaf_hashes = await self.hash_leaves(pool=pool)
        self.piece_hashes = self.hash_pieces(leaf_hashes=self.leaf_hashes)
        self.root_hash = self.get_root_hash(self.piece_hashes)

        return self.leaf_hashes, self.piece_hashes, self.root_hash

    async def hash_leaves(self, pool: PoolType | None = None) -> list[bytes]:
        """Iterate through leaf blocks, hashing"""
        made_pool = False
        if pool is None:
            made_pool = True
            pool = mp.Pool(processes=self.n_processes)

        read_pbar: PbarLike
        hash_pbar: PbarLike
        if self.progress:
            read_pbar = tqdm(total=self.n_blocks + self.n_pad_blocks, desc="Reading", position=1)
            hash_pbar = tqdm(total=self.n_blocks + self.n_pad_blocks, desc="Hashing", position=2)
        else:
            read_pbar = DummyPbar(total=self.n_blocks, desc="Reading")
            hash_pbar = DummyPbar(total=self.n_blocks, desc="Hashing")

        results: deque[ApplyResult] = deque()
        leaf_hashes = []

        async for chunk in iter_blocks(self.path):
            read_pbar.update(1)
            results.append(pool.apply_async(self.hash_block, (chunk.chunk, chunk.idx)))

            if len(results) == 0:
                continue
            res = results.popleft()
            try:
                leaf_hashes.append(res.get(timeout=0))
                hash_pbar.update()
            except mp.TimeoutError:
                results.appendleft(res)

        while len(results) > 0:
            res = results.popleft()
            leaf_hashes.append(res.get())
            hash_pbar.update(1)

        # if we made the pool, we close it
        if made_pool:
            pool.close()
        read_pbar.close()
        hash_pbar.close()

        leaf_hashes = sorted(leaf_hashes, key=lambda leaf: leaf[1])
        leaf_hashes = [leaf[0] for leaf in leaf_hashes]

        # add padding "hashes" of all 0s to make leaf hashes divisible by pieces
        leaf_hashes += [bytes(32)] * self.n_pad_blocks

        self.leaf_hashes = leaf_hashes
        return self.leaf_hashes

    def hash_pieces(self, leaf_hashes: list[bytes] | None = None) -> list[bytes]:
        """Compute the piece hashes for the layer dict"""
        if self.n_pieces <= 1:
            return []
        if leaf_hashes is None and len(self.leaf_hashes) == 0:
            raise ValueError("No precomputed leaf hashes and none passed!")
        elif leaf_hashes is None:
            leaf_hashes = self.leaf_hashes

        self._validate_leaf_count(leaf_hashes)
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.n_processes) as executor:
            piece_hashes = [
                self.hash_root(leaf_hashes[idx : idx + self.blocks_per_piece], executor=executor)
                for idx in range(0, len(leaf_hashes), self.blocks_per_piece)
            ]
        self.piece_hashes = piece_hashes
        return self.piece_hashes

    def get_root_hash(self, piece_hashes: list[bytes] | None = None) -> bytes:
        """
        Compute the root hash, including any zero-padding pieces needed to balance the tree.

        If n_pieces == 0, the root hash is just the hash tree of the blocks,
        padded with all-zero blocks to have enough blocks for a full piece
        """
        if self.n_pieces <= 1:
            self._validate_leaf_count(self.leaf_hashes)
            self.root_hash = self.hash_root(self.leaf_hashes)
            return self.root_hash

        if piece_hashes is None and len(self.piece_hashes) == 0:
            raise ValueError("No precomputed piece hashes and none passed!")
        elif piece_hashes is None:
            piece_hashes = self.piece_hashes

        if len(piece_hashes) == 1:
            return piece_hashes[0]

        if len(piece_hashes) == self.n_pieces and self.n_pad_pieces > 0:
            pad_piece_hash = self.hash_root([bytes(32)] * self.blocks_per_piece)
            piece_hashes = piece_hashes + ([pad_piece_hash] * self.n_pad_pieces)
        elif len(piece_hashes) != self.n_pieces + self.n_pad_pieces:
            raise ValueError(
                f"Expected either {self.n_pieces} (unpadded) piece hashes or "
                f"{self.n_pieces + self.n_pad_pieces} hashes "
                f"(with padding for merkle tree balance). "
                f"Got: {len(piece_hashes)}"
            )

        root_hash = self.hash_root(piece_hashes)
        self.root_hash = root_hash
        return root_hash

    @property
    def blocks_per_piece(self) -> int:
        return self.piece_length // BLOCK_SIZE

    @cached_property
    def file_size(self) -> int:
        return get_size(self.path)

    @cached_property
    def n_blocks(self) -> int:
        """Number of total blocks in the file (excluding padding blocks)"""
        return ceil(self.file_size / BLOCK_SIZE)

    @cached_property
    def n_pieces(self) -> int:
        """Number of pieces in the file (or 0, if file is < piece_length)"""
        n_pieces = self.file_size / self.piece_length
        if n_pieces < 1:
            return 0
        else:
            return ceil(n_pieces)

    @cached_property
    def n_pad_blocks(self) -> int:
        """Number of blank blocks required to reach piece length"""
        if self.n_pieces <= 1:
            total_blocks = 1 << (self.n_blocks - 1).bit_length()
            return total_blocks - self.n_blocks
        else:
            return self.n_blocks % self.blocks_per_piece

    @cached_property
    def n_pad_pieces(self) -> int:
        """Number of blank pieces required to balance merkle tree"""
        if self.n_pieces < 1:
            return 0
        return (1 << (self.n_pieces - 1).bit_length()) - self.n_pieces

    @classmethod
    @overload
    def hash_block(cls, block: bytes, idx: int) -> tuple[bytes, int]: ...

    @classmethod
    @overload
    def hash_block(cls, block: bytes) -> bytes: ...

    @classmethod
    def hash_block(cls, block: bytes, idx: int | None = None) -> bytes | tuple[bytes, int]:
        hash = hashlib.sha256(block).digest()
        if idx is not None:
            return hash, idx
        else:
            return hash

    @classmethod
    async def hash_block_async(cls, block: bytes) -> bytes:
        return cls.hash_block(block)

    @classmethod
    def hash_root(
        cls,
        hashes: list[bytes],
        n_processes: int = mp.cpu_count(),
        executor: concurrent.futures.ProcessPoolExecutor | None = None,
    ) -> bytes:
        """
        Given hashes of 16KiB leaves, compute their root.
        To compute the items in the piece layers dict,
        pass piece_length / 16KiB leaf hashes at a time.

        References:
            - https://www.bittorrent.org/beps/bep_0052_torrent_creator.py
        """
        assert len(hashes) & (len(hashes) - 1) == 0

        # if we are hashing a tiny amount of hashes, the process spawning overhead isn't worth it
        if (len(hashes) < (n_processes * 100) or n_processes == 1) and not executor:
            while len(hashes) > 1:
                hashes = [
                    hashlib.sha256(left + right).digest()
                    for left, right in zip(*[iter(hashes)] * 2)
                ]
        else:
            if executor:
                while len(hashes) > 1:
                    hashes = list(
                        executor.map(
                            cls.hash_block,
                            [left + right for left, right in zip(*[iter(hashes)] * 2)],
                        )
                    )
            else:
                with concurrent.futures.ProcessPoolExecutor(max_workers=n_processes) as executor:
                    while len(hashes) > 1:
                        hashes = list(
                            executor.map(
                                cls.hash_block,
                                [left + right for left, right in zip(*[iter(hashes)] * 2)],
                            )
                        )
        return hashes[0]

    def _validate_leaf_count(self, leaf_hashes: list[bytes]) -> None:
        """Ensure that we have the right number of leaves for a merkle tree"""
        if self.n_pieces == 0:
            # ensure that n_blocks is a power of two
            n = len(leaf_hashes)
            assert (n & (n - 1) == 0) and n != 0, (
                "For files smaller than one piece, "
                "must pad number of leaf blocks with zero blocks so n leaves is a power of two. "
                f"Got {len(leaf_hashes)} leaf hashes with blocks_per_piece {self.blocks_per_piece}"
            )
        else:
            assert len(leaf_hashes) % self.blocks_per_piece == 0, (
                f"leaf hashes must be a multiple of blocks per piece, pad with zeros. "
                f"Got {len(leaf_hashes)} leaf hashes with blocks_per_piece {self.blocks_per_piece}"
            )


class FileTree(BaseModel):
    """
    A v2 torrent file tree is like

    - `folder/file1.png`
    - `file2.png`

    ```
    {
        "folder": {
            "file1.png": {
                "": {
                    "length": 123,
                    "pieces root": b"<hash>",
                }
            }
        },
        "file2.png": {
            "": {
                "length": 123,
                "pieces root": b"<hash>",
            }
        }
    }
    ```
    """

    tree: FileTreeType

    @classmethod
    def flatten_tree(cls, tree: FileTreeType) -> dict[str, FileTreeItem]:
        """
        Flatten a file tree, mapping each path to the item description
        """
        return _flatten_tree(tree)

    @classmethod
    def unflatten_tree(cls, tree: dict[str, FileTreeItem]) -> FileTreeType:
        """
        Turn a flattened file tree back into a nested file tree
        """
        return _unflatten_tree(tree)

    @cached_property
    def flat(self) -> dict[str, FileTreeItem]:
        """Flattened FileTree"""
        return self.flatten_tree(self.tree)

    @classmethod
    def from_flat(cls, tree: dict[str, FileTreeItem]) -> "FileTree":
        return cls(tree=cls.unflatten_tree(tree))

    @classmethod
    def from_trees(cls, trees: list[MerkleTree], base_path: Path | None = None) -> "FileTree":
        flat = {}
        for tree in trees:
            if tree.torrent_path:
                # tree already knows its relative directory, use that
                rel_path = tree.torrent_path
            else:
                if base_path is None:
                    raise ValueError(
                        f"Merkle tree for {tree.path} does not have a torrent_path set,"
                        f"and no base_path was provided."
                        f"Unsure what relative path should go in a torrent file."
                    )
                rel_path = tree.path.relative_to(base_path)
            tree.root_hash = cast(bytes, tree.root_hash)
            flat[rel_path.as_posix()] = FileTreeItem(
                **{"pieces root": tree.root_hash, "length": get_size(tree.path)}
            )
        return cls.from_flat(flat)


def _flatten_tree(val: dict, parts: list[str] | list[bytes] | None = None) -> dict:
    # NOT a general purpose dictionary walker.
    out: dict[bytes | str, dict] = {}
    if parts is None:
        # top-level, copy the input value
        val = deepcopy(val)
        parts = []

    for k, v in val.items():
        if isinstance(k, bytes):
            k = k.decode("utf-8")
        if k in (b"", ""):
            if isinstance(k, bytes):
                parts = cast(list[bytes], parts)
                out[b"/".join(parts)] = v
            elif isinstance(k, str):
                parts = cast(list[str], parts)
                out["/".join(parts)] = v
        else:
            out.update(_flatten_tree(v, parts + [k]))
    return out


def _unflatten_tree(val: dict) -> dict:
    out: dict[str | bytes, dict] = {}
    for k, v in val.items():
        is_bytes = isinstance(k, bytes)
        if is_bytes:
            k = k.decode("utf-8")
        parts = k.split(b"/") if is_bytes else k.split("/")
        parts = [p for p in parts if p not in (b"", "")]
        nested_subdict = out
        for part in parts:
            if part not in nested_subdict:
                nested_subdict[part] = {}
                nested_subdict = nested_subdict[part]
            else:
                nested_subdict = nested_subdict[part]
        if is_bytes:
            nested_subdict[b""] = v
        else:
            nested_subdict[""] = v
    return out


@dataclass
class PieceLayers:
    """
    Constructor for piece layers, along with the file tree, from a list of files

    Constructed together since file tree is basically a mapping of paths to root hashes -
    they are joint objects
    """

    piece_length: int
    """piece length (hash piece_length/16KiB blocks per piece hash)"""
    piece_layers: dict[bytes, bytes]
    """piece layers: mapping from root hash to concatenated piece hashes"""
    file_tree: FileTree

    @classmethod
    def from_trees(
        cls, trees: list[MerkleTree] | MerkleTree, base_path: Path | None = None
    ) -> "PieceLayers":
        if not isinstance(trees, list):
            trees = [trees]
        lengths = [t.piece_length for t in trees]
        assert all(
            [lengths[0] == ln for ln in lengths]
        ), "Differing piece lengths in supplied merkle trees!"
        piece_length = lengths[0]
        piece_layers = {
            tree.root_hash: b"".join(tree.piece_hashes)
            for tree in trees
            if tree.piece_hashes and tree.root_hash is not None
        }
        file_tree = FileTree.from_trees(trees)
        return PieceLayers(
            piece_length=piece_length, piece_layers=piece_layers, file_tree=file_tree
        )

    @classmethod
    def from_paths(
        cls,
        paths: list[Path],
        piece_length: int,
        path_root: Path | None = None,
        n_processes: int = mp.cpu_count(),
        progress: bool = False,
    ) -> "PieceLayers":
        """
        Hash all the paths, construct the piece layers and file tree
        """
        if path_root is None:
            path_root = Path.cwd()

        file_pbar: PbarLike
        if progress:
            file_pbar = tqdm(total=len(paths), desc="Hashing files...", position=0)
        else:
            file_pbar = DummyPbar()

        piece_layers = {}
        file_tree = {}
        pool = mp.Pool(processes=n_processes)
        for path in paths:
            file_pbar.set_description(f"Hashing {path}")
            if path.is_absolute():
                raise ValueError(
                    f"Got absolute path {path}, "
                    f"paths must be relative unless you want to put the whole filesystem "
                    f"in a torrent. (don't put the whole filesystem in a torrent)."
                )
            abs_path = path_root / path
            tree = MerkleTree.from_path(
                path=abs_path, piece_length=piece_length, pool=pool, progress=progress
            )
            tree.root_hash = cast(bytes, tree.root_hash)
            file_tree[path.as_posix()] = FileTreeItem(
                **{"pieces root": tree.root_hash, "length": get_size(abs_path)}
            )
            if tree.piece_hashes:
                piece_layers[tree.root_hash] = b"".join(tree.piece_hashes)
            file_pbar.update()

        file_tree = FileTree.from_flat(file_tree)
        piece_layers = piece_layers

        file_pbar.close()
        pool.close()
        return PieceLayers(
            piece_length=piece_length,
            file_tree=file_tree,
            piece_layers=piece_layers,
        )
