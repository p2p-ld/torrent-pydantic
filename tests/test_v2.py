import pytest

from torrent_models.hashing.v2 import FileTree

# add test cases of paired flattened and unflattened file trees here:

FILE_TREES = (
    {
        "directory": {
            "file1.exe": {"": {"length": 1, "pieces root": "sup"}},
            "subdir": {"file2.exe": {"": {"length": 2, "pieces root": "sup"}}},
        },
        "file3.exe": {"": {"length": 3, "pieces root": "sup"}},
    },
)

FLAT_FILE_TREES = (
    {
        "directory/file1.exe": {"length": 1, "pieces root": "sup"},
        "directory/subdir/file2.exe": {"length": 2, "pieces root": "sup"},
        "file3.exe": {"length": 3, "pieces root": "sup"},
    },
)


@pytest.mark.parametrize("tree,flat", zip(FILE_TREES, FLAT_FILE_TREES))
def test_flatten_tree(tree, flat):
    """
    We can flatten a tree!
    """
    assert FileTree.flatten_tree(tree) == flat


@pytest.mark.parametrize("tree,flat", zip(FILE_TREES, FLAT_FILE_TREES))
def test_unflatten_tree(tree, flat):
    """
    We can unflatten a tree!
    """
    assert FileTree.unflatten_tree(flat) == tree


@pytest.mark.parametrize("tree,flat", zip(FILE_TREES, FLAT_FILE_TREES))
def test_roundtrip_tree(tree, flat):
    """
    We can roundtrip flatten and unflattening trees!
    """
    assert FileTree.unflatten_tree(FileTree.flatten_tree(tree)) == tree
    assert FileTree.flatten_tree(FileTree.unflatten_tree(flat)) == flat
