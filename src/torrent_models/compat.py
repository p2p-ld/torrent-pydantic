import os.path
import sys
from pathlib import Path


def get_size(path: Path) -> int:
    """
    Windows, helpfully, reports different sizes to stat,
    so we have to use the os.path.getsize() function instead.

    This *may* not be necessary, but is left in place in case we need
    compatibility for windows sizes in the future,
    which can be impacted by the NTFS filesystem.
    """
    if sys.platform == "win32":
        return os.path.getsize(path)
    else:
        return path.stat().st_size
