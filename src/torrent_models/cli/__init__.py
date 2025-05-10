import warnings
from importlib.util import find_spec

__all__ = ["main"]

if find_spec("click"):

    from torrent_models.cli import main
else:
    warnings.warn(
        "cli dependencies are not installed - install with torrent-models[cli]", stacklevel=2
    )
