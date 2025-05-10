from datetime import timedelta
from pathlib import Path
from time import time
from typing import Literal as L
from typing import cast

import click
import humanize
from rich import print
from rich.console import Group
from rich.pretty import Pretty
from rich.table import Table

from torrent_models import TorrentCreate
from torrent_models.compat import get_size
from torrent_models.const import DEFAULT_TORRENT_CREATOR
from torrent_models.create import list_files
from torrent_models.info import InfoDictHybrid, InfoDictHybridCreate, InfoDictV1, InfoDictV2
from torrent_models.torrent import Torrent
from torrent_models.types.common import TorrentVersion
from torrent_models.types.v1 import FileItem, V1PieceLength
from torrent_models.types.v2 import V2PieceLength


@click.group("torrent")
def main() -> None:
    """
    torrent-models CLI
    """


@main.command("make")
@click.option(
    "-p",
    "--path",
    required=True,
    help="Path to a directory or file to create .torrent from",
    type=click.Path(exists=True),
)
@click.option(
    "-t",
    "--tracker",
    required=False,
    default=None,
    multiple=True,
    help="Trackers to add to the torrent. can be used multiple times for multiple trackers. ",
)
@click.option(
    "-s",
    "--piece-size",
    default=512 * (2**10),
    help="Piece size, in bytes",
    show_default=True,
)
@click.option(
    "--comment",
    default=None,
    required=False,
    help="Optional comment field for torrent",
)
@click.option(
    "--creator",
    default=DEFAULT_TORRENT_CREATOR,
    show_default=True,
    required=False,
    help="Optional creator field for torrent",
)
@click.option(
    "-w",
    "--webseed",
    required=False,
    default=None,
    multiple=True,
    help="Add HTTP webseeds as additional sources for torrent. Can be used multiple times. "
    "See https://www.bittorrent.org/beps/bep_0019.html",
)
@click.option(
    "--similar",
    required=False,
    default=None,
    multiple=True,
    help="Add infohash of a similar torrent. "
    "Similar torrents are torrents who have files in common with this torrent, "
    "clients are able to reuse files from the other torrents if they already have them downloaded.",
)
@click.option(
    "--version",
    default="hybrid",
    type=click.Choice(["v1", "v2", "hybrid"]),
    help="What kind of torrent to create, default is hybrid",
)
@click.option("--progress/--no-progress", default=True, help="Enable progress bar (default True)")
@click.option(
    "-o",
    "--output",
    required=False,
    default=None,
    type=click.Path(exists=False),
    help=".torrent file to write to. Otherwise to stdout",
)
def make(
    path: Path,
    tracker: list[str] | tuple[str] | None = None,
    piece_size: V1PieceLength | V2PieceLength = 512 * (2**10),
    comment: str | None = None,
    creator: str = DEFAULT_TORRENT_CREATOR,
    webseed: list[str] | None = None,
    similar: list[str] | None = None,
    version: L["v1", "v2", "hybrid"] = "hybrid",
    progress: bool = True,
    output: Path | None = None,
) -> None:
    path = Path(path)
    files = list_files(path)
    start_time = time()
    created = TorrentCreate(
        trackers=tracker,
        files=files,
        path_root=path,
        comment=comment,
        creator=creator,
        info=InfoDictHybridCreate(piece_length=piece_size, name=path.name),
    )
    generated = created.generate(version=version, progress=progress)
    bencoded = generated.bencode()
    if output:
        with open(output, "wb") as f:
            f.write(bencoded)

        torrent_size = get_size(Path(output))

        end_time = time()
        duration = end_time - start_time
        total_size = generated.info.total_length
        speed = total_size / duration
        click.echo(
            f"Created torrent {output}\n"
            f"Total size: {humanize.naturalsize(total_size, binary=True)}\n"
            f"Torrent size: {humanize.naturalsize(torrent_size, binary=True)}\n"
            f"Duration: {humanize.naturaldelta(timedelta(seconds=duration))}\n"
            f"Speed: {humanize.naturalsize(speed, binary=True)}/s"
        )
    else:
        click.echo(bencoded)


@main.command("print")
@click.argument("torrent", type=click.Path(exists=True))
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="""
              Verbose output, repeat for increasing verbosity\n
              
              -v show files in separate table
              -vv show truncated v1 piece hashes
              -vvv show everything as-is
              """,
)
def pprint(torrent: Path, verbose: int = 0) -> None:
    """
    Print the contents of a torrent file.

    By default, prints only the top-level metadata in a way that should always be
    smaller than one screen.

    Increase verbosity to show more of the torrent.

    Hashes are printed as hexadecimal numbers and split into individual pieces,
    but they are properly encoded in the torrent.
    """
    torrent = Path(torrent)
    t = Torrent.read(torrent)
    # summary stats
    summary = {
        "# Files": humanize.number.intcomma(t.n_files),
        "Total Size": humanize.naturalsize(t.total_size, binary=True),
        "Torrent Size": humanize.naturalsize(get_size(torrent), binary=True),
        "Piece Size": humanize.naturalsize(t.info.piece_length, binary=True),
    }
    v1_infohash = t.v1_infohash
    v2_infohash = t.v2_infohash
    if v1_infohash:
        summary["V1 Infohash"] = v1_infohash.hex()
    if v2_infohash:
        summary["V2 Infohash"] = v2_infohash.hex()
    table = Table(title=str(torrent.name), show_header=False)
    table.add_column("", justify="left", style="magenta bold", no_wrap=True)
    table.add_column("")
    for k, v in summary.items():
        table.add_row(k, v)

    exclude = {}
    context = {"mode": "print", "hash_truncate": True}
    file_table = None
    if verbose <= 1:
        exclude = {"info": {"pieces", "file tree", "file_tree", "files"}, "piece_layers": True}
    elif verbose <= 2:
        exclude = {"info": {"file tree", "file_tree", "files"}, "piece_layers": True}
    else:
        context["hash_truncate"] = False

    # make file table
    if 1 <= verbose <= 2:
        file_table = Table(title="Files")
        file_table.add_column("Path", no_wrap=True)
        file_table.add_column("Size")

        if t.torrent_version == TorrentVersion.v1:
            t.info = cast(InfoDictV1, t.info)
            tfiles = (
                t.info.files
                if t.info.files is not None
                else [FileItem(path=t.info.name, length=t.info.length)]
            )

            files = [
                ("/".join(f.path), humanize.naturalsize(f.length, binary=True), "")
                for f in tfiles
                if f.attr not in (b"p", "p")
            ]
        else:
            t.info = cast(InfoDictV2 | InfoDictHybrid, t.info)
            file_table.add_column("Hash")
            tree = t.flat_files
            assert tree is not None
            files = [
                (
                    str(k),
                    humanize.naturalsize(v["length"], binary=True),
                    v["pieces root"].hex()[0:8],
                )
                for k, v in tree.items()
            ]

        for f in files:
            file_table.add_row(*f)

    dumped = t.model_dump(
        by_alias=True, exclude=exclude, exclude_none=True, context=context  # type: ignore
    )

    if verbose < 1 or verbose > 2:
        group = Group(
            table,
            Pretty(dumped),
        )
    elif verbose <= 2:
        assert file_table is not None
        group = Group(table, Pretty(dumped), file_table)

    print(group)
