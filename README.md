# torrent-models

.torrent file parsing and creation with pydantic
(and models for other bittorrent things too)

~ alpha software primarily intended for use with [sciop](https://codeberg.org/Safeguarding/sciop) ~

## Initial development

- [x] Parsing
  - [x] v1
  - [x] v2
- [x] Generation
  - [x] v1
  - [x] v2
  - [ ] hybrid

## See also

These are also good projects, and probably more battle tested
(but we don't know them well and can't vouch for their use):

- [`torrentfile`](https://alexpdev.github.io/torrentfile/)
- [`dottorrent`](https://dottorrent.readthedocs.io)
- [`torf`](https://github.com/rndusr/torf)
- [`torrenttool`](https://github.com/idlesign/torrentool)
- [`PyBitTorrent`](https://github.com/gaffner/PyBitTorrent)
- [`torrent_parser`](https://github.com/7sDream/torrent_parser)

The reason this package exists is that none of them are a pure, complete
parser and generator of torrent files that...
- can handle v1, v2, hybrid, and all other .torrent-related BEPs
- is focused on library usage
- is simple and focused
- has few dependencies
- is performant
- uses modern python typing

Specifically
- `torf` has some notable performance problems, and doesn't support v2
- `torrentfile` is focused on the cli and doesn't appear to be able to validate torrent files, 
  and there is no dedicated method for parsing them, 
  e.g. editing [directly manipulates the bencoded dict](https://github.com/alexpdev/torrentfile/blob/d50d942dc72c93f052c63b443aaec38c592a14df/torrentfile/edit.py#L65)
  and [rebuilding requires the files to be present](https://github.com/alexpdev/torrentfile/blob/d50d942dc72c93f052c63b443aaec38c592a14df/torrentfile/rebuild.py)
- `dottorrent` can only write, not parse torrent files.
- `torrenttool` doesn't validate torrents
- `PyBitTorrent` doesn't validate torrents
- `torrent_parser` doesn't validate torrents and doesn't have a torrent file class