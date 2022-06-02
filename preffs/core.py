import pandas as pd

import fsspec
from fsspec.asyn import AsyncFileSystem
import asyncio


class PRefFileSystem(AsyncFileSystem):
    """View byte ranges of some other file as a file system

    Needs a parquet table full of references, using the following columns:

    | key | path | offset | size | raw |

    Either (path, offset, size) or raw must be set.
    """

    protocol = "preffs"

    def __init__(self, fo, **kwargs):
        super().__init__(**kwargs)
        self._df = pd.read_parquet(fo)
        self._fss = {}

    def get_fs(self, protocol):
        try:
            return self._fss[protocol]
        except KeyError:
            fs = fsspec.filesystem(protocol)
            self._fss[protocol] = fs
            return fs

    def _gen_pieces(self, path, start=None, end=None):
        path = self._strip_protocol(path)
        current_offset = 0
        for row in self._df.loc[[path]].itertuples():
            if pd.isna(row.raw):
                size = int(row.size)
            else:
                size = len(row.raw)
            if start is None:
                piece_start = 0
            else:
                if start >= current_offset + size:
                    current_offset += size
                    continue
                else:
                    piece_start = max(0, start - current_offset)
            if end is None:
                piece_end = size
            else:
                if end <= current_offset:
                    current_offset += size
                    break
                else:
                    piece_end = min(end - current_offset, size)

            if pd.isna(row.raw):
                offset = int(row.offset)
                protocol, _ = fsspec.core.split_protocol(row.path)
                yield protocol, row.path, offset + piece_start, offset + piece_end
            else:
                yield row.raw[piece_start:piece_end]
            current_offset += size

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        def gen():
            for piece in self._gen_pieces(path, start, end):
                if isinstance(piece, tuple):
                    protocol, piece_path, piece_start, piece_end = piece
                    yield self.get_fs(protocol)._cat_file(piece_path, start=piece_start, end=piece_end)
                else:
                    f = asyncio.Future()
                    f.set_result(piece)
                    yield f
        return b''.join(await asyncio.gather(*gen()))

    def cat_file(self, path, start=None, end=None, **kwargs):
        def gen():
            for piece in self._gen_pieces(path, start, end):
                if isinstance(piece, tuple):
                    protocol, piece_path, piece_start, piece_end = piece
                    yield self.get_fs(protocol).cat_file(piece_path, start=piece_start, end=piece_end)
                else:
                    yield piece
        return b''.join(gen())

    def isdir(self, path):
        return len(self._df.loc[f"{path}/":"{path}0"]) > 0  # 0 is the ascii character following /

    def isfile(self, path):
        try:
            self._df.loc[path]
        except KeyError:
            return False
        else:
            return True
