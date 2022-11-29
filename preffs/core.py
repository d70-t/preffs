import pandas as pd

import fsspec
from fsspec.spec import AbstractFileSystem
from fsspec.asyn import AsyncFileSystem, sync
import asyncio
from functools import lru_cache


def _first(d):
    return list(d.values())[0]


def _prot_in_references(path, df):
    subdf = df.loc[[path]]
    is_raw = not pd.isna(subdf.raw).all()
    if is_raw:
        return None
    has_raw = not pd.isna(subdf.raw).any()
    if has_raw:
        raise ValueError(f"mixed raw and references in one path: {path}")
    protocols = set(fsspec.core.split_protocol(p)[0] for p in subdf.path)
    if len(protocols) != 1:
        raise ValueErrir(f"not a unique protocol in references for {path}: {protocols}")
    return list(protocols)[0]


def _protocol_groups(paths, df):
    if isinstance(paths, str):
        return {_prot_in_references(paths, df): [paths]}
    out = {}
    for path in paths:
        protocol = _prot_in_references(path, df)
        out.setdefault(protocol, []).append(path)
    return out


class PRefFileSystem(AsyncFileSystem):
    """View byte ranges of some other file as a file system

    Needs a parquet table full of references, using the following columns:

    | key | path | offset | size | raw |

    Either (path, offset, size) or raw must be set.
    """

    protocol = "preffs"

    def __init__(
            self,
            fo,
            prefix=None,
            **kwargs):
        super().__init__(**kwargs)
        target_options = kwargs.get("target_options", {})
        target_protocol = kwargs.get("target_protocol", None)

        if target_protocol:
            extra = {"protocol": target_protocol}
        else:
            extra = {}

        with fsspec.open(fo, "rb", **target_options, **extra) as fh:
            self._df = pd.read_parquet(fh)

        if prefix is not None:
            self._df['path'] = prefix+self._df['path']

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
        try:
            return b''.join(await asyncio.gather(*gen()))
        except KeyError:
            raise FileNotFoundError(path)

    def cat_file(self, path, start=None, end=None, **kwargs):
        def gen():
            for piece in self._gen_pieces(path, start, end):
                if isinstance(piece, tuple):
                    protocol, piece_path, piece_start, piece_end = piece
                    yield self.get_fs(protocol).cat_file(piece_path, start=piece_start, end=piece_end)
                else:
                    yield piece
        try:
            return b''.join(gen())
        except KeyError:
            raise FileNotFoundError(path)

    def _select_dir(self, path):
        path = self._strip_protocol(path)
        if len(path) > 0:
            return self._df.loc[f"{path}/":f"{path}0"]
        else:
            return self._df

    def isdir(self, path):
        return len(self._select_dir(path)) > 0  # 0 is the ascii character following /

    def isfile(self, path):
        path = self._strip_protocol(path)
        try:
            self._df.loc[path]
        except KeyError:
            return False
        else:
            return True
    
    def exists(self, path):
        return self.isfile(path) or self.isdir(path)

    @classmethod
    def _strip_protocol(cls, path):
        return super()._strip_protocol(path).lstrip("/")

    @lru_cache(512)
    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        def gen():
            current = None
            if len(path) > 0:
                striplen = len(path) + 1
                dirprefix = path + "/"
            else:
                striplen = 0
                dirprefix = ""
            size = 0
            for row in self._select_dir(path).itertuples():
                if size and current != row.Index:
                    yield {"name": current, "size": size, "type": "file"}
                    size = 0
                is_dir = "/" in row.Index[striplen:]
                if is_dir:
                    dirname = dirprefix + row.Index[striplen:].split("/", 1)[0]
                    if current == dirname:
                        continue
                    else:
                        yield {"name": dirname, "size": 0, "type": "directory"}
                        current = dirname
                else:
                    if pd.isna(row.size):
                        size += len(row.raw)
                    else:
                        size += int(row.size)
                    current = row.Index
            if size:
                yield {"name": current, "size": size, "type": "file"}

        if detail:
            return list(gen())
        else:
            return [o["name"] for o in gen()]


    async def _ls(self, path, detail=True, **kwargs):
        return self.ls(path, detail, **kwargs)

    async def _info(self, path, **kwargs):
        try:
            entries = self._df.loc[[self._strip_protocol(path)]]
        except KeyError:
            if self.isdir(path):
                return {"name": path, "size": 0, "type": "directory"}
            else:
                return None
        else:
            return {"name": path, "size": sum(len(row.raw) if pd.isna(row.size) else int(row.size) for row in entries.itertuples()), "type": "file"}

    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        proto_dict = _protocol_groups(path, self._df)
        out = {}
        for proto, paths in proto_dict.items():
            if proto is None:
                # binary/string
                for p in paths:
                    try:
                        out[p] = self.cat_file(p, **kwargs)
                    except Exception as e:
                        if on_error == "raise":
                            raise
                        if on_error == "return":
                            out[p] = e

            elif self.get_fs(proto).async_impl:
                # TODO: asyncio.gather on multiple async FSs
                out.update(
                    sync(
                        self.loop,
                        self._cat,
                        paths,
                        recursive,
                        on_error=on_error,
                        **kwargs,
                    )
                )
            elif isinstance(paths, list):
                if recursive or any("*" in p for p in paths):
                    raise NotImplementedError
                for p in paths:
                    try:
                        out[p] = self.cat_file(p, **kwargs)
                    except Exception as e:
                        if on_error == "raise":
                            raise
                        if on_error == "return":
                            out[p] = e
            else:
                out.update(self.cat_file(paths))
        if len(out) == 1 and isinstance(path, str) and "*" not in path:
            return _first(out)
        return out
