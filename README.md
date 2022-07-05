# PREFFS

**WARNING:** this is experimental software, it seems to work on the specific use cases it's being tried with (i.e. opening kerchunk-like zarr's), but I'd not be surprised if unexpected behaviour comes up in any corner.

parquet-reference-filesystem

This is a fsspec-reference filesystem based on `parquet`-files. The parquet data must look like:

| key | path | offset | size | raw |
|:---:|:----:|:------:|:----:|:---:|
| a/b | a.dat | 123 | 14 | - |
| a/b | b.dat | 12 | 17 | - |
| b   | - | - | - | foo |

Where `key` is the filename as seen by the users of the preffs-filesystem, `path` is the internal path to a data chunk, `offset` is the number of bytes into the internal path, `size` is the number of bytes in this chunk and `raw` is a bytestring of raw content. Either `path`, `offset` and `size` **or** `raw` must be set. It's not allowed to set both.
`key`s may be repeated in which case all chunks are concatenated together.

## opening

Just like anything in `fsspec`. You can use the `preffs::` protocol, e.g. `xr.open_zarr("preffs::some.parquet")`.
