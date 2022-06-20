import os
from tempfile import TemporaryDirectory
import pandas as pd

import pytest

from preffs import PRefFileSystem


@pytest.fixture
def testdata():
    with TemporaryDirectory() as d:
        datafilename = os.path.join(d, "data.bin")
        with open(datafilename, "wb") as df:
            df.write(b"0123456789")
        df = pd.DataFrame([
            ["a/b", datafilename, 0, 4, None],
            ["a/b", datafilename, 6, 4, None],
            ["a/c", datafilename, 0, 10, None],
            ["b", None, None, None, b"test"]],
            columns=["key", "path", "offset", "size", "raw"]).set_index("key")
        reffilename = os.path.join(d, "reference.parquet")
        df.to_parquet(reffilename)
        yield reffilename


def test_basic_preffs(testdata):
    fs = PRefFileSystem(testdata)
    assert fs.cat_file("a/b") == b"01236789"
    assert fs.cat_file("a/c") == b"0123456789"
    assert fs.cat_file("b") == b"test"

def test_ls(testdata):
    fs = PRefFileSystem(testdata)
    assert fs.ls("a", detail=False) == ["a/b", "a/c"]
    assert fs.ls("", detail=False) == ["a", "b"]
    assert fs.ls("a/", detail=False) == ["a/b", "a/c"]

def test_exists(testdata):
    fs = PRefFileSystem(testdata)
    assert fs.exists("a")
    assert fs.exists("a/")
    assert fs.exists("a/b")
    assert fs.exists("b")
    assert not fs.exists("c")
    assert not fs.exists("c/")

def test_mapper(testdata):
    m = PRefFileSystem(testdata).get_mapper()
    assert m["a/b"] == b"01236789"

