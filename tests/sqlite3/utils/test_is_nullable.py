import pytest

from typing import Optional

from data_model import *
from data_model.sqlite3.utils import *


def test_int():
    assert is_nullable(int) == (False, int)


def test_optional_int():
    assert is_nullable(Optional[int]) == (True, int)


def test_int_none():
    assert is_nullable(int | None) == (True, int)


def test_float():
    assert is_nullable(float) == (False, float)


def test_union_int_none():
    assert is_nullable(Union[int, None]) == (True, int)
