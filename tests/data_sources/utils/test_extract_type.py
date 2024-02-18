from typing import Optional, Union

import pytest

from data_model.data_sources.utils import *


def test_int():
    assert extract_type(int) == int


def test_optional_int():
    assert extract_type(Optional[int]) == int


def test_int_none():
    assert extract_type(int | None) == int


def test_union_int_none():
    assert extract_type(Union[int, None]) == int


def test_union_int_str():
    with pytest.raises(TypeError):
        extract_type(Union[int, str])


def test_union_int_str_none():
    with pytest.raises(TypeError):
        extract_type(Union[int, str, None])
