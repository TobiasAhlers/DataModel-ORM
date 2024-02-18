from typing import Optional

import pytest
from pydantic.fields import FieldInfo

from data_model_orm import *
from data_model_orm.data_sources.utils import *


def test_int():
    assert is_nullable(FieldInfo(annotation=int)) == False


def test_optional_int():
    assert is_nullable(FieldInfo(annotation=Optional[int])) == True


def test_int_none():
    assert is_nullable(FieldInfo(annotation=int | None)) == True


def test_union_int_none():
    assert is_nullable(FieldInfo(annotation=Union[int, None])) == True


def test_union_str_int_none():
    assert is_nullable(FieldInfo(annotation=Union[str, int, None])) == True


def test_union_str_int():
    assert is_nullable(FieldInfo(annotation=Union[str, int])) == False
