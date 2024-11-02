import pytest

from sqlmodel import select
from data_model_orm import *
from conftest import *


def test_create_source():
    TestDataModel.__engine__ = create_engine("sqlite:///:memory:")
    TestDataModel.create_source()

    with TestDataModel.__engine__.begin() as conn:
        result = conn.execute(select(TestDataModel))
        assert result.all() == []
        conn.commit()


def test_create_source_ignore_if_exists():
    TestDataModel.__engine__ = create_engine("sqlite:///:memory:")
    TestDataModel.create_source()
    TestDataModel.create_source(ignore_if_exists=True)

    with TestDataModel.__engine__.begin() as conn:
        result = conn.execute(select(TestDataModel))
        assert result.all() == []
        conn.commit()


def test_create_source_ignore_if_exists_false():
    TestDataModel.__engine__ = create_engine("sqlite:///:memory:")
    TestDataModel.create_source()
    with pytest.raises(Exception):
        TestDataModel.create_source()
