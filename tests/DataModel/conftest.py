import pytest

from typing import Optional
from data_model_orm import *
from sqlmodel import create_engine


class TestDataModel(DataModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    name: str
    age: int


@pytest.fixture
def test_data_model() -> TestDataModel:
    TestDataModel.__engine__ = create_engine("sqlite:///:memory:")
    TestDataModel.create_source()
    return TestDataModel