import pytest

from data_model_orm import *
from sqlmodel import Session
from conftest import *


def test_DataModel_get_one_existing(test_data_model: TestDataModel):
    with Session(test_data_model.__engine__) as session:
        session.add_all(
            [
                TestDataModel(id=1, name="John Doe", age=30),
                TestDataModel(id=2, name="Jane Doe", age=30),
            ]
        )
        session.commit()

    result = TestDataModel.get_one(id=1)
    assert result.id == 1
    assert result.name == "John Doe"
    assert result.age == 30


def test_DataModel_get_one_non_existing(test_data_model: TestDataModel):
    with Session(test_data_model.__engine__) as session:
        session.add_all(
            [
                TestDataModel(id=1, name="John Doe", age=30),
                TestDataModel(id=2, name="Jane Doe", age=30),
            ]
        )
        session.commit()

    result = TestDataModel.get_one(id=3)
    assert result is None


def test_DataModel_get_one_multiple_results(test_data_model):
    with Session(test_data_model.__engine__) as session:
        session.add_all(
            [
                TestDataModel(id=1, name="John Doe", age=30),
                TestDataModel(id=2, name="Jane Doe", age=30),
            ]
        )
        session.commit()

    assert TestDataModel.get_one(age=30) == TestDataModel(id=1, name="John Doe", age=30)
