import pytest

from data_model_orm.base import DataModel, ORMConfig


test_location = "test.db"
expected_dict = {
    "db_location": test_location,
    "ignore_if_exists": False,
    }

def test_default_orm_config():
    assert ORMConfig().db_location == "database.db"
    assert ORMConfig().__dict__ == {"db_location": "database.db", "ignore_if_exists": False}


def test_configured_orm_config():
    assert ORMConfig(db_location=test_location).db_location == test_location
    assert ORMConfig(db_location=test_location).__dict__ == expected_dict


def test_configured_orm_config_instance():
    test_instance = ORMConfig(db_location=test_location)
    assert test_instance.db_location == test_location
    assert test_instance.__dict__ == expected_dict


def test_default_data_model():
    assert DataModel.__orm_config__ == ORMConfig()
    assert DataModel.__data_source__.database == "database.db"


def test_configured_data_model():
    
    class TestModel(DataModel):
        __orm_config__ = ORMConfig(
            db_location=test_location
            )
    
    assert TestModel.__orm_config__ == ORMConfig(db_location=test_location)
    # assert TestModel.__data_source__.database == test_location


def test_configured_data_model_instance():
    
    class TestModel(DataModel):
        __orm_config__ = ORMConfig(
            db_location=test_location
            )
    
    test_instance = TestModel()
    
    assert test_instance.__orm_config__ == ORMConfig(db_location=test_location)
    assert test_instance.__data_source__.database == test_location


def test_config_not_accepted_as_argument():
    test_instance = DataModel(
        __orm_config__ = ORMConfig(
            db_location=test_location
            )
        )
    assert test_instance.__orm_config__ == ORMConfig()
    assert test_instance.__data_source__.database == "database.db"