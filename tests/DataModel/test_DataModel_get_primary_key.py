import pytest

from data_model_orm import *
from conftest import *


def test_get_primary_key(test_data_model: TestDataModel):
    assert TestDataModel().get_primary_key() == "id"
