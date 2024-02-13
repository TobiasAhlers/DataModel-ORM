import pytest

from typing import Optional
from pydantic import Field
from data_model import *
from data_model.sqlite3.utils import *


def test_id_and_attr():
    class ModelTest(DataModel):
        id: Optional[int] = Field(
            json_schema_extra={"primary_key": True, "autoincrement": True}
        )
        name: str = "NAME"

    assert generate_deletion_query(ModelTest) == "DELETE FROM ModelTest WHERE id = ?"
