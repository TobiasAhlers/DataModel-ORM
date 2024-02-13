from src.data_model import *

from typing import Optional
from pydantic import Field


class TestModel(DataModel):
    id: Optional[int] = Field(
        json_schema_extra={"primary_key": True, "autoincrement": True}, default=None
    )
    name: str
    age: int


# TestModel.create_source()

test = TestModel(name="test", age=20)
test.save()

print(TestModel.get_one({"id": 1}))
