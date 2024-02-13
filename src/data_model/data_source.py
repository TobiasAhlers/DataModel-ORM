from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .base import DataModel


class DataSource:
    def __init__(self) -> None:
        pass

    def create_source(self, data_model: type["DataModel"]) -> None:
        raise NotImplementedError()

    def get_one(self, data_model: type["DataModel"], where: dict) -> Union["DataModel", None]:
        raise NotImplementedError()

    def get_all(
        self, data_model: type["DataModel"], where: dict = None
    ) -> list["DataModel"]:
        raise NotImplementedError()

    def save(self, data_model: "DataModel") -> None:
        raise NotImplementedError()

    def delete(self, data_model: "DataModel") -> None:
        raise NotImplementedError()
