from sqlite3 import connect, Connection
from typing import TYPE_CHECKING

from ..data_source import DataSource

from .utils import *


if TYPE_CHECKING:
    from ..base import DataModel


class SQLite3DataSource(DataSource):

    def __init__(self, database: str) -> None:
        self.database = database

    def connect(self) -> Connection:
        return connect(self.database)

    def create_source(
        self, data_model: type["DataModel"], ignore_if_exists: bool = False
    ) -> None:
        raise NotImplementedError()

    def get_one(
        self, data_model: type["DataModel"], where: dict
    ) -> Union["DataModel", None]:
        raise NotImplementedError()

    def get_all(
        self, data_model: type["DataModel"], where: dict = None
    ) -> list["DataModel"]:
        raise NotImplementedError()

    def save(self, data_model: type["DataModel"]) -> None:
        raise NotImplementedError()

    def delete(self, data_model: type["DataModel"]) -> None:
        raise NotImplementedError()
