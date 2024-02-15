from typing import TYPE_CHECKING, Union, Any

if TYPE_CHECKING:
    from .base import DataModel


class DataSource:
    """
    DataSource is a base class for data sources. It defines the interface for data sources.
    Each method should be overridden by a subclass that implements the specific data source.
    """

    def __init__(self) -> None:
        """
        Initialize a new instance of DataSource.
        """
        pass

    def create_source(self, data_model: type["DataModel"]) -> None:
        """
        Create a new data source.

        Args:
            data_model (type["DataModel"]): The data model to create the source for.

        Raises:
            NotImplementedError: This method must be overridden by a subclass.

        Example:
            >>> ds = DataSourceSubclass()
            >>> ds.create_source(MyDataModel)
        """
        raise NotImplementedError()

    def get_one(
        self, data_model: type["DataModel"], where: dict
    ) -> Union["DataModel", None]:
        """
        Get a single record from the data source.

        Args:
            data_model (type["DataModel"]): The data model to get the record for.
            where (dict): A dictionary of conditions to match the record against.

        Returns:
            Union["DataModel", None]: The matching record, or None if no record was found.

        Raises:
            NotImplementedError: This method must be overridden by a subclass.

        Example:
            >>> ds = DataSourceSubclass()
            >>> record = ds.get_one(MyDataModel, {"id": 1})
        """
        raise NotImplementedError()

    def get_all(
        self, data_model: type["DataModel"], where: dict = None
    ) -> list["DataModel"]:
        """
        Get all records from the data source that match the given conditions.

        Args:
            data_model (type["DataModel"]): The data model to get the records for.
            where (dict, optional): A dictionary of conditions to match the records against. Defaults to None.

        Returns:
            list["DataModel"]: A list of matching records.

        Raises:
            NotImplementedError: This method must be overridden by a subclass.

        Example:
            >>> ds = DataSourceSubclass()
            >>> records = ds.get_all(MyDataModel, {"name": "John"})
        """
        raise NotImplementedError()

    def save(self, data_model: "DataModel") -> Any:
        """
        Save a record to the data source.

        Args:
            data_model ("DataModel"): The data model to save.

        Returns:
            Any: The result of the save operation.

        Raises:
            NotImplementedError: This method must be overridden by a subclass.

        Example:
            >>> ds = DataSourceSubclass()
            >>> record = MyDataModel(name="John", age=30)
            >>> ds.save(record)
        """
        raise NotImplementedError()

    def delete(self, data_model: "DataModel") -> None:
        """
        Delete a record from the data source.

        Args:
            data_model ("DataModel"): The data model to delete.

        Example:
            >>> ds = DataSourceSubclass()
            >>> record = ds.get_one(MyDataModel, {"id": 1})
            >>> ds.delete(record)
        """
        raise NotImplementedError()
