class DataModelError(Exception):
    """
    Base class for all data model errors.
    """


class DataSourceError(DataModelError):
    """
    Base class for all data source errors.
    """


class DataSourceAlreadyExistsError(DataSourceError):
    """
    Raised when trying to create a data source that already exists.
    """
