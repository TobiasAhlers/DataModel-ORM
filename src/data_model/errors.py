class DataModelError(Exception):
    pass


class UnsupportedTypeError(DataModelError):
    pass


class MissingPrimaryKeyError(DataModelError):
    pass


class MultiplePrimaryKeysError(DataModelError):
    pass


class AutoIncrementError(DataModelError):
    pass


class InvalidForeignKeyError(DataModelError):
    pass


class QueryError(DataModelError):
    pass


class DataSourceError(Exception):
    pass


class SourceAlreadyExistsError(DataSourceError):
    pass


class SourceDoesNotExistError(DataSourceError):
    pass


class InvalidSourceError(DataSourceError):
    pass