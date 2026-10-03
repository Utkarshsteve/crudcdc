class CrudCDCError(Exception):
    """Base class for crudcdc errors."""


class UntrackedWriteError(CrudCDCError):
    """A bulk ORM statement would change a tracked table without recording changes."""


class EncodingError(CrudCDCError):
    """A column value can't be converted to JSON for the change feed."""


class InvalidCursorError(CrudCDCError, ValueError):
    """A cursor token is not one this library produced."""
