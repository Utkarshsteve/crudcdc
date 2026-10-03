"""crudcdc: async CRUD on SQLAlchemy with a built-in change data capture feed."""

from .capture import register_encoder, track
from .crud import AsyncCRUD
from .errors import CrudCDCError, EncodingError, InvalidCursorError, UntrackedWriteError
from .feed import Batch, ChangeEvent, ChangeFeed
from .models import CDCBase, Change, ConsumerOffset

__all__ = [
    "AsyncCRUD",
    "Batch",
    "CDCBase",
    "Change",
    "ChangeEvent",
    "ChangeFeed",
    "ConsumerOffset",
    "CrudCDCError",
    "EncodingError",
    "InvalidCursorError",
    "UntrackedWriteError",
    "register_encoder",
    "track",
]
__version__ = "0.1.0"
