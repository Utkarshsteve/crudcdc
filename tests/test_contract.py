"""The ChangeEvent shape is the public contract: changing it breaks consumers."""

import dataclasses

from sqlalchemy.ext.asyncio import AsyncSession

from crudcdc import ChangeEvent

from .helpers import events
from .models import User

FIELDS = ["seq", "op", "table", "pk", "before", "after", "changed", "tx_id", "changed_at"]


def golden(e: ChangeEvent) -> dict[str, object]:
    d = dataclasses.asdict(e)
    for volatile in ("seq", "tx_id", "changed_at"):
        d.pop(volatile)
    return d


async def test_event_fields() -> None:
    assert [f.name for f in dataclasses.fields(ChangeEvent)] == FIELDS


async def test_event_shapes(session: AsyncSession) -> None:
    session.add(u := User(id=1, name="ada"))
    await session.commit()
    u.name = "grace"
    await session.commit()
    await session.delete(u)
    await session.commit()
    row_a = {"id": 1, "name": "ada", "joined": None}
    row_g = {"id": 1, "name": "grace", "joined": None}
    assert [golden(e) for e in await events(session)] == [
        {
            "op": "insert",
            "table": "users",
            "pk": {"id": 1},
            "before": None,
            "after": row_a,
            "changed": (),
        },
        {
            "op": "update",
            "table": "users",
            "pk": {"id": 1},
            "before": row_a,
            "after": row_g,
            "changed": ("name",),
        },
        {
            "op": "delete",
            "table": "users",
            "pk": {"id": 1},
            "before": row_g,
            "after": None,
            "changed": (),
        },
    ]
