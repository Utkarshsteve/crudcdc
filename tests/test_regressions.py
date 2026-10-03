"""Bugs found by the 2026-10-03 design review probes (see docs/specs)."""

from sqlalchemy.ext.asyncio import AsyncSession

from crudcdc import AsyncCRUD

from .helpers import events, ops
from .models import Color, Kid, Paint, Parent


async def test_cascade_delete_records_children(session: AsyncSession) -> None:
    session.add(Parent(id=1, name="p", kids=[Kid(id=1), Kid(id=2)]))
    await session.commit()
    await AsyncCRUD(Parent).delete(session, 1)
    await session.commit()
    # children are deleted before their parent
    assert ops(await events(session))[3:] == [
        ("kids", "delete"),
        ("kids", "delete"),
        ("parents", "delete"),
    ]


async def test_plain_session_add_is_captured(session: AsyncSession) -> None:
    session.add(Parent(id=1, name="p"))
    await session.commit()
    assert ops(await events(session)) == [("parents", "insert")]


async def test_enum_column_encodes(session: AsyncSession) -> None:
    await AsyncCRUD(Paint).create(session, id=1, color=Color.RED)
    await session.commit()
    assert (await events(session))[0].after == {"id": 1, "color": "red"}


async def test_server_default_and_expired_update(session: AsyncSession) -> None:
    session.add(p := Parent(id=1, name="p"))
    await session.commit()
    session.expire(p)
    await AsyncCRUD(Parent).update(session, 1, name="q")
    await session.commit()
    ins, upd = await events(session)
    assert ins.after["created"] is not None
    assert upd.changed == ("name",)
