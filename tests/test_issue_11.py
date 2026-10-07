"""#11: ORM write paths that bypass capture fail loudly, or are documented as not capturable."""

import itertools
from collections.abc import Callable
from typing import Any

import pytest
from sqlalchemy import delete, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, aliased, mapped_column

from crudcdc import CDCBase, Change, ChangeFeed, UntrackedWriteError, capture, track

from .helpers import events
from .models import Base, Log, User

Stmt = Callable[[AsyncSession], Any]


def upsert(session: AsyncSession) -> Any:
    return pg_insert if session.get_bind().dialect.name == "postgresql" else sqlite_insert


# Each builds a statement that writes to the tracked `users` table without the unit of work.
BYPASSES: dict[str, Stmt] = {
    "insert(Model) list": lambda s: (insert(User), [{"id": 1, "name": "a"}]),
    "insert(Model).values": lambda s: (insert(User).values(id=1, name="a"),),
    "insert(Model).returning": lambda s: (insert(User).values(id=1, name="a").returning(User.id),),
    "upsert do_update": lambda s: (
        upsert(s)(User)
        .values(id=1, name="a")
        .on_conflict_do_update(index_elements=["id"], set_={"name": "b"}),
    ),
    "upsert do_nothing": lambda s: (
        upsert(s)(User).values(id=1, name="a").on_conflict_do_nothing(),
    ),
    "insert(Model).from_select": lambda s: (
        insert(User).from_select(["id", "name"], select(Log.id, Log.line)),
    ),
    "update(aliased(Model))": lambda s: (update(aliased(User)).values(name="b"),),
    "insert(Model.__table__)": lambda s: (insert(User.__table__), [{"id": 1, "name": "a"}]),
    "update(Model.__table__)": lambda s: (update(User.__table__).values(name="b"),),
    "delete(Model.__table__)": lambda s: (delete(User.__table__),),
}


@pytest.mark.parametrize("name", list(BYPASSES))
async def test_bypass_raises(session: AsyncSession, name: str) -> None:
    stmt, *params = BYPASSES[name](session)
    with pytest.raises(UntrackedWriteError, match="users"):
        await session.execute(stmt, *params)
    await session.rollback()
    assert await events(session) == []


@pytest.mark.parametrize("name", list(BYPASSES))
async def test_bypass_escape_hatch(session: AsyncSession, name: str) -> None:
    stmt, *params = BYPASSES[name](session)
    await session.execute(stmt.execution_options(crudcdc_untracked=True), *params)
    await session.commit()
    assert await events(session) == []  # deliberately not captured


async def test_untracked_table_unaffected(session: AsyncSession) -> None:
    await session.execute(insert(Log), [{"id": 1, "line": "a"}])
    await session.execute(insert(Log.__table__), [{"id": 2, "line": "b"}])
    await session.commit()


_late = itertools.count()


async def test_model_defined_after_first_guarded_statement(session: AsyncSession) -> None:
    with pytest.raises(UntrackedWriteError):  # the guard has already run once
        await session.execute(update(User.__table__).values(name="x"))
    await session.rollback()

    n = next(_late)  # a fresh class per backend run: tables can't be defined twice
    late = type(
        f"Late{n}",
        (Base,),
        {
            "__tablename__": f"late_{n}",
            "__annotations__": {"id": Mapped[int]},
            "id": mapped_column(primary_key=True),
        },
    )
    with pytest.raises(UntrackedWriteError, match=f"late_{n}"):
        await session.execute(insert(late.__table__), [{"id": 1}])  # raises before any SQL runs
    await session.rollback()


@pytest.mark.parametrize("target", [CDCBase, Change])
def test_crudcdc_tables_cannot_be_tracked(target: type) -> None:
    with pytest.raises(ValueError, match="crudcdc"):
        track(target)


async def test_legacy_bulk_methods_are_not_captured(session: AsyncSession) -> None:
    """Pinned: Session.bulk_* fire no events, so they can't be guarded (README "Not captured").

    If SQLAlchemy ever routes them through the unit of work or do_orm_execute, this fails and the
    docs need revisiting.
    """
    await session.run_sync(lambda s: s.bulk_save_objects([User(id=1, name="a")]))
    await session.run_sync(lambda s: s.bulk_insert_mappings(User, [{"id": 2, "name": "b"}]))
    await session.run_sync(lambda s: s.bulk_update_mappings(User, [{"id": 1, "name": "z"}]))
    await session.commit()
    assert await events(session) == []


async def test_consumer_offsets_skip_the_tracked_table_lookup(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """read_for / ack / forget write crudcdc's own offsets: the guard must not walk every mapper."""
    session.add(User(id=1, name="a"))
    await session.commit()

    def fail() -> set[object]:
        raise AssertionError("_tracked_tables() called for crudcdc's own table")

    monkeypatch.setattr(capture, "_tracked_tables", fail)
    feed = ChangeFeed(session)
    batch = await feed.read_for("c")
    assert batch.next_cursor
    await feed.ack("c", batch.next_cursor)
    await feed.forget("c")
    await session.commit()
