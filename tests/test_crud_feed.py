from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from crudcdc import AsyncCRUD, CDCBase, ChangeFeed


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    joined: Mapped[datetime | None] = mapped_column(default=None)


users = AsyncCRUD(User)


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(CDCBase.metadata.create_all)
    async with async_sessionmaker(engine, expire_on_commit=False)() as s:
        yield s
    await engine.dispose()


async def test_crud_roundtrip(session: AsyncSession) -> None:
    u = await users.create(session, name="ada")
    assert (await users.get(session, u.id)).name == "ada"  # type: ignore[union-attr]
    await users.update(session, u.id, name="grace")
    assert [x.name for x in await users.list(session)] == ["grace"]
    assert await users.delete(session, u.id) is True
    assert await users.delete(session, u.id) is False
    assert await users.update(session, u.id, name="x") is None


async def test_feed_records_each_write_in_order(session: AsyncSession) -> None:
    u = await users.create(session, name="ada", joined=datetime(2026, 1, 2, tzinfo=UTC))
    await users.update(session, u.id, name="grace")
    await users.delete(session, u.id)
    await session.commit()

    batch = await ChangeFeed(session).read()
    assert [c.op for c in batch.changes] == ["insert", "update", "delete"]
    ins, upd, dele = batch.changes
    assert ins.before is None and ins.after["name"] == "ada"  # type: ignore[index]
    assert ins.after["joined"].startswith("2026-01-02")  # type: ignore[index]
    assert upd.before["name"] == "ada" and upd.after["name"] == "grace"  # type: ignore[index]
    assert dele.before["name"] == "grace" and dele.after is None  # type: ignore[index]
    assert {c.pk for c in batch.changes} == {str(u.id)}


async def test_rollback_leaves_no_changes(session: AsyncSession) -> None:
    await users.create(session, name="ghost")
    await session.rollback()
    assert (await ChangeFeed(session).read()).changes == []


async def test_cursor_paging_and_consumer_offsets(session: AsyncSession) -> None:
    for n in range(5):
        await users.create(session, name=f"u{n}")
    await session.commit()
    feed = ChangeFeed(session)

    first = await feed.read(limit=2)
    second = await feed.read(first.next_cursor, limit=2)
    assert [c.seq for c in first.changes + second.changes] == sorted(
        c.seq for c in first.changes + second.changes
    )
    assert len(first.changes) == len(second.changes) == 2
    assert (await feed.read(10**9)).next_cursor == 10**9  # empty read keeps the cursor

    batch = await feed.read_for("billing", limit=3)
    assert len(batch.changes) == 3
    await feed.ack("billing", batch.next_cursor)
    assert len((await feed.read_for("billing")).changes) == 2
    await feed.ack("billing", 0)  # never moves back
    assert await feed.offset("billing") == batch.next_cursor


async def test_table_filter(session: AsyncSession) -> None:
    await users.create(session, name="a")
    feed = ChangeFeed(session)
    assert len((await feed.read(tables=["users"])).changes) == 1
    assert (await feed.read(tables=["other"])).changes == []


async def test_prune_respects_slowest_consumer(session: AsyncSession) -> None:
    for n in range(3):
        await users.create(session, name=f"u{n}")
    await session.commit()
    feed = ChangeFeed(session)
    future = datetime.now(UTC) + timedelta(days=1)

    all_changes = (await feed.read()).changes
    await feed.ack("slow", all_changes[0].seq)
    await feed.ack("fast", all_changes[-1].seq)
    assert await feed.prune(future) == 1  # only what the slow consumer has seen
    await feed.ack("slow", all_changes[-1].seq)
    assert await feed.prune(future) == 2
