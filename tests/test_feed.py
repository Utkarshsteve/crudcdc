from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crudcdc import Change, ChangeFeed, InvalidCursorError

from .helpers import events
from .models import User

Sessions = async_sessionmaker[AsyncSession]


async def add_users(session: AsyncSession, *ids: int) -> None:
    for i in ids:
        session.add(User(id=i, name=f"u{i}"))
        await session.commit()


def pg_only(session: AsyncSession) -> None:
    if session.get_bind().dialect.name != "postgresql":
        pytest.skip("Postgres concurrency")


async def test_paging_with_cursor(session: AsyncSession) -> None:
    await add_users(session, 1, 2, 3, 4, 5)
    feed = ChangeFeed(session)
    first = await feed.read(limit=2)
    second = await feed.read(first.next_cursor, limit=2)
    rest = await feed.read(second.next_cursor)
    assert [e.pk["id"] for e in first.events + second.events + rest.events] == [1, 2, 3, 4, 5]
    empty = await feed.read(rest.next_cursor)
    assert (empty.events, empty.next_cursor) == ((), rest.next_cursor)
    assert (await feed.read()).next_cursor is not None


async def test_empty_feed(session: AsyncSession) -> None:
    assert await ChangeFeed(session).read() == type(await ChangeFeed(session).read())((), None)


async def test_invalid_cursor(session: AsyncSession) -> None:
    for bad in ("nope", "1", "1:2:3", "-1:5"):
        with pytest.raises(InvalidCursorError):
            await ChangeFeed(session).read(bad)
    with pytest.raises(InvalidCursorError):
        await ChangeFeed(session).ack("c", "x")


async def test_table_filter(session: AsyncSession) -> None:
    await add_users(session, 1)
    feed = ChangeFeed(session)
    assert len((await feed.read(tables=["users"])).events) == 1
    assert (await feed.read(tables=["other"])).events == ()


async def test_consumer_offsets(session: AsyncSession) -> None:
    await add_users(session, 1, 2, 3, 4, 5)
    feed = ChangeFeed(session)
    assert await feed.offset("billing") is None
    batch = await feed.read_for("billing", limit=3)
    assert [e.pk["id"] for e in batch.events] == [1, 2, 3]
    assert batch.next_cursor is not None
    await feed.ack("billing", batch.next_cursor)
    await session.commit()
    assert [e.pk["id"] for e in (await feed.read_for("billing")).events] == [4, 5]
    first = (await feed.read(limit=1)).next_cursor
    assert first is not None
    await feed.ack("billing", first)  # never moves back
    assert await feed.offset("billing") == batch.next_cursor


async def test_prune_respects_slowest_consumer_and_forget(session: AsyncSession) -> None:
    await add_users(session, 1, 2, 3)
    feed = ChangeFeed(session)
    future = datetime.now(UTC) + timedelta(days=1)
    first = (await feed.read(limit=1)).next_cursor
    last = (await feed.read()).next_cursor
    assert first and last
    await feed.ack("slow", first)
    await feed.ack("fast", last)
    assert await feed.prune(future) == 1
    await feed.forget("slow")
    assert await feed.prune(future) == 2
    assert await feed.prune(datetime.now(UTC) - timedelta(days=1)) == 0


async def test_prune_respects_age(session: AsyncSession) -> None:
    await add_users(session, 1)
    assert await ChangeFeed(session).prune(datetime.now(UTC) - timedelta(hours=1)) == 0
    assert await ChangeFeed(session).prune(datetime.now(UTC) + timedelta(hours=1)) == 1


async def test_crashed_worker_batch_is_redelivered(sessions: Sessions) -> None:
    async with sessions() as s:
        await add_users(s, 1, 2)
    async with sessions() as w1:
        batch = await ChangeFeed(w1).read_for("c")
        assert batch.next_cursor
        await ChangeFeed(w1).ack("c", batch.next_cursor)
        await w1.rollback()  # crash before commit
    async with sessions() as w2:
        assert len((await ChangeFeed(w2).read_for("c")).events) == 2


async def test_second_worker_gets_empty_batch(sessions: Sessions) -> None:
    async with sessions() as s:
        await add_users(s, 1, 2)
        pg_only(s)
        await ChangeFeed(s).read_for("c")  # register the consumer
        await s.commit()
    async with sessions() as w1, sessions() as w2:
        assert len((await ChangeFeed(w1).read_for("c")).events) == 2
        held = await ChangeFeed(w2).read_for("c")
        assert held.events == ()
        await w1.commit()
        assert len((await ChangeFeed(w2).read_for("c")).events) == 2


async def _naive_seq_read(session: AsyncSession, after_seq: int) -> list[int]:
    """What a reader ordering by seq alone would see (the scaffold's design)."""
    rows = await session.execute(select(Change.seq).where(Change.seq > after_seq))
    return sorted(rows.scalars())


async def test_later_seq_committing_first_is_not_skipped(sessions: Sessions) -> None:
    async with sessions() as s:
        pg_only(s)
    async with sessions() as t1, sessions() as t2, sessions() as reader:
        t1.add(User(id=1, name="slow"))
        await t1.flush()  # seq 1, transaction still open
        t2.add(User(id=2, name="fast"))
        await t2.commit()  # seq 2 commits first

        batch = await ChangeFeed(reader).read()
        assert batch.events == ()  # held back while t1 is open
        naive_seen = await _naive_seq_read(reader, 0)
        await reader.commit()
        await t1.commit()

        assert [e.pk["id"] for e in (await events(reader))] == [1, 2]
        # A seq-only reader saw seq 2 first; acking it would skip seq 1 forever.
        assert naive_seen == [max(naive_seen)] and len(naive_seen) == 1


async def test_lower_xid_with_higher_seq_is_not_skipped(sessions: Sessions) -> None:
    """The case found in the spec self-review: xid order and seq order disagree."""
    async with sessions() as s:
        pg_only(s)
    async with sessions() as t1, sessions() as t2, sessions() as reader:
        await t1.execute(text("select pg_current_xact_id()"))  # t1 gets the lower xid
        t2.add(User(id=2, name="t2"))
        await t2.flush()  # t2 (higher xid) takes the lower seq, stays open
        t1.add(User(id=1, name="t1"))
        await t1.commit()  # t1 (lower xid) has the higher seq and commits

        feed = ChangeFeed(reader)
        first = await feed.read()
        assert [e.pk["id"] for e in first.events] == [1]
        await reader.commit()
        await t2.commit()

        rest = await feed.read(first.next_cursor)
        assert [e.pk["id"] for e in rest.events] == [2]  # still delivered


async def test_changed_at_is_utc(session: AsyncSession) -> None:
    await add_users(session, 1)
    at = (await events(session))[0].changed_at
    assert at.tzinfo == UTC and abs(datetime.now(UTC) - at) < timedelta(minutes=5)
