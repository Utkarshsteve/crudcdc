"""#13: sessions bound to several databases (``binds={Model: engine}``)."""

from collections.abc import AsyncIterator

import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import UnboundExecutionError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from crudcdc import CDCBase, Change, ChangeFeed, ConsumerOffset

from .conftest import PG_URL
from .models import Account, Base, User

Engines = tuple[AsyncEngine, AsyncEngine]


async def _reset(eng: AsyncEngine) -> None:
    async with eng.begin() as conn:
        for md in (CDCBase.metadata, Base.metadata):
            await conn.run_sync(md.drop_all)
            await conn.run_sync(md.create_all)


async def _second_pg_database() -> str:
    assert PG_URL
    url = make_url(PG_URL)
    admin = create_async_engine(url, isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        if not await conn.scalar(text("select 1 from pg_database where datname = 'crudcdc_b'")):
            await conn.execute(text("create database crudcdc_b"))
    await admin.dispose()
    return url.set(database="crudcdc_b").render_as_string(hide_password=False)


@pytest.fixture(params=["sqlite", "postgres", "mixed"])
async def engines(request: pytest.FixtureRequest, tmp_path) -> AsyncIterator[Engines]:
    """Database A holds `users`, database B holds `accounts`; both have the CDC tables."""
    if request.param != "sqlite" and not PG_URL:
        pytest.skip("CRUDCDC_TEST_PG_URL not set")
    if request.param == "sqlite":
        a = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/a.db")
        b = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/b.db")
    elif request.param == "postgres":
        a, b = create_async_engine(PG_URL), create_async_engine(await _second_pg_database())
    else:  # one session mixing SQLite and Postgres: dialect handling must be per database
        a, b = (
            create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/a.db"),
            create_async_engine(PG_URL),
        )
    for eng in (a, b):
        await _reset(eng)
    yield a, b
    for eng in (a, b):
        await eng.dispose()


def _sessions(
    a: AsyncEngine, b: AsyncEngine, default: bool, cdc: AsyncEngine | None = None
) -> async_sessionmaker[AsyncSession]:
    kw = {"bind": a} if default else {}
    binds: dict[type, AsyncEngine] = {User: a, Account: b} | ({CDCBase: cdc} if cdc else {})
    return async_sessionmaker(binds=binds, expire_on_commit=False, **kw)


async def _change_tables(eng: AsyncEngine) -> list[tuple[str, str]]:
    async with eng.connect() as conn:
        rows = await conn.execute(select(Change.table_name, Change.op).order_by(Change.seq))
        return [tuple(r) for r in rows]


@pytest.mark.parametrize("default", [True, False], ids=["default-bind", "no-default-bind"])
async def test_change_rows_go_to_each_models_database(engines: Engines, default: bool) -> None:
    a, b = engines
    async with _sessions(a, b, default)() as s:
        s.add_all([User(id=1, name="u"), Account(id=1, display="x")])
        await s.commit()
        user, account = await s.get(User, 1), await s.get(Account, 1)
        assert user and account
        user.name = "v"
        s.expire(account)
        account.display = "y"  # old value unknown: before_flush must read it from database B
        await s.commit()
        await s.delete(user)
        await s.delete(account)
        await s.commit()

    assert await _change_tables(a) == [
        ("users", "insert"),
        ("users", "update"),
        ("users", "delete"),
    ]
    assert await _change_tables(b) == [
        ("accounts", "insert"),
        ("accounts", "update"),
        ("accounts", "delete"),
    ]
    async with _sessions(a, b, default)() as s:
        upd = [e for e in (await ChangeFeed(s, bind=b).read()).events if e.op == "update"]
        assert (upd[0].before, upd[0].after) == (
            {"id": 1, "display_name": "x"},
            {"id": 1, "display_name": "y"},
        )
    async with a.connect() as ca, b.connect() as cb:  # one commit, but a transaction per database
        first = select(Change.tx_id).order_by(Change.seq).limit(1)
        assert await ca.scalar(first) != await cb.scalar(first)


@pytest.mark.parametrize("default", [True, False], ids=["default-bind", "no-default-bind"])
async def test_feed_reads_the_named_database(engines: Engines, default: bool) -> None:
    a, b = engines
    async with _sessions(a, b, default)() as s:
        s.add_all([User(id=1, name="u"), Account(id=1, display="x")])
        await s.commit()

        assert [e.table for e in (await ChangeFeed(s, bind=a).read()).events] == ["users"]
        feed = ChangeFeed(s, bind=b)
        batch = await feed.read_for("audit")
        assert [e.table for e in batch.events] == ["accounts"]
        assert batch.next_cursor
        await feed.ack("audit", batch.next_cursor)
        await s.commit()
        assert await feed.offset("audit") == batch.next_cursor
        assert (await feed.read_for("audit")).events == ()
        await s.commit()

    for eng, expected in ((a, []), (b, ["audit"])):  # the offset lives in database B only
        async with eng.connect() as conn:
            assert list(await conn.scalars(select(ConsumerOffset.consumer))) == expected


async def test_bind_must_be_an_async_engine(engines: Engines) -> None:
    a, b = engines
    async with _sessions(a, b, True)() as s, b.connect() as conn:
        with pytest.raises(TypeError, match="AsyncEngine"):
            ChangeFeed(s, bind=conn)  # type: ignore[arg-type]


@pytest.mark.parametrize("default", [True, False], ids=["default-bind", "no-default-bind"])
async def test_feed_without_bind_reads_the_default_bind(engines: Engines, default: bool) -> None:
    a, b = engines
    async with _sessions(a, b, default)() as s:
        s.add_all([User(id=1, name="u"), Account(id=1, display="x")])
        await s.commit()
        if default:
            assert [e.table for e in (await ChangeFeed(s).read()).events] == ["users"]
        else:
            with pytest.raises(UnboundExecutionError):
                await ChangeFeed(s).read()


@pytest.mark.parametrize("default", [True, False], ids=["default-bind", "no-default-bind"])
async def test_feed_without_bind_follows_a_cdcbase_bind(engines: Engines, default: bool) -> None:
    """``binds={CDCBase: b}`` sends the feed's statements to B: its dialect must be B's too."""
    a, b = engines
    sessions = _sessions(a, b, default, cdc=b)
    async with sessions() as s:
        s.add(Account(id=1, display="x"))
        await s.commit()
        feed = ChangeFeed(s)
        batch = await feed.read_for("audit")
        assert [e.table for e in batch.events] == ["accounts"]
        assert batch.next_cursor
        await feed.ack("audit", batch.next_cursor)
        await s.commit()
        assert await feed.offset("audit") == batch.next_cursor

    if b.dialect.name != "postgresql":
        return
    async with sessions() as open_tx, sessions() as s:  # Postgres' hold-back must apply
        open_tx.add(Account(id=2, display="y"))
        await open_tx.flush()
        s.add(Account(id=3, display="z"))
        await s.commit()
        assert (await ChangeFeed(s).read(batch.next_cursor)).events == ()
