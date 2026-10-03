"""What does tracking cost a write? Same table shape, tracked vs untracked.

    uv run python bench/writes.py            # SQLite, plus Postgres if CRUDCDC_TEST_PG_URL is set

Prints a Markdown table per backend: median of RUNS timed runs after one warm-up.
"""

import asyncio
import os
import statistics
import tempfile
import time
from collections.abc import Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import crudcdc
from crudcdc import CDCBase, ChangeFeed

RUNS = 9
SINGLE = 300  # transactions per run, one row each
BATCH, BATCHES = 1000, 5  # rows per transaction, transactions per run
FEED_EVENTS, FEED_BATCH = 10_000, 500


class Row:
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    email: Mapped[str]
    score: Mapped[int]


class Plain(DeclarativeBase): ...


class Tracked(DeclarativeBase): ...


class Lean(DeclarativeBase): ...


class PlainRow(Row, Plain):
    __tablename__ = "bench_plain"


class TrackedRow(Row, Tracked):
    __tablename__ = "bench_tracked"


class LeanRow(Row, Lean):
    __tablename__ = "bench_lean"


crudcdc.track(Tracked)
crudcdc.track(Lean, track_before=False)
MODELS = {"untracked": PlainRow, "tracked": TrackedRow, "tracked, no before": LeanRow}
Model = type[PlainRow] | type[TrackedRow] | type[LeanRow]
Sessions = async_sessionmaker[AsyncSession]


def row(model: Model, i: int) -> Row:
    return model(id=i, name=f"user {i}", email=f"user{i}@example.com", score=i)


async def reset(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        for md in (CDCBase.metadata, Plain.metadata, Tracked.metadata, Lean.metadata):
            await conn.run_sync(md.drop_all)
            await conn.run_sync(md.create_all)


async def preload(S: Sessions, model: Model, n: int) -> None:
    async with S() as s:
        s.add_all(row(model, i) for i in range(n))
        await s.commit()


# Each scenario: (setup, timed body, operations per run)
async def single_insert(S: Sessions, m: Model) -> None:
    for i in range(SINGLE):
        async with S() as s:
            s.add(row(m, i))
            await s.commit()


async def single_update(S: Sessions, m: Model) -> None:
    for i in range(SINGLE):
        async with S() as s:
            r = await s.get(m, i)
            assert r is not None
            r.score += 1
            await s.commit()


async def single_delete(S: Sessions, m: Model) -> None:
    for i in range(SINGLE):
        async with S() as s:
            r = await s.get(m, i)
            await s.delete(r)
            await s.commit()


async def batch_insert(S: Sessions, m: Model) -> None:
    for b in range(BATCHES):
        async with S() as s:
            s.add_all(row(m, b * BATCH + i) for i in range(BATCH))
            await s.commit()


async def batch_update(S: Sessions, m: Model) -> None:
    for b in range(BATCHES):
        async with S() as s:
            rows = await s.scalars(select(m).where(m.id >= b * BATCH, m.id < (b + 1) * BATCH))
            for r in rows:
                r.score += 1
            await s.commit()


async def no_setup(S: Sessions, m: Model) -> None:
    pass


async def setup_single(S: Sessions, m: Model) -> None:
    await preload(S, m, SINGLE)


async def setup_batch(S: Sessions, m: Model) -> None:
    await preload(S, m, BATCH * BATCHES)


Body = Callable[[Sessions, Model], Awaitable[None]]
SCENARIOS: list[tuple[str, Body, Body, int]] = [
    ("insert, 1 row per transaction", no_setup, single_insert, SINGLE),
    ("update, 1 row per transaction", setup_single, single_update, SINGLE),
    ("delete, 1 row per transaction", setup_single, single_delete, SINGLE),
    (f"insert, {BATCH} rows per transaction", no_setup, batch_insert, BATCH * BATCHES),
    (f"update, {BATCH} rows per transaction", setup_batch, batch_update, BATCH * BATCHES),
]


async def timed(engine: AsyncEngine, S: Sessions, setup: Body, body: Body, m: Model) -> float:
    await reset(engine)
    await setup(S, m)
    start = time.perf_counter()
    await body(S, m)
    return time.perf_counter() - start


async def feed_rate(engine: AsyncEngine, S: Sessions) -> float:
    """Events per second through read_for + ack + commit, in batches of FEED_BATCH."""
    times = []
    for _ in range(RUNS + 1):
        await reset(engine)
        for b in range(FEED_EVENTS // BATCH):
            async with S() as s:
                s.add_all(row(TrackedRow, b * BATCH + i) for i in range(BATCH))
                await s.commit()
        start = time.perf_counter()
        seen = 0
        while True:
            async with S() as s:
                feed = ChangeFeed(s)
                batch = await feed.read_for("bench", limit=FEED_BATCH)
                if not batch.events:
                    break
                seen += len(batch.events)
                assert batch.next_cursor
                await feed.ack("bench", batch.next_cursor)
                await s.commit()
        times.append(time.perf_counter() - start)
        assert seen == FEED_EVENTS
    return FEED_EVENTS / statistics.median(times[1:])


async def bench(name: str, url: str) -> None:
    engine = create_async_engine(url)
    S = async_sessionmaker(engine, expire_on_commit=False)
    print(f"\n### {name}\n")
    print(
        "| Scenario | untracked ops/s | tracked ops/s | overhead | tracked, no before | overhead |"
    )
    print("|---|---:|---:|---:|---:|---:|")
    for label, setup, body, ops in SCENARIOS:
        runs: dict[str, list[float]] = {k: [] for k in MODELS}
        for i in range(RUNS + 1):  # run 0 is the warm-up; models interleaved to share drift
            for key, m in MODELS.items():
                t = await timed(engine, S, setup, body, m)
                if i:
                    runs[key].append(t)
        rate = {k: ops / statistics.median(v) for k, v in runs.items()}
        base = rate["untracked"]
        tr, lean = rate["tracked"], rate["tracked, no before"]
        print(
            f"| {label} | {base:,.0f} | {tr:,.0f} | {base / tr - 1:+.0%} "
            f"| {lean:,.0f} | {base / lean - 1:+.0%} |"
        )
    print(
        f"\nFeed: {await feed_rate(engine, S):,.0f} events/s "
        f"(read_for + ack + commit, batches of {FEED_BATCH})"
    )
    await engine.dispose()


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        await bench("SQLite (file)", f"sqlite+aiosqlite:///{tmp}/bench.db")
    if url := os.environ.get("CRUDCDC_TEST_PG_URL"):
        await bench("Postgres", url)


if __name__ == "__main__":
    asyncio.run(main())
