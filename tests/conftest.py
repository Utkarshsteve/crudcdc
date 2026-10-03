import os
from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from crudcdc import CDCBase, track

from .models import Base, Note, Untracked

track(Base)
track(Note, track_before=False)

PG_URL = os.environ.get("CRUDCDC_TEST_PG_URL")


@pytest.fixture(params=["sqlite", "postgres"])
async def engine(request: pytest.FixtureRequest, tmp_path) -> AsyncIterator[AsyncEngine]:
    if request.param == "postgres":
        if not PG_URL:
            pytest.skip("CRUDCDC_TEST_PG_URL not set")
        eng = create_async_engine(PG_URL)
    else:
        # A file, not :memory:, so several connections (concurrency tests) share one database.
        eng = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/test.db")
    async with eng.begin() as conn:
        for md in (CDCBase.metadata, Base.metadata, Untracked.metadata):
            await conn.run_sync(md.drop_all)
            await conn.run_sync(md.create_all)
    yield eng
    await eng.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
async def session(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncSession]:
    async with sessions() as s:
        yield s
