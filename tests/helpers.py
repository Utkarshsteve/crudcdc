from sqlalchemy.ext.asyncio import AsyncSession

from crudcdc import ChangeEvent, ChangeFeed


async def events(session: AsyncSession) -> list[ChangeEvent]:
    """Every event in feed order."""
    return list((await ChangeFeed(session).read(limit=10_000)).events)


def ops(evs: list[ChangeEvent]) -> list[tuple[str, str]]:
    return [(e.table, e.op) for e in evs]
