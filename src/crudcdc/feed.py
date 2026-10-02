"""Pull side of the CDC: read changes after a cursor, track per-consumer offsets, prune."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Change, ConsumerOffset


@dataclass(frozen=True, slots=True)
class Batch:
    changes: Sequence[Change]
    next_cursor: int  # pass back as ``since`` (or to ``ack``) to continue after this batch


class ChangeFeed:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def read(
        self, since: int = 0, *, limit: int = 100, tables: Sequence[str] | None = None
    ) -> Batch:
        """Changes with ``seq > since`` in order. ``next_cursor == since`` means nothing new."""
        stmt = select(Change).where(Change.seq > since).order_by(Change.seq).limit(limit)
        if tables:
            stmt = stmt.where(Change.table_name.in_(tables))
        changes = (await self.session.execute(stmt)).scalars().all()
        return Batch(changes, changes[-1].seq if changes else since)

    async def read_for(
        self, consumer: str, *, limit: int = 100, tables: Sequence[str] | None = None
    ) -> Batch:
        """Read from the consumer's stored offset. Call ``ack`` after processing the batch."""
        return await self.read(await self.offset(consumer), limit=limit, tables=tables)

    async def offset(self, consumer: str) -> int:
        row = await self.session.get(ConsumerOffset, consumer)
        return row.cursor if row else 0

    async def ack(self, consumer: str, cursor: int) -> None:
        """Record that ``consumer`` has processed everything up to ``cursor``. Never moves back."""
        row = await self.session.get(ConsumerOffset, consumer)
        if row is None:
            self.session.add(ConsumerOffset(consumer=consumer, cursor=cursor))
        elif cursor > row.cursor:
            row.cursor = cursor
        await self.session.flush()

    async def prune(self, older_than: datetime) -> int:
        """Delete changes older than ``older_than`` that every registered consumer has acked."""
        stmt = delete(Change).where(Change.created_at < older_than)
        slowest = (await self.session.execute(select(func.min(ConsumerOffset.cursor)))).scalar()
        if slowest is not None:
            stmt = stmt.where(Change.seq <= slowest)
        result = await self.session.execute(stmt, execution_options={"synchronize_session": False})
        return result.rowcount or 0  # type: ignore[attr-defined]
