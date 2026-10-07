"""Pull side of the CDC: read events after a cursor, per-consumer offsets, pruning."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

from sqlalchemy import delete, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from .errors import InvalidCursorError
from .models import Change, ConsumerOffset

Position = tuple[int, int]  # (xid, seq)
_START: Position = (0, 0)
_PG_XMIN = text("pg_snapshot_xmin(pg_current_snapshot())::text::bigint")


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    seq: int  # unique event id, not the cursor
    op: Literal["insert", "update", "delete"]
    table: str
    pk: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    changed: tuple[str, ...]
    tx_id: str
    changed_at: datetime  # UTC; transaction start on Postgres, write time on SQLite


@dataclass(frozen=True, slots=True)
class Batch:
    events: tuple[ChangeEvent, ...]
    next_cursor: str | None  # pass back as ``since`` (or to ``ack``) to continue


def _token(pos: Position) -> str:
    return f"{pos[0]}:{pos[1]}"


def _parse(token: str) -> Position:
    try:
        xid, seq = (int(p) for p in token.split(":"))
    except (ValueError, AttributeError):
        raise InvalidCursorError(f"invalid cursor {token!r}") from None
    if xid < 0 or seq < 0:
        raise InvalidCursorError(f"invalid cursor {token!r}")
    return xid, seq


def _event(c: Change) -> ChangeEvent:
    at = c.changed_at if c.changed_at.tzinfo else c.changed_at.replace(tzinfo=UTC)
    return ChangeEvent(
        seq=c.seq,
        op=cast(Literal["insert", "update", "delete"], c.op),
        table=c.table_name,
        pk=c.pk,
        before=c.before,
        after=c.after,
        changed=tuple(c.changed),
        tx_id=c.tx_id,
        changed_at=at.astimezone(UTC),
    )


class ChangeFeed:
    """The change feed of one database.

    ``bind`` names which database to read when the session spans several (``binds={...}``);
    by default it's the session's own bind.
    """

    def __init__(self, session: AsyncSession, *, bind: AsyncEngine | None = None) -> None:
        if bind is not None and not isinstance(bind, AsyncEngine):
            raise TypeError(
                f"bind must be an AsyncEngine, not {type(bind).__name__}: a connection's "
                "transaction isn't owned by the session, so their commits could disagree"
            )
        self.session = session
        # bind_arguments needs the sync engine; an AsyncEngine fails with AsyncContextNotStarted.
        self._bind = None if bind is None else bind.sync_engine

    @property
    def _dialect(self) -> str:
        return (self._bind or self.session.get_bind()).dialect.name

    async def _execute(self, stmt: Any, **kw: Any) -> Any:
        if self._bind is not None:
            kw["bind_arguments"] = {"bind": self._bind}
        return await self.session.execute(stmt, **kw)

    async def _scalar(self, stmt: Any) -> Any:
        return (await self._execute(stmt)).scalar()

    async def read(
        self, since: str | None = None, *, limit: int = 100, tables: Sequence[str] | None = None
    ) -> Batch:
        """Events after ``since`` (``None`` = from the start), in feed order."""
        return await self._read(_parse(since) if since else _START, since, limit, tables)

    async def _read(
        self, pos: Position, since: str | None, limit: int, tables: Sequence[str] | None
    ) -> Batch:
        stmt = (
            select(Change)
            .where(tuple_(Change.xid, Change.seq) > tuple_(*pos))
            .order_by(Change.xid, Change.seq)
            .limit(limit)
        )
        if tables:
            stmt = stmt.where(Change.table_name.in_(tables))
        if self._dialect == "postgresql":
            # Only transactions older than every running one: nothing can commit behind us.
            stmt = stmt.where(Change.xid < _PG_XMIN)
        rows = (await self._execute(stmt)).scalars().all()
        if not rows:
            return Batch((), since)
        return Batch(tuple(_event(c) for c in rows), _token((rows[-1].xid, rows[-1].seq)))

    async def read_for(
        self, consumer: str, *, limit: int = 100, tables: Sequence[str] | None = None
    ) -> Batch:
        """Read from ``consumer``'s offset, holding it until the transaction ends.

        Another worker reading the same consumer meanwhile gets an empty batch (Postgres) or
        waits (SQLite, single writer). The very first read of a new consumer name also waits
        for a concurrent first read to commit. Call ``ack`` and commit when done.
        """
        await self._ensure(consumer)
        if self._dialect == "postgresql":
            row = await self._scalar(
                select(ConsumerOffset)
                .where(ConsumerOffset.consumer == consumer)
                .with_for_update(skip_locked=True)
            )
            if row is None:  # held by another worker
                return Batch((), await self.offset(consumer))
        else:
            # No row locks in SQLite: a no-op write takes the database write lock instead.
            await self._execute(
                update(ConsumerOffset)
                .where(ConsumerOffset.consumer == consumer)
                .values(seq=ConsumerOffset.seq)
                .execution_options(synchronize_session=False)
            )
        pos = await self._position(consumer)
        return await self._read(pos, None if pos == _START else _token(pos), limit, tables)

    async def _ensure(self, consumer: str) -> None:
        # Check first: inserting over a row another worker holds would block on its lock.
        exists = await self._scalar(
            select(ConsumerOffset.consumer).where(ConsumerOffset.consumer == consumer)
        )
        if exists is not None:
            return
        ins = pg_insert if self._dialect == "postgresql" else sqlite_insert
        stmt = ins(ConsumerOffset).values(consumer=consumer, xid=0, seq=0)
        await self._execute(stmt.on_conflict_do_nothing())

    async def _position(self, consumer: str) -> Position:
        row = (
            await self._execute(
                select(ConsumerOffset.xid, ConsumerOffset.seq).where(
                    ConsumerOffset.consumer == consumer
                )
            )
        ).first()
        return (row[0], row[1]) if row else _START

    async def offset(self, consumer: str) -> str | None:
        """The consumer's acknowledged cursor, or ``None`` if it has acked nothing."""
        pos = await self._position(consumer)
        return None if pos == _START else _token(pos)

    async def ack(self, consumer: str, cursor: str) -> None:
        """Record that ``consumer`` processed everything up to ``cursor``. Never moves back."""
        pos = _parse(cursor)
        await self._ensure(consumer)
        await self._execute(
            update(ConsumerOffset)
            .where(
                ConsumerOffset.consumer == consumer,
                tuple_(ConsumerOffset.xid, ConsumerOffset.seq) < tuple_(*pos),
            )
            .values(xid=pos[0], seq=pos[1])
            .execution_options(synchronize_session=False)
        )

    async def forget(self, consumer: str) -> None:
        """Drop a consumer's offset, so it no longer holds back ``prune``."""
        await self._execute(
            delete(ConsumerOffset)
            .where(ConsumerOffset.consumer == consumer)
            .execution_options(synchronize_session=False)
        )

    async def prune(self, older_than: datetime) -> int:
        """Delete events older than ``older_than`` that every registered consumer has acked."""
        if self._dialect == "sqlite":  # stored as naive UTC text
            older_than = older_than.astimezone(UTC).replace(tzinfo=None)
        stmt = delete(Change).where(Change.changed_at < older_than)
        slowest = (
            await self._execute(
                select(ConsumerOffset.xid, ConsumerOffset.seq)
                .order_by(ConsumerOffset.xid, ConsumerOffset.seq)
                .limit(1)
            )
        ).first()
        if slowest is not None:
            stmt = stmt.where(tuple_(Change.xid, Change.seq) <= tuple_(*slowest))
        result = await self._execute(stmt, execution_options={"synchronize_session": False})
        return cast(int, result.rowcount or 0)
