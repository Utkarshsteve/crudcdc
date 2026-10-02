"""Async CRUD that records every write to the change log in the caller's transaction."""

from __future__ import annotations

import datetime as dt
import decimal
import uuid
from collections.abc import Sequence
from typing import Any, Generic, TypeVar

from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from .models import Change

T = TypeVar("T", bound=DeclarativeBase)


def _jsonable(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, (decimal.Decimal, uuid.UUID)):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def _snapshot(obj: DeclarativeBase) -> dict[str, Any]:
    mapper = inspect(obj).mapper
    return {a.key: _jsonable(getattr(obj, a.key)) for a in mapper.column_attrs}


def _pk(obj: DeclarativeBase) -> str:
    return ",".join(str(v) for v in inspect(obj).identity or ())


class AsyncCRUD(Generic[T]):
    """CRUD for one model. Methods flush but never commit: you own the transaction.

    The change row is written in the same transaction as the data change, so a change is in
    the feed if and only if the write committed.
    """

    def __init__(self, model: type[T], *, track_before: bool = True) -> None:
        self.model = model
        self.track_before = track_before
        self._table = model.__tablename__

    async def create(self, session: AsyncSession, **data: Any) -> T:
        obj = self.model(**data)
        session.add(obj)
        await session.flush()
        self._log(session, "insert", obj, before=None, after=_snapshot(obj))
        return obj

    async def get(self, session: AsyncSession, pk: Any) -> T | None:
        return await session.get(self.model, pk)

    async def list(
        self, session: AsyncSession, *, limit: int = 100, offset: int = 0
    ) -> Sequence[T]:
        result = await session.execute(select(self.model).limit(limit).offset(offset))
        return result.scalars().all()

    async def update(self, session: AsyncSession, pk: Any, **data: Any) -> T | None:
        obj = await session.get(self.model, pk)
        if obj is None:
            return None
        before = _snapshot(obj) if self.track_before else None
        for key, value in data.items():
            setattr(obj, key, value)
        await session.flush()
        await session.refresh(obj)
        self._log(session, "update", obj, before=before, after=_snapshot(obj))
        return obj

    async def delete(self, session: AsyncSession, pk: Any) -> bool:
        obj = await session.get(self.model, pk)
        if obj is None:
            return False
        before = _snapshot(obj) if self.track_before else None
        pk_str = _pk(obj)
        await session.delete(obj)
        await session.flush()
        session.add(
            Change(op="delete", table_name=self._table, pk=pk_str, before=before, after=None)
        )
        await session.flush()
        return True

    def _log(
        self,
        session: AsyncSession,
        op: str,
        obj: T,
        *,
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
    ) -> None:
        session.add(Change(op=op, table_name=self._table, pk=_pk(obj), before=before, after=after))
