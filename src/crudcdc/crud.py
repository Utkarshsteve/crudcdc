"""Optional async CRUD helpers. Plain ORM operations: capture happens in ``track()``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Generic, TypeVar

from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

T = TypeVar("T", bound=DeclarativeBase)


class AsyncCRUD(Generic[T]):
    """CRUD for one model. Methods flush but never commit: you own the transaction."""

    def __init__(self, model: type[T]) -> None:
        self.model = model

    async def create(self, session: AsyncSession, **data: Any) -> T:
        obj = self.model(**data)
        session.add(obj)
        await session.flush()
        return obj

    async def get(self, session: AsyncSession, pk: Any) -> T | None:
        return await session.get(self.model, pk)

    async def list(
        self, session: AsyncSession, *, limit: int = 100, offset: int = 0
    ) -> Sequence[T]:
        stmt = (
            select(self.model)
            .order_by(*inspect(self.model).primary_key)
            .limit(limit)
            .offset(offset)
        )
        return (await session.execute(stmt)).scalars().all()

    async def update(self, session: AsyncSession, pk: Any, **data: Any) -> T | None:
        obj = await session.get(self.model, pk)
        if obj is None:
            return None
        for key, value in data.items():
            setattr(obj, key, value)
        await session.flush()
        return obj

    async def delete(self, session: AsyncSession, pk: Any) -> bool:
        obj = await session.get(self.model, pk)
        if obj is None:
            return False
        await session.delete(obj)
        await session.flush()
        return True
