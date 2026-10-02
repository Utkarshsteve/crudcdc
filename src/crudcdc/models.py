"""Tables that back the change feed. Create them with ``Base.metadata.create_all``."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# SQLite only autoincrements a plain INTEGER primary key.
_SeqType = BigInteger().with_variant(Integer, "sqlite")


class CDCBase(DeclarativeBase):
    """Separate metadata, so ``CDCBase.metadata.create_all`` only creates the CDC tables."""


class Change(CDCBase):
    """One row per create/update/delete, written in the same transaction as the data change."""

    __tablename__ = "crudcdc_changes"

    seq: Mapped[int] = mapped_column(_SeqType, primary_key=True, autoincrement=True)
    op: Mapped[str] = mapped_column(String(8))  # "insert" | "update" | "delete"
    table_name: Mapped[str] = mapped_column(String(255), index=True)
    pk: Mapped[str] = mapped_column(String(255))
    before: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    after: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True
    )


class ConsumerOffset(CDCBase):
    """The last sequence number each named consumer has acknowledged."""

    __tablename__ = "crudcdc_consumer_offsets"

    consumer: Mapped[str] = mapped_column(String(255), primary_key=True)
    cursor: Mapped[int] = mapped_column(_SeqType, default=0)
