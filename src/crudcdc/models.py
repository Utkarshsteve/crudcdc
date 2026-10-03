"""Tables that back the change feed. Create them with ``CDCBase.metadata.create_all``."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, DateTime, Index, Integer, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# SQLite only autoincrements a plain INTEGER primary key.
_SeqType = BigInteger().with_variant(Integer, "sqlite")


class CDCBase(DeclarativeBase):
    """Separate metadata, so ``CDCBase.metadata.create_all`` only creates the CDC tables."""


class Change(CDCBase):
    """One row per changed object, written in the same transaction as the change."""

    __tablename__ = "crudcdc_changes"
    __table_args__ = (
        Index("ix_crudcdc_changes_position", "xid", "seq"),
        Index("ix_crudcdc_changes_table_position", "table_name", "xid", "seq"),
    )

    seq: Mapped[int] = mapped_column(_SeqType, primary_key=True, autoincrement=True)
    # Postgres transaction id (pg_current_xact_id); 0 on SQLite. Feed order is (xid, seq).
    xid: Mapped[int] = mapped_column(BigInteger, default=0)
    op: Mapped[str] = mapped_column(String(8))
    table_name: Mapped[str] = mapped_column(String(255))
    pk: Mapped[dict[str, Any]] = mapped_column(JSON)
    before: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    after: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    changed: Mapped[list[str]] = mapped_column(JSON, default=list)
    tx_id: Mapped[str] = mapped_column(String(64))
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ConsumerOffset(CDCBase):
    """The last position ``(xid, seq)`` each named consumer has acknowledged."""

    __tablename__ = "crudcdc_consumer_offsets"

    consumer: Mapped[str] = mapped_column(String(255), primary_key=True)
    xid: Mapped[int] = mapped_column(BigInteger, default=0)
    seq: Mapped[int] = mapped_column(BigInteger, default=0)
