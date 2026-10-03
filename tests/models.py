import enum
from datetime import datetime

from sqlalchemy import ForeignKey, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Color(enum.Enum):
    RED = "red"


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    joined: Mapped[datetime | None] = mapped_column(default=None)


class Parent(Base):
    __tablename__ = "parents"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    created: Mapped[datetime] = mapped_column(server_default=func.now())
    kids: Mapped[list["Kid"]] = relationship(cascade="all, delete-orphan")


class Kid(Base):
    __tablename__ = "kids"
    id: Mapped[int] = mapped_column(primary_key=True)
    parent_id: Mapped[int | None] = mapped_column(ForeignKey("parents.id"))


class Paint(Base):
    __tablename__ = "paints"
    id: Mapped[int] = mapped_column(primary_key=True)
    color: Mapped[Color]
