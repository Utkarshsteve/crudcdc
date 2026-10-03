import enum
from datetime import datetime

from sqlalchemy import ForeignKey, Integer, TypeDecorator, func
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


class Note(Base):  # tracked with track_before=False
    __tablename__ = "notes"
    id: Mapped[int] = mapped_column(primary_key=True)
    body: Mapped[str]


class Account(Base):  # attribute name differs from column name
    __tablename__ = "accounts"
    id: Mapped[int] = mapped_column(primary_key=True)
    display: Mapped[str] = mapped_column("display_name")


class Membership(Base):  # composite primary key
    __tablename__ = "memberships"
    org_id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(primary_key=True)
    role: Mapped[str]


class Money:
    def __init__(self, cents: int) -> None:
        self.cents = cents


class MoneyType(TypeDecorator[Money]):
    impl = Integer
    cache_ok = True

    def process_bind_param(self, value: Money | None, dialect: object) -> int | None:
        return None if value is None else value.cents

    def process_result_value(self, value: int | None, dialect: object) -> Money | None:
        return None if value is None else Money(value)


class Wallet(Base):  # custom Python type: needs register_encoder
    __tablename__ = "wallets"
    id: Mapped[int] = mapped_column(primary_key=True)
    balance: Mapped[Money] = mapped_column(MoneyType)


class Untracked(DeclarativeBase):
    pass


class Log(Untracked):
    __tablename__ = "logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    line: Mapped[str]
