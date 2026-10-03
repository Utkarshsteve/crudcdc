import pytest
from sqlalchemy import delete, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from crudcdc import AsyncCRUD, EncodingError, UntrackedWriteError, register_encoder

from .helpers import events, ops
from .models import Account, Kid, Log, Membership, Money, Note, Parent, User, Wallet


async def test_insert_update_delete_events(session: AsyncSession) -> None:
    u = User(id=1, name="ada")
    session.add(u)
    await session.commit()
    u.name = "grace"
    await session.commit()
    await session.delete(u)
    await session.commit()

    ins, upd, dele = await events(session)
    assert (ins.op, ins.pk, ins.before, ins.after) == (
        "insert",
        {"id": 1},
        None,
        {"id": 1, "name": "ada", "joined": None},
    )
    assert ins.changed == ()
    assert (upd.before["name"], upd.after["name"], upd.changed) == ("ada", "grace", ("name",))
    assert (dele.op, dele.before["name"], dele.after) == ("delete", "grace", None)


async def test_crud_helper_and_plain_orm_give_same_events(session: AsyncSession) -> None:
    users = AsyncCRUD(User)
    await users.create(session, id=1, name="a")
    await users.update(session, 1, name="b")
    await users.delete(session, 1)
    session.add(u := User(id=2, name="a"))
    await session.flush()
    u.name = "b"
    await session.flush()
    await session.delete(u)
    await session.commit()

    evs = await events(session)
    strip = [(e.op, e.before, e.after, e.changed) for e in evs]
    first, second = strip[:3], strip[3:]
    assert [(o, b and {**b, "id": 0}, a and {**a, "id": 0}, c) for o, b, a, c in first] == [
        (o, b and {**b, "id": 0}, a and {**a, "id": 0}, c) for o, b, a, c in second
    ]


async def test_relationship_change_is_captured(session: AsyncSession) -> None:
    p1, p2, kid = Parent(id=1, name="a"), Parent(id=2, name="b", kids=[]), Kid(id=1)
    p1.kids.append(kid)
    session.add_all([p1, p2])
    await session.commit()
    p1.kids.remove(kid)
    p2.kids.append(kid)
    await session.commit()
    last = (await events(session))[-1]
    assert (last.table, last.op, last.changed) == ("kids", "update", ("parent_id",))
    assert (last.before["parent_id"], last.after["parent_id"]) == (1, 2)


async def test_inserts_ordered_parents_first(session: AsyncSession) -> None:
    session.add(Parent(id=1, name="p", kids=[Kid(id=1)]))
    await session.commit()
    assert ops(await events(session)) == [("parents", "insert"), ("kids", "insert")]


async def test_several_changes_in_one_flush_collapse(session: AsyncSession) -> None:
    session.add(u := User(id=1, name="a"))
    await session.commit()
    u.name = "b"
    u.name = "c"
    await session.commit()
    upd = (await events(session))[-1]
    assert (upd.before["name"], upd.after["name"]) == ("a", "c")


async def test_flushes_share_tx_id(session: AsyncSession) -> None:
    session.add(u := User(id=1, name="a"))
    await session.flush()
    u.name = "b"
    await session.flush()
    await session.commit()
    session.add(User(id=2, name="x"))
    await session.commit()
    a, b, c = await events(session)
    assert a.tx_id == b.tx_id != c.tx_id


async def test_noop_update_emits_nothing(session: AsyncSession) -> None:
    session.add(u := User(id=1, name="a"))
    await session.commit()
    u.name = "a"
    await session.commit()
    assert ops(await events(session)) == [("users", "insert")]


async def test_update_of_expired_object_has_correct_before(session: AsyncSession) -> None:
    session.add(User(id=1, name="a"))
    await session.commit()
    session.expire_all()
    u = await session.get(User, 1)
    session.expire(u)
    u.name = "b"  # set without loading the old value
    await session.commit()
    upd = (await events(session))[-1]
    assert (upd.before["name"], upd.after["name"]) == ("a", "b")


async def test_primary_key_change(session: AsyncSession) -> None:
    session.add(k := Kid(id=1))
    await session.commit()
    k.id = 5
    await session.commit()
    upd = (await events(session))[-1]
    assert (upd.op, upd.pk, upd.before["id"], upd.changed) == ("update", {"id": 5}, 1, ("id",))


async def test_composite_pk(session: AsyncSession) -> None:
    session.add(Membership(org_id=1, user_id=7, role="admin"))
    await session.commit()
    assert (await events(session))[0].pk == {"org_id": 1, "user_id": 7}


async def test_column_names_not_attribute_names(session: AsyncSession) -> None:
    session.add(Account(id=1, display="x"))
    await session.commit()
    assert (await events(session))[0].after == {"id": 1, "display_name": "x"}


async def test_track_before_false(session: AsyncSession) -> None:
    session.add(n := Note(id=1, body="a"))
    await session.commit()
    n.body = "b"
    await session.commit()
    await session.delete(n)
    await session.commit()
    _, upd, dele = await events(session)
    assert (upd.before, upd.changed, dele.before, dele.pk) == (None, ("body",), None, {"id": 1})


async def test_savepoint_rollback_discards_its_events(session: AsyncSession) -> None:
    session.add(User(id=1, name="kept"))
    nested = await session.begin_nested()
    session.add(User(id=2, name="dropped"))
    await session.flush()
    await nested.rollback()
    await session.commit()
    assert [e.pk for e in await events(session)] == [{"id": 1}]


async def test_rollback_discards_events(session: AsyncSession) -> None:
    session.add(User(id=1, name="ghost"))
    await session.flush()
    await session.rollback()
    assert await events(session) == []


async def test_untracked_model_ignored(session: AsyncSession) -> None:
    session.add(Log(id=1, line="x"))
    await session.commit()
    assert await events(session) == []


async def test_encoding_error_rolls_back_the_write(session: AsyncSession) -> None:
    session.add(Wallet(id=1, balance=Money(500)))
    with pytest.raises(EncodingError, match=r"wallets\.balance"):
        await session.commit()
    await session.rollback()
    assert await session.get(Wallet, 1) is None
    assert await events(session) == []


async def test_register_encoder(session: AsyncSession) -> None:
    class Coins(Money):  # registrations are global: keep plain Money unregistered
        pass

    register_encoder(Coins, lambda m: m.cents)
    session.add(Wallet(id=1, balance=Coins(500)))
    await session.commit()
    assert (await events(session))[0].after == {"id": 1, "balance": 500}


async def test_bulk_statements_raise(session: AsyncSession) -> None:
    session.add(User(id=1, name="a"))
    await session.commit()
    with pytest.raises(UntrackedWriteError, match="users"):
        await session.execute(update(User).values(name="b"))
    with pytest.raises(UntrackedWriteError):
        await session.execute(delete(User))
    await session.rollback()


async def test_bulk_escape_hatch_and_raw_sql_not_captured(session: AsyncSession) -> None:
    session.add(User(id=1, name="a"))
    await session.commit()
    await session.execute(update(User).values(name="b").execution_options(crudcdc_untracked=True))
    await session.execute(text("update users set name = 'c'"))
    await session.commit()
    assert ops(await events(session)) == [("users", "insert")]


async def test_bulk_on_untracked_table_allowed(session: AsyncSession) -> None:
    session.add(Log(id=1, line="x"))
    await session.commit()
    await session.execute(update(Log).values(line="y"))
    await session.commit()


async def test_new_parent_adopting_existing_child(session: AsyncSession) -> None:
    p1, kid = Parent(id=1, name="a"), Kid(id=1)
    p1.kids.append(kid)
    session.add(p1)
    await session.commit()
    session.add(Parent(id=2, name="b", kids=[kid]))  # p1 untouched
    await session.commit()
    evs = await events(session)
    upd = [e for e in evs if e.table == "kids" and e.op == "update"]
    assert len(upd) == 1 and (upd[0].before["parent_id"], upd[0].after["parent_id"]) == (1, 2)


async def test_schema_qualified_table(session: AsyncSession) -> None:
    if session.get_bind().dialect.name != "postgresql":
        pytest.skip("schemas need Postgres")
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    from crudcdc import track

    class Billing(DeclarativeBase):
        pass

    class Invoice(Billing):
        __tablename__ = "invoices"
        __table_args__ = {"schema": "billing"}
        id: Mapped[int] = mapped_column(primary_key=True)

    track(Invoice)
    conn = await session.connection()
    await conn.execute(text("drop schema if exists billing cascade"))
    await conn.execute(text("create schema billing"))
    await conn.run_sync(Billing.metadata.create_all)
    session.add(Invoice(id=1))
    await session.commit()
    assert (await events(session))[0].table == "billing.invoices"


async def test_track_before_false_still_sees_collection_moves(session: AsyncSession) -> None:
    from crudcdc import track

    track(Kid, track_before=False)
    try:
        p1, p2, kid = Parent(id=1, name="a"), Parent(id=2, name="b", kids=[]), Kid(id=1)
        p1.kids.append(kid)
        session.add_all([p1, p2])
        await session.commit()
        p1.kids.remove(kid)
        p2.kids.append(kid)  # parent_id is only set during the flush
        await session.commit()
        last = (await events(session))[-1]
        assert (last.table, last.op, last.before, last.changed) == (
            "kids",
            "update",
            None,
            ("parent_id",),
        )
    finally:
        track(Kid)
