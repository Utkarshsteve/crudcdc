"""Change capture through SQLAlchemy session events: any ORM write to a tracked model."""

from __future__ import annotations

import datetime as dt
import decimal
import enum
import functools
import uuid
from collections.abc import Callable
from typing import Any, cast

from sqlalchemy import Table, event, insert, inspect, literal_column, select
from sqlalchemy.orm import Mapper, ORMExecuteState, Session, UOWTransaction
from sqlalchemy.orm.state import InstanceState

from .errors import EncodingError, UntrackedWriteError
from .models import CDCBase, Change

_models: dict[Mapper[Any], bool] = {}  # mapper -> track_before
_registries: dict[Any, bool] = {}  # registry of a tracked declarative base -> track_before
_BEFORE = "_crudcdc_before"
_TX = "_crudcdc_tx"


def track(*targets: type[Any], track_before: bool = True) -> None:
    """Capture changes for these models, or for every model of a declarative base."""
    for target in targets:
        if issubclass(target, CDCBase):
            raise ValueError("crudcdc's own tables can't be tracked")
        if "__table__" not in vars(target) and "__tablename__" not in vars(target):
            _registries[target.registry] = track_before  # a base: models may be defined later
        else:
            _models[inspect(target)] = track_before
    if not event.contains(Session, "before_flush", _before_flush):
        event.listen(Session, "before_flush", _before_flush)
        event.listen(Session, "after_flush", _after_flush)
        event.listen(Session, "after_transaction_end", _after_transaction_end)
        event.listen(Session, "do_orm_execute", _guard_bulk)


def register_encoder(type_: type, fn: Callable[[Any], Any]) -> None:
    """Teach the change feed how to turn values of ``type_`` (and subclasses) into JSON."""
    _encode.register(type_, lambda value: _encode(fn(value)))


def _table(mapper: Mapper[Any]) -> str:
    return cast(Table, mapper.local_table).fullname


def _track_before(mapper: Mapper[Any]) -> bool | None:
    """None if untracked, else whether to record ``before``."""
    if mapper in _models:
        return _models[mapper]
    return _registries.get(mapper.registry)


@functools.singledispatch
def _encode(value: Any) -> Any:
    raise EncodingError(f"can't encode {type(value).__name__!r}; use crudcdc.register_encoder")


@_encode.register
def _(value: bool | int | float | str | None) -> Any:
    return value


@_encode.register
def _(value: enum.Enum) -> Any:
    return _encode(value.value)


@_encode.register
def _(value: dt.date | dt.time) -> str:  # dt.datetime is a dt.date
    return value.isoformat()


@_encode.register
def _(value: decimal.Decimal | uuid.UUID) -> str:
    return str(value)


@_encode.register
def _(value: bytes) -> str:
    return value.hex()


@_encode.register
def _(value: dict) -> dict[str, Any]:  # type: ignore[type-arg]
    return {str(k): _encode(v) for k, v in value.items()}


@_encode.register
def _(value: list | tuple) -> list[Any]:  # type: ignore[type-arg]
    return [_encode(v) for v in value]


@functools.cache
def _columns(mapper: Mapper[Any]) -> tuple[tuple[str, str], ...]:
    """(attribute key, column name) for every mapped column."""
    return tuple((p.key, p.columns[0].name) for p in mapper.column_attrs)


@functools.cache
def _identity_keys(mapper: Mapper[Any]) -> frozenset[str]:
    """Attribute keys of primary and foreign key columns."""
    return frozenset(
        p.key for p in mapper.column_attrs if p.columns[0].primary_key or p.columns[0].foreign_keys
    )


def _row(state: InstanceState[Any], values: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for key, name in _columns(state.mapper):
        if key not in values:
            continue
        try:
            out[name] = _encode(values[key])
        except EncodingError as e:
            raise EncodingError(f"{_table(state.mapper)}.{name}: {e}") from None
    return out


def _current(state: InstanceState[Any]) -> dict[str, Any]:
    return {key: getattr(state.obj(), key) for key, _ in _columns(state.mapper)}


def _old_values(session: Session, state: InstanceState[Any], keys: list[str]) -> dict[str, Any]:
    """Values of ``keys`` as they are in the database, before this flush."""
    values, unknown = {}, []
    for key in keys:
        hist = state.attrs[key].history
        if hist.deleted:
            values[key] = hist.deleted[0]
        elif hist.unchanged:
            values[key] = hist.unchanged[0]
        elif hist.added:
            unknown.append(key)  # set without loading the old value first
        else:
            values[key] = getattr(state.obj(), key)  # expired and unchanged: loads it
    if unknown:
        mapper = state.mapper
        cols = [mapper.get_property(k).columns[0] for k in unknown]
        where = [c == v for c, v in zip(mapper.primary_key, state.identity or (), strict=True)]
        row = session.connection().execute(select(*cols).where(*where)).one()
        values.update(zip(unknown, row, strict=True))
    return values


def _before_flush(session: Session, _ctx: UOWTransaction, _instances: object) -> None:
    befores: dict[InstanceState[Any], dict[str, Any]] = {}
    candidates = [*session.dirty, *session.deleted]
    # Objects moved between collections only get their foreign keys set during the flush.
    for obj in [*session.new, *session.dirty]:
        state = inspect(obj)
        for rel in state.mapper.relationships:
            hist = state.attrs[rel.key].history
            candidates += [o for o in (*hist.added, *hist.deleted) if o is not None]
    for obj in dict.fromkeys(candidates):
        state = inspect(obj)
        keep_before = _track_before(state.mapper)
        if keep_before is None or state.key is None:
            continue
        keys = [key for key, _ in _columns(state.mapper)]
        if not keep_before:
            # Only what's needed to find the changed columns and the primary key. Foreign keys
            # are always included: a move between collections sets them later, during the flush.
            identity = _identity_keys(state.mapper)
            keys = [k for k in keys if k in identity or state.attrs[k].history.has_changes()]
        befores[state] = _row(state, _old_values(session, state, keys))
    session.info[_BEFORE] = befores


def _pk(state: InstanceState[Any], row: dict[str, Any]) -> dict[str, Any]:
    return {c.name: row[c.name] for c in state.mapper.primary_key}


def _table_ranks(states: list[InstanceState[Any]]) -> dict[Table, int]:
    """Dependency order of the tables involved: parents before children."""
    ranks: dict[Table, int] = {}
    for metadata in {cast(Table, s.mapper.local_table).metadata for s in states}:
        ranks.update((t, i) for i, t in enumerate(metadata.sorted_tables))
    return ranks


def _after_flush(session: Session, _ctx: UOWTransaction) -> None:
    befores = session.info.pop(_BEFORE, {})
    new = [inspect(o) for o in session.new]
    dirty = [inspect(o) for o in session.dirty]
    deleted = [inspect(o) for o in session.deleted]
    tracked = [s for s in (*new, *dirty, *deleted) if _track_before(s.mapper) is not None]
    if not tracked:
        return
    ranks = _table_ranks(tracked)

    def rank(state: InstanceState[Any]) -> int:
        return ranks.get(cast(Table, state.mapper.local_table), 0)

    rows: list[dict[str, Any]] = []
    # Flush order: parents before children for inserts/updates, children first for deletes.
    for state in sorted(new, key=rank):
        if _track_before(state.mapper) is not None:
            after = _row(state, _current(state))
            rows.append(_change("insert", state, None, after, (), _pk(state, after)))
    for state in sorted(dirty, key=rank):
        keep_before = _track_before(state.mapper)
        if keep_before is None or state not in befores:
            continue
        before, after = befores[state], _row(state, _current(state))
        changed = tuple(k for k in after if k in before and after[k] != before[k])
        if changed:
            kept = before if keep_before else None
            rows.append(_change("update", state, kept, after, changed, _pk(state, after)))
    for state in sorted(deleted, key=rank, reverse=True):
        keep_before = _track_before(state.mapper)
        if keep_before is None or state not in befores:
            continue
        before = befores[state]
        rows.append(
            _change("delete", state, before if keep_before else None, None, (), _pk(state, before))
        )
    if not rows:
        return
    conn = session.connection()
    stmt = insert(Change)
    if conn.dialect.name == "postgresql":
        # The database fills in the transaction id: no separate round trip to fetch it.
        stmt = stmt.values(
            xid=literal_column("pg_current_xact_id()::text::bigint"),
            tx_id=literal_column("pg_current_xact_id()::text"),
        )
    else:
        tx_id = session.info.setdefault(_TX, uuid.uuid4().hex)
        rows = [{**r, "xid": 0, "tx_id": tx_id} for r in rows]
    conn.execute(stmt, rows)


def _change(
    op: str,
    state: InstanceState[Any],
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    changed: tuple[str, ...],
    pk: dict[str, Any],
) -> dict[str, Any]:
    return {
        "op": op,
        "table_name": _table(state.mapper),
        "pk": pk,
        "before": before,
        "after": after,
        "changed": list(changed),
    }


def _after_transaction_end(session: Session, transaction: Any) -> None:
    if transaction.parent is None:
        session.info.pop(_TX, None)
        session.info.pop(_BEFORE, None)


def _tracked_tables() -> set[Table]:
    # Not cached: track(Base) allows models defined later, and a cache cleared only by track()
    # would miss them. Runs only for DML statements; registries are small.
    tables = {cast(Table, m.local_table) for m in _models}
    for registry in _registries:
        tables |= {cast(Table, m.local_table) for m in registry.mappers}
    return tables


def _guard_bulk(state: ORMExecuteState) -> None:
    """ORM DML on a tracked table bypasses the unit of work, so it would never reach the feed."""
    if not (state.is_insert or state.is_update or state.is_delete):
        return
    if state.execution_options.get("crudcdc_untracked"):
        return
    # Model.__table__ statements carry no mappers, so check the target table as well.
    table = getattr(state.statement, "table", None)
    tables = {cast(Table, m.local_table) for m in state.all_mappers if _track_before(m) is not None}
    if table is not None and table in _tracked_tables():
        tables.add(table)
    if tables:
        kind = "INSERT / upsert" if state.is_insert else "UPDATE" if state.is_update else "DELETE"
        raise UntrackedWriteError(
            f"bulk {kind} on tracked table {min(t.fullname for t in tables)!r} would not appear "
            "in the change feed; use ORM objects, or .execution_options(crudcdc_untracked=True) "
            "to skip capture"
        )
