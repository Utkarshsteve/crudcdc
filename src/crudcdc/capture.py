"""Change capture through SQLAlchemy session events: any ORM write to a tracked model."""

from __future__ import annotations

import datetime as dt
import decimal
import enum
import uuid
from collections.abc import Callable
from typing import Any, cast

from sqlalchemy import Table, event, insert, inspect, select, text
from sqlalchemy.orm import Mapper, ORMExecuteState, Session, UOWTransaction
from sqlalchemy.orm.state import InstanceState

from .errors import EncodingError, UntrackedWriteError
from .models import Change

_models: dict[Mapper[Any], bool] = {}  # mapper -> track_before
_registries: dict[Any, bool] = {}  # registry of a tracked declarative base -> track_before
_encoders: dict[type, Callable[[Any], Any]] = {}
_BEFORE = "_crudcdc_before"
_TX = "_crudcdc_tx"


def track(*targets: type[Any], track_before: bool = True) -> None:
    """Capture changes for these models, or for every model of a declarative base."""
    for target in targets:
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
    """Teach the change feed how to turn values of ``type_`` into JSON."""
    _encoders[type_] = fn


def _table(mapper: Mapper[Any]) -> str:
    return cast(Table, mapper.local_table).fullname


def _track_before(mapper: Mapper[Any]) -> bool | None:
    """None if untracked, else whether to record ``before``."""
    if mapper in _models:
        return _models[mapper]
    return _registries.get(mapper.registry)


def _encode(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, enum.Enum):
        return _encode(value.value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, (decimal.Decimal, uuid.UUID)):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(k): _encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(v) for v in value]
    for cls in type(value).__mro__:
        if cls in _encoders:
            return _encode(_encoders[cls](value))
    raise EncodingError(f"can't encode {type(value).__name__!r}; use crudcdc.register_encoder")


def _columns(mapper: Mapper[Any]) -> list[tuple[str, str]]:
    """(attribute key, column name) for every mapped column."""
    return [(p.key, p.columns[0].name) for p in mapper.column_attrs]


def _row(state: InstanceState[Any], values: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for key, name in _columns(state.mapper):
        try:
            out[name] = _encode(values[key])
        except EncodingError as e:
            raise EncodingError(f"{_table(state.mapper)}.{name}: {e}") from None
    return out


def _current(state: InstanceState[Any]) -> dict[str, Any]:
    return {key: getattr(state.obj(), key) for key, _ in _columns(state.mapper)}


def _old_values(session: Session, state: InstanceState[Any]) -> dict[str, Any]:
    """Column values as they are in the database, before this flush."""
    values, unknown = {}, []
    for key, _ in _columns(state.mapper):
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
        if _track_before(state.mapper) is None or state.key is None:
            continue
        befores[state] = _row(state, _old_values(session, state))
    session.info[_BEFORE] = befores


def _pk(state: InstanceState[Any], row: dict[str, Any]) -> dict[str, Any]:
    return {c.name: row[c.name] for c in state.mapper.primary_key}


def _order(state: InstanceState[Any]) -> int:
    # ponytail: re-sorts tables per object; cache per flush if huge flushes show up in profiles
    table = cast(Table, state.mapper.local_table)
    tables = table.metadata.sorted_tables
    return tables.index(table) if table in tables else 0


def _after_flush(session: Session, _ctx: UOWTransaction) -> None:
    befores = session.info.pop(_BEFORE, {})
    rows: list[dict[str, Any]] = []
    deletes: list[dict[str, Any]] = []
    # Flush order: parents before children for inserts/updates, children first for deletes.
    for obj in sorted(session.new, key=lambda o: _order(inspect(o))):
        state = inspect(obj)
        if _track_before(state.mapper) is not None:
            after = _row(state, _current(state))
            rows.append(_change("insert", state, None, after, (), _pk(state, after)))
    for obj in sorted(session.dirty, key=lambda o: _order(inspect(o))):
        state = inspect(obj)
        keep_before = _track_before(state.mapper)
        if keep_before is None or state not in befores:
            continue
        before, after = befores[state], _row(state, _current(state))
        changed = tuple(k for k in after if after[k] != before.get(k))
        if changed:
            kept = before if keep_before else None
            rows.append(_change("update", state, kept, after, changed, _pk(state, after)))
    for obj in sorted(session.deleted, key=lambda o: -_order(inspect(o))):
        state = inspect(obj)
        keep_before = _track_before(state.mapper)
        if keep_before is None or state not in befores:
            continue
        before = befores[state]
        deletes.append(
            _change("delete", state, before if keep_before else None, None, (), _pk(state, before))
        )
    rows += deletes
    if not rows:
        return
    conn = session.connection()
    if conn.dialect.name == "postgresql":
        xid = int(conn.execute(text("select pg_current_xact_id()::text")).scalar_one())
        tx_id = str(xid)
    else:
        xid, tx_id = 0, session.info.setdefault(_TX, uuid.uuid4().hex)
    conn.execute(insert(Change), [{**r, "xid": xid, "tx_id": tx_id} for r in rows])


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


def _guard_bulk(state: ORMExecuteState) -> None:
    if not (state.is_update or state.is_delete):
        return
    if state.execution_options.get("crudcdc_untracked"):
        return
    for mapper in state.all_mappers:
        if _track_before(mapper) is not None:
            raise UntrackedWriteError(
                f"bulk {'UPDATE' if state.is_update else 'DELETE'} on tracked table "
                f"{_table(mapper)!r} would not appear in the change feed; "
                "use ORM objects, or .execution_options(crudcdc_untracked=True) to skip capture"
            )
