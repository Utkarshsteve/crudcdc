---
title: crudcdc v0.1 core redesign spec
type: spec
project: "[[crudcdc]]"
created: 2026-10-03
status: implemented 2026-10-03 (see §9 for deviations)
repo_copy: ~/Projects/crudcdc/docs/specs/2026-10-03-core-redesign.md
tags: [crudcdc, spec, design]
---

# crudcdc v0.1: core redesign

Result of reviewing the first scaffold (see [[Scaffold]]) and the decisions in [[Design-Decisions]]. Every section below was approved on 2026-10-03.

## 1. Why the scaffold changes

Probes against the scaffold (SQLite, SQLAlchemy 2.1.2) showed:

| Probe | Result |
|---|---|
| Cascade delete of a parent with two children | ❌ children deleted, only `parents/delete` in the feed |
| Plain `session.add(obj)` + commit | ❌ no change recorded |
| Model with an `Enum` column | ❌ `create` crashes: not JSON serializable |
| `server_default` column | ✅ |
| `update` after commit with `expire_on_commit=True` | ✅ |

Problems this spec fixes:
1. **Capture depends on the call path.** Only `AsyncCRUD` methods logged changes. Cascades, relationship changes, plain ORM writes and bulk statements were silently missed.
2. **Postgres readers can skip changes.** Sequence numbers are assigned at insert, not at commit, so a later `seq` can become visible before an earlier one. A consumer that acks the later one never sees the earlier one.
3. **Encoding crashes or loses information** (enums crash; nested JSON values unhandled).
4. **The public payload is fragile:** comma-joined `pk`, Python attribute names instead of column names, `table_name` from `__tablename__` (no schema), ORM `Change` objects returned to consumers.
5. **Consumer gaps:** two workers on one consumer name both process the same batch; an abandoned consumer blocks `prune` forever.
6. Small: `list()` has no `ORDER BY`; the `table_name` index doesn't serve filtered reads; `op` is an untyped string.

## 2. Decisions

| # | Decision |
|---|---|
| D1 | Capture via **SQLAlchemy session events**, not inside `AsyncCRUD`. Any ORM write to a tracked model is captured, including cascades. `AsyncCRUD` becomes an optional convenience with no capture logic. |
| D2 | **Opt-in by model only:** `crudcdc.track(*models_or_base)`. The events are attached to all sessions; untracked models cost nothing. There is no `install()` step, so a forgotten session setup cannot cause silent gaps. |
| D3 | **Bulk ORM statements** (`update(Model)…`, `delete(Model)…`) on a tracked table raise `UntrackedWriteError` unless the statement carries `.execution_options(crudcdc_untracked=True)`. |
| D4 | The **`ChangeEvent`** contract in §3. |
| D5 | **Postgres visibility:** each change row stores its transaction id `xid` (`pg_current_xact_id()`). The feed is ordered by **position `(xid, seq)`**, not `seq` alone, and the reader only returns rows with `xid < pg_snapshot_xmin(pg_current_snapshot())`. Every transaction below that bound has finished, and every future change will have `xid ≥` the bound, so nothing can ever appear behind a consumer's position. Ordering by `seq` alone is **not** enough: a lower-xid transaction can hold a higher `seq` and be returned while a higher-xid, lower-`seq` transaction is still running, and that change would then be skipped (found in the spec self-review). No writer cost; a long-running transaction delays the feed until it ends (documented). On SQLite (single writer) `xid` is always 0, so the order is `seq`. |
| D5a | **Opaque cursor:** the public cursor is a string token encoding the position (`"<xid>:<seq>"`). Consumers only store and pass back tokens; `None` means "from the beginning". Same format on both backends. `ChangeEvent.seq` remains the event's unique id but is not the cursor. |
| D6 | **Consumer locking:** `read_for` locks the consumer's offset row with `SELECT … FOR UPDATE SKIP LOCKED` until the transaction ends. A second worker on the same name gets an empty batch. A crashed worker's transaction rolls back, so the batch is redelivered. |
| D7 | `forget(consumer)` removes a consumer's offset so it stops blocking `prune`. |
| D8 | **Never write data without its change:** any capture failure (encoding, bulk statement) fails the flush and the transaction rolls back. |

Unchanged from [[Design-Decisions]]: async only, Python ≥3.11, SQLAlchemy ≥2.1, outbox in the same transaction, "pull" = change feed, MIT, no push sinks in 0.1.

## 3. Public contract: `ChangeEvent`

```python
@dataclass(frozen=True, slots=True)
class ChangeEvent:
    seq: int                          # unique event id (not the cursor; see D5a)
    op: Literal["insert", "update", "delete"]
    table: str                        # Table.fullname: "users", or "billing.invoices" with a schema
    pk: dict[str, Any]                # {"id": 5}; composite: {"org_id": 1, "user_id": 7}
    before: dict[str, Any] | None     # None for insert, and for update/delete when track_before=False
    after: dict[str, Any] | None      # None for delete
    changed: tuple[str, ...]          # update: changed column names; insert/delete: ()
    tx_id: str                        # same value for every event of one transaction
    changed_at: datetime              # UTC, database clock (see below)
```

- Keys in `pk`, `before`, `after` and `changed` are **database column names**.
- On update, `before` and `after` are the full row (all mapped columns), not only the changed ones.
- **Encoding** (`before`, `after`, `pk` values) to JSON types: `Enum` → `.value`; `datetime`/`date`/`time` → ISO 8601 string; `Decimal`, `UUID` → string; `bytes` → hex string; `dict`/`list` (JSON columns) encoded recursively. Custom types: `crudcdc.register_encoder(type, fn)`. Anything else raises `EncodingError` (D8). Original Python types are not preserved; consumers that need them read the table schema.
- `changed_at`: from the database clock, in UTC. On Postgres it is `now()`, the **start time of the transaction**; on SQLite it is `CURRENT_TIMESTAMP` at write time. It is not the commit time, which the database cannot know when the row is written (hence not `committed_at`).
- `tx_id`: on Postgres, the transaction id as a decimal string; on SQLite, a UUID4 generated when the session's transaction begins.
- `track(Model, track_before=False)` disables `before` for that model.

This shape is frozen by a contract test (§7). Changing it after publishing is a breaking change.

## 4. Modules

| Module | Responsibility | Depends on |
|---|---|---|
| `tracking.py` | `track(*models_or_base, track_before=True)` registry; `is_tracked(mapper)`; attaches the session events once, on first `track()` | SQLAlchemy |
| `capture.py` | `before_flush`: before-images for dirty/deleted tracked objects. `after_flush`: one change row per object that actually changed, inserted through `session.connection()`. `do_orm_execute`: enforces D3. `after_begin` (SQLite): generates `tx_id` | `tracking`, `encoding`, `models` |
| `encoding.py` | Row → JSON dict by column name; encoder registry; `EncodingError` | — |
| `models.py` | `CDCBase`; `Change` table `crudcdc_changes` (`seq`, `xid` BigInteger not null, 0 on SQLite, `op`, `table`, `pk` JSON, `before` JSON, `after` JSON, `changed` JSON, `tx_id`, `changed_at`); indexes `(xid, seq)` and `(table, xid, seq)`. `ConsumerOffset` (`crudcdc_consumer_offsets`): `consumer` PK, position stored as two columns `xid`, `seq` (so `prune` can take the minimum in SQL) | SQLAlchemy |
| `events.py` | `ChangeEvent` dataclass | — |
| `cursor.py` | Encode/decode the opaque token `"<xid>:<seq>"` ↔ position; invalid token raises `InvalidCursorError` | — |
| `feed.py` | `ChangeFeed(session)`: `read(since: str \| None, limit, tables)`, `read_for(consumer, limit, tables)`, `offset(consumer) -> str \| None`, `ack(consumer, cursor: str)`, `forget(consumer)`, `prune(older_than)`; returns `Batch(events: tuple[ChangeEvent, ...], next_cursor: str \| None)` | `models`, `events`, `cursor` |
| `crud.py` | `AsyncCRUD[T]`: create, get, list (ordered by primary key), update, delete as plain ORM operations | SQLAlchemy |
| `errors.py` | `CrudCDCError` base, `UntrackedWriteError`, `EncodingError`, `InvalidCursorError` | — |

## 5. Data flow

**Write:** user code changes ORM objects (directly or via `AsyncCRUD`) → flush → `before_flush` records before-images for tracked dirty and deleted objects → SQLAlchemy executes the SQL (cascades included) → `after_flush` inserts one `crudcdc_changes` row per tracked object that actually changed, on the same connection → commit saves data and changes together; rollback discards both.

**Read:** `read_for("billing")` → lock the offset row (`FOR UPDATE SKIP LOCKED`; if locked, return an empty batch) → `SELECT … WHERE (xid, seq) > (offset.xid, offset.seq) [AND table IN …] [AND xid < pg_snapshot_xmin(pg_current_snapshot())] ORDER BY xid, seq LIMIT n` → `Batch` of `ChangeEvent`s → consumer processes, optionally writes results in the same transaction → `ack(consumer, batch.next_cursor)` → commit.

`next_cursor` rule: the token of the last returned event's `(xid, seq)`, or the input cursor unchanged if the batch is empty (`None` stays `None`). Events of one transaction are contiguous in this order.

## 6. Edge cases and errors

| Situation | Behavior |
|---|---|
| Bulk `update()`/`delete()` on a tracked table | `UntrackedWriteError` naming the table, unless `crudcdc_untracked=True` |
| Raw SQL (`text()`), other services, DB-level `ON DELETE CASCADE` with `passive_deletes=True` | **Not captured.** Documented in the README; fixed later by the logical replication backend |
| Value the encoder can't handle | `EncodingError` (table, column, type, hint to register an encoder); flush fails, transaction rolls back |
| Several changes to one row in one flush | One event: first before, last after |
| Same row changed in two flushes of one transaction | Two events, same `tx_id` |
| Savepoint rolled back | Its change rows roll back with it; capture state is per flush, nothing leaks |
| Update that changes no column value | No event |
| Primary key changed by an update | One `update` event; `pk` is the new key, `before` holds the old one |
| Untracked model | Ignored |
| Postgres long-running transaction | Feed holds back changes until it ends; documented |
| `read_for` while another worker holds the consumer | Empty batch, `next_cursor` = current offset |
| Malformed cursor token passed to `read` or `ack` | `InvalidCursorError` |
| `ack` with a position behind the stored one | Ignored (offsets never move backwards) |
| `prune(older_than)` | Deletes changes older than `older_than` (by `changed_at`) and at or before the slowest registered consumer's position `(xid, seq)`; with no consumers, everything older than `older_than` |

## 7. Testing

- Every test runs on **SQLite (aiosqlite)** and **Postgres (asyncpg)** via a parametrized fixture. Postgres comes from a CI service container, locally from Docker (`CRUDCDC_TEST_PG_URL`); Postgres tests skip when it isn't set.
- **Regression tests from the probes:** cascade delete emits child events; plain `session.add` is captured; enum column encodes; `server_default` and expired-object updates work.
- **Capture:** plain ORM writes and `AsyncCRUD` produce identical events; relationship changes; collapse within one flush; separate events across flushes with one `tx_id`; no-op update emits nothing; primary key change; savepoint rollback; transaction rollback; untracked model ignored; bulk statement raises and the escape hatch works; `EncodingError` rolls back the data write; `track_before=False`; schema-qualified `table`; column names differing from attribute names.
- **Feed (Postgres):** two concurrent transactions where the later `seq` commits first: the earlier change must not be skipped. Plus the self-review case: a lower-xid transaction writes the higher `seq` and commits while a higher-xid, lower-`seq` transaction is still open; after it commits, its change must still be delivered. A seq-only reader must fail both tests (reproduce, then fix).
- **Cursor:** token round-trip; malformed token raises; `None` reads from the start; tokens from SQLite and Postgres have the same format. Two workers on one consumer name: the second gets an empty batch. Crashed worker (rollback): batch redelivered. `forget` unblocks `prune`. `ack` never moves backwards.
- **Contract:** a golden JSON snapshot of a `ChangeEvent` for each `op`.
- Unchanged: `ruff`, `mypy --strict`, CI matrix 3.11–3.14 (plus the Postgres service).

## 8. Scope

**In 0.1:** everything in §2–§7, and a README rewritten around `track()`, with the "exactly-once: `ack` in the same transaction as your results" pattern as a main example and a "what is not captured" list.

**Out of 0.1** (roadmap): push sinks / webhook relay, Postgres logical replication backend, sync API, timed leases for slow consumers, expanding bulk statements into per-row events, Alembic helpers (the docs explain including `CDCBase.metadata` in migrations).

## 9. Implementation notes (2026-10-03)

Built as planned in [[Plan-v0.1]]; 75 tests pass on SQLite and Postgres 17 (Python 3.11, 3.13, 3.14). Deviations from the text above:

- **SQLite `read_for`:** SQLite has no row locks, so a second worker on the same consumer **waits** for the first to commit instead of getting an empty batch. Still no duplicates. (§6 said "empty batch" for both backends.)
- **First read of a new consumer name:** if two workers read a never-seen consumer at the same moment, the second waits for the first to commit (Postgres blocks on the new row). After that, the empty-batch behavior applies.
- **Collection moves:** moving an object between collections (`p1.kids.remove(k); p2.kids.append(k)`, or a new parent adopting it) only sets its foreign key during the flush, so `before_flush` also records before-images for objects added to or removed from a new or dirty object's relationships.
- **Before-image of a value set without loading the old one** (expired attribute): read from the database in `before_flush`, one `SELECT` per such object.
- **Known limitation found after release (2026-10-04, #12):** the `(xid, seq)` order guarantees no event is skipped, but **not per-row commit order**. A transaction with a lower xid can commit a change to a row after a higher-xid transaction already did. Consumers must treat `seq` as the row version (highest `seq` per row wins). Proper fix: #19.
- **ORM write paths that bypassed capture (found 2026-10-04, #11; fixed for 0.1.2):** the D3 guard (and §6) covered bulk update/delete with mappers only. Inserts, upserts and DML aimed at `Model.__table__` on tracked tables were silently not captured. The guard now covers all of them by checking the statement's target table too. Still not capturable, and documented in the README: the legacy `Session.bulk_*` methods, Core statements run directly on a connection or engine, and statements built on a different `Table` object for the same table.
- **Modules:** 6, not 8 (`tracking` + `encoding` merged into `capture.py`; `events` + `cursor` into `feed.py`).
