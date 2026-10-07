# crudcdc

Change data capture for SQLAlchemy 2.1 (async), with a change feed that consumers read.

Every ORM insert, update and delete on a tracked model is recorded in a `crudcdc_changes` table
**in the same transaction** as the write (the outbox pattern). Cascades, relationship changes and
plain `session.add` are all captured. A change is in the feed if and only if its write committed.

```python
import crudcdc
from crudcdc import CDCBase, ChangeFeed

crudcdc.track(Base)                       # every model of this declarative base
async with engine.begin() as conn:
    await conn.run_sync(CDCBase.metadata.create_all)

async with Session() as s:                # any AsyncSession: nothing else to install
    user = await s.get(User, 1)
    user.name = "grace"
    await s.commit()
```

## Reading the feed

```python
async with Session() as s:
    feed = ChangeFeed(s)
    batch = await feed.read_for("billing", limit=100)   # resumes from the stored offset
    for e in batch.events:
        print(e.op, e.table, e.pk, e.changed, e.before, e.after)
    if batch.next_cursor:
        await feed.ack("billing", batch.next_cursor)
    await s.commit()
```

**Exactly-once processing:** offsets live in the same database. Write your results and `ack` in
the same transaction, and either both happen or neither does. If a worker crashes before
committing, the batch is delivered again.

- `read_for` holds the consumer until the transaction ends. A second worker on the same name
  gets an empty batch (Postgres) or waits (SQLite). Keep batches quick.
- `read(since=cursor)` reads without offsets. Cursors are opaque strings; store them as-is.
- `prune(older_than)` deletes old events, never past the slowest consumer. `forget(name)` drops a
  consumer that no longer exists.

## Events

```python
ChangeEvent(
    seq=42,                          # unique event id
    op="update",                     # insert | update | delete
    table="users",                   # schema-qualified when the table has a schema
    pk={"id": 1},
    before={"id": 1, "name": "ada"}, # None for insert, or with track(Model, track_before=False)
    after={"id": 1, "name": "grace"},# None for delete
    changed=("name",),
    tx_id="1234",                    # same for every event of one transaction
    changed_at=datetime(...),        # UTC: transaction start on Postgres, write time on SQLite
)
```

Keys are database column names. Values are JSON: enums become their value, dates ISO strings,
`Decimal`/`UUID` strings, bytes hex. Other types: `crudcdc.register_encoder(MyType, fn)`; until
then writing one raises `EncodingError` and the transaction rolls back.

## Not captured

- Raw SQL (`text(...)`) and writes from other services.
- Bulk ORM statements bypass the unit of work, so on a tracked table they **raise**
  `UntrackedWriteError` instead of writing silently: `insert(User)` with a list of rows,
  `insert/update/delete(User)`, upserts (`on_conflict_do_update` / `do_nothing`), and the same
  statements aimed at `User.__table__`. Add `.execution_options(crudcdc_untracked=True)` to run
  one anyway.
- Core statements executed directly on a connection or engine, for example
  `(await session.connection()).execute(insert(User), rows)`: they never pass through the
  session, so crudcdc can't see them.
- Statements built on a `Table` object other than the model's own (a reflected
  `Table("users", MetaData(), autoload_with=...)` or a separate Core definition of the same
  table): the guard recognises a tracked table by its model's `Table` object.
- The legacy `Session.bulk_save_objects`, `bulk_insert_mappings` and `bulk_update_mappings`. They
  fire no SQLAlchemy events at all, so crudcdc can't capture them or even refuse them. Don't use
  them on tracked models.
- Database-level `ON DELETE CASCADE` with `passive_deletes=True`.

crudcdc's own tables (`CDCBase`) can't be tracked: `track(CDCBase)` raises `ValueError`.

On Postgres, a long-running transaction holds back events committed after it started, until it
ends. That's what guarantees no consumer skips an event.

Migrations: include `CDCBase.metadata` in your Alembic `target_metadata`.

## Performance

A tracked write also inserts its change row in the same transaction. On an M5 Pro with Postgres 17
in Docker: **+25–38% per single-row transaction**, +85–97% for 1,000-row transactions. Details,
method and SQLite numbers: [bench/RESULTS.md](bench/RESULTS.md).

## Install

```
pip install "crudcdc[postgres]"   # asyncpg
pip install "crudcdc[sqlite]"     # aiosqlite
```

Python 3.11+, SQLAlchemy 2.1+.

## Development

```
uv sync
docker run -d --name crudcdc-pg -e POSTGRES_PASSWORD=pg -p 55432:5432 postgres:17-alpine
export CRUDCDC_TEST_PG_URL=postgresql+asyncpg://postgres:pg@localhost:55432/postgres
uv run pytest                     # Postgres tests skip if the variable isn't set
uv run ruff check . && uv run mypy src
```
