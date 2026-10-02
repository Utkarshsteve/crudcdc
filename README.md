# crudcdc

Async CRUD on SQLAlchemy 2.1 with a built-in **change data capture feed** that consumers can read.

Every create/update/delete is recorded in a `crudcdc_changes` table **in the same transaction** as
the write (the outbox pattern), so a change is in the feed if and only if the write committed.

```python
from crudcdc import AsyncCRUD, CDCBase, ChangeFeed

users = AsyncCRUD(User)  # User is your SQLAlchemy model
await conn.run_sync(CDCBase.metadata.create_all)

async with session.begin():
    u = await users.create(session, name="ada")
    await users.update(session, u.id, name="grace")

feed = ChangeFeed(session)
batch = await feed.read_for("billing", limit=100)  # resumes from this consumer's offset
for change in batch.changes:  # ordered by seq
    print(change.op, change.table_name, change.pk, change.before, change.after)
await feed.ack("billing", batch.next_cursor)  # commit your session to persist the offset
```

- **Pull:** `read(since=cursor)` or `read_for(consumer)` + `ack(consumer, cursor)`. Delivery is
  at-least-once; consumers keep their own offsets.
- **Retention:** `prune(older_than=...)` never deletes rows a registered consumer hasn't acked.
- **Payload:** `op`, `table_name`, `pk`, `before`, `after` (JSON), `created_at`. Pass
  `AsyncCRUD(Model, track_before=False)` to skip `before`.

## Install

```
pip install crudcdc[sqlite]     # aiosqlite
pip install crudcdc[postgres]   # asyncpg
```

Python 3.11+, SQLAlchemy 2.1+.

## Known limitations (v0.1)

- Writes made outside `AsyncCRUD` (raw SQL, other services) are not captured. A Postgres logical
  replication backend is planned.
- On Postgres, concurrent transactions can commit sequence numbers out of order, so a reader can
  skip a row. A snapshot-aware reader (`pg_snapshot_xmin`) is planned. SQLite is single-writer and
  unaffected.
- No push sinks yet (webhook / callback relay next, Kafka and Redis as extras later).

## Development

```
uv sync
uv run pytest
uv run ruff check . && uv run mypy src
```
