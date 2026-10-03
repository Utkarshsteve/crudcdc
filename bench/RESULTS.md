# Benchmark results

What tracking costs a write: the same table shape, tracked vs untracked. Produced by
`bench/writes.py`; median of 9 timed runs after a warm-up, models interleaved.

- **Machine:** Apple M5 Pro, 24 GB, macOS 27.0.1
- **Postgres 17.11** in Docker Desktop (15 CPUs, 7.7 GB), reached over `localhost`
- **SQLite 3.53.1**, file database
- Python 3.13.15, SQLAlchemy 2.1.2, asyncpg 0.31.0, aiosqlite 0.22.1, crudcdc 0.1.0 + capture fixes
- Table: `id`, `name`, `email`, `score`. 1-row scenarios: 300 transactions per run.
  Batch scenarios: 5 transactions of 1,000 rows per run.

"Overhead" is how much longer a tracked write takes: `untracked ops/s ÷ tracked ops/s − 1`.

## Reading the numbers

- A tracked write also inserts its change row (with `before`/`after` as JSON) in the same
  transaction. That second row is the cost of the outbox pattern, and most of what's below.
- **One row per transaction** (typical web request): **+25–38% on Postgres, +31–35% on SQLite**.
  The commit dominates, so the extra row is a fraction of the time.
- **1,000 rows per transaction:** +85–97% on Postgres. Every row now writes two rows and is
  encoded to JSON in Python, and there's no commit cost to hide it behind.
- **Docker on macOS adds latency to every round trip**, which makes the per-transaction share of
  the cost (and so the 1-row overhead %) look smaller than against a database on a low-latency
  network. Batch numbers are less affected.
- `track_before=False` barely changes these numbers: on a 4-column table, the work it skips
  (encoding unchanged columns) is small. It matters for wide rows or large JSON columns.

### SQLite (file)

| Scenario | untracked ops/s | tracked ops/s | overhead | tracked, no before | overhead |
|---|---:|---:|---:|---:|---:|
| insert, 1 row per transaction | 2,183 | 1,621 | +35% | 1,598 | +37% |
| update, 1 row per transaction | 1,647 | 1,243 | +32% | 1,241 | +33% |
| delete, 1 row per transaction | 1,655 | 1,267 | +31% | 1,279 | +29% |
| insert, 1000 rows per transaction | 98,850 | 59,995 | +65% | 59,677 | +66% |
| update, 1000 rows per transaction | 118,230 | 51,412 | +130% | 51,380 | +130% |

Feed: 117,501 events/s (read_for + ack + commit, batches of 500)

### Postgres

| Scenario | untracked ops/s | tracked ops/s | overhead | tracked, no before | overhead |
|---|---:|---:|---:|---:|---:|
| insert, 1 row per transaction | 1,639 | 1,192 | +38% | 1,225 | +34% |
| update, 1 row per transaction | 1,148 | 905 | +27% | 918 | +25% |
| delete, 1 row per transaction | 1,161 | 924 | +26% | 926 | +25% |
| insert, 1000 rows per transaction | 76,540 | 41,351 | +85% | 39,639 | +93% |
| update, 1000 rows per transaction | 55,103 | 27,992 | +97% | 29,512 | +87% |

Feed: 91,282 events/s (read_for + ack + commit, batches of 500)

## Before and after the capture fixes (2026-10-03)

The first benchmark run found three inefficiencies, fixed in the same commit as this file:
the transaction id cost an extra round trip per Postgres flush, tables were re-sorted for every
changed object, and `track_before=False` still built the full "before" row. Tracked throughput,
same machine, run back to back:

| Scenario | Postgres before → after | SQLite before → after |
|---|---:|---:|
| insert, 1 row per transaction | 998 → 1,192 (+19%) | 1,618 → 1,621 |
| update, 1 row per transaction | 809 → 905 (+12%) | 1,215 → 1,243 |
| delete, 1 row per transaction | 819 → 924 (+13%) | 1,271 → 1,267 |
| insert, 1000 rows per transaction | 38,723 → 41,351 (+7%) | 54,978 → 59,995 (+9%) |
| update, 1000 rows per transaction | 27,064 → 27,992 (+3%) | 46,487 → 51,412 (+11%) |
| untracked models, insert 1000 rows per transaction | 68,827 → 76,540 (+11%) | 86,636 → 98,850 (+14%) |

The last row is the surprise: before the fix, sessions paid the per-object sort even for
untracked models.

## Run it

```
docker start crudcdc-pg
CRUDCDC_TEST_PG_URL=postgresql+asyncpg://postgres:pg@localhost:55432/postgres uv run python bench/writes.py
```
