---
title: crudcdc v0.1 implementation plan
type: plan
project: "[[crudcdc]]"
spec: "[[Spec-v0.1]]"
created: 2026-10-03
status: awaiting go-ahead
repo_copy: ~/Projects/crudcdc/docs/plans/2026-10-03-core-redesign-plan.md
tags: [crudcdc, plan]
---

# Implementation plan for [[Spec-v0.1]]

Test-first: each task starts with tests that fail, then the minimum code to pass them. One commit per task. Every test runs on SQLite and Postgres.

## Simplifications (no contract change)
- **5 modules, not 8.** `tracking` + `encoding` merge into `capture.py`; `events` + `cursor` merge into `feed.py`. The public names in §3 of the spec stay the same.
- **Collapsing several changes to one row in one flush needs no code.** SQLAlchemy already flushes each object once, and attribute history keeps the first "before" value. One test proves it.
- **No `testcontainers` dependency.** Postgres comes from `docker run` locally (Docker 29.7 is installed) and a service container in CI. The tests read `CRUDCDC_TEST_PG_URL`.
- **`register_encoder` is a dict plus one function**, not a class.

## Tasks

| # | Task | Tests first (must fail before the code) | Done when |
|---|---|---|---|
| 0 | **Test harness.** `conftest.py`: an `engine` fixture parametrized over SQLite and Postgres; Postgres is skipped when the env var isn't set; each test creates a fresh schema. One-line `docker run` documented in the README | — | the old 6 tests pass on both backends |
| 1 | **Reproduce the bugs.** Tests: cascade delete records the child events; plain `session.add` is captured; enum column encodes | 3 failing tests | they fail for the reasons the probes showed |
| 2 | **Models + errors.** `Change` gains `xid` (0 on SQLite), `pk`/`changed` as JSON, `tx_id`, `changed_at`, indexes `(xid, seq)` and `(table, xid, seq)`. `ConsumerOffset` stores `xid` + `seq`. `errors.py` | schema test: columns and indexes exist on both backends | it passes |
| 3 | **Capture** (`capture.py`). `track()`, `before_flush` (before-images), `after_flush` (insert rows on the session connection: `xid = pg_current_xact_id()`, `changed_at = now()`), `after_begin` (SQLite `tx_id`), encoder + `register_encoder`, `EncodingError` | the task-1 tests, plus: relationship change, one event per flush (collapse), one `tx_id` across flushes, no-op update emits nothing, pk change, savepoint rollback, transaction rollback, untracked model ignored, encoding error rolls back the data, `track_before=False`, schema-qualified table, column name ≠ attribute name | all pass |
| 4 | **Bulk guard.** `do_orm_execute` raises `UntrackedWriteError` for ORM bulk `update`/`delete` on tracked tables; `crudcdc_untracked=True` allows it | raises, and the escape hatch works | they pass |
| 5 | **Feed** (`feed.py`). `ChangeEvent`, cursor token, `read` ordered by `(xid, seq)` with the `pg_snapshot_xmin` filter on Postgres, `read_for` with `FOR UPDATE SKIP LOCKED`, `ack` (never backwards), `forget`, `prune` | the **two Postgres skip scenarios** (first written against a `seq`-only reader to show they fail), second worker gets an empty batch, crashed worker's batch is redelivered, `forget` unblocks `prune`, cursor round-trip and `InvalidCursorError`, golden JSON per `op` | all pass |
| 6 | **`AsyncCRUD` reduced to plain ORM operations.** Remove its change-writing code; `list` ordered by primary key | an `AsyncCRUD` write and a plain ORM write produce identical events | it passes |
| 7 | **Docs + CI.** README rewritten around `track()`: the exactly-once example and the "not captured" list. Postgres service in `ci.yml`. Old scaffold tests deleted or folded in | — | `ruff`, `mypy --strict`, all tests on both backends, `uv build` |
| 8 | **Vault.** Update [[crudcdc]] status, [[Scaffold]] (new layout), and this plan's status | — | notes match the code |

## Risks to watch
- **`after_flush` inserts on the session's connection** while async: these must stay synchronous `connection.execute` calls inside the greenlet. If SQLAlchemy 2.1 rejects that, fall back to `after_flush_postexec` plus a second flush.
- **`pg_snapshot_xmin` and the reader's own transaction:** `FOR UPDATE` gives the reader a transaction ID. That doesn't matter, because changes older than it are already below the bound, but the task-5 tests cover it.
- **Before-images of expired objects:** reading them in `before_flush` triggers a load. That's fine in a greenlet, but verify it on Postgres.
