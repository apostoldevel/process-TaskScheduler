[![ru](https://img.shields.io/badge/lang-ru-green.svg)](README.ru-RU.md)

Task Scheduler
-
**TaskScheduler** is a process for [Apostol](https://github.com/apostoldevel/apostol).

Description
-
**TaskScheduler** is a long-running background process that executes scheduled database jobs. It polls the job queue and runs each job's SQL body as a PostgreSQL query, tracking state transitions through the job lifecycle.

The process runs independently inside the Apostol master process, sharing the same `epoll`-based event loop — no threads, no blocking I/O.

How it works
-
1. Authenticates via OAuth2 `client_credentials` as `apibot`; re-authenticates every 24 hours.
2. Polls `api.job('enabled')` every second.
3. For each enabled job, transitions it to `executed` state via `api.execute_object_action(id, 'execute')`.
4. Runs the job's `body` field directly as a SQL statement.
5. On completion — transitions to `done` (periodic) or `complete` (one-shot).
6. In-progress jobs with `canceled` state are cancelled by sending a PostgreSQL query cancellation.

Job lifecycle
-

| State | Action |
|-------|--------|
| `enabled` / `aborted` / `failed` | Start the job → transition to `executed` |
| `executed` | Run `body` SQL |
| _(done, `periodic.job`)_ | `execute_object_action('done')` — job repeats on next poll |
| _(done, other types)_ | `execute_object_action('complete')` — one-shot, job finished |
| `canceled` _(while running)_ | Cancel PQ query → `execute_object_action('abort')` |
| _(SQL error)_ | `execute_object_action('fail')` — records error label |

Job types
-
| Type code | Behaviour |
|-----------|-----------|
| `periodic.job` | Repeating: after each successful run transitions to `done` and re-runs on the next poll cycle |
| _(any other)_ | One-shot: transitions to `complete` after first successful run |

Jobs in `aborted` or `failed` state are automatically retried on the next poll.

Database module
-
TaskScheduler is tightly coupled to the **`job`** module of [db-platform](https://github.com/apostoldevel/db-platform) (`db/sql/platform/entity/object/document/job/`).

Key database objects:

| Object | Purpose |
|--------|---------|
| `db.job` | Job record: references a `scheduler` (period) and a `program` (SQL body to execute) |
| `db.scheduler` | Defines the repeat period; `dateRun` on insert = `now() + scheduler.period` |
| `db.program` | Stores the SQL body executed when the job fires |
| `api.job(state)` | Returns jobs where `dateRun <= now()` and state matches (e.g. `'enabled'`) |
| `api.execute_object_action(id, action)` | State transitions: `'execute'`, `'done'`, `'complete'`, `'abort'`, `'cancel'`, `'fail'` |

The `body` column returned by `api.job` comes from the associated `program` entity and is executed directly as a SQL statement by the scheduler process.

Configuration
-
```ini
[process/TaskScheduler]
enable=true
```

No external config files are required.

Installation
-
Follow the build and installation instructions for [Apostol](https://github.com/apostoldevel/apostol#build-and-installation).
