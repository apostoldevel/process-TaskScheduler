[![ru](https://img.shields.io/badge/lang-ru-green.svg)](README.ru-RU.md)

Task Scheduler
-

**Process** for [Apostol](https://github.com/apostoldevel/apostol) + [db-platform](https://github.com/apostoldevel/db-platform) — **Apostol CRM**[^crm].

Description
-

**Task Scheduler** is a background process module for the [Apostol](https://github.com/apostoldevel/apostol) framework. It runs as an independent forked process and polls the `db.job` queue, executing the SQL body of each scheduled task.

Key characteristics:

* Written in C++20 using an asynchronous, non-blocking I/O model based on the **epoll** API.
* Connects to **PostgreSQL** via the `libpq` library using the `apibot` database role (helper connection pool).
* Authenticates via OAuth2 `client_credentials` grant using `BotSession` — re-authenticates every 24 hours.
* Supports two job types: **periodic** (cycles back to `enabled` after execution) and **disposable** (transitions to `completed`).
* Handles error recovery: failed jobs are marked with the error message; the scheduler pauses for 10 seconds on fatal errors.

### Architecture

Task Scheduler follows the **ProcessModule** pattern introduced in apostol.v2:

```
Application
  └── ModuleProcess (generic process shell: signals, EventLoop, PgPool)
        └── TaskScheduler (ProcessModule: business logic only)
```

The process lifecycle (signal handling, crash recovery, PgPool setup, heartbeat timer) is managed by the generic `ModuleProcess` shell. `TaskScheduler` only contains the job scheduling logic.

### How it works

```
heartbeat (1s)
  └── BotSession::refresh_if_needed()
  └── if authenticated → check_jobs()
        └── api.authorize(session)
        └── api.job('enabled') ORDER BY created
        └── enum_jobs():
              for each job:
                enabled/aborted/failed → do_start(id)
                  └── api.execute_object_action(id, 'execute')
                  └── do_run(id, body_sql)
                        └── api.authorize(session) + <body SQL>
                        └── periodic.job  → do_done()  → action 'done'
                        └── disposable    → do_complete() → action 'complete'
                        └── on error      → do_fail()  → action 'fail' + set_object_label
                canceled → do_abort(id)
                  └── api.execute_object_action(id, 'abort')
```

### Job state machine (db-platform)

```
created ──enable──► enabled ──execute──► executed ──done────► enabled   (periodic)
                                                   ──complete► completed (disposable)
                                                   ──fail────► failed
                                                   ──cancel──► canceled ──abort──► aborted
```

Configuration
-

In the application config (`conf/apostol.json`):

```json
{
  "module": {
    "TaskScheduler": {
      "enable": true,
      "heartbeat": 1000
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable` | bool | `false` | Enable/disable the process |
| `heartbeat` | int | `1000` | Job check interval in milliseconds |

The process also requires:
* `postgres.helper` connection string in the config (used for the `apibot` connection pool)
* OAuth2 `service` credentials in `conf/oauth2/default.json`

Installation
-

Follow the build and installation instructions for [Apostol](https://github.com/apostoldevel/apostol#building-and-installation).

[^crm]: **Apostol CRM** is an abstract term, not a standalone product. It refers to any project that uses both the [Apostol](https://github.com/apostoldevel/apostol) C++ framework and [db-platform](https://github.com/apostoldevel/db-platform) together through purpose-built modules and processes. Each framework can be used independently; combined, they form a full-stack backend platform.
