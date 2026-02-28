[![en](https://img.shields.io/badge/lang-en-green.svg)](README.md)

Планировщик задач
-

**Процесс** для [Apostol](https://github.com/apostoldevel/apostol) + [db-platform](https://github.com/apostoldevel/db-platform) — **Apostol CRM**[^crm].

Описание
-

**Планировщик задач** — фоновый процесс-модуль для фреймворка [Апостол](https://github.com/apostoldevel/apostol). Запускается как отдельный форкнутый процесс и опрашивает очередь задач `db.job`, выполняя SQL-тело каждой запланированной задачи.

Основные характеристики:

* Написан на C++20 с использованием асинхронной неблокирующей модели ввода-вывода на базе **epoll** API.
* Подключается к **PostgreSQL** через библиотеку `libpq`, используя роль `apibot` (пул соединений helper).
* Аутентифицируется через OAuth2 `client_credentials` с помощью `BotSession` — повторная аутентификация каждые 24 часа.
* Поддерживает два типа задач: **периодические** (после выполнения возвращаются в состояние `enabled`) и **одноразовые** (переходят в состояние `completed`).
* Обрабатывает ошибки: для неуспешных задач сохраняется сообщение об ошибке; при критических ошибках планировщик приостанавливается на 10 секунд.

### Архитектура

Планировщик задач следует паттерну **ProcessModule**, введённому в apostol.v2:

```
Application
  └── ModuleProcess (generic-оболочка процесса: сигналы, EventLoop, PgPool)
        └── TaskScheduler (ProcessModule: только бизнес-логика)
```

Жизненный цикл процесса (обработка сигналов, crash recovery, настройка PgPool, таймер heartbeat) управляется generic-оболочкой `ModuleProcess`. `TaskScheduler` содержит только логику планирования задач.

### Как это работает

```
heartbeat (1 сек)
  └── BotSession::refresh_if_needed()
  └── если аутентифицирован → check_jobs()
        └── api.authorize(session)
        └── api.job('enabled') ORDER BY created
        └── enum_jobs():
              для каждой задачи:
                enabled/aborted/failed → do_start(id)
                  └── api.execute_object_action(id, 'execute')
                  └── do_run(id, body_sql)
                        └── api.authorize(session) + <SQL-тело>
                        └── periodic.job  → do_done()  → action 'done'
                        └── одноразовая   → do_complete() → action 'complete'
                        └── при ошибке    → do_fail()  → action 'fail' + set_object_label
                canceled → do_abort(id)
                  └── api.execute_object_action(id, 'abort')
```

### Машина состояний задачи (db-platform)

```
created ──enable──► enabled ──execute──► executed ──done────► enabled   (периодическая)
                                                   ──complete► completed (одноразовая)
                                                   ──fail────► failed
                                                   ──cancel──► canceled ──abort──► aborted
```

Конфигурация
-

В конфигурационном файле приложения (`conf/apostol.json`):

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

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|-------------|----------|
| `enable` | bool | `false` | Включить/отключить процесс |
| `heartbeat` | int | `1000` | Интервал проверки задач в миллисекундах |

Также необходимы:
* Строка подключения `postgres.helper` в конфигурации (используется для пула соединений `apibot`)
* Учётные данные OAuth2 `service` в файле `conf/oauth2/default.json`

Установка
-

Следуйте указаниям по сборке и установке [Апостол](https://github.com/apostoldevel/apostol#building-and-installation).

[^crm]: **Apostol CRM** — абстрактный термин, а не самостоятельный продукт. Он обозначает любой проект, в котором совместно используются фреймворк [Apostol](https://github.com/apostoldevel/apostol) (C++) и [db-platform](https://github.com/apostoldevel/db-platform) через специально разработанные модули и процессы. Каждый фреймворк можно использовать независимо; вместе они образуют полноценную backend-платформу.
