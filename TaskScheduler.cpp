#ifdef WITH_POSTGRESQL

#include "TaskScheduler/TaskScheduler.hpp"

#include "apostol/application.hpp"
#include "apostol/pg_utils.hpp"

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstdlib>

namespace apostol
{

namespace
{

// The mode and the lease of the channel `job`, read without an error where the
// database has no notify module: the function is looked up first, and PL/pgSQL
// plans the call only when the branch runs. A failed read would be logged by the
// pool once a second on every database that predates the module.
constexpr std::string_view kModeSql =
    "DO $ts$\n"
    "BEGIN\n"
    "  IF to_regprocedure('notify.channel_policy(text)') IS NOT NULL THEN\n"
    "    PERFORM set_config('task_scheduler.mode', coalesce(p.mode, ''), true),\n"
    "            set_config('task_scheduler.lease', coalesce((extract(epoch FROM p.lease) * 1000)::bigint::text, ''), true)\n"
    "       FROM notify.channel_policy('job') p;\n"
    "  END IF;\n"
    "END\n"
    "$ts$;\n"
    "SELECT current_setting('task_scheduler.mode', true), current_setting('task_scheduler.lease', true)";

std::string text_at(const PgResult& r, int row, int col)
{
    return r.is_null(row, col) ? std::string() : std::string(r.value(row, col));
}

// The connection or the server went while the batch was in its hands: written
// or not is unknown here (NotifySubscriber::Failure). Class 08 and 57P0x — the
// server's own word on it, also through a pooler; "" — no answer at all. Any
// other code is an error of the batch, and the batch is one transaction.
bool outcome_unknown(std::string_view sqlstate)
{
    return sqlstate.empty() || sqlstate.starts_with("08") || sqlstate.starts_with("57P0");
}

// notify.hold takes the row of the queue under its token, first in the batch:
// the locks go "row, then the job", the order of notify.sweep, which closes the
// row and then calls JobAbandon on the job. `done` first and the ack last took
// them the other way round and deadlocked with the sweep at the end of the
// lease of the last delivery (T1163). The sweep and notify.take skip a row held
// so: while the batch lives on the server its row is given to nobody, past its
// lease too. A stale token is NT409 here, before anything is written.
std::string hold_sql(const db_platform::NotifyRow& row)
{
    return fmt::format("SELECT notify.hold({}, {});\n", row.id, row.token);
}

} // namespace

// ─── on_start ────────────────────────────────────────────────────────────────

void TaskScheduler::on_start(EventLoop& loop, Application& app)
{
    pool_   = &app.db_pool();
    logger_ = &app.logger();
    loop_   = &loop;
    node_   = app.node_id();

    // Create BotSession for apibot authentication
    bot_ = std::make_unique<BotSession>(*pool_, "TaskScheduler/2.0", "127.0.0.1");

    // Read OAuth2 credentials from conf/oauth2/default.json → "service" app
    auto [client_id, client_secret] = app.providers().credentials("service");
    if (!client_id.empty())
        bot_->set_credentials(std::move(client_id), std::move(client_secret));

    // Read heartbeat interval from config
    if (auto* cfg = app.module_config("TaskScheduler")) {
        if (cfg->contains("heartbeat") && (*cfg)["heartbeat"].is_number())
            check_interval_ = milliseconds((*cfg)["heartbeat"].get<int>());
    }

    logger_->notice("TaskScheduler started (check_interval={}ms)",
                    check_interval_.count());
}

// ─── heartbeat ───────────────────────────────────────────────────────────────

void TaskScheduler::heartbeat(std::chrono::system_clock::time_point now)
{
    if (!bot_ || !pool_)
        return;

    bot_->refresh_if_needed();

    if (status_ == Status::stopped) {
        if (bot_->valid())
            status_ = Status::running;
        return;
    }

    // Status::running
    if (now >= next_check_) {
        check_jobs();
        next_check_ = now + check_interval_;
    }
}

// ─── on_stopping / busy / on_stop ────────────────────────────────────────────

void TaskScheduler::on_stopping()
{
    // Not chosen as an addressee any more, nothing new taken; a row taken goes
    // on to its end. The path `legacy` takes nothing between ticks.
    if (notify_)
        notify_->drain();
}

bool TaskScheduler::busy() const
{
    // Rows taken and not yet completed — a claim in flight. A body sent with its
    // ack is the pool's to count.
    return notify_ && notify_->busy();
}

void TaskScheduler::on_stop()
{
    // First: the rows still here go back (nack) and the subscriber is
    // deregistered, on the listener, while the pool is still there.
    if (notify_)
        notify_->stop();
    notify_.reset();

    if (bot_)
        bot_->sign_out();
    bot_.reset();
}

// ─── check_jobs ──────────────────────────────────────────────────────────────
//
// The mode of the channel `job`, read now — every tick, never kept from the last
// one (T1104): `single` — the database hands out the runs (offer_jobs); anything
// else — the poll (poll_jobs).
//

void TaskScheduler::check_jobs()
{
    if (!bot_->valid() || mode_in_flight_)
        return;

    mode_in_flight_ = true;

    // No error handler: a refusal comes to the result handler, as in the poll.
    pool_->execute(std::string(kModeSql),
        [this](std::vector<PgResult> results) {
            mode_in_flight_ = false;
            on_mode(std::move(results));
        },
        {}, /*quiet=*/true);
}

void TaskScheduler::on_mode(std::vector<PgResult> results)
{
    // results[0] = DO, results[1] = mode and lease
    if (!action_ok(results) || results[1].rows() != 1) {
        // Not an answer. Polling now could make this the second executor of a
        // channel that is `single`: wait, as after any other failure.
        on_fatal(fmt::format("the mode of the channel job is unknown: {}", result_error(results)));
        return;
    }

    const auto& r = results[1];

    // The lease bounds the body on both paths (T1104, З1): a body that runs by
    // the poll at the moment of the switch is left alone by api.job_due only while
    // it is younger than the lease. No channel — no lease, no bound, as before.
    lease_ = milliseconds(std::strtoll(text_at(r, 0, 1).c_str(), nullptr, 10));

    if (text_at(r, 0, 0) != "single") {
        // A subscriber left active next to the poll would still take rows, and
        // the poll would cancel what it runs as an orphan.
        stop_notify();
        poll_jobs();
        return;
    }

    offer_jobs();
}

// ─── poll_jobs ───────────────────────────────────────────────────────────────
//
// Path `legacy`. Mirrors v1 CTaskScheduler::CheckJob():
//   1. api.authorize(session)
//   2. api.job('enabled') ORDER BY created
//

void TaskScheduler::poll_jobs()
{
    // One pass per scope. api.job answers within the authorized session's scope,
    // so asking once under the first session would leave every other scope's jobs
    // permanently unenumerated — which is what happened when this was ported from
    // v1's api.get_sessions to the singular form.
    for (const auto& session : bot_->sessions()) {
        auto sql = fmt::format(
            "SELECT * FROM api.authorize({});\n"
            "SELECT * FROM api.job('enabled') ORDER BY created",
            pq_quote_literal(session));

        pool_->execute(sql,
            [this, session](std::vector<PgResult> results) {
                enum_jobs(session, std::move(results));
            },
            [this](std::string_view error) {
                on_fatal(std::string(error));
            },
            /*quiet=*/true);
    }
}

// ─── enum_jobs ───────────────────────────────────────────────────────────────
//
// Mirrors v1 CTaskScheduler::EnumJob():
//   For each job in results:
//     - If not in_progress and state in (enabled, aborted, failed) → do_start
//     - If in_progress and state == canceled → do_abort
//

void TaskScheduler::enum_jobs(const std::string& session, std::vector<PgResult> results)
{
    // results[0] = authorize, results[1] = job list
    if (!action_ok(results))
        return;

    auto& res = results[1];
    int rows = res.rows();

    // Column indices (api.job returns: id, typecode, statecode, created, daterun, body)
    int col_id        = res.column_index("id");
    int col_typecode  = res.column_index("typecode");
    int col_statecode = res.column_index("statecode");
    int col_body      = res.column_index("body");

    if (col_id < 0 || col_typecode < 0 || col_statecode < 0 || col_body < 0)
        return;

    for (int r = 0; r < rows; ++r) {
        std::string id        = res.value(r, col_id)        ? res.value(r, col_id)        : "";
        std::string type_code = res.value(r, col_typecode)  ? res.value(r, col_typecode)  : "";
        std::string state     = res.value(r, col_statecode) ? res.value(r, col_statecode) : "";
        std::string body      = res.value(r, col_body)      ? res.value(r, col_body)      : "";

        if (id.empty())
            continue;

        if (in_progress(id)) {
            // Already running — check if cancel was requested
            if (state == "canceled")
                do_abort(session, id);
        } else {
            // Not running — start if eligible
            if (state == "enabled" || state == "aborted" || state == "failed")
                do_start(session, id, type_code, body);
            else if (state == "executed")
                do_cancel(session, id);   // orphan from previous run → cancel
            else if (state == "canceled")
                do_abort(session, id);    // orphan canceled → abort
        }
    }
}

// ─── offer_jobs ──────────────────────────────────────────────────────────────
//
// Path `single`, in every scope:
//   1. api.authorize(session)
//   2. the session's scope (api.current_area — apibot has no USAGE on kernel,
//      where current_scope lives) — the row names its scope; the claim needs
//      the session
//   3. api.job_due()     — the due runs become rows of the channel `job`
//
// api.job_due is not asked before this process has a subscriber number: a row it
// writes is offered to the live subscribers, and until then this one is not.
//

void TaskScheduler::offer_jobs()
{
    const bool subscribed = notify_ && notify_->number() != 0;

    for (const auto& session : bot_->sessions()) {
        auto sql = fmt::format(
            "SELECT * FROM api.authorize({});\n"
            "SELECT scope FROM api.current_area()",
            pq_quote_literal(session));

        if (subscribed)
            sql += ";\nSELECT * FROM api.job_due()";

        pool_->execute(std::move(sql),
            [this, session](std::vector<PgResult> results) {
                on_offered(session, std::move(results));
            },
            [this](std::string_view error) {
                on_fatal(std::string(error));
            },
            /*quiet=*/true);
    }
}

void TaskScheduler::on_offered(const std::string& session, std::vector<PgResult> results)
{
    // results[0] = authorize, results[1] = scope, results[2] = the runs offered
    if (!action_ok(results))
        return;

    if (results[1].rows() != 1) {
        // Its rows would be refused for want of a session — and with no scope
        // known at all, no subscriber is made: say so rather than fall silent.
        logger_->warn("TaskScheduler: the scope of a session is not known ({} row(s) from api.current_area)",
                      results[1].rows());
        return;
    }

    const std::string scope = text_at(results[1], 0, 0);
    if (!scope.empty())
        scope_sessions_[scope] = session;

    if (!notify_)
        start_notify();

    if (results.size() >= 3 && results[2].ok() && results[2].rows() > 0)
        logger_->debug("TaskScheduler: {} job(s) offered in scope {}", results[2].rows(), scope);
}

void TaskScheduler::start_notify()
{
    db_platform::NotifySubscriber::Options options;
    options.channel     = "job";
    options.node        = node_;
    options.application = "TaskScheduler";

    notify_ = std::make_unique<db_platform::NotifySubscriber>(*pool_, *loop_, *logger_,
        std::move(options),
        [this](const NotifyRow& row) { on_row(row); });
    ++notify_generation_;

    notify_->start();
}

void TaskScheduler::stop_notify()
{
    if (!notify_)
        return;

    // The rows taken and not sent with a body go back; a body already sent ends
    // as it would — complete() calls its handlers without the subscriber.
    logger_->notice("TaskScheduler: the channel job is not in mode single; its subscriber is stopped");
    notify_->stop("stopping: the channel job is not in mode single any more");
    notify_.reset();
}

// ─── on_row ──────────────────────────────────────────────────────────────────
//
// A row taken: the job and its scope. api.job_claim makes it `executed` in a
// transaction of its own and hands back the body — read there, under the gates
// of the feed, never from the row.
//

void TaskScheduler::on_row(const NotifyRow& row)
{
    const auto payload = nlohmann::json::parse(row.payload, nullptr, /*allow_exceptions=*/false);

    std::string id, scope;
    if (payload.is_object()) {
        if (auto it = payload.find("id"); it != payload.end() && it->is_string())
            id = it->get<std::string>();
        if (auto it = payload.find("scope"); it != payload.end() && it->is_string())
            scope = it->get<std::string>();
    }

    if (id.empty() || !row.has_key) {
        // Will not get better on another instance: confirmed, nothing run.
        logger_->error("TaskScheduler: row {} of the channel job is not a job run: {}", row.id, row.payload);
        notify_->complete(row, {});
        return;
    }

    const auto it = scope_sessions_.find(scope);
    if (it == scope_sessions_.end()) {
        notify_->refuse(row, fmt::format("no session in scope {}", scope));
        return;
    }

    const std::string session = it->second;

    auto sql = fmt::format(
        "SELECT * FROM api.authorize({});\n"
        "SELECT * FROM api.job_claim({}::uuid, {})",
        pq_quote_literal(session), pq_quote_literal(id), pq_quote_literal(row.key));

    pool_->execute(std::move(sql),
        [this, row, generation = notify_generation_, session, id](std::vector<PgResult> results) {
            on_claimed(row, generation, session, id, std::move(results));
        },
        {}, /*quiet=*/true);
}

void TaskScheduler::on_claimed(const NotifyRow& row, unsigned generation, const std::string& session,
                               const std::string& id, std::vector<PgResult> results)
{
    // The subscriber that took the row is gone (stopped, its rows given back):
    // a claim that committed leaves the job `executed`, and the poll or the
    // next delivery settles it.
    if (!notify_ || generation != notify_generation_)
        return;

    // results[0] = authorize, results[1] = the claim
    if (!action_ok(results)) {
        // Rolled back, or lost on the way: given out again (a claim that did
        // commit is found `executed` by the next one and its body handed out).
        notify_->refuse(row, fmt::format("claim failed: {}", result_error(results)));
        return;
    }

    const auto& r = results[1];

    if (r.rows() == 0) {
        // Not to be run: gone, moved on, not due, canceled (aborted by the claim).
        notify_->complete(row, {});
        return;
    }

    if (const auto it = jobs_.find(id); it != jobs_.end()) {
        if (it->second.row_token == 0) {
            // Running here by the path `legacy` — a tick before the channel became
            // `single`. Its own `done` settles the job; a second body is the fault
            // the switch must not cause.
            logger_->warn("TaskScheduler: job {} is offered while its body runs here; not run twice", id);
            notify_->complete(row, {});
            return;
        }

        // An earlier delivery of this row, still here: its lease ran out before
        // its body got a connection. Its hold is refused under the old token
        // before its body runs — this one is the run.
        logger_->warn("TaskScheduler: job {} is handed out again (row {}, attempt {}) while its earlier run is "
                      "still here; that one is stopped by its hold", id, row.id, row.attempts);
    }

    const int col_typecode = r.column_index("typecode");
    const int col_body     = r.column_index("body");

    if (col_typecode < 0 || col_body < 0) {
        notify_->refuse(row, "claim answered without typecode or body");
        return;
    }

    run_body(row, session, id, text_at(r, 0, col_typecode), text_at(r, 0, col_body));
}

// ─── run_body ────────────────────────────────────────────────────────────────
//
// One transaction:
//   1. the limits below the lease
//   2. notify.hold — the row of the queue, before the job (hold_sql)
//   3. api.authorize(session)
//   4. <body SQL>
//   5. done (periodic) or complete (one-shot)
//   6. notify.ack — appended by NotifySubscriber
//
// The ack stays last, after `done` (T1104, В2): the ack first deadlocked with
// api.job_due of another instance two times out of two. The hold first does not
// (measured, T1163: 2 of 2 against api.job_due, and the sweep at the end of the
// lease passes the held row by). The limits are set_config and lock nothing;
// ahead of the hold they bound its wait too. A stale token (NT409) on the hold
// stops the batch before the body runs.
//
// The connection lost with the batch (T1161): the outcome is the database's —
// give_back(), not fail_body(), which would close the row of a body that may
// never have run. A loss past the transaction limit is transaction_timeout,
// the body's own: fail_body(), as an error of the body.
//

void TaskScheduler::run_body(const NotifyRow& row, const std::string& session,
                             const std::string& id, const std::string& type_code,
                             const std::string& body)
{
    jobs_[id] = Job{id, type_code, session, std::chrono::system_clock::now(), 0, row.token};

    logger_->debug("TaskScheduler: starting job {} (type={}, row {}, attempt {})",
                   id, type_code, row.id, row.attempts);

    // The body on lines of its own: a "-- comment" closing it would swallow what
    // follows, and a body without a final ';' would run into it.
    auto sql = fmt::format(
        "{}"
        "{}"
        "SELECT * FROM api.authorize({});\n"
        "{}\n"
        ";\n"
        "SELECT * FROM api.execute_object_action({}::uuid, {})",
        body_limits(),
        hold_sql(row),
        pq_quote_literal(session),
        body,
        pq_quote_literal(id),
        pq_quote_literal(type_code == "periodic.job" ? "done" : "complete"));

    // transaction_timeout ends the session, and the pool tells that as a lost
    // connection with no SQLSTATE, like a failover (measured on 18, T1161). The
    // server ends it no sooner than the limit after the batch began, and the
    // batch begins no sooner than it is sent from here: a loss sooner than that
    // is not the timeout. Later, it is taken for the body's own — `fail`, as
    // before T1161; so is a failover that late (the pool's queue counts in), and
    // any late loss on a server without transaction_timeout.
    const auto limit = transaction_limit();
    const auto sent  = std::chrono::steady_clock::now();

    // quiet: the statement carries a session code. PgPool prints statement
    // text at debug into postgres.log — inside the container, readable
    // by any process there.
    notify_->complete(row, std::move(sql),
        [this, id, token = row.token](std::vector<PgResult> /*results*/) {
            logger_->debug("TaskScheduler: job {} done", id);
            delete_run(id, token);
        },
        [this, row, session, id, limit, sent](const db_platform::NotifySubscriber::Failure& failure, bool stale) {
            if (stale) {
                // Refused by the hold, before the body: the row was given out
                // again (whoever holds it now runs the job) or closed by the
                // sweep. Or it was no longer with the subscriber, and nothing
                // was sent.
                logger_->warn("TaskScheduler: job {}: the row is no longer under this token "
                              "(given out again or closed); the body did not run", id);
                delete_run(id, row.token);
                return;
            }
            const bool timed_out = limit > milliseconds(0) && std::chrono::steady_clock::now() - sent >= limit;
            if (outcome_unknown(failure.sqlstate) && !timed_out) {
                give_back(row, id, failure.error);
                return;
            }
            fail_body(row, session, id, failure.error);
        },
        /*quiet=*/true);
}

// ─── fail_body ───────────────────────────────────────────────────────────────
//
// The body (or its `done`) failed and was rolled back: notify.hold, `fail` with
// the error as the label, then notify.ack — a transaction of its own, the hold
// first and the ack last for the same reasons as above. A stale token stops the
// `fail` at the hold: the run is then somebody else's. Refused (the job was
// canceled meanwhile, or the connection went): the row goes back, and the next
// claim aborts the job or hands its body out again.
//

void TaskScheduler::fail_body(const NotifyRow& row, const std::string& session,
                              const std::string& id, const std::string& error)
{
    logger_->error("TaskScheduler: job {} failed: {}", id, error);

    auto sql = db_platform::NotifySubscriber::ack_sql(row, fmt::format(
        "{}"
        "SELECT * FROM api.authorize({});\n"
        "SELECT * FROM api.execute_object_action({}::uuid, {});\n"
        "SELECT * FROM api.set_object_label({}::uuid, {})",
        hold_sql(row),
        pq_quote_literal(session),
        pq_quote_literal(id), pq_quote_literal("fail"),
        pq_quote_literal(id), pq_quote_literal(error)));

    pool_->execute(std::move(sql),
        [this, row, id](std::vector<PgResult> results) {
            delete_run(id, row.token);

            const auto bad = std::find_if(results.begin(), results.end(),
                [](const PgResult& r) { return !r.ok(); });

            if (bad == results.end())
                return;

            if (std::string_view(bad->sqlstate()) == "NT409") {
                logger_->warn("TaskScheduler: job {}: the row went to another executor "
                              "before the failure was recorded", id);
                return;
            }

            logger_->error("TaskScheduler: cannot record failure of job {}: {}", id,
                           bad->error_message());

            // The row is no longer with the subscriber (complete() took it):
            // given back here, or it would wait out its lease.
            pool_->execute(fmt::format("SELECT notify.nack({}, {}, {})", row.id, row.token,
                                       pq_quote_literal("fail refused: " + std::string(bad->error_message()))),
                [this, id](std::vector<PgResult> nack) {
                    if (nack.empty() || !nack.back().ok())
                        logger_->warn("TaskScheduler: job {}: nack refused: {}", id, result_error(nack));
                },
                {}, /*quiet=*/false);
        },
        {}, /*quiet=*/true);
}

// ─── give_back ───────────────────────────────────────────────────────────────
//
// The connection went with the batch of the body (failover, a cut network,
// pg_terminate_backend): committed or rolled back is unknown here, and known
// to the database (T1161). notify.nack under the token asks it. The hold at the
// head of the batch keeps the row locked while its transaction lives on the
// server, so the nack waits for it to end: rolled back — the row goes out again
// by the channel's repeat, and the next claim finds the job `executed` and
// hands its body out; committed — the ack has closed the row, and the nack is
// refused as NT409. `fail` here closed the row of a body that may never have
// run: a periodic job lost its period, a one-shot one ended `disabled`. The
// nack lost as well: the row returns by its lease.
//

void TaskScheduler::give_back(const NotifyRow& row, const std::string& id, const std::string& error)
{
    logger_->warn("TaskScheduler: job {}: the connection went with its body, the row is given back: {}", id, error);

    delete_run(id, row.token);

    pool_->execute(fmt::format("SELECT notify.nack({}, {}, {})", row.id, row.token,
                               pq_quote_literal("outcome unknown: " + error)),
        [this, id](std::vector<PgResult> results) {
            if (!results.empty() && results.back().ok())
                return;
            if (!results.empty() && std::string_view(results.back().sqlstate()) == "NT409") {
                logger_->info("TaskScheduler: job {}: the row is closed or with another executor — "
                              "the body was committed, or its lease ran out", id);
                return;
            }
            logger_->warn("TaskScheduler: job {}: nack refused, the row returns by its lease: {}", id,
                          result_error(results));
        },
        {}, /*quiet=*/false);
}

std::string TaskScheduler::body_limits() const
{
    if (lease_ <= milliseconds(0))
        return {};

    // Each statement below 8/10 of the lease — the error is the body's own
    // ("canceling statement due to statement timeout"), the job fails. The whole
    // transaction below 9/10 where the server has transaction_timeout (17 and
    // later): a body of several statements is bounded too, at the cost of the
    // connection — the server ends the session. The lease counts from the take,
    // and the claim and the pool's queue come before the body: a batch that
    // reaches the server after the lease may find its row handed out again — its
    // hold is refused under the old token, and the body does not run. Once held,
    // the row is given to nobody until the batch ends. Never '0': that is
    // "no limit".
    return fmt::format(
        "SELECT set_config('statement_timeout', '{}', true);\n"
        "SELECT set_config('transaction_timeout', '{}', true) FROM pg_settings WHERE name = 'transaction_timeout';\n",
        std::max<std::int64_t>(1, lease_.count() * 8 / 10), transaction_limit().count());
}

TaskScheduler::milliseconds TaskScheduler::transaction_limit() const
{
    if (lease_ <= milliseconds(0))
        return milliseconds(0);
    return milliseconds(std::max<std::int64_t>(1, lease_.count() * 9 / 10));
}

// ─── do_start ────────────────────────────────────────────────────────────────
//
// Transition: enabled/aborted/failed → executed
// Then run the job body.
//

void TaskScheduler::do_start(const std::string& session, const std::string& id,
                             const std::string& type_code, const std::string& body)
{
    jobs_[id] = Job{id, type_code, session, std::chrono::system_clock::now()};

    logger_->debug("TaskScheduler: starting job {} (type={})", id, type_code);

    // Explicit scope, though the entry was inserted one line above: reading it back
    // out of jobs_ is the very pattern this file was fixed to stop using, and with a
    // fallback in place a future breakage would be silent instead of loud.
    execute_action(session, id, "execute",
        [this, session, id, type_code, body](std::vector<PgResult> /*results*/) {
            do_run(session, id, type_code, body);
        });
}

// ─── do_run ──────────────────────────────────────────────────────────────────
//
// Execute the job body SQL:
//   1. api.authorize(session)
//   2. <body SQL>
//

void TaskScheduler::do_run(const std::string& session, const std::string& id,
                           const std::string& type_code, const std::string& body)
{
    // The job may have been aborted while the 'execute' action was in flight: a pass
    // between do_start and this callback can enumerate it as 'canceled', run do_abort
    // and erase the entry. Running the body afterwards would execute work that was
    // deliberately stopped. Until the scope became explicit this was prevented only by
    // accident — the lookup returned an empty session and api.authorize refused.
    if (!in_progress(id))
        return;

    if (!bot_->valid()) {
        // The job is already 'executed' in the database and is about to leave our
        // tracking: that is exactly how an orphan is born. Say so — until this line
        // existed, an orphan appeared in the database without a word in the log.
        logger_->warn("TaskScheduler: dropping job {} before run — no session; "
                      "it stays 'executed' in the database", id);
        delete_job(id);
        return;
    }

    auto sql = fmt::format(
        "{}"
        "SELECT * FROM api.authorize({});\n"
        "{}",
        body_limits(),
        pq_quote_literal(session),
        body);

    // quiet: the statement carries a session code. PgPool prints statement
    // text at debug into postgres.log — inside the container, readable
    // by any process there.
    auto qid = pool_->execute(sql,
        [this, id, type_code](std::vector<PgResult> /*results*/) {
            if (!in_progress(id))
                return;

            // Periodic jobs cycle back to enabled; one-shot jobs complete
            if (type_code == "periodic.job")
                do_done(id);
            else
                do_complete(id);
        },
        [this, id](std::string_view error) {
            if (in_progress(id))
                do_fail(id, std::string(error));
            else
                delete_job(id);
        },
        /*quiet=*/true);

    // Store query handle for cancel support
    auto it = jobs_.find(id);
    if (it != jobs_.end())
        it->second.query_id = qid;
}

// ─── do_done ─────────────────────────────────────────────────────────────────
//
// Periodic job: executed → enabled (dateRun recalculated by EventJobDone in DB)
//

void TaskScheduler::do_done(const std::string& id)
{
    logger_->debug("TaskScheduler: job {} done (periodic)", id);

    // Sent again after a lost connection (T627): `done` and `complete` exist
    // only in state `executed`, so a repeat of one that committed is refused
    // by the workflow and lands where `never` would have — the error handler.
    execute_action(id, "done",
        [this, id](std::vector<PgResult> /*results*/) {
            delete_job(id);
        },
        PgRetry::if_lost);
}

// ─── do_complete ─────────────────────────────────────────────────────────────
//
// Disposable job: executed → completed
//

void TaskScheduler::do_complete(const std::string& id)
{
    logger_->debug("TaskScheduler: job {} complete (disposable)", id);

    // Sent again after a lost connection — see do_done.
    execute_action(id, "complete",
        [this, id](std::vector<PgResult> /*results*/) {
            delete_job(id);
        },
        PgRetry::if_lost);
}

// ─── do_fail ─────────────────────────────────────────────────────────────────
//
// executed → failed + store error as object label
//

void TaskScheduler::do_fail(const std::string& id, const std::string& error)
{
    logger_->error("TaskScheduler: job {} failed: {}", id, error);

    if (!bot_->valid()) {
        // Same as in do_run: dropped here, the job keeps its 'executed' state and
        // becomes an orphan for the next pass. Do not let that happen silently.
        logger_->warn("TaskScheduler: cannot record failure of job {} — no session; "
                      "it stays 'executed' in the database", id);
        delete_job(id);
        return;
    }

    auto sql = fmt::format(
        "SELECT * FROM api.authorize({});\n"
        "SELECT * FROM api.execute_object_action({}::uuid, {});\n"
        "SELECT * FROM api.set_object_label({}::uuid, {})",
        pq_quote_literal(job_session(id)),
        pq_quote_literal(id), pq_quote_literal("fail"),
        pq_quote_literal(id), pq_quote_literal(error));

    // quiet: the statement carries a session code. PgPool prints statement
    // text at debug into postgres.log — inside the container, readable
    // by any process there.
    pool_->execute(sql,
        [this, id](std::vector<PgResult> /*results*/) {
            delete_job(id);
        },
        [this, id](std::string_view err) {
            logger_->error("TaskScheduler: do_fail SQL error for {}: {}", id, err);
            delete_job(id);
        },
        /*quiet=*/true);
}

// ─── do_cancel ───────────────────────────────────────────────────────────────
//
// Orphan cleanup: executed → canceled (job was left from a previous run)
//

void TaskScheduler::do_cancel(const std::string& session, const std::string& id)
{
    logger_->notice("TaskScheduler: canceling orphan job {}", id);

    // The scope travels in from enum_jobs. It cannot be read back from jobs_ here:
    // this branch is reached precisely when the job is NOT tracked, so the map has
    // no entry and never will. Asking it returned an empty session, and BotSession
    // refuses an empty session before reaching the database — the action was never
    // issued, the job stayed 'executed', and the next pass repeated it forever.
    execute_action(session, id, "cancel",
        [this, session, id](std::vector<PgResult> results) {
            // The result is examined as well as the exception handler: until libapostol
            // b7cc42d (09.09) a SQL refusal arrived HERE, as a non-ok result; now it goes
            // to the handler, as do BotSession's own refusals. Proceeding to do_abort on a cancel
            // that did not take would ask the workflow for a transition it has no method
            // for, fail, and be repeated by the next pass a second later.
            //
            // on_fatal is kept deliberately, and the comparison that decides it is
            // with PRODUCTION, not with the previous revision of this file. Today the
            // live stacks run this very pause every ten seconds, for every pass, on all
            // four sites: that is what T192 measures. Keeping it means the worst case
            // after this change is exactly the worst case before it, while the cause
            // that fires it there — an empty session — is gone. Dropping it would
            // confine the damage to one job but print once a second instead of once per
            // ten, five times denser than the flood this card exists to remove.
            //
            // Neither choice is right, and that is the point: without a per-job backoff
            // there is no option that is both quiet and confined. That backoff is card
            // T219, and it is the other half of this fix, not an improvement on it.
            if (!action_ok(results)) {
                logger_->error("TaskScheduler: cancel refused for {}: {}", id,
                               result_error(results));
                on_fatal("cancel refused by the database");
                return;
            }

            do_abort(session, id);
        });
}

// ─── do_abort ────────────────────────────────────────────────────────────────
//
// canceled → aborted (in-flight SQL will finish but result is ignored)
//

void TaskScheduler::do_abort(const std::string& session, const std::string& id)
{
    logger_->notice("TaskScheduler: aborting job {}", id);

    // Cancel running SQL body if any (PQcancel → PostgreSQL)
    auto it = jobs_.find(id);
    if (it != jobs_.end() && it->second.query_id != 0)
        pool_->cancel(it->second.query_id);

    // Remove from tracking immediately — canceled query results will be discarded
    delete_job(id);

    // The scope comes from enum_jobs, not from jobs_: the entry has just been erased
    // one line above, so reading it back here yielded an empty session on every
    // abort — tracked or orphaned alike. This branch has never worked since 2026-08-23.
    // Fire-and-forget: do not trigger on_fatal if abort action fails
    bot_->execute_action(session, id, "abort",
        [this, id](std::vector<PgResult> results) {
            // Same contract as above: a refusal arrives as a non-ok result, not through
            // the handler. Fire-and-forget stays fire-and-forget — this is said, not
            // acted upon — but it is said with the database's own words rather than
            // left invisible, which is how the previous refusal spent a day unread.
            if (!action_ok(results))
                logger_->warn("TaskScheduler: abort refused for {}: {}", id,
                              result_error(results));
        },
        [this, id](std::string_view error) {
            logger_->warn("TaskScheduler: abort action failed for {}: {}", id, error);
        });
}

// ─── execute_action ──────────────────────────────────────────────────────────

void TaskScheduler::execute_action(const std::string& id, std::string_view action,
                                   PgQuery::ResultHandler on_result, PgRetry retry)
{
    execute_action(job_session(id), id, action, std::move(on_result), retry);
}

void TaskScheduler::execute_action(const std::string& session, const std::string& id,
                                   std::string_view action,
                                   PgQuery::ResultHandler on_result, PgRetry retry)
{
    bot_->execute_action(session, id, action, std::move(on_result),
        [this, id, act = std::string(action)](std::string_view error) {
            logger_->error("TaskScheduler: action '{}' failed for {}: {}", act, id, error);
            delete_job(id);
            on_fatal(std::string(error));
        },
        retry);
}

// ─── delete_job / in_progress ────────────────────────────────────────────────

bool TaskScheduler::action_ok(const std::vector<PgResult>& results)
{
    return results.size() >= 2 && results[1].ok();
}

std::string TaskScheduler::result_error(const std::vector<PgResult>& results)
{
    // PostgreSQL's simple query protocol abandons the rest of the batch at the first
    // error, so a failure in api.authorize leaves ONE result — and the reason in it.
    for (const auto& r : results) {
        if (!r.ok())
            return r.error_message();
    }

    return results.empty() ? std::string("no result at all")
                           : std::string("no failing result");
}

void TaskScheduler::delete_job(const std::string& id)
{
    jobs_.erase(id);
}

void TaskScheduler::delete_run(const std::string& id, std::int64_t row_token)
{
    if (const auto it = jobs_.find(id); it != jobs_.end() && it->second.row_token == row_token)
        jobs_.erase(it);
}

std::string TaskScheduler::job_session(const std::string& id) const
{
    // Fall back to the first session rather than to an empty one, as MessageServer
    // and ReportServer do. An empty session is refused by BotSession before the
    // statement is built, so the caller gets "not authenticated" for an object that
    // has nothing to do with authentication. A first-scope guess can still fail, but
    // it fails in the database, where the error names the real problem.
    auto it = jobs_.find(id);
    return it == jobs_.end() ? bot_->session() : it->second.session;
}

bool TaskScheduler::in_progress(const std::string& id) const
{
    return jobs_.count(id) > 0;
}

// ─── on_fatal ────────────────────────────────────────────────────────────────
//
// Catastrophic error — pause for 10 seconds before retrying.
//

void TaskScheduler::on_fatal(const std::string& error)
{
    status_ = Status::stopped;
    next_check_ = std::chrono::system_clock::now() + std::chrono::seconds(10);
    logger_->error("TaskScheduler: fatal error, pausing 10s: {}", error);
}

} // namespace apostol

#endif // WITH_POSTGRESQL
