#ifdef WITH_POSTGRESQL

#include "TaskScheduler/TaskScheduler.hpp"

#include "apostol/application.hpp"
#include "apostol/pg_utils.hpp"

#include <fmt/format.h>

namespace apostol
{

// ─── on_start ────────────────────────────────────────────────────────────────

void TaskScheduler::on_start(EventLoop& /*loop*/, Application& app)
{
    pool_   = &app.db_pool();
    logger_ = &app.logger();

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

// ─── on_stop ─────────────────────────────────────────────────────────────────

void TaskScheduler::on_stop()
{
    if (bot_)
        bot_->sign_out();
    bot_.reset();
}

// ─── check_jobs ──────────────────────────────────────────────────────────────
//
// Mirrors v1 CTaskScheduler::CheckJob():
//   1. api.authorize(session)
//   2. api.job('enabled') ORDER BY created
//

void TaskScheduler::check_jobs()
{
    if (!bot_->valid())
        return;

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
        "SELECT * FROM api.authorize({});\n"
        "{}",
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

    execute_action(id, "done",
        [this, id](std::vector<PgResult> /*results*/) {
            delete_job(id);
        });
}

// ─── do_complete ─────────────────────────────────────────────────────────────
//
// Disposable job: executed → completed
//

void TaskScheduler::do_complete(const std::string& id)
{
    logger_->debug("TaskScheduler: job {} complete (disposable)", id);

    execute_action(id, "complete",
        [this, id](std::vector<PgResult> /*results*/) {
            delete_job(id);
        });
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
            // The result is examined, not the exception handler: a SQL refusal arrives
            // HERE, as a non-ok result, and the handler below is reachable only through
            // BotSession's own synchronous refusals. Proceeding to do_abort on a cancel
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
                                   PgQuery::ResultHandler on_result)
{
    execute_action(job_session(id), id, action, std::move(on_result));
}

void TaskScheduler::execute_action(const std::string& session, const std::string& id,
                                   std::string_view action,
                                   PgQuery::ResultHandler on_result)
{
    bot_->execute_action(session, id, action, std::move(on_result),
        [this, id, act = std::string(action)](std::string_view error) {
            logger_->error("TaskScheduler: action '{}' failed for {}: {}", act, id, error);
            delete_job(id);
            on_fatal(std::string(error));
        });
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
