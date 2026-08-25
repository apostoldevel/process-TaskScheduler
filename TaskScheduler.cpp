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
    if (results.size() < 2 || !results[1].ok())
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
                do_abort(id);
        } else {
            // Not running — start if eligible
            if (state == "enabled" || state == "aborted" || state == "failed")
                do_start(session, id, type_code, body);
            else if (state == "executed")
                do_cancel(id);   // orphan from previous run → cancel
            else if (state == "canceled")
                do_abort(id);    // orphan canceled → abort
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

    execute_action(id, "execute",
        [this, id, type_code, body](std::vector<PgResult> /*results*/) {
            do_run(id, type_code, body);
        });
}

// ─── do_run ──────────────────────────────────────────────────────────────────
//
// Execute the job body SQL:
//   1. api.authorize(session)
//   2. <body SQL>
//

void TaskScheduler::do_run(const std::string& id, const std::string& type_code,
                           const std::string& body)
{
    if (!bot_->valid()) {
        delete_job(id);
        return;
    }

    auto sql = fmt::format(
        "SELECT * FROM api.authorize({});\n"
        "{}",
        pq_quote_literal(job_session(id)),
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
        delete_job(id);
        return;
    }

    auto sql = fmt::format(
        "SELECT * FROM api.authorize({});\n"
        "SELECT * FROM api.execute_object_action({}::uuid, {});\n"
        "SELECT * FROM api.set_object_label({}::uuid, {})",
        pq_quote_literal(bot_->session()),
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

void TaskScheduler::do_cancel(const std::string& id)
{
    logger_->notice("TaskScheduler: canceling orphan job {}", id);

    execute_action(id, "cancel",
        [this, id](std::vector<PgResult> /*results*/) {
            do_abort(id);
        });
}

// ─── do_abort ────────────────────────────────────────────────────────────────
//
// canceled → aborted (in-flight SQL will finish but result is ignored)
//

void TaskScheduler::do_abort(const std::string& id)
{
    logger_->notice("TaskScheduler: aborting job {}", id);

    // Cancel running SQL body if any (PQcancel → PostgreSQL)
    auto it = jobs_.find(id);
    if (it != jobs_.end() && it->second.query_id != 0)
        pool_->cancel(it->second.query_id);

    // Remove from tracking immediately — canceled query results will be discarded
    delete_job(id);

    // Fire-and-forget: do not trigger on_fatal if abort action fails
    bot_->execute_action(job_session(id), id, "abort",
        [](std::vector<PgResult> /*results*/) {},
        [this, id](std::string_view error) {
            logger_->warn("TaskScheduler: abort action failed for {}: {}", id, error);
        });
}

// ─── execute_action ──────────────────────────────────────────────────────────

void TaskScheduler::execute_action(const std::string& id, std::string_view action,
                                   PgQuery::ResultHandler on_result)
{
    bot_->execute_action(job_session(id), id, action, std::move(on_result),
        [this, id, act = std::string(action)](std::string_view error) {
            logger_->error("TaskScheduler: action '{}' failed for {}: {}", act, id, error);
            delete_job(id);
            on_fatal(std::string(error));
        });
}

// ─── delete_job / in_progress ────────────────────────────────────────────────

void TaskScheduler::delete_job(const std::string& id)
{
    jobs_.erase(id);
}

std::string TaskScheduler::job_session(const std::string& id) const
{
    auto it = jobs_.find(id);
    return it == jobs_.end() ? std::string() : it->second.session;
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
