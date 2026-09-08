#pragma once

#ifdef WITH_POSTGRESQL

#include "apostol/process_module.hpp"

#include "apostol/bot_session.hpp"
#include "apostol/pg.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

namespace apostol
{

class Application;
class EventLoop;
class Logger;

// ─── TaskScheduler ───────────────────────────────────────────────────────────
//
// Background process module that polls the db.job queue and executes SQL bodies.
//
// Mirrors v1 CTaskScheduler from apostol-crm.
//
// Architecture: logic lives here (ProcessModule), injected into a generic
// ModuleProcess shell via add_custom_process(unique_ptr<ProcessModule>).
// This pattern separates business logic from process lifecycle boilerplate
// and is reused for all background processes (MessageServer, ReportServer, etc.).
//
// Job lifecycle:
//   - Authenticates as "apibot" via OAuth2 client_credentials (BotSession)
//   - Polls api.job('enabled') every heartbeat interval
//   - Executes each job's SQL body via deferred PG queries
//   - Periodic jobs cycle: enabled → executed → enabled (with dateRun bump)
//   - Disposable jobs: enabled → executed → completed
//   - Failed jobs: executed → failed (error stored as object label)
//   - Canceled jobs: executed → canceled → aborted
//
// Configuration (in apostol.json):
//   "module": {
//     "TaskScheduler": {
//       "enable": true,
//       "heartbeat": 1000
//     }
//   }
//
class TaskScheduler final : public ProcessModule
{
public:
    std::string_view name() const override { return "task-scheduler"; }

    void on_start(EventLoop& loop, Application& app) override;
    void heartbeat(std::chrono::system_clock::time_point now) override;
    void on_stop() override;

private:
    using time_point   = std::chrono::system_clock::time_point;
    using milliseconds = std::chrono::milliseconds;

    // ── State ────────────────────────────────────────────────────────────────

    PgPool*     pool_{nullptr};     // borrowed from Application
    Logger*     logger_{nullptr};   // borrowed from Application

    std::unique_ptr<BotSession> bot_;

    enum class Status { stopped, running };
    Status status_{Status::stopped};

    struct Job
    {
        std::string id;
        std::string type_code;
        std::string session;      // the scope this job was found in; act in it
        time_point  started_at;
        uint64_t    query_id{0};  // PgPool query handle for cancel
    };

    std::unordered_map<std::string, Job> jobs_;

    time_point   next_check_{};
    milliseconds check_interval_{1000};

    // ── Job lifecycle ────────────────────────────────────────────────────────

    void check_jobs();
    void enum_jobs(const std::string& session, std::vector<PgResult> results);

    void do_start(const std::string& session, const std::string& id,
                  const std::string& type_code, const std::string& body);
    void do_run(const std::string& session, const std::string& id,
                const std::string& type_code, const std::string& body);

    /// The session a job was enumerated under; the first session when the job is no
    /// longer tracked, as in MessageServer and ReportServer. Never empty while the bot
    /// holds a session — BotSession refuses an empty one before building a statement,
    /// and the caller then sees an authentication error for an object that has nothing
    /// to do with authentication.
    ///
    /// Prefer the explicit-session overloads below wherever the scope is known from
    /// enumeration: a job found in one scope must be acted on in that scope, and the
    /// first session is only a guess.
    std::string job_session(const std::string& id) const;
    void do_done(const std::string& id);
    void do_complete(const std::string& id);
    void do_fail(const std::string& id, const std::string& error);

    // Orphan paths. Both are reached from enum_jobs for a job that is NOT in jobs_,
    // so the scope must travel in as an argument — there is no entry to read it back
    // from, and asking anyway is what produced the permanent cancel loop of T192.
    void do_cancel(const std::string& session, const std::string& id);
    void do_abort(const std::string& session, const std::string& id);

    /// Acts under the scope the job is tracked in, falling back to the first session.
    void execute_action(const std::string& id, std::string_view action,
                        PgQuery::ResultHandler on_result);
    /// Acts under an explicit scope — use this when the job came from enumeration.
    void execute_action(const std::string& session, const std::string& id,
                        std::string_view action,
                        PgQuery::ResultHandler on_result);
    /// True when an action statement came back and its second result is ok.
    ///
    /// This is the ONLY way a failed action can be noticed: PgPool delivers a SQL error
    /// through the RESULT handler, not the exception one — PgQuery::fail has no call
    /// sites in the library, so every on_exception passed to pool_->execute is dead.
    /// Taking success on faith here would send an abort at an object the cancel never
    /// moved, fail again, and repeat every second.
    static bool action_ok(const std::vector<PgResult>& results);

    /// The database's own words for a failed statement batch. When the batch stopped at
    /// the first statement there is no second result, and the reason is in the first —
    /// saying "no result" there would repeat the misdirection T217 was opened for.
    static std::string result_error(const std::vector<PgResult>& results);

    void delete_job(const std::string& id);
    bool in_progress(const std::string& id) const;

    void on_fatal(const std::string& error);
};

} // namespace apostol

#endif // WITH_POSTGRESQL
