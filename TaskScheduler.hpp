#pragma once

#ifdef WITH_POSTGRESQL

#include "apostol/process_module.hpp"

#include "apostol/bot_session.hpp"
#include "apostol/notify_subscriber.hpp"
#include "apostol/pg.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

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
//   - Every heartbeat interval reads the mode of the notify channel `job`
//     (notify.channel_policy) — on every tick, never cached: the mode decides
//     which of the two paths below this tick takes, and a cached `legacy` after
//     the channel became `single` is the poller that runs a body twice.
//   - Periodic jobs cycle: enabled → executed → enabled (with dateRun bump)
//   - Disposable jobs: enabled → executed → completed
//   - Failed jobs: executed → failed (error stored as object label)
//   - Canceled jobs: executed → canceled → aborted
//
// Path `legacy` (no channel, a database without the notify module, or the
// channel in any mode but `single`) — as before the channel existed:
//   - Polls api.job('enabled') in every scope
//   - Executes each job's SQL body via deferred PG queries
//   - Where the channel exists, the body is bounded below its lease as on the
//     path `single`: a body that runs here when the channel is switched is left
//     alone by api.job_due only while it is younger than the lease.
//
// Path `single` — the database hands each due run to one instance:
//   - api.job_due() in every scope: the due jobs become rows of the channel
//   - NotifySubscriber takes a row → api.job_claim: `execute`, its own
//     committed transaction, and the body (read there, never from the row)
//   - the body, then `done` / `complete`, then notify.ack — one transaction.
//     `done` before the ack: the other order deadlocks with another
//     instance's api.job_due (measured, T1104). A stale token (NT409) on the
//     ack rolls the body back with it.
//   - a body that fails: `fail` + label, then notify.ack — a transaction of
//     its own.
//   - the body is bounded below the channel's lease (statement_timeout, and
//     transaction_timeout where the server has it): a body outliving the lease
//     is handed out again and would run twice.
//   - a job canceled while its body runs is not interrupted: its `done` is
//     refused, the body is rolled back with it, the row goes back and
//     api.job_claim aborts the job.
//   - a row handed out again while this process still runs its earlier
//     delivery (the lease ran out in the pool's queue) is run: the earlier
//     ack is refused under the old token and its body rolled back.
//   - the guarantees above hold for a body that does not COMMIT itself: one
//     that does splits the batch, and what it committed stays.
//   - the channel read in any mode but `single` again: the subscriber is
//     stopped (its rows go back); a body already sent ends as it would.
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
    void on_stopping() override;
    bool busy() const override;
    void on_stop() override;

private:
    using time_point   = std::chrono::system_clock::time_point;
    using milliseconds = std::chrono::milliseconds;
    using NotifyRow    = db_platform::NotifyRow;

    // ── State ────────────────────────────────────────────────────────────────

    PgPool*     pool_{nullptr};     // borrowed from Application
    Logger*     logger_{nullptr};   // borrowed from Application
    EventLoop*  loop_{nullptr};     // borrowed from the process
    std::string node_;              // Application::node_id(), for notify.subscriber

    std::unique_ptr<BotSession> bot_;

    // Path `single`: the subscriber of the channel `job`, made at the first tick
    // that reads `single` — a database without the channel never sees one.
    std::unique_ptr<db_platform::NotifySubscriber> notify_;
    unsigned notify_generation_{0};  // bumped per subscriber: a claim answered
                                     // for an earlier one is not this one's

    // Scope id → the bot's session in it: the row names the scope, the claim
    // needs the session. Written by every tick of the path `single`; a scope the
    // bot has left stays until the restart — its rows are refused.
    std::unordered_map<std::string, std::string> scope_sessions_;

    milliseconds lease_{0};         // the channel's lease, as the last tick read it
    bool         mode_in_flight_{false};

    enum class Status { stopped, running };
    Status status_{Status::stopped};

    struct Job
    {
        std::string id;
        std::string type_code;
        std::string session;      // the scope this job was found in; act in it
        time_point  started_at;
        uint64_t    query_id{0};  // PgPool query handle for cancel
        // Path `single`: the row the body runs under. Zero — a run of the path
        // `legacy`; a token is never zero.
        std::int64_t row_token{0};
    };

    std::unordered_map<std::string, Job> jobs_;

    time_point   next_check_{};
    milliseconds check_interval_{1000};

    // ── Job lifecycle ────────────────────────────────────────────────────────

    /// The tick: reads the channel's mode, then takes one of the two paths.
    void check_jobs();
    void on_mode(std::vector<PgResult> results);

    // Path `legacy`.
    void poll_jobs();
    void enum_jobs(const std::string& session, std::vector<PgResult> results);

    // Path `single`.
    void offer_jobs();
    void on_offered(const std::string& session, std::vector<PgResult> results);
    void start_notify();
    void stop_notify();
    void on_row(const NotifyRow& row);
    void on_claimed(const NotifyRow& row, unsigned generation, const std::string& session,
                    const std::string& id, std::vector<PgResult> results);
    void run_body(const NotifyRow& row, const std::string& session, const std::string& id,
                  const std::string& type_code, const std::string& body);
    void fail_body(const NotifyRow& row, const std::string& session, const std::string& id,
                   const std::string& error);
    /// The statements that bound the body below the lease; empty without one.
    std::string body_limits() const;

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
                        PgQuery::ResultHandler on_result,
                        PgRetry retry = PgRetry::never);
    /// Acts under an explicit scope — use this when the job came from enumeration.
    void execute_action(const std::string& session, const std::string& id,
                        std::string_view action,
                        PgQuery::ResultHandler on_result,
                        PgRetry retry = PgRetry::never);
    /// True when an action statement came back and its second result is ok.
    ///
    /// Until libapostol b7cc42d (09.09) this was the ONLY way a failed action could be
    /// noticed: PgPool delivered a SQL error through the RESULT handler. Now it goes to
    /// on_exception when there is one — execute_action always passes one — and this
    /// check stays as the guard for a result that is not ok all the same. Taking
    /// success on faith would send an abort at an object the cancel never moved, fail
    /// again, and repeat every second.
    static bool action_ok(const std::vector<PgResult>& results);

    /// The database's own words for a failed statement batch. When the batch stopped at
    /// the first statement there is no second result, and the reason is in the first —
    /// saying "no result" there would repeat the misdirection T217 was opened for.
    static std::string result_error(const std::vector<PgResult>& results);

    void delete_job(const std::string& id);
    /// Path `single`: forgets the run under this row's token only — a later
    /// delivery of the same job may be running here by now.
    void delete_run(const std::string& id, std::int64_t row_token);
    bool in_progress(const std::string& id) const;

    void on_fatal(const std::string& error);
};

} // namespace apostol

#endif // WITH_POSTGRESQL
