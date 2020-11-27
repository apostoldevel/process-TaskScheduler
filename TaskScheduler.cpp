/*++

Program name:

  Apostol Web Service

Module Name:

  TaskScheduler.cpp

Notices:

  Process: Task Scheduler

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#include "Core.hpp"
#include "TaskScheduler.hpp"
//----------------------------------------------------------------------------------------------------------------------

#define PROVIDER_APPLICATION_NAME "service"
#define CONFIG_SECTION_NAME "process/TaskScheduler"

#define API_BOT_USERNAME "apibot"

extern "C++" {

namespace Apostol {

    namespace Processes {

        //--------------------------------------------------------------------------------------------------------------

        //-- CTaskScheduler --------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CTaskScheduler::CTaskScheduler(CCustomProcess *AParent, CApplication *AApplication):
                inherited(AParent, AApplication, "task scheduler") {

            m_Agent = "Task Scheduler";
            m_Host = CApostolModule::GetIPByHostName(CApostolModule::GetHostName());

            const auto now = Now();

            m_AuthDate = now;
            m_CheckDate = now;

            m_HeartbeatInterval = 5000;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::BeforeRun() {
            sigset_t set;

            Application()->Header(Application()->Name() + ": task scheduler");

            Log()->Debug(0, MSG_PROCESS_START, GetProcessName(), Application()->Header().c_str());

            InitSignals();

            Reload();

            SetUser(Config()->User(), Config()->Group());

            InitializePQServer(Application()->Title());

            PQServerStart("helper");

            SigProcMask(SIG_UNBLOCK, SigAddSet(&set));

            SetTimerInterval(1000);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::AfterRun() {
            CApplicationProcess::AfterRun();
            PQServerStop();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Run() {
            while (!sig_exiting) {

                log_debug0(APP_LOG_DEBUG_EVENT, Log(), 0, "task scheduler cycle");

                try
                {
                    PQServer().Wait();
                }
                catch (Delphi::Exception::Exception &E)
                {
                    Log()->Error(APP_LOG_EMERG, 0, E.what());
                }

                if (sig_terminate || sig_quit) {
                    if (sig_quit) {
                        sig_quit = 0;
                        Log()->Error(APP_LOG_NOTICE, 0, "gracefully shutting down");
                        Application()->Header("task scheduler is shutting down");
                    }

                    //DoExit();

                    if (!sig_exiting) {
                        sig_exiting = 1;
                    }
                }

                if (sig_reconfigure) {
                    sig_reconfigure = 0;
                    Log()->Error(APP_LOG_NOTICE, 0, "reconfiguring");

                    Reload();
                }

                if (sig_reopen) {
                    sig_reopen = 0;
                    Log()->Error(APP_LOG_NOTICE, 0, "reopening logs");
                }
            }

            Log()->Error(APP_LOG_NOTICE, 0, "stop task scheduler");
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Reload() {
            CServerProcess::Reload();

            const auto now = Now();

            m_AuthDate = now;
            m_CheckDate = now;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Authentication() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                CPQueryResults Result;
                CStringList SQL;

                try {
                    CApostolModule::QueryToResults(APollQuery, Result);
                    const auto &login = Result[0][0];

                    m_Session = login["session"];
                    m_Secret = login["secret"];

                    m_AuthDate = Now() + (CDateTime) 24 / HoursPerDay;
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            CString Application(PROVIDER_APPLICATION_NAME);

            const auto &Providers = Server().Providers();
            const auto &Provider = Providers.DefaultValue();

            m_ClientId = Provider.ClientId(Application);
            m_ClientSecret = Provider.Secret(Application);

            CStringList SQL;

            SQL.Add(CString().Format("SELECT * FROM api.login(%s, %s, %s, %s);",
                                     PQQuoteLiteral(m_ClientId).c_str(),
                                     PQQuoteLiteral(m_ClientSecret).c_str(),
                                     PQQuoteLiteral(m_Agent).c_str(),
                                     PQQuoteLiteral(m_Host).c_str()
            ));

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Authorize(CStringList &SQL, const CString &Username) {
            SQL.Add(CString().Format("SELECT * FROM api.authorize(%s);",
                                     PQQuoteLiteral(m_Session).c_str()
            ));

            SQL.Add(CString().Format("SELECT * FROM api.su(%s, %s);",
                                     PQQuoteLiteral(Username).c_str(),
                                     PQQuoteLiteral(m_ClientSecret).c_str()
            ));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::RunAction(CStringList &SQL, const CString &Id, const CString &Action) {
            SQL.Add(CString().Format("SELECT * FROM api.run_action(%s, %s);",
                                     Id.c_str(),
                                     PQQuoteLiteral(Action).c_str()
            ));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::SetArea(CStringList &SQL, const CString &Area) {
            SQL.Add(CString().Format("SELECT * FROM api.set_session_area(%s);",
                                     PQQuoteLiteral(Area).c_str()
            ));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::SetObjectLabel(CStringList &SQL, const CString &Id, const CString &Label) {
            SQL.Add(CString().Format("SELECT * FROM api.set_object_label(%s, %s);",
                                     Id.c_str(),
                                     PQQuoteLiteral(Label).c_str()
            ));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::CheckTask() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                CPQueryResults Result;
                CStringList SQL;

                try {
                    CApostolModule::QueryToResults(APollQuery, Result);

                    const auto &enabled = Result[2]; // enabled -> execute
                    for (int Row = 0; Row < enabled.Count(); Row++) {
                        const auto &Record = enabled[Row];
                        DoExecute(Record["id"]);
                    }

                    const auto &canceled = Result[3]; // canceled -> abort
                    for (int Row = 0; Row < canceled.Count(); Row++) {
                        const auto &Record = canceled[Row];
                        DoAbort(Record["id"]);
                    }

                    const auto &completed = Result[4]; // completed -> execute
                    for (int Row = 0; Row < completed.Count(); Row++) {
                        const auto &Record = completed[Row];
                        DoExecute(Record["id"]);
                    }

                    const auto &failed = Result[5]; // failed -> execute
                    for (int Row = 0; Row < failed.Count(); Row++) {
                        const auto &Record = failed[Row];
                        DoExecute(Record["id"]);
                    }

                    const auto &aborted = Result[6]; // aborted -> execute
                    for (int Row = 0; Row < aborted.Count(); Row++) {
                        const auto &Record = aborted[Row];
                        DoExecute(Record["id"]);
                    }

                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            struct timeval now = {0, 0};

            gettimeofday(&now, nullptr);

            CStringList SQL;

            Authorize(SQL, API_BOT_USERNAME);

            SQL.Add(CString().Format("SELECT * FROM api.task('enabled', %d) ORDER BY created LIMIT 5;", now.tv_sec));
            SQL.Add(CString().Format("SELECT * FROM api.task('canceled', %d) ORDER BY created LIMIT 5;", now.tv_sec));
            SQL.Add(CString().Format("SELECT * FROM api.task('completed', %d) ORDER BY created LIMIT 5;", now.tv_sec));
            SQL.Add(CString().Format("SELECT * FROM api.task('failed', %d) ORDER BY created LIMIT 5;", now.tv_sec));
            SQL.Add(CString().Format("SELECT * FROM api.task('aborted', %d) ORDER BY created LIMIT 5;", now.tv_sec));

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoError(const Delphi::Exception::Exception &E) {
            const auto now = Now();

            m_Token.Clear();
            m_Session.Clear();
            m_Secret.Clear();

            m_AuthDate = now + (CDateTime) m_HeartbeatInterval / MSecsPerDay;
            m_CheckDate = now + (CDateTime) m_HeartbeatInterval * 2 / MSecsPerDay;

            Log()->Error(APP_LOG_EMERG, 0, E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoHeartbeat() {
            const auto now = Now();

            if ((now >= m_AuthDate)) {
                Authentication();
            }

            if ((now >= m_CheckDate)) {
                m_CheckDate = now + (CDateTime) m_HeartbeatInterval / MSecsPerDay;
                if (!m_Session.IsEmpty()) {
                    CheckTask();
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoTimer(CPollEventHandler *AHandler) {
            uint64_t exp;

            auto pTimer = dynamic_cast<CEPollTimer *> (AHandler->Binding());
            pTimer->Read(&exp, sizeof(uint64_t));

            try {
                DoHeartbeat();
            } catch (Delphi::Exception::Exception &E) {
                DoServerEventHandlerException(AHandler, E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoExecute(const CString &Id) {
            CStringList SQL;

            Authorize(SQL, API_BOT_USERNAME);
            RunAction(SQL, Id, "execute");

            Log()->Message("[%s] Task executed.", Id.c_str());

            try {
                ExecSQL(SQL);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoDone(const CString &Id) {
            CStringList SQL;

            Authorize(SQL, API_BOT_USERNAME);
            RunAction(SQL, Id, "done");

            Log()->Message("[%s] Task completed.", Id.c_str());

            try {
                ExecSQL(SQL);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoFail(const CString &Id, const CString &Error) {
            CStringList SQL;

            Authorize(SQL, API_BOT_USERNAME);
            RunAction(SQL, Id, "fail");
            SetObjectLabel(SQL, Id, Error);

            Log()->Message("[%s] Task failed.", Id.c_str());

            try {
                ExecSQL(SQL);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoAbort(const CString &Id) {
            CStringList SQL;

            Authorize(SQL, API_BOT_USERNAME);
            RunAction(SQL, Id, "abort");

            Log()->Message("[%s] Task aborted.", Id.c_str());

            try {
                ExecSQL(SQL);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        CPQPollQuery *CTaskScheduler::GetQuery(CPollConnection *AConnection) {
            auto pQuery = CServerProcess::GetQuery(AConnection);

            if (Assigned(pQuery)) {
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                pQuery->OnPollExecuted([this](auto && APollQuery) { DoPostgresQueryExecuted(APollQuery); });
                pQuery->OnException([this](auto && APollQuery, auto && AException) { DoPostgresQueryException(APollQuery, AException); });
#else
                pQuery->OnPollExecuted(std::bind(&CTaskScheduler::DoPostgresQueryExecuted, this, _1));
                pQuery->OnException(std::bind(&CTaskScheduler::DoPostgresQueryException, this, _1, _2));
#endif
            }

            return pQuery;
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CTaskScheduler::DoExecute(CTCPConnection *AConnection) {
            return true;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoPostgresQueryExecuted(CPQPollQuery *APollQuery) {
            CPQResult *pResult;
            try {
                for (int I = 0; I < APollQuery->Count(); I++) {
                    pResult = APollQuery->Results(I);

                    if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                }
            } catch (Delphi::Exception::Exception &E) {
                Log()->Error(APP_LOG_EMERG, 0, E.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
            Log()->Error(APP_LOG_EMERG, 0, E.what());
        }
        //--------------------------------------------------------------------------------------------------------------
    }
}

}
