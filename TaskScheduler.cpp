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

#define QUERY_INDEX_AUTH    0
#define QUERY_INDEX_SU      1
#define QUERY_INDEX_JOB     2

extern "C++" {

namespace Apostol {

    namespace Processes {

        //--------------------------------------------------------------------------------------------------------------

        //-- CTaskScheduler --------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CTaskScheduler::CTaskScheduler(CCustomProcess *AParent, CApplication *AApplication):
                inherited(AParent, AApplication, "job scheduler") {

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

            Application()->Header(Application()->Name() + ": job scheduler");

            Log()->Debug(APP_LOG_DEBUG_CORE, MSG_PROCESS_START, GetProcessName(), Application()->Header().c_str());

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

        bool CTaskScheduler::InProgress(const CString &Id) {
            return m_Jobs.IndexOf(Id) != -1;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Run() {
            while (!sig_exiting) {

                Log()->Debug(APP_LOG_DEBUG_EVENT, _T("job scheduler cycle"));

                try
                {
                    PQServer().Wait();
                }
                catch (Delphi::Exception::Exception &E)
                {
                    Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
                }

                if (sig_terminate || sig_quit) {
                    if (sig_quit) {
                        sig_quit = 0;
                        Log()->Debug(APP_LOG_DEBUG_EVENT, _T("gracefully shutting down"));
                        Application()->Header(_T("job scheduler is shutting down"));
                    }

                    //DoExit();

                    if (!sig_exiting) {
                        sig_exiting = 1;
                    }
                }

                if (sig_reconfigure) {
                    sig_reconfigure = 0;
                    Log()->Debug(APP_LOG_DEBUG_EVENT, _T("reconfiguring"));

                    Reload();
                }

                if (sig_reopen) {
                    sig_reopen = 0;
                    Log()->Debug(APP_LOG_DEBUG_EVENT, _T("reopening logs"));
                }
            }

            Log()->Debug(APP_LOG_DEBUG_EVENT, _T("stop job scheduler"));
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

                CPQueryResults pqResults;
                CStringList SQL;

                try {
                    CApostolModule::QueryToResults(APollQuery, pqResults);
                    const auto &login = pqResults[0][0];

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

        void CTaskScheduler::ExecuteObjectAction(CStringList &SQL, const CString &Id, const CString &Action) {
            SQL.Add(CString().Format("SELECT * FROM api.execute_object_action(%s::uuid, %s);",
                                     PQQuoteLiteral(Id).c_str(),
                                     PQQuoteLiteral(Action).c_str()
            ));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::SetArea(CStringList &SQL, const CString &Area) {
            SQL.Add(CString().Format("SELECT * FROM api.set_session_area(%s::uuid);",
                                     PQQuoteLiteral(Area).c_str()
            ));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::SetObjectLabel(CStringList &SQL, const CString &Id, const CString &Label) {
            SQL.Add(CString().Format("SELECT * FROM api.set_object_label(%s::uuid, %s);",
                                     PQQuoteLiteral(Id).c_str(),
                                     PQQuoteLiteral(Label).c_str()
            ));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::CheckTask() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                CPQueryResults pqResults;
                CStringList SQL;
                CString Error;

                int Index;

                try {
                    CApostolModule::QueryToResults(APollQuery, pqResults);

                    const auto &Jobs = pqResults[QUERY_INDEX_JOB];
                    for (int Row = 0; Row < Jobs.Count(); ++Row) {

                        const auto &Job = Jobs[Row];

                        const auto &Id = Job.Values("id");
                        const auto &StateCode = Job.Values("statecode");

                        Index = m_Jobs.IndexOf(Id);
                        if (Index != -1) {
                            if (StateCode == "canceled") {
                                auto pQuery = dynamic_cast<CPQQuery *> (m_Jobs.Objects(Index));
                                if (pQuery->CancelQuery(Error))
                                    DoAbort(Id);
                                else
                                    DoFail(Id, Error);
                            }
                            continue;
                        }

                        if (StateCode == "executed") {
                            DoAbort(Id);
                        } else {
                            DoStart(Id);
                        }
                    }

                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            CStringList SQL;

            Authorize(SQL, API_BOT_USERNAME);

            SQL.Add(CString().Format("SELECT * FROM api.job('enabled') ORDER BY created;"));

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

            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
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

        void CTaskScheduler::DoStart(const CString &Id) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                CPQResult *pResult;
                try {
                    for (int I = 0; I < APollQuery->Count(); I++) {
                        pResult = APollQuery->Results(I);

                        if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                            throw Delphi::Exception::EDBError(pResult->GetErrorMessage());

                        if (I <= QUERY_INDEX_SU)
                            continue;

                        const CJSON Json(pResult->GetValue(0, 0));

                        const auto &Id = Json["object"].AsString();

                        const auto Index = m_Jobs.IndexOf(Id);
                        if (Index != -1)
                            m_Jobs.Delete(Index);
                    }
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            CStringList SQL;

            Authorize(SQL, API_BOT_USERNAME);
            ExecuteObjectAction(SQL, Id, "execute");

            Log()->Message("[%s] Task started.", Id.c_str());

            try {
                m_Jobs.AddObject(Id, (CPQQuery *) ExecSQL(SQL, nullptr, OnExecuted, OnException));
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoFail(const CString &Id, const CString &Error) {
            CStringList SQL;

            Authorize(SQL, API_BOT_USERNAME);
            ExecuteObjectAction(SQL, Id, "fail");
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
            ExecuteObjectAction(SQL, Id, "abort");

            Log()->Message("[%s] Task aborted.", Id.c_str());

            try {
                ExecSQL(SQL);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
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
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
            DoError(E);
        }
        //--------------------------------------------------------------------------------------------------------------
    }
}

}
