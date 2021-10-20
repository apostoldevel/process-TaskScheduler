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

#define SERVICE_APPLICATION_NAME "service"
#define CONFIG_SECTION_NAME "process/TaskScheduler"

#define API_BOT_USERNAME "apibot"

#define QUERY_INDEX_DATA     1

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

            m_AuthDate = 0;
            m_CheckDate = 0;

            m_HeartbeatInterval = 5000;
            m_Status = psStopped;
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

            m_AuthDate = 0;
            m_CheckDate = 0;

            m_Jobs.Clear();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Authentication() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                CPQueryResults pqResults;
                CStringList SQL;

                try {
                    CApostolModule::QueryToResults(APollQuery, pqResults);

                    m_Session = pqResults[0][0]["session"];
                    m_Secret = pqResults[0][0]["secret"];

                    m_ApiBot = pqResults[1][0]["get_session"];

                    m_AuthDate = Now() + (CDateTime) 24 / HoursPerDay;

                    m_Status = psRunning;
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            CString Application(SERVICE_APPLICATION_NAME);

            const auto &Providers = Server().Providers();
            const auto &Provider = Providers.DefaultValue();

            m_ClientId = Provider.ClientId(Application);
            m_ClientSecret = Provider.Secret(Application);

            CStringList SQL;

            api::login(SQL, m_ClientId, m_ClientSecret, m_Agent, m_Host);
            api::get_session(SQL, API_BOT_USERNAME, m_Agent, m_Host);

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::CheckTask() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                CPQueryResults pqResults;
                CStringList SQL;
                CString Error;

                int index;

                try {
                    CApostolModule::QueryToResults(APollQuery, pqResults);

                    const auto &jobs = pqResults[QUERY_INDEX_DATA];
                    for (int row = 0; row < jobs.Count(); ++row) {

                        const auto &job = jobs[row];

                        const auto &id = job["id"];
                        const auto &state_code = job["statecode"];

                        index = m_Jobs.IndexOf(id);
                        if (index != -1) {
                            if (state_code == "canceled") {
                                auto pQuery = dynamic_cast<CPQQuery *> (m_Jobs.Objects(index));
                                if (pQuery->CancelQuery(Error))
                                    DoAbort(id);
                                else
                                    DoFail(id, Error);
                            }

                            if (state_code == "executed")
                              continue;

                            if (state_code == "enabled")
                                m_Jobs.Delete(index);
                        }

                        if (state_code == "executed") {
                            DoAbort(id);
                        } else if (state_code == "enabled" || state_code == "aborted" || state_code == "failed") {
                            DoStart(id);
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

            api::authorize(SQL, m_ApiBot);

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

                CPQueryResults pqResults;
                try {
                    CApostolModule::QueryToResults(APollQuery, pqResults);

                    const CJSON Json(pqResults[QUERY_INDEX_DATA][0]["execute_object_action"]);

                    const auto &object = Json["object"].AsString();

                    const auto index = m_Jobs.IndexOf(object);
                    if (index != -1)
                        m_Jobs.Delete(index);
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            CStringList SQL;

            api::authorize(SQL, m_ApiBot);
            api::execute_object_action(SQL, Id, "execute");

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

            api::authorize(SQL, m_ApiBot);
            api::execute_object_action(SQL, Id, "fail");
            api::set_object_label(SQL, Id, Error);

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

            api::authorize(SQL, m_ApiBot);
            api::execute_object_action(SQL, Id, "abort");

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
