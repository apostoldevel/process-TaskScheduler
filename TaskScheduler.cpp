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

#define QUERY_INDEX_AUTH     0
#define QUERY_INDEX_DATA     1

#define SLEEP_SECOND_AFTER_ERROR 10

extern "C++" {

namespace Apostol {

    namespace Processes {

        //--------------------------------------------------------------------------------------------------------------

        //-- CTaskScheduler --------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CTaskScheduler::CTaskScheduler(CCustomProcess *AParent, CApplication *AApplication):
                inherited(AParent, AApplication, "task scheduler") {

            m_Agent = CString().Format("%s (Task Scheduler)", Application()->Title().c_str());
            m_Host = CApostolModule::GetIPByHostName(CApostolModule::GetHostName());

            m_AuthDate = 0;
            m_CheckDate = 0;

            m_HeartbeatInterval = 1000;
            m_Status = psStopped;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::BeforeRun() {
            sigset_t set;

            Application()->Header(Application()->Name() + ": task scheduler");

            Log()->Debug(APP_LOG_DEBUG_CORE, MSG_PROCESS_START, GetProcessName(), Application()->Header().c_str());

            InitSignals();

            Reload();

            SetUser(Config()->User(), Config()->Group());

            InitializePQClient(Application()->Title(), 1, Config()->PostgresPollMin());

            PQClientStart("helper");

            SigProcMask(SIG_UNBLOCK, SigAddSet(&set));

            SetTimerInterval(1000);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::AfterRun() {
            CApplicationProcess::AfterRun();
            PQClientStop();
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CTaskScheduler::InProgress(const CString &Id) {
            return m_Jobs.IndexOf(Id) != -1;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Run() {
            while (!sig_exiting) {

                Log()->Debug(APP_LOG_DEBUG_EVENT, _T("task scheduler cycle"));

                try {
                    PQClient().Wait();
                } catch (Delphi::Exception::Exception &E) {
                    Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
                }

                if (sig_terminate || sig_quit) {
                    if (sig_quit) {
                        sig_quit = 0;
                        Log()->Debug(APP_LOG_DEBUG_EVENT, _T("gracefully shutting down"));
                        Application()->Header(_T("task scheduler is shutting down"));
                    }

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

            Log()->Debug(APP_LOG_DEBUG_EVENT, _T("stop task scheduler"));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Reload() {
            CServerProcess::Reload();

            m_Session.Clear();
            m_Secret.Clear();
            m_Sessions.Clear();
            m_Jobs.Clear();

            m_AuthDate = 0;
            m_CheckDate = 0;

            m_Status = psStopped;

            Log()->Notice("[%s] Successful reloading", CONFIG_SECTION_NAME);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Authentication() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                CPQueryResults pqResults;

                CStringList SQL;

                try {
                    CApostolModule::QueryToResults(APollQuery, pqResults);

                    const auto &login = pqResults[0];
                    const auto &sessions = pqResults[1];

                    const CString oldSession(m_Session);

                    m_Session = login.First()["session"];
                    m_Secret = login.First()["secret"];

                    m_Sessions.Clear();
                    for (int i = 0; i < sessions.Count(); ++i) {
                        m_Sessions.Add(sessions[i]["get_sessions"]);
                    }

                    if (!oldSession.IsEmpty()) {
                        SignOut(oldSession);
                    }

                    m_AuthDate = Now() + (CDateTime) 24 / HoursPerDay;

                    m_Status = psRunning;
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            const auto &caProviders = Server().Providers();
            const auto &caProvider = caProviders.DefaultValue();

            m_ClientId = caProvider.ClientId(SERVICE_APPLICATION_NAME);
            m_ClientSecret = caProvider.Secret(SERVICE_APPLICATION_NAME);

            CStringList SQL;

            api::login(SQL, m_ClientId, m_ClientSecret, m_Agent, m_Host);
            api::get_sessions(SQL, API_BOT_USERNAME, m_Agent, m_Host);

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::SignOut(const CString &Session) {
            CStringList SQL;

            api::signout(SQL, Session);

            try {
                ExecSQL(SQL);
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

                const auto &session = APollQuery->Data()["session"];

                try {
                    CApostolModule::QueryToResults(APollQuery, pqResults);

                    const auto &authorize = pqResults[QUERY_INDEX_AUTH].First();

                    if (authorize["authorized"] != "t")
                        throw Delphi::Exception::ExceptionFrm("Authorization failed: %s", authorize["message"].c_str());

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
                                    DoAbort(session, id);
                                else
                                    DoFail(session, id, Error);
                            }

                            if (state_code == "executed")
                              continue;

                            if (state_code == "enabled")
                                m_Jobs.Delete(index);
                        }

                        if (state_code == "executed") {
                            DoAbort(session, id);
                        } else if (state_code == "enabled" || state_code == "aborted" || state_code == "failed") {
                            DoStart(session, id);
                        }
                    }
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            for (int i = 0; i < m_Sessions.Count(); ++i) {
                const auto &session = m_Sessions[i];

                CStringList SQL;

                api::authorize(SQL, session);
                api::job(SQL, "enabled");

                try {
                    auto pQuery = ExecSQL(SQL, nullptr, OnExecuted, OnException);
                    pQuery->Data().AddPair("session", session);
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoError(const Delphi::Exception::Exception &E) {
            m_Session.Clear();
            m_Secret.Clear();

            m_AuthDate = Now() + (CDateTime) SLEEP_SECOND_AFTER_ERROR / SecsPerDay; // 10 sec;
            m_CheckDate = m_AuthDate;

            m_Status = psStopped;

            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
            Log()->Notice("Continue after %d seconds", SLEEP_SECOND_AFTER_ERROR);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoHeartbeat() {
            const auto now = Now();

            if ((now >= m_AuthDate)) {
                m_AuthDate = now + (CDateTime) 5 / SecsPerDay; // 5 sec
                Authentication();
            }

            if (m_Status == psRunning) {
                if ((now >= m_CheckDate)) {
                    m_CheckDate = now + (CDateTime) m_HeartbeatInterval / MSecsPerDay;
                    if (!m_Session.IsEmpty()) {
                        CheckTask();
                    }
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

        void CTaskScheduler::DoStart(const CString &Session, const CString &Id) {

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

            api::authorize(SQL, Session);
            api::execute_object_action(SQL, Id, "execute");

            Log()->Message("[%s] Task started.", Id.c_str());

            try {
                m_Jobs.AddObject(Id, (CPQQuery *) ExecSQL(SQL, nullptr, OnExecuted, OnException));
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoFail(const CString &Session, const CString &Id, const CString &Error) {
            CStringList SQL;

            api::authorize(SQL, Session);
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

        void CTaskScheduler::DoAbort(const CString &Session, const CString &Id) {
            CStringList SQL;

            api::authorize(SQL, Session);
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

        void CTaskScheduler::DoPQClientException(CPQClient *AClient, const Delphi::Exception::Exception &E) {
            CServerProcess::DoPQClientException(AClient, E);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoPQConnectException(CPQConnection *AConnection, const Delphi::Exception::Exception &E) {
            CServerProcess::DoPQConnectException(AConnection, E);
            if (m_Status == psRunning) {
                DoError(E);
            }
        }
    }
}

}
