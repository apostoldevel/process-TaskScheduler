/*++

Program name:

  Apostol CRM

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

            m_Agent = CString().Format("%s (%s)", GApplication->Title().c_str(), ProcessName().c_str());
            m_Host = CApostolModule::GetIPByHostName(CApostolModule::GetHostName());

            m_AuthDate = 0;
            m_CheckDate = 0;

            m_HeartbeatInterval = 1000;
            m_Status = psStopped;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::BeforeRun() {
            Application()->Header(Application()->Name() + ": task scheduler");

            Log()->Debug(APP_LOG_DEBUG_CORE, MSG_PROCESS_START, GetProcessName(), Application()->Header().c_str());

            InitSignals();

            Reload();

            SetUser(Config()->User(), Config()->Group());

            InitializePQClients(Application()->Title(), 1, Config()->PostgresPollMin());

            SigProcMask(SIG_UNBLOCK);

            SetTimerInterval(1000);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::AfterRun() {
            CApplicationProcess::AfterRun();
            PQClientsStop();
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CTaskScheduler::InProgress(const CString &Id) {
            return m_Jobs.IndexOf(Id) != -1;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Run() {
            auto &PQClient = PQClientStart("helper");

            while (!sig_exiting) {

                Log()->Debug(APP_LOG_DEBUG_EVENT, _T("task scheduler cycle"));

                try {
                    PQClient.Wait();
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

                    const auto &session = login.First()["session"];

                    m_Sessions.Clear();
                    for (int i = 0; i < sessions.Count(); ++i) {
                        m_Sessions.Add(sessions[i]["get_sessions"]);
                    }

                    m_AuthDate = Now() + (CDateTime) 24 / HoursPerDay;
                    m_Status = psRunning;

                    SignOut(session);
                } catch (Delphi::Exception::Exception &E) {
                    DoFatal(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoFatal(E);
            };

            const auto &caProviders = Server().Providers();
            const auto &caProvider = caProviders.DefaultValue();

            const auto &clientId = caProvider.ClientId(SERVICE_APPLICATION_NAME);
            const auto &clientSecret = caProvider.Secret(SERVICE_APPLICATION_NAME);

            CStringList SQL;

            api::login(SQL, clientId, clientSecret, m_Agent, m_Host);
            api::get_sessions(SQL, API_BOT_USERNAME, m_Agent, m_Host);

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoFatal(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::SignOut(const CString &Session) {
            CStringList SQL;

            api::signout(SQL, Session);

            try {
                ExecSQL(SQL);
            } catch (Delphi::Exception::Exception &E) {
                DoFatal(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::EnumJob(const CString &Session, const CPQueryResult &List) {
            int index;
            CString Error;

            for (int row = 0; row < List.Count(); ++row) {
                const auto &job = List[row];

                const auto &id = job["id"];
                const auto &type_code = job["typecode"];
                const auto &state_code = job["statecode"];
                const auto &body = job["body"];

                index = m_Jobs.IndexOf(id);
                if (index != -1) {
                    if (state_code == "canceled") {
                        auto pQuery = dynamic_cast<CPQQuery *> (m_Jobs.Objects(index));
                        if (pQuery != nullptr) {
                            if (pQuery->CancelQuery(Error)) {
                                DoAbort(Session, id);
                            } else {
                                DoFail(Session, id, Error);
                            }
                        }
                    }
                } else {
                    if (state_code == "enabled" || state_code == "aborted" || state_code == "failed") {
                        DoStart(Session, id, type_code, body);
                    } else if (state_code == "executed") {
                        DoCancel(Session, id);
                    } else if (state_code == "canceled") {
                        DoAbort(Session, id);
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::CheckJob() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                CPQueryResults pqResults;
                CStringList SQL;

                const auto &session = APollQuery->Data()["session"];

                try {
                    CApostolModule::QueryToResults(APollQuery, pqResults);

                    const auto &authorize = pqResults[QUERY_INDEX_AUTH].First();

                    if (authorize["authorized"] != "t")
                        throw Delphi::Exception::ExceptionFrm("Authorization failed: %s", authorize["message"].c_str());

                    EnumJob(session, pqResults[QUERY_INDEX_DATA]);
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoFatal(E);
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
                    DoFatal(E);
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoFatal(const Delphi::Exception::Exception &E) {
            m_AuthDate = Now() + (CDateTime) SLEEP_SECOND_AFTER_ERROR / SecsPerDay; // 10 sec;
            m_CheckDate = m_AuthDate;

            m_Status = psStopped;

            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
            Log()->Notice("Continue after %d seconds", SLEEP_SECOND_AFTER_ERROR);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoError(const Delphi::Exception::Exception &E) {
            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::Heartbeat(CDateTime Now) {
            if ((Now >= m_AuthDate)) {
                m_AuthDate = Now + (CDateTime) 5 / SecsPerDay; // 5 sec
                Authentication();
            }

            if (m_Status == psRunning) {
                if ((Now >= m_CheckDate)) {
                    m_CheckDate = Now + (CDateTime) m_HeartbeatInterval / MSecsPerDay;
                    CheckJob();
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoTimer(CPollEventHandler *AHandler) {
            uint64_t exp;

            auto pTimer = dynamic_cast<CEPollTimer *> (AHandler->Binding());
            pTimer->Read(&exp, sizeof(uint64_t));

            try {
                Heartbeat(AHandler->TimeStamp());
            } catch (Delphi::Exception::Exception &E) {
                DoServerEventHandlerException(AHandler, E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DeleteJob(const CString &Id) {
            const auto index = m_Jobs.IndexOf(Id);
            if (index != -1)
                m_Jobs.Delete(index);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoRun(const CString &Session, const CString &Id, const CString &TypeCode, const CString &Body) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                const auto &session = APollQuery->Data()["session"];
                const auto &id = APollQuery->Data()["id"];
                const auto &type_code = APollQuery->Data()["type_code"];

                CPQResult *pResult;
                try {
                    for (int i = 0; i < APollQuery->Count(); i++) {
                        pResult = APollQuery->Results(i);

                        if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                            throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    if (type_code == "periodic.job") {
                        DoDone(session, id);
                    } else {
                        DoComplete(session, id);
                    }
                } catch (Delphi::Exception::Exception &E) {
                    DeleteJob(id);
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                DoFatal(E);
            };

            CStringList SQL;

            api::authorize(SQL, Session);
            SQL.Add(Body);

            Log()->Message("[%s] Task started.", Id.c_str());

            try {
                auto pQuery = ExecSQL(SQL, nullptr, OnExecuted, OnException);

                pQuery->Data().AddPair("session", Session);
                pQuery->Data().AddPair("id", Id);
                pQuery->Data().AddPair("type_code", TypeCode);

                const auto index = m_Jobs.IndexOf(Id);
                if (index != -1)
                    m_Jobs.Objects(index, (CPQQuery *) pQuery);
            } catch (Delphi::Exception::Exception &E) {
                DeleteJob(Id);
                DoFatal(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoStart(const CString &Session, const CString &Id, const CString &TypeCode, const CString &Body) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                const auto &session = APollQuery->Data()["session"];
                const auto &id = APollQuery->Data()["id"];
                const auto &type_code = APollQuery->Data()["type_code"];
                const auto &body = APollQuery->Data()["body"];

                CPQResult *pResult;
                try {
                    for (int i = 0; i < APollQuery->Count(); i++) {
                        pResult = APollQuery->Results(i);

                        if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                            throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    DoRun(session, id, type_code, body);
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                DoFatal(E);
            };

            CStringList SQL;

            api::authorize(SQL, Session);
            api::execute_object_action(SQL, Id, "execute");

            try {
                auto pQuery = ExecSQL(SQL, nullptr, OnExecuted, OnException);

                pQuery->Data().AddPair("session", Session);
                pQuery->Data().AddPair("id", Id);
                pQuery->Data().AddPair("type_code", TypeCode);
                pQuery->Data().AddPair("body", Body);

                m_Jobs.Add(Id);
            } catch (Delphi::Exception::Exception &E) {
                DoFatal(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoFail(const CString &Session, const CString &Id, const CString &Error) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                Log()->Message("[%s] Task failed.", id.c_str());
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                DoError(E);
            };

            CStringList SQL;

            api::authorize(SQL, Session);
            api::execute_object_action(SQL, Id, "fail");
            api::set_object_label(SQL, Id, Error);

            try {
                auto pQuery = ExecSQL(SQL, nullptr, OnExecuted, OnException);
                pQuery->Data().AddPair("id", Id);
            } catch (Delphi::Exception::Exception &E) {
                DoFatal(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoDone(const CString &Session, const CString &Id) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                Log()->Message("[%s] Task done.", id.c_str());
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                DoError(E);
            };

            CStringList SQL;

            api::authorize(SQL, Session);
            api::execute_object_action(SQL, Id, "done");

            try {
                auto pQuery = ExecSQL(SQL, nullptr, OnExecuted, OnException);
                pQuery->Data().AddPair("id", Id);
            } catch (Delphi::Exception::Exception &E) {
                DoFatal(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoComplete(const CString &Session, const CString &Id) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                Log()->Message("[%s] Task completed.", id.c_str());
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                DoError(E);
            };

            CStringList SQL;

            api::authorize(SQL, Session);
            api::execute_object_action(SQL, Id, "complete");

            try {
                auto pQuery = ExecSQL(SQL, nullptr, OnExecuted, OnException);
                pQuery->Data().AddPair("id", Id);
            } catch (Delphi::Exception::Exception &E) {
                DoFatal(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoAbort(const CString &Session, const CString &Id) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                Log()->Message("[%s] Task aborted.", id.c_str());
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                DoError(E);
            };

            CStringList SQL;

            api::authorize(SQL, Session);
            api::execute_object_action(SQL, Id, "abort");

            try {
                auto pQuery = ExecSQL(SQL, nullptr, OnExecuted, OnException);
                pQuery->Data().AddPair("id", Id);
            } catch (Delphi::Exception::Exception &E) {
                DoFatal(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CTaskScheduler::DoCancel(const CString &Session, const CString &Id) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                Log()->Message("[%s] Task canceled.", id.c_str());
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                const auto &id = APollQuery->Data()["id"];
                DeleteJob(id);
                DoError(E);
            };

            CStringList SQL;

            api::authorize(SQL, Session);
            api::execute_object_action(SQL, Id, "cancel");

            try {
                auto pQuery = ExecSQL(SQL, nullptr, OnExecuted, OnException);
                pQuery->Data().AddPair("id", Id);
            } catch (Delphi::Exception::Exception &E) {
                DoFatal(E);
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
                for (int i = 0; i < APollQuery->Count(); i++) {
                    pResult = APollQuery->Results(i);

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

        void CTaskScheduler::DoPQConnectException(CPQConnection *AConnection, const Delphi::Exception::Exception &E) {
            CServerProcess::DoPQConnectException(AConnection, E);
            if (m_Status == psRunning) {
                DoFatal(E);
            }
        }
    }
}

}
