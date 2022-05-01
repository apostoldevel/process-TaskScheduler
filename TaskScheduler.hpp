/*++

Program name:

  Apostol Web Service

Module Name:

  TaskScheduler.hpp

Notices:

  Process: Task Scheduler

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#ifndef APOSTOL_PROCESS_TASK_SCHEDULER_HPP
#define APOSTOL_PROCESS_TASK_SCHEDULER_HPP
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Processes {

        //--------------------------------------------------------------------------------------------------------------

        //-- CTaskScheduler --------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CTaskScheduler: public CProcessCustom {
            typedef CProcessCustom inherited;

        private:
            CProcessStatus m_Status;

            CString m_Session;
            CString m_Secret;
            CString m_ClientId;
            CString m_ClientSecret;
            CString m_Agent;
            CString m_Host;

            CStringList m_Sessions;

            CDateTime m_AuthDate;
            CDateTime m_CheckDate;

            CStringList m_Jobs;

            int m_HeartbeatInterval;

            void BeforeRun() override;
            void AfterRun() override;

            void Authentication();
            void SignOut(const CString &Session);

            void CheckJobs(const CString &Session, const CPQueryResult &Jobs);
            void CheckTasks();

            void Heartbeat(CDateTime Now);

        protected:

            void DoTimer(CPollEventHandler *AHandler) override;

            void DoError(const Delphi::Exception::Exception &E);

            void DoStart(const CString &Session, const CString &Id);
            void DoAbort(const CString &Session, const CString &Id);
            void DoFail(const CString &Session, const CString &Id, const CString &Error);

            bool DoExecute(CTCPConnection *AConnection) override;

            void DoPostgresQueryExecuted(CPQPollQuery *APollQuery);
            void DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E);

            void DoPQClientException(CPQClient *AClient, const Delphi::Exception::Exception &E) override;
            void DoPQConnectException(CPQConnection *AConnection, const Delphi::Exception::Exception &E) override;

        public:

            explicit CTaskScheduler(CCustomProcess* AParent, CApplication *AApplication);

            ~CTaskScheduler() override = default;

            static class CTaskScheduler *CreateProcess(CCustomProcess *AParent, CApplication *AApplication) {
                return new CTaskScheduler(AParent, AApplication);
            }

            bool InProgress(const CString &Id);

            void Run() override;
            void Reload() override;

        };
        //--------------------------------------------------------------------------------------------------------------

    }
}

using namespace Apostol::Processes;
}
#endif //APOSTOL_PROCESS_TASK_SCHEDULER_HPP
