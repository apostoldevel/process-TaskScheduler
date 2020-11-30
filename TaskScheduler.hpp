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

            CString m_Token;
            CString m_Session;
            CString m_Secret;
            CString m_ClientId;
            CString m_ClientSecret;
            CString m_Agent;
            CString m_Host;

            int m_HeartbeatInterval;

            CDateTime m_AuthDate;
            CDateTime m_CheckDate;

            void CheckTask();

            void BeforeRun() override;
            void AfterRun() override;

            void Authentication();
            void Authorize(CStringList &SQL, const CString &Username);

            static void ExecuteObjectAction(CStringList &SQL, const CString &MsgId, const CString &Action);
            static void SetArea(CStringList &SQL, const CString &Area);
            static void SetObjectLabel(CStringList &SQL, const CString &MsgId, const CString &Label);

        protected:

            void DoTimer(CPollEventHandler *AHandler) override;

            void DoHeartbeat();
            void DoError(const Delphi::Exception::Exception &E);

            void DoExecute(const CString &Id);
            void DoDone(const CString &Id);

            void DoAbort(const CString &Id);
            void DoFail(const CString &Id, const CString &Error);

            bool DoExecute(CTCPConnection *AConnection) override;

            void DoPostgresQueryExecuted(CPQPollQuery *APollQuery);
            void DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E);

        public:

            explicit CTaskScheduler(CCustomProcess* AParent, CApplication *AApplication);

            ~CTaskScheduler() override = default;

            static class CTaskScheduler *CreateProcess(CCustomProcess *AParent, CApplication *AApplication) {
                return new CTaskScheduler(AParent, AApplication);
            }

            void Run() override;
            void Reload() override;

            CPQPollQuery *GetQuery(CPollConnection *AConnection) override;

        };
        //--------------------------------------------------------------------------------------------------------------

    }
}

using namespace Apostol::Processes;
}
#endif //APOSTOL_PROCESS_TASK_SCHEDULER_HPP
