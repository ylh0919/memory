#include<string>
#include <libwebsockets.h>
#include <thread>
#include <queue>
#include <mutex>
namespace kungfu
{
    namespace wingchun
    {
        namespace common
        {
            class CWebsocket;
            class CWebsocketCallBack
            {
            public:
                virtual ~CWebsocketCallBack(){};
                virtual void OnConnected(const CWebsocket* instance)=0;
                virtual void OnReceivedMessage(const CWebsocket* instance,const std::string& msg)=0;
                virtual void OnDisconnected(const CWebsocket* instance)=0;
                virtual void DoLoopItem(){};
            };
            class CWebsocket
            {
            public:
                CWebsocketCallBack* m_ptrCallback=nullptr;
                std::string m_strUrl;
                int m_nPort;
                lws_context* m_context = nullptr;
                lws*  m_connection =  nullptr;
                std::shared_ptr<std::thread> m_ptrThread;
                bool m_bIsRunning;
            public:
                CWebsocket();
                CWebsocket(const CWebsocket&);
                ~CWebsocket();
                bool Connect(const std::string& url,int port = 443);
                void Close();
                void SendMessage(const std::string& msg);
                void RegisterCallBack(CWebsocketCallBack* callback);
                void SendMessageInner();
                void StartThread();
            private:
                void loop();
            private:
                std::queue<std::string> m_queueMsg;
                std::mutex* m_mutexQueue;
            };     
        }
    }
}