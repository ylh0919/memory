//
// Created by wang on 11/18/18.
//

#ifndef KUNGFU_DEAMON_H
#define KUNGFU_DEAMON_H
#include "DaemonWrapper.h"
#include "SingleSendMail.h"
#include <thread>
#include <memory>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "MONITOR_DECLARE.h"
#include "uWS.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "process/process.h"
#include <queue>
#include <set>
#include "longfist/LFDataStruct.h"
#include "JournalReader.h"
#include <map>
#include <string.h>
#include "Timer.h"
#include <vector>
#include "Smtp.h"
#include <ThreadPool.h>
#include "Poco/Message.h"
#include "Poco/Net/MailMessage.h"
#include "Poco/Net/MailRecipient.h"
#include "Poco/Net/SMTPClientSession.h"
#include "Poco/Net/StringPartSource.h"
#include "Poco/Path.h"
#include "Poco/Exception.h"

using Poco::Message;
 
using Poco::Net::MailMessage;
using Poco::Net::MailRecipient;
using Poco::Net::SMTPClientSession;
using Poco::Net::StringPartSource;
using Poco::Path;
using Poco::Exception;

using rapidjson::Document;
using namespace rapidjson;
using namespace uWS;
MONITOR_NAMESPACE_START
using yijinjing::JournalReaderPtr;
using yijinjing::KfLogPtr;
extern volatile int g_signal_received;
extern DaemonConfig g_daemon_config;
struct DaemonConfig
{
    std::string ip;
    int         port;
    std::string scriptPath;
    std::set<std::string> whiteList;
};
struct TolerateMsg
{
    int     time_s;//容忍时间
    int     count;//容忍次数
};
struct TolerateMD
{
    int     last_time;
    int     counts;
};
struct RestapiConfig
{
    std::string AccessKeySecret;
    std::string AccessKeyId;
    std::string AccountName;
    std::string Subject;
    std::string TextBody;
    std::vector<std::string> ToAddress;
    std::map<int,TolerateMsg> TolerateMap;
};
struct ClientInfo
{
    std::string name;
    std::string type;
    std::string max_run_time;
    int64_t login_time;
    std::map<int,TolerateMD> TolerateMDMap;
};
struct ClientManager
{
    ClientInfo clientInfo;
    int64_t     pingValue = 0;
    int64_t     pongValue = 0;
};
class IRequest
{
public:
    IRequest(const std::string& name, const std::string& type):m_name(name),m_type(type){}
    virtual ~IRequest(){}
    virtual int execute(){}
protected:
    std::string m_name;
    std::string m_type;
};
using IRequestPtr = std::shared_ptr<IRequest>;

class SendMailReq:public IRequest
{
private:
    KfLogPtr    m_logger;
    RestapiConfig  m_restapi;
    std::vector<std::string> m_target_addr;
    std::string m_title;
    std::string m_content;
public:
    SendMailReq(RestapiConfig restapi,const std::vector<std::string>& target_addr,const std::string& title,const std::string& content):IRequest("", "")
    {
        m_logger = yijinjing::KfLog::getLogger("Monitor.Daemon");
        m_restapi = restapi;
        m_content = content;
        m_target_addr = target_addr;
        m_title = title;
    }
    virtual int execute() override
    {
        /*
        try
        {
            KF_LOG_INFO(m_logger, "send mail to:" << m_target_addr);
            CSmtp smtp(
                    m_smtp.port,									
                    m_smtp.serverAddress,						
                    m_smtp.userName,				    
                    m_smtp.password,					
                    m_target_addr,		            
                    m_title,							    
                    m_content		                		
            );
            int err;
            int send_count = 0;
            while ((err = smtp.SendEmail_Ex()) != 0)
            {
                ++send_count;
                if (err == 1)
                    cout << "the network is blocked!";
                else if (err == 2)
                    cout << "username error!";
                else if (err == 3)
                    cout << "password or security code error!";
                else if (err == 4)
                    cout << "Attachment directory is incorrect!";
                if(send_count >= 3)
                {
                    cout << " tried 3 times,failed to send mail" << endl;
                    break;
                }
                else
                {
                    cout << " try again" << endl;
                }  
            }
            KF_LOG_INFO(m_logger, "send mail to:" << m_target_addr << " end");
            return 0;
        }
        catch(const std::exception& e)
        {
            KF_LOG_INFO(m_logger, "send mail to "<< g_daemon_config.scriptPath <<" exception:"<< e.what()<<",code:-1");
        }
        return -1;
        */
        /*int count = 1;
        do
        {
            try
            { 
                KF_LOG_INFO(m_logger, "send mail "<< m_title <<", count:" << count);

                std::string target_mails = "";
                MailMessage message;
                message.setSender(m_smtp.userName);
                for(auto& addr:m_target_addr)
                {
                    target_mails += addr+",";
                    message.addRecipient(MailRecipient(MailRecipient::PRIMARY_RECIPIENT, addr));
                }
                KF_LOG_INFO(m_logger, "send mail "<< m_title <<" to:" << target_mails);
                message.setSubject(m_title);
                message.setContent(m_content);
                SMTPClientSession session(m_smtp.serverAddress);
                session.open();
                session.login(SMTPClientSession::LoginMethod::AUTH_LOGIN,m_smtp.userName,m_smtp.password);
                session.sendMessage(message);
                session.close();
                KF_LOG_INFO(m_logger, "send mail "<< m_title <<" end");
                break;
            }
            catch (const std::exception& e)
            {
                KF_LOG_INFO(m_logger, "send mail "<< m_title <<" failed, exception:"<< e.what());
                std::this_thread::sleep_for(std::chrono::seconds(1));
                ++count;
            }
        }while(count <= 3);*/
        return 0;
    }
};
class RestartReq:public IRequest
{
private:
    std::vector<std::string> m_args;
    KfLogPtr                 m_logger;
public:
    RestartReq(const std::string& name, const std::string& type):IRequest(name, type)
    {
        m_logger = yijinjing::KfLog::getLogger("Monitor.Daemon");
        m_args.push_back("0");
        m_args.push_back(type + "_" + name);  
    }
    virtual int execute() override
    {
        try
        {
            if (m_args.size() != 2)
            {
                KF_LOG_INFO(m_logger, "restart error:must 2 args");
                return -1;
            }
            if (g_daemon_config.whiteList.find(m_type + "_" + m_name) == g_daemon_config.whiteList.end())
            {
                KF_LOG_INFO(m_logger, "restart info:" << m_name <<" needn't be restarted");
                return -1;
            }
            //KF_LOG_DEBUG(m_logger, "restart command args:bash " << g_daemon_config.scriptPath << " " << m_args[0]<< " " << m_args[1] << " &");
            std::string commandLine = "bash " + g_daemon_config.scriptPath + " " + m_args[0] +  " " + m_args[1];
	        KF_LOG_DEBUG(m_logger,"restart command:" + commandLine);
            /*procxx::process restart("bash", g_daemon_config.scriptPath, m_args[0], m_args[1]);
            procxx::process::limits_t limits;
            limits.cpu_time(30);
            restart.limit(limits);
            restart.exec();
            restart.wait();
            auto ret = restart.code();
            KF_LOG_INFO(m_logger, "restart "<< g_daemon_config.scriptPath <<" over,code:"<< ret);
            */
            system(commandLine.c_str());
            std::this_thread::sleep_for(std::chrono::seconds(15));
        }
        catch(const std::exception& e)
        {
            KF_LOG_INFO(m_logger, "restart "<< g_daemon_config.scriptPath <<" exception:"<< e.what()<<",code:-1");
        }
        return -1;
    }
};
class StartStrategyReq:public IRequest
{
private:
    std::vector<std::string> m_args;
    KfLogPtr                 m_logger;
public:
    StartStrategyReq(const std::vector<std::string>& list_strategy_name):IRequest("", "st")
    {
        m_logger = yijinjing::KfLog::getLogger("Monitor.Daemon");
        m_args.push_back("1");
        std::string names = "";
        for(auto& name:list_strategy_name)
        {
            names += name+" ";
        }
        m_args.push_back(names);
        m_name = names;
    }
    StartStrategyReq(const std::string& strategy_name):IRequest("", "st")
    {
        m_logger = yijinjing::KfLog::getLogger("Monitor.Daemon");
        m_args.push_back("1");
        m_name = strategy_name;
        m_args.push_back(m_name);
    }
    virtual int execute() override
    {
        try
        {
            if (m_args.size() != 2)
            {
                KF_LOG_INFO(m_logger, "restart error:must 2 args");
                return -1;
            }
            if ( m_args[1].empty())
            {
                KF_LOG_INFO(m_logger, "restart info:" << m_name <<" needn't be restarted");
                return -1;
            }
            std::string commandLine = "bash " + g_daemon_config.scriptPath + " " + m_args[0] +  " " + m_args[1];
	        KF_LOG_DEBUG(m_logger,"restart command:" + commandLine);
            return system(commandLine.c_str());
        }
        catch(const std::exception& e)
        {
            KF_LOG_INFO(m_logger, "restart "<< g_daemon_config.scriptPath <<" exception:"<< e.what()<<",code:-1");
        }
        return -1;
    }
};
class StopStrategyReq:public IRequest
{
private:
    std::vector<std::string> m_args;
    KfLogPtr                 m_logger;
public:
    StopStrategyReq(const std::vector<std::string>& list_strategy_ids):IRequest("", "st")
    {
        m_logger = yijinjing::KfLog::getLogger("Monitor.Daemon");
        m_args.push_back("2");
        std::string names = "";
        for(auto& name:list_strategy_ids)
        {
            names += name+" ";
        }
        m_args.push_back(names);
        m_name = names;
    }
    StopStrategyReq(const std::string& strategy_id):IRequest("", "st")
    {
        m_logger = yijinjing::KfLog::getLogger("Monitor.Daemon");
        m_args.push_back("2");
        m_name = strategy_id;
        m_args.push_back(m_name);
    }
    virtual int execute() override
    {
        try
        {
            if (m_args.size() != 2)
            {
                KF_LOG_INFO(m_logger, "restart error:must 2 args");
                return -1;
            }
            if ( m_args[1].empty())
            {
                KF_LOG_INFO(m_logger, "restart info:" << m_name <<" needn't be restarted");
                return -1;
            }
            std::string commandLine = "bash " + g_daemon_config.scriptPath + " " + m_args[0] +  " " + m_args[1];
	        KF_LOG_DEBUG(m_logger,"restart command:" + commandLine);
            return system(commandLine.c_str());
        }
        catch(const std::exception& e)
        {
            KF_LOG_INFO(m_logger, "restart "<< g_daemon_config.scriptPath <<" exception:"<< e.what()<<",code:-1");
        }
        return -1;
    }
};
using CLIENT_MANAGER_MAP = std::map<WebSocket<SERVER>*, ClientManager>;
class Daemon
{
public:
    Daemon(KfLogPtr);
    ~Daemon();
    bool init();
    bool start();
    void stop();
    void wait();
public:
    void onMessage(WebSocket<SERVER> *, char *, size_t, OpCode);
    void onConnection(WebSocket<SERVER> *, HttpRequest);
    void onDisconnection(WebSocket<SERVER> *, int code, char *, size_t);
    void onPing(WebSocket<SERVER> *, char *, size_t);
    void onPong(WebSocket<SERVER> *, char *, size_t);
public:
    void mail_init();
    void mail_start();
    void mail_listening();
    void mail_stop();
    bool sendEmail(const LFErrorMsgField* error,long time);
    
    int add_client(const string name,const string folder);
    int remove_client(const string name);
    void add_all_to_journal();
    void get_server_docker(string sname, string sip, string dname);
    void set_restapi_config(RestapiConfig& restapi);
    string stamp_to_standard(long stampTime);
private:
    void handle_login_mail(std::string type,std::string name,long time);
    void handle_send_mail(RestapiConfig restapi,const std::string& AccessKeySecret,const std::string& AccessKeyId,const std::string& AccountName,const std::string& Subject,const std::string& TextBody,const std::vector<std::string>& ToAddress);
    void handle_error(std::string type,std::string name,int errorID,std::string errorMsg,long time);
    void handle_error(const LFErrorMsgField& error,long time);
    void doLogin(const rapidjson::Document& login, WebSocket<SERVER> *ws);
    bool parseUrl();
    void startPingThread();
    void stopPingThread();
    void checkClient();
    void startReqThread();
    void stopReqThread();
    void push(IRequestPtr);
    IRequestPtr pop();
private:
    Hub                     m_hub;
    KfLogPtr                m_logger;
    std::atomic<bool>       m_isRunning;
    std::thread             m_pingThread;
    std::mutex              m_pingMutex;
    std::condition_variable m_pingCond;
    CLIENT_MANAGER_MAP      m_clients;
    std::mutex              m_clientMutex;
    std::thread             m_rqThread;
    std::queue<IRequestPtr> m_rq;
    std::mutex              m_rqMutex;
    std::condition_variable m_rqCond;
    map<string,string> client_map;
    JournalReaderPtr reader;
    long cur_time;
    KfLogPtr logger;
    vector<string> folders;
    vector<string> names;
    map<string,vector<string>> td_st_map;
    map<string,int> st_pid_map;
    string server_name;
    string server_ip;
    string docker_name; 
    RestapiConfig m_restapiConfig;
    kungfu::wingchun::ThreadPool* m_ThreadPoolPtr = nullptr;
    std::map<std::string,int> m_mapStategys;
    std::map<std::string,int> m_mapStategysNeedToBeStarted;
};
MONITOR_NAMESPACE_END
#endif //KUNGFU_DEAMON_Hd
