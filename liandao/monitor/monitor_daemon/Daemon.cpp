//
// Created by wang on 11/18/18.
//
#include "Daemon.h"
#include <iostream>
#include <thread>
#include <algorithm>
#include "longfist/LFUtils.h"
#include "longfist/sys_messages.h"
#include "longfist/LFConstants.h"
#include "PosHandler.hpp"

#include "time.h"
using namespace std;
MONITOR_NAMESPACE_START
volatile int g_signal_received = -1;
int is_running = 1;
DaemonConfig g_daemon_config{};
Daemon::Daemon(KfLogPtr log):m_logger(log)
{
    m_isRunning = true;
    m_ThreadPoolPtr = new kungfu::wingchun::ThreadPool(1);
}

Daemon::~Daemon()
{
    stop();
    delete m_ThreadPoolPtr;
}

bool Daemon::init()
{
    m_hub.onConnection(std::bind(&Daemon::onConnection,this, std::placeholders::_1, std::placeholders::_2));
    m_hub.onDisconnection(std::bind(&Daemon::onDisconnection,this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,std::placeholders::_4));
    m_hub.onMessage(std::bind(&Daemon::onMessage,this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,std::placeholders::_4));
    m_hub.onPong(std::bind(&Daemon::onPong,this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    m_hub.onPing(std::bind(&Daemon::onPing,this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    startPingThread();
    startReqThread();
    mail_init();
    KF_LOG_INFO(m_logger, "Daemon init success");
    return true;
}

bool Daemon::start()
{
    if(!m_hub.listen(g_daemon_config.ip.c_str(), g_daemon_config.port))
    {
        KF_LOG_INFO(m_logger, "Daemon start error");
        return false;
    }
    KF_LOG_INFO(m_logger, "Daemon listen on " << g_daemon_config.ip << ":" << g_daemon_config.port) ;
    mail_start();
    return true;
}

void Daemon::stop()
{
    stopPingThread();
    stopReqThread();
    mail_stop();
}

void Daemon::wait()
{
    KF_LOG_INFO(m_logger, "Daemon poll");
    while( m_isRunning && g_signal_received < 0)
    {
        m_hub.poll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

//{"messageType":"login","name":"xxx","clientType":"md"}
void Daemon::onMessage(WebSocket<SERVER> *ws, char *data, size_t len, OpCode)
{
    std::string value(data, len);
    KF_LOG_DEBUG(m_logger, "received data,ws:" << ws << ",msg:"<< value);
    Document jsond;
    jsond.Parse(value.c_str());
    if(jsond.HasParseError())
    {
        KF_LOG_ERROR(m_logger, "msg parse error");
        return;
    }
    if(!jsond.HasMember("messageType") || !jsond["messageType"].IsString())
    {
        KF_LOG_ERROR(m_logger, "msg format error");
        return;
    }
    //do login
    if (!std::string("login").compare(jsond["messageType"].GetString()))
    {
        doLogin(jsond,ws);
    }
    //send td_strategy
    if (!std::string("td_strategy").compare(jsond["messageType"].GetString()))
    {
        const auto& sts_json = jsond["strategy"].GetArray();
        const auto& td_name = jsond["name"].GetString();
        vector<string> sts;
        for(auto iter = sts_json.Begin(); iter != sts_json.End(); iter++)
        {
            string st_str = iter->GetString();
            sts.push_back(st_str);
            auto it = m_mapStategys.find(st_str);
            if(it == m_mapStategys.end())
            {
                it = m_mapStategys.insert(std::make_pair(st_str,1)).first;
            }
            else
            {
                it->second+=1;
            }
            auto it_restart = m_mapStategysNeedToBeStarted.find(st_str);
            if(it_restart != m_mapStategysNeedToBeStarted.end() && it_restart->second == it->second)
            {
                push(std::make_shared<StartStrategyReq>(st_str));
                m_mapStategysNeedToBeStarted.erase(it_restart);
            }
        }
        td_st_map[td_name] = sts;
        KF_LOG_DEBUG(m_logger, "onMessage,name:" << td_name << ",st count:"<< sts.size());
    }
}
void Daemon::handle_login_mail(std::string type,std::string name,long time)
{
    LFErrorMsgField error{};
    error.ErrorId = 0;
    strncpy(error.Name,name.c_str(),sizeof(error.Name));
    strncpy(error.Type,type.c_str(),sizeof(error.Type));
    handle_error(error,cur_time);
}
void Daemon::handle_send_mail(RestapiConfig restapi,const std::string& AccessKeySecret,const std::string& AccessKeyId,const std::string& AccountName,const std::string& Subject,const std::string& TextBody,const std::vector<std::string>& ToAddress)
{
    int count = 1;
    do
    {
        try
        { 
            KF_LOG_INFO(m_logger, "send mail "<< Subject <<", count:" << count);
            std::string target_mails = "";
            for(auto& addr:ToAddress)
                target_mails += addr+",";
            target_mails.pop_back();
            KF_LOG_INFO(m_logger, "send mail "<< Subject <<" to:" << target_mails);
            SingleSendMail(AccessKeySecret,AccessKeyId,AccountName,Subject,TextBody,target_mails);
            KF_LOG_INFO(m_logger, "send mail "<< Subject <<" end");
            break;
        }
        catch (const std::exception& e)
        {
            KF_LOG_INFO(m_logger, "send mail "<< Subject <<" failed, exception:"<< e.what());
            std::this_thread::sleep_for(std::chrono::seconds(1));
            ++count;
        }
    }while(count <= 3);
}
void Daemon::handle_error(std::string type,std::string name,int errorID,std::string errorMsg,long cur_time)
{
    LFErrorMsgField error{};
    error.ErrorId = errorID;
    strncpy(error.ErrorMsg,errorMsg.c_str(),sizeof(error.ErrorMsg));
    strncpy(error.Name,name.c_str(),sizeof(error.Name));
    strncpy(error.Type,type.c_str(),sizeof(error.Type));
    handle_error(error,cur_time);
}
void Daemon::handle_error(const LFErrorMsgField& error,long cur_time)
{
    KF_LOG_DEBUG(m_logger, "handle_error, name:"<< error.Name << ",type:"<< error.Type <<"error id:" << error.ErrorId << ",msg:"<< error.ErrorMsg);
    sendEmail(&error,cur_time);
    if (error.ErrorId >= 0 && error.ErrorId <= 100)
    {//不重启任何模块
        return;
    }
    else if (error.ErrorId >= 101 && error.ErrorId <= 200)
    {//仅重启报错模块
        if(strcmp(error.Type,"st") == 0)
        {
            remove_client(error.Name);
            auto it = m_mapStategysNeedToBeStarted.find(error.Name);
            if(it != m_mapStategysNeedToBeStarted.end())
            {
                KF_LOG_DEBUG(m_logger, "handle_error,module:" << error.Name << " is already in restart list");
                return;
            }
            map<string,int>::iterator st_pid_iter = st_pid_map.find(error.Name);
            if (st_pid_iter != st_pid_map.end())
            {
                string st_pid = std::to_string(st_pid_iter->second);
                push(std::make_shared<StopStrategyReq>(st_pid));
            }
            KF_LOG_DEBUG(m_logger, "handle_error,restart module:" << error.Name);
            push(std::make_shared<StartStrategyReq>(error.Name));
            
        }
        else
        {
            push(std::make_shared<RestartReq>(error.Name, error.Type));
        }
    }
    else if (error.ErrorId >= 201 && error.ErrorId <= 300)
    {//重启报错模块，并重启相关模块
        KF_LOG_DEBUG(m_logger, "handle_error,error id:" << error.ErrorId << ",msg:"<< error.ErrorMsg);
        auto td_iter = td_st_map.find(error.Name);
        if(td_iter != td_st_map.end())
        {
            std::vector<std::string> listIds;
            for(auto& st:td_iter->second)
            {
                auto it_st = st_pid_map.find(st);
                if(it_st != st_pid_map.end())
                {
                    listIds.push_back(std::to_string(it_st->second));
                }
                //记录TD重启时，相关策略的数据（关联的TD有几个）
                auto it = m_mapStategys.find(st);
                if(it != m_mapStategys.end())
                {
                    m_mapStategysNeedToBeStarted.insert(std::make_pair(it->first,it->second));
                    it->second -= 1;
                    if(it->second == 0)
                    {
                        m_mapStategys.erase(it);
                    }
                }
            }
            if(listIds.size() > 0)
            {
                push(std::make_shared<StopStrategyReq>(listIds));
            }
        }
        else
        {
            KF_LOG_DEBUG(m_logger, "handle_error,not found name:" << error.Name);
        }

        
        push(std::make_shared<RestartReq>(error.Name, error.Type));
    } 
    else if (error.ErrorId >= 301 && error.ErrorId <= 500)
    {//不重启任何模块
        return;
    }
    return;
}
void Daemon::doLogin(const rapidjson::Document& login, WebSocket<SERVER> *ws)
{
    KF_LOG_DEBUG(m_logger, "doLogin,ws:" << ws);
    if (!login["name"].IsString() || !login["clientType"].IsString())
    {
        return;
    }
    string name = login["name"].GetString();
    string clientType = login["clientType"].GetString();
    int64_t imax_run_time = std::numeric_limits<int64_t>::max();
    string max_run_time = std::to_string(imax_run_time);
    if(login.HasMember("max_run_time")){
        max_run_time = login["max_run_time"].GetString();
    }

    int64_t login_time = time(0);
    KF_LOG_INFO(logger,"login_time1:"<<login_time);
    if(login.HasMember("pid"))
    {
        int pid = login["pid"].GetInt();
        KF_LOG_DEBUG(m_logger, " pid:" << pid);
        st_pid_map[name] = pid;
    }
    
    //
    {
        std::unique_lock<std::mutex> l(m_clientMutex);
        auto userIter = m_clients.insert(std::make_pair(ws,ClientManager())).first;
        if( userIter != m_clients.end())
        {
            userIter->second.clientInfo.name = name;
            userIter->second.clientInfo.type = clientType;
            userIter->second.clientInfo.max_run_time = max_run_time;
            userIter->second.clientInfo.login_time = login_time;
            std::map<int,TolerateMsg>::iterator it;
            for(it = m_restapiConfig.TolerateMap.begin();it != m_restapiConfig.TolerateMap.end();it++){
                KF_LOG_DEBUG(m_logger, "m_restapiConfig.TolerateMap.begin()");
                TolerateMD toleratemd;
                toleratemd.last_time = login_time;
                toleratemd.counts = 0;
                userIter->second.clientInfo.TolerateMDMap.insert(make_pair(it->first,toleratemd));
            }
        }
        
    }
    handle_login_mail(clientType,name,time(0) * 1e9);
    KF_LOG_DEBUG(m_logger, "user login,name:" << name << ",ws:" << ws);

}

void Daemon::onConnection(WebSocket<SERVER> *ws, HttpRequest)
{
    std::unique_lock<std::mutex> l(m_clientMutex);
    //m_clients[ws];
    KF_LOG_DEBUG(m_logger, "user connected ,ws:" << ws);
}

void Daemon::onDisconnection(WebSocket<SERVER> *ws, int code, char *data, size_t len)
{
    std::string value(data, len);
    KF_LOG_DEBUG(m_logger, "user disconnected ,ws:" << ws << ",msg:" << value);
    std::unique_lock<std::mutex> lck(m_clientMutex);
    auto cur_iter = m_clients.find(ws);
    if(cur_iter != m_clients.end())
    {
        KF_LOG_DEBUG(m_logger, "user disconnect,name:" << cur_iter->second.clientInfo.name << ",ws:" << ws <<",code:" << code);
        //push(std::make_shared<RestartReq>(cur_iter->second.clientInfo.name, cur_iter->second.clientInfo.type, 101));
        LFErrorMsgField error;
        strcpy(error.Name,(char*)(cur_iter->second.clientInfo.name).c_str());
        strcpy(error.Type,(char*)(cur_iter->second.clientInfo.type).c_str());
        error.ErrorId = 101;
        if(strcmp(error.Type, "td") ==0)
        {
            error.ErrorId = 201;
        }
        strcpy(error.ErrorMsg,"disconnected!");
        lck.unlock();
        m_clients.erase(cur_iter);
        handle_error(error,time(0) * 1e9);
    }
}

void Daemon::onPing(WebSocket<SERVER> *, char *, size_t)
{

}

void Daemon::onPong(WebSocket<SERVER> *ws, char *data, size_t len)
{
    std::string value(data, len);
    KF_LOG_DEBUG(m_logger,"Pong("<<ws << "):" << value);
    int64_t pongValue = std::strtoll(value.c_str(), NULL, 10);
    {
        std::unique_lock<std::mutex> l(m_clientMutex);
        auto cur_iter = m_clients.find(ws);
        if(cur_iter != m_clients.end())
        {
            cur_iter->second.pongValue = pongValue;
            KF_LOG_DEBUG(m_logger, "user pong,type:" << cur_iter->second.clientInfo.type<< ",name:" << cur_iter->second.clientInfo.name << ",ws:" << ws << ",pong value:" << pongValue);
        }
        else
        {
            KF_LOG_DEBUG(m_logger, "not found clientw" << ws << ",pong value:" << pongValue);
        }
        
    }
}



void Daemon::startPingThread()
{
    m_pingThread = std::thread([&]{
        KF_LOG_INFO(m_logger, "start ping thread:" << std::this_thread::get_id());
        while (m_isRunning)
        {
            checkClient();
            KF_LOG_INFO(m_logger, "checkClient ping");
            std::unique_lock<std::mutex> l(m_pingMutex);
            m_pingCond.wait_for(l, std::chrono::seconds(10));
        }
        KF_LOG_INFO(m_logger, "exit ping thread:" << std::this_thread::get_id());
    });
}

void Daemon::checkClient()
{
    std::unique_lock<std::mutex> l(m_clientMutex);
    for (auto clientIter = m_clients.begin(); clientIter != m_clients.end();)
    {
        if (!m_isRunning)
        {
            return;
        }
        //check client timeout
        auto& clientInfo = clientIter->second;
        int64_t now_time = time(0);
        int64_t max_run_time = stoll(clientInfo.clientInfo.max_run_time);
        KF_LOG_INFO(logger,"now_time:"<<now_time<<" login_time:"<<clientInfo.clientInfo.login_time<<" max_run_time:"<<max_run_time);
        if(clientInfo.pingValue - clientInfo.pongValue >= 2)
        {
            //clientIter->first->close();
            KF_LOG_DEBUG(m_logger, "user:" << clientInfo.clientInfo.name << ",ws:" << clientIter->first << " is timeout,close it");
            //push(std::make_shared<RestartReq>(clientInfo.clientInfo.name, clientInfo.clientInfo.type, 100));
            //clientIter = m_clients.erase(clientIter);
            LFErrorMsgField error;
            strcpy(error.Name,(char*)(clientInfo.clientInfo.name).c_str());
            strcpy(error.Type,(char*)(clientInfo.clientInfo.type).c_str());
            error.ErrorId = 101;
            if(clientInfo.clientInfo.type == "td")
            {
                error.ErrorId = 201;
            }
            strcpy(error.ErrorMsg,"timeout!");
            clientIter = m_clients.erase(clientIter);
            handle_error(error,time(0) * 1e9);
            continue;
        }
        else if(now_time - clientInfo.clientInfo.login_time >= max_run_time)
        {
            //clientIter->first->close();
            KF_LOG_DEBUG(m_logger, "user:" << clientInfo.clientInfo.name << ",ws:" << clientIter->first << " max_run_time,close it");
            //push(std::make_shared<RestartReq>(clientInfo.clientInfo.name, clientInfo.clientInfo.type, 100));
            //clientIter = m_clients.erase(clientIter);
            LFErrorMsgField error;
            strcpy(error.Name,(char*)(clientInfo.clientInfo.name).c_str());
            strcpy(error.Type,(char*)(clientInfo.clientInfo.type).c_str());
            error.ErrorId = 102;
            if(clientInfo.clientInfo.type == "td")
            {
                error.ErrorId = 202;
            }
            strcpy(error.ErrorMsg,"max_run_time!");
            clientIter = m_clients.erase(clientIter);
            handle_error(error,time(0) * 1e9);
            continue;
        }
        KF_LOG_DEBUG(m_logger,"clientInfo.clientInfo.TolerateMDMap.size()="<<clientInfo.clientInfo.TolerateMDMap.size());
        std::map<int,TolerateMD>::iterator it;
        for(it = clientInfo.clientInfo.TolerateMDMap.begin();it != clientInfo.clientInfo.TolerateMDMap.end();it++){
            auto itr = m_restapiConfig.TolerateMap.find(it->first);
            if(itr != m_restapiConfig.TolerateMap.end()){
                if(now_time - it->second.last_time >= itr->second.time_s){
                    KF_LOG_DEBUG(m_logger,"change time:"<<it->first);
                    it->second.last_time = now_time;
                    it->second.counts = 0;
                }
            }
        }

        //send ping msg
        clientIter->first->ping(std::to_string(++clientInfo.pingValue).c_str());
        KF_LOG_DEBUG(m_logger, "user ping,type:" << clientInfo.clientInfo.type << ",name:" << clientInfo.clientInfo.name << ",ws:" << clientIter->first << ",ping value:" << clientInfo.pingValue);
        ++clientIter;
    }
}

void Daemon::stopPingThread()
{
    {
        m_isRunning = false;
        m_pingCond.notify_all();
    }
    if (m_pingThread.joinable())
    {
        m_pingThread.join();
    }
}

void Daemon::startReqThread()
{
    m_rqThread = std::thread([&]{
        KF_LOG_INFO(m_logger, "start request thread:" << std::this_thread::get_id());
        while (m_isRunning)
        {
            std::unique_lock<std::mutex> l(m_rqMutex);
            while (m_rq.empty() && m_isRunning)
            {
                m_rqCond.wait_for(l, std::chrono::seconds(10));
            }
            if (m_isRunning)
            {
                auto req = pop();
                if (req)
                {
                    req->execute();
                }
            }
        }
        KF_LOG_INFO(m_logger, "exit request thread:" << std::this_thread::get_id());
    });
}

void Daemon::stopReqThread()
{
    {
        std::unique_lock<std::mutex> l(m_rqMutex);
        m_isRunning = false;
        m_rqCond.notify_all();
    }
    if (m_rqThread.joinable())
    {
        m_rqThread.join();
    }
}

void Daemon::push(IRequestPtr req)
{
    std::unique_lock<std::mutex> l(m_rqMutex);
    if(m_rq.size() > 10000)
    {
        m_rq.pop();
    }
    m_rq.push(req);
    KF_LOG_INFO(m_logger, "push::req size:" << m_rq.size());
}

IRequestPtr Daemon::pop()
{
    if(!m_rq.empty())
    {
        auto req = m_rq.front();
        m_rq.pop();
        KF_LOG_INFO(m_logger, "pop::req size:" << m_rq.size());
        return  req;
    }
    return IRequestPtr();
}

void Daemon::add_all_to_journal()
{
    auto mapSources = get_map_sources();
    for(auto iter = mapSources.begin();iter != mapSources.end();iter++)
    {
        JournalPair jp = getMdJournalPair(iter->first);
        folders.push_back(jp.first);
        names.push_back(jp.second);
        jp = getTdJournalPair(iter->first);
        folders.push_back(jp.first);
        names.push_back(jp.second);
    }
}

void Daemon::mail_init()
{
    add_all_to_journal();
    reader = kungfu::yijinjing::JournalReader::createReaderWithSys(folders, names, kungfu::yijinjing::getNanoTime(), "MonitorDaemonEmail");
    logger = yijinjing::KfLog::getLogger("Monitor.DaemonEmail");
    KF_LOG_INFO(logger, "DaemonEmail Initialize Successed!");
}

void Daemon::mail_start()
{
    thread t(&Daemon::mail_listening, this);
    t.detach();
    KF_LOG_INFO(logger, "DaemonEmail Start Listening!");
}

void Daemon::mail_listening()
{
    yijinjing::FramePtr frame;
    while (is_running)
    {
        frame = reader->getNextFrame();
        if (frame.get() != nullptr)
        {
            short msg_type = frame->getMsgType();
            short msg_source = frame->getSource();
            cur_time = frame->getNano();
            if (msg_type == MSG_TYPE_STRATEGY_START)
            {
                try
                {
                    string content((char*)frame->getData());
                    json j_request = json::parse(content);
                    string folder = j_request["folder"].get<string>();
                    string name = j_request["name"].get<string>();
                    if (add_client(name,folder))
                    {
                        KF_LOG_INFO(logger, "[user] Accepted: " << name);
                    }
                    else
                    {
                        KF_LOG_INFO(logger, "[user] Rejected: " << name);
                    }
                }
                catch (...)
                {
                    KF_LOG_ERROR(logger, "error in parsing STRATEGY_START: " << (char*)frame->getData());
                }
            }
            else if (msg_type == MSG_TYPE_LF_ERRORMSG)
            {
                KF_LOG_INFO(logger, "error message detected!");
                void* fdata = frame->getData();
                KF_LOG_INFO(logger, "frame_address:" << frame << " msg_source" << msg_source);
                LFErrorMsgField* error = (LFErrorMsgField*)fdata;
                //if (error->ErrorId > 100)
                {
                    std::unique_lock<std::mutex> l(m_clientMutex);
                    for(auto userIter = m_clients.begin();userIter != m_clients.end();userIter++)
                    {
                        if(strcmp(userIter->second.clientInfo.name.c_str(),error->Name)==0 && strcmp(userIter->second.clientInfo.type.c_str(),error->Type)==0)
                        {
                            KF_LOG_INFO(m_logger, "error from "<< error->Type << "_" << error->Name<<" id:"<<error->ErrorId);
                            auto it = userIter->second.clientInfo.TolerateMDMap.find(error->ErrorId);
                            if(it != userIter->second.clientInfo.TolerateMDMap.end()){
                                it->second.counts += 1;
                                int tolerate_count = 0;
                                auto itr = m_restapiConfig.TolerateMap.find(error->ErrorId);
                                if(itr != m_restapiConfig.TolerateMap.end()){
                                    tolerate_count = itr->second.count;
                                }
                                if(it->second.counts >= tolerate_count){
                                    if (error->ErrorId > 100) m_clients.erase(userIter);
                                    handle_error(*error,cur_time);
                                    break;                                    
                                }
                            }
                            else{
                                //KF_LOG_INFO(m_logger, "erase "<< error->Type << "_" << error->Name);
                                if (error->ErrorId > 100) m_clients.erase(userIter);
                                handle_error(*error,cur_time);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

int Daemon::add_client(const string name,const string folder)
{
    KF_LOG_INFO(logger, "Add Client Start!");
    auto iter = client_map.find(folder);
    if(iter != client_map.end())
    {
        KF_LOG_INFO(logger, "Client already exist: " << name);
        return 0;
    }
    client_map[folder] = name;
    KF_LOG_INFO(logger, "folder:" << folder << " name:" << name);
    reader->addJournal(folder, name);
    KF_LOG_INFO(logger, "Add Client End!");
    return 1;
}
int Daemon::remove_client(const string name)
{
    KF_LOG_INFO(logger, "rm client start!");
    for(auto iter = client_map.begin();iter != client_map.end();++iter)
    {
        if(iter->second == name)
        {
            client_map.erase(iter);
            break;
        }
    }
    return 0;
}


void Daemon::mail_stop()
{
    is_running = 0;
}

bool Daemon::sendEmail(const LFErrorMsgField* error,long unix_time)
{
    
    KF_LOG_INFO(logger, "send error message email start!");
    KF_LOG_INFO(logger, "type:" << error->Type << " name:" << error->Name << " errorId:" << error->ErrorId << " errorMsg:" << error->ErrorMsg);
    string type = error->Type;
    string name = error->Name;
    int EI = error->ErrorId;
    string title = server_name + "-" + server_ip  + "-"+docker_name+"-"+type + "_" + name;
    string content = "";
    if (EI == 0)
    {
        title = title + "-started";
        content = type + "_" + name + " start success";
    }
    else if (EI > 0 && EI <= 300)
    {
        char ei[32];
        sprintf(ei, "%d", EI);
        string errorId = ei;
        string errorMsg = error->ErrorMsg;
        string time = stamp_to_standard(unix_time);
        title = title + "-" + errorMsg;
        content = type + "_" + name + "\nerrorId:" + errorId + "\nerrorMsg:" + errorMsg + "\ntime:" + time + "\nserver_name:" + server_name + "\nserver_ip:" + server_ip + "\ndocker_name:" + docker_name;
    }
    else if (EI > 300 && EI <= 500)
    {
        // sendMailTest(symbolLeft + ' pair short close', 'pair short close position', 'lizandthebluebird@qq.com, fsy@beavotech.com, 1435825986@qq.com')
        // sendMailTest(symbolLeft + ' pair long open', 'pair long open position', 'lizandthebluebird@qq.com, fsy@beavotech.com, 1435825986@qq.com')
        // sendMailTest(symbolLeft + ' pair long close', 'pair long close position', 'lizandthebluebird@qq.com, fsy@beavotech.com, 1435825986@qq.com')
        // sendMailTest(symbolLeft + ' pair short open', 'pair short open position', 'lizandthebluebird@qq.com, fsy@beavotech.com, 1435825986@qq.com')
        if (EI == 301)
        {
            char ei[32];
            sprintf(ei, "%d", EI);
            string errorId = ei;
            string errorMsg = error->ErrorMsg;
            vector<string>tokens;
            string delimiters = "-";
            string::size_type lastPos = errorMsg.find_first_not_of(delimiters, 0);
            string::size_type pos = errorMsg.find_first_of(delimiters, lastPos);
            while (string::npos != pos || string::npos != lastPos)
            {
                tokens.push_back(errorMsg.substr(lastPos, pos - lastPos));
                lastPos = errorMsg.find_first_not_of(delimiters, pos);
                pos = errorMsg.find_first_of(delimiters, lastPos);
            }
            if (tokens.size() != 3)
            {
                tokens.resize(3);
                tokens[0] = tokens[1] = tokens[2] = "Parameter error";
            }
            string symbolLeft = tokens[0];
            string operation_type = tokens[1];
	    string position = tokens[2];
            string time = stamp_to_standard(unix_time);
            title = title + "-" + operation_type;
            content = type + "_" + name + "\nsymbolLeft:" + symbolLeft + "\noperation type:" + operation_type + "\nposition:" + position + "\nerrorId:" + errorId + "\nerrorMsg:" + errorMsg + "\ntime:" + time + "\nserver_name:" + server_name + "\nserver_ip:" + server_ip + "\ndocker_name:" + docker_name;
        }
    }
    KF_LOG_INFO(logger, "title:" << title << " content:" << content);
    m_ThreadPoolPtr->commit(std::bind(&Daemon::handle_send_mail, this, m_restapiConfig, m_restapiConfig.AccessKeySecret, m_restapiConfig.AccessKeyId, m_restapiConfig.AccountName, title, content, m_restapiConfig.ToAddress));
    KF_LOG_INFO(logger, "send email end!");
    return true;
}

string Daemon::stamp_to_standard(long stampTime)
{
    long time = stampTime / 1e9;
	time_t tick = (time_t)time;
	struct tm* tm; 
	char s[100]{};
	tm = localtime(&tick);
    sprintf(s,"%04d-%02d-%02d %02d:%02d:%02d",tm->tm_year+1900,tm->tm_mon+1,tm->tm_mday,tm->tm_hour,tm->tm_min,tm->tm_sec);
	return s;
}

void Daemon::get_server_docker(string sname, string sip, string dname)
{
    server_name = sname;
    server_ip = sip;
    docker_name = dname;
}
 void Daemon::set_restapi_config(RestapiConfig& restapi)
 {
    KF_LOG_INFO(m_logger,"set_restapi_config");
    m_restapiConfig = restapi;
 }

MONITOR_NAMESPACE_END
