//
// Created by wang on 11/18/18.
//
#include "Daemon.h"
#include "DaemonWrapper.h"
#include <boost/python.hpp>
#include <pythonrun.h>
#include <memory>
#include <csignal>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "json.hpp"
using namespace nlohmann;
using namespace boost::python;
using namespace boost;

MONITOR_NAMESPACE_START

static void signal_handler(int signum)
{
    g_signal_received = signum;
}

DaemonWrapper::DaemonWrapper()
{
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGHUP,  signal_handler);
    std::signal(SIGQUIT, signal_handler);
    std::signal(SIGKILL, signal_handler);
    m_logger = yijinjing::KfLog::getLogger("Monitor.Daemon");
    m_daemon = new Daemon(m_logger);
    
}

DaemonWrapper::~DaemonWrapper()
{
    if(m_daemon)
    {
        delete m_daemon;
        m_daemon = nullptr;
    }
}

bool DaemonWrapper::init(const string &json)
{
    if(!m_daemon)
    {
        exit(-1);
    }
    if (parseConfig(json))
    {
        if (m_daemon->init())
        {
            return true;
        }
    }
    exit(-1);
}

bool DaemonWrapper::start()
{
    if (m_daemon)
    {
        if (m_daemon->start())
        {
            return true;
        }
    }
    exit(0);
}

void DaemonWrapper::stop()
{
    if (m_daemon)
    {
        m_daemon->stop();
    }
}

void DaemonWrapper::wait()
{
    if (m_daemon)
    {
        m_daemon->wait();
    }
}

bool DaemonWrapper::parseConfig(const std::string &json)
{
    nlohmann::json jsonConfig = nlohmann::json::parse(json);
    auto localHost = parseCsv(jsonConfig["localHost"].get<std::string>(), ":");
    if (localHost.size() != 2)
    {
        KF_LOG_INFO(m_logger, "parse daemon local host error,must be xxx.xxx.xxx:xxx");
        return false;
    }
    g_daemon_config.ip = localHost[0];
    g_daemon_config.port = std::atoi(localHost[1].c_str());
    KF_LOG_INFO(m_logger,"parse daemon local host,ip:" << g_daemon_config.ip  << ",port:" << g_daemon_config.port);
    g_daemon_config.scriptPath= jsonConfig["scriptPath"].get<std::string>();
    KF_LOG_INFO(m_logger,"parse daemon script path:" << g_daemon_config.scriptPath);
    auto whiteList = parseCsv(jsonConfig["whiteList"].get<std::string>(), ",");
    string server_name = jsonConfig["server_name"].get<std::string>();
    KF_LOG_INFO(m_logger,"parse daemon server name:" << server_name);
    string server_ip = jsonConfig["server_ip"].get<std::string>();
    KF_LOG_INFO(m_logger,"parse daemon server ip:" << server_ip);
    string docker_name = jsonConfig["docker_name"].get<std::string>();
    KF_LOG_INFO(m_logger,"parse daemon docker name:" << docker_name);
    m_daemon->get_server_docker(server_name, server_ip, docker_name);
    g_daemon_config.whiteList.insert(whiteList.begin(), whiteList.end());

    if(jsonConfig.find("restapi") == jsonConfig.end())
    {
        KF_LOG_INFO(m_logger, "parse daemon config error:no restapi info");
        return false;
    }
    auto restapi = jsonConfig["restapi"].get<nlohmann::json>(); 
    if(restapi.is_object())
    {
        KF_LOG_INFO(m_logger, "parse restapi info");
        RestapiConfig restapiConfig;
        restapiConfig.AccessKeySecret = restapi["AccessKeySecret"].get<string>();
        restapiConfig.AccessKeyId = restapi["AccessKeyId"].get<string>();
        restapiConfig.AccountName = restapi["AccountName"].get<string>();

        if(jsonConfig.find("tolerate_error") != jsonConfig.end()){
            nlohmann::json tolerate_error = jsonConfig["tolerate_error"].get<nlohmann::json>();
            if(tolerate_error.is_object()){
                for (nlohmann::json::iterator it = tolerate_error.begin(); it != tolerate_error.end(); ++it){
                    KF_LOG_INFO(m_logger,"it.key()"<<it.key()<<" it.value()"<<it.value());
                    std::string erroridstr = it.key();
                    char_64 cerrorid;
                    strcpy(cerrorid,erroridstr.c_str());
                    int errorid = stoi(erroridstr);
                    TolerateMsg toleratemsg;
                    toleratemsg.time_s = tolerate_error[cerrorid]["time_s"].get<int>();
                    toleratemsg.count = tolerate_error[cerrorid]["count"].get<int>();
                    restapiConfig.TolerateMap.insert(make_pair(errorid,toleratemsg));                    
                }
            }
        }

        KF_LOG_INFO_FMT(m_logger, "restapi info: AccessKeySecret:%s,AccessKeyId:%s,AccountName:%s",
            restapiConfig.AccessKeySecret.c_str(),restapiConfig.AccessKeyId.c_str(),restapiConfig.AccountName.c_str());
        const nlohmann::json& emails = restapi["ToAddress"];
        for(auto& email:emails)
        {
            auto mail_addr = email.get<string>();
            restapiConfig.ToAddress.push_back(mail_addr);
            KF_LOG_INFO_FMT(m_logger, "restapi info: mail addr:%s",mail_addr.c_str());
        }
        KF_LOG_INFO(m_logger,"m_daemon");
        if(m_daemon == nullptr){
            KF_LOG_INFO(m_logger,"m_daemon1");
        }
        m_daemon->set_restapi_config(restapiConfig);
    }
    else
    {
        KF_LOG_INFO(m_logger, "parse daemon config error:read restapi info error");
        return false;
    }
    
    
    return true;
}

//url -> 127.0.0.1:8989
std::vector<std::string> DaemonWrapper::parseCsv(const std::string& csv, const std::string& key)
{
    std::vector<std::string> result{};
    try
    {
        boost::split(result, csv, boost::is_any_of(key));
    }
    catch (std::exception& e)
    {
        KF_LOG_INFO(m_logger, "parse csv " << csv << " exception:"<< e.what());
    }
    return std::move(result);
}


BOOST_PYTHON_MODULE(libmonitordaemon)
{
    class_<DaemonWrapper, std::shared_ptr<DaemonWrapper>>("Monitor")
            .def(init<>())
            .def("init", &DaemonWrapper::init)
            .def("start", &DaemonWrapper::start)
            .def("stop", &DaemonWrapper::stop)
            .def("wait_for_stop", &DaemonWrapper::wait);
}
MONITOR_NAMESPACE_END