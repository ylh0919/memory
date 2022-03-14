#include "MDEngineKuCoin.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/LFDataStruct.h"

#include <writer.h>
#include <stringbuffer.h>
#include <document.h>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <string>
#include <cpr/cpr.h>
#include <chrono>
#include <ctime>
#include <queue>
using cpr::Get;
using cpr::Url;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;

using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using std::string;
using std::to_string;
using std::stod;
using std::stoi;


USING_WC_NAMESPACE
std::mutex kline_mutex;
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;
std::mutex update_mutex;
std::mutex book_mutex;
struct SymbolTask
{
    SymbolTask()
    {
    }
    SymbolTask(const SymbolTask& source)
    {
    }
    std::mutex symbol_mutex;
    std::queue<std::string> queueTask;
};
std::map<std::string,SymbolTask> mapSymbolTask;
static MDEngineKuCoin* global_md = nullptr;

static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{
    std::stringstream ss;
    ss << "lws_callback,reason=" << reason << ",";
    switch( reason )
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            ss << "LWS_CALLBACK_CLIENT_ESTABLISHED.";
            global_md->writeErrorLog(ss.str());
            //lws_callback_on_writable( wsi );
            break;
        }
        case LWS_CALLBACK_PROTOCOL_INIT:
        {
             ss << "LWS_CALLBACK_PROTOCOL_INIT.";
            global_md->writeErrorLog(ss.str());
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {
             ss << "LWS_CALLBACK_CLIENT_RECEIVE.";
            global_md->writeErrorLog(ss.str());
            if(global_md)
            {
                global_md->on_lws_data(wsi, (const char*)in, len);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {
            ss << "LWS_CALLBACK_CLIENT_WRITEABLE.";
            global_md->writeErrorLog(ss.str());
            int ret = 0;
            if(global_md)
            {
                ret = global_md->lws_write_subscribe(wsi);
            }
            break;
        }
        case LWS_CALLBACK_CLOSED:
        {
           // ss << "LWS_CALLBACK_CLOSED.";
           // global_md->writeErrorLog(ss.str());
           // break;
        }
        case LWS_CALLBACK_WSI_DESTROY:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
           // ss << "LWS_CALLBACK_CLIENT_CONNECTION_ERROR.";
            global_md->writeErrorLog(ss.str());
             if(global_md)
            {
                global_md->on_lws_connection_error(wsi);
            }
            break;
        }
        default:
              global_md->writeErrorLog(ss.str());
            break;
    }

    return 0;
}

static struct lws_protocols protocols[] =
    {
            {
                    "md-protocol",
                    ws_service_cb,
                          0,
                             65536,
            },
            { NULL, NULL, 0, 0 } /* terminator */
    };


enum protocolList {
    PROTOCOL_TEST,

    PROTOCOL_LIST_COUNT
};

struct session_data {
    int fd;
};

MDEngineKuCoin::MDEngineKuCoin(): IMDEngine(SOURCE_KUCOIN)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.KuCoin");
    m_mutexPriceBookData = new std::mutex;
    m_ThreadPoolPtr = nullptr;
}

MDEngineKuCoin::~MDEngineKuCoin()
{
   if(m_mutexPriceBookData)
   {
       delete m_mutexPriceBookData;
   }
   if(m_ThreadPoolPtr != nullptr) delete m_ThreadPoolPtr;
}

void MDEngineKuCoin::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}

void MDEngineKuCoin::load(const json& j_config)
{
    KF_LOG_ERROR(logger, "MDEngineKuCoin::load:");
    /*edited by zyy,starts here*/
    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    /*edited by zyy ends here*/
    if(j_config.find("bar_duration_s") != j_config.end()) {
        bar_duration_s = j_config["bar_duration_s"].get<int>();
    }
    if(j_config.find("snapshot_check_s") != j_config.end()) {
        snapshot_check_s = j_config["snapshot_check_s"].get<int>();
    }

    auto iter = j_config.find("price_range");
    if (iter != j_config.end() && iter.value().size() > 0){
        for (auto& j_account: iter.value()){
            PriceRange pricerange;
            std::string ticker = j_account["ticker"].get<string>();
            pricerange.min_price = j_account["min_price"].get<int64_t>();
            pricerange.max_price = j_account["max_price"].get<int64_t>();
            price_range_map.insert(make_pair(ticker, pricerange));
        }
    }
    KF_LOG_INFO(logger,"price_range_map.size()="<<price_range_map.size());

    auto iter1 = j_config.find("message_type");//"message_type":[105,106,110]
    if (iter1 != j_config.end() && iter1.value().size() > 0){
        for (auto& j_account1: iter1.value()){
            int type = j_account1.get<int>();
            message_type_map.insert(std::make_pair(type, 1));
        }
    }
    KF_LOG_INFO(logger,"message_type_map.size()="<<message_type_map.size());
    int thread_pool_size = 0;
    if(j_config.find("thread_pool_size") != j_config.end()) {
        thread_pool_size = j_config["thread_pool_size"].get<int>();
    }
    KF_LOG_INFO(logger, "thread_pool_size:" << thread_pool_size);
    if(thread_pool_size > 0)
    {
        m_ThreadPoolPtr = new ThreadPoolOfMultiTask(thread_pool_size);
    }
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    book_depth_count = j_config["book_depth_count"].get<int>();
    rest_try_count = j_config["rest_try_count"].get<int>();
    readWhiteLists(j_config);

    debug_print(keyIsStrategyCoinpairWhiteList);
    //display usage:
    if(keyIsStrategyCoinpairWhiteList.size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineKuCoin::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }

    int64_t nowTime = getTimestamp();
    std::map<std::string, std::string>::iterator it;
    for(it = keyIsStrategyCoinpairWhiteList.begin();it != keyIsStrategyCoinpairWhiteList.end();it++)
    {
        std::unique_lock<std::mutex> lck(book_mutex);
        control_book_map.insert(make_pair(it->first, nowTime));
        lck.unlock();
    }
}

void MDEngineKuCoin::readWhiteLists(const json& j_config)
{
    KF_LOG_INFO(logger, "[readWhiteLists]");

    if(j_config.find("whiteLists") != j_config.end()) {
        KF_LOG_INFO(logger, "[readWhiteLists] found whiteLists");
        //has whiteLists
        json whiteLists = j_config["whiteLists"].get<json>();
        if(whiteLists.is_object())
        {
            for (json::iterator it = whiteLists.begin(); it != whiteLists.end(); ++it) {
                    std::string strategy_coinpair = it.key();
                    std::string exchange_coinpair = it.value();
                    KF_LOG_INFO(logger, "[readWhiteLists] (strategy_coinpair) " << strategy_coinpair << " (exchange_coinpair) " << exchange_coinpair);
                    keyIsStrategyCoinpairWhiteList.insert(std::pair<std::string, std::string>(strategy_coinpair, exchange_coinpair));
                    mapSymbolTask.insert(std::make_pair(exchange_coinpair,SymbolTask()));
                    auto it2 = message_type_map.find(106);
                    if(it2 != message_type_map.end()){
                        m_vstrSubscribeJsonString.push_back(makeSubscribeL2Update(exchange_coinpair));
                    }
                    auto it1 = message_type_map.find(105);
                    if(it1 != message_type_map.end()){
                        m_vstrSubscribeJsonString.push_back(makeSubscribeMatch(exchange_coinpair));
                    }
            }
        }
    }
}

std::string MDEngineKuCoin::getWhiteListCoinpairFrom(std::string md_coinpair)
{
    std::string& ticker = md_coinpair;
    //std::transform(ticker.begin(), ticker.end(), ticker.begin(), ::toupper);

    KF_LOG_INFO(logger, "[getWhiteListCoinpairFrom] find md_coinpair (md_coinpair) " << md_coinpair << " (toupper(ticker)) " << ticker);
    std::map<std::string, std::string>::iterator map_itr;
    map_itr = keyIsStrategyCoinpairWhiteList.begin();
    while(map_itr != keyIsStrategyCoinpairWhiteList.end()) {
        if(ticker == map_itr->second)
        {
            KF_LOG_INFO(logger, "[getWhiteListCoinpairFrom] found md_coinpair (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);
            return map_itr->first;
        }
        map_itr++;
    }
    KF_LOG_INFO(logger, "[getWhiteListCoinpairFrom] not found md_coinpair (md_coinpair) " << md_coinpair);
    return "";
}

void MDEngineKuCoin::debug_print(std::map<std::string, std::string> &keyIsStrategyCoinpairWhiteList)
{
    std::map<std::string, std::string>::iterator map_itr;
    map_itr = keyIsStrategyCoinpairWhiteList.begin();
    while(map_itr != keyIsStrategyCoinpairWhiteList.end()) {
        KF_LOG_INFO(logger, "[debug_print] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (md_coinpair) "<< map_itr->second);
        map_itr++;
    }
}

void MDEngineKuCoin::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineKuCoin::connect:");
    connected = true;
}

bool MDEngineKuCoin::getToken(Document& d) 
{
    int nTryCount = 0;
    cpr::Response response;
    do{
        std::string url = "https://api.kucoin.com/api/v1/bullet-public";
       response = Post(Url{url.c_str()}, Parameters{}); 
       
    }while(++nTryCount < rest_try_count && response.status_code != 200);

    if(response.status_code != 200)
    {
        KF_LOG_ERROR(logger, "MDEngineKuCoin::login::getToken Error");
        return false;
    }

    KF_LOG_INFO(logger, "MDEngineKuCoin::getToken: " << response.text.c_str());

    d.Parse(response.text.c_str());
    return true;
}


bool MDEngineKuCoin::getServers(Document& d)
{
    m_vstServerInfos.clear();
    m_strToken = "";
     if(d.HasMember("data"))
     {
         auto& data = d["data"];
         if(data.HasMember("token"))
         {
             m_strToken = data["token"].GetString();
             if(data.HasMember("instanceServers"))
             {
                 int nSize = data["instanceServers"].Size();
                for(int nPos = 0;nPos<nSize;++nPos)
                {
                    ServerInfo stServerInfo;
                    auto& server = data["instanceServers"].GetArray()[nPos];
                    if(server.HasMember("pingInterval"))
                    {
                        stServerInfo.nPingInterval = server["pingInterval"].GetInt();
                    }
                    if(server.HasMember("pingTimeOut"))
                    {
                        stServerInfo.nPingTimeOut = server["pingTimeOut"].GetInt();
                    }
                    if(server.HasMember("endpoint"))
                    {
                        stServerInfo.strEndpoint = server["endpoint"].GetString();
                    }
                    if(server.HasMember("protocol"))
                    {
                        stServerInfo.strProtocol = server["protocol"].GetString();
                    }
                    if(server.HasMember("encrypt"))
                    {
                        stServerInfo.bEncrypt = server["encrypt"].GetBool();
                    }
                    m_vstServerInfos.push_back(stServerInfo);
                }
             }
         }
     }
    if(m_strToken == "" || m_vstServerInfos.empty())
    {
        KF_LOG_ERROR(logger, "MDEngineKuCoin::login::getServers Error");
        return false;
    }
    return true;
}

std::string MDEngineKuCoin::getId()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  std::to_string(timestamp);
}

int64_t MDEngineKuCoin::getMSTime()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  timestamp;
}

void MDEngineKuCoin::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineKuCoin::login:");

    Document d;
    if(!getToken(d))
    {
        return;
    }
    if(!getServers(d))
   {
       return;
   }
    m_nSubscribePos = 0;
    global_md = this;
    int inputPort = 8443;
    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;

    struct lws_context_creation_info ctxCreationInfo;
    struct lws_client_connect_info clientConnectInfo;
    struct lws *wsi = NULL;
    struct lws_protocols protocol;

    memset(&ctxCreationInfo, 0, sizeof(ctxCreationInfo));
    memset(&clientConnectInfo, 0, sizeof(clientConnectInfo));

    ctxCreationInfo.port = CONTEXT_PORT_NO_LISTEN;
    ctxCreationInfo.iface = NULL;
    ctxCreationInfo.protocols = protocols;
    ctxCreationInfo.ssl_cert_filepath = NULL;
    ctxCreationInfo.ssl_private_key_filepath = NULL;
    ctxCreationInfo.extensions = NULL;
    ctxCreationInfo.gid = -1;
    ctxCreationInfo.uid = -1;
    ctxCreationInfo.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    ctxCreationInfo.fd_limit_per_thread = 1024;
    ctxCreationInfo.max_http_header_pool = 1024;
    ctxCreationInfo.ws_ping_pong_interval=1;
    ctxCreationInfo.ka_time = 10;
    ctxCreationInfo.ka_probes = 10;
    ctxCreationInfo.ka_interval = 10;

    protocol.name  = protocols[PROTOCOL_TEST].name;
    protocol.callback = &ws_service_cb;
    protocol.per_session_data_size = sizeof(struct session_data);
    protocol.rx_buffer_size = 0;
    protocol.id = 0;
    protocol.user = NULL;

    context = lws_create_context(&ctxCreationInfo);
    KF_LOG_INFO(logger, "MDEngineKuCoin::login: context created.");


    if (context == NULL) {
        KF_LOG_ERROR(logger, "MDEngineKuCoin::login: context is NULL. return");
        return;
    }

    // Set up the client creation info
    auto& stServerInfo = m_vstServerInfos.front();
    std::string strAddress = stServerInfo.strEndpoint;
    size_t nAddressEndPos = strAddress.find_last_of('/');
    std::string strPath = strAddress.substr(nAddressEndPos);
    strPath += "?token=";
    strPath += m_strToken;
    strPath += "&[connectId=" +  getId() +"]";
    strAddress = strAddress.substr(0,nAddressEndPos);
    strAddress = strAddress.substr(strAddress.find_last_of('/') + 1);
    clientConnectInfo.address = strAddress.c_str();
    clientConnectInfo.path = strPath.c_str(); // Set the info's path to the fixed up url path
    clientConnectInfo.context = context;
    clientConnectInfo.port = 443;
    clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    clientConnectInfo.host =strAddress.c_str();
    clientConnectInfo.origin = strAddress.c_str();
    clientConnectInfo.ietf_version_or_minus_one = -1;
    clientConnectInfo.protocol = protocols[PROTOCOL_TEST].name;
    clientConnectInfo.pwsi = &wsi;

    KF_LOG_INFO(logger, "MDEngineKuCoin::login: address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);

    wsi = lws_client_connect_via_info(&clientConnectInfo);
    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "MDEngineKuCoin::login: wsi create error.");
        return;
    }
    KF_LOG_INFO(logger, "MDEngineKuCoin::login: wsi create success.");
    logged_in = true;
    timer = getTimestamp();
}

void MDEngineKuCoin::logout()
{
   KF_LOG_INFO(logger, "MDEngineKuCoin::logout:");
}

void MDEngineKuCoin::release_api()
{
   KF_LOG_INFO(logger, "MDEngineKuCoin::release_api:");
}

void MDEngineKuCoin::set_reader_thread()
{
    IMDEngine::set_reader_thread();
    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKuCoin::rest_loop, this)));
	ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKuCoin::loop, this)));
    kline_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKuCoin::klineloop, this)));
    check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKuCoin::check_loop, this)));
}

void MDEngineKuCoin::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
   KF_LOG_INFO(logger, "MDEngineKuCoin::subscribeMarketData:");
}

std::string MDEngineKuCoin::makeSubscribeL2Update(std::string& strSymbol)
{
    StringBuffer sbL2Update;
    Writer<StringBuffer> writer(sbL2Update);
    writer.StartObject();
    writer.Key("id");
    writer.String(getId().c_str());
    writer.Key("type");
    writer.String("subscribe");
    writer.Key("topic");
    std::string strTopic = "/market/level2:";
    strTopic += strSymbol;
    writer.String(strTopic.c_str());
    writer.Key("response");
    writer.String("true");
    writer.EndObject();
    std::string strL2Update = sbL2Update.GetString();

    return strL2Update;
}

std::string MDEngineKuCoin::makeSubscribeMatch(std::string& strSymbol)
{
     StringBuffer sbMacth;
    Writer<StringBuffer> writer1(sbMacth);
    writer1.StartObject();
    writer1.Key("id");
    writer1.String(getId().c_str());
    writer1.Key("type");
    writer1.String("subscribe");
    writer1.Key("topic");
    std::string strTopic1 = "/market/match:";
    strTopic1 += strSymbol;
    writer1.String(strTopic1.c_str());
    writer1.Key("privateChannel");
    writer1.String("false");
    writer1.Key("response");
    writer1.String("true");
    writer1.EndObject();
    std::string strLMatch = sbMacth.GetString();

    return strLMatch;
}

int MDEngineKuCoin::lws_write_subscribe(struct lws* conn)
{
    //KF_LOG_INFO(logger, "MDEngineKuCoin::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    if(keyIsStrategyCoinpairWhiteList.size() == 0) return 0;
    int ret = 0;
    if(m_nSubscribePos < m_vstrSubscribeJsonString.size())
    {
        std::string& strSubscribe = m_vstrSubscribeJsonString[m_nSubscribePos];
        unsigned char msg[512];
        memset(&msg[LWS_PRE], 0, 512-LWS_PRE);
        int length = strSubscribe.length();
        KF_LOG_INFO(logger, "MDEngineKuCoin::lws_write_subscribe: " << strSubscribe.c_str() << " ,len = " << length);
        strncpy((char *)msg+LWS_PRE, strSubscribe.c_str(), length);
        int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
        m_nSubscribePos++;
        lws_callback_on_writable(conn);
    }
    else
    {
        if(shouldPing)
        {
            isPong = false;
            Ping(conn);
        }
    }
    
    return ret;
}

std::string MDEngineKuCoin::dealDataSprit(const char* src)
{
     std::string strData = src;
     auto nPos = strData.find("\\");
     while(nPos != std::string::npos)
     {
        strData.replace(nPos,1,"");
        nPos = strData.find("\\");
     }

     return strData;
}

void MDEngineKuCoin::onPong(struct lws* conn)
{
    Ping(conn);
}

void MDEngineKuCoin::Ping(struct lws* conn)
{
     shouldPing = false;
    StringBuffer sbPing;
    Writer<StringBuffer> writer(sbPing);
    writer.StartObject();
    writer.Key("id");
    writer.String(getId().c_str());
    writer.Key("type");
    writer.String("ping");
    writer.EndObject();
    std::string strPing = sbPing.GetString();
    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512-LWS_PRE);
     int length = strPing.length();
    KF_LOG_INFO(logger, "MDEngineKuCoin::lws_write_ping: " << strPing.c_str() << " ,len = " << length);
    strncpy((char *)msg+LWS_PRE, strPing.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
}

void MDEngineKuCoin::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "MDEngineKuCoin::on_lws_data: " << data);
     Document json;
    json.Parse(data);
    if(!json.HasParseError() && json.IsObject() && json.HasMember("type") && json["type"].IsString())
    {
        std::string strType = json["type"].GetString();
        if(strType == "welcome")
        {
            KF_LOG_INFO(logger, "MDEngineKuCoin::on_lws_data: welcome");
            lws_callback_on_writable(conn);
        }
        else if(strType == "pong")
        {
            KF_LOG_INFO(logger, "MDEngineKuCoin::on_lws_data: pong");
            isPong = true;
            m_conn = conn;
        }
        else if(strType == "message" && json.HasMember("data") && json["data"].HasMember("symbol"))
        {
            auto& jsonData = json["data"];
            std::string symbol = jsonData["symbol"].GetString();
            if(nullptr == m_ThreadPoolPtr)
            {
                handle_lws_data(conn,std::string(data));
            }
            else
            {
                m_ThreadPoolPtr->commit(symbol,std::bind(&MDEngineKuCoin::handle_lws_data,this,conn,std::string(data)));
            }
        }
    }
    else 
    {
        KF_LOG_ERROR(logger, "MDEngineKuCoin::on_lws_data . parse json error: " << data);
    }
}
void MDEngineKuCoin::handle_lws_data(struct lws* conn, std::string data)
{
    KF_LOG_INFO(logger, "MDEngineKuCoin::handle_lws_data,thread_id:"<< std::this_thread::get_id());
    Document json;
    json.Parse(data.c_str());
    auto& jsonData = json["data"];
    std::string symbol = jsonData["symbol"].GetString();
    if(strcmp(json["subject"].GetString(), "trade.l2update") == 0)
    {
        KF_LOG_INFO(logger, "MDEngineKuCoin::handle_lws_data: is trade.l2update");
        onDepth(jsonData,symbol);
    }
    else if(strcmp(json["subject"].GetString(), "trade.l3match") == 0)
    {
        KF_LOG_INFO(logger, "MDEngineKuCoin::handle_lws_data: is trade.l3match");
        onFills(jsonData,symbol);
    }         
}

void MDEngineKuCoin::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "MDEngineKuCoin::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineKuCoin::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    clearPriceBook();
    isPong = false;
    shouldPing = true;
    //no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;

    login(timeout_nsec);
}

void MDEngineKuCoin::clearPriceBook()
{
    //std::lock_guard<std::mutex> lck(*m_mutexPriceBookData);
    //m_mapPriceBookData.clear();
    mapLastData.clear();
    priceBook20Assembler.clearPriceBook();
}

std::string MDEngineKuCoin::get_utc_date(int64_t timestamp)
{
    int ms = timestamp % 1000;
    tm utc_time{};
    time_t time = timestamp/1000;
    gmtime_r(&time, &utc_time);
    char timeStr[50]{};
    sprintf(timeStr, "%04d%02d%02d", utc_time.tm_year + 1900, utc_time.tm_mon + 1, utc_time.tm_mday);
    return std::string(timeStr);
}
std::string MDEngineKuCoin::get_utc_time(int64_t timestamp)
{
    int ms = timestamp % 1000;
    tm utc_time{};
    time_t time = timestamp/1000;
    gmtime_r(&time, &utc_time);
    char timeStr[50]{};
    sprintf(timeStr, "%02d:%02d:%02d.%03d", utc_time.tm_hour, utc_time.tm_min, utc_time.tm_sec,ms);
    return std::string(timeStr);
}

std::string MDEngineKuCoin::dealzero(std::string time)
{
    if(time.length()==1){
        time = "0" + time;
    }
    return time;
}

void MDEngineKuCoin::onFills(Value& jsonData,std::string& ticker)
{
    std::string strInstrumentID = getWhiteListCoinpairFrom(ticker);
    if(strInstrumentID == "")
    {
        KF_LOG_INFO(logger, "MDEngineKuCoin::onFills: invaild data " << ticker.c_str());
        return;
    }

    LFL2TradeField trade;
    memset(&trade, 0, sizeof(trade));
    strcpy(trade.InstrumentID, strInstrumentID.c_str());
    strcpy(trade.ExchangeID, "kucoin");
    std::string strTime = jsonData["time"].GetString();
    std::string strTakerOrderId = jsonData["takerOrderId"].GetString();
    std::string strMakerOrderId = jsonData["makerOrderId"].GetString();
    std::string strTradeId = jsonData["tradeId"].GetString();
    std::string strSequence = jsonData["sequence"].GetString();
    trade.TimeStamp = std::stoll(strTime);
    strTime = timestamp_to_formatISO8601(trade.TimeStamp/1000000);
    strncpy(trade.TradeTime, strTime.c_str(),sizeof(trade.TradeTime));
    strncpy(trade.MakerOrderID, strMakerOrderId.c_str(),sizeof(trade.MakerOrderID));
    strncpy(trade.TakerOrderID, strTakerOrderId.c_str(),sizeof(trade.TakerOrderID));
    strncpy(trade.TradeID, strTradeId.c_str(),sizeof(trade.TradeID));
     strncpy(trade.Sequence, strSequence.c_str(),sizeof(trade.Sequence));
    //strncpy(trade.MakerOrderID, strMakerOrderId.c_str(),sizeof(trade.MakerOrderID));
    trade.Price = std::round(std::stod(jsonData["price"].GetString()) * scale_offset);
    trade.Volume = std::round(std::stod(jsonData["size"].GetString()) * scale_offset);
    static const string strBuy = "buy" ;
    trade.OrderBSFlag[0] = (strBuy == jsonData["side"].GetString()) ? 'B' : 'S';

    KF_LOG_INFO(logger, "MDEngineKuCoin::[onFills] (ticker)" << ticker <<
                                                                " (Price)" << trade.Price <<
                                                                " (Volume)" << trade.Volume << 
                                                                "(OrderBSFlag)" << trade.OrderBSFlag);

    KF_LOG_INFO(logger,"ts="<<trade.TimeStamp);
    //std::unique_lock<std::mutex> lock(kline_mutex);
    //auto it = kline_receive_map.find(strInstrumentID);
    //if(it == kline_receive_map.end()){
    KlineData klinedata;
    klinedata.ts = trade.TimeStamp;
    klinedata.price = trade.Price;
    klinedata.volume = trade.Volume;
    //klinedata.TradingDay = getdate();
    klinedata.InstrumentID = strInstrumentID;
    klinedata.ExchangeID = "kucoin";
    /*klinedata.StartUpdateTime = gettime();
    klinedata.StartUpdateMillisec = getTimestamp();
    klinedata.EndUpdateTime = gettime();
    klinedata.EndUpdateMillisec = getTimestamp();
    klinedata.PeriodMillisec = bar_duration_s;*/
    std::unique_lock<std::mutex> lock(kline_mutex);
    kline_receive_vec.push_back(klinedata);  
    lock.unlock(); 
    /*    klinedata.Open = trade.Price;
        klinedata.Close = trade.Price;
        klinedata.Low = trade.Price;
        klinedata.High = trade.Price;
        klinedata.Volume = trade.Volume;
        kline_receive_map.insert(make_pair(strInstrumentID,klinedata));
    }else{
        it->second.ts = ts;
        it->second.EndUpdateTime = gettime();
        it->second.EndUpdateMillisec = getTimestamp();
        it->second.Close = trade.Price;
        if(trade.Price < it->second.Low){
            it->second.Low = trade.Price;
        }else if(trade.Price > it->second.High){
            it->second.High = trade.Price;
        }
        it->second.Volume += trade.Volume;
    }*/
    //lock.unlock();

    on_trade(&trade);
}

bool MDEngineKuCoin::shouldUpdateData(const LFPriceBook20Field& md)
{
    bool has_update = false;
    auto it = mapLastData.find (md.InstrumentID);
    if(it == mapLastData.end())
    {
        mapLastData[md.InstrumentID] = md;
        has_update = true;
    }
     else
     {
        LFPriceBook20Field& lastMD = it->second;
        if(md.BidLevelCount != lastMD.BidLevelCount)
        {
            has_update = true;
        }
        else
        {
            for(int i = 0;i < md.BidLevelCount; ++i)
            {
                if(md.BidLevels[i].price != lastMD.BidLevels[i].price || md.BidLevels[i].volume != lastMD.BidLevels[i].volume)
                {
                    has_update = true;
                    break;
                }
            }
        }
        if(!has_update && md.AskLevelCount != lastMD.AskLevelCount)
        {
            has_update = true;
        }
        else if(!has_update)
        {
            for(int i = 0;i < md.AskLevelCount ;++i)
            {
                if(md.AskLevels[i].price != lastMD.AskLevels[i].price || md.AskLevels[i].volume != lastMD.AskLevels[i].volume)
                {
                    has_update = true;
                    break;
                }
            }
        }
        if(has_update)
        {
             mapLastData[md.InstrumentID] = md;
        }
    }    

    return has_update;
}

bool MDEngineKuCoin::getInitPriceBook(const std::string& strSymbol,std::map<std::string,PriceBookData>::iterator& itPriceBookData)
{   //获取snapshot
    /*
    一个map中有多个pair组成
    一个pair有：pair.first(键值)
               pair.second(数值)
    struct PriceBookData
    {
        std::map<int64_t, uint64_t> mapAskPrice;
        std::map<int64_t, uint64_t> mapBidPrice;
        int64_t nSequence = -1;
    };
    在这个函数中用到的itPriceBookData就是由string作为key值，PriceBookData作为value值
    map结构:
    *******************************
    pair:n1.first:string
         n1.second:PriceBookData
    -------------------------------
    pair:n2.first:string
         n2.second:PriceBookData
    *******************************
    */
    int nTryCount = 0;
    cpr::Response response;//<response.h>
    std::string url = "https://api.kucoin.com/api/v1/market/orderbook/level2_100?symbol=";
    //std::string url = "https://api.kucoin.com/api/v2/market/orderbook/level2?symbol=";
    url += strSymbol;
    
    std::string ticker = getWhiteListCoinpairFrom(strSymbol);

    do{  
       response = Get(Url{url.c_str()}, Parameters{}); 
       
    }while(++nTryCount < rest_try_count && response.status_code != 200);

    if(response.status_code != 200)
    {
        //status_code和200比较是为什么
        KF_LOG_ERROR(logger, "MDEngineKuCoin::login::getInitPriceBook Error, response = " <<response.text.c_str());
        return false;
    }
    KF_LOG_INFO(logger, "MDEngineKuCoin::getInitPriceBook:(" << strSymbol << ")" << response.text.c_str());
    priceBook20Assembler.clearPriceBook(ticker);
    //priceBook20Assembler是PriceBook20Assembler.h
    //clearPriceBook清空本地缓存
    //每次获取一个新的snapshot就意味着清空上一次的缓存

    Document d; //document.h
    d.Parse(response.text.c_str());
    //std::map<std::string,PriceBookData> m_mapPriceBookData
    itPriceBookData = m_mapPriceBookData.insert(std::make_pair(ticker,PriceBookData())).first;
    
    if(!d.HasMember("data"))
    {
        return  true;
    }
    auto& jsonData = d["data"];
    if(jsonData.HasMember("sequence"))
    {
        itPriceBookData->second.nSequence = std::round(stod(jsonData["sequence"].GetString()));
        //round函数：四舍五入
        // stod函数：截取前面部分的浮点数，直到不满足
    }

    if(jsonData.HasMember("bids"))
    {
        auto& bids =jsonData["bids"];
         if(bids .IsArray()) 
         {
                int len = bids.Size();
                for(int i = 0 ; i < len; i++)
                {
                    int64_t price = std::round(stod(bids.GetArray()[i][0].GetString()) * scale_offset);
                    uint64_t volume = std::round(stod(bids.GetArray()[i][1].GetString()) * scale_offset);
                    priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
                   //itPriceBookData->second.mapBidPrice[price] = volume;
                }
         }
    }
    if(jsonData.HasMember("asks"))
    {
        auto& asks =jsonData["asks"];
         if(asks .IsArray()) 
         {
                int len = asks.Size();
                for(int i = 0 ; i < len; i++)
                {
                    int64_t price = std::round(stod(asks.GetArray()[i][0].GetString()) * scale_offset);
                    uint64_t volume = std::round(stod(asks.GetArray()[i][1].GetString()) * scale_offset);
                    priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
                   //itPriceBookData->second.mapAskPrice[price] = volume;
                }
         }
    }
    KF_LOG_INFO(logger,"ticker="<<ticker);
     //printPriceBook(itPriceBookData->second);

    return true;
}

void MDEngineKuCoin::printPriceBook(const PriceBookData& stPriceBookData)
{
    std::stringstream ss;
    ss << "Bids[";
    for(auto it = stPriceBookData.mapBidPrice.rbegin(); it != stPriceBookData.mapBidPrice.rend();++it)
    {
        ss <<  "[" << it->first << "," << it->second << "],";
    }
    ss << "],Ask[";
     for(auto& pair : stPriceBookData.mapAskPrice)
    {
        ss <<  "[" << pair.first << "," << pair.second << "],";
    }
    ss << "].";

    // KF_LOG_INFO(logger, "MDEngineKuCoin::printPriceBook: " << ss.str());
}


void MDEngineKuCoin::onDepth(Value& jsonData,std::string& symbol )//每一条的level-2数据都会进一下这个函数
{
    bool update = false;
    std::string ticker;
    ticker = getWhiteListCoinpairFrom(symbol);
    if(ticker.length() == 0) {
        KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: invaild data");
        return;
    }
    
    KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:" << "(ticker) " << ticker);

    std::lock_guard<std::mutex> lck(*m_mutexPriceBookData);
    auto itPriceBook = m_mapPriceBookData.find(ticker);//判断本地有没有ticker的orderbook，ticker就是币对
    if(itPriceBook == m_mapPriceBookData.end())        //如果没有就进来这个if分支
    {
        if(!getInitPriceBook(symbol,itPriceBook))
        {
            return;
        }
    }
    if(jsonData.HasMember("sequenceStart") && jsonData.HasMember("sequenceEnd"))
    {
        auto sequenceStart = jsonData["sequenceStart"].GetInt64();
        auto sequenceEnd = jsonData["sequenceEnd"].GetInt64();
        /*
        *下面两种情况都是不对数据进行处理的条件，第一种是因为丢包，导致数据不是线性的，这种情况直接获取下一个snapshot
        *然后继续执行下去，不能return退出。所以这里给出就是循环执行判断sequence+1和sequencestart、sequenceEnd的比较
        *如果是丢包就获取新的snapshot然后继续上面的比较过程，如果是处理过的，那就直接退出即可
        */
        bool flag = 0;
        bool msgSend = false;
        while(1)
        {
            //丢数据了，不执行，重新获取snapshot，但是不应退出
            
            if(itPriceBook->second.nSequence + 1 <  sequenceStart)
            {
                //开始丢包之后需要明确自己确实进入到了丢包的这一步，即是发错误到自己的邮箱，下面两行代码完成
                string errorMsg = "Orderbook update sequence missed, request for a new snapshot."+ symbol;
                if (!msgSend)
                {
                    write_errormsg(5,errorMsg);
                    msgSend = true;
                }
                KF_LOG_ERROR(logger, "lichengyi:Orderbook update missing"<< itPriceBook->second.nSequence<<"-" << sequenceStart);

                if(!getInitPriceBook(symbol,itPriceBook))
                {
                    //如果是由于丢包，需要获取新的snapshot，如果失败才返回
                    KF_LOG_ERROR(logger, "lichengyi:get a new snapshot"<< itPriceBook->second.nSequence<<"-" << sequenceStart);
                    return;
                }
            }
            //处理过了，不处理
            else if (itPriceBook->second.nSequence >= sequenceEnd)
            {
                KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:  old data,last sequence:" << itPriceBook->second.nSequence << ">= now sequenceEnd:" << sequenceEnd);
                return;
            }
            else
            {
                KF_LOG_INFO(logger, "lichengyi:No error" << itPriceBook->second.nSequence << ">= now sequenceEnd:" << sequenceEnd);
                flag = 1;
            }
            if(flag == 1)
            {
                KF_LOG_INFO(logger, "lichengyi:No error can break" << itPriceBook->second.nSequence << ">= now sequenceEnd:" << sequenceEnd);
                break;
            }
        }
    }    

    else
    {
        KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:  no sequenceStart or sequenceEnd");
        return;
    }
    
    if(jsonData.HasMember("changes"))
    {
        if(jsonData["changes"].HasMember("asks")) 
        {      
            auto& asks = jsonData["changes"]["asks"];
            if(asks .IsArray()) {
                int len = asks.Size();
                for(int i = 0 ; i < len; i++)
                {
                    int64_t nSequence = std::round(stod(asks.GetArray()[i][2].GetString()));
                    if(nSequence <= itPriceBook->second.nSequence)
                    {
                        KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:  old ask data,nSequence=" << nSequence);
                        continue;
                    }
                    update = true;
                    int64_t price = std::round(stod(asks.GetArray()[i][0].GetString()) * scale_offset);
                    auto it = price_range_map.find(ticker);
                    if(it != price_range_map.end()){
                        if(price < it->second.min_price || price > it->second.max_price){
                            continue;
                        }
                    }
                    uint64_t volume = std::round(stod(asks.GetArray()[i][1].GetString()) * scale_offset);
                    if(volume == 0 ) {
                       // KF_LOG_INFO(logger,"ask erase");
                        priceBook20Assembler.EraseAskPrice(ticker, price);
                    }
                    else {
                        //KF_LOG_INFO(logger,"ask update");
                        priceBook20Assembler.UpdateAskPrice(ticker,price,volume);
                    }
                }
            }
            else {KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:  asks not Array");}
        }
        else { KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:  asks not found");}

        if(jsonData["changes"].HasMember("bids")) 
        {      
            auto& bids = jsonData["changes"]["bids"];
            if(bids .IsArray()) 
            {
                int len = bids.Size();
                for(int i = 0 ; i < len; i++)
                {
                    int64_t nSequence = std::round(stod(bids.GetArray()[i][2].GetString()));
                    if(nSequence <= itPriceBook->second.nSequence)
                    {
                        KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:  old bid data,nSequence=" << nSequence);
                        continue;
                    }
                    update = true;
                    int64_t price = std::round(stod(bids.GetArray()[i][0].GetString()) * scale_offset);
                    auto it = price_range_map.find(ticker);
                    if(it != price_range_map.end()){
                        if(price < it->second.min_price || price > it->second.max_price){
                            continue;
                        }
                    }
                    uint64_t volume = std::round(stod(bids.GetArray()[i][1].GetString()) * scale_offset);
                    if(volume == 0){
                         //KF_LOG_INFO(logger,"bid erase");
                        priceBook20Assembler.EraseBidPrice(ticker, price);
                    }
                    else {
                        //KF_LOG_INFO(logger,"bid update");
                        priceBook20Assembler.UpdateBidPrice(ticker,price,volume);
                    }
                }
            }
            else {KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:  bids not Array");}
        } else { KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:  bids not found");}
    }
    else
    {
          KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:  data not found");
    }
    
     //printPriceBook(itPriceBook->second);
    if(update){
        itPriceBook->second.nSequence = std::round(jsonData["sequenceEnd"].GetInt64());
        KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth:  sequenceEnd = " << itPriceBook->second.nSequence);
    }

    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));
    strcpy(md.ExchangeID, "kucoin");
    if(priceBook20Assembler.Assembler(ticker, md))
    {
        md.UpdateMicroSecond = itPriceBook->second.nSequence;
        KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: on_price_book_update," << ticker );
        /*edited by zyy,starts here*/
        //timer = getTimestamp();
        KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: wait to lock book_mutex");
        std::unique_lock<std::mutex> lck(book_mutex);
        KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: book_mutex lock");
        auto it = control_book_map.find(ticker);
        if(it != control_book_map.end())
        {
            it->second = getTimestamp();
        }
        lck.unlock();
        KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: book_mutex unlock");

        if(md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold){    
            KF_LOG_INFO(logger, "BidLevelCount="<<md.BidLevelCount<<",AskLevelCount="<<md.AskLevelCount<<",level_threshold="<<level_threshold);    
            string errorMsg = "orderbook level below threshold";
            write_errormsg(112,errorMsg);
            //on_price_book_update(&md);
        }
        else if (md.BidLevels[0].price <=0 || md.AskLevels[0].price <=0 || md.BidLevels[0].price >= md.AskLevels[0].price){
            KF_LOG_INFO(logger, "order book crossed");
            string errorMsg = "orderbook crossed";
            md.Status = 2;
            write_errormsg(113,errorMsg);
        }else{
            /*KF_LOG_INFO(logger,"sequence1="<<itPriceBook->second.nSequence);
            for(int i = 0; i < 20; i++){
                KF_LOG_INFO(logger,"bidprice="<<md.BidLevels[i].price<<" bisvol="<<md.BidLevels[i].volume<<
                    " askprice="<<md.AskLevels[i].price<<" askvol="<<md.AskLevels[i].volume);
            }*/
            BookMsg bookmsg;
            bookmsg.book = md;
            bookmsg.time = getTimestamp();
            bookmsg.sequence = itPriceBook->second.nSequence;

            KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: wait to lock ws_book_mutex");
            std::unique_lock<std::mutex> lck1(ws_book_mutex);
            KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: lock ws_book_mutex now");
            auto itr = ws_book_map.find(ticker);

            // fixed here
            // 在这里把检查所有记录改成只检查最后一条
            bookmsg.isChecked = false;
            if(itr == ws_book_map.end()){
                //std::vector<BookMsg> bookmsg_vec;
                //bookmsg_vec.push_back(bookmsg);                
                //ws_book_map.insert(make_pair(ticker, bookmsg_vec));
                ws_book_map.insert(make_pair(ticker, bookmsg));
                KF_LOG_INFO(logger,"insert:"<<ticker);
            }
            else{
                itr->second = bookmsg;
            }            

            lck1.unlock();
            KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: ws_book_mutex unlock");
            /*std::unique_lock<std::mutex> lck2(update_mutex);
            auto it = has_bookupdate_map.find(ticker);
            if(it == has_bookupdate_map.end()){
                KF_LOG_INFO(logger,"insert"<<ticker);
                has_bookupdate_map.insert(make_pair(ticker, md.UpdateMicroSecond));
            }
            lck2.unlock();*/
            KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: wait to lock book_mutex");
            std::unique_lock<std::mutex> lck4(book_mutex);
            book_map[ticker] = bookmsg;
            lck4.unlock();
            KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: book_mutex unlock");
        }
        //KF_LOG_INFO(logger, "write order book to journal");
        KF_LOG_INFO(logger,"ws sequence="<<md.UpdateMicroSecond);
        on_price_book_update(&md);
    }else {
        KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: same data not update" );
        if(update){

            KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: wait to lock book_mutex");
            std::unique_lock<std::mutex> lck4(book_mutex);
            KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: lock book_mutex now");

            auto it = book_map.find(ticker);
            if(it != book_map.end()){
                BookMsg bookmsg;
                bookmsg.book = it->second.book;
                bookmsg.time = getTimestamp();
                bookmsg.sequence = itPriceBook->second.nSequence;

                KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: wait to lock ws_book_mutex");
                std::unique_lock<std::mutex> lck1(ws_book_mutex);
                KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: lock ws_book_mutex now");

                // fixed here
                // 在这里把检查所有记录改成只检查最后一条
                auto itr = ws_book_map.find(ticker);
                if(itr != ws_book_map.end()){
                    //itr->second.push_back(bookmsg);
                    bookmsg.isChecked = false;
                    itr->second = bookmsg;
                }

                lck1.unlock(); 
                KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: ws_book_mutex unlock");
                /*KF_LOG_INFO(logger,"sequence3="<<itPriceBook->second.nSequence);
                for(int i = 0; i < 20; i++){
                    KF_LOG_INFO(logger,"bidprice="<<bookmsg.book.BidLevels[i].price<<" bisvol="<<bookmsg.book.BidLevels[i].volume<<
                        " askprice="<<bookmsg.book.AskLevels[i].price<<" askvol="<<bookmsg.book.AskLevels[i].volume);
                } */
            }
            lck4.unlock();
            KF_LOG_INFO(logger, "MDEngineKuCoin::onDepth: book_mutex unlock");
            /*BookMsg bookmsg;
            bookmsg.book = md;
            bookmsg.time = getTimestamp();
            bookmsg.sequence = itPriceBook->second.nSequence;
            std::unique_lock<std::mutex> lck1(ws_book_mutex);
            ws_book_vec.push_back(bookmsg);
            lck1.unlock(); */  
        }    
    }
}

std::string MDEngineKuCoin::parseJsonToString(const char* in)
{
    Document d;
    d.Parse(reinterpret_cast<const char*>(in));

    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}

int64_t MDEngineKuCoin::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void MDEngineKuCoin::handle_kline(int64_t time)
{
    KF_LOG_INFO(logger,"handle_kline");
    std::map<std::string, KlineData>::iterator it;
    for(it = kline_hand_map.begin();it != kline_hand_map.end();it++){
        KF_LOG_INFO(logger,"it in");
        LFBarMarketDataField market;
        memset(&market, 0, sizeof(market));
        strcpy(market.InstrumentID, it->second.InstrumentID.c_str());
        strcpy(market.ExchangeID, "kucoin");

        int64_t startmsec = (time - bar_duration_s)*1000;
        std::string date = get_utc_date(startmsec);
        std::string starttime = get_utc_time(startmsec);
        int64_t endmsec = time * 1000 - 1;
        std::string endtime = get_utc_time(endmsec);
        strcpy(market.TradingDay, date.c_str());
        strcpy(market.StartUpdateTime, starttime.c_str());
        strcpy(market.EndUpdateTime, endtime.c_str());

        market.StartUpdateMillisec = startmsec;
        market.EndUpdateMillisec = endmsec;
        market.PeriodMillisec = it->second.PeriodMillisec;
        market.Open = it->second.Open;
        market.Close = it->second.Close;
        market.Low = it->second.Low;
        market.High = it->second.High;
        market.Volume = it->second.Volume;
        KF_LOG_INFO(logger,"market="<<market.StartUpdateMillisec<<" "<<market.EndUpdateMillisec<<" "<<market.PeriodMillisec);
        KF_LOG_INFO(logger,market.Open<<" "<<market.Close<<" "<<market.Low<<" "<<market.High<<" "<<market.Volume);
        KF_LOG_INFO(logger,std::string(market.TradingDay)<<" "<<std::string(market.StartUpdateTime));
        //last_market = market;
        on_market_bar_data(&market);
        //it = kline_hand_map.erase(it);
        //KF_LOG_INFO(logger,"erase it");
    }
    KF_LOG_INFO(logger,"handle_kline end");    
}

void MDEngineKuCoin::control_kline(bool first_kline,int64_t time)
{
    KF_LOG_INFO(logger,"control_kline");
    std::unique_lock<std::mutex> lock(kline_mutex);
    int size = kline_receive_vec.size();
    if(size == 0 && !first_kline){
        KF_LOG_INFO(logger,"receive no kline");
        if(kline_hand_map.size() > 0){
            handle_kline(time);
        }
        /*if(last_market.PeriodMillisec > 0){
            on_market_bar_data(&last_market);
        }*/
        //write_errormsg(114,"kline max refresh wait time exceeded");
    }else if(size > 0){
        KF_LOG_INFO(logger,"update kline");
        bool update = false;
        //kline_hand_map.clear();
        std::vector<KlineData>::iterator it;
        for(it = kline_receive_vec.begin();it != kline_receive_vec.end();it++){
            if(it->ts >= (time - bar_duration_s)*1e9 && it->ts < time*1e9){
                update = true;
                auto it1 = kline_hand_map.find(it->InstrumentID);
                if(it1 != kline_hand_map.end()){
                    kline_hand_map.erase(it1);
                }
            }
        }

        for(it = kline_receive_vec.begin();it != kline_receive_vec.end();){ 
            int64_t tradetime = it->ts;
            if(tradetime >= (time - bar_duration_s)*1e9 && tradetime < time*1e9){
                /*auto it1 = kline_hand_map.find(it->InstrumentID);
                if(it1 != kline_hand_map.end()){
                    kline_hand_map.erase(it1);
                }
                update = true;*/
                auto itr = kline_hand_map.find(it->InstrumentID);
                if(itr == kline_hand_map.end()){
                    KlineData klinedata;
                    //klinedata.TradingDay = getdate(time - bar_duration_s);
                    klinedata.InstrumentID = it->InstrumentID;
                    klinedata.ExchangeID = "kucoin";
                    //klinedata.StartUpdateTime = gettime(time - bar_duration_s);
                    //klinedata.StartUpdateMillisec = (time - bar_duration_s)*1000;
                    //klinedata.EndUpdateTime = gettime(time);
                    //klinedata.EndUpdateMillisec = time * 1000 - 1;
                    klinedata.PeriodMillisec = bar_duration_s * 1000;
                    klinedata.Open = it->price;
                    klinedata.Close = it->price;
                    klinedata.Low = it->price;
                    klinedata.High = it->price;
                    klinedata.Volume = it->volume;
                    kline_hand_map.insert(make_pair(it->InstrumentID,klinedata));
                }else{
                    //KF_LOG_INFO(logger,"else");
                    //itr->second.EndUpdateTime = it->EndUpdateTime;
                    //itr->second.EndUpdateMillisec = it->EndUpdateMillisec;
                    itr->second.Close = it->price;
                    if(it->price < itr->second.Low){
                        //KF_LOG_INFO(logger,"low");
                        itr->second.Low = it->price;
                    }else if(it->price > itr->second.High){
                        //KF_LOG_INFO(logger,"high");
                        itr->second.High = it->price;
                    }
                    itr->second.Volume += it->volume;
                }
                it = kline_receive_vec.erase(it);
            }else{
                it++;
            }
        }
        if(update){
            handle_kline(time);
        }else{
            if(kline_hand_map.size() > 0){
                handle_kline(time);
            }
            //write_errormsg(114,"kline max refresh wait time exceeded");            
        }
    }
    lock.unlock();
}
void MDEngineKuCoin::get_snapshot_via_rest()
{
    KF_LOG_INFO(logger, "MDEngineKuCoin::get_snapshot_via_rest ");
    //while (isRunning)
    //{
        for(const auto& item : keyIsStrategyCoinpairWhiteList)
        {
            std::string url = "https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol=";
            url+=item.second;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            //"code":"200000"
            if(d.IsObject() && d.HasMember("code") && d["code"].GetString() == std::string("200000") && d.HasMember("data"))
            {
                auto& tick = d["data"];
                LFPriceBook20Field priceBook {0};
                strcpy(priceBook.ExchangeID, "kucoin");
                strncpy(priceBook.InstrumentID, item.first.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, item.first.size()));
                if(tick.HasMember("bids") && tick["bids"].IsArray())
                {
                    auto& bids = tick["bids"];
                    int len = std::min((int)bids.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.BidLevels[i].price = std::round(stod(bids[i][0].GetString()) * scale_offset);
                        priceBook.BidLevels[i].volume = std::round(stod(bids[i][1].GetString()) * scale_offset);
                    }
                    priceBook.BidLevelCount = len;
                }
                if (tick.HasMember("asks") && tick["asks"].IsArray())
                {
                    auto& asks = tick["asks"];
                    int len = std::min((int)asks.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.AskLevels[i].price = std::round(stod(asks[i][0].GetString()) * scale_offset);
                        priceBook.AskLevels[i].volume = std::round(stod(asks[i][1].GetString()) * scale_offset);
                    }
                    priceBook.AskLevelCount = len;
                }
                if(tick.HasMember("sequence"))
                {
                    priceBook.UpdateMicroSecond =std::stoll(tick["sequence"].GetString());
                }

                BookMsg bookmsg;
                bookmsg.time = getTimestamp();
                bookmsg.book = priceBook;
                bookmsg.sequence = std::stoll(tick["sequence"].GetString());
                std::unique_lock<std::mutex> lck3(rest_book_mutex);
                rest_book_vec.push_back(bookmsg);    
                lck3.unlock();           

                on_price_book_update_from_rest(&priceBook);
            }
        }
    //}
}

void MDEngineKuCoin::check_snapshot()
{
    KF_LOG_INFO(logger, "MDEngineKuCoin::check_snapshot begin");

    std::vector<BookMsg>::iterator rest_it;
    std::unique_lock<std::mutex> lck3(rest_book_mutex);
    KF_LOG_INFO(logger, "MDEngineKuCoin::check_snapshot lock rest_book_mutex");

    for(rest_it = rest_book_vec.begin();rest_it != rest_book_vec.end();){
        bool has_same_book = false;
        int64_t now = getTimestamp();
        //bool has_error = false;
        //KF_LOG_INFO(logger,"string(rest_it->book.InstrumentID)"<<string(rest_it->book.InstrumentID));

        std::unique_lock<std::mutex> lck1(ws_book_mutex);
        KF_LOG_INFO(logger, "MDEngineKuCoin::check_snapshot lock ws_book_mutex");

        auto map_itr = ws_book_map.find(string(rest_it->book.InstrumentID));
        if(map_itr != ws_book_map.end()){

            // fixed here
            // 在这里把检查所有记录改成只检查最后一条
            // 修改后的代码
            BookMsg bookMsg = map_itr->second;
            if (bookMsg.isChecked == true) {
                KF_LOG_INFO(logger, "MDEngineKuCoin::check_snapshot bookMsg is checked, InstrumentID:" << rest_it->book.InstrumentID);
                continue;
            }
            else {
                bookMsg.isChecked == true;
                if (bookMsg.sequence < rest_it->sequence) {
                    KF_LOG_INFO(logger, "sequence_1=" << bookMsg.sequence << ", sequence_2=" << rest_it->sequence);
                    KF_LOG_INFO(logger, "erase old, continue");
                    continue;
                }
                else if (bookMsg.sequence == rest_it->sequence) {
                    bool same_book = true;
                    for (int i = 0; i < 20; i++) {
                        if (bookMsg.book.BidLevels[i].price != rest_it->book.BidLevels[i].price || bookMsg.book.BidLevels[i].volume != rest_it->book.BidLevels[i].volume ||
                            bookMsg.book.AskLevels[i].price != rest_it->book.AskLevels[i].price || bookMsg.book.AskLevels[i].volume != rest_it->book.AskLevels[i].volume)
                        {
                            same_book = false;
                            //has_error = true;
                            KF_LOG_INFO(logger, "2ws snapshot is not same as rest snapshot.sequence = " << rest_it->sequence);
                            KF_LOG_INFO(logger, "bookMsg:" << bookMsg.book.BidLevels[i].price << " " << bookMsg.book.BidLevels[i].volume <<
                                " " << bookMsg.book.AskLevels[i].price << " " << bookMsg.book.AskLevels[i].volume);
                            KF_LOG_INFO(logger, "rest_it:" << rest_it->book.BidLevels[i].price << " " << rest_it->book.BidLevels[i].volume <<
                                " " << rest_it->book.AskLevels[i].price << " " << rest_it->book.AskLevels[i].volume);
                            string errorMsg = "ws snapshot is not same as rest snapshot";
                            write_errormsg(116, errorMsg);
                            break;
                        }
                    }
                    if (same_book)
                    {
                        has_same_book = true;
                        if (bookMsg.time - rest_it->time > snapshot_check_s * 1000) {
                            KF_LOG_INFO(logger, "ws snapshot is later than rest snapshot");
                            //rest_it = rest_book_vec.erase(rest_it);
                            string errorMsg = "ws snapshot is later than rest snapshot";
                            write_errormsg(115, errorMsg);
                        }
                        KF_LOG_INFO(logger, "same_book:" << rest_it->book.InstrumentID);
                        KF_LOG_INFO(logger, "bookMsg_time=" << bookMsg.time << " rest_time=" << rest_it->time);
                        //break;
                    }
                    break;
                }
            }
            // 修改前的代码
            /*
            std::vector<BookMsg>::iterator ws_it;
            for(ws_it = map_itr->second.begin(); ws_it != map_itr->second.end();){
                //continue
                if(ws_it->sequence < rest_it->sequence){
                    KF_LOG_INFO(logger, "sequence_1=" << ws_it->sequence << ", sequence_2=" << rest_it->sequence);
                    KF_LOG_INFO(logger, "map_itr->second.size()=" << map_itr->second.size() << ", ws_it-map_itr->second.begin()=" << ws_it - map_itr->second.begin() << ", map_itr->second.end()-ws_it=" << map_itr->second.end() - ws_it);
                    KF_LOG_INFO(logger,"erase old");
                    ws_it = map_itr->second.erase(ws_it);
                    continue;
                }
                //break
                else if(ws_it->sequence == rest_it->sequence){
                    bool same_book = true;
                    for(int i = 0; i < 20; i++ ){
                        if(ws_it->book.BidLevels[i].price != rest_it->book.BidLevels[i].price || ws_it->book.BidLevels[i].volume != rest_it->book.BidLevels[i].volume || 
                           ws_it->book.AskLevels[i].price != rest_it->book.AskLevels[i].price || ws_it->book.AskLevels[i].volume != rest_it->book.AskLevels[i].volume)
                        {
                            same_book = false;
                            //has_error = true;
                            KF_LOG_INFO(logger, "2ws snapshot is not same as rest snapshot.sequence = "<< rest_it->sequence);
                            KF_LOG_INFO(logger,"ws_it:"<<ws_it->book.BidLevels[i].price<<" "<<ws_it->book.BidLevels[i].volume<<
                                " "<<ws_it->book.AskLevels[i].price<<" "<<ws_it->book.AskLevels[i].volume);
                            KF_LOG_INFO(logger,"rest_it:"<<rest_it->book.BidLevels[i].price<<" "<<rest_it->book.BidLevels[i].volume<<
                                " "<<rest_it->book.AskLevels[i].price<<" "<<rest_it->book.AskLevels[i].volume);
                            string errorMsg = "ws snapshot is not same as rest snapshot";
                            write_errormsg(116,errorMsg);                           
                            break;
                        }
                    }
                    if(same_book)
                    {
                        has_same_book = true;
                        if(ws_it->time - rest_it->time > snapshot_check_s * 1000){
                            KF_LOG_INFO(logger, "ws snapshot is later than rest snapshot");
                            //rest_it = rest_book_vec.erase(rest_it);
                            string errorMsg = "ws snapshot is later than rest snapshot";
                            write_errormsg(115,errorMsg);
                        }
                        KF_LOG_INFO(logger, "same_book:"<<rest_it->book.InstrumentID);
                        KF_LOG_INFO(logger,"ws_time="<<ws_it->time<<" rest_time="<<rest_it->time);
                        //break;
                    }
                    break;
                }
                else{
                    ws_it++;
                }
            }
            */
            // fixed end
        }
        lck1.unlock();
        rest_it = rest_book_vec.erase(rest_it);
    }
    lck3.unlock();
}

int64_t last_rest_time = 0;
void MDEngineKuCoin::rest_loop()
{
        while(isRunning)
        {
            int64_t now = getTimestamp();
            if((now - last_rest_time) >= rest_get_interval_ms)
            {
                last_rest_time = now;
                get_snapshot_via_rest();
            }
        }
}
int64_t last_check_time = 0;
void MDEngineKuCoin::check_loop()
{
        while(isRunning)
        {
            int64_t now = getTimestamp();
            if((now - last_check_time) >= rest_get_interval_ms)
            {
                last_check_time = now;
                check_snapshot();
            }
        }
}
void MDEngineKuCoin::klineloop()
{
    bool first_kline = true;
    int64_t last_time = time(0);
    KF_LOG_INFO(logger,"start="<<last_time);
    while(isRunning){       
        last_time = time(0);
        if(last_time % bar_duration_s == 10){
            KF_LOG_INFO(logger,"last_time="<<last_time);
            control_kline(first_kline,last_time-10);
            first_kline = false;
            break;
        }
    }
    if(!first_kline){
        KF_LOG_INFO(logger,"!first_kline");
        while(isRunning){
            int64_t now = time(0);
            if(now - last_time >= bar_duration_s){
                last_time = now;
                KF_LOG_INFO(logger,"now1="<<now);
                control_kline(first_kline,now-10);
            }
        }
    }
}

void MDEngineKuCoin::loop()
{
        time_t nLastTime = time(0);

        while(isRunning)
        {
             time_t nNowTime = time(0);
            if(isPong && (nNowTime - nLastTime>= 30))
            {
                isPong = false;
                nLastTime = nNowTime;
                KF_LOG_INFO(logger, "MDEngineKuCoin::loop: last time = " <<  nLastTime << ",now time = " << nNowTime << ",isPong = " << isPong);
                shouldPing = true;
                lws_callback_on_writable(m_conn);  
            }
            /*edited by zyy,starts here*/
            int64_t now = getTimestamp();
            int errorId = 0;
            std::string errorMsg = "";
            KF_LOG_INFO(logger, "MDEngineKuCoin::loop: wait to lock book_mutex");
            std::unique_lock<std::mutex> lck(book_mutex);
            std::map<std::string,int64_t>::iterator it1;
            for(it1 = control_book_map.begin(); it1 != control_book_map.end(); it1++){
                if((now - it1->second) > refresh_normal_check_book_s * 1000){
	                errorId = 114;
	                errorMsg = it1->first + " orderbook max refresh wait time exceeded";
	                KF_LOG_INFO(logger,"114"<<errorMsg); 
	                write_errormsg(errorId,errorMsg);
	                it1->second = now;           		
                }
            } 
            lck.unlock();

            /*edited by zyy ends here*/
            KF_LOG_INFO(logger, "MDEngineKuCoin::loop: lws_service start");
            lws_service( context, rest_get_interval_ms );
            KF_LOG_INFO(logger, "MDEngineKuCoin::loop: lws_service end");
        }
}

BOOST_PYTHON_MODULE(libkucoinmd)
{
    using namespace boost::python;
    class_<MDEngineKuCoin, boost::shared_ptr<MDEngineKuCoin> >("Engine")
    .def(init<>())
    .def("init", &MDEngineKuCoin::initialize)
    .def("start", &MDEngineKuCoin::start)
    .def("stop", &MDEngineKuCoin::stop)
    .def("logout", &MDEngineKuCoin::logout)
    .def("wait_for_stop", &MDEngineKuCoin::wait_for_stop);
}