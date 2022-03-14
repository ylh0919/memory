#include "MDEngineDeribit.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/LFDataStruct.h"

#include <writer.h>
#include <ctype.h>
#include <string.h>
#include <stringbuffer.h>
#include <document.h>
#include <cctype>
#include <algorithm>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <ctime>
#include <cpr/cpr.h>
#include <chrono>
#include <utility>
#include <atomic>

#include "../../utils/crypto/openssl_util.h"


using cpr::Get;
using cpr::Url;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;
using cpr::Body;
using cpr::Timeout;

using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using utils::crypto::base64_encode;
using std::string;
using std::to_string;
using std::stod;
using std::stoi;
using namespace std;

map<std::string, uint64_t>::iterator iter;

USING_WC_NAMESPACE

std::mutex trade_mutex;
std::mutex book_mutex;

std::atomic<int64_t> timestamp (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
static MDEngineDeribit* global_md = nullptr;
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;
std::mutex ticker_mutex;

static int64_t changeId = 0;

static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{

    switch( reason )
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            lws_callback_on_writable( wsi );
            break;
        }
        case LWS_CALLBACK_PROTOCOL_INIT:
        {
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {
            if(global_md)
            {
                global_md->on_lws_data(wsi, (const char*)in, len);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_CLOSED:
        {
            if(global_md) {
                std::cout << "3.1415926 LWS_CALLBACK_CLIENT_CLOSED 2,  (call on_lws_connection_error)  reason = " << reason << std::endl;
                global_md->on_lws_connection_error(wsi);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE_PONG:
        {
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {
            if(global_md)
            {
                global_md->lws_write_subscribe(wsi);
            }
            break;
        }
        case LWS_CALLBACK_TIMER:
        {
            break;
        }
        case LWS_CALLBACK_CLOSED:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
            std::cout << "3.1415926 LWS_CALLBACK_CLOSED/LWS_CALLBACK_CLIENT_CONNECTION_ERROR writeable, reason = " << reason << std::endl;
            if(global_md)
            {
                global_md->on_lws_connection_error(wsi);
            }
            break;
        }
        default:
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

MDEngineDeribit::MDEngineDeribit(): IMDEngine(SOURCE_DERIBIT)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Deribit");
    m_mutexPriceBookData = new std::mutex;
}

MDEngineDeribit::~MDEngineDeribit()
{
   if(m_mutexPriceBookData)
   {
       delete m_mutexPriceBookData;
   }
}

void MDEngineDeribit::clearPriceBook()
{
    std::lock_guard<std::mutex> lck(*m_mutexPriceBookData);
    m_mapPriceBookData.clear();
    mapLastData.clear();
    priceBook20Assembler.clearPriceBook();
}

void MDEngineDeribit::load(const json& j_config) 
{
    book_depth_count = j_config["book_depth_count"].get<int>();
    trade_count = j_config["trade_count"].get<int>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    KF_LOG_INFO(logger, "MDEngineDeribit:: rest_get_interval_ms: " << rest_get_interval_ms);
    //  变量 112 113
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) 
    {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    if(j_config.find("refresh_normal_check_trade_s") != j_config.end()) 
    {
        refresh_normal_check_trade_s = j_config["refresh_normal_check_trade_s"].get<int>();
    }
    if(j_config.find("level_threshold") != j_config.end()) 
    {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("snapshot_check_s") != j_config.end()) 
    {
        snapshot_check_s = j_config["snapshot_check_s"].get<int>();
    }
    if(j_config.find("funding_get_interval_s") != j_config.end()) 
    {
        funding_get_interval_s = j_config["funding_get_interval_s"].get<int>();
    }
    if(j_config.find("max_subscription_per_message") != j_config.end()) 
    {
        max_subscription_per_message = j_config["max_subscription_per_message"].get<int>();
    }
    //access_key = j_config["access_key"].get<string>();
    //secret_key = j_config["secret_key"].get<string>();
    //baseUrl = j_config["baseUrl"].get<string>();


    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    makeWebsocketSubscribeJsonString();
    debug_print(websocketSubscribeJsonString);

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineDeribit::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_28sep18\": \"BTC-28SEP18\",");
        //KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
        KF_LOG_ERROR(logger, "},");
    }

    KF_LOG_INFO(logger, "MDEngineDeribit::load:  book_depth_count: "
            << book_depth_count << " trade_count: " << trade_count << " rest_get_interval_ms: " << rest_get_interval_ms);

    int64_t nowTime = getTimestamp();
    std::unordered_map<std::string, std::string>::iterator it;
    for(it = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();it != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end();it++)
    {
        std::unique_lock<std::mutex> lck(trade_mutex);
        control_trade_map.insert(make_pair(it->first, nowTime));
        lck.unlock();

        std::unique_lock<std::mutex> lck1(book_mutex);
        control_book_map.insert(make_pair(it->first, nowTime));
        lck1.unlock();
    }
}

std::string MDEngineDeribit::createJsonString(std::string signature,std::vector<std::string> exchange_coinpairs)
{
    std::vector<std::string> listJsonStr;
    if(signature=="trade"){
        for(auto& exchange_coinpair:exchange_coinpairs)
        {
            listJsonStr.emplace_back("trades."+exchange_coinpair+".raw");
        }
    }
    else if(signature=="book")
    {
        for(auto& exchange_coinpair:exchange_coinpairs)
        {
            //listJsonStr.emplace_back("book."+exchange_coinpair+".raw");
            listJsonStr.emplace_back("book."+exchange_coinpair+".none.20.100ms");//book.BTC-PERPETUAL.none.20.100ms
        }
    }
    else if(signature=="priceindex")
    {
        for(auto& exchange_coinpair:exchange_coinpairs)
        {
            listJsonStr.emplace_back("deribit_price_index."+exchange_coinpair.substr(0,3)+"_usd");
        }
    }
    else if(signature=="markprice")
    {
        for(auto& exchange_coinpair:exchange_coinpairs)
        {
            listJsonStr.emplace_back("markprice.options."+exchange_coinpair.substr(0,3)+"_usd");
        }
    }
    else if(signature=="ticker")
    {
        for(auto& exchange_coinpair:exchange_coinpairs)
        {
            listJsonStr.emplace_back("ticker."+exchange_coinpair+".raw");
        }
    }
    else if(signature=="perpetual")
    {
        for(auto& exchange_coinpair:exchange_coinpairs)
        {
            listJsonStr.emplace_back("perpetual."+exchange_coinpair+".raw");
        }
    }  
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();

    writer.Key("jsonrpc");
    writer.String("2.0");

    writer.Key("method");
    writer.String("public/subscribe");

    writer.Key("id");
    writer.Int(42);

    writer.Key("params");
    writer.StartObject();

    writer.Key("channels");
    writer.StartArray();
    for(auto& jsonstr : listJsonStr)
    {
        //listJsonStr就是此时需要订阅的币对
        writer.String(jsonstr.c_str());
    }
    writer.EndArray();

    writer.EndObject(); 
    writer.EndObject(); 

    KF_LOG_DEBUG(logger,"MDEngineDeribit::subscribe msg:"<<s.GetString());
    return s.GetString();  
     
}

void MDEngineDeribit::makeWebsocketSubscribeJsonString()//创建请求
{
    for(auto it = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();it != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); ++it)
    {
        std::string pair = it->second;
        std::vector<std::string> exchange_coinpairs;
        bool isNeedSubTicker = false;
        if(pair =="BTC-PERPETUAL" || pair == "ETH-PERPETUAL")
        {
            KF_LOG_INFO(logger,pair);
            exchange_coinpairs.push_back(pair);
            std::unique_lock<std::mutex> lck1(ticker_mutex);
            ticker_vec.push_back(pair);
            lck1.unlock();
        }
        else if(pair=="BTC_future"){
            KF_LOG_INFO(logger,"BTC_future");
            string url ="https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&kind=future";
            const auto response = Get(Url{url});
            KF_LOG_INFO(logger," (response.text) " << response.text.c_str());
            Document d;
            d.Parse(response.text.c_str());
            if(d.IsObject())
            {
                int jsonsize = d["result"].Size();
                for(int j=0;j<jsonsize;j++){
                    std::string instrument_name = d["result"].GetArray()[j]["instrument_name"].GetString();
                    exchange_coinpairs.push_back(instrument_name);
                    std::unique_lock<std::mutex> lck1(ticker_mutex);
                    ticker_vec.push_back(instrument_name);
                    lck1.unlock();
                }
            }
        }
        else if(pair=="BTC_option"){
            KF_LOG_INFO(logger,"BTC_option");
            string url ="https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&kind=option";
            const auto response = Get(Url{url});
            KF_LOG_INFO(logger," (response.text) " << response.text.c_str());
            Document d;
            d.Parse(response.text.c_str());
            if(d.IsObject())
            {
                int jsonsize = d["result"].Size();
                for(int j=0;j<jsonsize;j++){
                    std::string instrument_name = d["result"].GetArray()[j]["instrument_name"].GetString();
                    exchange_coinpairs.push_back(instrument_name);
                    std::unique_lock<std::mutex> lck1(ticker_mutex);
                    ticker_vec.push_back(instrument_name);
                    lck1.unlock();
                }    
                isNeedSubTicker = true;
            }
        }
        else if(pair!="BTC_future" && pair.find("BTC")!=-1 && pair.find("_future")!=-1){
            KF_LOG_INFO(logger,"BTC_future4");
            std::string coinpair=pair.erase(pair.length()-7,pair.length());
            exchange_coinpairs.push_back(coinpair);
            std::unique_lock<std::mutex> lck1(ticker_mutex);
            ticker_vec.push_back(coinpair);
            lck1.unlock();
        }
        else if(pair!="BTC_option" && pair.find("BTC")!=-1&&pair.find("_option")!=-1){
            KF_LOG_INFO(logger,"BTC_option5");
            std::string optionpair=pair.erase(pair.length()-7,pair.length());
            KF_LOG_INFO(logger,"optionpair:"<<optionpair);
            //string url = access_key+"BTC&kind=option";
            string url ="https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&kind=option";
            KF_LOG_INFO(logger,"url:"<<url);
            const auto response = Get(Url{url}, 
                                      Timeout{3000});
            KF_LOG_INFO(logger, "[Get] (url) " << url << " (response.status_code) " << response.status_code <<
                " (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str());
            Document d;
            d.Parse(response.text.c_str());
            if(d.IsObject())
            {
                int jsonsize = d["result"].Size();
                for(int j=0;j<jsonsize;j++){
                    std::string instrument_name = d["result"].GetArray()[j]["instrument_name"].GetString();
                    if(instrument_name.find(optionpair.c_str())!= -1){
                        exchange_coinpairs.push_back(instrument_name);
                        std::unique_lock<std::mutex> lck1(ticker_mutex);
                        ticker_vec.push_back(instrument_name);
                        lck1.unlock();
                    }
                } 
                isNeedSubTicker = true;
            }             
        }
        else if(pair=="ETH_future"){
            KF_LOG_INFO(logger,"ETH_future");
            string url = "https://www.deribit.com/api/v2/public/get_instruments?currency=ETH&kind=future";
            const auto response = Get(Url{url} );
            KF_LOG_INFO(logger," (response.text) " << response.text.c_str());
            Document d;
            d.Parse(response.text.c_str());
            if(d.IsObject())
            {
                int jsonsize = d["result"].Size();
                for(int j=0;j<jsonsize;j++){
                    std::string instrument_name = d["result"].GetArray()[j]["instrument_name"].GetString();
                    exchange_coinpairs.push_back(instrument_name);
                    std::unique_lock<std::mutex> lck1(ticker_mutex);
                    ticker_vec.push_back(instrument_name);
                    lck1.unlock();
                }
            }
        }
        else if(pair=="ETH_option"){
            KF_LOG_INFO(logger,"ETH_option");
            string url = "https://www.deribit.com/api/v2/public/get_instruments?currency=ETH&kind=option";
            const auto response = Get(Url{url} 
                                      );
            KF_LOG_INFO(logger," (response.text) " << response.text.c_str());
            Document d;
            d.Parse(response.text.c_str());
            if(d.IsObject())
            {
                int jsonsize = d["result"].Size();
                for(int j=0;j<jsonsize;j++){
                    std::string instrument_name = d["result"].GetArray()[j]["instrument_name"].GetString();
                    exchange_coinpairs.push_back(instrument_name);
                    std::unique_lock<std::mutex> lck1(ticker_mutex);
                    ticker_vec.push_back(instrument_name);
                    lck1.unlock();
                } 
                isNeedSubTicker = true;
            }           
        }
        else if(pair!="ETH_future"&&pair.find("ETH")!=-1&&pair.find("_future")!=-1){
            KF_LOG_INFO(logger,"ETH_future4");
            std::string coinpair=pair.erase(pair.length()-7,pair.length());
            exchange_coinpairs.push_back(coinpair);
            std::unique_lock<std::mutex> lck1(ticker_mutex);
            ticker_vec.push_back(coinpair);
            lck1.unlock();
        }
        else if(pair!="ETH_option"&&pair.find("ETH")!=-1&&pair.find("_option")!=-1){
            KF_LOG_INFO(logger,"ETH_option5");
            std::string optionpair=pair.erase(pair.length()-7,pair.length());
            //string url = access_key+"ETH&kind=option";
            string url = "https://www.deribit.com/api/v2/public/get_instruments?currency=ETH&kind=option";
            const auto response = Get(Url{url} 
                                      );
            KF_LOG_INFO(logger," (response.text) " << response.text.c_str());
            Document d;
            d.Parse(response.text.c_str());
            if(d.IsObject())
            {
                int jsonsize = d["result"].Size();
                for(int j=0;j<jsonsize;j++){
                    std::string instrument_name = d["result"].GetArray()[j]["instrument_name"].GetString();
                    if(instrument_name.find(optionpair.c_str())!= -1){
                        exchange_coinpairs.push_back(instrument_name);
                        std::unique_lock<std::mutex> lck1(ticker_mutex);
                        ticker_vec.push_back(instrument_name);
                        lck1.unlock();
                    }
                } 
                isNeedSubTicker = true;
            }             
        }
        else 
        {
            KF_LOG_INFO(logger, "pair: " << pair);
            exchange_coinpairs.push_back(pair);
            std::unique_lock<std::mutex> lck1(ticker_mutex);
            ticker_vec.push_back(pair);
            lck1.unlock();
        }
        int nCount = exchange_coinpairs.size()/max_subscription_per_message;
        for(int nIndex = 0; nIndex <= nCount;++nIndex)
        {
            auto begin = exchange_coinpairs.begin() + nIndex * max_subscription_per_message;
            auto end = exchange_coinpairs.end();
            if((nIndex + 1)* max_subscription_per_message < exchange_coinpairs.size())
            {
                end = begin + max_subscription_per_message;
            }
            else
            {//
                nIndex = nCount;
            }
            std::vector<std::string> tmpList(begin,end);
            KF_LOG_INFO(logger, "[makeWebsocketSubscribeJsonString] websocketSubscribeJsonString (count) " << tmpList.size());
            std::string jsonBookString = createJsonString("book",tmpList);
            websocketSubscribeJsonString.push_back(jsonBookString);
            std::string jsonTradeString = createJsonString("trade",tmpList);
            websocketSubscribeJsonString.push_back(jsonTradeString);
            if(isNeedSubTicker)
            {
                std::string jsonTickerString = createJsonString("ticker",tmpList);
                websocketSubscribeJsonString.push_back(jsonTickerString);
            }
        }
        exchange_coinpairs.clear();
    }
}

void MDEngineDeribit::debug_print(std::vector<std::string> &subJsonString)
{
    size_t count = subJsonString.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
    }
}

void MDEngineDeribit::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineDeribit::connect:");
    connected = true;
}

void MDEngineDeribit::login(long timeout_nsec) 
{//连接到服务器
    KF_LOG_INFO(logger, "MDEngineDeribit::login:");
    global_md = this;

    if (context == NULL) {
        struct lws_context_creation_info info;
        memset( &info, 0, sizeof(info) );

        info.port = CONTEXT_PORT_NO_LISTEN;
        info.protocols = protocols;
        info.iface = NULL;
        info.ssl_cert_filepath = NULL;
        info.ssl_private_key_filepath = NULL;
        info.extensions = NULL;
        info.gid = -1;
        info.uid = -1;
        info.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
        info.max_http_header_pool = 1024;
        info.fd_limit_per_thread = 1024;
        info.ws_ping_pong_interval = 10;
        info.ka_time = 10;
        info.ka_probes = 10;
        info.ka_interval = 10;

        context = lws_create_context( &info );
        KF_LOG_INFO(logger, "MDEngineDeribit::login: context created.");
    }

    if (context == NULL) {
        KF_LOG_ERROR(logger, "MDEngineDeribit::login: context is NULL. return");
        return;
    }

    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
    lws_set_log_level(logs, NULL);

    struct lws_client_connect_info ccinfo = {0};

    static std::string host  = "www.deribit.com";
    static std::string path = "/ws/api/v2";
    static int port = 443;

    ccinfo.context     = context;
    ccinfo.address     = host.c_str();
    ccinfo.port     = port;
    ccinfo.path     = path.c_str();
    ccinfo.host     = host.c_str();
    ccinfo.origin     = host.c_str();
    ccinfo.ietf_version_or_minus_one = -1;
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

    struct lws* wsi = lws_client_connect_via_info(&ccinfo);
    KF_LOG_INFO(logger, "MDEngineDeribit::login: Connecting to " <<  ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "MDEngineDeribit::login: wsi create error.");
        return;
    }
    KF_LOG_INFO(logger, "MDEngineDeribit::login: wsi create success.");

    logged_in = true;
    snapshot_finish = false;
}

void MDEngineDeribit::set_reader_thread()
{
    IMDEngine::set_reader_thread();

    ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineDeribit::loop, this)));
    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineDeribit::rest_loop, this)));
    //check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineDeribit::check_loop, this)));
    //funding_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineDeribit::funding_loop, this)));
}

void MDEngineDeribit::logout()
{
    KF_LOG_INFO(logger, "MDEngineDeribit::logout:");
}

void MDEngineDeribit::release_api()
{
    KF_LOG_INFO(logger, "MDEngineDeribit::release_api:");
}

void MDEngineDeribit::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEngineDeribit::subscribeMarketData:");
}

int MDEngineDeribit::lws_write_subscribe(struct lws* conn)
{
    KF_LOG_INFO(logger, "MDEngineDeribit::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    //有待发送的数据，先把待发送的发完，再继续订阅逻辑  
    if(websocketPendingSendMsg.size() > 0) {
        unsigned char msg[20480];
        memset(&msg[LWS_PRE], 0, 20480-LWS_PRE);

        std::string jsonString = websocketPendingSendMsg[websocketPendingSendMsg.size() - 1];
        websocketPendingSendMsg.pop_back();
        KF_LOG_INFO(logger, "MDEngineDeribit::lws_write_subscribe: websocketPendingSendMsg" << jsonString.c_str());
        int length = jsonString.length();

        strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
        int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

        if(websocketPendingSendMsg.size() > 0)
        {    //still has pending send data, emit a lws_callback_on_writable()
            lws_callback_on_writable( conn );
            KF_LOG_INFO(logger, "MDEngineDeribit::lws_write_subscribe: (websocketPendingSendMsg,size)" << websocketPendingSendMsg.size());
        }
        return ret;
    }

    if(websocketSubscribeJsonString.size() == 0) return 0;//
    //sub depth
    if(subscribe_index >= websocketSubscribeJsonString.size())
    {
        //subscribe_index = 0;
        KF_LOG_INFO(logger, "MDEngineDeribit::lws_write_subscribe: (none reset subscribe_index = 0, just return 0)");
        return 0;
    }

    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

    std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

    KF_LOG_INFO(logger, "MDEngineDeribit::lws_write_subscribe: " << jsonString.c_str() << " ,after ++, (subscribe_index)" << subscribe_index);
    int length = jsonString.length();

    strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

    if(subscribe_index < websocketSubscribeJsonString.size())
    {
        lws_callback_on_writable( conn );
        KF_LOG_INFO(logger, "MDEngineDeribit::lws_write_subscribe: (subscribe_index < websocketSubscribeJsonString) call lws_callback_on_writable");
    }

    return ret;
}

void MDEngineDeribit::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "MDEngineDeribit::on_lws_data: " << data<<",len:"<<len);
    Document json;
    json.Parse(data);

    if(json.HasParseError()) {
        KF_LOG_ERROR(logger, "MDEngineDeribit::on_lws_data. parse json error: " << data);
        return;
    }
   
    //vector<string> v = split(json["channel"].GetString(), "_");
    if(json.HasMember("testnet")) {
        KF_LOG_ERROR(logger, "MDEnginebitFlyer::on_lws_data:subscribe success ");
    }
    
    if(json.HasMember("params")){
        string channel = json["params"].GetObject()["channel"].GetString();
        channel = channel.substr(0,4);
        if(channel=="trad"){
            onTrade(json);
        }
        else if(channel=="book"){
            onBook(json);
        }
        else if(channel=="deri"){
            onPriceIndex(json);
        }
        else if(channel=="mark"){
            onMarkPrice(json);
        }
        else if(channel=="perp"){
            onPerpetual(json);
        }
        else if(channel=="tick"){
            onTicker(json);
        }       
    }
   
    else KF_LOG_INFO(logger, "MDEngineDeribit::on_lws_data: unknown data: " << parseJsonToString(json));
}

void MDEngineDeribit::on_lws_connection_error(struct lws* conn) //liu
{
    KF_LOG_ERROR(logger, "MDEngineDeribit::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineDeribit::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;
    //std::this_thread::sleep_for(std::chrono::minutes(5));
    login(timeout_nsec);
}

int64_t MDEngineDeribit::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void MDEngineDeribit::debug_print(std::vector<SubscribeChannel> &websocketSubscribeChannel)
{
    size_t count = websocketSubscribeChannel.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeChannel (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeChannel (subType) "
                            << websocketSubscribeChannel[i].subType <<
                            " (exchange_coinpair)" << websocketSubscribeChannel[i].exchange_coinpair <<
                            " (channelId)" << websocketSubscribeChannel[i].channelId);
    }
}

void MDEngineDeribit::onTrade(Document& json)
{
    //std::string ticker = coinPairWhiteList.GetKeyByValue(split(json["channel"].GetString()));
        KF_LOG_INFO(logger, "MDEngineDeribit::onTrade");

        /*auto& data = json["result"];
        int len = (int)data.GetArray().Size();
        int i=0;*/
        Value &node=json["params"];
        int len = node.GetObject()["data"].Size();
        
        //LFL2TradeField trade;
        double amount;
        uint64_t volume;

        for(int i=0;i<len;i++){
            /*auto& result = json["result"].GetArray()[i];
            std::string ticker =coinPairWhiteList_websocket.GetKeyByValue((std::string)result["instrument"].GetString()) ;*/
            std::string ticker = node.GetObject()["data"].GetArray()[i]["instrument_name"].GetString();
            //int tradetime = node.GetObject()["data"].GetArray()[i]["timestamp"].GetInt();
            //KF_LOG_INFO(logger, "MDEngineDeribit::onTrade: ticker:" << ticker.c_str());
            LFL2TradeField trade;
            memset(&trade, 0, sizeof(trade));
            strcpy(trade.InstrumentID, ticker.c_str());
            strcpy(trade.ExchangeID, "deribit");
            //strcpy(trade.TradeTime,std::to_string(tradetime).c_str());  
            trade.Price = std::round(node.GetObject()["data"].GetArray()[i]["price"].GetDouble() * scale_offset);
            amount = node.GetObject()["data"].GetArray()[i]["amount"].GetDouble();
            volume = std::round( amount * scale_offset);
            trade.Volume = volume;
            //KF_LOG_INFO(logger, "MDEngineDeribit::pricev");
            strcpy(trade.TradeID,node.GetObject()["data"].GetArray()[i]["trade_id"].GetString());
            //KF_LOG_INFO(logger, "MDEngineDeribit::tradeid");
            trade.TimeStamp = node.GetObject()["data"].GetArray()[i]["timestamp"].GetInt64();
            string strTime  = timestamp_to_formatISO8601(trade.TimeStamp);
            trade.TimeStamp *= 1000000;
            strcpy(trade.TradeTime,strTime.c_str());
            //strcpy(trade.TradeTime,node.GetObject()["data"].GetArray()[i]["timestamp"].GetString());
            //KF_LOG_INFO(logger, "MDEngineDeribit::Tradetime");
            //trade.OrderBSFlag[0] = node.GetObject()["data"].GetArray()[i]["direction"].GetString() == "sell" ? 'S' : 'B';
            std::string side = node.GetObject()["data"].GetArray()[i]["direction"].GetString();
            trade.OrderBSFlag[0] = side == "sell" ? 'S' : 'B';
            //KF_LOG_INFO(logger, "side"  <<side);
            //strcpy(trade.MakerOrderID,std::to_string(result["orderId"].GetInt()).c_str());
            //strcpy(trade.TakerOrderID,std::to_string(result["matchingId"].GetInt()).c_str());

            KF_LOG_INFO(logger, "MDEngineDeribit::[onTrade]"  <<" (Price)" << trade.Price << " (trade.Volume)" << trade.Volume);

            std::unique_lock<std::mutex> lck(trade_mutex);
            auto it = control_trade_map.find(ticker);
            if(it != control_trade_map.end())
            {
                it->second = getTimestamp();
            }
            lck.unlock();

            on_trade(&trade);
        }
}

void MDEngineDeribit::onPriceIndex(Document& json)
{
    //std::string ticker = coinPairWhiteList.GetKeyByValue(split(json["channel"].GetString()));
        KF_LOG_INFO(logger, "MDEngineDeribit::onPriceIndex");

        Value &node=json["params"].GetObject()["data"];

        std::string ticker = node.GetObject()["index_name"].GetString();
        //KF_LOG_INFO(logger, "MDEngineDeribit::onTrade: ticker:" << ticker.c_str());
            LFPriceIndex priceindex;
            memset(&priceindex, 0, sizeof(priceindex));
            strcpy(priceindex.InstrumentID, ticker.c_str());
            //strcpy(trade.ExchangeID, "deribit");  
            priceindex.Price = std::round(node.GetObject()["price"].GetDouble() * scale_offset);

            int64_t timestamp=node.GetObject()["timestamp"].GetInt64();
            std::string timestampstr = std::to_string(timestamp);
            strcpy(priceindex.TimeStamp, timestampstr.c_str());
            
            KF_LOG_INFO(logger, "MDEngineDeribit::[onPriceIndex]"  << "(TimeStamp)"<<priceindex.TimeStamp<<
                                                                " (Price)" << priceindex.Price);
                                                               
            on_priceindex(&priceindex);
        
}

void MDEngineDeribit::onMarkPrice(Document& json)
{
        KF_LOG_INFO(logger, "MDEngineDeribit::onMarkPrice");

        Value &node=json["params"];
        int len = node.GetObject()["data"].Size();
        
        double amount;
        uint64_t volume;

        for(int i=0;i<len;i++){
            std::string ticker = node.GetObject()["data"].GetArray()[i]["instrument_name"].GetString();
            
            KF_LOG_INFO(logger, "MDEngineDeribit::onTrade: ticker:" << ticker.c_str());
            LFMarkPrice markprice;
            memset(&markprice, 0, sizeof(markprice));
            strcpy(markprice.InstrumentID, ticker.c_str());
            //strcpy(trade.ExchangeID, "deribit");
 
            markprice.MarkPrice = std::round(node.GetObject()["data"].GetArray()[i]["mark_price"].GetDouble() * scale_offset);
            markprice.Iv = std::round(node.GetObject()["data"].GetArray()[i]["iv"].GetDouble() * scale_offset);

            KF_LOG_INFO(logger, "MDEngineDeribit::[onMarkPrice]"  <<
                                                                " (MarkPrice)" << markprice.MarkPrice <<
                                                                " (markprice.Iv)" << markprice.Iv);
            on_markprice(&markprice);
        }
}

void MDEngineDeribit::onPerpetual(Document& json)
{
    //std::string ticker = coinPairWhiteList.GetKeyByValue(split(json["channel"].GetString()));
        KF_LOG_INFO(logger, "MDEngineDeribit::onPerpetual");

        Value &node=json["params"].GetObject()["data"];

        LFPerpetual perpetual;
            memset(&perpetual, 0, sizeof(perpetual));
            //strcpy(perpetual.InstrumentID, ticker.c_str());
            //strcpy(trade.ExchangeID, "deribit");  
            perpetual.Interest = (node.GetObject()["interest"].GetDouble());
            std::string channel = json["params"].GetObject()["channel"].GetString();
            channel=channel.erase(0,10);
            std::string ticker=channel.substr(0,channel.length()-4);
            strcpy(perpetual.InstrumentID, ticker.c_str());
            
            KF_LOG_INFO(logger, "MDEngineDeribit::[onPerpetual]"  <<"(InstrumentID)"<<perpetual.InstrumentID<<
                                                                " (Interest)" << perpetual.Interest);
                                                               
            on_perpetual(&perpetual);
        
}

void MDEngineDeribit::onTicker(Document& json)
{
    //std::string ticker = coinPairWhiteList.GetKeyByValue(split(json["channel"].GetString()));
        KF_LOG_INFO(logger, "MDEngineDeribit::onTicker");

        Value &node=json["params"]["data"];

        std::string ticker = node["instrument_name"].GetString();
        KF_LOG_INFO(logger, "MDEngineDeribit::onTicker: ticker:" << ticker.c_str());
        LFTicker tick;
            memset(&tick, 0, sizeof(tick));
            strcpy(tick.InstrumentID, ticker.c_str());
              
            tick.Best_ask_price = std::round(node["best_ask_price"].GetDouble() * scale_offset);
            tick.Best_ask_amount = std::round(node["best_ask_amount"].GetDouble() * scale_offset);
            tick.Best_bid_price = std::round(node["best_bid_price"].GetDouble() * scale_offset);
            tick.Best_bid_amount = std::round(node["best_bid_amount"].GetDouble() * scale_offset);
            tick.Mark_price = std::round(node["mark_price"].GetDouble() * scale_offset);
            if(node["last_price"].IsNumber()){
                tick.Last_price = std::round(node["last_price"].GetDouble() * scale_offset);
            }
            else{
                tick.Last_price = 0;
            }
            
            tick.Ask_iv = node["ask_iv"].GetDouble();
            tick.Bid_iv = node["bid_iv"].GetDouble();
            tick.Open_interest = node["open_interest"].GetDouble();
            tick.Underlying_price = std::round(node["underlying_price"].GetDouble() * scale_offset);
            tick.Delta = node["greeks"]["delta"].GetDouble();
            tick.Vega = node["greeks"]["vega"].GetDouble();
            if(node["stats"]["volume"].IsNumber()){
                tick.Volume24 = node["stats"]["volume"].GetDouble();
            }
            else{
                tick.Volume24 = 0;
            }
            std::string underlying_index=node["underlying_index"].GetString();
            strcpy(tick.Underlying_index, underlying_index.c_str());
                                                               
            on_ticker(&tick);
        
}

void MDEngineDeribit::onBook(Document& json)
{
    if(!json.HasMember("params"))
    {
        return;
    }
    Value &node = json["params"].GetObject()["data"];
    Value &bidsnode = node.GetObject()["bids"];
    Value &asksnode = node.GetObject()["asks"];

    int len1 = node.GetObject()["bids"].Size();
    int len2 = node.GetObject()["asks"].Size();
    //std::string tickers = node["instrument_name"].GetString();
    std::string ticker = node["instrument_name"].GetString();
    std::string symbol = getWhiteListCoinpairFrom(ticker);
    priceBook20Assembler.clearPriceBook(ticker);
    //LFPriceBook20Field priceBook {0};
    KF_LOG_INFO(logger, "MDEngineDeribit::onBook: (symbol) " << ticker.c_str() << "-" << symbol);

    KF_LOG_DEBUG(logger, "onBook start");

        //KF_LOG_DEBUG(logger, "onBook start");
        //KF_LOG_DEBUG(logger, "ticker="<<ticker<<" len1="<<len1<<" len2="<<len2);
        
        /*auto& result = json["result"].GetArray()[i];
        auto& bids = result["bids"];
        auto&asks = result["asks"];*/
        //ticker = coinPairWhiteList_websocket.GetKeyByValue( (std::string)result["instrument"].GetString());
        //strcpy(priceBook.ExchangeID, "deribit");
        //strcpy(priceBook.InstrumentID, ticker.c_str());
    KF_LOG_DEBUG(logger, "ticker="<<ticker<<" len1="<<len1<<" len2="<<len2);  
    /*if(!node.HasMember("prev_change_id")){
    snapshot_finish = true;
    } 
    KF_LOG_INFO(logger,"snapshot_finish:"<<snapshot_finish);
    */
    changeId = node["change_id"].GetInt64();
    uint64_t otherChangeId = changeId;
    bool flag = 0;
    KF_LOG_INFO(logger, "change_id = "<<changeId<<" otherChangeId = "<<otherChangeId);
    /*
    解释一下otherChangeId作用：
    因为在下面的代码中会有改变changId的过程，otherChangeId的作用就是作为这条交易中的changId的副本
    如果没有这个副本，就会丢失此次交易数据中的change_id
    */

    //包含这个字段说明是接收的交易数据，要进行change_id和pre_change_id的比较
    /*
    在头文件中有一个saveChangeID的map，用这个来存放每个币对的当前的changeId
    在每接收一条数据之后就判断其是否是snapshot，不是snapshot则在该map中必有
    一个pair是存放了该币对的当前changeID的值，如果是一个snapshot则选择放进
    这个map
    */
    /*if(node.HasMember("prev_change_id"))
    {
        KF_LOG_INFO(logger, "lichengyi-MDEngineDeribit::onDepth--have prev_change_id,is not snapshot");
        //在不是snapshot的情况下查看map里面是否有该ticker，有的话就把该pair的changeId作为比较的数据
        iter = saveChangeID.find(ticker);
        if(iter != saveChangeID.end())
        {
            KF_LOG_INFO(logger, "lichengyi::onBook--get the ticker's changId = "<<iter->second<<"---"<<iter->first);
            changeId = iter->second;
        }
        //有change_id字段才是一个合法的数据，不然退出即可
        if(node.HasMember("change_id"))
        {
            auto prevChangeId = node["prev_change_id"].GetInt64();
            KF_LOG_INFO(logger, "not snapshot:changID is "<<changeId);
            while(1)
            {
                if(changeId == prevChangeId)
                {
                    KF_LOG_INFO(logger, "lichengyi-MDEngineDeribit::onDepth--this is a legal data "<<changeId<<"-"<<prevChangeId);
                    //获得的数据合法就直接break跳出while进行下面的处理
                    flag = 1;
                    break;
                }
                else if(changeId > prevChangeId)
                {
                    //此时的id大于收到的数据的prev_id表示snapshot超前了
                    KF_LOG_INFO(logger, "lichengyi-MDEngineDeribit::onDepth--changeId > prevChangeId "<<changeId<<"-"<<prevChangeId);
                    return;
                }
                else if(changeId < prevChangeId)
                {
                    std::lock_guard<std::mutex> lck(*m_mutexPriceBookData);
                    //当此时的prev_id大于前一个snapshot的id时，说明丢包，重新获取
                    std::map<std::string,PriceBookData>::iterator itPriceBook;
                    string errorMsg = "Orderbook update sequence missed, request for a new snapshot";
                    write_errormsg(5,errorMsg);

                    KF_LOG_INFO(logger, "lichengyi-MDEngineDeribit::onDepth--changeId < prevChangeId "<<changeId<<"-"<<prevChangeId);
                    if(!getInitPriceBook(ticker,itPriceBook))
                    {
                        return;
                    }
                    changeId = itPriceBook->second.idChange;
                }
            }
        }
        else
        {
            KF_LOG_INFO(logger, "lichengyi-MDEngineDeribit::onDepth--the data which get is error");
            return;
        }

    }*/
    //不包含上面那个字段的时候就是snapshot直接添加到本地snapshot，即是直接执行下面代码
    {
        if(flag == 0) priceBook20Assembler.clearPriceBook(ticker);
        //设置的一个bool类型数据，如果是经过上面含有prev_change_id的部分代码则falg设置为1，
        //否则传进来的就是snapshot，需要清空一下。
        iter = saveChangeID.find(ticker);
        if(iter == saveChangeID.end())
        {
            //这种情况下我打印一下map的值查看
            for(iter = saveChangeID.begin(); iter != saveChangeID.end(); ++iter)
                KF_LOG_INFO(logger, "lichengyi::ticker = "<<iter->first<<" changeId = "<<iter->second);

            saveChangeID.insert(std::make_pair(ticker, changeId));
        }
        else
        {
            iter->second = otherChangeId;
            KF_LOG_INFO(logger, "lichengyi::onBook--get the changId = "<<iter->second);
        }       
        //当是snapshot的时候就把此时的changeId和币对信息插入

        KF_LOG_INFO(logger, "lichengyi-MDEngineDeribit::onDepth--don't have prev_change_id,is snapshot");
        KF_LOG_INFO(logger, "snapshot:changeID is "<<changeId);
        if(len1>0){
            for (int i = 0; i < len1; i++) {
                int64_t price = std::round(bidsnode.GetArray()[i].GetArray()[0].GetDouble() * SCALE_OFFSET);
                uint64_t volume = std::round(bidsnode.GetArray()[i].GetArray()[1].GetDouble() * SCALE_OFFSET);
                /*if(bidsnode.GetArray()[i].GetArray()[0].GetString()=="delete"||volume == 0) {
                    priceBook20Assembler.EraseBidPrice(ticker, price);
                } else {*/
                    priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
                //}
            }
        }
        if(len2>0){
            for (int i = 0; i < len2; i++) {
                int64_t price = std::round(asksnode.GetArray()[i].GetArray()[0].GetDouble() * SCALE_OFFSET);
                uint64_t volume = std::round(asksnode.GetArray()[i].GetArray()[1].GetDouble() * SCALE_OFFSET);
                /*if(asksnode.GetArray()[i].GetArray()[0].GetString()=="delete"||volume == 0) {
                    priceBook20Assembler.EraseAskPrice(ticker, price);
                } else {*/
                    priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
                //}
            }
        }                                
        LFPriceBook20Field md;
        memset(&md, 0, sizeof(md));
        if(priceBook20Assembler.Assembler(ticker, md)) 
        {
            strcpy(md.ExchangeID, "deribit");
            strcpy(md.InstrumentID, ticker.c_str());
            KF_LOG_INFO(logger, "MDEngineDerbit::onbook: on_price_book_update");
            //判断9，两个判断 （112 、113）全局变量时间获取
            timestamp = getTimestamp();
            if (md.AskLevelCount < level_threshold || md.BidLevelCount < level_threshold){
                   string errorMsg = "orderbook level below threshold";
                write_errormsg(112,errorMsg);
                KF_LOG_INFO(logger, "MDEngineDerbit::onbook: " << errorMsg);
                //on_price_book_update(&md);
               }
            else if(md.AskLevelCount > 0 && md.BidLevelCount > 0 && (md.BidLevels[0].price > md.AskLevels[0].price || md.BidLevels[0].price <= 0 || md.AskLevels[0].price <= 0)){
                string errorMsg = "orderbook crossed";
                write_errormsg(113,errorMsg);
                KF_LOG_INFO(logger, "MDEngineDerbit::onbook: " << errorMsg);
            }
            else
            {
                int64_t now = getTimestamp();
                std::unique_lock<std::mutex> lck_ws_book(ws_book_mutex);
                auto itr = ws_book_map.find(ticker);
                if(itr == ws_book_map.end()){
                    std::map<uint64_t, int64_t> bookmsg_map;
                    bookmsg_map.insert(std::make_pair(otherChangeId, now));
                    ws_book_map.insert(std::make_pair(ticker, bookmsg_map));
                    KF_LOG_INFO(logger,"insert:"<<ticker);
                }else{
                    itr->second.insert(std::make_pair(otherChangeId, now));
                }
                lck_ws_book.unlock();

                on_price_book_update(&md);
            }        
        }
        else { KF_LOG_INFO(logger, "MDEngineDeribit::onDepth: same data not update" );}
        std::unique_lock<std::mutex> lck1(book_mutex);
        auto it = control_book_map.find(ticker);
        if(it != control_book_map.end())
        {
            it->second = getTimestamp();
        }
        lck1.unlock();
    }
}

std::string MDEngineDeribit::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}

// std::string CoinPairWhiteList::GetKeyByValue(std::string exchange_coinpair)
//         {
//             std::unordered_map<std::string, std::string>::iterator map_itr;
//             map_itr = keyIsStrategyCoinpairWhiteList.begin();
//             while(map_itr != keyIsStrategyCoinpairWhiteList.end())
//             {
//                 if(exchange_coinpair == map_itr->second)
//                 {
// //                    std::cout << "[GetKeyByValue] found (strategy_coinpair) " <<
// //                              map_itr->first << " (exchange_coinpair) " << map_itr->second << std::endl;

//                     return map_itr->first;
//                 }
//                 map_itr++;
//             }
// //            std::cout << "[getWhiteListCoinpairFrom] not found (exchange_coinpair) " << exchange_coinpair << std::endl;
//             return "";
//         }

std::string MDEngineDeribit::getWhiteListCoinpairFrom(std::string md_coinpair)
{
    std::string& ticker = md_coinpair;

    KF_LOG_INFO(logger, "lichengyi_Deribit-getWhite-[getWhiteListCoinpairFrom] find md_coinpair (md_coinpair) " << md_coinpair << " (toupper(ticker)) " << ticker);
    std::map<std::string, std::string>::iterator map_itr;
    map_itr = keyIsStrategyCoinpairWhiteList.begin();
    while(map_itr != keyIsStrategyCoinpairWhiteList.end()) {
        if(ticker == map_itr->second)
        {
            KF_LOG_INFO(logger, "lichengyi_Deribit-getWhite-[getWhiteListCoinpairFrom] found md_coinpair (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);
            return map_itr->first;
        }
        map_itr++;
    }
    KF_LOG_INFO(logger, "lichengyi_Deribit-getWhite-[getWhiteListCoinpairFrom] not found md_coinpair (md_coinpair) " << md_coinpair);
    return "";
}

bool MDEngineDeribit::getInitPriceBook(const std::string& strSymbol,std::map<std::string,PriceBookData>::iterator& itPriceBookData)
{   //获取snapshot
    /*
    在deribit这个交易所获取的信息中：
    "change_id":3571657703,
    "bids":[
            [8608.5,450.0],
            [8608.0,150.0],
            [8607.5,500.0],
            [8607.0,1950.0],
            [8605.5,1800.0]
        ],
    "asks":[
            [8609.0,17240.0],
            [8609.5,672130.0],
            [8610.0,182670.0],
            [8610.5,1800.0],
            [8611.0,140.0]
        ]   
    这些都是数字，不需要再进行转化
    */
    int nTryCount = 0;
    cpr::Response response;
    std::string url = "https://www.deribit.com/api/v2/public/get_order_book?instrument_name=";
    url += strSymbol;

    std::string ticker = strSymbol;

    do{  
       response = Get(Url{url.c_str()}, Parameters{}); 
       
    }while(++nTryCount < rest_try_count && response.status_code != 200);

    if(response.status_code != 200)
    {
        KF_LOG_ERROR(logger, "lichengyi_Deribit-getInit-MDEngineDertbit::login::getInitPriceBook Error, response = " <<response.text.c_str());
        return false;
    }
    KF_LOG_INFO(logger, "lichengyi_Deribit-getInit-MDEngineDeribit::getInitPriceBook: " << response.text.c_str());
    priceBook20Assembler.clearPriceBook(ticker);
    //clearPriceBook清空本地缓存

    Document d;
    d.Parse(response.text.c_str());
    itPriceBookData = m_mapPriceBookData.insert(std::make_pair(ticker,PriceBookData())).first;
    if(!d.HasMember("result"))
    {
        return  false;
    }
    auto& jsonData = d["result"];
    if(jsonData.HasMember("change_id"))
    {
        itPriceBookData->second.idChange = jsonData["change_id"].GetInt64();
    }
    //后面关于bids和asks的处理不需要变动
    if(jsonData.HasMember("bids"))
    {
        auto& bids =jsonData["bids"];
         if(bids .IsArray()) 
         {
                int len = bids.Size();
                for(int i = 0 ; i < len; i++)
                {
                    int64_t price = std::round(bids.GetArray()[i][0].GetDouble() * scale_offset);
                    uint64_t volume = std::round(bids.GetArray()[i][1].GetDouble() * scale_offset);
                    priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
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
                    int64_t price = std::round(asks.GetArray()[i][0].GetDouble() * scale_offset);
                    uint64_t volume = std::round(asks.GetArray()[i][1].GetDouble() * scale_offset);
                    priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
                }
         }
    }
    KF_LOG_INFO(logger,"lichengyi_Deribit-getInit-ticker="<<ticker);

    //string errorMsg = "lichengyi:Get a snapshot";
    //write_errormsg(5,errorMsg);

    return true;
}
void MDEngineDeribit::get_funding_rest(int64_t end_timestamp)
{
    //while (isRunning)
    //{
        auto& symbol_map = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList();
        for(const auto& item : symbol_map)
        {
            if(item.second.find("PERPETUAL") == -1)
            {
                continue;
            }
            std::string end_time = std::to_string(end_timestamp);
            std::string start_time = std::to_string(stoll(end_time) - funding_get_interval_s*1000);
            std::string url = "https://www.deribit.com/api/v2/public/get_funding_rate_value?instrument_name=";//"&end_timestamp=1593412743000&start_timestamp=1593410000000";
            url+=item.second;
            url = url + "&end_timestamp=" + end_time + "&start_timestamp=" + start_time;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_funding_rest get("<< url << "):" << response.text);
            
            if(d.IsObject() && d.HasMember("result"))
            {
                LFFundingField fundingdata;
                memset(&fundingdata, 0, sizeof(fundingdata));
                strcpy(fundingdata.InstrumentID, item.first.c_str());
                strcpy(fundingdata.ExchangeID, "deribit");

                fundingdata.Rate = d["result"].GetDouble();
                fundingdata.TimeStamp = stoll(end_time);
                fundingdata.Interval = funding_get_interval_s*1000;
                KF_LOG_INFO(logger,"Rate:"<<fundingdata.Rate);
                
                on_funding_update(&fundingdata);
            }
        }
    //}
}

void MDEngineDeribit::get_snapshot_via_rest()
{
    {
        std::vector<std::string>::iterator map_itr;
        std::unique_lock<std::mutex> lck1(ticker_mutex);
        for(map_itr = ticker_vec.begin(); map_itr != ticker_vec.end(); map_itr++)
        {
            std::string url = "https://www.deribit.com/api/v2/public/get_order_book?depth=20&instrument_name=";
            url += *map_itr;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            //"code":"200000"
            if(d.IsObject() && d.HasMember("result"))
            {
                auto& tick = d["result"];
                uint64_t sequence = tick["change_id"].GetInt64();

                LFPriceBook20Field priceBook {0};
                strcpy(priceBook.ExchangeID, "deribit");
                strncpy(priceBook.InstrumentID, (*map_itr).c_str(),std::min(sizeof(priceBook.InstrumentID)-1, (*map_itr).size()));
                if(tick.HasMember("bids") && tick["bids"].IsArray())
                {
                    auto& bids = tick["bids"];
                    int len = std::min((int)bids.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.BidLevels[i].price = std::round(bids[i][0].GetDouble() * scale_offset);
                        priceBook.BidLevels[i].volume = std::round(bids[i][1].GetDouble() * scale_offset);
                    }
                    priceBook.BidLevelCount = len;
                }
                if (tick.HasMember("asks") && tick["asks"].IsArray())
                {
                    auto& asks = tick["asks"];
                    int len = std::min((int)asks.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.AskLevels[i].price = std::round(asks[i][0].GetDouble() * scale_offset);
                        priceBook.AskLevels[i].volume = std::round(asks[i][1].GetDouble() * scale_offset);
                    }
                    priceBook.AskLevelCount = len;
                }

                BookMsg bookmsg;
                bookmsg.InstrumentID = *map_itr;
                bookmsg.sequence = sequence;
                bookmsg.time = getTimestamp();
                std::unique_lock<std::mutex> lck_rest_book(rest_book_mutex);
                rest_book_vec.push_back(bookmsg);
                lck_rest_book.unlock();

                on_price_book_update_from_rest(&priceBook);
            }
        }
        lck1.unlock();
    }
    

}

void MDEngineDeribit::check_snapshot()
{
    std::vector<BookMsg>::iterator rest_it;
    std::unique_lock<std::mutex> lck_rest_book(rest_book_mutex);
    for(rest_it = rest_book_vec.begin();rest_it != rest_book_vec.end();){
        int64_t now = getTimestamp();
        std::unique_lock<std::mutex> lck_ws_book(ws_book_mutex);
        auto map_itr = ws_book_map.find(rest_it->InstrumentID);
        if(map_itr != ws_book_map.end()){
            std::map<uint64_t, int64_t>::iterator ws_it;
            for(ws_it = map_itr->second.begin(); ws_it != map_itr->second.end();){
                //if(now - ws_it->second > 10000){
                if(ws_it->first < rest_it->sequence){
                    KF_LOG_INFO(logger,"erase old");
                    ws_it = map_itr->second.erase(ws_it);
                    continue;
                }else if(ws_it->first == rest_it->sequence){
                    if(ws_it->second - rest_it->time > snapshot_check_s * 1000){
                        KF_LOG_INFO(logger, "ws snapshot is later than rest snapshot");
                        string errorMsg = "ws snapshot is later than rest snapshot";
                        write_errormsg(115,errorMsg);
                    }
                    KF_LOG_INFO(logger, "same_book:"<<rest_it->InstrumentID);
                    KF_LOG_INFO(logger,"ws_time="<<ws_it->second<<" rest_time="<<rest_it->time);                    
                }
                ws_it++;
            }
        }
        lck_ws_book.unlock();
        rest_it = rest_book_vec.erase(rest_it);
    }
    lck_rest_book.unlock();
}

int64_t last_rest_time = 0;
int64_t last_funding_time = 0;
void MDEngineDeribit::rest_loop()
{
        while(isRunning)
        {
            int64_t now = getTimestamp();
            if((now - last_rest_time) >= rest_get_interval_ms)
            {
                last_rest_time = now;
                get_snapshot_via_rest();
                check_snapshot();
            }
            if((now - last_funding_time) >= funding_get_interval_s * 1000)
            {
                last_funding_time = now;
                get_funding_rest(now);
            }
        }
}

void MDEngineDeribit::loop()
{
    while(isRunning)
    {
        int n = lws_service( context, rest_get_interval_ms );
        std::cout << " 3.1415 loop() lws_service (n)" << n << std::endl;
        int errorId = 0;
        string errorMsg = "";
        /*quest3 edited by fxw starts here*/
        /*判断是否在设定时间内更新与否，*/
         //114错误
        int64_t now = getTimestamp();
        //KF_LOG_INFO(logger, "quest3: update check ");
        /*if ((now - timestamp) > refresh_normal_check_book_s * 1000)
        {
                errorId = 114;
                errorMsg = "failed price book update";
                KF_LOG_INFO(logger, "orderbook max refresh wait time exceeded");
                write_errormsg(errorId,errorMsg);
                timestamp = now;
        }*/

        std::unique_lock<std::mutex> lck(trade_mutex);
        std::map<std::string,int64_t>::iterator it;
        for(it = control_trade_map.begin(); it != control_trade_map.end(); it++){
            if((now - it->second) > refresh_normal_check_trade_s * 1000){
                errorId = 115;
                errorMsg = it->first + " trade max refresh wait time exceeded";
                KF_LOG_INFO(logger,"115"<<errorMsg); 
                write_errormsg(errorId,errorMsg);
                it->second = now;                   
            }
        }
        lck.unlock();

        std::unique_lock<std::mutex> lck1(book_mutex);
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
        lck1.unlock();
       
    }
}

BOOST_PYTHON_MODULE(libderibitmd)
{
    using namespace boost::python;
    class_<MDEngineDeribit, boost::shared_ptr<MDEngineDeribit> >("Engine")
            .def(init<>())
            .def("init", &MDEngineDeribit::initialize)
            .def("start", &MDEngineDeribit::start)
            .def("stop", &MDEngineDeribit::stop)
            .def("logout", &MDEngineDeribit::logout)
            .def("wait_for_stop", &MDEngineDeribit::wait_for_stop);
}
