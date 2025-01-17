#include "TDEngineBinance.h"
#include "longfist/ctp.h"
#include "longfist/LFUtils.h"
#include "TypeConvert.hpp"
#include <boost/algorithm/string.hpp>

#include <writer.h>
#include <stringbuffer.h>
#include <document.h>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <cpr/cpr.h>
#include <chrono>
#include <queue>
#include <map>
#include <utility>
#include "../../utils/crypto/openssl_util.h"

using cpr::Delete;
using cpr::Get;
using cpr::Url;
using cpr::Body;
using cpr::Header;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;
using cpr::Timeout;
using cpr::Interface;
using cpr::Put;

using rapidjson::StringRef;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using std::string;
using std::to_string;
using std::stod;
using std::stoi;
using utils::crypto::hmac_sha256;
using utils::crypto::hmac_sha256_byte;
using utils::crypto::base64_encode;


#define HTTP_CONNECT_REFUSED 429
#define HTTP_CONNECT_BANS	418
#define HTTP_CONNECT_ERROR	403
#define HTTP_GATEWAY_TIMEOUT 504

USING_WC_NAMESPACE

int orderid=1;

static TDEngineBinance* global_td = nullptr;


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
            if(global_td)
            {
                global_td->on_lws_data(wsi, (const char*)in, len);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_CLOSED:
        {
            if(global_td) {
                global_td->on_lws_connection_error(wsi);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE_PONG:
        {
            //std::cout << "3.1415926 LWS_CALLBACK_CLIENT_RECEIVE_PONG, reason = " << reason << std::endl;
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {
            if(global_td)
            {
                global_td->lws_write_subscribe(wsi);
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
            std::cout << "in: " << in << std::endl;
            if(global_td)
            {
                global_td->on_lws_connection_error(wsi);
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
                        "td-protocol",
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

AccountUnitBinance::AccountUnitBinance()
{
    mutex_weight = new std::mutex();
    mutex_handle_429 = new std::mutex();
    mutex_order_and_trade = new std::mutex();
    mutex_data_map = new std::mutex();
    mutex_order_count = new std::mutex();
}
AccountUnitBinance::~AccountUnitBinance()
{
    if(nullptr != mutex_weight)
        delete mutex_weight;
    if(nullptr != mutex_handle_429)
        delete mutex_handle_429;
    if(nullptr != mutex_order_and_trade)
        delete mutex_order_and_trade;
    if(nullptr != mutex_data_map)
        delete mutex_data_map;
    if(nullptr != mutex_order_count)
        delete mutex_order_count;
}
AccountUnitBinance::AccountUnitBinance(const AccountUnitBinance& source)
{
    api_key =source.api_key;
    secret_key = source.secret_key;
    listenKey= source.listenKey;
    // internal flags
    logged_in = source.logged_in;
    newOrderStatus = source.newOrderStatus;
    pendingOrderStatus= source.pendingOrderStatus;
    newTradeStatus= source.newTradeStatus;
    pendingTradeStatus= source.pendingTradeStatus;

    newOnRtnTrades= source.newOnRtnTrades;
    pendingOnRtnTrades= source.pendingOnRtnTrades;
    whiteListInstrumentIDs= source.whiteListInstrumentIDs;
    sendOrderFilters= source.sendOrderFilters;
    ordersMap= source.ordersMap;
    // the trade id that has been called on_rtn_trade. Do not send it again.
    newSentTradeIds= source.newSentTradeIds;
    sentTradeIds= source.sentTradeIds;

    coinPairWhiteList= source.coinPairWhiteList;
    positionWhiteList= source.positionWhiteList;


    order_total_count = source.order_total_count;
    time_queue= source.time_queue;


    weight_count = source.weight_count;
    mutex_weight = new std::mutex();
    weight_data_queue = source.weight_data_queue;

    
    bHandle_429 = source.bHandle_429;
    mutex_handle_429 = new std::mutex();
    startTime_429 = source.startTime_429;
    mutex_order_and_trade = new std::mutex();
    context = source.context;
    websocketConn= source.websocketConn;
    mutex_data_map = new std::mutex();
    mutex_order_count = new std::mutex();
}
std::mutex http_mutex;
std::mutex account_mutex;
std::mutex cancel_mutex;
std::mutex interface_mutex; 
//std::mutex mutex_m_switch_interfaceMgr ;//m_interfaceMgr
//std::mutex mutex_account ;//account_units
std::condition_variable cancel_cv;

TDEngineBinance::TDEngineBinance(): ITDEngine(SOURCE_BINANCE)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.Binance");
    KF_LOG_INFO(logger, "[ATTENTION] default to confirm settlement and no authentication!");

    
    
}

TDEngineBinance::~TDEngineBinance()
{
    // if(m_ThreadPoolPtr_insert!=nullptr) delete m_ThreadPoolPtr_insert;
    // if(m_ThreadPoolPtr_action!=nullptr) delete m_ThreadPoolPtr_action;
    if(m_ThreadPoolPtr!=nullptr) delete m_ThreadPoolPtr;
}

void TDEngineBinance::init()
{
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
}

void TDEngineBinance::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineBinance::resize_accounts(int account_num)
{
    
}

TradeAccount TDEngineBinance::load_account(int idx, const json& j_config)
{

    KF_LOG_INFO(logger, "[load_account]");

	string interfaces;
    
	int interface_timeout = 300000;
	if(j_config.find("interfaces") != j_config.end()) {
		interfaces = j_config["interfaces"].get<string>();
	}

	if(j_config.find("interface_timeout_ms") != j_config.end()) {
		interface_timeout = j_config["interface_timeout_ms"].get<int>();
	}

	if(j_config.find("interface_switch") != j_config.end()) {
		m_interface_switch = j_config["interface_switch"].get<int>();
	}

	KF_LOG_INFO(logger, "[load_account] interface switch: " << m_interface_switch);
	if (m_interface_switch > 0) {
		m_interfaceMgr.init(interfaces, interface_timeout);
		m_interfaceMgr.print();
        std::vector<std::string> ip_addrs(m_interfaceMgr.getInterface(interfaces));
        for(int i=0;i<m_interface_switch;i++)
        {
            interface_map[ip_addrs[i]];
        }
	}

	
    //string api_key = j_config["APIKey"].get<string>();
    //string secret_key = j_config["SecretKey"].get<string>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

    if(j_config.find("baseUrl") != j_config.end()) {
        restBaseUrl = j_config["baseUrl"].get<string>();
    }
    if(j_config.find("wsUrl") != j_config.end()) {
        wsBaseUrl = j_config["wsUrl"].get<string>();
    }
    if(j_config.find("sync_time_interval") != j_config.end()) {
        SYNC_TIME_DEFAULT_INTERVAL = j_config["sync_time_interval"].get<int>();
    }
   
    KF_LOG_INFO(logger, "[load_account] (SYNC_TIME_DEFAULT_INTERVAL)" << SYNC_TIME_DEFAULT_INTERVAL);
    if(j_config.find("sub") != j_config.end()){
        nlohmann::json sub = j_config["sub"].get<nlohmann::json>();
        if(sub.is_object()){
            for (nlohmann::json::iterator it = sub.begin(); it != sub.end(); ++it){
                KF_LOG_INFO(logger,"it.key()"<<it.key()<<" it.value()"<<it.value());
                std::string name = it.key();
                char_64 cname;
                strcpy(cname,name.c_str());
                KEY key;
                key.APIKey = sub[cname]["APIKey"].get<string>();
                key.SecretKey = sub[cname]["SecretKey"].get<string>();
                sub_map.insert(make_pair(name,key));                    
            }
        }
    }

    if(j_config.find("exchange_shift_ms") != j_config.end()) {
        exchange_shift_ms = j_config["exchange_shift_ms"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (exchange_shift_ms)" << exchange_shift_ms);

    if(j_config.find("order_insert_recvwindow_ms") != j_config.end()) {
        order_insert_recvwindow_ms = j_config["order_insert_recvwindow_ms"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (order_insert_recvwindow_ms)" << order_insert_recvwindow_ms);

    if(j_config.find("order_action_recvwindow_ms") != j_config.end()) {
        order_action_recvwindow_ms = j_config["order_action_recvwindow_ms"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (order_action_recvwindow_ms)" << order_action_recvwindow_ms);

    if(j_config.find("order_count_per_second") != j_config.end()) {
        order_count_per_second = j_config["order_count_per_second"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (order_count_per_second)" << order_count_per_second);

    if(j_config.find("request_weight_per_minute") != j_config.end()) {
        request_weight_per_minute = j_config["request_weight_per_minute"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (request_weight_per_minute)" << request_weight_per_minute);

    if(j_config.find("prohibit_order_ms") != j_config.end()) {
        prohibit_order_ms = j_config["prohibit_order_ms"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (prohibit_order_ms)" << prohibit_order_ms);

    if(j_config.find("default_429_rest_interval_ms") != j_config.end()) {
        default_429_rest_interval_ms = j_config["default_429_rest_interval_ms"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (default_429_rest_interval_ms)" << default_429_rest_interval_ms);

    if(j_config.find("m_CurrentTDIndex") != j_config.end()) {
       m_CurrentTDIndex = j_config["m_CurrentTDIndex"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (m_CurrentTDIndex)" << m_CurrentTDIndex);
    // int thread_pool_size = 0;
    // if(j_config.find("thread_pool_size_insert") != j_config.end()) {
    //     thread_pool_size = j_config["thread_pool_size_insert"].get<int>();
    // }
    // if(thread_pool_size > 0)
    // {
    //     m_ThreadPoolPtr_insert = new ThreadPool(thread_pool_size);
    // }
    // KF_LOG_INFO(logger, "[load_account] (thread_pool_size_insert)" << thread_pool_size);


    // thread_pool_size = 0;
    // if(j_config.find("thread_pool_size_action") != j_config.end()) {
    //     thread_pool_size = j_config["thread_pool_size_action"].get<int>();
    // }
    // if(thread_pool_size > 0)
    // {
    //     m_ThreadPoolPtr_action = new ThreadPool(thread_pool_size);
    // }
    // KF_LOG_INFO(logger, "[load_account] (thread_pool_size_action)" << thread_pool_size);

    int thread_pool_size = 0;
    if(j_config.find("thread_pool_size") != j_config.end()) {
        thread_pool_size = j_config["thread_pool_size"].get<int>();
    }
    if(thread_pool_size > 0)
    {
        m_ThreadPoolPtr = new ThreadPool(thread_pool_size);
    }
    KF_LOG_INFO(logger, "[load_account] (thread_pool_size)" << thread_pool_size);

    if(j_config.find("UFR_limit") != j_config.end()) {
        UFR_limit = j_config["UFR_limit"].get<float>();
    }
    KF_LOG_INFO(logger, "[load_account] (UFR_limit)" << UFR_limit);

    if(j_config.find("UFR_order_lower_limit") != j_config.end()) {
        UFR_order_lower_limit = j_config["UFR_order_lower_limit"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (UFR_order_lower_limit)" << UFR_order_lower_limit);

     if(j_config.find("GCR_limit") != j_config.end()) {
        GCR_limit = j_config["GCR_limit"].get<float>();
    }
    KF_LOG_INFO(logger, "[load_account] (GCR_limit)" << GCR_limit);

    if(j_config.find("GCR_order_lower_limit") != j_config.end()) {
        GCR_order_lower_limit = j_config["GCR_order_lower_limit"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (GCR_order_lower_limit)" << GCR_order_lower_limit);


 
 
    if(j_config.find("max_rest_retry_times") != j_config.end()) {
        max_rest_retry_times = j_config["max_rest_retry_times"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (max_rest_retry_times)" << max_rest_retry_times);


    if(j_config.find("retry_interval_milliseconds") != j_config.end()) {
        retry_interval_milliseconds = j_config["retry_interval_milliseconds"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (retry_interval_milliseconds)" << retry_interval_milliseconds);

    if(j_config.find("cancel_timeout_ms") != j_config.end()) {
        cancel_timeout_milliseconds = j_config["cancel_timeout_ms"].get<int>();
    }
    if(j_config.find("account_type") != j_config.end())
    {
        if((j_config["account_type"].get<string>() == "margin")){
            account_type = j_config["account_type"].get<string>();
        }
    }
    KF_LOG_INFO(logger, "[load_account] (cancel_timeout_ms)" << cancel_timeout_milliseconds);

    


    // internal load
    auto iter = j_config.find("users");
    if (iter != j_config.end() && iter.value().size() > 0)
    { 
        for (auto& j_account: iter.value())
        {
            AccountUnitBinance unit;
            unit.api_key = j_account["APIKey"].get<string>();
            unit.secret_key = j_account["SecretKey"].get<string>();

            unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
            //unit.coinPairWhiteList.Debug_print();

            unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
            //unit.positionWhiteList.Debug_print();
        
            if(unit.coinPairWhiteList.Size() == 0) {
                //display usage:
                KF_LOG_ERROR(logger, "TDEngineBinance::load_account: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
                KF_LOG_ERROR(logger, "\"whiteLists\":{");
                KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
                KF_LOG_ERROR(logger, "    \"btc_usdt\": \"BTCUSDT\",");
                KF_LOG_ERROR(logger, "     \"etc_eth\": \"ETCETH\"");
                KF_LOG_ERROR(logger, "},");
            }
            else
            {
                //cancel all openning orders on TD startup
                std::unordered_map<std::string, std::string>::iterator map_itr;
                map_itr = unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
                while(map_itr != unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end())
                {
                    KF_LOG_INFO(logger, "[load_account] (api_key)" << unit.api_key << " (cancel_all_orders of instrumentID) of exchange coinpair: " << map_itr->second);

                    Document d;
                    if( account_type == "margin")
                    {
                        get_margin_open_orders(unit, map_itr->second.c_str(), d);
                        KF_LOG_INFO(logger, "[load_margin_account] print get_open_orders");
                    }
                    else
                    {
                        get_open_orders(unit, map_itr->second.c_str(), d);
                        KF_LOG_INFO(logger, "[load_account] print get_open_orders");
                    }
                    KF_LOG_INFO(logger, "[load_account] print get_open_orders");
                    printResponse(d);

                    if(!d.HasParseError() && d.IsArray()) { // expected success response is array
                        size_t len = d.Size();
                        KF_LOG_INFO(logger, "[load_account][get_open_orders] (length)" << len);
                        for (size_t i = 0; i < len; i++) {
                            if(d.GetArray()[i].IsObject() && d.GetArray()[i].HasMember("symbol") && d.GetArray()[i].HasMember("clientOrderId"))
                            {
                                if(d.GetArray()[i]["symbol"].IsString() && d.GetArray()[i]["clientOrderId"].IsString())
                                {
                                    std::string symbol = d.GetArray()[i]["symbol"].GetString();
                                    std::string orderRef = d.GetArray()[i]["clientOrderId"].GetString();
                                    Document cancelResponse;
                                    if( account_type == "margin")
                                    {
                                        cancel_margin_order(unit, symbol.c_str(), 0, orderRef.c_str(), "", cancelResponse);
                                        KF_LOG_INFO(logger, "[load_account] cancel_margin_order:");
                                    }
                                    else
                                    {
                                        cancel_order(unit, symbol.c_str(), 0, orderRef.c_str(), "", cancelResponse);

                                        KF_LOG_INFO(logger, "[load_account] cancel_order:");
                                    }
                                    printResponse(cancelResponse);
                                    int errorId = 0;
                                    std::string errorMsg = "";
                                    if(d.HasParseError() )
                                    {
                                        errorId = 100;
                                        errorMsg = "cancel_order http response has parse error. please check the log";
                                        KF_LOG_ERROR(logger, "[load_account] cancel_order error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
                                    }
                                    if(!cancelResponse.HasParseError() && cancelResponse.IsObject() && cancelResponse.HasMember("code") && cancelResponse["code"].IsNumber())
                                    {
                                        errorId = cancelResponse["code"].GetInt();
                                        if(cancelResponse.HasMember("msg") && cancelResponse["msg"].IsString())
                                        {
                                            errorMsg = cancelResponse["msg"].GetString();
                                        }

                                        KF_LOG_ERROR(logger, "[load_account] cancel_order failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
                                    }
                                }
                            }
                        }
                    }

                    map_itr++;
                }
            }
        //    std::unique_lock<std::mutex> lck(mutex_account);
            account_units.emplace_back(unit);
          //  lck.unlock();
        }
    }
    else
    {
        KF_LOG_ERROR(logger, "[load_account] no tarde account info !");
    }
    
    // set up
   // std::unique_lock<std::mutex> lck(mutex_account);
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, account_units[0].api_key.c_str(), 15);
    strncpy(account.Password, account_units[0].secret_key.c_str(), 20);
    genUniqueKey ();
    KF_LOG_INFO(logger, "[load_account] SUCCESS !");
    return account;
}

void TDEngineBinance::genUniqueKey ()
{
    struct tm cur_time= getCurLocalTime();
    char key[11]{0};
    snprintf((char*)key,11,"%02d%02d%02d%02d%02d",cur_time.tm_sec,cur_time.tm_min,cur_time.tm_hour,cur_time.tm_mday,m_CurrentTDIndex);
    m_uniqueKey  = key;
}
std::atomic<uint64_t> nIndex{0};
std::string TDEngineBinance::genClientid (const std::string &orderRef)
{
    return m_uniqueKey + orderRef;
}

void TDEngineBinance::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    //std::unique_lock<std::mutex> lck(mutex_account);
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitBinance& unit = account_units[idx];

        KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
        if (!unit.logged_in)
        {
            //exchange infos
            Document doc;
            get_exchange_infos(unit, doc);
            KF_LOG_INFO(logger, "[connect] get_exchange_infos");

            if(loadExchangeOrderFilters(unit, doc))
            {
                unit.logged_in = true;
                lws_login(unit, timeout_nsec);
            } else {
                KF_LOG_ERROR(logger, "[connect] logged_in = false for loadExchangeOrderFilters return false");
            }
            debug_print(unit.sendOrderFilters);
        }
    }
    //sync time of exchange
    getTimeDiffOfExchange(account_units[0]);
}

bool TDEngineBinance::loadExchangeOrderFilters(AccountUnitBinance& unit, Document &doc)
{
    KF_LOG_INFO(logger, "[loadExchangeOrderFilters]");
    if(doc.HasParseError() || !doc.IsObject())
    {
        return false;
    }
    if(doc.HasMember("symbols") && doc["symbols"].IsArray())
    {
        int symbolsCount = doc["symbols"].Size();
        for (int i = 0; i < symbolsCount; i++) {
            const rapidjson::Value& sym = doc["symbols"].GetArray()[i];
            std::string symbol = sym["symbol"].GetString();
	/*    std::string symbol1 = sym["symbol"].GetString();*/
            if(sym.IsObject() && sym.HasMember("filters") && sym["filters"].IsArray()) {
                int filtersCount = sym["filters"].Size();
                for (int j = 0; j < filtersCount; j++) {
                    const rapidjson::Value& filter = sym["filters"].GetArray()[j];
                    if (strcmp("PRICE_FILTER", filter["filterType"].GetString()) == 0) {
                        std::string tickSizeStr =  filter["tickSize"].GetString();
                        //KF_LOG_INFO(logger, "[loadExchangeOrderFilters] sendOrderFilters (symbol)" << symbol <<
                        //                                                                           " (tickSizeStr)" << tickSizeStr);
                        //0.0000100; 0.001;
                        unsigned int locStart = tickSizeStr.find( ".", 0 );
                        unsigned int locEnd = tickSizeStr.find( "1", 0 );
                        if( locStart != string::npos  && locEnd != string::npos ){
                            int num = locEnd - locStart;
                            SendOrderFilter afilter;
                            strncpy(afilter.InstrumentID, symbol.c_str(), 31);
                            afilter.ticksize = num;
                            unit.sendOrderFilters.insert(std::make_pair(symbol, afilter));
                            //KF_LOG_INFO(logger, "[loadExchangeOrderFilters] sendOrderFilters (symbol)" << symbol <<
                            //                                                                          " (tickSizeStr)" << tickSizeStr
                            //                                                                           <<" (tickSize)" << afilter.ticksize);
                        }
                    } 
                    if (strcmp("LOT_SIZE", filter["filterType"].GetString()) == 0) {
                        std::string stepSizeStr =  filter["stepSize"].GetString();
                        //KF_LOG_INFO(logger, "[loadExchangeOrderFilters] sendOrderFilters (symbol)" << symbol <<" (stepSizeStr)" << stepSizeStr);
                        //0.0000100; 0.001;
                        unsigned int locStart = stepSizeStr.find( ".", 0 );
                        unsigned int locEnd = stepSizeStr.find( "1", 0 );
                        if( locStart != string::npos  && locEnd != string::npos ){
                            int num = locEnd - locStart;                
                            auto iter = unit.sendOrderFilters.find(symbol);
                            if(iter == unit.sendOrderFilters.end()){
                                SendOrderFilter afilter;
                                strncpy(afilter.InstrumentID, symbol.c_str(), 31);
                                afilter.stepsize = num;
                                unit.sendOrderFilters.insert(std::make_pair(symbol,afilter));
                            }
                            else{
                                unit.sendOrderFilters[symbol].stepsize = num;
                            }
                           // KF_LOG_INFO(logger, "[loadExchangeOrderFilters] sendOrderFilters (symbol)" << symbol <<" (stepSizeStr)" << stepSizeStr<<" (stepSize)" << afilter.stepsize);
                        }
                    }

                }
            }
        }
    }
    return true;
}

void TDEngineBinance::debug_print(std::map<std::string, SendOrderFilter> &sendOrderFilters)
{
    std::map<std::string, SendOrderFilter>::iterator map_itr = sendOrderFilters.begin();
    while(map_itr != sendOrderFilters.end())
    {
        KF_LOG_INFO(logger, "[debug_print] sendOrderFilters (symbol)" << map_itr->first <<
                                                                                   " (tickSize)" << map_itr->second.ticksize<<" (stepsize)"<<map_itr->second.stepsize);
        map_itr++;
    }
}

SendOrderFilter TDEngineBinance::getSendOrderFilter(AccountUnitBinance& unit, const char *symbol)
{
    auto map_itr = unit.sendOrderFilters.find(symbol);
    if(map_itr != unit.sendOrderFilters.end())
    {
        return map_itr->second;
    }
    SendOrderFilter defaultFilter;
    defaultFilter.ticksize = 8;
    strcpy(defaultFilter.InstrumentID, "notfound");
    return defaultFilter;
}

void TDEngineBinance::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[login]");
    connect(timeout_nsec);
}

void TDEngineBinance::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineBinance::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineBinance::is_logged_in() const
{
    KF_LOG_INFO(logger, "[is_logged_in]");
    //std::unique_lock<std::mutex> lck(mutex_account);
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
  //  lck.unlock();
    return true;
}

bool TDEngineBinance::is_connected() const
{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}



std::string TDEngineBinance::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "BUY";
    } else if (LF_CHAR_Sell == input) {
        return "SELL";
    } else {
        return "UNKNOWN";
    }
}

 LfDirectionType TDEngineBinance::GetDirection(std::string input) {
    if ("BUY" == input) {
        return LF_CHAR_Buy;
    } else if ("SELL" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineBinance::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "LIMIT";
    } else if (LF_CHAR_AnyPrice == input) {
        return "MARKET";
    } else {
        return "UNKNOWN";
    }
}

LfOrderPriceTypeType TDEngineBinance::GetPriceType(std::string input) {
    if ("LIMIT" == input) {
        return LF_CHAR_LimitPrice;
    } else if ("MARKET" == input) {
        return LF_CHAR_AnyPrice;
    } else {
        return '0';
    }
}

std::string TDEngineBinance::GetTimeInForce(const LfTimeConditionType& input) {
    if (LF_CHAR_IOC == input) {
      return "IOC";
    } else if (LF_CHAR_GTC == input) {
      return "GTC";
    } else if (LF_CHAR_FOK == input) {
      return "FOK";
    } else {
      return "UNKNOWN";
    }
}

LfTimeConditionType TDEngineBinance::GetTimeCondition(std::string input) {
    if ("IOC" == input) {
      return LF_CHAR_IOC;
    } else if ("GTC" == input) {
      return LF_CHAR_GTC;
    } else if ("FOK" == input) {
      return LF_CHAR_FOK;
    } else {
      return '0';
    }
}

LfOrderStatusType TDEngineBinance::GetOrderStatus(std::string input) {
    if ("NEW" == input) {
      return LF_CHAR_NotTouched;
    } else if ("PARTIALLY_FILLED" == input) {
      return LF_CHAR_PartTradedQueueing;
    } else if ("FILLED" == input) {
      return LF_CHAR_AllTraded;
    } else if ("CANCELED" == input) {
      return LF_CHAR_Canceled;
    } else if ("PENDING_CANCEL" == input) {
      return LF_CHAR_NotTouched;
    } else if ("REJECTED" == input) {
      return LF_CHAR_Error;
    } else if ("EXPIRED" == input) {
      return LF_CHAR_Error;
    } else {
      return LF_CHAR_NotTouched;
    }
}
size_t current_account_idx = -1;
AccountUnitBinance& TDEngineBinance::get_current_account()
{
    current_account_idx++;
    current_account_idx %= account_units.size();
    return account_units[current_account_idx];
}
AccountUnitBinance& get_account_from_orderref(std::string& ref)
{

}

/**
 * req functions
 */
void TDEngineBinance::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_investor_position]");
    //std::unique_lock<std::mutex> lck(mutex_account);
    AccountUnitBinance& unit = account_units[0];
    //lck.unlock();
    KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key);

    LFRspPositionField pos;
    memset(&pos, 0, sizeof(LFRspPositionField));
    strncpy(pos.BrokerID, data->BrokerID, 32);
    strncpy(pos.InvestorID, data->InvestorID, 32);
    strncpy(pos.InstrumentID, data->InstrumentID, 31);
    pos.PosiDirection = LF_CHAR_Long;
    pos.Position = 0;

    int errorId = 0;
    std::string errorMsg = "";
    
    KF_LOG_INFO(logger, "[req_investor_position] (data->BrokerID)" << data->BrokerID);    
 
    // User Balance
    int response_code;
    Document d;
    if( !strcmp(data->BrokerID, "master-main"))
    {
        response_code = get_account(unit, d, pos, requestId);
        KF_LOG_INFO(logger, "[req_investor_position] get_account");
    }
    else if( !strcmp(data->BrokerID, "master-margin")){
        response_code = get_margin_account(unit, d, pos, requestId);
        KF_LOG_INFO(logger, "[req_investor_position] get_margin_account");
    }
    else if( !strcmp(data->BrokerID, "sub-main")){
        response_code = get_sub_account(data->InvestorID, unit, d, pos, requestId);
        KF_LOG_INFO(logger, "[req_investor_position] get_sub_account");
    }
    else if( !strcmp(data->BrokerID, "sub-margin")){
        get_sub_margin_account(data->InvestorID, unit, d,pos, requestId);
        KF_LOG_INFO(logger, "[req_investor_position] get_sub_margin_account");
    }
    else{
        errorId=202;
        errorMsg= "this account is not main or margin or sub, get position error.";
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_BINANCE, 1, requestId, errorId, errorMsg.c_str());
        return ;
    }
    printResponse(d);

    if(response_code == HTTP_GATEWAY_TIMEOUT){
        KF_LOG_INFO(logger,"Gateway Timeout");
        return;
    }

    if(d.HasParseError() )
    {
        errorId=100;
        if( !strcmp(data->BrokerID, "master-main")){
            errorMsg= "get_account http response has parse error. please check the log";
            KF_LOG_ERROR(logger, "[req_investor_position] get_account error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
        else if( !strcmp(data->BrokerID, "master-margin")){
            errorMsg= "get_margin_account http response has parse error. please check the log";
            KF_LOG_ERROR(logger, "[req_investor_position] get_margin_account error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
        else if( !strcmp(data->BrokerID, "sub-main")){
            errorMsg= "get_sub_account http response has parse error. please check the log";
            KF_LOG_ERROR(logger, "[req_investor_position] get_sub_account error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
        else if( !strcmp(data->BrokerID, "sub-margin")){
            errorMsg= "get_sub_margin_account http response has parse error. please check the log";
            KF_LOG_ERROR(logger, "[req_investor_position] get_sub_account error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
    }

    if(!d.HasParseError() && d.IsObject() && d.HasMember("code") && d["code"].IsNumber())
    {
        errorId = d["code"].GetInt();
        if(d.HasMember("msg") && d["msg"].IsString())
        {
            errorMsg = d["msg"].GetString();
        }
        if( !strcmp(data->BrokerID, "master-main")){
            KF_LOG_ERROR(logger, "[req_investor_position] get_account failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
        else if( !strcmp(data->BrokerID, "master-margin")){
            KF_LOG_ERROR(logger, "[req_investor_position] get_margin_account failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
        else if( !strcmp(data->BrokerID, "sub-main")){
            KF_LOG_ERROR(logger, "[req_investor_position] get_sub_account failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
        else if( !strcmp(data->BrokerID, "sub-margin")){
            KF_LOG_ERROR(logger, "[req_investor_position] get_sub_margin_account failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
    }
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_BINANCE, 1, requestId);


    std::vector<LFRspPositionField> tmp_vector;
    if( !strcmp(data->BrokerID, "master-main")){
        if(!d.HasParseError() && d.IsObject() && d.HasMember("balances"))
        {
        int len = d["balances"].Size();
        for ( int i  = 0 ; i < len ; i++ ) {
            std::string symbol = d["balances"].GetArray()[i]["asset"].GetString();
            std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
            if(ticker.length() > 0) {
                strncpy(pos.InstrumentID, ticker.c_str(), 31);
                pos.Position = std::round(stod(d["balances"].GetArray()[i]["free"].GetString()) * scale_offset);
                tmp_vector.push_back(pos);
                KF_LOG_INFO(logger,  "[connect] (symbol)" << symbol << " (free)" <<  d["balances"].GetArray()[i]["free"].GetString()
                                                          << " (locked)" << d["balances"].GetArray()[i]["locked"].GetString());
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
            }
        }
        }
    }
    else if(!strcmp(data->BrokerID, "sub-main")){
        if(!d.HasParseError() && d.IsObject() && d.HasMember("balances"))
        {
        int len = d["balances"].Size();
        for ( int i  = 0 ; i < len ; i++ ) {
            std::string symbol = d["balances"].GetArray()[i]["asset"].GetString();
            std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
            if(ticker.length() > 0) {
                strncpy(pos.InstrumentID, ticker.c_str(), 31);
                pos.Position = std::round((d["balances"].GetArray()[i]["free"].GetDouble()) * scale_offset);
                tmp_vector.push_back(pos);
                KF_LOG_INFO(logger,  "[connect] (symbol)" << symbol << " (free)" <<  d["balances"].GetArray()[i]["free"].GetDouble()
                                                          << " (locked)" << d["balances"].GetArray()[i]["locked"].GetDouble());
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
            }
        }
        }
    }
    else if( !strcmp(data->BrokerID, "master-margin") ){
        if(!d.HasParseError() && d.IsObject() && d.HasMember("userAssets"))
        {
        int len = d["userAssets"].Size();
        for ( int i  = 0 ; i < len ; i++ ) {
            std::string symbol = d["userAssets"].GetArray()[i]["asset"].GetString();
            std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
            if(ticker.length() > 0) {
                strncpy(pos.InstrumentID, ticker.c_str(), 31);
                pos.Position = std::round(stod(d["userAssets"].GetArray()[i]["free"].GetString()) * scale_offset);                
                tmp_vector.push_back(pos);
                KF_LOG_INFO(logger,  "[connect] (symbol)" << symbol << " (free)" <<  d["userAssets"].GetArray()[i]["free"].GetString()
                                                          << " (locked)" << d["userAssets"].GetArray()[i]["locked"].GetString());
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
            }
        }
        }
    } 
    else if( !strcmp(data->BrokerID, "sub-margin") ){
        if(!d.HasParseError() && d.IsObject() && d.HasMember("marginUserAssetVoList"))
        {
            int len = d["marginUserAssetVoList"].Size();
            auto& dataArray = d["marginUserAssetVoList"];
            for ( int i  = 0 ; i < len ; i++ ) {
                std::string symbol = dataArray.GetArray()[i]["asset"].GetString();
                std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
                if(ticker.length() > 0) {
                    strncpy(pos.InstrumentID, ticker.c_str(), 31);
                    pos.Position = std::round(stod(dataArray.GetArray()[i]["free"].GetString()) * scale_offset);                
                    tmp_vector.push_back(pos);
                    KF_LOG_INFO(logger,  "req_investor_position (symbol)" << symbol << " (free)" <<  dataArray.GetArray()[i]["free"].GetString()
                                                            << " (locked)" << dataArray.GetArray()[i]["locked"].GetString());
                    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
                }
            }
        }
    }      

    bool findSymbolInResult = false;
    //send the filtered position
    int position_count = tmp_vector.size();
    for (int i = 0; i < position_count; i++)
    {
        on_rsp_position(&tmp_vector[i], i == (position_count - 1), requestId, errorId, errorMsg.c_str());
        findSymbolInResult = true;
    }

    if(!findSymbolInResult)
    {
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
    }
    if(errorId != 0)
    {
        raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_BINANCE, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineBinance::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}


int64_t TDEngineBinance::fixPriceTickSize(int keepPrecision, int64_t price, bool isBuy)
{
    //the 8 is come from 1e8.
    if(keepPrecision == 8) return price;
    int removePrecisions = (8 - keepPrecision);
    double cutter =  pow(10, removePrecisions);
    int64_t new_price = 0;
    if(isBuy)
    {
        new_price = std::floor(price / cutter) * cutter;
    } else {
        new_price = std::ceil(price / cutter) * cutter;
    }
    return new_price;
}

int64_t TDEngineBinance::fixVolumeStepSize(int keepPrecision, int64_t volume, bool isBuy)
{
    //the 8 is come from 1e8.
    if(keepPrecision == 8) return volume;
    int removePrecisions = (8 - keepPrecision);
    double cutter =  pow(10, removePrecisions);
    int64_t new_volume = 0;
    new_volume = std::floor(volume / cutter) * cutter;
    return new_volume;
}


void TDEngineBinance::send_order_thread(SendOrderParam param, const LFInputOrderField data,std::string account_type,int requestId,int64_t timestamp, double fixedVolume,double fixedPrice)
{
    KF_LOG_DEBUG(logger, "[send_order_thread] current thread is:" <<std::this_thread::get_id()<<" current CPU is  "<<sched_getcpu());
    Document d;//全局，之后函数会用到
    int errorId =0;
    std::string errorMsg = "";
    LFRtnOrderField rtn_order;
    memset(&rtn_order, 0, sizeof(LFRtnOrderField));
    strcpy(rtn_order.ExchangeID, "binance");
    strncpy(rtn_order.UserID, param.unit->api_key.c_str(), 16);
    strncpy(rtn_order.InstrumentID, data.InstrumentID, 31);
    rtn_order.Direction = data.Direction;
    rtn_order.TimeCondition = data.TimeCondition;
    rtn_order.OrderPriceType = data.OrderPriceType;
    strncpy(rtn_order.OrderRef, data.OrderRef, 13);
    rtn_order.VolumeTraded = 0;
    rtn_order.VolumeTotalOriginal = fixedVolume;
    rtn_order.VolumeTotal = fixedVolume;
    rtn_order.LimitPrice = fixedPrice;
    rtn_order.RequestID = requestId;
    rtn_order.OrderStatus = LF_CHAR_Unknown;
    std::unique_lock<std::mutex> lck_acc1(*(param.unit->mutex_order_and_trade));
    param.unit->newordersMap.insert(std::make_pair(param.newClientOrderId.c_str(),rtn_order));
    lck_acc1.unlock();

    int response_code;
    /*if(!param.unit->is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        return;
    }else{*/
        if( account_type == "margin")
        {
            response_code = send_margin_order(*param.unit, param.symbol.c_str(), param.side.c_str(), param.type.c_str(),
            param.timeInForce.c_str(), param.quantity, param.price, param.stopPrice, param.newClientOrderId.c_str(),
            param.icebergQty, param.active, d,data , requestId, param.isPostOnly);
            KF_LOG_INFO(logger, "[req_order_insert] send_margin_order");
        }
        else
        {
            response_code = send_order(*param.unit, param.symbol.c_str(), param.side.c_str(), param.type.c_str(),
            param.timeInForce.c_str(), param.quantity, param.price, param.newClientOrderId.c_str(),param.stopPrice, 
            param.icebergQty, d ,data , requestId, param.isPostOnly);
            KF_LOG_INFO(logger, "[req_order_insert] send_order");
        }
    //}
    //    KF_LOG_INFO(logger, "[req_order_insert] send_order");
    //    printResponse(d);
    if(response_code == HTTP_GATEWAY_TIMEOUT){
        KF_LOG_INFO(logger,"Gateway Timeout");
        return;
    }

    if(d.HasParseError() )
    {
        errorId=100;
        errorMsg= "send_order http response has parse error. please check the log";
        KF_LOG_ERROR(logger, "[req_order_insert] send_order error! (rid)" << requestId << " (errorId)" <<
                                                                          errorId << " (errorMsg) " << errorMsg);
    }
    if(!d.HasParseError() && d.IsObject() && d.HasMember("code") && d["code"].IsNumber())
    {
        errorId = d["code"].GetInt();
        if(d.HasMember("msg") && d["msg"].IsString())
        {
            errorMsg = d["msg"].GetString();
        }
        KF_LOG_ERROR(logger, "[req_order_insert] send_order failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }

    //if(errorId != 0)
    {
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());//write —info
    }
    raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BINANCE, 1, requestId, errorId, errorMsg.c_str());

    //paser the order/trade info in the response result
    if(!d.HasParseError() && d.IsObject() && !d.HasMember("code")&&d.HasMember("orderId"))
    {
        //GCR 
        auto tif =  GetTimeInForce(data.TimeCondition);
        if(tif == "GTC")
        {
            std::unique_lock<std::mutex> lck_data_map(*param.unit->mutex_data_map);
            auto it_rate = param.unit->rate_limit_data_map.find(data.InstrumentID);
            if ( it_rate == param.unit->rate_limit_data_map.end())
            {
                KF_LOG_ERROR(logger, "TDEngineBinance::send_order_thread can't find correcr unit" );
                return;
            }
            it_rate->second.mapOrderTime.insert(std::make_pair(data.OrderRef,timestamp));//write  加锁
            lck_data_map.unlock();
        }
        std::unique_lock<std::mutex> lck_acc2(account_mutex);
        mapInsertOrders.insert(std::make_pair(data.OrderRef,param.unit));
        lck_acc2.unlock();
        
        std::string remoteorderId=std::to_string(d["orderId"].GetInt64());
        //order insert success,on_rtn_order with NotTouched status first
        //onRtnNewOrder(&data, *param.unit, requestId,remoteorderId,fixedVolume,fixedPrice);//write 加锁
        std::unique_lock<std::mutex> lck(*(param.unit->mutex_order_and_trade));
        auto it = param.unit->newordersMap.find(rtn_order.OrderRef);
        if(it != param.unit->newordersMap.end())
        {
            rtn_order.OrderStatus = LF_CHAR_NotTouched;
            on_rtn_order(&rtn_order);
            raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
                            source_id, MSG_TYPE_LF_RTN_ORDER_BINANCE,
                            1/*islast*/, (rtn_order.RequestID > 0) ? rtn_order.RequestID: -1);
            param.unit->ordersMap.insert(std::make_pair(remoteorderId,it->second));
            param.unit->newordersMap.erase(it);
            std::unique_lock<std::mutex> lck_data_map(*(param.unit->mutex_data_map));
            //收到委托回报，在这里委托量+1
            param.unit->rate_limit_data_map[data.InstrumentID].order_total+=fixedVolume;
            param.unit->rate_limit_data_map[data.InstrumentID].order_count++;
            //测试日志
            KF_LOG_DEBUG(logger, "[onRtnNewOrder] order_total++ " << " InstrumentID "<< data.InstrumentID <<
                    " current order_total " << param.unit->rate_limit_data_map[data.InstrumentID].order_total <<" trade_total " << param.unit->rate_limit_data_map[data.InstrumentID].trade_total );
            lck_data_map.unlock();
        }

    }
}

void TDEngineBinance::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    //yijinjing::FramePtr frame;
    AccountUnitBinance& unit = get_current_account();
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BINANCE, 1/*ISLAST*/, requestId);

    int errorId = 0;
    std::string errorMsg = "";
    on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 201;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_insert]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());//write in _ info
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BINANCE, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);

    if(!unit.is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        return;
    }

    double stopPrice = 0;
    double icebergQty = 0;
   

    SendOrderFilter filter = getSendOrderFilter(unit, ticker.c_str());

    int64_t fixedPrice = fixPriceTickSize(filter.ticksize, data->LimitPrice, LF_CHAR_Buy == data->Direction);//read 
    int64_t fixedVolume = fixVolumeStepSize(filter.stepsize, data->Volume, LF_CHAR_Buy == data->Direction);//read 


    KF_LOG_DEBUG(logger, "[req_order_insert] SendOrderFilter  (Tid)" << ticker <<
                                                                     " (LimitPrice)" << data->LimitPrice <<
                                                                     " (ticksize)" << filter.ticksize <<
                                                                     " (fixedPrice)" << fixedPrice);

   time_t curTime = time(0);
    //--------------rate limit------------------------------------------
    int64_t timestamp = getTimestamp();
    //若InstrumentId不在map中则添加
    
    std::unique_lock<std::mutex> lck_data_map(*unit.mutex_data_map);
    auto it_rate = unit.rate_limit_data_map.find(data->InstrumentID);
    if ( it_rate == unit.rate_limit_data_map.end())
    {
        auto ret_tmp = unit.rate_limit_data_map.insert(std::make_pair(data->InstrumentID, RateLimitUnit()));
        it_rate = ret_tmp.first;
    }
    else
    {
        //测试日志
        KF_LOG_DEBUG(logger, "[req_order_insert] rate_limit_data_map InstrumentID " << data->InstrumentID << " rate_limit_data_map.size " << unit.rate_limit_data_map.size() <<
                                " order_total " << it_rate->second.order_total <<" trade_total " << it_rate->second.trade_total << 
                                " gtc_order_total " << it_rate->second.gtc_canceled_order_total);
        //判断是否需要整十分钟重置
        
        if (timestamp - unit.last_rate_limit_timestamp >=  Rate_Limit_Reset_Interval)
        {
            //测试日志
            KF_LOG_DEBUG(logger, "[req_order_insert] reset rate_limit_data_map per 10mins" <<
                                    " last_rate_limit_timestamp " << unit.last_rate_limit_timestamp <<
                                    " current timestamp " << timestamp);
            unit.last_rate_limit_timestamp = timestamp;//write in
            //reset
            for (auto& iter : unit.rate_limit_data_map)
            {
                iter.second.Reset();//wirte in 
            }
        }
    
        //判断是否达到触发条件·委托单数量>=UFR_order_lower_limit
        uint64_t tmpOrderCount = it_rate->second.order_total+fixedVolume;
        if ((it_rate->second.order_count+1) >= UFR_order_lower_limit)
        {
            //计算UFR
            double UFR = 1 - it_rate->second.trade_total*1.0/tmpOrderCount;
            //测试日志
            KF_LOG_DEBUG(logger, "[req_order_insert] order_total is reaching to UFR_order_lower_limit " << "InstrumentID "<< data->InstrumentID <<
                                    " current UFR " << UFR );
            if (UFR >= UFR_limit)
            {
                //测试日志
                KF_LOG_DEBUG(logger, "[req_order_insert] UFR above limit! " <<  " InstrumentID "<< data->InstrumentID << ", ufr_limit " << UFR_limit);
                //未成交率达到上限，报错并return
                errorId = 123;
                errorMsg = std::string(data->InstrumentID) + " UFR above limit !";
                KF_LOG_ERROR(logger, "[req_order_insert]: UFR above limit: (rid)" << requestId << " (errorId)" <<
                        errorId << " (errorMsg) " << errorMsg);
                on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());//write in _ info
                raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BINANCE, 1, requestId, errorId, errorMsg.c_str());//write in _ info
                return;
            }
        }      
    }
    lck_data_map.unlock();
   
    bool active;
    auto tif1 =  GetTimeInForce(data->TimeCondition);
    if(tif1 == "GTC")
    {
        active = false;
    }
    else if(tif1 == "IOC")
    {
        active = true;
    }
    SendOrderParam param{&unit, ticker.c_str(), GetSide(data->Direction).c_str(), GetType(data->OrderPriceType).c_str(),
        GetTimeInForce(data->TimeCondition).c_str(), fixedVolume*1.0/scale_offset, fixedPrice*1.0/scale_offset, stopPrice, genClientid(data->OrderRef) ,
        icebergQty, active, is_post_only(data)};
    
    KF_LOG_DEBUG(logger, "[[req_order_insert_time]: ] cur_time" << curTime );
      if(nullptr == m_ThreadPoolPtr)
    {
        send_order_thread(param,*data,account_type,requestId,timestamp,fixedVolume,fixedPrice);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineBinance::send_order_thread,this,param,*data,account_type,requestId,timestamp,fixedVolume,fixedPrice));
          KF_LOG_DEBUG(logger, "[req_order_insert] [left thread count ]: ] "<< m_ThreadPoolPtr->idlCount());
    }
   // long long  temp=curTime;
//     time_t temp=curTime;
//     curTime = time(0);
//     temp = curTime-temp; 
//    // curTime=frame->getNano();
//    // temp = curTime - temp;
//     KF_LOG_DEBUG(logger, "[[req_order_insert_time ]: ] cur_time" << curTime <<" req_insert_sub_time "<<temp);
  
    
}

void TDEngineBinance::onRtnNewOrder(const LFInputOrderField* data, AccountUnitBinance& unit, int requestId,string remoteOrderId,int64_t fixedVolume,int64_t fixedPrice)
{

    LFRtnOrderField rtn_order;
    memset(&rtn_order, 0, sizeof(LFRtnOrderField));
    strcpy(rtn_order.ExchangeID, "binance");
    strncpy(rtn_order.UserID, unit.api_key.c_str(), 16);
    strncpy(rtn_order.InstrumentID, data->InstrumentID, 31);
    rtn_order.Direction = data->Direction;
    rtn_order.TimeCondition = data->TimeCondition;
    rtn_order.OrderPriceType = data->OrderPriceType;
    strncpy(rtn_order.OrderRef, data->OrderRef, 13);
    rtn_order.VolumeTraded = 0;
    rtn_order.VolumeTotalOriginal = fixedVolume;
    rtn_order.VolumeTotal = fixedVolume;
    rtn_order.LimitPrice = fixedPrice;
    rtn_order.RequestID = requestId;
    rtn_order.OrderStatus = LF_CHAR_NotTouched;
    on_rtn_order(&rtn_order);
    raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
                            source_id, MSG_TYPE_LF_RTN_ORDER_BINANCE,
                            1/*islast*/, (rtn_order.RequestID > 0) ? rtn_order.RequestID: -1);
    std::unique_lock<std::mutex> lck(*unit.mutex_order_and_trade);
    unit.ordersMap.insert(std::make_pair(remoteOrderId,rtn_order));
    //mapSendOrder.insert(std::make_pair(data->OrderRef,OrderInfo{getTimestamp(),rtn_order,requestId,unit,GetType(data->OrderPriceType)}));
    //KF_LOG_INFO(logger, "[mapSendOrder] mapSendOrder.size: " << mapSendOrder.size());
    std::vector<std::string>::iterator it;
    for(it = unit.wsOrderStatus.begin(); it != unit.wsOrderStatus.end(); it++)
    {
        Document json;
        json.Parse((*it).c_str());
        if(json.HasMember("i")){
            string wsOrderId = std::to_string(json["i"].GetInt64());
            if(remoteOrderId == wsOrderId){
                onOrder(unit,json);
                it = unit.wsOrderStatus.erase(it);
                if(unit.wsOrderStatus.size() > 0)
                {
                    it--;
                }
                else
                {
                    break;
                }    
            }
        }
    }
    lck.unlock();
    std::unique_lock<std::mutex> lck_data_map(*unit.mutex_data_map);
    //收到委托回报，在这里委托量+1
    unit.rate_limit_data_map[data->InstrumentID].order_total+=fixedVolume;
    unit.rate_limit_data_map[data->InstrumentID].order_count++;
    //测试日志
    KF_LOG_DEBUG(logger, "[onRtnNewOrder] order_total++ " << " InstrumentID "<< data->InstrumentID <<
            " current order_total " << unit.rate_limit_data_map[data->InstrumentID].order_total <<" trade_total " << unit.rate_limit_data_map[data->InstrumentID].trade_total );
    lck_data_map.unlock();
}

void TDEngineBinance::action_order_thread(AccountUnitBinance* unit,  string ticker,const LFOrderActionField data, std::string account_type,int requestId  )
{

    KF_LOG_DEBUG(logger, "[action_order_thread] current thread is:" <<std::this_thread::get_id()<<" current CPU is  "<<sched_getcpu());
    Document d;
    int errorId =0;
    std::string errorMsg = "";
    int response_code;
    if(!unit->is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        return;
    }else{
        if( account_type == "margin")
        {
            response_code = cancel_margin_order(*unit, ticker.c_str(), 0,genClientid(data.OrderRef).c_str(), "", d, data, requestId);
            KF_LOG_INFO(logger, "[req_order_action] cancel_margin_order");
        }
        else
        {
            response_code = cancel_order(*unit, ticker.c_str(), 0, genClientid(data.OrderRef).c_str(), "", d, data, requestId);
            KF_LOG_INFO(logger, "[req_order_action] cancel_order");
        }
    }
    //    KF_LOG_INFO(logger, "[req_order_action] cancel_order");
    //    printResponse(d);
    if(response_code == HTTP_GATEWAY_TIMEOUT){
        KF_LOG_INFO(logger,"Gateway Timeout");
        return;
    }

    if(d.HasParseError() )
    {
        errorId=100;
        errorMsg= "cancel_order http response has parse error. please check the log";
        if( account_type == "margin")
            KF_LOG_ERROR(logger, "[req_order_action] cancel_margin_order error! (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        else
            KF_LOG_ERROR(logger, "[req_order_action] cancel_order error! (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    if(!d.HasParseError() && d.IsObject() && d.HasMember("code") && d["code"].IsNumber())
    {
        errorId = d["code"].GetInt();
        if(d.HasMember("msg") && d["msg"].IsString())
        {
            errorMsg = d["msg"].GetString();
            if(errorId == -2011 && errorMsg == "Unknown order sent."){
                errorId = 200;
                //errorMsg = std::string(data.OrderRef) + " is not found, ignore it";
            }
        }
        if( account_type == "margin")
            KF_LOG_ERROR(logger, "[req_order_action] cancel_margin_order failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        else
            KF_LOG_ERROR(logger, "[req_order_action] cancel_order failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    if(errorId != 0)
    {
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
    }
    else
    {
    //     std::unique_lock<std::mutex> lck1(cancel_mutex);
    //     KF_LOG_INFO(logger,"zafinsert:"<<data->OrderRef);
    //    // mapCancelOrder.insert(std::make_pair(data->OrderRef,OrderActionInfo{getTimestamp(),*data,requestId,unit}));

    //     KF_LOG_INFO(logger, "[mapCancelOrder] (size)" << mapCancelOrder.size() );
    //     lck1.unlock();
    	std::string remoteOrderId = std::to_string(d["orderId"].GetInt64());   
        std::unique_lock<std::mutex> lck1(*unit->mutex_order_and_trade);   
		auto it = unit->ordersMap.find(remoteOrderId);
		if (it == unit->ordersMap.end())
		{ 
            errorId=120;
            errorMsg= "cancel_order ,no order match,cancleFailed";
            //on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
			KF_LOG_ERROR_FMT(logger, "TDEngineBinance::onOrder,no order match(%s),cancleFailed",remoteOrderId.c_str());
            //on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
			return;
		}
		LFRtnOrderField& rtn_order = it->second;
        char status = GetOrderStatus(d["status"].GetString());				
        rtn_order.OrderStatus = status;
        std::string strTradeVolume = d["executedQty"].GetString();
        uint64_t volumeTraded = std::round(std::stod(strTradeVolume)*scale_offset);
        uint64_t oldVolumeTraded = rtn_order.VolumeTraded;
		rtn_order.VolumeTraded += volumeTraded;
        rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTraded;
		KF_LOG_INFO(logger, "TDEngineBinance::req_order_action ,rtn_order");
		on_rtn_order(&rtn_order);
		raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_ORDER_BINANCE,1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);
        
        unit->ordersMap.erase(it);

        std::unique_lock<std::mutex> lck2(account_mutex);
        auto it2 = mapInsertOrders.find(rtn_order.OrderRef);
        if(it2 != mapInsertOrders.end())
        {
            mapInsertOrders.erase(it2);
        }
    }
    KF_LOG_DEBUG(logger,"[req_order_action] rest return");
    raw_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BINANCE, 1, requestId, errorId, errorMsg.c_str());
}

void TDEngineBinance::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    std::unique_lock<std::mutex> lck(account_mutex);
    int errorId = 0;
    std::string errorMsg = "";
    auto it  = mapInsertOrders.find(data->OrderRef);
    if(it == mapInsertOrders.end())
    {
        errorId = 200;
        errorMsg = std::string(data->OrderRef) + " is not found, ignore it";
        KF_LOG_ERROR(logger, errorMsg << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BINANCE, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    AccountUnitBinance& unit = *(it->second);
    lck.unlock();
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BINANCE, 1, requestId);

    //GCR check
    int64_t timestamp = getTimestamp();
    //若InstrumentId不在map中则添加
    auto it_rate = unit.rate_limit_data_map.find(data->InstrumentID);
    if ( it_rate != unit.rate_limit_data_map.end())
    {
        //判断是否需要整十分钟重置
        if (timestamp - unit.last_rate_limit_timestamp >=  Rate_Limit_Reset_Interval)
        {
            KF_LOG_DEBUG(logger, "[req_order_action] reset rate_limit_data_map per 10mins" <<
                                    " last_rate_limit_timestamp " << unit.last_rate_limit_timestamp <<
                                    " current timestamp " << timestamp);
            unit.last_rate_limit_timestamp = timestamp;
            //reset
            for (auto& iter : unit.rate_limit_data_map)
            {
                iter.second.Reset();
            }
        }
        else
        {
            auto it_gcr = it_rate->second.mapOrderTime.find(data->OrderRef);
            if(it_rate->second.mapOrderTime.size() >= GCR_order_lower_limit && it_gcr != it_rate->second.mapOrderTime.end() && timestamp - it_gcr->second < 2500)
            {
                it_rate->second.gtc_canceled_order_total++;
                double dGCR = it_rate->second.gtc_canceled_order_total*1.0/it_rate->second.mapOrderTime.size();
                if(dGCR >= GCR_limit)
                {
                    errorId = 100;
                    errorMsg = std::string(data->InstrumentID) + " is over GCR Limit, reject this action";
                    KF_LOG_ERROR(logger, "[req_order_action]:(rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
                    on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
                    raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BINANCE, 1, requestId, errorId, errorMsg.c_str());
                    return;
                }
            }
        }
        
    }
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 201;
        errorMsg = std::string(data->InstrumentID) + "not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it. (rid)" << requestId << " (errorId)" <<
                                                                                      errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BINANCE, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
     
    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);
    if(nullptr == m_ThreadPoolPtr)
    {
        action_order_thread(&unit,ticker,*data,account_type,requestId);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineBinance::action_order_thread,this,&unit,ticker,*data,account_type,requestId));
    }
 //   KF_LOG_DEBUG(logger, "[req_order_action][left thread count ]: ] "<< m_ThreadPoolPtr_action->idlCount());
  }

void TDEngineBinance::req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId){
    AccountUnitBinance& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_withdraw_currency]" << " (rid) " << requestId
                                              << " (APIKey) " << unit.api_key
                                              << " (Currency) " << data->Currency
                                              << " (Volume) " << data->Volume
                                              << " (Address) " << data->Address
                                              << " (Tag) " << data->Tag);
    send_writer->write_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_BINANCE, 1, requestId);
    int errorId = 0;
    std::string errorMsg = "";

    std::string currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
    if(currency.length() == 0) 
    {
        errorId = 200;
        errorMsg = std::string(data->Currency) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_withdraw_currency]: not in WhiteList, ignore it  (rid)" << requestId <<
                                                                                      " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_withdraw(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_INFO(logger, "[req_withdraw_currency] (exchange_currency)" << currency);

    string address = data->Address, tag = data->Tag;
    if(address == ""){
        errorId = 100;
        errorMsg = "address is null";
        KF_LOG_ERROR(logger,"[req_withdraw_currency] address is null");
        //on_withdraw(data, requestId, errorId, errorMsg.c_str());
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_BINANCE, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }
    string volume = to_string(data->Volume);
    string fixvolume;
    dealnum(volume,fixvolume);
    Document json;
    int response_code;
    response_code = withdrawl_currency(data->Currency,address,tag,fixvolume,json,unit,*data,requestId);
    if(response_code == HTTP_GATEWAY_TIMEOUT){
        return;
    }

    if (json.HasParseError() || !json.IsObject()){
        errorId = 101;
        errorMsg = "json has parse error.";
        KF_LOG_ERROR(logger,"[withdrawl_currency] json has parse error.");
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_BINANCE, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }
    if(json.HasMember("success") && json["success"].GetBool()){
        errorId = 0;
        string id = json["id"].GetString();
        LFWithdrawField* dataCpy = (LFWithdrawField*)data;
        strncpy(dataCpy->ID,id.c_str(),64);
        //string message = json["msg"].GetString();
        //KF_LOG_INFO(logger, "[withdrawl_currency] (msg) " << message);
        KF_LOG_INFO(logger, "[withdrawl_currency] (id) " << id);
        KF_LOG_INFO(logger, "[withdrawl_currency] withdrawl success. no error message");
        on_rsp_withdraw(dataCpy,requestId,errorId,errorMsg.c_str());
    }else if(json.HasMember("success") && !json["success"].GetBool()){
        
        string message = json["msg"].GetString();
        KF_LOG_INFO(logger, "[withdrawl_currency] (msg) " << message);
        KF_LOG_INFO(logger, "[withdrawl_currency] withdrawl faild!");
        errorId = 102;
        errorMsg = message;
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_BINANCE, 1, 
            requestId, errorId, errorMsg.c_str());
    }
}

int TDEngineBinance::withdrawl_currency(string asset, string address, string addresstag, string amount, Document& json, AccountUnitBinance& unit,const LFWithdrawField data,int requestId){
    
    cpr::Response response;
    long recvWindow = order_insert_recvwindow_ms;
    std::string Timestamp = getTimestampString();
    std::string Method = "POST";
    // wapi -> sapi
    //std::string requestPath = restBaseUrl + "/wapi/v3/withdraw.html?";
    std::string requestPath = restBaseUrl + "/sapi/v1/capital/withdraw/apply?";
    // end
    std::string queryString("");
    std::string body = "";

    // wapi -> sapi
    //queryString.append( "asset=" );
    //queryString.append( asset );
    queryString.append( "coin=" );
    queryString.append( asset );
    // sset -> coin

    queryString.append("&address=");
    queryString.append( address );

    if( addresstag != "") {
        queryString.append("&addressTag=");
        queryString.append( addresstag );
    }

    queryString.append("&amount=");
    queryString.append( amount );

    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( to_string( recvWindow) );
    }

    queryString.append("&timestamp=");
    queryString.append(Timestamp);

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );
    string url = requestPath + queryString;

    KF_LOG_INFO(logger, "[withdrawl_currency] (asset) " << asset << " (amount) " << amount
        << " (address) " << address );

    std::unique_lock<std::mutex> lck(http_mutex);
    response = Post(Url{url},
                            Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                            Body{body}, Timeout{30000});

    KF_LOG_INFO(logger, "[withdrawl_currency] (url) " << url << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str());
    lck.unlock();

    if(response.status_code == HTTP_GATEWAY_TIMEOUT){
        int errorId = 504;
        std::string errorMsg = response.text;
        on_rsp_withdraw(&data,requestId,errorId,errorMsg.c_str());
    }

    json.Parse(response.text.c_str());
    return response.status_code;
}

void TDEngineBinance::req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw)
{
	KF_LOG_INFO(logger, "[req_transfer_history]");
    AccountUnitBinance& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_transfer_history] (api_key)" << unit.api_key);

    LFTransferHistoryField his;
    memset(&his, 0, sizeof(LFTransferHistoryField));
    strncpy(his.UserID, data->UserID, 64);
    strncpy(his.ExchangeID, data->ExchangeID, 11);

    int errorId = 0;
    std::string errorMsg = "";
    KF_LOG_INFO(logger, "[req_transfer_history] (data->UserID)" << data->UserID);
    
    string asset = data->Currency;
    string startTime = data->StartTime;
    string endTime = data->EndTime;
    Document d;
    int response_code;
    if(!isWithdraw)
    {
        response_code = get_deposit_history(unit, d, asset, data->Status, startTime, endTime, *data, requestId);
        KF_LOG_INFO(logger, "[req_transfer_history] req_deposit_history");
    }
    else
    {
    	response_code = get_withdraw_history(unit, d, asset, data->Status, startTime, endTime, *data, requestId);
        KF_LOG_INFO(logger, "[req_transfer_history] req_withdraw_history");
    }
    printResponse(d);

    if(response_code == HTTP_GATEWAY_TIMEOUT){
        return;
    }

    if(d.HasParseError() )
    {
        errorId=100;
        errorMsg= "req_transfer_history http response has parse error. please check the log";
        KF_LOG_ERROR(logger, "[req_transfer_history] req_transfer_history error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }

    if(!d.HasParseError() && d.IsObject() && d.HasMember("code") && d["code"].IsNumber())
    {
        errorId = d["code"].GetInt();
        if(d.HasMember("msg") && d["msg"].IsString())
        {
            errorMsg = d["msg"].GetString();
        }
        KF_LOG_ERROR(logger, "[req_transfer_history] req_transfer_history failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    send_writer->write_frame(data, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_BINANCE, 1, requestId);

    std::vector<LFTransferHistoryField> tmp_vector;
    if(!d.HasParseError() && d.IsObject())
    {
        if(!isWithdraw)
        {
            if(d.HasMember("depositList"))
            {
                int len = d["depositList"].Size();
                for ( int i  = 0 ; i < len ; i++ ) {
                    char Currency[32];
                    strncpy(Currency, d["depositList"].GetArray()[i]["asset"].GetString(), 32);
                    std::string ticker = unit.positionWhiteList.GetKeyByValue(Currency);
                    if(ticker.length() > 0) {
                        strncpy(his.Currency, Currency, 32 );
                        strncpy(his.TimeStamp, (std::to_string(d["depositList"].GetArray()[i]["insertTime"].GetDouble())).c_str(), 32);
                        his.Status = get_deposit_status(d["depositList"].GetArray()[i]["status"].GetInt());
                        his.Volume = std::round(d["depositList"].GetArray()[i]["amount"].GetDouble() * scale_offset);
                        strncpy(his.Address, d["depositList"].GetArray()[i]["address"].GetString(), 130);

                        if(d["depositList"].GetArray()[i].HasMember("addressTag") && d["depositList"].GetArray()[i]["addressTag"].IsString())
                        {
                            strncpy(his.Tag, d["depositList"].GetArray()[i]["addressTag"].GetString(), 64);
                        }
                        strncpy(his.TxId, d["depositList"].GetArray()[i]["txId"].GetString(), 130);
                        tmp_vector.push_back(his);
                        KF_LOG_INFO(logger,  "[req_deposit_history] (insertTime)" << his.TimeStamp << " (amount)" <<  his.Volume
                                             << " (asset)" << his.Currency<< " (address)" << his.Address <<" (addressTag)" <<his.Tag
                                             << " (txId)" << his.TxId <<" (status)" <<his.Status <<" (requestId)" << requestId );
                   }
               }
            }
        }
        else
        {
            if(d.HasMember("withdrawList"))
            {
                int len = d["withdrawList"].Size();
                for ( int i  = 0 ; i < len ; i++ ) {
                    char Currency[32];
                    strncpy(Currency, d["withdrawList"].GetArray()[i]["asset"].GetString(), 32);

                    std::string ticker = unit.positionWhiteList.GetKeyByValue(Currency);
                    if(ticker.length() > 0) {
                        strncpy(his.Currency, Currency, 32 );
                        strncpy(his.TimeStamp, (std::to_string(d["withdrawList"].GetArray()[i]["applyTime"].GetDouble())).c_str(), 32);

                        his.Status = get_withdraw_status(d["withdrawList"].GetArray()[i]["status"].GetInt());
                        his.Volume = std::round(d["withdrawList"].GetArray()[i]["amount"].GetDouble() * scale_offset);

                        strncpy(his.Address, d["withdrawList"].GetArray()[i]["address"].GetString(), 130);

                        strncpy(his.FromID, d["withdrawList"].GetArray()[i]["id"].GetString(), 64);
                        if(d["withdrawList"].GetArray()[i].HasMember("addressTag") && d["withdrawList"].GetArray()[i]["addressTag"].IsString())
                        {
                            strncpy(his.Tag, d["withdrawList"].GetArray()[i]["addressTag"].GetString(), 64);
                        }
                        strncpy(his.TxId, d["withdrawList"].GetArray()[i]["txId"].GetString(), 130);
                        tmp_vector.push_back(his);
                        KF_LOG_INFO(logger,  "[req_withdraw_history] (applyTime)" << his.TimeStamp << " (amount)" <<  his.Volume
                                             << " (asset)" << his.Currency<< " (address)" << his.Address <<" (addressTag)" <<his.Tag
                                             << " (txId)" << his.TxId <<" (status)" <<his.Status <<"(requestId)" << requestId );
                   }
               }
            }
        }
    }
    bool findSymbolInResult = false;
    
    int history_count = tmp_vector.size();
    if(history_count == 0)
    {
        his.Status = -1;
    }
    for (int i = 0; i < history_count; i++)
    {
    	on_rsp_transfer_history(&tmp_vector[i], isWithdraw, i == (history_count - 1), requestId,errorId, errorMsg.c_str());
        findSymbolInResult = true;
    }

    if(!findSymbolInResult)
    {
    	on_rsp_transfer_history(&his, isWithdraw, 1, requestId,errorId, errorMsg.c_str());
    }
    if(errorId != 0)
    {
        raw_writer->write_error_frame(&his, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_BINANCE, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineBinance::req_get_kline_via_rest(const GetKlineViaRest* data, int account_index, int requestId, long rcv_time)
{
    KF_LOG_INFO(logger, "TDEngineBinance::req_get_kline_via_rest: (symbol)" << data->Symbol << " (interval)" << data->Interval << " (IgnoreStartTime)" << data->IgnoreStartTime);
    //writer->write_frame(data, sizeof(GetKlineViaRest), source_id, MSG_TYPE_LF_GET_KLINE_VIA_REST_BINANCE, 1/*islast*/, requestId);
    writer->write_frame(data, sizeof(GetKlineViaRest), source_id, MSG_TYPE_LF_GET_KLINE_VIA_REST, 1/*islast*/, requestId);

    AccountUnitBinance& unit = account_units[account_index];
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(data->Symbol);
    if (ticker.empty())
    {
        KF_LOG_INFO(logger, "symbol not in white list");
        return;
    }
    if (unit.bHandle_429 && isHandling(unit))
    {
        return;
    }
    int param_limit = data->Limit;
    if (param_limit > 1000)
        param_limit = 1000;
    else if (param_limit < 1)
        param_limit = 1;

    int64_t timeNow = getTimestamp();
    int64_t interval_ms = get_millsecond_interval(data->Interval);
    int64_t endTs = timeNow - timeNow % interval_ms;
    int64_t startTs = endTs - interval_ms * data->Limit;
    KF_LOG_INFO(logger, "startTs " << startTs << " endTs " << endTs);

    const auto static url = "https://api.binance.com/api/v3/klines";
    string interface;
    if (m_interface_switch > 0) {
        interface = m_interfaceMgr.getActiveInterface();
        KF_LOG_INFO(logger, "[get_kline] interface: [" << interface << "].");
        if (interface.empty()) {
            KF_LOG_INFO(logger, "[get_kline] interface is empty, decline message sending!");
            return;
        }
    }
    handle_request_weight(unit,interface,GetKline_Type);
    cpr::Response response;
    if (data->IgnoreStartTime)
        //response = Get(Url{ url }, Parameters{ {"symbol", ticker},{"interval", data->Interval},{"limit", to_string(param_limit)} });
        response = Get(Url{ url }, Parameters{ {"symbol", ticker},{"interval", data->Interval},
            {"startTime", to_string(startTs)}, {"endTime", to_string(endTs)},{"limit", to_string(param_limit)} });
    else
        response = Get(Url{ url }, Parameters{ {"symbol", ticker},{"interval", data->Interval},
            {"startTime", to_string(data->StartTime)}, {"endTime", to_string(data->EndTime)}, {"limit", to_string(param_limit)} });
    KF_LOG_INFO(logger, "rest response url " << response.url);

    for(auto header:response.header)
    {
        KF_LOG_INFO(logger, " header: " << header.first << " -> " << header.second);
    }
    if (response.status_code == HTTP_CONNECT_REFUSED)
    {
        meet_429(unit);
    }
    if (response.status_code == HTTP_CONNECT_REFUSED || response.status_code == HTTP_CONNECT_BANS) {
        if (m_interface_switch > 0) {
            m_interfaceMgr.disable(interface);
            KF_LOG_INFO(logger, "[get_kline] interface [" << interface << "] is disabled!");
        }
    }
    else if (response.status_code >= 400) {
        KF_LOG_INFO(logger, "req_get_kline_via_rest Error [" << response.status_code << "] making request ");
        KF_LOG_INFO(logger, "Request took " << response.elapsed);
        KF_LOG_INFO(logger, "Body: " << response.text);

        string errMsg;
        errMsg = "req_get_kline_via_rest Error [";
        errMsg += response.status_code;
        errMsg += "] making request ";
        errMsg += response.text;
        write_errormsg(218, errMsg);
        return;
    }
    //[1620940500000,"49748.23","49798.98","49562.80","49648.33","38454.988",1620941399999,"1910302536.51889",1779,"15055.420","747938369.27177","0"]
    KF_LOG_INFO(logger, "TDEngineBinance::req_get_kline_via_rest: parse response" << response.text.c_str());

    Document d;
    d.Parse(response.text.c_str());

    if (d.IsArray()) {
        LFBarSerial1000Field bars;
        memset(&bars, 0, sizeof(bars));
        strncpy(bars.InstrumentID, data->Symbol, 31);
        strcpy(bars.ExchangeID, "binance");

        for (int i = 0; i < d.Size(); i++) {
            if (!d[i].IsArray()) {
                KF_LOG_INFO(logger, "TDEngineBinance::req_get_kline_via_rest: response is abnormal" << response.text.c_str());
                break;
            }

            int64_t nStartTime = d[i][0].GetInt64();
            int64_t nEndTime = d[i][6].GetInt64();
            bars.BarSerial[i].StartUpdateMillisec = nStartTime;
            bars.BarSerial[i].EndUpdateMillisec = nEndTime;
            bars.BarSerial[i].PeriodMillisec = nEndTime - nStartTime + 1;
            //scale_offset = 1e8
            bars.BarSerial[i].Open = std::round(std::stod(d[i][1].GetString()) * scale_offset);
            bars.BarSerial[i].Close = std::round(std::stod(d[i][4].GetString()) * scale_offset);
            bars.BarSerial[i].Low = std::round(std::stod(d[i][3].GetString()) * scale_offset);
            bars.BarSerial[i].High = std::round(std::stod(d[i][2].GetString()) * scale_offset);

            bars.BarSerial[i].Volume = std::round(std::stod(d[i][5].GetString()) * scale_offset);
            bars.BarSerial[i].BusinessVolume = std::round(std::stod(d[i][7].GetString()) * scale_offset);
            bars.BarSerial[i].ActInVolume = std::round(std::stod(d[i][9].GetString()) * scale_offset);
            bars.BarSerial[i].ActInBusinessVolume = std::round(std::stod(d[i][10].GetString()) * scale_offset);

            bars.BarSerial[i].TransactionsNum = d[i][8].GetInt();

            bars.BarLevel = i + 1;
        }
        on_bar_serial1000(&bars, data->RequestID);
    }
    else 
    {
        KF_LOG_INFO(logger, "TDEngineBinance::req_get_kline_via_rest: response is abnormal");
    }
}

int TDEngineBinance::get_deposit_history(AccountUnitBinance& unit, Document &json, string asset ,
                  int status, string startTime, string endTime, const LFTransferHistoryField data, int requestId )
{
    KF_LOG_INFO(logger, "[get_deposit_history]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    // wapi -> sapi
    //std::string requestPath = restBaseUrl + "/wapi/v3/depositHistory.html?";
    std::string requestPath = restBaseUrl + "/sapi/v1/capital/deposit/hisrec?";
    //
    std::string queryString("");
    std::string body = "";

    if(asset != "")
    {
        // wapi -> sapi
        //queryString.append( "asset=" );
        queryString.append( "coin=" );
        queryString.append( asset );
        //
    }
    if(status !=-1)
    {
        queryString.append("&status=");
        queryString.append( to_string( get_deposit_status(status)) );
    }
    if(startTime != "")
    {
        queryString.append("&startTime=");
        queryString.append(  startTime );
    }
    if(endTime != "")
    {
        queryString.append("&endTime=");
        queryString.append(  endTime );
    }

    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( to_string( recvWindow) );
    }

    queryString.append("&timestamp=");
    queryString.append( Timestamp );

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    string url = requestPath + queryString;

    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                                 Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                                 Body{body}, Timeout{100000});

    KF_LOG_INFO(logger, "[get_deposit_history] (url) " << url << " (response.status_code) " << response.status_code <<
                                                 " (response.error.message) " << response.error.message <<
                                                 " (response.text) " << response.text.c_str());
    lck.unlock();

    if(response.status_code == HTTP_GATEWAY_TIMEOUT){
        int errorId = 504;
        std::string errorMsg = response.text;
        on_rsp_transfer_history(&data, false, 1, requestId,errorId, errorMsg.c_str());
    }
     
    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}

int TDEngineBinance::get_withdraw_history(AccountUnitBinance& unit, Document &json, string asset,
                  int status, string startTime, string endTime , const LFTransferHistoryField data, int requestId)
{
    KF_LOG_INFO(logger, "[get_withdraw_history]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    // wapi -> sapi
    //std::string requestPath = restBaseUrl + "/wapi/v3/withdrawHistory.html?";
    std::string requestPath = restBaseUrl + "/sapi/v1/capital/withdraw/history?";
    //
    std::string queryString("");
    std::string body = "";

    if(asset != "")
    {
        // wapi -> sapi
        //queryString.append( "asset=" );
        queryString.append("coin=");
        queryString.append(asset);
        //
    }
    if(status !=-1)
    {
        queryString.append("&status=");
        queryString.append( to_string( get_query_withdraw_status(status)) );
    }
    if(startTime != "")
    {
        queryString.append("&startTime=");
        queryString.append(  startTime );
    }
    if(endTime != "")
    {
        queryString.append("&endTime=");
        queryString.append(  endTime );
    }
    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( to_string( recvWindow) );
    }

    queryString.append("&timestamp=");
    queryString.append( Timestamp );

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    string url = requestPath + queryString;

    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                                 Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                                 Body{body}, Timeout{100000});

    KF_LOG_INFO(logger, "[get_withdraw_history] (url) " << url << " (response.status_code) " << response.status_code <<
                                                 " (response.error.message) " << response.error.message <<
                                                 " (response.text) " << response.text.c_str());
    lck.unlock();

    if(response.status_code == HTTP_GATEWAY_TIMEOUT){
        int errorId = 504;
        std::string errorMsg = response.text;
        on_rsp_transfer_history(&data, true, 1, requestId,errorId, errorMsg.c_str());
    }
     
    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}


void TDEngineBinance::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on AccountUnitBinance::loop");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBinance::loop, this)));

    KF_LOG_INFO(logger,"[set_reader_thread] cancle_thread start on AccountUnitBinance::caloop");
    cancel_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBinance::caloop,this)));

   // KF_LOG_INFO(logger,"[set_reader_thread] send_thread start on AccountUnitBinance::seloop");
  //  send_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBinance::seloop,this)));

    // //仿照上面在这里创建新线程，loop是创建的线程里的主函数
    // KF_LOG_INFO(logger,"[set_reader_thread] rest_thread start on AccountUnitBinance::testUTC");
    // test_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBinance::testUTC,this)));
}

void TDEngineBinance::loop()
{
    KF_LOG_INFO(logger, "[loop] (isRunning) " << isRunning <<"id" << std::this_thread::get_id());
    while(isRunning)
    {
        
        auto current_ms = getTimestamp();
        //uint64_t tmp_rest_get_interval_ms = rest_get_interval_ms;
    //    std::unique_lock<std::mutex> lck(mutex_account);

        for (size_t idx = 0; idx < account_units.size(); idx++)
        {
            
            AccountUnitBinance& unit = account_units[idx];
            
            if(unit.last_put_time != 0 && current_ms - unit.last_put_time > 1800000)
            {
                Document json;
                if( account_type == "margin")
                    put_margin_listen_key(unit, json);
                else
                    put_listen_key(unit,json);
                unit.last_put_time = getTimestamp();
            }
            lws_service( unit.context, rest_get_interval_ms );
            /*if(unit.destroy)
            {
                unit.destroy = false;
                lws_context_destroy(unit.context);
            }*/
        }
        
        if( current_ms - last_rest_get_ts > SYNC_TIME_DEFAULT_INTERVAL) {
            //reset
            //sync_time_interval = SYNC_TIME_DEFAULT_INTERVAL;
            getTimeDiffOfExchange(account_units[0]);
            last_rest_get_ts = current_ms;
        }
      //  lck.unlock();
        
    }
}

void TDEngineBinance::caloop()
{
    KF_LOG_INFO(logger, "[caloop] (isRunning) " << isRunning <<"id" << std::this_thread::get_id());
    while(isRunning)
    {
        std::map<std::string,OrderActionInfo>::iterator it;
        int64_t current_ms = getTimestamp();
        int errorId = 0;
        std::string errorMsg = "";
        std::unique_lock<std::mutex> lck(cancel_mutex);
        cancel_cv.wait_for(lck,std::chrono::milliseconds(rest_get_interval_ms));
        for( it = mapCancelOrder.begin(); it != mapCancelOrder.end(); )
        {
            //KF_LOG_INFO(logger,"zafloop");
            if((current_ms - it->second.rcv_time) > rest_get_interval_ms)
            {
                AccountUnitBinance& unit = it->second.unit;
                LFOrderActionField data = it->second.data;
                int response_code;
                Document json;
                std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data.InstrumentID));
                KF_LOG_INFO(logger, "[mapCancelOrder] data.InstrumentID : "<< data.InstrumentID);
                KF_LOG_INFO(logger, "[mapCancelOrder] symbol : "<< ticker);
                if( account_type == "margin")
                {
                    response_code = get_margin_order(unit, ticker.c_str(), 0, data.OrderRef, json, data, it->second.request_id);
                }
                else
                {
                    response_code = get_order(unit, ticker.c_str(), 0, data.OrderRef, json, data, it->second.request_id);
                }

                if(response_code == HTTP_GATEWAY_TIMEOUT){
                    //on_rsp_order_action(&data, it->second.request_id, errorId, errorMsg.c_str());
                    KF_LOG_INFO(logger,"504 it++");
                    it++;
                }

                if(json.HasParseError() ){
                    errorId=100;
                    errorMsg= "query_order http response has parse error. please check the log";
                    KF_LOG_ERROR(logger, "[get_order] query_order error! (errorId)" << errorId << " (errorMsg) " << errorMsg);
                }
                else if(json.HasMember("status"))
                {
                    string status=json["status"].GetString();
                    if(GetOrderStatus(status) != LF_CHAR_Canceled){
                        errorId = 110;
                        errorMsg = "Status is not canceled, error";
                        KF_LOG_ERROR(logger, "[mapCancelOrder_error]:" << errorMsg << " (rid)" << it->second.request_id << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
                        on_rsp_order_action(&data, it->second.request_id, errorId, errorMsg.c_str());
                        raw_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BINANCE, 1, it->second.request_id, errorId, errorMsg.c_str());
                    }
                    else
                    {
                        std::unique_lock<std::mutex> lck1(*unit.mutex_order_and_trade);
                        auto itr = unit.ordersMap.find(data.OrderRef);
                        if(itr != unit.ordersMap.end())
                        {
                            LFRtnOrderField data1 = itr->second;
                            data1.OrderStatus=GetOrderStatus(status);
                            on_rtn_order(&data1);
                        }
                        lck1.unlock();
                    }
                    it = mapCancelOrder.erase(it);
                }
            }
            else
            {
                it++;
            }            
        }
    }
}

void TDEngineBinance::dealnum(string pre_num,string& fix_num)
{
    int size = pre_num.size();
    if(size>8){
        string s1 = pre_num.substr(0,size-8);
        s1.append(".");
        string s2 = pre_num.substr(size-8,size);
        fix_num = s1 + s2;
    }
    else{
        string s1 = "0.";
        for(int i=0; i<8-size; i++){
            s1.append("0");
        }
        fix_num = s1 + pre_num;
    }
    KF_LOG_INFO(logger,"pre_num:"<<pre_num<<"fix_num:"<<fix_num);
}

//lyw edit inner_transfer
void TDEngineBinance::req_inner_transfer(const LFTransferField* data, int account_index, int requestId)
{
    AccountUnitBinance& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_inner_transfer]" << " (rid) " << requestId
                                              << " (APIKey) " << unit.api_key
                                              << " (from) " << data->From
                                              << " (Currency) " << data->Currency
                                              << " (Volume) " << data->Volume );
    send_writer->write_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_BINANCE, 1, requestId);
    int errorId = 0;
    std::string errorMsg = "";
    Document json;
    std::string type = "";
    cpr::Response response;
    std::string Currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
    if(Currency.length() == 0) {
        errorId = 201;
        errorMsg = "Currency not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_inner_transfer]: Currency not in WhiteList , ignore it. (rid)" << requestId << " (errorId)" <<
                                                                                      errorId << " (errorMsg) " << errorMsg);
        on_rsp_transfer(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_BINANCE, 1, requestId, errorId, errorMsg.c_str());
        return;
    }

    string strvolume = to_string(data->Volume);
    string fixvolume;
    dealnum(strvolume, fixvolume);

    std::string name = data->FromName;
    if ( !strcmp(data->From, "master-main") &&!strcmp(data->To, "master-margin"))
    {
        KF_LOG_INFO(logger,"master-main to master-margin");
        type = "1";
        response = inner_transfer(Currency,fixvolume,type,json,unit.api_key,unit.secret_key);
    }
    else if( !strcmp(data->From, "master-margin") &&!strcmp(data->To, "master-main"))
    {
        KF_LOG_INFO(logger,"master-margin to master-main");
        type = "2";
        response = inner_transfer(Currency,fixvolume,type,json,unit.api_key,unit.secret_key);
    }
    else if( (!strcmp(data->From, "master-main") &&!strcmp(data->To, "sub-main")))
    {
        KF_LOG_INFO(logger,"master-main to sub-main");
        response = sub_inner_transfer(data->FromName, data->ToName, Currency, fixvolume, json, unit.api_key,unit.secret_key);
    }
    else if( (!strcmp(data->From, "sub-main") &&!strcmp(data->To, "sub-main")) || (!strcmp(data->From, "sub-main") &&!strcmp(data->To, "master-main"))){
        KF_LOG_INFO(logger,"sub-main to sub-main/master-main");
        response = sub_inner_transfer(data->FromName, data->ToName, Currency, fixvolume, json, unit.api_key,unit.secret_key);
    }
    else if ( !strcmp(data->From, "sub-main") &&!strcmp(data->To, "sub-margin"))
    {
        KF_LOG_INFO(logger,"sub-main to sub-margin");
        auto it = sub_map.find(name);
        if(it == sub_map.end()){
            errorId = 200;
            errorMsg = "not find name";
            on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
            return;
        }else{
            type = "1";
            response = inner_transfer(Currency,fixvolume,type,json,it->second.APIKey,it->second.SecretKey);
        }
    }
    else if( !strcmp(data->From, "sub-margin") &&!strcmp(data->To, "sub-main"))
    {
        KF_LOG_INFO(logger,"from6");
        auto it = sub_map.find(name);
        if(it == sub_map.end()){
            errorId = 200;
            errorMsg = "not find name";
            on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
            return;
        }
        else
        {
            type = "2";
            response = inner_transfer(Currency,fixvolume,type,json,it->second.APIKey,it->second.SecretKey);
        }
    }
    if(response.status_code == HTTP_GATEWAY_TIMEOUT){
        errorId = 504;
        errorMsg = response.text;
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
        return;        
    }

    if (json.HasParseError() || !json.IsObject()){
        errorId = response.status_code;
        errorMsg = "json has parse error.";
        KF_LOG_ERROR(logger,"[inner_transfer] json has parse error.");
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_BINANCE, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }
    if(json.HasMember("success") && json["success"].GetBool() ||json.HasMember("tranId")){
        errorId = 0;
        KF_LOG_INFO(logger, "[inner_transfer] inner_transfer success. no error message");
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
    }else if(json.HasMember("success") && !json["success"].GetBool()){
        string message ="";
        if(json.HasMember("msg") && json["msg"].IsString())
        {
            message = json["msg"].GetString();
        }
        KF_LOG_INFO(logger, "[inner_transfer] (msg) " << message);
        KF_LOG_INFO(logger, "[inner_transfer] inner_transfer faild!");
        errorId = response.status_code;
        errorMsg = message;
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_BINANCE, 1, 
            requestId, errorId, errorMsg.c_str());
    }else if(json.HasMember("code")&&json.HasMember("msg")){
        errorId = 200;
        errorMsg = json["msg"].GetString();
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_BINANCE, 1, 
            requestId, errorId, errorMsg.c_str());        
    }
}

cpr::Response TDEngineBinance::inner_transfer(string asset, string amount, string type, Document& json, std::string api_key,std::string secret_key){
    KF_LOG_INFO(logger,"api_key="<<api_key<<"secret_key"<<secret_key);
    KF_LOG_INFO(logger, "[req_inner_transfer]");
    cpr::Response response;
    std::string Timestamp = getTimestampString();
    std::string Method = "POST";
    std::string requestPath = restBaseUrl + "/sapi/v1/margin/transfer?";
    std::string queryString("");
    std::string body = "";

    queryString.append( "asset=" );
    queryString.append( asset );

    queryString.append("&amount=");
    queryString.append( amount );

    queryString.append("&type=");
    queryString.append( type );
    
    queryString.append("&timestamp=");
    queryString.append(Timestamp);
    

    std::string signature =  hmac_sha256( secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );
    string url = requestPath + queryString;

    KF_LOG_INFO(logger, "[inner_transfer] (asset) " << asset << " (amount) " << amount
        << " (type) " << type );

    std::unique_lock<std::mutex> lck(http_mutex);
    response = Post(Url{url},
                            Header{{"X-MBX-APIKEY", api_key}}, cpr::VerifySsl{false},
                            Body{body}, Timeout{30000});

    KF_LOG_INFO(logger, "[inner_transfer] (url) " << url << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str());
    lck.unlock();
    json.Parse(response.text.c_str());
    return response;

}

cpr::Response TDEngineBinance::sub_inner_transfer(string fromEmail, string toEmail, string asset, string amount, Document& json, std::string api_key,std::string secret_key){
    KF_LOG_INFO(logger,"api_key="<<api_key<<"secret_key"<<secret_key);
    KF_LOG_INFO(logger, "[sub_inner_transfer]");
    cpr::Response response;
    long recvWindow = order_insert_recvwindow_ms;
    std::string Timestamp = getTimestampString();
    std::string Method = "POST";
    // wapi -> sapi
    //std::string requestPath = restBaseUrl + "/wapi/v3/sub-account/transfer.html?";
    std::string requestPath = restBaseUrl + "/sapi/v1/sub-account/universalTransfer?";
    // 万能划转
    std::string queryString("");
    std::string body = "";
    
    queryString.append( "fromEmail=" );
    queryString.append( fromEmail );

    queryString.append( "&toEmail=" );
    queryString.append( toEmail );

    // wapi -> sapi
    queryString.append("fromAccountType=");
    queryString.append("SPOT");

    queryString.append("&toAccountType=");
    queryString.append("SPOT");
    //

    queryString.append( "&asset=" );
    queryString.append( asset );


    queryString.append("&amount=");
    queryString.append( amount );

    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( to_string( recvWindow) );
    }

    queryString.append("&timestamp=");
    queryString.append(Timestamp);

    std::string signature =  hmac_sha256( secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );
    string url = requestPath + queryString;

    KF_LOG_INFO(logger, "[sub_inner_transfer] (asset) " << asset << " (amount) " << amount
        << " (fromEmail) " << fromEmail << " (toEmail)" << toEmail );

    std::unique_lock<std::mutex> lck(http_mutex);
    response = Post(Url{url},
                            Header{{"X-MBX-APIKEY", api_key}}, cpr::VerifySsl{false},
                            Body{body}, Timeout{30000});

    KF_LOG_INFO(logger, "[sub_inner_transfer] (url) " << url << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str());
    lck.unlock();

    json.Parse(response.text.c_str());
    return response;
}


std::vector<std::string> TDEngineBinance::split(std::string str, std::string token)
{
    std::vector<std::string>result;
    while (str.size()) {
        size_t index = str.find(token);
        if (index != std::string::npos) {
            result.push_back(str.substr(0, index));
            str = str.substr(index + token.size());
            if (str.size() == 0)result.push_back(str);
        }
        else {
            result.push_back(str);
            str = "";
        }
    }
    return result;
}

int TDEngineBinance::send_margin_order(AccountUnitBinance& unit, const char *symbol,
                const char *side,
                const char *type,
                const char *timeInForce,
                double quantity,
                double price,
                double stopPrice,
                const char *newClientOrderId,
                double icebergQty,
                bool active,
                Document& json,
                const LFInputOrderField data,
                int requestId,
                bool isPostOnly)
{
    KF_LOG_INFO(logger, "[send_margin_order]");

    std::string interface;//没有默认值，可能是隐患
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        should_retry = false;

        long recvWindow = order_insert_recvwindow_ms;//read 
        std::string Timestamp = getTimestampString();
        std::string Method = "POST";
        std::string requestPath = restBaseUrl + "/sapi/v1/margin/order?";
        std::string queryString("");
        std::string body = "";

        queryString.append( "symbol=" );
        queryString.append( symbol );

        queryString.append("&side=");
        queryString.append( side );

        if(isPostOnly){
            queryString.append("&type=");
            queryString.append("LIMIT_MAKER");
        }else{
            queryString.append("&type=");
            queryString.append( type );
        }

        queryString.append("&quantity=");
        queryString.append( to_string( quantity) );

        queryString.append("&newOrderRespType=");
        queryString.append( "ACK" );
        
        //if MARKET,not send price
        if(strcmp("MARKET", type) != 0)
        {
            queryString.append("&timeInForce=");
            queryString.append( timeInForce );
        }

        if(strcmp("MARKET", type) != 0)//??
        {
            queryString.append("&price=");
            std::string priceStr;
            std::stringstream convertStream;
            convertStream <<std::fixed << std::setprecision(8) << price;
            convertStream >> priceStr;

            KF_LOG_INFO(logger, "[send_margin_order] (priceStr)" << priceStr);

            queryString.append( priceStr );
        }

        if ( strlen( newClientOrderId ) > 0 ) {
            queryString.append("&newClientOrderId=");
            queryString.append( newClientOrderId );
        }

        if ( stopPrice > 0.0 ) {
            queryString.append("&stopPrice=");
            queryString.append( to_string( stopPrice ) );
        }

        if ( icebergQty > 0.0 ) {
            queryString.append("&icebergQty=");
            queryString.append( to_string( icebergQty ) );
        }

        if ( recvWindow > 0 ) {
            queryString.append("&recvWindow=");
            queryString.append( to_string( recvWindow) );
        }

        queryString.append("&timestamp=");
        queryString.append(Timestamp);

        queryString.append("&active=");
        queryString.append(to_string(active));

        std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
        queryString.append( "&signature=");
        queryString.append( signature );

        string url = requestPath + queryString;

        if (order_count_over_limit(unit))//write 判断请求次数是否超过限制,加锁
        {
            //send err msg to strategy
            std::string strErr = "{\"code\":-1429,\"msg\":\"order count over 100000 limit.\"}";
            json.Parse(strErr.c_str());
            return -1429;
        }

        if (unit.bHandle_429)//超出请求速率限制
        {
            if (isHandling(unit))//wirite ,已加锁
            {
                std::string strErr = "{\"code\":-1429,\"msg\":\"handle 429, prohibit send order.\"}";
                json.Parse(strErr.c_str());
                return -1429;
            }
        }

        

        if (m_interface_switch > 0) {//read 
            //std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);

            interface = m_interfaceMgr.getActiveInterface();
           // lck.unlock();
            KF_LOG_INFO(logger, "[send_margin_order] interface: [" << interface << "].");
            if (interface.empty()) {
                KF_LOG_INFO(logger, "[send_margin_order] interface is empty, decline message sending!");
                std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
                json.Parse(strRefused.c_str());
                return -1430;
            }
        }
        handle_request_weight(unit,interface,SendOrder_Type);//已加锁

        std::unique_lock<std::mutex> lck(http_mutex);
        response = Post(Url{url},
                                  Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                                  Body{body}, Timeout{100000}, Interface{interface});

        KF_LOG_INFO(logger, "[send_margin_order] (url) " << url << " (response.status_code) " << response.status_code <<
                                                         " (response.error.message) " << response.error.message <<
                                                         " (response.text) " << response.text.c_str());
        lck.unlock();

        if (response.status_code == HTTP_CONNECT_REFUSED)
        {
            meet_429(unit);// 已加锁 
            break;
        }
        else if(response.status_code == HTTP_GATEWAY_TIMEOUT){
            KF_LOG_INFO(logger,"504");
            int errorId = 504;
            std::string errorMsg = response.text;
            on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        }

        if(shouldRetry(response.status_code, response.error.message, response.text)) {
            should_retry = true;
            retry_times++;
            KF_LOG_DEBUG(logger, "[send_margin_order] out_retry sleep time :" <<retry_interval_milliseconds<<"ms");
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[send_margin_order] out_retry (response.status_code) " << response.status_code <<
                                                                         " interface [" << interface <<
                                                                         "] (response.error.message) " << response.error.message <<
                                                             " (response.text) " << response.text.c_str() );
    if (response.status_code == HTTP_CONNECT_REFUSED || response.status_code == HTTP_CONNECT_BANS) {
        if (m_interface_switch > 0) {
            m_interfaceMgr.disable(interface);//write 
            KF_LOG_INFO(logger, "[send_margin_order] interface [" << interface << "] is disabled!");
        }
    }
    
    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}

int TDEngineBinance::get_margin_order(AccountUnitBinance& unit, const char *symbol, long orderId, const char *origClientOrderId, Document& json, LFOrderActionField data,int requestId)
{
    KF_LOG_INFO(logger, "[get_margin_order]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    std::string requestPath = restBaseUrl +  "/sapi/v1/margin/order?";
    std::string queryString("");
    std::string body = "";

    queryString.append( "symbol=" );
    queryString.append( symbol );

    if ( orderId > 0 ) {
        queryString.append("&orderId=");
        queryString.append( to_string( orderId ) );
    }

    if ( strlen( origClientOrderId ) > 0 ) {
        queryString.append("&origClientOrderId=");
        queryString.append( origClientOrderId );
    }

    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( to_string( recvWindow) );
    }

    queryString.append("&timestamp=");
    queryString.append( Timestamp );

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    string url = requestPath + queryString;

    if (unit.bHandle_429)
    {
        isHandling(unit);
    }

    

    string interface;
    if (m_interface_switch > 0) {
        //std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
        interface = m_interfaceMgr.getActiveInterface();
        //lck.unlock();
        KF_LOG_INFO(logger, "[get_margin_order] interface: [" << interface << "].");
        if (interface.empty()) {
            KF_LOG_INFO(logger, "[get_margin_order] interface is empty, decline message sending!");
            return -1430;
        }
    }
    handle_request_weight(unit,interface,GetOrder_Type);
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                              Body{body}, Timeout{100000}, Interface{interface});

    KF_LOG_INFO(logger, "[get_margin_order] (url) " << url << " (response.status_code) " << response.status_code <<
                                              " interface [" << interface <<
                                              "] (response.error.message) " << response.error.message <<
                                              " (response.text) " << response.text.c_str());
    lck.unlock();
    if (response.status_code == HTTP_CONNECT_REFUSED)
    {
        meet_429(unit);
    }
    else if(response.status_code == HTTP_GATEWAY_TIMEOUT){
        int errorId = 504;
        std::string errorMsg = response.text;
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
    }

    if (response.status_code == HTTP_CONNECT_REFUSED || response.status_code == HTTP_CONNECT_BANS) {
        if (m_interface_switch > 0) {
            //std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
            m_interfaceMgr.disable(interface);
          //  lck.unlock();
            KF_LOG_INFO(logger, "[get_margin_order] interface [" << interface << "] is disabled!");
        }
    }
    
    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}

int TDEngineBinance::get_margin_open_orders(AccountUnitBinance& unit, const char *symbol, Document &json)
{
    KF_LOG_INFO(logger, "[get_margin_open_orders]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    std::string requestPath = restBaseUrl + "/sapi/v1/margin/query/openOrders?";
    std::string queryString("");
    std::string body = "";

    bool hasSetParameter = false;

    if(strlen(symbol) > 0) {
        queryString.append( "symbol=" );
        queryString.append( symbol );
        hasSetParameter = true;
    }

    if ( recvWindow > 0 ) {
        if(hasSetParameter)
        {
            queryString.append("&recvWindow=");
            queryString.append( to_string( recvWindow) );
        } else {
            queryString.append("recvWindow=");
            queryString.append( to_string( recvWindow) );
        }
        hasSetParameter = true;
    }

    if(hasSetParameter)
    {
        queryString.append("&timestamp=");
        queryString.append( Timestamp );
    } else {
        queryString.append("timestamp=");
        queryString.append( Timestamp );
    }

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    std::string interface;
    if (m_interface_switch > 0) {
        //   std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
        interface = m_interfaceMgr.getActiveInterface();
        //   lck.unlock()
        KF_LOG_INFO(logger, "[send_order] interface: [" << interface << "].");
        if (interface.empty()) {
            KF_LOG_INFO(logger, "[send_order] interface is empty, decline message sending!");
            std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
            json.Parse(strRefused.c_str());
            return -1430;
        }
    }
    handle_request_weight(unit,interface,GetOpenOrder_Type);

    string url = requestPath + queryString;

    
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                                 Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                                 Body{body}, Timeout{100000}, Interface{interface});

    KF_LOG_INFO(logger, "[get_margin_open_orders] (url) " << url << " (response.status_code) " << response.status_code <<
                                                 " (response.error.message) " << response.error.message <<
                                                 " (response.text) " << response.text.c_str());
    lck.unlock();
     
    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}


void TDEngineBinance::cancel_margin_order(AccountUnitBinance& unit, const char *symbol,
                  long orderId, const char *origClientOrderId, const char *newClientOrderId, Document &json)
{
    KF_LOG_INFO(logger, "[cancel_margin_order]");
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    string interface;
    do {
        should_retry = false;

        long recvWindow = order_action_recvwindow_ms;
        std::string Timestamp = getTimestampString();
        std::string Method = "DELETE";
        std::string requestPath =restBaseUrl + "/sapi/v1/margin/order?";
        std::string queryString("");
        std::string body = "";

        queryString.append( "symbol=" );
        queryString.append( symbol );

        if ( orderId > 0 ) {
            queryString.append("&orderId=");
            queryString.append( to_string( orderId ) );
        }

        if ( strlen( origClientOrderId ) > 0 ) {
            queryString.append("&origClientOrderId=");
            queryString.append( origClientOrderId );
        }

        if ( strlen( newClientOrderId ) > 0 ) {
            queryString.append("&newClientOrderId=");
            queryString.append( newClientOrderId );
        }

        if ( recvWindow > 0 ) {
            queryString.append("&recvWindow=");
            queryString.append( std::to_string( recvWindow) );
        }

        queryString.append("&timestamp=");
        queryString.append( Timestamp );

        std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
        queryString.append( "&signature=");
        queryString.append( signature );

        string url = requestPath + queryString;

        if (order_count_over_limit(unit))
        {
            std::string strErr = "{\"code\":-1429,\"msg\":\"order count over 100000 limit.\"}";
            json.Parse(strErr.c_str());
            return;
        }

        if (unit.bHandle_429)
        {
            if (isHandling(unit))
            {
                std::string strErr = "{\"code\":-1429,\"msg\":\"handle 429, prohibit cancel order.\"}";
                json.Parse(strErr.c_str());
                return;
            }
        }

        
        if (m_interface_switch > 0) {
           // std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
            interface = m_interfaceMgr.getActiveInterface();
           // lck.unlock();
            KF_LOG_INFO(logger, "[cancel_margin_order] interface: [" << interface << "].");
            if (interface.empty()) {
                KF_LOG_INFO(logger, "[cancel_margin_order] interface is empty, decline message sending!");
                std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
                json.Parse(strRefused.c_str());
                return;
            }
        }
        handle_request_weight(unit,interface,CancelOrder_Type);
        std::unique_lock<std::mutex> lck(http_mutex);
        response = Delete(Url{url},
                                  Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                                  Body{body}, Timeout{100000}, Interface{interface});

        KF_LOG_INFO(logger, "[cancel_margin_order] (url) " << url << " (response.status_code) " << response.status_code <<
                                                 " (response.error.message) " << response.error.message <<
                                                 " (response.text) " << response.text.c_str());
        lck.unlock();
        if (response.status_code == HTTP_CONNECT_REFUSED)
        {
            meet_429(unit);
            break;
        }

        if(shouldRetry(response.status_code, response.error.message, response.text)) {
            should_retry = true;
            retry_times++;
            KF_LOG_DEBUG(logger, "[cancel_margin_order] out_retry sleep time :" <<retry_interval_milliseconds<<"ms");
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[cancel_margin_order] out_retry (response.status_code) " << response.status_code <<
                                                                         " interface [" << interface <<
                                                                         "] (response.error.message) " << response.error.message <<
                                                                         " (response.text) " << response.text.c_str() );
    if (response.status_code == HTTP_CONNECT_REFUSED || response.status_code == HTTP_CONNECT_BANS) {
        if (m_interface_switch > 0) {
          //  std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
            m_interfaceMgr.disable(interface);
           // lck.unlock();
            KF_LOG_INFO(logger, "[cancel_margin_order] interface [" << interface << "] is disabled!");
        }
    }

    return getResponse(response.status_code, response.text, response.error.message, json);
}

int TDEngineBinance::cancel_margin_order(AccountUnitBinance& unit, const char *symbol,
                  long orderId, const char *origClientOrderId, const char *newClientOrderId, Document &json, const LFOrderActionField data, int requestId)
{
    KF_LOG_INFO(logger, "[cancel_margin_order]");
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    string interface;
    do {
        should_retry = false;

        long recvWindow = order_action_recvwindow_ms;
        std::string Timestamp = getTimestampString();
        std::string Method = "DELETE";
        std::string requestPath =restBaseUrl + "/sapi/v1/margin/order?";
        std::string queryString("");
        std::string body = "";

        queryString.append( "symbol=" );
        queryString.append( symbol );

        if ( orderId > 0 ) {
            queryString.append("&orderId=");
            queryString.append( to_string( orderId ) );
        }

        if ( strlen( origClientOrderId ) > 0 ) {
            queryString.append("&origClientOrderId=");
            queryString.append( origClientOrderId );
        }

        if ( strlen( newClientOrderId ) > 0 ) {
            queryString.append("&newClientOrderId=");
            queryString.append( newClientOrderId );
        }

        if ( recvWindow > 0 ) {
            queryString.append("&recvWindow=");
            queryString.append( std::to_string( recvWindow) );
        }

        queryString.append("&timestamp=");
        queryString.append( Timestamp );

        std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
        queryString.append( "&signature=");
        queryString.append( signature );

        string url = requestPath + queryString;

        if (order_count_over_limit(unit))
        {
            std::string strErr = "{\"code\":-1429,\"msg\":\"order count over 100000 limit.\"}";
            json.Parse(strErr.c_str());
            return -1429;
        }

        if (unit.bHandle_429)
        {
            if (isHandling(unit))
            {
                std::string strErr = "{\"code\":-1429,\"msg\":\"handle 429, prohibit cancel order.\"}";
                json.Parse(strErr.c_str());
                return -1429;
            }
        }

        
        if (m_interface_switch > 0) {
           // std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
            interface = m_interfaceMgr.getActiveInterface();
           // lck.unlock();
            KF_LOG_INFO(logger, "[cancel_margin_order] interface: [" << interface << "].");
            if (interface.empty()) {
                KF_LOG_INFO(logger, "[cancel_margin_order] interface is empty, decline message sending!");
                std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
                json.Parse(strRefused.c_str());
                return -1430;
            }
        }
        handle_request_weight(unit,interface,CancelOrder_Type);
        std::unique_lock<std::mutex> lck(http_mutex);
        response = Delete(Url{url},
                                  Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                                  Body{body}, Timeout{100000}, Interface{interface});

        KF_LOG_INFO(logger, "[cancel_margin_order] (url) " << url << " (response.status_code) " << response.status_code <<
                                                 " (response.error.message) " << response.error.message <<
                                                 " (response.text) " << response.text.c_str());
        lck.unlock();
        if (response.status_code == HTTP_CONNECT_REFUSED)
        {
            meet_429(unit);
            break;
        }
        else if(response.status_code == HTTP_GATEWAY_TIMEOUT){
            int errorId = 504;
            std::string errorMsg = response.text;
            on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        }

        if(shouldRetry(response.status_code, response.error.message, response.text)) {
            should_retry = true;
            retry_times++;
            KF_LOG_DEBUG(logger, "[cancel_margin_order] out_retry sleep time :" <<retry_interval_milliseconds<<"ms");
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[cancel_margin_order] out_retry (response.status_code) " << response.status_code <<
                                                                         " interface [" << interface <<
                                                                         "] (response.error.message) " << response.error.message <<
                                                                         " (response.text) " << response.text.c_str() );
    if (response.status_code == HTTP_CONNECT_REFUSED || response.status_code == HTTP_CONNECT_BANS) {
        if (m_interface_switch > 0) {
          //  std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
            m_interfaceMgr.disable(interface);
           // lck.unlock();
            KF_LOG_INFO(logger, "[cancel_margin_order] interface [" << interface << "] is disabled!");
        }
    }

    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}

int TDEngineBinance::send_order(AccountUnitBinance& unit, const char *symbol,
                const char *side,
                const char *type,
                const char *timeInForce,
                double quantity,
                double price,
                const char *newClientOrderId,
                double stopPrice,
                double icebergQty,
                Document &json,
                const LFInputOrderField data,
                int requestId,
                bool isPostOnly
                )
{
    KF_LOG_INFO(logger, "[send_order]");

	string interface;
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        should_retry = false;

        long recvWindow = order_insert_recvwindow_ms;
        std::string Timestamp = getTimestampString();
        std::string Method = "POST";
        std::string requestPath = restBaseUrl + "/api/v3/order?";
        std::string queryString("");
        std::string body = "";

        queryString.append( "symbol=" );
        queryString.append( symbol );

        queryString.append("&side=");
        queryString.append( side );

        if(isPostOnly){
            queryString.append("&type=");
            queryString.append("LIMIT_MAKER");            
        }else{
            queryString.append("&type=");
            queryString.append( type );
        }

        queryString.append("&newOrderRespType=");
        queryString.append( "ACK" );
        //if MARKET,not send price or timeInForce
        if(strcmp("MARKET", type) != 0 && !isPostOnly)
        {
            queryString.append("&timeInForce=");
            queryString.append( timeInForce );
        }

        queryString.append("&quantity=");
        queryString.append( to_string( quantity) );

        if(strcmp("MARKET", type) != 0)
        {
            queryString.append("&price=");
            std::string priceStr;
            std::stringstream convertStream;
            convertStream <<std::fixed << std::setprecision(8) << price;
            convertStream >> priceStr;

            KF_LOG_INFO(logger, "[send_order] (priceStr)" << priceStr);

            queryString.append( priceStr );
        }

        if ( strlen( newClientOrderId ) > 0 ) {
            queryString.append("&newClientOrderId=");
            queryString.append( newClientOrderId );
        }

        if ( stopPrice > 0.0 ) {
            queryString.append("&stopPrice=");
            queryString.append( to_string( stopPrice ) );
        }

        if ( icebergQty > 0.0 ) {
            queryString.append("&icebergQty=");
            queryString.append( to_string( icebergQty ) );
        }

        if ( recvWindow > 0 ) {
            queryString.append("&recvWindow=");
            queryString.append( to_string( recvWindow) );
        }

        queryString.append("&timestamp=");
        queryString.append(Timestamp);

        std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
        queryString.append( "&signature=");
        queryString.append( signature );

        string url = requestPath + queryString;

        if (order_count_over_limit(unit))
        {
            //send err msg to strategy
            std::string strErr = "{\"code\":-1429,\"msg\":\"order count over 100000 limit.\"}";
            json.Parse(strErr.c_str());
            return -1429;
        }

        if (unit.bHandle_429)
        {
            if (isHandling(unit))
            {
                std::string strErr = "{\"code\":-1429,\"msg\":\"handle 429, prohibit send order.\"}";
                json.Parse(strErr.c_str());
                return -1429;
            }
        }

        
		if (m_interface_switch > 0) {
         //   std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
	        interface = m_interfaceMgr.getActiveInterface();
         //   lck.unlock()
	        KF_LOG_INFO(logger, "[send_order] interface: [" << interface << "].");
	        if (interface.empty()) {
	            KF_LOG_INFO(logger, "[send_order] interface is empty, decline message sending!");
	            std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
	            json.Parse(strRefused.c_str());
	            return -1430;
	        }
		}
        handle_request_weight(unit,interface,SendOrder_Type);
        std::unique_lock<std::mutex> lck(http_mutex);
        
        response = Post(Url{url},
                                  Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                                  Body{body}, Timeout{100000}, Interface{interface});

        KF_LOG_INFO(logger, "[send_order] (url) " << url << " (response.status_code) " << response.status_code <<
                                                         " (response.error.message) " << response.error.message <<
                                                         " (response.text) " << response.text.c_str());
        lck.unlock();

        if (response.status_code == HTTP_CONNECT_REFUSED)
        {
            meet_429(unit);
            break;
        }
        else if(response.status_code == HTTP_GATEWAY_TIMEOUT){
            KF_LOG_INFO(logger,"504");
            int errorId = 504;
            std::string errorMsg = response.text;
            on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        }

        if(shouldRetry(response.status_code, response.error.message, response.text)) {
            should_retry = true;
            retry_times++;
            KF_LOG_DEBUG(logger, "[send_order] out_retry sleep time :" << retry_interval_milliseconds<<"ms ");
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[send_order] out_retry (response.status_code) " << response.status_code <<
																		 " interface [" << interface <<
                                                                         "] (response.error.message) " << response.error.message <<
                                                                         " (response.text) " << response.text.c_str() );
	if (response.status_code == HTTP_CONNECT_REFUSED || response.status_code == HTTP_CONNECT_BANS) {
        if (m_interface_switch > 0) {
            //std::unique_lock<std::mutex> lck(http_mutex);
			m_interfaceMgr.disable(interface);
            //lck.unlock();
			KF_LOG_INFO(logger, "[send_order] interface [" << interface << "] is disabled!");
		}
	}
	
    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}

/*
 * https://github.com/binance-exchange/binance-official-api-docs/blob/master/errors.md
-1021 INVALID_TIMESTAMP
Timestamp for this request is outside of the recvWindow.
Timestamp for this request was 1000ms ahead of the server's time.


 *  (response.status_code) 400 (response.error.message)  (response.text) {"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}
 * */
bool TDEngineBinance::shouldRetry(int http_status_code, std::string errorMsg, std::string text)
{
    if( 400 == http_status_code && text.find(":-1021") != std::string::npos )
    {
        //std::unique_lock<std::mutex> lck(mutex_account);
        getTimeDiffOfExchange(account_units[0]);
        return true;
    }
    return false;
}

bool TDEngineBinance::order_count_over_limit(AccountUnitBinance& unit)
{

    std::unique_lock<std::mutex> lck(*unit.mutex_order_count);
    //UTC 00：00：00 reset order_total_limit
    uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    uint64_t UTC_timestamp = timestamp + timeDiffOfExchange;

    if ((UTC_timestamp / 86400000) != (last_UTC_timestamp / 86400000))
    {
        last_UTC_timestamp = UTC_timestamp;
        KF_LOG_DEBUG(logger, "[order_count_over_limit] (order_total_count)" << unit.order_total_count << " at UTC 00:00:00 and reset");
        unit.order_total_count = 0;
    }

    if (unit.order_total_count >= 200000)
    {
        KF_LOG_DEBUG(logger, "[order_count_over_limit] (order_total_count)" << unit.order_total_count << " over 100000/day limit!");
        return true;
    }
    
    timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (unit.time_queue.size() <= 0)
    {
        unit.time_queue.push(timestamp);
        unit.order_total_count++;
        return false;
    }

    uint64_t startTime = unit.time_queue.front();
    int order_time_diff_ms = timestamp - startTime;
    KF_LOG_DEBUG(logger, "[order_count_over_limit] (order_time_diff_ms)" << order_time_diff_ms 
        << " (time_queue.size)" << unit.time_queue.size()
        << " (order_total_count)" << unit.order_total_count
        << " (order_count_per_second)" << order_count_per_second);
    
    const int order_ms = 1000;      //1s
    if (order_time_diff_ms < order_ms)
    {
        //in second        
        if(unit.time_queue.size() < order_count_per_second)
        {
            //do not reach limit in second
            unit.time_queue.push(timestamp);
            unit.order_total_count++;
            return false;
        }

        //reach limit in second/over limit count, sleep
        KF_LOG_DEBUG(logger, "[order_count_over_limit]  sleep time :" <<(order_ms - order_time_diff_ms)<<"ms");
        usleep((order_ms - order_time_diff_ms) * 1000);
        timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }

    //move receive window to next step
    unit.time_queue.pop();
    unit.time_queue.push(timestamp);
    unit.order_total_count++;

    //清理超过1秒的记录
    while(unit.time_queue.size() > 0)
    {
        uint64_t tmpTime = unit.time_queue.front();
        int tmp_time_diff_ms = timestamp - tmpTime;
        if (tmp_time_diff_ms <= order_ms)
        {
            break;
        }

        unit.time_queue.pop();
    }
    return false;
}

void TDEngineBinance::handle_request_weight(AccountUnitBinance& unit,std::string interface,RequestWeightType type)
{
    if (request_weight_per_minute <= 0)
    {
        //do nothing even meet 429
        return;
    }

    std::lock_guard<std::mutex> guard_mutex(interface_mutex);
    uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    std::queue<weight_data>& weight_data_queue = interface_map[interface].first;
    int& weight_count = interface_map[interface].second;
    if (weight_data_queue.size() <= 0 || weight_count <= 0)
    {
        weight_data wd;
        wd.time = timestamp;
        wd.addWeight(type);
        weight_count += wd.weight;
        weight_data_queue.push(wd);
        return;
    }

    weight_data front_data = weight_data_queue.front();
    int time_diff_ms = timestamp - front_data.time;
    KF_LOG_DEBUG(logger, "[handle_request_weight] (time_diff_ms:" << timestamp <<" - " << front_data.time << ")" << time_diff_ms 
        << " (weight_data_queue.size)" << weight_data_queue.size()
        << " (weight_count)" << weight_count
        << " (request_weight_per_minute)" << request_weight_per_minute);

    const int weight_ms = 60000;     //60s,1minute
    if (time_diff_ms <= weight_ms)
    {
        //in minute
        if(weight_count < request_weight_per_minute)
        {
            //do not reach limit in second
            weight_data wd;
            wd.time = timestamp;
            wd.addWeight(type);
            weight_count += wd.weight;
            weight_data_queue.push(wd);
            return;
        }
        //reach limit in minute/over weight limit count, sleep
        int time2sleep = std::max((weight_ms - time_diff_ms),100);
        usleep( time2sleep * 1000);
        KF_LOG_DEBUG(logger, "[handle_request_weight] sleep time :" <<time2sleep<<"ms");
        timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }
    
    weight_count -= front_data.weight;
    weight_data_queue.pop();

    weight_data wd;
    wd.time = timestamp;
    wd.addWeight(type);
    weight_count += wd.weight;
    weight_data_queue.push(wd);

    //清理时间超过60000ms(1分钟)的记录
    while(weight_data_queue.size() > 0)
    {
        weight_data tmp_data = weight_data_queue.front();
        int tmp_time_diff_ms = timestamp - tmp_data.time;
        if (tmp_time_diff_ms < weight_ms)
        {
            break;
        } 

        weight_count -= tmp_data.weight;
        weight_data_queue.pop();
    }
    KF_LOG_DEBUG(logger, "[handle_request_weight] end: (time_diff_ms)" <<  (weight_data_queue.back().time - weight_data_queue.front().time)
        << " (weight_data_queue.size)" << weight_data_queue.size()
        << " (weight_count)" << weight_count);
}

void TDEngineBinance::meet_429(AccountUnitBinance& unit)
{
    std::lock_guard<std::mutex> guard_mutex(*unit.mutex_handle_429);
    if (request_weight_per_minute <= 0)
    {
        KF_LOG_INFO(logger, "[meet_429] request_weight_per_minute <= 0, return");
        return;
    }

    if (unit.bHandle_429)
    {
        KF_LOG_INFO(logger, "[meet_429] 418 prevention mechanism already activated, return");
        return;
    }

    unit.startTime_429 = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    unit.bHandle_429 = true;
    KF_LOG_INFO(logger, "[meet_429] 429 warning received, current request_weight_per_minute: " << request_weight_per_minute);
}

bool TDEngineBinance::isHandling(AccountUnitBinance& unit)
{
    std::lock_guard<std::mutex> guard_mutex(*unit.mutex_handle_429);
    uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    int handle_429_time_diff_ms = timestamp - unit.startTime_429;
    if (handle_429_time_diff_ms > prohibit_order_ms)
    {
        //stop handle 429
        unit.startTime_429 = 0;
        unit.bHandle_429 = false;
        KF_LOG_INFO(logger, "[isHandling] handle_429_time_diff_ms > prohibit_order_ms, stop handle 429");
    }
    KF_LOG_INFO(logger, "[isHandling] " << " bHandle_429 " << unit.bHandle_429 << " request_weight_per_minute " << request_weight_per_minute);
    return unit.bHandle_429;
}

int TDEngineBinance::get_order(AccountUnitBinance& unit, const char *symbol, long orderId, const char *origClientOrderId, Document& json, LFOrderActionField data,int requestId)
{
    KF_LOG_INFO(logger, "[get_order]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    std::string requestPath = restBaseUrl +  "/api/v3/order?";
    std::string queryString("");
    std::string body = "";

    queryString.append( "symbol=" );
    queryString.append( symbol );

    if ( orderId > 0 ) {
        queryString.append("&orderId=");
        queryString.append( to_string( orderId ) );
    }

    if ( strlen( origClientOrderId ) > 0 ) {
        queryString.append("&origClientOrderId=");
        queryString.append( origClientOrderId );
    }

    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( to_string( recvWindow) );
    }

    queryString.append("&timestamp=");
    queryString.append( Timestamp );

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    string url = requestPath + queryString;

    if (unit.bHandle_429)
    {
        isHandling(unit);
    }

    

	string interface;
	if (m_interface_switch > 0) {
       //     std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
		interface = m_interfaceMgr.getActiveInterface();
        //lck.unlock();
		KF_LOG_INFO(logger, "[get_order] interface: [" << interface << "].");
		if (interface.empty()) {
		    KF_LOG_INFO(logger, "[get_order] interface is empty, decline message sending!");
		    return -1;
		}
	}
    handle_request_weight(unit,interface,GetOrder_Type);
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                              Body{body}, Timeout{100000}, Interface{interface});

    KF_LOG_INFO(logger, "[get_order] (url) " << url << " (response.status_code) " << response.status_code <<
											  " interface [" << interface <<
                                              "] (response.error.message) " << response.error.message <<
                                              " (response.text) " << response.text.c_str());
    lck.unlock();
    if (response.status_code == HTTP_CONNECT_REFUSED)
    {
        meet_429(unit);
    }
    else if(response.status_code == HTTP_GATEWAY_TIMEOUT){
        int errorId = 504;
        std::string errorMsg = response.text;
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
    }

	if (response.status_code == HTTP_CONNECT_REFUSED || response.status_code == HTTP_CONNECT_BANS) {
		if (m_interface_switch > 0) {
         //   std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);

			m_interfaceMgr.disable(interface);

          //  lck.unlock();
			KF_LOG_INFO(logger, "[get_order] interface [" << interface << "] is disabled!");
		}
	}

	
    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}

void TDEngineBinance::cancel_order(AccountUnitBinance& unit, const char *symbol,
                  long orderId, const char *origClientOrderId, const char *newClientOrderId, Document &json)
{
    KF_LOG_INFO(logger, "[cancel_order]");
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
	string interface;
    do {
        should_retry = false;

        long recvWindow = order_action_recvwindow_ms;
        std::string Timestamp = getTimestampString();
        std::string Method = "DELETE";
        std::string requestPath =restBaseUrl + "/api/v3/order?";
        std::string queryString("");
        std::string body = "";

        queryString.append( "symbol=" );
        queryString.append( symbol );

        if ( orderId > 0 ) {
            queryString.append("&orderId=");
            queryString.append( to_string( orderId ) );
        }

        else if ( strlen( origClientOrderId ) > 0 ) {
            queryString.append("&origClientOrderId=");
            queryString.append( origClientOrderId );
        }

        else if ( strlen( newClientOrderId ) > 0 ) {
            queryString.append("&newClientOrderId=");
            queryString.append( newClientOrderId );
        }

        if ( recvWindow > 0 ) {
            queryString.append("&recvWindow=");
            queryString.append( std::to_string( recvWindow) );
        }

        queryString.append("&timestamp=");
        queryString.append( Timestamp );

        std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
        queryString.append( "&signature=");
        queryString.append( signature );

        string url = requestPath + queryString;

        if (order_count_over_limit(unit))
        {
            std::string strErr = "{\"code\":-1429,\"msg\":\"order count over 100000 limit.\"}";
            json.Parse(strErr.c_str());
            return;
        }

        if (unit.bHandle_429)
        {
            if (isHandling(unit))
            {
                std::string strErr = "{\"code\":-1429,\"msg\":\"handle 429, prohibit cancel order.\"}";
                json.Parse(strErr.c_str());
                return;
            }
        }

       
		if (m_interface_switch > 0) {
           // std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
	        interface = m_interfaceMgr.getActiveInterface();
            //lck.unlock();
	        KF_LOG_INFO(logger, "[cancel_order] interface: [" << interface << "].");
	        if (interface.empty()) {
	            KF_LOG_INFO(logger, "[cancel_order] interface is empty, decline message sending!");
	            std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
	            json.Parse(strRefused.c_str());
	            return;
	        }
		}
        handle_request_weight(unit,interface,CancelOrder_Type);

        std::unique_lock<std::mutex> lck(http_mutex);
        response = Delete(Url{url},
                                  Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                                  Body{body}, Timeout{100000}, Interface{interface});

        KF_LOG_INFO(logger, "[cancel_order] (url) " << url << " (response.status_code) " << response.status_code <<
                                                 " (response.error.message) " << response.error.message <<
                                                 " (response.text) " << response.text.c_str());
        lck.unlock();
        if (response.status_code == HTTP_CONNECT_REFUSED)
        {
            meet_429(unit);
            break;
        }

        if(shouldRetry(response.status_code, response.error.message, response.text)) {
            should_retry = true;
            retry_times++;
            KF_LOG_DEBUG(logger, "[cancel_order] out_retry sleep time :" <<retry_interval_milliseconds<<"ms");
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[cancel_order] out_retry (response.status_code) " << response.status_code <<
                                                                         " interface [" << interface <<
                                                                         "] (response.error.message) " << response.error.message <<
                                                                         " (response.text) " << response.text.c_str() );
	if (response.status_code == HTTP_CONNECT_REFUSED || response.status_code == HTTP_CONNECT_BANS) {
        if (m_interface_switch > 0) {
            //std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);            
			m_interfaceMgr.disable(interface);
           // lck.unlock();
			KF_LOG_INFO(logger, "[cancel_order] interface [" << interface << "] is disabled!");
		}
	}

    return getResponse(response.status_code, response.text, response.error.message, json);
}

int TDEngineBinance::cancel_order(AccountUnitBinance& unit, const char *symbol,
                  long orderId, const char *origClientOrderId, const char *newClientOrderId, Document &json, const LFOrderActionField data, int requestId)
{
    KF_LOG_INFO(logger, "[cancel_order]");
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    string interface;
    do {
        should_retry = false;

        long recvWindow = order_action_recvwindow_ms;
        std::string Timestamp = getTimestampString();
        std::string Method = "DELETE";
        std::string requestPath =restBaseUrl + "/api/v3/order?";
        std::string queryString("");
        std::string body = "";

        queryString.append( "symbol=" );
        queryString.append( symbol );

        if ( orderId > 0 ) {
            queryString.append("&orderId=");
            queryString.append( to_string( orderId ) );
        }

        else if ( strlen( origClientOrderId ) > 0 ) {
            queryString.append("&origClientOrderId=");
            queryString.append( origClientOrderId );
        }

        else if ( strlen( newClientOrderId ) > 0 ) {
            queryString.append("&newClientOrderId=");
            queryString.append( newClientOrderId );
        }

        if ( recvWindow > 0 ) {
            queryString.append("&recvWindow=");
            queryString.append( std::to_string( recvWindow) );
        }

        queryString.append("&timestamp=");
        queryString.append( Timestamp );

        std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
        queryString.append( "&signature=");
        queryString.append( signature );

        string url = requestPath + queryString;

        if (order_count_over_limit(unit))
        {
            std::string strErr = "{\"code\":-1429,\"msg\":\"order count over 100000 limit.\"}";
            json.Parse(strErr.c_str());
            return -1429;
        }

        if (unit.bHandle_429)
        {
            if (isHandling(unit))
            {
                std::string strErr = "{\"code\":-1429,\"msg\":\"handle 429, prohibit cancel order.\"}";
                json.Parse(strErr.c_str());
                return -1429;
            }
        }

       
        if (m_interface_switch > 0) {
           // std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
            interface = m_interfaceMgr.getActiveInterface();
            //lck.unlock();
            KF_LOG_INFO(logger, "[cancel_order] interface: [" << interface << "].");
            if (interface.empty()) {
                KF_LOG_INFO(logger, "[cancel_order] interface is empty, decline message sending!");
                std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
                json.Parse(strRefused.c_str());
                return -1430;
            }
        }
        handle_request_weight(unit,interface,CancelOrder_Type);

        std::unique_lock<std::mutex> lck(http_mutex);
        response = Delete(Url{url},
                                  Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                                  Body{body}, Timeout{100000}, Interface{interface});

        KF_LOG_INFO(logger, "[cancel_order] (url) " << url << " (response.status_code) " << response.status_code <<
                                                 " (response.error.message) " << response.error.message <<
                                                 " (response.text) " << response.text.c_str());
        lck.unlock();
        if (response.status_code == HTTP_CONNECT_REFUSED)
        {
            meet_429(unit);
            break;
        }
        else if(response.status_code == HTTP_GATEWAY_TIMEOUT){
            int errorId = 504;
            std::string errorMsg = response.text;
            on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        }

        if(shouldRetry(response.status_code, response.error.message, response.text)) {
            should_retry = true;
            retry_times++;
            KF_LOG_DEBUG(logger, "[cancel_order] out_retry sleep time :" <<retry_interval_milliseconds<<"ms");
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[cancel_order] out_retry (response.status_code) " << response.status_code <<
                                                                         " interface [" << interface <<
                                                                         "] (response.error.message) " << response.error.message <<
                                                                         " (response.text) " << response.text.c_str() );
    if (response.status_code == HTTP_CONNECT_REFUSED || response.status_code == HTTP_CONNECT_BANS) {
        if (m_interface_switch > 0) {
            //std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);            
            m_interfaceMgr.disable(interface);
           // lck.unlock();
            KF_LOG_INFO(logger, "[cancel_order] interface [" << interface << "] is disabled!");
        }
    }

    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}

void TDEngineBinance::get_my_trades(AccountUnitBinance& unit, const char *symbol, int limit, int64_t fromId, Document &json)
{
    KF_LOG_INFO(logger, "[get_my_trades]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    std::string requestPath = restBaseUrl + "/api/v3/myTrades?";
    std::string queryString("");
    std::string body = "";

    queryString.append( "symbol=" );
    queryString.append( symbol );
    if ( limit > 0 ) {
        queryString.append("&limit=");
        queryString.append( std::to_string( limit ) );
    }
    if ( fromId > 0 ) {
        queryString.append("&fromId=");
        queryString.append( std::to_string( fromId ) );
    }

    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( std::to_string( recvWindow ) );
    }

    queryString.append("&timestamp=");
    queryString.append( Timestamp );


    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    string url = requestPath + queryString;

    if (unit.bHandle_429)
    {
        isHandling(unit);
    }

    

	string interface;
	if (m_interface_switch > 0) {
           // std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);

		interface = m_interfaceMgr.getActiveInterface();
       // lck.unlock();
		KF_LOG_INFO(logger, "[get_my_trades] interface: [" << interface << "].");
		if (interface.empty()) {
			KF_LOG_INFO(logger, "[get_my_trades] interface is empty, decline message sending!");
			return;
		}
	}
    handle_request_weight(unit,interface,TradeList_Type);

	std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                              Body{body}, Timeout{100000}, Interface{interface});

    KF_LOG_INFO(logger, "[get_my_trades] (url) " << url << " (response.status_code) " << response.status_code <<
												" interface [" << interface <<
                                                "] (response.error.message) " << response.error.message <<
                                                " (response.text) " << response.text.c_str());
    lck.unlock();
    if (response.status_code == HTTP_CONNECT_REFUSED)
    {
        meet_429(unit);
    }
	if (response.status_code == HTTP_CONNECT_REFUSED || response.status_code == HTTP_CONNECT_BANS) {
		if (m_interface_switch > 0) {
            //std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);

			m_interfaceMgr.disable(interface);
            //lck.unlock();
			KF_LOG_INFO(logger, "[get_my_trades] interface [" << interface << "] is disabled!");
		}
	}

    return getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineBinance::get_open_orders(AccountUnitBinance& unit, const char *symbol, Document &json)
{
    KF_LOG_INFO(logger, "[get_open_orders]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    std::string requestPath = restBaseUrl + "/api/v3/openOrders?";
    std::string queryString("");
    std::string body = "";

    bool hasSetParameter = false;

    if(strlen(symbol) > 0) {
        queryString.append( "symbol=" );
        queryString.append( symbol );
        hasSetParameter = true;
    }

    if ( recvWindow > 0 ) {
        if(hasSetParameter)
        {
            queryString.append("&recvWindow=");
            queryString.append( to_string( recvWindow) );
        } else {
            queryString.append("recvWindow=");
            queryString.append( to_string( recvWindow) );
        }
        hasSetParameter = true;
    }

    if(hasSetParameter)
    {
        queryString.append("&timestamp=");
        queryString.append( Timestamp );
    } else {
        queryString.append("timestamp=");
        queryString.append( Timestamp );
    }

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    string url = requestPath + queryString;

    std::string interface;
    if (m_interface_switch > 0) {
        //   std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
        interface = m_interfaceMgr.getActiveInterface();
        //   lck.unlock()
        KF_LOG_INFO(logger, "[send_order] interface: [" << interface << "].");
        if (interface.empty()) {
            KF_LOG_INFO(logger, "[send_order] interface is empty, decline message sending!");
            std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
            json.Parse(strRefused.c_str());
            return;
        }
    }
    handle_request_weight(unit,interface,GetOpenOrder_Type);

    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                                 Header{{"X-MBX-APIKEY", unit.api_key}}, cpr::VerifySsl{false},
                                 Body{body}, Timeout{100000},Interface{interface});

    KF_LOG_INFO(logger, "[get_open_orders] (url) " << url << " (response.status_code) " << response.status_code <<
                                                 " (response.error.message) " << response.error.message <<
                                                 " (response.text) " << response.text.c_str());
    lck.unlock();
    /*If the symbol is not sent, orders for all symbols will be returned in an array.
    [
      {
        "symbol": "LTCBTC",
        "orderId": 1,
        "clientOrderId": "myOrder1",
        "price": "0.1",
        "origQty": "1.0",
        "executedQty": "0.0",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "stopPrice": "0.0",
        "icebergQty": "0.0",
        "time": 1499827319559,
        "isWorking": trueO
      }
    ]
     * */
     
    return getResponse(response.status_code, response.text, response.error.message, json);
}


void TDEngineBinance::get_exchange_time(AccountUnitBinance& unit, Document &json)
{
    KF_LOG_INFO(logger, "[get_exchange_time]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = std::to_string(getTimestamp());
    std::string Method = "GET";
    std::string requestPath = restBaseUrl + "/api/v3/time";
    std::string queryString("");
    std::string body = "";

    string url = requestPath + queryString;
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}},
                              Body{body}, Timeout{100000});

    KF_LOG_INFO(logger, "[get_exchange_time] (url) " << url << " (response.status_code) " << response.status_code <<
                                                      " (response.error.message) " << response.error.message <<
                                                      " (response.text) " << response.text.c_str());
    return getResponse(response.status_code, response.text, response.error.message, json);
}


void TDEngineBinance::get_exchange_infos(AccountUnitBinance& unit, Document &json)
{
    KF_LOG_INFO(logger, "[get_exchange_infos]");
    long recvWindow = 5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    std::string requestPath =restBaseUrl +"/api/v3/exchangeInfo";
    std::string queryString("");
    std::string body = "";

    string url = requestPath + queryString;
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}},
                              Body{body}, Timeout{100000});

    KF_LOG_INFO(logger, "[get_exchange_infos] (url) " << url << " (response.status_code) " << response.status_code <<
                                                   " (response.error.message) " << response.error.message <<
                                                   " (response.text) " << response.text.c_str());
    return getResponse(response.status_code, response.text, response.error.message, json);
}

int TDEngineBinance::get_account(AccountUnitBinance& unit, Document &json, LFRspPositionField pos,int requestId)
{
    KF_LOG_INFO(logger, "[get_account]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    std::string requestPath =restBaseUrl + "/api/v3/account?";
    std::string queryString("");
    std::string body = "";

    queryString.append("timestamp=");
    queryString.append( Timestamp );

    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( std::to_string( recvWindow ) );
    }

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    string url = requestPath + queryString;
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}},
                              Body{body}, Timeout{100000});

    KF_LOG_INFO(logger, "[get_account] (url) " << url << " (response.status_code) " << response.status_code <<
                                                      " (response.error.message) " << response.error.message <<
                                                      " (response.text) " << response.text.c_str());
    lck.unlock();

    if(response.status_code == HTTP_GATEWAY_TIMEOUT){
        int errorId = 504;
        std::string errorMsg = response.text;
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());  
    }

    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}

int TDEngineBinance::get_margin_account(AccountUnitBinance& unit, Document &json, LFRspPositionField pos,int requestId)
{
    KF_LOG_INFO(logger, "[get_margin_ account]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    std::string requestPath =restBaseUrl + "/sapi/v1/margin/account?";
    std::string queryString("");
    std::string body = "";
   
    queryString.append("timestamp=");
    queryString.append( Timestamp );

    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( std::to_string( recvWindow ) );
    }
    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    string url = requestPath + queryString;
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}},
                              Body{body}, Timeout{100000});

    KF_LOG_INFO(logger, "[get_margin_account] (url) " << url << " (response.status_code) " << response.status_code <<
                                                      " (response.error.message) " << response.error.message <<
                                                      " (response.text) " << response.text.c_str());
    lck.unlock();

    if(response.status_code == HTTP_GATEWAY_TIMEOUT){
        int errorId = 504;
        std::string errorMsg = response.text;
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());  
    }

    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}

int TDEngineBinance::get_sub_account(string email, AccountUnitBinance& unit, Document &json, LFRspPositionField pos,int requestId)
{
    KF_LOG_INFO(logger, "[get_sub_account]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    // wapi -> sapi
    //std::string requestPath =restBaseUrl + "/wapi/v3/sub-account/assets.html?";
    std::string requestPath =restBaseUrl + "/sapi/v3/sub-account/assets?";
    //
    std::string queryString("");
    std::string body = "";

    queryString.append( "email=" );
    queryString.append( email );


    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( std::to_string( recvWindow ) );
    }

    queryString.append("&timestamp=");
    queryString.append( Timestamp );

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    string url = requestPath + queryString;
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}},
                              Body{body}, Timeout{100000});

    KF_LOG_INFO(logger, "[get_sub_account] (url) " << url << " (response.status_code) " << response.status_code <<
                                                      " (response.error.message) " << response.error.message <<
                                                      " (response.text) " << response.text.c_str());
    lck.unlock();

    if(response.status_code == HTTP_GATEWAY_TIMEOUT){
        int errorId = 504;
        std::string errorMsg = response.text;
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());  
    }

    getResponse(response.status_code, response.text, response.error.message, json);
    return response.status_code;
}
void TDEngineBinance::get_sub_margin_account(string email, AccountUnitBinance& unit, Document &json, LFRspPositionField pos,int requestId)
{
    KF_LOG_INFO(logger, "[get_sub_margin_account]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "GET";
    std::string requestPath =restBaseUrl + "/sapi/v1/sub-account/margin/account?";
    std::string queryString("");
    std::string body = "";

    queryString.append( "email=" );
    queryString.append( email );


    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( std::to_string( recvWindow ) );
    }

    queryString.append("&timestamp=");
    queryString.append( Timestamp );

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    string url = requestPath + queryString;
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Get(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}},
                              Body{body}, Timeout{100000});

    KF_LOG_INFO(logger, "[get_sub_margin_account] (url) " << url << " (response.status_code) " << response.status_code <<
                                                      " (response.error.message) " << response.error.message <<
                                                      " (response.text) " << response.text.c_str());
    if(response.status_code == HTTP_GATEWAY_TIMEOUT){
        int errorId = 504;
        std::string errorMsg = response.text;
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());  
    }
    return getResponse(response.status_code, response.text, response.error.message, json);
}
void TDEngineBinance::get_listen_key(AccountUnitBinance& unit, Document &json)
{
    KF_LOG_INFO(logger, "[get_listen_key]" << std::this_thread::get_id());
    std::string Timestamp = getTimestampString();
    std::string Method = "POST";
    std::string requestPath = restBaseUrl +"/api/v3/userDataStream";
    std::string queryString("");
    std::string body = "";


    /*if (order_count_over_limit(unit))
    {
        //send err msg to strategy
        std::string strErr = "{\"code\":-1429,\"msg\":\"order count over 100000 limit.\"}";
        json.Parse(strErr.c_str());
        return;
    }
    */
    if (unit.bHandle_429)
    {
        if (isHandling(unit))
        {
            std::string strErr = "{\"code\":-1429,\"msg\":\"handle 429, prohibit send order.\"}";
            json.Parse(strErr.c_str());
            return;
        }
    }
    std::string interface;
    if (m_interface_switch > 0) {
        //   std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
        interface = m_interfaceMgr.getActiveInterface();
        //   lck.unlock()
        KF_LOG_INFO(logger, "[send_order] interface: [" << interface << "].");
        if (interface.empty()) {
            KF_LOG_INFO(logger, "[send_order] interface is empty, decline message sending!");
            std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
            json.Parse(strRefused.c_str());
            return;
        }
    }
     handle_request_weight(unit,interface,GetListenKey_Type);

    string url = requestPath + queryString;
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Post(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}},
                              Body{body}, Timeout{100000},Interface{interface});

    KF_LOG_INFO(logger, "[get_listen_key] (url) " << url << " (response.status_code) " << response.status_code <<
                                                   " (response.error.message) " << response.error.message <<
                                                   " (response.text) " << response.text.c_str());
    return getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineBinance::put_listen_key(AccountUnitBinance& unit, Document &json)
{
    KF_LOG_INFO(logger, "[put_listen_key]");
    std::string Timestamp = getTimestampString();
    std::string Method = "PUT";
    std::string requestPath = restBaseUrl +"/api/v3/userDataStream";
    std::string queryString("?listenKey="+unit.listenKey);
    std::string body ="{ \"listenKey\":"+ unit.listenKey + "}";


    /*if (order_count_over_limit(unit))
    {
        //send err msg to strategy
        std::string strErr = "{\"code\":-1429,\"msg\":\"order count over 100000 limit.\"}";
        json.Parse(strErr.c_str());
        return;
    }
    */
    if (unit.bHandle_429)
    {
        if (isHandling(unit))
        {
            std::string strErr = "{\"code\":-1429,\"msg\":\"handle 429, prohibit send order.\"}";
            json.Parse(strErr.c_str());
            return;
        }
    }

    std::string interface;
    if (m_interface_switch > 0) {
        //   std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
        interface = m_interfaceMgr.getActiveInterface();
        //   lck.unlock()
        KF_LOG_INFO(logger, "[send_order] interface: [" << interface << "].");
        if (interface.empty()) {
            KF_LOG_INFO(logger, "[send_order] interface is empty, decline message sending!");
            std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
            json.Parse(strRefused.c_str());
            return;
        }
    }
    
    handle_request_weight(unit,interface,PutListenKey_Type);

    


    string url = requestPath + queryString;
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Put(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}},
                              Timeout{100000},Interface{interface});

    KF_LOG_INFO(logger, "[put_listen_key] (url) " << url << " (response.status_code) " << response.status_code <<
                                                   " (response.error.message) " << response.error.message <<
                                                   " (response.text) " << response.text.c_str());
    return getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineBinance::get_margin_listen_key(AccountUnitBinance& unit, Document &json)
{
    KF_LOG_INFO(logger, "[get_margin_listen_key]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "POST";
    std::string requestPath = restBaseUrl +"/sapi/v1/userDataStream?";
    std::string queryString("");
    std::string body = "";

    bool flag=false;

    if ( recvWindow > 0 ) {
        queryString.append("recvWindow=");
        queryString.append( std::to_string( recvWindow ) );
        flag=true;
    }

    if(flag)
        queryString.append("&timestamp=");
    else
        queryString.append("timestamp=");
    queryString.append( Timestamp );    

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );
    /*
    if (order_count_over_limit(unit))
    {
        //send err msg to strategy
        std::string strErr = "{\"code\":-1429,\"msg\":\"order count over 100000 limit.\"}";
        json.Parse(strErr.c_str());
        return;
    }
    */
    if (unit.bHandle_429)
    {
        if (isHandling(unit))
        {
            std::string strErr = "{\"code\":-1429,\"msg\":\"handle 429, prohibit send order.\"}";
            json.Parse(strErr.c_str());
            return;
        }
    }

    std::string interface;
    if (m_interface_switch > 0) {
        //   std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
        interface = m_interfaceMgr.getActiveInterface();
        //   lck.unlock()
        KF_LOG_INFO(logger, "[send_order] interface: [" << interface << "].");
        if (interface.empty()) {
            KF_LOG_INFO(logger, "[send_order] interface is empty, decline message sending!");
            std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
            json.Parse(strRefused.c_str());
            return;
        }
    }
    handle_request_weight(unit,interface,GetListenKey_Type);

    string url = requestPath + queryString;
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Post(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}},
                              Body{body}, Timeout{100000},Interface{interface});

    KF_LOG_INFO(logger, "[get_margin_listen_key] (url) " << url << " (response.status_code) " << response.status_code <<
                                                   " (response.error.message) " << response.error.message <<
                                                   " (response.text) " << response.text.c_str());
    return getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineBinance::put_margin_listen_key(AccountUnitBinance& unit, Document &json)
{
    KF_LOG_INFO(logger, "[put_margin_listen_key]");
    long recvWindow = order_insert_recvwindow_ms;//5000;
    std::string Timestamp = getTimestampString();
    std::string Method = "PUT";
    std::string requestPath = restBaseUrl +"/sapi/v1/userDataStream";
    std::string queryString("?listenKey="+unit.listenKey);
    std::string body ="{ \"listenKey\":"+ unit.listenKey + "}";

    if ( recvWindow > 0 ) {
        queryString.append("&recvWindow=");
        queryString.append( std::to_string( recvWindow ) );
    }

    queryString.append("&timestamp=");
    queryString.append( Timestamp );

    std::string signature =  hmac_sha256( unit.secret_key.c_str(), queryString.c_str() );
    queryString.append( "&signature=");
    queryString.append( signature );

    /*
    if (order_count_over_limit(unit))
    {
        //send err msg to strategy
        std::string strErr = "{\"code\":-1429,\"msg\":\"order count over 100000 limit.\"}";
        json.Parse(strErr.c_str());
        return;
    }
    */
    if (unit.bHandle_429)
    {
        if (isHandling(unit))
        {
            std::string strErr = "{\"code\":-1429,\"msg\":\"handle 429, prohibit send order.\"}";
            json.Parse(strErr.c_str());
            return;
        }
    }

        std::string interface;
    if (m_interface_switch > 0) {
        //   std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);
        interface = m_interfaceMgr.getActiveInterface();
        //   lck.unlock()
        KF_LOG_INFO(logger, "[send_order] interface: [" << interface << "].");
        if (interface.empty()) {
            KF_LOG_INFO(logger, "[send_order] interface is empty, decline message sending!");
            std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
            json.Parse(strRefused.c_str());
            return;
        }

    }
    handle_request_weight(unit,interface,PutListenKey_Type);


    string url = requestPath + queryString;
    std::unique_lock<std::mutex> lck(http_mutex);
    const auto response = Put(Url{url},
                              Header{{"X-MBX-APIKEY", unit.api_key}},
                              Timeout{100000},Interface{interface});

    KF_LOG_INFO(logger, "[put_margin_listen_key] (url) " << url << " (response.status_code) " << response.status_code <<
                                                   " (response.error.message) " << response.error.message <<
                                                   " (response.text) " << response.text.c_str());
    return getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineBinance::printResponse(const Document& d)
{
    if(d.IsObject() && d.HasMember("code") && d.HasMember("msg")) {
        KF_LOG_INFO(logger, "[printResponse] error (code) " << d["code"].GetInt() << " (msg) " << d["msg"].GetString());
    } else {
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        d.Accept(writer);
        KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
    }
}

void TDEngineBinance::getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json)
{
    json.Parse(responseText.c_str());
}

int64_t TDEngineBinance::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

//m -> 分钟; h -> 小时; d -> 天; w -> 周; M -> 月
//1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M
int64_t TDEngineBinance::get_millsecond_interval(string interval)
{
    int res;
    if (interval.find("m") != -1)
        res = std::stoi(interval.substr(0, interval.length() - 1)) * 60 * 1000;
    else if (interval.find("h") != -1)
        res = std::stoi(interval.substr(0, interval.length() - 1)) * 60 * 60 * 1000;
    else if (interval.find("d") != -1)
        res = std::stoi(interval.substr(0, interval.length() - 1)) * 24 * 60 * 60 * 1000;
    else if (interval.find("w") != -1)
        res = std::stoi(interval.substr(0, interval.length() - 1)) * 7 * 24 * 60 * 60 * 1000;
    else if (interval.find("M") != -1)
        res = std::stoi(interval.substr(0, interval.length() - 1)) * 30 * 24 * 60 * 60 * 1000;
    else
        res = 0;
    return res;
}

std::string TDEngineBinance::getTimestampString()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    KF_LOG_DEBUG(logger, "[getTimestampString] (timestamp)" << timestamp << " (timeDiffOfExchange)" << timeDiffOfExchange << " (exchange_shift_ms)" << exchange_shift_ms);
    timestamp =  timestamp + timeDiffOfExchange + exchange_shift_ms;
    KF_LOG_INFO(logger, "[getTimestampString] (new timestamp)" << timestamp);
    std::string timestampStr;
    std::stringstream convertStream;
    convertStream << timestamp;
    convertStream >> timestampStr;
    return timestampStr;
}


int64_t TDEngineBinance::getTimeDiffOfExchange(AccountUnitBinance& unit)
{
    KF_LOG_INFO(logger, "[getTimeDiffOfExchange] " << std::this_thread::get_id());
    //reset to 0
    Document d;
    int64_t start_time = getTimestamp();
    get_exchange_time(unit, d);
    if(!d.HasParseError() && d.HasMember("serverTime"))
    {//binance serverTime
        int64_t exchangeTime = d["serverTime"].GetInt64();
        //KF_LOG_INFO(logger, "[getTimeDiffOfExchange] (i) " << i << " (exchangeTime) " << exchangeTime);
        int64_t finish_time = getTimestamp();
        timeDiffOfExchange = exchangeTime-(finish_time+start_time)/2;
    }
    return timeDiffOfExchange;
}

void TDEngineBinance::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "TDEngineBinance::on_lws_connection_error.");
    AccountUnitBinance& unit = findAccountUnitByWebsocketConn(conn);
    unit.is_connecting = false;
    std::lock_guard<std::mutex> lck(*unit.mutex_order_and_trade); 
    std::map<std::string, LFRtnOrderField>::iterator it;  
    long timeout_nsec = 0;
    unit.context=NULL;
    if(lws_login(unit, timeout_nsec))
    {
        it = unit.ordersMap.begin() ;
        while( it != unit.ordersMap.end() )
        {
            int errorId;string errorMsg;
            long remoteOrderId=atoi(it->first.c_str());
            LFRtnOrderField& data = it->second;
            Document json;
            std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data.InstrumentID));
            //撤销订单
            if( account_type == "margin")
            {
                cancel_margin_order(unit, ticker.c_str(), remoteOrderId, data.OrderRef, "", json);
            }
            else
            {
                cancel_order(unit, ticker.c_str(), remoteOrderId, data.OrderRef, "", json);
            }
            ++it ;
        }
        it = unit.ordersMap.begin();
        while(it != unit.ordersMap.end() )
        {
            int errorId;
            string errorMsg;
            long remoteOrderId = atoi(it->first.c_str());
            LFRtnOrderField& data = it->second;
            Document json;
            std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data.InstrumentID));
        
            LFOrderActionField data2;
            int requestId = data.RequestID;
            int response_code;
            std::unique_lock<std::mutex> lck1(cancel_mutex);
            auto itr1 = mapCancelOrder.find(data.OrderRef);
            if(itr1 != mapCancelOrder.end() ) //&& GetOrderStatus(status) != LF_CHAR_Canceled)
            {
                data2 = itr1->second.data;
                //KF_LOG_ERROR(logger, "[mapCancelOrder_error]:" << errorMsg << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
            }
            lck1.unlock();            
            //查询订单状态
            if( account_type == "margin")
            {
                response_code = get_margin_order(unit, ticker.c_str(), remoteOrderId, data.OrderRef, json, data2, requestId);
            }
            else
            {
                response_code = get_order(unit, ticker.c_str(), remoteOrderId, data.OrderRef, json, data2, requestId);
            }

            if(response_code == HTTP_GATEWAY_TIMEOUT){
               //on_rsp_order_action(&data2, requestId, errorId, errorMsg.c_str()); 
               KF_LOG_INFO(logger,"504 ++it");
               ++it;
            }

            if(json.HasParseError() )
            {
                errorId=100;
                errorMsg= "query_order http response has parse error. please check the log";
                KF_LOG_ERROR(logger, "[req_order_action] query_order error! (remoteOrderId)" << remoteOrderId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
            }
            else if(json.HasMember("status"))
            {
                string status = json["status"].GetString();
                data.OrderStatus=GetOrderStatus(status);
                on_rtn_order(&data);
                //std::unique_lock<std::mutex> lck1(cancel_mutex);
                lck1.lock();
                auto itr = mapCancelOrder.find(data.OrderRef);
                if(itr != mapCancelOrder.end() ) //&& GetOrderStatus(status) != LF_CHAR_Canceled)
                {
                    LFOrderActionField data1 = itr->second.data;
                    errorMsg = "Status is not canceled, error";
                    errorId = 110;
                    //int requestId = data.RequestID;
                    KF_LOG_ERROR(logger, "[mapCancelOrder_error]:" << errorMsg << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
                    on_rsp_order_action(&data1, requestId, errorId, errorMsg.c_str());
                }
                lck1.unlock();
            }
            else if(json.HasMember("code"))
            {
                int code = json["code"].GetInt();
                errorId = code;
                if (code < 200 || code > 299)
                {
                    errorMsg = json["msg"].GetString();
                    KF_LOG_ERROR(logger, " [get_order] query_order error! (remoteOrderId)" << remoteOrderId << "(errorId)" << errorId << " (errorMsg) " << errorMsg);
                }
                std::unique_lock<std::mutex> lck1(cancel_mutex);
                auto itr = mapCancelOrder.find(data.OrderRef);
                if(itr != mapCancelOrder.end())
                {
                    LFOrderActionField data1 = itr->second.data;
                    int requestId = data.RequestID;
                    on_rsp_order_action(&data1, requestId, errorId, errorMsg.c_str());
                    //mapCancelOrder.erase(itr);
                }
                lck1.unlock();
            }
            ++it;
            if(it == unit.ordersMap.end())
                break;
        }
        unit.ordersMap.clear();
        unit.wsOrderStatus.clear();
        mapCancelOrder.clear();
    }
    
}

int TDEngineBinance::lws_write_subscribe(struct lws* conn)
{
    return 0;
}

int TDEngineBinance::get_withdraw_status(int status)
{
    int local_status = LFTransferStatus::TRANSFER_STRATUS_SUCCESS;
    switch (status)
    {
    case 0:
    case 2:
    case 4:
        local_status = LFTransferStatus::TRANSFER_STRATUS_PROCESSING;
        break;
    case 1:
        local_status = LFTransferStatus::TRANSFER_STRATUS_CANCELED;
        break;
    case 3:
        local_status = LFTransferStatus::TRANSFER_STRATUS_REJECTED;
        break;
    case 5:
        local_status = LFTransferStatus::TRANSFER_STRATUS_FAILURE;
        break;
    case 6:
        local_status = LFTransferStatus::TRANSFER_STRATUS_SUCCESS;
        break;
    default:
        break;
    }
    return local_status;
}
int TDEngineBinance::get_deposit_status(int status)
{
    int local_status = LFTransferStatus::TRANSFER_STRATUS_FAILURE;
    switch (status)
    {
    case 0:
        local_status = LFTransferStatus::TRANSFER_STRATUS_PROCESSING;
        break;
    case 1:
        local_status = LFTransferStatus::TRANSFER_STRATUS_SUCCESS;
        break;
    default:
        break;
    }
    return local_status;
}
int TDEngineBinance::get_query_withdraw_status(int status)
{
    int qurery_status = 6;
    switch (status)
    {
    case LFTransferStatus::TRANSFER_STRATUS_PROCESSING:
        qurery_status = 4;
        break;
    case LFTransferStatus::TRANSFER_STRATUS_CANCELED:
        qurery_status = 1;
        break;
    case LFTransferStatus::TRANSFER_STRATUS_REJECTED:
        qurery_status = 3;
        break;
    case LFTransferStatus::TRANSFER_STRATUS_FAILURE:
        qurery_status = 5;
        break;
    case LFTransferStatus::TRANSFER_STRATUS_SUCCESS:
        qurery_status = 6;
        break;
    default:
        break;
    }
    return qurery_status;
}
int TDEngineBinance::get_query_deposit_status(int status)
{
    int qurery_status = 1;
    switch (status)
    {
    case LFTransferStatus::TRANSFER_STRATUS_PROCESSING:
        qurery_status = 0;
        break;
    case LFTransferStatus::TRANSFER_STRATUS_SUCCESS:
        qurery_status = 1;
        break;
    case LFTransferStatus::TRANSFER_STRATUS_FAILURE:
        qurery_status = 6;
    default:
        break;
    }
    return qurery_status;
}
bool TDEngineBinance::lws_login(AccountUnitBinance& unit, long timeout_nsec) {
    KF_LOG_INFO(logger, "TDEngineBinance::lws_login:");
    global_td = this;
    //
    unit.context = NULL;
    int errorId = 0;
    string errorMsg = "";
    Document json;
    if( account_type == "margin")
        get_margin_listen_key(unit, json);
    else
        get_listen_key(unit,json);
    if(json.HasParseError() )
    {
        errorId=100;
        if( account_type == "margin")
        {
            errorMsg= "get_margin_listen_key http response has parse error. please check the log";
            KF_LOG_ERROR(logger, "[lws_login] get_margin_listen_key error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
        else
        {
            errorMsg= "get_listen_key http response has parse error. please check the log";
            KF_LOG_ERROR(logger, "[lws_login] get_listen_key error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
    }
    else 
    {
        if(json.IsObject() && json.HasMember("listenKey"))
        {
            unit.listenKey = json["listenKey"].GetString();
        }
        else
        {
            errorId  = 101;
            errorMsg = "unknown error";
            if( account_type == "margin")
                KF_LOG_ERROR(logger, "[lws_login] get_marginaccount failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
            else
                KF_LOG_ERROR(logger, "[lws_login] get_account failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
        
    }
    if(errorId != 0) 
        return false;


    if (unit.context == NULL) {
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

        unit.context = lws_create_context( &info );
        KF_LOG_INFO(logger, "TDEngineBinance::lws_login: context created:"<< unit.api_key);
    }

    if (unit.context == NULL) {
        KF_LOG_ERROR(logger, "TDEngineBinance::lws_login: context of" << unit.api_key <<" is NULL. return");
        return false;
    }

    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
    lws_set_log_level(logs, NULL);

    struct lws_client_connect_info ccinfo = {0};

    std::string host  = wsBaseUrl;
    std::string path = "/ws/"+unit.listenKey;
    int port = 9443;

    ccinfo.context 	= unit.context;
    ccinfo.address 	= host.c_str();
    ccinfo.port 	= port;
    ccinfo.path 	= path.c_str();
    ccinfo.host 	= host.c_str();
    ccinfo.origin 	= host.c_str();
    ccinfo.ietf_version_or_minus_one = -1;
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

    unit.websocketConn = lws_client_connect_via_info(&ccinfo);
    KF_LOG_INFO(logger, "TDEngineBinance::lws_login: Connecting to " <<  ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

    if (unit.websocketConn == NULL) {
        KF_LOG_ERROR(logger, "TDEngineBinance::lws_login: wsi create error.");
        return false;
    }
    unit.last_put_time = getTimestamp();
    KF_LOG_INFO(logger, "TDEngineBinance::lws_login: wsi create success.");
    unit.is_connecting = true;
    return true;
}


void TDEngineBinance::on_lws_data(struct lws* conn, const char* data, size_t len) {// web sock
    AccountUnitBinance &unit = findAccountUnitByWebsocketConn(conn);
    KF_LOG_INFO(logger, "TDEngineBinance::on_lws_data: " << data);
    Document json;
    json.Parse(data,len);
    if (json.HasParseError() || !json.IsObject()) {
        KF_LOG_ERROR(logger, "TDEngineBinance::on_lws_data. parse json error: " << data);        
    }
	else if(json.HasMember("e"))
	{
		
        std::string eventType = json["e"].GetString();
        if(eventType == "executionReport")
        {
            KF_LOG_INFO(logger, "TDEngineBinance::on_lws_data. Order Update ");
            AccountUnitBinance& unit= findAccountUnitByWebsocketConn(conn);
            std::lock_guard<std::mutex> lck(*unit.mutex_order_and_trade);
            onOrder(unit,json);
        }
        else if(eventType == "outboundAccountInfo")
        {
            KF_LOG_INFO(logger, "TDEngineBinance::on_lws_data. Account Update ");
        }
        
	}
	
}



AccountUnitBinance& TDEngineBinance::findAccountUnitByWebsocketConn(struct lws * websocketConn)
{
    for (size_t idx = 0; idx < account_units.size(); idx++) {
        AccountUnitBinance &unit = account_units[idx];
        if(unit.websocketConn == websocketConn) {
            return unit;
        }
    }
    return account_units[0];
}

//cys add
std::string TDEngineBinance::parseJsonToString(Document &d){
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}
/*
 {"e":"executionReport","E":1560804132647,"s":"BTCPAX","c":"1","S":"SELL","o":"MARKET","f":"GTC","q":"0.49680700","p":"0.00000000",
 "P":"0.00000000","F":"0.00000000","g":-1,"C":"null","x":"TRADE","X":"PARTIALLY_FILLED","r":"NONE","i":44992393,"l":"0.02656900",
 "z":"0.02656900","L":"9268.56000000","n":"0.24625637","N":"PAX","T":1560804132640,"t":4718951,"I":94230823,"w":false,"m":false,
 "M":true,"O":1560804132640,"Z":"246.25637064","Y":"246.25637064"}
*/
void TDEngineBinance::onOrder(AccountUnitBinance& unit, Document& json) {
	KF_LOG_INFO(logger, "TDEngineBinance::onOrder");
    if (json.HasMember("c")&& json.HasMember("i") && json.HasMember("X")&& json.HasMember("l")&& json.HasMember("L")&& json.HasMember("z")&& json.HasMember("t")) {
		
		//std::lock_guard<std::mutex> lck(*unit.mutex_order_and_trade);		
		std::string remoteOrderId= std::to_string(json["i"].GetInt64());   
        std::string clientOrderId= json["c"].GetString();     
		auto it = unit.newordersMap.find(clientOrderId);
		if (it == unit.newordersMap.end())
		{ 
            it = unit.ordersMap.find(remoteOrderId);
            if (it == unit.ordersMap.end())
            {
                KF_LOG_ERROR(logger, "TDEngineBinance::onOrder,no order match:" << clientOrderId);
                return;
            }
		}
        else
        {
            it = unit.ordersMap.insert(std::make_pair(remoteOrderId,it->second)).first;
            unit.newordersMap.erase(clientOrderId);
        }
        
		LFRtnOrderField& rtn_order = it->second;
        char status = GetOrderStatus(json["X"].GetString());	

        if(status == LF_CHAR_NotTouched && rtn_order.OrderStatus == status)
        {
            KF_LOG_INFO(logger, "TDEngineBinance::onOrder,status is not changed");
            return;
        }
        char oldOrderStatus = rtn_order.OrderStatus;
        rtn_order.OrderStatus = status;
        std::string strTradeVolume = json["l"].GetString();
        uint64_t volumeTraded = std::round(std::stod(strTradeVolume)*scale_offset);
        uint64_t oldVolumeTraded = rtn_order.VolumeTraded;
		rtn_order.VolumeTraded += volumeTraded;
        rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTraded;
        KF_LOG_INFO(logger, "TDEngineBinance::[OnOrder] (request__ID)"<<rtn_order.RequestID<<" (old_status) "<<oldOrderStatus<<"(current_startus)"<<status);
        if(!(oldOrderStatus == status && status == LF_CHAR_Canceled))
        {//与req_order_action函数中返回的 canceled 互补，先收到交易所数据的先处理，后收到的不处理
            KF_LOG_INFO(logger, "TDEngineBinance::onOrder,rtn_order");
            on_rtn_order(&rtn_order);
            raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_ORDER_BINANCE,1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);
        }    
        LFRtnTradeField rtn_trade;
		memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
		strncpy(rtn_trade.OrderRef, rtn_order.OrderRef, 13);
		strcpy(rtn_trade.ExchangeID, rtn_order.ExchangeID);
		strncpy(rtn_trade.UserID, rtn_order.UserID, 16);
		strncpy(rtn_trade.InstrumentID, rtn_order.InstrumentID, 31);
		rtn_trade.Direction = rtn_order.Direction;	
		rtn_trade.Volume = volumeTraded;
        strcpy(rtn_trade.TradeID,rtn_order.BusinessUnit);
		std::string strTradePrice = json["L"].GetString();
        int64_t priceTraded = std::round(std::stod(strTradePrice)*scale_offset);
		rtn_trade.Price = priceTraded;
		
        if(oldVolumeTraded != rtn_order.VolumeTraded){
            KF_LOG_INFO(logger, "TDEngineBinance::onOrder,rtn_trade");
		    on_rtn_trade(&rtn_trade);
		    raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),source_id, MSG_TYPE_LF_RTN_TRADE_BINANCE, 1, -1);
        }
         //---------------成交总量+1------------------------
        //判断该orderref是否已经在map中，没有的话加入进去
        auto it_rate =unit.rate_limit_data_map.find(rtn_trade.InstrumentID);
        if ( it_rate != unit.rate_limit_data_map.end())
        {
            it_rate->second.trade_total+= volumeTraded;
        }
        //---------------成交总量---------------------------

        if (rtn_order.OrderStatus == LF_CHAR_AllTraded || rtn_order.OrderStatus == LF_CHAR_PartTradedNotQueueing ||
			rtn_order.OrderStatus == LF_CHAR_Canceled || rtn_order.OrderStatus == LF_CHAR_NoTradeNotQueueing || rtn_order.OrderStatus == LF_CHAR_Error)
		{
			unit.ordersMap.erase(it);

            std::unique_lock<std::mutex> lck2(account_mutex);
            auto it2 = mapInsertOrders.find(rtn_order.OrderRef);
            if(it2 != mapInsertOrders.end())
            {
                mapInsertOrders.erase(it2);
            }
            lck2.unlock();

            std::unique_lock<std::mutex> lck3(cancel_mutex);
            auto it3 = mapCancelOrder.find(rtn_order.OrderRef);
            if(it3 != mapCancelOrder.end() && rtn_order.OrderStatus == LF_CHAR_Canceled)
            {
                mapCancelOrder.erase(it3);
            }
            lck3.unlock();
		}       
    }

}



#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

BOOST_PYTHON_MODULE(libbinancetd)
{
    using namespace boost::python;
    class_<TDEngineBinance, boost::shared_ptr<TDEngineBinance> >("Engine")
    .def(init<>())
    .def("init", &TDEngineBinance::initialize)
    .def("start", &TDEngineBinance::start)
    .def("stop", &TDEngineBinance::stop)
    .def("logout", &TDEngineBinance::logout)
    .def("wait_for_stop", &TDEngineBinance::wait_for_stop);
}
