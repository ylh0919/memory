#include "TDEngineBitmex.h"
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
#include <algorithm>
#include "../../utils/crypto/openssl_util.h"
using cpr::Delete;
using cpr::DeleteAsync;
using cpr::Get;
using cpr::GetAsync;
using cpr::Url;
using cpr::Body;
using cpr::Header;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;
using cpr::PostAsync;
using cpr::Timeout;

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

USING_WC_NAMESPACE

int g_RequestGap=5*60;

static TDEngineBitmex* global_td = nullptr;
std::mutex account_mutex;
std::mutex g_reqMutex;
std::mutex unit_mutex;
std::mutex local_mutex;
std::mutex avgPx_mutex;
//td::mutex mutex_orderaction;
std::atomic<uint64_t> nIndex{0};

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
            std::cout << "3.1415926 LWS_CALLBACK_CLIENT_CLOSED, reason = " << reason << std::endl;
            if(global_td) {
                std::cout << "3.1415926 LWS_CALLBACK_CLIENT_CLOSED 2,  (call on_lws_connection_error)  reason = " << reason << std::endl;
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



TDEngineBitmex::TDEngineBitmex(): ITDEngine(SOURCE_BITMEX)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.Bitmex");
    KF_LOG_INFO(logger, "[TDEngineBitmex]");
  
}

TDEngineBitmex::~TDEngineBitmex()
{
    if(m_ThreadPoolPtr!=nullptr) delete m_ThreadPoolPtr;
}

AccountUnitBitmex ::AccountUnitBitmex()
{
    mutex_timemap = new std::mutex();
    mutex_ordermap= new std::mutex();
    mutex_orderaction = new std::mutex();
}
AccountUnitBitmex ::~AccountUnitBitmex()
{
    if(nullptr!=mutex_ordermap)
        delete mutex_ordermap;
    if(nullptr!=mutex_timemap)
        delete mutex_timemap;
    if(nullptr != mutex_orderaction)
        delete mutex_orderaction;
}

AccountUnitBitmex::AccountUnitBitmex(const AccountUnitBitmex &source)
{
    api_key=source.api_key;
    secret_key = source.secret_key;
    baseUrl = source.baseUrl;
    wsUrl= source.wsUrl;

    logged_in = source.logged_in;
    sendOrderFilters= source.sendOrderFilters;
    ordersMap = source.ordersMap;
    ordersInsertTimeMap = source.ordersInsertTimeMap;
    coinPairWhiteList = source.coinPairWhiteList;
    positionWhiteList = source.positionWhiteList;
    mutex_timemap = new std::mutex();
    mutex_ordermap = new std::mutex();
    mutex_orderaction = new std::mutex();
    websocketConn = nullptr;
    context = nullptr;
    wsStatus =0;
    maxRetryCount = 3;
}
void TDEngineBitmex::init()
{
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");


    std::time_t baseNow = std::time(nullptr);
    struct tm* tm = std::localtime(&baseNow);
    tm->tm_sec += 30;
    std::time_t next = std::mktime(tm);

    std::cout << "std::to_string(next):" << std::to_string(next)<< std::endl;

    std::cout << "getTimestamp:" << std::to_string(getTimestamp())<< std::endl;



}

void TDEngineBitmex::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineBitmex::resize_accounts(int account_num)
{
    //account_units.resize(account_num);
    //KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineBitmex::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    string interfaces;
	int interface_timeout = 300000;
	if(j_config.find("interfaces") != j_config.end()) {
		interfaces = j_config["interfaces"].get<string>();
	}
	KF_LOG_INFO(logger, "[load_account] interface switch: " << m_interface_switch);

	if(j_config.find("interface_timeout_ms") != j_config.end()) {
		interface_timeout = j_config["interface_timeout_ms"].get<int>();
	}
	KF_LOG_INFO(logger, "[load_account] interface switch: " << m_interface_switch);

	if(j_config.find("interface_switch") != j_config.end()) {
		m_interface_switch = j_config["interface_switch"].get<int>();
	}

	KF_LOG_INFO(logger, "[load_account] interface switch: " << m_interface_switch);
	if (m_interface_switch > 0) {
		m_interfaceMgr.init(interfaces, interface_timeout);
		m_interfaceMgr.print();
	}
	
    int thread_pool_size = 0;
    if(j_config.find("thread_pool_size") != j_config.end()) {
        thread_pool_size = j_config["thread_pool_size"].get<int>();
    }
    if(thread_pool_size > 0)
    {
        m_ThreadPoolPtr = new ThreadPool(thread_pool_size);
    }
    KF_LOG_INFO(logger, "[load_account] (thread_pool_size)" << thread_pool_size);
    string baseUrl = j_config["baseUrl"].get<string>();
    // internal load
    auto iter = j_config.find("users");
    if(iter != j_config.end()&& iter.value().size() >0)
    {
        int i=0;
        for (auto& j_account: iter.value())
        {
            AccountUnitBitmex unit;
            string api_key = j_account["APIKey"].get<string>();
           // KF_LOG_DEBUG(logger,"[load_account] api_key is "<<api_key);
            string secret_key = j_account["SecretKey"].get<string>();
            unit.api_key = api_key;
            unit.secret_key = secret_key;
            unit.baseUrl = "https://" +baseUrl;
            unit.wsUrl = baseUrl;  
            KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl);
            
            unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
            unit.coinPairWhiteList.Debug_print();

            unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
            unit.positionWhiteList.Debug_print();
            KF_LOG_INFO(logger, "[load_account] 0 !");
            //display usage:
            if(unit.coinPairWhiteList.Size() == 0) {
                KF_LOG_ERROR(logger, "TDEngineBitmex::load_account: please add whiteLists in kungfu.json like this :");
                KF_LOG_ERROR(logger, "\"whiteLists\":{");
                KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
                KF_LOG_ERROR(logger, "    \"btc_usdt\": \"BTC_USDT\",");
                KF_LOG_ERROR(logger, "     \"etc_eth\": \"ETC_ETH\"");
                KF_LOG_ERROR(logger, "},");
            }
            account_units.emplace_back(unit);
            Document d;
            cancel_all_orders(unit, d);
            KF_LOG_DEBUG(logger,""<<"api_key is"<<account_units[account_units.size()-1].api_key);
            i++;
        }
    }
    else
    {
         KF_LOG_ERROR(logger, "[load_account] no trade account info !");
    }
    base_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    base_interval_ms = std::max(base_interval_ms,(int64_t)500);

    if(j_config.find("no_response_wait_ms") != j_config.end()) {
        no_response_wait_ms = j_config["no_response_wait_ms"].get<int>();
        no_response_wait_ms = std::max(no_response_wait_ms,(int64_t)500);
    }

    if(j_config.find("current_td_index") != j_config.end()) {
        m_CurrentTDIndex = j_config["current_td_index"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (current_td_index)" << m_CurrentTDIndex);
    genUniqueKey();  
    KF_LOG_INFO(logger,"load1");
    // set up
    TradeAccount account = {};
    KF_LOG_INFO(logger,"load2");
    //partly copy this fields
    strncpy(account.UserID, account_units[0].api_key.c_str(), 16);
    KF_LOG_INFO(logger,"load3");
    strncpy(account.Password, account_units[0].secret_key.c_str(), 21);
    KF_LOG_INFO(logger,"load4");
    return account;
    KF_LOG_INFO(logger,"load5");
}


void TDEngineBitmex::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (int idx = 0; idx < account_units.size(); idx ++)
    {
        AccountUnitBitmex& unit = account_units[idx];
        KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
        if (!unit.logged_in)
        {
            //exchange infos
            Document doc;
            //TODO
            get_products(unit, doc);
            KF_LOG_INFO(logger, "[connect] get_products");
            printResponse(doc);

            if(loadExchangeOrderFilters(unit, doc))
            {
                unit.logged_in = true;
            } else {
                KF_LOG_ERROR(logger, "[connect] logged_in = false for loadExchangeOrderFilters return false");
            }
            debug_print(unit.sendOrderFilters);
            get_order(unit,getTimestampMS());
            unit.logged_in = lws_login(unit, 0);
            KF_LOG_DEBUG(logger,"[connect]  idx="<<idx <<"wsConn = "<<unit.websocketConn ) ;
        }
    }
}

size_t current_account_idx = -1;
AccountUnitBitmex& TDEngineBitmex::get_current_account()
{
    current_account_idx++;
    current_account_idx %= account_units.size();
    KF_LOG_DEBUG(logger,"[get_current_account] idx = "<<current_account_idx);
    return account_units[current_account_idx];
}
//TODO
bool TDEngineBitmex::loadExchangeOrderFilters(AccountUnitBitmex& unit, Document &doc) {
    KF_LOG_INFO(logger, "[loadExchangeOrderFilters]");
//    //parse bitmex json
    /*
//     [{"baseCurrency":"LTC","baseMaxSize":"100000.00","baseMinSize":"0.001","code":"LTC_BTC","quoteCurrency":"BTC","quoteIncrement":"8"},
//     {"baseCurrency":"BCH","baseMaxSize":"100000.00","baseMinSize":"0.001","code":"BCH_BTC","quoteCurrency":"BTC","quoteIncrement":"8"},
//     {"baseCurrency":"ETH","baseMaxSize":"100000.00","baseMinSize":"0.001","code":"ETH_BTC","quoteCurrency":"BTC","quoteIncrement":"8"},
//     {"baseCurrency":"ETC","baseMaxSize":"100000.00","baseMinSize":"0.01","code":"ETC_BTC","quoteCurrency":"BTC","quoteIncrement":"8"},
//     ...
     ]
//     * */
    if (doc.HasParseError() || doc.IsObject()) {
        return false;
    }
    if (doc.IsArray()) {
        int symbolsCount = doc.Size();
        for (SizeType i = 0; i < symbolsCount; i++) {
            const rapidjson::Value &sym = doc[i];
            if (sym.HasMember("symbol") && sym.HasMember("tickSize") && sym.HasMember("lotSize")) {
                std::string symbol = sym["symbol"].GetString();
                double tickSize = sym["tickSize"].GetDouble();
                double lotSize =0.00000001;
                if(sym["lotSize"].IsDouble())
                {
                    lotSize = sym["lotSize"].GetDouble();
                }
                else if(sym["lotSize"].IsInt())
                {
                    lotSize = sym["lotSize"].GetInt()*1.0;
                }
                KF_LOG_INFO(logger, "[loadExchangeOrderFilters] sendOrderFilters (symbol)" << symbol << " (tickSize)" << tickSize << " (lotSize)" << lotSize);
                //0.0000100; 0.001;  1; 10
                SendOrderFilter afilter;
                afilter.InstrumentID = symbol;
                afilter.ticksize = tickSize;
                afilter.lotsize = lotSize;
                unit.sendOrderFilters.insert(std::make_pair(symbol, afilter));
            }
        }
    }

    return true;
}

void TDEngineBitmex::debug_print(std::map<std::string, SendOrderFilter> &sendOrderFilters)
{
    std::map<std::string, SendOrderFilter>::iterator map_itr = sendOrderFilters.begin();
    while(map_itr != sendOrderFilters.end())
    {
        KF_LOG_DEBUG(logger, "[debug_print] sendOrderFilters (symbol)" << map_itr->first <<
                                                                      " (tickSize)" << map_itr->second.ticksize << " (lotSize)" <<  map_itr->second.lotsize);
        map_itr++;
    }
}

SendOrderFilter TDEngineBitmex::getSendOrderFilter(AccountUnitBitmex& unit, const std::string& symbol) {
    std::map<std::string, SendOrderFilter>::iterator map_itr = unit.sendOrderFilters.find(symbol);
    if (map_itr != unit.sendOrderFilters.end()) {
        return map_itr->second;
    }
    SendOrderFilter defaultFilter;
    defaultFilter.ticksize = 0.00000001;
    defaultFilter.lotsize = 0.00000001;
    defaultFilter.InstrumentID = "";
    return defaultFilter;
}

void TDEngineBitmex::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[login]");
    connect(timeout_nsec);
}

void TDEngineBitmex::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineBitmex::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineBitmex::is_logged_in() const
{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineBitmex::is_connected() const
{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}



std::string TDEngineBitmex::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "Buy";
    } else if (LF_CHAR_Sell == input) {
        return "Sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineBitmex::GetDirection(std::string input) {
    if ("Buy" == input) {
        return LF_CHAR_Buy;
    } else if ("Sell" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineBitmex::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "Limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "Market";
    } else {
        return "";
    }
}

LfOrderPriceTypeType TDEngineBitmex::GetPriceType(std::string input) {
    if ("Limit" == input) {
        return LF_CHAR_LimitPrice;
    } else if ("Market" == input) {
        return LF_CHAR_AnyPrice;
    } else {
        return '0';
    }
}
//订单状态，﻿open（未成交）、filled（已完成）、canceled（已撤销）、cancel（撤销中）、partially-filled（部分成交）
LfOrderStatusType TDEngineBitmex::GetOrderStatus(std::string input) {

    if("Pending New" == input){
        return LF_CHAR_Unknown;
    }
    else if("New" == input){
        return LF_CHAR_NotTouched;
    }
    else if ("PartiallyFilled" == input) {
        return LF_CHAR_PartTradedQueueing;
    } else if ("Filled" == input) {
        return LF_CHAR_AllTraded;
    } else if ("Canceled" == input) {
        return LF_CHAR_Canceled;
    } else if ("Rejected" == input) {
        return LF_CHAR_NoTradeNotQueueing;
    } else {
        return LF_CHAR_NotTouched;
    }
}

/**
 * req functions
 */
void TDEngineBitmex::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitBitmex& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key << " (InstrumentID) " << data->InstrumentID);

    int errorId = 0;
    std::string errorMsg = "";
    Document d;
    get_account(unit, d);

    if(d.IsObject() && d.HasMember("code"))
    {
        errorId = d["code"].GetInt();
        if(d.HasMember("message") && d["message"].IsString())
        {
            errorMsg = d["message"].GetString();
        }
        KF_LOG_ERROR(logger, "[req_investor_position] failed!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_BITMEX, 1, requestId);

    LFRspPositionField pos;
    memset(&pos, 0, sizeof(LFRspPositionField));
    strncpy(pos.BrokerID, data->BrokerID, 11);
    strncpy(pos.InvestorID, data->InvestorID, 19);
    strncpy(pos.InstrumentID, data->InstrumentID, 31);
    pos.PosiDirection = LF_CHAR_Long;
    pos.HedgeFlag = LF_CHAR_Speculation;
    pos.Position = 0;
    pos.YdPosition = 0;
    pos.PositionCost = 0;

    std::string ticker = unit.coinPairWhiteList.GetValueByKey(data->InstrumentID);
    KF_LOG_ERROR(logger,"actoin over:[req_investor_position]") ;
/*
 # Response
    [{"available":"0.099","balance":"0.099","currencyCode":"BTC","hold":"0","id":83906},{"available":"188","balance":"188","currencyCode":"MVP","hold":"0","id":83906}]
 * */
    bool findSymbolInResult = false;
    if(d.IsArray())
    {
        SizeType len = d.Size();
        KF_LOG_INFO(logger, "[req_investor_position] (asset.length)" << len);
        for(SizeType i = 0; i < len; i++)
        {
            std::string symbol = d[i]["symbol"].GetString();          
            if(symbol.length() > 0 && symbol == ticker) {
                //strncpy(pos.InstrumentID, ticker.c_str(), 31);
                pos.Position = std::round(d[i]["currentQty"].GetDouble() * scale_offset);
               
                //KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol
                //                                                         << " hold:" << d.GetArray()[i]["currentQty"].GetDouble()
                //                                                          << " balance: " << d.GetArray()[i]["currentCost"].GetDouble());
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
                on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
                findSymbolInResult = true;
            }
        }
    }

  
    //send the filtered position
    /*int position_count = tmp_vector.size();
    for (int i = 0; i < position_count; i++)
    {
        on_rsp_position(&tmp_vector[i], i == (position_count - 1), requestId, errorId, errorMsg.c_str());
        findSymbolInResult = true;
    }*/

    if(!findSymbolInResult)
    {
        KF_LOG_INFO(logger, "[req_investor_position] (!findSymbolInResult) (requestId)" << requestId);
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
    }
    if(errorId != 0)
    {
        raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_BITMEX, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineBitmex::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

int64_t TDEngineBitmex::fixPriceTickSize(double keepPrecision, int64_t price, bool isBuy) {


    int64_t tickSize = std::round((keepPrecision+0.000000001)* scale_offset);

    KF_LOG_INFO(logger, "[fixPriceTickSize input]" << "(price)" << price);
    int64_t count = price/tickSize;
    int64_t new_price = tickSize * count;
    if(isBuy){
        KF_LOG_INFO(logger, "[fixPriceTickSize output]" << "(price is buy)"  << new_price);
    } else {
        if(price%tickSize > 0)
        {
            new_price+=tickSize;
        }
        KF_LOG_INFO(logger, "[fixPriceTickSize output]" << "(price is sell)" << new_price);
    }
    return new_price;
}
int64_t TDEngineBitmex::fixVolumeLotSize(double keepPrecision, int64_t volume)
{
    int64_t tickSize = std::round((keepPrecision+0.000000001)* scale_offset);
    int64_t count = volume/tickSize;
    int64_t new_volume = tickSize * count;
     KF_LOG_INFO(logger, "[fixVolumeLotSize]" << " (input volume)" << volume << " (output volume)"  << new_volume);
    return new_volume;
}
int TDEngineBitmex::Round(std::string tickSizeStr) {
    size_t docAt = tickSizeStr.find( ".", 0 );
    size_t oneAt = tickSizeStr.find( "1", 0 );

    if(docAt == string::npos) {
        //not ".", it must be "1" or "10"..."100"
        return -1 * (tickSizeStr.length() -  1);
    }
    //there must exist 1 in the string.
    return oneAt - docAt;
}

bool TDEngineBitmex::ShouldRetry(const Document& json)
{
    //std::lock_guard<std::mutex> lck(unit_mutex);
    if(json.IsObject() && json.HasMember("code") && json["code"].IsNumber())
    {
        int code = json["code"].GetInt();
        if (code == 503 || code == 429)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(rest_get_interval_ms));
            return true;
        }
          
    }
    return false;
}
void TDEngineBitmex::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitBitmex& unit = get_current_account();
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITMEX, 1/*ISLAST*/, requestId);

    int errorId = 0;
    std::string errorMsg = "";
    on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_insert]: not in WhiteList, ignore it  (rid)" << requestId <<
                                                                                      " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITMEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);

    if(nullptr == m_ThreadPoolPtr)
    {
        send_order_thread(&unit,*data,ticker,requestId);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineBitmex::send_order_thread,this,&unit,*data,ticker,requestId));
        KF_LOG_DEBUG(logger, "[req_order_insert] [left thread count ]: ] "<< m_ThreadPoolPtr->idlCount());
    }
}
void TDEngineBitmex::send_order_thread(AccountUnitBitmex *unit, LFInputOrderField data, string ticker,int requestId)
{
    int errorId = 0;
    string errorMsg ="";
    double funds = 0;
    Document d;

    SendOrderFilter filter = getSendOrderFilter(*unit, ticker.c_str());

    int64_t fixedPrice = fixPriceTickSize(filter.ticksize, data.LimitPrice, LF_CHAR_Buy == data.Direction);
    int64_t fixedVolume = fixVolumeLotSize(filter.lotsize, data.Volume);
    KF_LOG_DEBUG(logger, "[req_order_insert] SendOrderFilter  (Tid)" << ticker <<
                                                                     " (LimitPrice)" << data.LimitPrice <<
                                                                     " (ticksize)" << filter.ticksize <<
                                                                     " (fixedPrice)" << fixedPrice <<
                                                                     " (fixedVolume)" << fixedVolume <<
                                                                     " (requestId)"<< requestId);
    std::string newClientId;
    if(!unit->is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        return;
    }else{
        newClientId = send_order(*unit, ticker.c_str(), &data, fixedVolume, fixedPrice, requestId, d, is_post_only(&data));
    }
    KF_LOG_DEBUG(logger,"[send_order_thread] newClientId"<<newClientId<<" (requestId)"<<(requestId));
    /*
     {"orderID":"18eb8aeb-3a29-b546-b2fe-1b55f24ef63f","clOrdID":"5","clOrdLinkID":"","account":272991,"symbol":"XBTUSD","side":"Buy",
     "simpleOrderQty":null,"orderQty":10,"price":1,"displayQty":null,"stopPx":null,"pegOffsetValue":null,"pegPriceType":"","currency":"USD",
     "settlCurrency":"XBt","ordType":"Limit","timeInForce":"GoodTillCancel","execInst":"","contingencyType":"",
     "exDestination":"XBME","ordStatus":"New","triggered":"","workingIndicator":true,"ordRejReason":"","simpleLeavesQty":null,
     "leavesQty":10,"simpleCumQty":null,"cumQty":0,"avgPx":null,"multiLegReportingType":"SingleSecurity","text":"Submitted via API.",
     "transactTime":"2018-11-18T13:18:33.598Z","timestamp":"2018-11-18T13:18:33.598Z"}
     */
    //not expected response
    if(d.HasParseError() || !d.IsObject())
    {
        errorId = 100;
        errorMsg = "send_order http response has parse error or is not json. please check the log";
        KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
                                                                           errorId << " (errorMsg) " << errorMsg);
    } else  if(d.HasMember("orderID"))
    {
            //if send successful and the exchange has received ok, then add to  pending query order list
            LocalOrderRef localref;
            localref.remoteOrderId = d["orderID"].GetString();
            localref.clordID = d["clOrdID"].GetString();
            KF_LOG_DEBUG(logger,"[send_order_thread] clOrderId is "<<localref.clordID<<" unit is"<< unit->api_key);

            std::unique_lock<std::mutex> lck_local(local_mutex) ;
            localOrderRefRemoteOrderId.insert(std::make_pair(std::string(data.OrderRef), localref));
            lck_local.unlock();
            KF_LOG_INFO(logger, "[req_order_insert] after send  (rid)" << requestId << " (OrderRef) " <<
                                                                       data.OrderRef << " (remoteOrderId) " << localref.remoteOrderId);
            
            std::unique_lock<std::mutex> lck_map(*unit->mutex_ordermap);  
            std::string OrderRef= localref.clordID;
            auto it = unit->ordersMap.find(OrderRef);
            if (it == unit->ordersMap.end())
            { 
                KF_LOG_ERROR(logger, "TDEngineBitmex::send_order_thread,no order match " << OrderRef);
                return;
            }
            LFRtnOrderField& rtn_order = it->second.data;
            if(rtn_order.OrderStatus == LF_CHAR_Unknown )
            {
                rtn_order.OrderStatus = LF_CHAR_NotTouched;
                on_rtn_order(&rtn_order);
            }
            //success, only record raw data
            raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITMEX, 1, requestId, errorId, errorMsg.c_str());
            std::unique_lock<std::mutex> lck_acc2(account_mutex);
            mapInsertOrders.insert(std::make_pair(data.OrderRef,unit));
            lck_acc2.unlock();
    }  else if (d.HasMember("code") && d["code"].IsNumber()) {
        //send error, example: http timeout.
        errorId = d["code"].GetInt();
        if(d.HasMember("message") && d["message"].IsString())
        {
            errorMsg = d["message"].GetString();
            //if(errorMsg.find("Duplicate") !=string::npos && errorMsg.find("clOrdID") != string::npos)
             //   errorId = 0;
        }
        KF_LOG_ERROR(logger, "[req_order_insert] failed!" << " (rid)" << requestId << " (errorId)" <<
                                                          errorId << " (errorMsg) " << errorMsg);
    }


    if(errorId != 0)
    {
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        std::unique_lock<std::mutex> lck(*(unit->mutex_ordermap));
        unit->ordersMap.erase(newClientId);
        unit->ordersInsertTimeMap.erase(newClientId);
        lck.unlock();
    }
    raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITMEX, 1, requestId, errorId, errorMsg.c_str());
}


void TDEngineBitmex::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    std::unique_lock<std::mutex> lck_account(account_mutex);
    auto it  = mapInsertOrders.find(data->OrderRef);
    int errorId = 0;
    std::string errorMsg = "";
    on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
    if(it == mapInsertOrders.end())
    {
        errorId = 200;
        errorMsg = std::string(data->OrderRef) + " is not found, ignore it";
        KF_LOG_ERROR(logger, errorMsg << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BINANCE, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    AccountUnitBitmex& unit = *(it->second);
    lck_account.unlock();
    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITMEX, 1, requestId);
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITMEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);
    std::unique_lock<std::mutex> lck(local_mutex);
    auto itr = localOrderRefRemoteOrderId.find(data->OrderRef);
    std::string remoteOrderId;
    std::string clordID;
    if(itr == localOrderRefRemoteOrderId.end()) {
        errorId = 1;
        std::stringstream ss;
        ss << "[req_order_action] not found in localOrderRefRemoteOrderId map (orderRef) " << data->OrderRef;
        errorMsg = ss.str();
        KF_LOG_ERROR(logger, "[req_order_action] not found in localOrderRefRemoteOrderId map. "
                             << " (rid)" << requestId << " (orderRef)" << data->OrderRef << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITMEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    } else {
        remoteOrderId = itr->second.remoteOrderId;
        clordID = itr->second.clordID;
        KF_LOG_DEBUG(logger, "[req_order_action] found in localOrderRefRemoteOrderId map (orderRef) "
                             << data->OrderRef << " (remoteOrderId) " << remoteOrderId);
    }
    lck.unlock();
    if(nullptr == m_ThreadPoolPtr)
    {
        action_order_thread(&unit,*data,ticker,requestId,remoteOrderId,clordID);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineBitmex::action_order_thread,this,&unit,*data,ticker,requestId,remoteOrderId,clordID));
    }
}
void TDEngineBitmex::action_order_thread(AccountUnitBitmex* unit,LFOrderActionField data,std::string ticker, int requestId,std::string remoteOrderId,std::string clordID)
{
    int errorId = 0;
    std::string errorMsg = "";
    Document d;
    if(!unit->is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        return;
    }else{
        cancel_order(*unit, remoteOrderId, d);
    }
    //int errorId = 0;
    //std::string errorMsg = "";
    //cancel order response "" as resultText, it cause json.HasParseError() == true, and json.IsObject() == false.
    //it is not an error, so dont check it.
    //not expected response
    if(d.IsObject() && !d.HasParseError() && d.HasMember("code") && d["code"].IsNumber()) {
        errorId = d["code"].GetInt();
        if(d.HasMember("message") && d["message"].IsString())
        {
            errorMsg = d["message"].GetString();
        }
        KF_LOG_ERROR(logger, "[req_order_action] cancel_order failed!" << " (rid)" << requestId
                                                                       << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }

   if(errorId != 0)
    {
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
    }
    else
    {
        addRemoteOrderIdOrderActionSentTime(*unit, &data, requestId,clordID);
    }
    raw_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITMEX, 1, requestId, errorId, errorMsg.c_str());
}

//zaf new add
void TDEngineBitmex::addRemoteOrderIdOrderActionSentTime(AccountUnitBitmex& unit,const LFOrderActionField* data, int requestId, std::string remoteOrderId)
{
    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;        
    newOrderActionSent.sentNameTime = getTimestampMS() ;      //记住时间
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    std::lock_guard<std::mutex> guard_mutex_order_action(*unit.mutex_orderaction);
    unit.remoteOrderIdOrderActionSentTime.insert(std::make_pair(remoteOrderId, newOrderActionSent));
}




void TDEngineBitmex::addNewQueryOrdersAndTrades(AccountUnitBitmex& unit, const LFInputOrderField* data, const LfOrderStatusType OrderStatus,
                                                const int64_t LimitPrice,const int64_t VolumeTotal,int reqID, std::string newClientId)
{
    //add new orderId for GetAndHandleOrderTradeResponse
    KF_LOG_INFO(logger, "[addNewQueryOrdersAndTrades] unit is "<<unit.api_key);
    OrderTime order;
    order.startime = getTimestampMS();
    memset(&order, 0, sizeof(LFRtnOrderField));
    order.data.OrderStatus = OrderStatus;
    order.data.VolumeTraded = 0;
    order.data.VolumeTotalOriginal = VolumeTotal;
    order.data.VolumeTotal = VolumeTotal;
    strncpy(order.data.OrderRef, data->OrderRef, 21);
    strncpy(order.data.InstrumentID, data->InstrumentID, 31);
    order.data.RequestID = reqID;
    strcpy(order.data.ExchangeID, "BitMEX");
    strncpy(order.data.UserID, unit.api_key.c_str(), 16);
    order.data.Direction = data->Direction;
    order.data.TimeCondition = data->TimeCondition;
    order.data.LimitPrice = LimitPrice;
    order.data.OrderPriceType = data->OrderPriceType;
    memcpy(&order.datal, data, sizeof(LFInputOrderField));
    InsertTime Insertorder;
    Insertorder.insert_time = getTimestampMS();
    std::unique_lock<std::mutex> olck(*unit.mutex_ordermap);
    unit.ordersMap.insert(std::make_pair(newClientId, order));
    olck.unlock();
    std::unique_lock<std::mutex> tlck(*unit.mutex_timemap);
    unit.ordersInsertTimeMap.insert(std::make_pair(newClientId,Insertorder));
    KF_LOG_INFO(logger, "[addNewQueryOrdersAndTrades] (InstrumentID) " << data->InstrumentID
                                                                       << " (OrderRef) " << data->OrderRef
                                                                       << "(VolumeTotal)" << VolumeTotal << " (clent order id)" << newClientId);
}


void TDEngineBitmex::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] ws_thread start on TDEngineBitmex::wsloop");
    ws_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBitmex::wsloop, this)));

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineBitmex::loop");
    //rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBitmex::loop, this)));

}


void TDEngineBitmex::wsloop()
{
    KF_LOG_INFO(logger, "[loop] (isRunning) " << isRunning);
    while(isRunning)
    {
        for(auto& unit:account_units)
        {
            lws_service( unit.context, base_interval_ms );
        }
        //std::cout << " 3.1415 loop() lws_service (n)" << n << std::endl;
    }
}


//zaf new add
void TDEngineBitmex::loop()
{
    KF_LOG_INFO(logger, "[loop] (isRunning) " << isRunning);
    int64_t mintime;
    while(isRunning)
    {
        for(auto& unit:account_units)
        {
            int64_t mintime = getTimestampMS();
            int64_t NowTime = getTimestampMS() ;
            std::unique_lock<std::mutex> lck(*unit.mutex_timemap) ;
            
            if(unit.ordersInsertTimeMap.size() != 0)
            {
                auto it = unit.ordersInsertTimeMap.begin() ;
                mintime = it->second.insert_time;
            }
            lck.unlock();

            std::unique_lock<std::mutex> lck1(*unit.mutex_orderaction) ;
            auto itr = unit.remoteOrderIdOrderActionSentTime.begin();
            for(itr = unit.remoteOrderIdOrderActionSentTime.begin(); itr != unit.remoteOrderIdOrderActionSentTime.end();itr++)
            {
                if(mintime >= itr -> second.sentNameTime)
                    mintime = itr -> second.sentNameTime;   
            }
            lck1.unlock();

            if(NowTime - mintime > no_response_wait_ms)
            {
        
                auto list_orders = get_order(unit,mintime);   //查询记录 

                std::unique_lock<std::mutex> lck2(*unit.mutex_orderaction) ;
                auto its = unit.remoteOrderIdOrderActionSentTime.begin();
                while( its != unit.remoteOrderIdOrderActionSentTime.end())
                {
                    /*if(its -> second.data.OrderPriceType == LF_CHAR_AnyPrice &&  NowTime - its->second.startime > no_response_wait_ms)
                    {    
                        lck.unlock();
                        lws_context_destroy(context);  //断联
                        lck.lock();
                        break;
                    }*/
                    if(its ->second.sentNameTime + no_response_wait_ms < NowTime)
                    {
                        std::unique_lock<std::mutex> lck_map(*unit.mutex_ordermap) ;
                        auto itr = unit.ordersMap.find(its -> first);
                        if(itr != unit.ordersMap.end())
                        {
                            int errorId=100;   
                            string errorMsg= " this cancel_order was unsuccessful,no response for a long time,query status is not canceled";
                            KF_LOG_ERROR(logger, "[req_order_action] cancel_order error! (remoteOrderId)" << its->second.data.InstrumentID << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
                            on_rsp_order_action(&its->second.data, its->second.requestId, errorId, errorMsg.c_str()); //给策略报错
                        }
                        lck_map.unlock();
                        its = unit.remoteOrderIdOrderActionSentTime.erase(its);
                    } 
                    else
                        ++its; 
                }
                lck2.unlock();
                lck.lock();
                for(auto it = unit.ordersInsertTimeMap.begin(); it != unit.ordersInsertTimeMap.end(); it++)
                {
                    if(it->second.flags == 3)
                    {
                        int errorId=100;
                        string errorMsg= " this insert_order was unsuccessful,no response for a long time";
                        std::unique_lock<std::mutex> lck_map(*unit.mutex_ordermap);
                        auto order = unit.ordersMap.find(it->first);
                        if(order != unit.ordersMap.end())
                        {
                            KF_LOG_ERROR(logger, "[req_order_action] cancel_order error! (remoteOrderId)" << order->second.data.InstrumentID << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
                            on_rsp_order_insert(&order->second.datal, order->second.data.RequestID, errorId, errorMsg.c_str());
                            unit.ordersMap.erase(order);
                        }
                        lck_map.unlock();
                        it = unit.ordersInsertTimeMap.erase(it);
                        std::unique_lock<std::mutex> lck3(*unit.mutex_orderaction) ;
                        unit.remoteOrderIdOrderActionSentTime.erase(it->first);
                        lck3.unlock();
                        continue;
                    }
                    if(NowTime - it->second.insert_time > no_response_wait_ms)
                    {
                        it->second.insert_time = getTimestampMS();
                        it->second.flags++;
                    }
                }
                lck.unlock();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));           
        }        
    }
}

std::vector<std::string> TDEngineBitmex::split(std::string str, std::string token)
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

void TDEngineBitmex::printResponse(const Document& d)
{
    if(d.IsObject() && d.HasMember("code")) {
        KF_LOG_INFO(logger, "[printResponse] error (code) " << d["code"].GetInt() << " (message) " << d["message"].GetString());
    } else {
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        d.Accept(writer);
        KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
    }
}

std::string TDEngineBitmex::getLwsAuthReq(AccountUnitBitmex& unit) {
    std::string expires = std::to_string(getTimestamp() + g_RequestGap);
    std::string message = "GET/realtime" + expires;
    std::string signature = hmac_sha256(unit.secret_key.c_str(), message.c_str());
    return "\"" + unit.api_key + "\"," + expires + ",\"" + signature + "\"";
}
std::string TDEngineBitmex::getLwsSubscribe(AccountUnitBitmex& unit) {
    return R"("order")";
}

void TDEngineBitmex::handleResponse(cpr::Response rsp, Document& json)
{
    auto& header = rsp.header;
    std::stringstream stream;
    for (auto& item : header)
    {
        stream << item.first << ':' << item.second << ',';
    }
    KF_LOG_INFO(logger, "[handleResponse] (header) " << stream.str());
    
    if(rsp.status_code == HTTP_RESPONSE_OK)
    {
        KF_LOG_INFO(logger, "[handleResponse] code is ok " );
        auto iter = header.find("x-ratelimit-remaining");
        if(iter != header.end())
        {
            m_limitRate_Remain = atoi(iter->second.c_str());
        }
        else
            m_limitRate_Remain =300;
        iter = header.find("x-ratelimit-reset");
        if(iter != header.end())
        {
            m_TimeStamp_Reset = atoll(iter->second.c_str());
            rest_get_interval_ms = (m_TimeStamp_Reset - getTimestamp())*1000;
            rest_get_interval_ms = std::max(rest_get_interval_ms,base_interval_ms);

        } 
        else
        {
            m_TimeStamp_Reset =getTimestamp();
            rest_get_interval_ms = 0;
        }
    } 
    else if(rsp.status_code == 429)
    {
        auto iter  = header.find("Retry-After");
        if(iter != header.end())
        {
            rest_get_interval_ms = atoll(iter->second.c_str());
        }
        else
        {
            rest_get_interval_ms = base_interval_ms;
        }
    }
    else if(rsp.status_code == 503)
    {
        rest_get_interval_ms =base_interval_ms;
    }
    else
    {
        rest_get_interval_ms = 0;
    }
    
    getResponse(rsp.status_code, rsp.text, rsp.error.message, json);
}

//an error:
/*
 * {"error": {
      "message": "...",
      "name": "HTTPError" | "ValidationError" | "WebsocketError" | "Error"
    }}
 * */
void TDEngineBitmex::getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json)
{
    if(http_status_code == HTTP_RESPONSE_OK)
    {
        //KF_LOG_INFO(logger, "[getResponse] (http_status_code == 200) (responseText)" << responseText << " (errorMsg) " << errorMsg);
        json.Parse(responseText.c_str());
        //KF_LOG_INFO(logger, "[getResponse] (http_status_code == 200) (HasParseError)" << json.HasParseError());
    } else if(http_status_code == 0 && responseText.length() == 0)
    {
        json.SetObject();
        Document::AllocatorType& allocator = json.GetAllocator();
        int errorId = 1;
        json.AddMember("code", errorId, allocator);
        //KF_LOG_INFO(logger, "[getResponse] (errorMsg)" << errorMsg);
        rapidjson::Value val;
        val.SetString(errorMsg.c_str(), errorMsg.length(), allocator);
        json.AddMember("message", val, allocator);
    } else
    {
        Document d;
        d.Parse(responseText.c_str());
        //KF_LOG_INFO(logger, "[getResponse] (err) (responseText)" << responseText.c_str());

        json.SetObject();
        Document::AllocatorType& allocator = json.GetAllocator();
        json.AddMember("code", http_status_code, allocator);
        if(!d.HasParseError() && d.IsObject()) {
            if(d.HasMember("error"))
            {
                auto& error = d["error"];
                if( error.HasMember("message")) 
                {
                    //KF_LOG_INFO(logger, "[getResponse] (err) (errorMsg)" << error["message"].GetString());
                    std::string message = error["message"].GetString();
                    rapidjson::Value val;
                    val.SetString(message.c_str(), message.length(), allocator);
                    json.AddMember("message", val, allocator);
                }
            }
            else if( d.HasMember("message")) {
                //KF_LOG_INFO(logger, "[getResponse] (err) (errorMsg)" << d["message"].GetString());
                std::string message = d["message"].GetString();
                rapidjson::Value val;
                val.SetString(message.c_str(), message.length(), allocator);
                json.AddMember("message", val, allocator);
            }
        } 
        else {
            rapidjson::Value val;
            val.SetString(errorMsg.c_str(), errorMsg.length(), allocator);
            json.AddMember("message", val, allocator);
        }
    }
}


void TDEngineBitmex::get_account(AccountUnitBitmex& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_account]");
    std::string Timestamp = std::to_string(getTimestamp()+g_RequestGap);
    std::string Method = "GET";
    std::string requestPath = "/api/v1/position";
    std::string queryString= "?count=100000";
    std::string body = "";
    string Message = Method + requestPath + queryString + Timestamp + body;

    std::string signature = hmac_sha256(unit.secret_key.c_str(), Message.c_str());
    string url = unit.baseUrl + requestPath + queryString;
    std::string interface;
    std::unique_lock<std::mutex> lck(g_reqMutex);
    const auto response = Get(Url{url},
                                 Header{{"api-key", unit.api_key},
                                        {"Content-Type", "application/json"},
                                        {"api-signature", signature},
                                        {"api-expires", Timestamp }},
                                 Timeout{30000});

    KF_LOG_INFO(logger, "[get_account] (url) " << url  << " (response.status_code) " << response.status_code <<
                                                " (response.error.message) " << response.error.message <<
                                                " (response.text) " << response.text.c_str());
    lck.unlock();
    return handleResponse(response,json);
}

void TDEngineBitmex::get_products(AccountUnitBitmex& unit, Document& json)
{
 /*
[
  {
    "symbol": "XBTZ14",
    "rootSymbol": "XBT",
    "state": "Settled",
    ...........
    "maxOrderQty": 10000000,
    "maxPrice": 1000000,
    "lotSize": 1,
    "tickSize": 0.01,
    "multiplier": 1000,
    .........
  },
  .......
  ]
  * */
    KF_LOG_INFO(logger, "[get_products]");
    std::string Timestamp = std::to_string(getTimestamp());
    std::string Method = "GET";
    std::string requestPath = "/api/v1/instrument/activeAndIndices";
    std::string queryString= "";
    std::string body = "";

    string url = unit.baseUrl + requestPath;
    std::unique_lock<std::mutex> lck(g_reqMutex);
    const auto response = Get(Url{url},
                              Header{
                                     {"Content-Type", "application/json"}},
                                     Timeout{10000} );

    KF_LOG_INFO(logger, "[get_products] (url) " << url  << " (response.status_code) " << response.status_code <<
                                                     " (response.error.message) " << response.error.message <<
                                                     " (response.text) " << response.text.c_str());
    lck.unlock();
    return handleResponse(response, json);
}

std::string time_to_iso_str(int64_t time)
{
    time/=1000;
    tm* gmt_time = gmtime(&time);
    //2019-06-21T03:03:04.130Z
    char buf[64];
    sprintf(buf,"%04d-%02d-%02dT%02d:%02d:%02d.000Z",gmt_time->tm_year+1900,gmt_time->tm_mon+1,gmt_time->tm_mday,gmt_time->tm_hour,gmt_time->tm_min,gmt_time->tm_sec);
    return buf;
}

std::vector<std::string> TDEngineBitmex::get_order(AccountUnitBitmex& unit,int64_t startTime)
{
    std::vector<std::string> listOrders;
    char buf[512];
    std::string strStartTime = time_to_iso_str(startTime);
    std::string strEndTime = time_to_iso_str(getTimestampMS()+2000);
    sprintf(buf,"?startTime=%s&endTime=%s",strStartTime.c_str(),strEndTime.c_str());
    
    std::string Timestamp = std::to_string(getTimestamp()+g_RequestGap);
    std::string Method = "GET";
    std::string requestPath = "/api/v1/order";
    std::string queryString= buf;
    std::string body = "";

    string Message = Method + requestPath + queryString + Timestamp + body;
    KF_LOG_INFO(logger, "[get_order] (Message)" << Message);

    std::string signature = hmac_sha256(unit.secret_key.c_str(), Message.c_str());
    string url = unit.baseUrl + requestPath + queryString;
    std::string interface;
    //std::unique_lock<std::mutex> lck1(g_reqMutex);
    auto ret = GetAsync(Url{url},
                               Header{{"api-key", unit.api_key},
                                      {"Content-Type", "application/json"},
                                      {"api-signature", signature},
                                      {"api-expires", Timestamp}}
                                       , Timeout{30000});
    auto response = ret.get();                            
    //lck1.unlock();

    //{ "error": {"message": "Authorization Required","name": "HTTPError"} }
    KF_LOG_INFO(logger, "[get_order] (url) " << url << " (response.status_code) " << response.status_code <<
        " (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str());
    Document d;
    KF_LOG_INFO(logger,"get_order.hand");
    handleResponse(response, d);
    if(!d.HasParseError() && d.IsArray() && d.Size() > 0)
    {
        for(int i =0;i < d.Size();++i)
        {
            auto& order = d.GetArray()[i];
            std::string closed_order = handle_order(unit,order);
            if(!closed_order.empty())
                listOrders.push_back(closed_order);
        }
    }  
    return listOrders;
}

//https://www.bitmex.com/api/explorer/#!/Order/Order_new
std::string TDEngineBitmex::send_order(AccountUnitBitmex& unit, const char *code,
                                    const LFInputOrderField* data, int64_t size, int64_t price, int reqID , Document& json, bool isPostOnly)
{
    KF_LOG_INFO(logger, "[send_order]");

    std::string priceStr;
    std::stringstream convertPriceStream;
    auto side = GetSide(data->Direction); 
    auto type = GetType(data->OrderPriceType);
    convertPriceStream <<std::fixed << std::setprecision(8) << price*1.0/scale_offset;
    convertPriceStream >> priceStr;

    std::string sizeStr;
    std::stringstream convertSizeStream;
    convertSizeStream <<std::fixed << std::setprecision(8) << size*1.0/scale_offset;
    convertSizeStream >> sizeStr;
    int retry_times = 0;
    bool should_retry = false;

    KF_LOG_INFO(logger, "[send_order] (code) " << code << " (side) "<< side.c_str() << " (type) " <<
                                               type.c_str() << " (size) "<< sizeStr << " (price) "<< priceStr);
    std::string newClientId = "";
    do{
        should_retry = false;
        newClientId = genClinetid(data->OrderRef);
        //std::unique_lock<std::mutex> lck(unit_mutex);
        addNewQueryOrdersAndTrades(unit, data, LF_CHAR_Unknown,price, size, reqID, newClientId);
        //lck.unlock();
        Document document;
        document.SetObject();

        Document::AllocatorType& allocator = document.GetAllocator();
        document.AddMember("symbol", StringRef(code), allocator);
        document.AddMember("side", StringRef(side.c_str()), allocator);
        document.AddMember("ordType", StringRef(type.c_str()), allocator);
        document.AddMember("orderQty", StringRef(sizeStr.c_str()), allocator);
        if(isPostOnly){
            document.AddMember("execInst", StringRef("ParticipateDoNotInitiate"), allocator);
        }
        if (strcmp(type.c_str(), "Limit") == 0)
        {
            document.AddMember("price", StringRef(priceStr.c_str()), allocator);
        }
        document.AddMember("clOrdID", StringRef(newClientId.c_str()), allocator);

        StringBuffer jsonStr;
        Writer<StringBuffer> writer(jsonStr);
        document.Accept(writer);

        std::string Timestamp = std::to_string(getTimestamp()+g_RequestGap);
        std::string Method = "POST";
        std::string requestPath = "/api/v1/order";
        std::string queryString= "";
        std::string body = jsonStr.GetString();

        string Message = Method + requestPath + queryString + Timestamp + body;
        KF_LOG_INFO(logger, "[send_order] (Message)" << Message);

        std::string signature = hmac_sha256(unit.secret_key.c_str(), Message.c_str());
        string url = unit.baseUrl + requestPath + queryString;
        std::string interface;
        if (m_interface_switch > 0) 
        {//read 
            //std::unique_lock<std::mutex> lck(mutex_m_switch_interfaceMgr);

            interface = m_interfaceMgr.getActiveInterface();
            // lck.unlock();
            KF_LOG_INFO(logger, "[send_margin_order] interface: [" << interface << "].");
            if (interface.empty()) {
                KF_LOG_INFO(logger, "[send_margin_order] interface is empty, decline message sending!");
                std::string strRefused = "{\"code\":-1430,\"msg\":\"interface is empty.\"}";
                json.Parse(strRefused.c_str());
                return "";
            }
        }
        //std::unique_lock<std::mutex> lck_http(g_reqMutex);
        auto ret = PostAsync(Url{url},
                                   Header{{"api-key", unit.api_key},
                                          {"Accept", "application/json"},
                                          {"Content-Type", "application/json"},
                                          {"Content-Length", to_string(body.size())},
                                          {"api-signature", signature},
                                          {"api-expires", Timestamp}},
                                   Body{body}, Timeout{30000});
        auto response = ret.get();
        KF_LOG_INFO(logger, "[send_order] (url) " << url << " (body) "<< body << " (response.status_code) " << response.status_code <<
                                                  " (response.error.message) " << response.error.message <<
                                                  " (response.text) " << response.text.c_str());
        //lck_http.unlock();
        handleResponse(response, json);
        if(ShouldRetry(json)) {
            should_retry = true;
            retry_times++;
            KF_LOG_INFO(logger,"[send_order] failed! unit is "<<unit.api_key<<" retry_times is "<<retry_times<<" reqID is "<<reqID);
            std::unique_lock<std::mutex> lck(*unit.mutex_ordermap);
            unit.ordersMap.erase(newClientId);
            lck.unlock();

            std::unique_lock<std::mutex> lck2(*unit.mutex_timemap);
            unit.ordersInsertTimeMap.erase(newClientId);
            lck2.unlock();
        }
    }while(should_retry && retry_times < unit.maxRetryCount);
    return newClientId;
}


void TDEngineBitmex::cancel_all_orders(AccountUnitBitmex& unit, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_all_orders]");
    std::string Timestamp = std::to_string(getTimestamp()+g_RequestGap);
    std::string Method = "DELETE";
    std::string requestPath = "/api/v1/order/all";
    std::string queryString= "";
    std::string body = "";

    string Message = Method + requestPath + queryString + Timestamp + body;

    std::string signature = hmac_sha256(unit.secret_key.c_str(), Message.c_str());
    string url = unit.baseUrl + requestPath;
    std::unique_lock<std::mutex> lck(g_reqMutex);
    const auto response = Delete(Url{url},
                                 Header{{"api-key", unit.api_key},
                                        {"Content-Type", "application/json"},
                                        {"api-signature", signature},
                                        {"api-expires", Timestamp }},
                                 Timeout{30000});

    KF_LOG_INFO(logger, "[cancel_all_orders] (url) " << url  << " (response.status_code) " << response.status_code <<
                                                     " (response.error.message) " << response.error.message <<
                                                     " (response.text) " << response.text.c_str());
    lck.unlock();
    return handleResponse(response, json);
}


void TDEngineBitmex::cancel_order(AccountUnitBitmex& unit, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_order]");
    std::string Timestamp = std::to_string(getTimestamp()+g_RequestGap);
    std::string Method = "DELETE";
    std::string requestPath = "/api/v1/order";
    std::string queryString= "?orderID="+orderId;
    std::string body = "";

    string Message = Method + requestPath + queryString + Timestamp + body;
    std::string signature = hmac_sha256(unit.secret_key.c_str(), Message.c_str());

    string url = unit.baseUrl + requestPath + queryString;

    /*
     *
     #
    # GET with complex querystring (value is URL-encoded)
    #
    verb = 'GET'
    # Note url-encoding on querystring - this is '/api/v1/instrument?filter={"symbol": "XBTM15"}'
    # Be sure to HMAC *exactly* what is sent on the wire
    path = '/api/v1/instrument?filter=%7B%22symbol%22%3A+%22XBTM15%22%7D'
    expires = 1518064237 # 2018-02-08T04:30:37Z
    data = ''
    # HEX(HMAC_SHA256(apiSecret, 'GET/api/v1/instrument?filter=%7B%22symbol%22%3A+%22XBTM15%22%7D1518064237'))
    # Result is:
    # 'e2f422547eecb5b3cb29ade2127e21b858b235b386bfa45e1c1756eb3383919f'
    signature = HEX(HMAC_SHA256(apiSecret, verb + path + str(expires) + data))
    #
     * */
    //std::unique_lock<std::mutex> lck(g_reqMutex);
    auto ret = DeleteAsync(Url{url},
                                 Header{{"api-key", unit.api_key},
                                        {"Content-Type", "application/json"},
                                        {"api-signature", signature},
                                        {"api-expires", Timestamp }},
                                 Timeout{30000});
    auto response = ret.get();
    KF_LOG_INFO(logger, "[cancel_order] (url) " << url  << " (body) "<< body << " (response.status_code) " << response.status_code <<
                                                " (response.error.message) " << response.error.message <<
                                                " (response.text) " << response.text.c_str());
    //lck.unlock();
    handleResponse(response, json);
    KF_LOG_INFO(logger, "[cancel_order] is over");

}



inline int64_t TDEngineBitmex::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

inline int64_t TDEngineBitmex::getTimestampMS()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}


void TDEngineBitmex::on_lws_connection_error(struct lws* conn)
{
    AccountUnitBitmex& unit = findAccountUnitByWebsocketConn(conn);
    unit.is_connecting = false;
    KF_LOG_ERROR(logger, "TDEngineBitmex::on_lws_connection_error.");
    sleep(100);
    //AccountUnitBitmex& unit = findAccountUnitByWebsocketConn(conn);
    std::lock_guard<std::mutex> lck(unit_mutex); //防止多个账户一起重连  
    int64_t mintime ;
    long timeout_nsec = 0;
    unit.context = NULL;
    if(lws_login(unit, timeout_nsec))
    {
        unit.logged_in = true;
        std::unique_lock<std::mutex> lck_map(*unit.mutex_ordermap);
        mintime = unit.ordersMap.begin()->second.startime;
        for(auto it=unit.ordersMap.begin();it!=unit.ordersMap.end();it++)
        {
            KF_LOG_ERROR(logger,"orderRef:"<<it->first);
            std::unique_lock<std::mutex> lck_local(local_mutex) ;
            auto local = localOrderRefRemoteOrderId.find(it->second.data.OrderRef);
            LFRtnOrderField data=it->second.data;
            Document json;
            if(local != localOrderRefRemoteOrderId.end())
            {
                cancel_order(unit, local->second.remoteOrderId, json);
                KF_LOG_ERROR(logger,"orderID:"<<local->second.remoteOrderId);
                KF_LOG_ERROR(logger,"orderRef:"<<local->second.clordID);
            }
            lck_local.unlock();
            if(mintime >= it -> second.startime)
                mintime = it -> second.startime;
        }
        lck_map.unlock();

        auto list_orders = get_order(unit,mintime); //查找
        std::unique_lock<std::mutex> lck1(*unit.mutex_orderaction) ;
        auto its = unit.remoteOrderIdOrderActionSentTime.begin();
        lck_map.lock();
        while( its != unit.remoteOrderIdOrderActionSentTime.end())
        {  
            auto itr = unit.ordersMap.find(its->first); //查找是否存在撤单的订单
            if(itr != unit.ordersMap.end())
            {
                int errorId;
                string errorMsg;
                errorId=100;
                errorMsg= "Websocket interrupt ! There was no response to the cancel_order for a long time.";
                KF_LOG_ERROR(logger, "[req_order_action] cancel_order error! (remoteOrderId)" << its->second.data.InstrumentID << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
                on_rsp_order_action(&its->second.data, its->second.requestId, errorId, errorMsg.c_str()); //给策略报错
                unit.ordersMap.erase(itr); //移除订单
            } 
            else
            {
                 ++its;
            }
            
        }
        lck_map.unlock();
        lck1.unlock();
        for(auto& order :unit.ordersMap)
        {
            int errorId;
            string errorMsg;
            string remoteOrderId=order.first;
            LFRtnOrderField data=order.second.data;
            errorId=100;
            errorMsg= "Websocket interrupt ! There was no response to the insert_order for a long time.";
            KF_LOG_ERROR(logger, "[on_lws_connection_error] insert_order error! (remoteOrderId)" << data.InstrumentID << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
            on_rsp_order_insert(&order.second.datal, data.RequestID, errorId, errorMsg.c_str());
        } 
        lck_map.unlock();
    }
    std::unique_lock<std::mutex> lck_map(*unit.mutex_ordermap);
    unit.ordersMap.clear();
    lck_map.unlock();

    unit.ordersInsertTimeMap.clear();
    unit.remoteOrderIdOrderActionSentTime.clear();
}

int TDEngineBitmex::lws_write_subscribe(struct lws* conn)
{
    //KF_LOG_INFO(logger,"TDEngineBitmex::lws_write_subscribe");
    auto& unit = findAccountUnitByWebsocketConn(conn);
    std::string reqMsg,args;
    if(unit.wsStatus == 0)
    {
        args = getLwsAuthReq(unit);
        reqMsg = "{\"op\": \"authKeyExpires\", \"args\": [" + args + "]}";
    }
    else if (unit.wsStatus == 1)
    {
        args = getLwsSubscribe(unit);
        reqMsg = "{\"op\": \"subscribe\", \"args\": [" + args + "]}";
    }
    else
    {
        return 0;
        args = getLwsSubscribe(unit);
        reqMsg = "{\"op\": \"unsubscribe\", \"args\": [" + args + "]}";
    }
    int length = reqMsg.length();
    unsigned char *msg  = new unsigned char[LWS_PRE+ length];
    memset(&msg[LWS_PRE], 0, length);
    KF_LOG_INFO(logger, "TDEngineBitfinex::lws_write_subscribe: " + reqMsg);
  

    strncpy((char *)msg+LWS_PRE, reqMsg.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

    if(unit.wsStatus == 0)
    {    //still has pending send data, emit a lws_callback_on_writable()
        lws_callback_on_writable( conn );
    }
    unit.wsStatus += 1;
    return ret;
}

bool TDEngineBitmex::lws_login(AccountUnitBitmex& unit, long timeout_nsec) {
    KF_LOG_INFO(logger, "TDEngineBitmex::lws_login:");
    global_td = this;

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
        KF_LOG_INFO(logger, "TDEngineBitmex::lws_login: context created.");
    }

    if (unit.context == NULL) {
        KF_LOG_ERROR(logger, "TDEngineBitmex::lws_login: context is NULL. return");
        return false;
    }

    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
    lws_set_log_level(logs, NULL);

    struct lws_client_connect_info ccinfo = {0};

    static std::string host  = unit.wsUrl;
    static std::string path = "/realtime";
    static int port = 443;

    ccinfo.context  = unit.context;
    ccinfo.address  = host.c_str();
    ccinfo.port     = port;
    ccinfo.path     = path.c_str();
    ccinfo.host     = host.c_str();
    ccinfo.origin   = host.c_str();
    ccinfo.ietf_version_or_minus_one = -1;
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

    unit.websocketConn = lws_client_connect_via_info(&ccinfo);
    KF_LOG_INFO(logger, "TDEngineBitmex::lws_login: Connecting to " <<  ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

    if (unit.websocketConn == NULL) {
        KF_LOG_ERROR(logger, "TDEngineBitmex::lws_login: wsi create error.");
        return false;
    }
    KF_LOG_INFO(logger, "TDEngineBitmex::lws_login: wsi create success.");
    unit.is_connecting = true;
    return true;
}


void TDEngineBitmex::on_lws_data(struct lws* conn, const char* data, size_t len) {
    AccountUnitBitmex &unit = findAccountUnitByWebsocketConn(conn);
    KF_LOG_INFO(logger, "TDEngineBitmex::on_lws_data: " << data);
    Document json;
    json.Parse(data,len);
    if (json.HasParseError() || !json.IsObject()) {
        KF_LOG_ERROR(logger, "TDEngineBitmex::on_lws_data. parse json error: " << data);        
    }
    else if (json.HasMember("error"))
    {
        KF_LOG_ERROR(logger, "TDEngineBitmex::on_lws_data. subscribe error: " << json["error"].GetString());
    }
    else if(json.HasMember("subscribe"))
    {
        KF_LOG_ERROR(logger, "TDEngineBitmex::on_lws_data. subscribe sucess ");
    }
    else if(json.HasMember("table"))
    {
        std::string  tablename = json["table"].GetString();
        if (tablename == "order")
        {
            onOrder(conn, json);
        }
    }
}



AccountUnitBitmex& TDEngineBitmex::findAccountUnitByWebsocketConn(struct lws * websocketConn)
{
    for (size_t idx = 0; idx < account_units.size(); idx++) {
        AccountUnitBitmex &unit = account_units[idx];
        if(unit.websocketConn == websocketConn) {
            KF_LOG_DEBUG(logger,"[findAccountUnitByWebsocketConn] idx="<<idx);
            return unit;
        }
    }
    return account_units[0];
}



std::string TDEngineBitmex::handle_order(AccountUnitBitmex& unit,Value& order)
{
    
    std::string closed_order = "";
    std::string OrderRef= order["clOrdID"].GetString();
    std::string orderId = order["orderID"].GetString();
    std::unique_lock<std::mutex> lck_map(*unit.mutex_ordermap);
    auto it = unit.ordersMap.find(OrderRef);
    KF_LOG_DEBUG(logger,"[handle_order] cOrderId is "<<OrderRef<<"unit is"<<unit.api_key);
    if (it == unit.ordersMap.end())
    { 
        KF_LOG_ERROR(logger, "TDEngineBitmex::onOrder,no order match " << OrderRef);
        return OrderRef;
    }
    lck_map.unlock();
    LFRtnOrderField& rtn_order = it->second.data;
    LFRtnTradeField rtn_trade ;

    memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
    strncpy(rtn_trade.OrderRef, it->second.data.OrderRef, 13);
    uint64_t volume ;
    double amount ;
    char status=LF_CHAR_NotTouched;
    if (order.HasMember("ordStatus"))
    {   
        status = GetOrderStatus(order["ordStatus"].GetString());                
    }
    if(status == LF_CHAR_NotTouched && rtn_order.OrderStatus == LF_CHAR_NotTouched)
    {
            //KF_LOG_INFO(logger, "TDEngineBitmex::onOrder,status is not changed");
        KF_LOG_INFO(logger, "TDEngineBitmex::handle_order,status is not changed");
        return closed_order;
    }
    else if(rtn_order.OrderStatus == LF_CHAR_Unknown && status != LF_CHAR_NotTouched)
    {
        rtn_order.OrderStatus = LF_CHAR_NotTouched;
        on_rtn_order(&rtn_order);
    }
    
    strcpy(rtn_trade.ExchangeID, "BitMEX");
    strncpy(rtn_trade.UserID, unit.api_key.c_str(), 16);
    strncpy(rtn_trade.InstrumentID, it->second.data.InstrumentID, 31);
    rtn_trade.Direction = it->second.data.Direction;

    rtn_order.OrderStatus = status;
    /*if (order.HasMember("leavesQty"))
        rtn_order.VolumeTotal = std::round(order["leavesQty"].GetDouble()*scale_offset); */  
    //if (order.HasMember("side"))
    //  rtn_order.Direction = GetDirection(order["side"].GetString());
    //if (order.HasMember("ordType"))
    //  rtn_order.OrderPriceType = GetPriceType(order["ordType"].GetString());
    //if (order.HasMember("orderQty"))
    //  rtn_order.VolumeTotalOriginal = int64_t(order["orderQty"].GetDouble()*scale_offset);
    if (order.HasMember("cumQty"))
    {
        rtn_trade.Volume = std::round(order["cumQty"].GetDouble()*scale_offset) - rtn_order.VolumeTraded ;
        rtn_order.VolumeTraded = std::round(order["cumQty"].GetDouble()*scale_offset) ;
        rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTraded;
    }

    if (order.HasMember("avgPx") && order["avgPx"].IsNumber())
    {
        double avgPx = order["avgPx"].GetDouble();
        std::unique_lock<std::mutex> lck4(avgPx_mutex);
        auto itr = avgPx_map.find(orderId);
        if(itr == avgPx_map.end()){
            avgPx_map.insert(make_pair(orderId,avgPx));
        }else{
            itr->second = avgPx;
        }
        lck4.unlock();

        amount = avgPx * rtn_order.VolumeTraded  - it->second.amount  ;
        KF_LOG_ERROR(logger, "onTrade old price:"<<it->second.amount) ;
        it->second.amount = avgPx * rtn_order.VolumeTraded  ;
        KF_LOG_ERROR(logger, "onTrade price:"<<it->second.amount) ;
        rtn_trade.Price = std::round((amount / rtn_trade.Volume) * scale_offset) ;
        KF_LOG_ERROR(logger, "onTrade new price:"<<rtn_trade.Price) ;
    }else{
        std::unique_lock<std::mutex> lck4(avgPx_mutex);
        auto itr = avgPx_map.find(orderId);
        if(itr != avgPx_map.end()){
            double avgPx = itr->second;
            amount = avgPx * rtn_order.VolumeTraded  - it->second.amount  ;
            KF_LOG_ERROR(logger, "onTrade old price:"<<it->second.amount) ;
            it->second.amount = avgPx * rtn_order.VolumeTraded  ;
            KF_LOG_ERROR(logger, "onTrade price:"<<it->second.amount) ;
            rtn_trade.Price = std::round((amount / rtn_trade.Volume) * scale_offset) ;
            KF_LOG_ERROR(logger, "onTrade new price:"<<rtn_trade.Price) ;            
        }
        lck4.unlock();
    }

    KF_LOG_INFO(logger, "TDEngineBitmex::onOrder,rtn_order");
    on_rtn_order(&rtn_order);
    raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_ORDER_BITMEX,1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);
    
    if(rtn_trade.Volume > 0)
    {
        KF_LOG_ERROR(logger, "TDEngineBitmex::onTrade,rtn_trade");
        on_rtn_trade(&rtn_trade);
        raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
            source_id, MSG_TYPE_LF_RTN_TRADE_BITMEX, 1, -1);
    }
   
    if (rtn_order.OrderStatus == LF_CHAR_AllTraded || rtn_order.OrderStatus == LF_CHAR_PartTradedNotQueueing ||
        rtn_order.OrderStatus == LF_CHAR_Canceled || rtn_order.OrderStatus == LF_CHAR_NoTradeNotQueueing || rtn_order.OrderStatus == LF_CHAR_Error)
    {
        lck_map.lock();
        unit.ordersMap.erase(it);
        lck_map.unlock();
        //unit.ordersInsertTimeMap.erase(OrderRef);
        std::unique_lock<std::mutex> lck_local(local_mutex) ;
        localOrderRefRemoteOrderId.erase(it->second.data.OrderRef);
        lck_local.unlock();
        closed_order = OrderRef;
        std::unique_lock<std::mutex> lck2(account_mutex);
        auto it2 = mapInsertOrders.find(OrderRef);
        if(it2 != mapInsertOrders.end())
        {
            mapInsertOrders.erase(it2);
        }
        lck2.unlock();
        std::unique_lock<std::mutex> lck4(avgPx_mutex);
        auto it3 = avgPx_map.find(orderId);
        if(it3 != avgPx_map.end()){
            KF_LOG_INFO(logger,"erase it3");
            avgPx_map.erase(it3);
        }
        lck4.unlock();
    }
    return closed_order;
}

void TDEngineBitmex::onOrder(struct lws* conn, Document& json) {
    KF_LOG_INFO(logger, "TDEngineBitmex::onOrder,"<< conn);

    if (json.HasMember("data") && json["data"].IsArray()) {
        AccountUnitBitmex &unit = findAccountUnitByWebsocketConn(conn);
        
        auto& arrayData = json["data"];
        for (SizeType index = 0; index < arrayData.Size(); ++index)
        {
            auto& order = arrayData[index]; 
            std::string OrderRef= order["clOrdID"].GetString(); 
            std::unique_lock<std::mutex> lck(*unit.mutex_timemap);
            auto it_time = unit.ordersInsertTimeMap.find(OrderRef);
            if(it_time != unit.ordersInsertTimeMap.end())
            {
                unit.ordersInsertTimeMap.erase(it_time);
            }   
            lck.unlock();
            std::string list_order = handle_order(unit,order);
            std::unique_lock<std::mutex> lck1(*unit.mutex_orderaction) ;
            unit.remoteOrderIdOrderActionSentTime.erase(list_order);
            lck1.unlock();
        }  
    }

}


std::string TDEngineBitmex::createAuthJsonString(AccountUnitBitmex& unit )
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("event");
    writer.String("signin");

    writer.Key("params");
    writer.StartObject();

    writer.Key("api_key");
    writer.String(unit.api_key.c_str());


    writer.EndObject();
    writer.EndObject();
    return s.GetString();
}

/*
 {
    event: "subscribe",
    params: {
        "type": "orders",
        "zip": false,
        "biz": "spot",
    }
}
 * */
std::string TDEngineBitmex::createOrderJsonString()
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("event");
    writer.String("subscribe");

    writer.Key("params");
    writer.StartObject();

    writer.Key("type");
    writer.String("orders");

    writer.Key("zip");
    writer.Bool(false);

    writer.Key("biz");
    writer.String("spot");

    writer.EndObject();
    writer.EndObject();
    return s.GetString();
}

void TDEngineBitmex::genUniqueKey()
{
    struct tm cur_time = getCurLocalTime();
    //SSMMHHDDN
    char key[11]{0};
    snprintf((char*)key, 11, "%02d%02d%02d%02d%02d", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_CurrentTDIndex);
    m_uniqueKey = key;
}

std::string TDEngineBitmex::genClinetid(const std::string &orderRef)
{
    //static int nIndex = 0;
    return m_uniqueKey + orderRef + std::to_string(nIndex++);
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

BOOST_PYTHON_MODULE(libbitmextd)
{
    using namespace boost::python;
    class_<TDEngineBitmex, boost::shared_ptr<TDEngineBitmex> >("Engine")
            .def(init<>())
            .def("init", &TDEngineBitmex::initialize)
            .def("start", &TDEngineBitmex::start)
            .def("stop", &TDEngineBitmex::stop)
            .def("logout", &TDEngineBitmex::logout)
            .def("wait_for_stop", &TDEngineBitmex::wait_for_stop);
}
