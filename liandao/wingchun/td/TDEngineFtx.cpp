#include "TDEngineFtx.h"
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
#include <mutex>
#include <chrono>
#include <time.h>
#include <math.h>
#include <zlib.h>
#include <string.h>
#include <queue>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/bio.h>
#include "../../utils/crypto/openssl_util.h"
using cpr::Post;
using cpr::Delete;
using cpr::Url;
using cpr::Body;
using cpr::Header;
using cpr::Parameters;
using cpr::Payload;
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
USING_WC_NAMESPACE
typedef char char_64[64];
std::mutex order_mutex;
std::mutex g_httpMutex;
TDEngineFtx::TDEngineFtx(): ITDEngine(SOURCE_FTX)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.Ftx");
    KF_LOG_INFO(logger, "[TDEngineFtx]");
}

TDEngineFtx::~TDEngineFtx()
{
    if(m_ThreadPoolPtr!=nullptr) delete m_ThreadPoolPtr;
    
}

static TDEngineFtx* global_md = nullptr;

std::string string_to_hex(unsigned char* data, std::size_t len)
{
    constexpr char hexmap[] = {'0','1','2','3','4','5','6', '7','8','9','a','b','c','d','e','f'};
    std::string s(len * 2, ' ');
    for (std::size_t i = 0; i < len; ++i) {
        s[2 * i] = hexmap[(data[i] & 0xF0) >> 4];
        s[2 * i + 1] = hexmap[data[i] & 0x0F];
    }
    return s;
}

std::string hmac_sha256(const std::string& secret,std::string msg)
{
    char signed_msg[64];
    HMAC_CTX ctx;
    HMAC_CTX_init(&ctx);
    HMAC_Init_ex(&ctx, secret.data(), (int)secret.size(), EVP_sha256(), nullptr);
    HMAC_Update(&ctx, (unsigned char*)msg.data(), msg.size());
    HMAC_Final(&ctx, (unsigned char*)signed_msg, nullptr);
    HMAC_CTX_cleanup(&ctx);
    std::string hmacced{signed_msg, 32};
    return string_to_hex((unsigned char*)hmacced.c_str(), 32);
}
int64_t formatISO8601_to_timestamp(std::string time)
{
    //extern long timezone;  
    int year,month,day,hour,min,sec;
    sscanf(time.c_str(),"%04d-%02d-%02dT%02d:%02d:%02dZ",&year,&month,&day,&hour,&min,&sec);
    tm utc_time{};
    utc_time.tm_year = year - 1900;
    utc_time.tm_mon = month -1;
    utc_time.tm_mday = day;
    utc_time.tm_hour = hour;
    utc_time.tm_min = min;
    utc_time.tm_sec = sec;
    time_t timet = mktime(&utc_time);
    tzset();
    return (timet-timezone);
}
int64_t formatISO8601_to_timestamp_ms(std::string time)
{
    //extern long timezone;  
    int year,month,day,hour,min,sec,ms;
    sscanf(time.c_str(),"%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",&year,&month,&day,&hour,&min,&sec,&ms);
    tm utc_time{};
    utc_time.tm_year = year - 1900;
    utc_time.tm_mon = month -1;
    utc_time.tm_mday = day;
    utc_time.tm_hour = hour;
    utc_time.tm_min = min;
    utc_time.tm_sec = sec;
    time_t timet = mktime(&utc_time);
    tzset();
    return (timet-timezone)*1000+ms;
}
bool TDEngineFtx::isClosed(char status)
{
    return status == LF_CHAR_AllTraded || status == LF_CHAR_Canceled;
}
int64_t timeDiff = 0;
void TDEngineFtx::getServerTime()
{
    std::unique_lock<std::mutex> lock(g_httpMutex);
    int64_t localTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto response = cpr::Get(Url{"https://otc.ftx.com/api/time"}, Header{},Timeout{30000});
    KF_LOG_INFO(logger, "[GET] (url) https://otc.ftx.com/api/time"  << " (response.status_code) " << response.status_code <<
                                        " (response.error.message) " << response.error.message <<
                                        " (response.text) " << response.text.c_str());
    lock.unlock();
    Document json;
    json.Parse(response.text.c_str());
    if(json.IsObject() && json.HasMember("success") && json["success"].GetBool() && json.HasMember("result"))
    {
        //2020-07-24T06:34:38.983246+00:00
        std::string strTime = json["result"].GetString();
        strTime = strTime.substr(0,23);
        strTime+="Z";
        int64_t time = formatISO8601_to_timestamp_ms(strTime);
        
        timeDiff = time - localTime;
        KF_LOG_INFO(logger,"local:" << localTime << ",server:" <<time << ",diff:" <<   timeDiff);
    }
}

cpr::Response TDEngineFtx::Get(const std::string& method_url, AccountUnitFtx& unit,std::string parameters)
{
    int64_t timestamp = getTimestamp();
    KF_LOG_INFO(logger,"timestamp="<<timestamp);
    string Message = std::to_string(timestamp) + "GET" + method_url /*+ parameters*/;

    KF_LOG_INFO(logger,"Message="<<Message);
    std::string signature = hmac_sha256(unit.secret_key, Message);

    KF_LOG_INFO(logger,"signature ="<<signature);
    
    string url = unit.baseUrl + method_url;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    auto response = cpr::Get(Url{url}, Header{{"FTX-KEY", unit.api_key}, 
                                        {"FTX-TS", std::to_string(timestamp)},
                                        {"FTX-SIGN", signature}},
                                        Timeout{30000});
    lock.unlock();
    //if(response.text.length()<500){
    KF_LOG_INFO(logger, "[GET] (url) " << url << " (response.status_code) " << response.status_code <<
                                        " (response.error.message) " << response.error.message <<
                                        " (response.text) " << response.text.c_str());
    //}
    return response;
}
cpr::Response TDEngineFtx::Delete(const std::string& method_url, AccountUnitFtx& unit,std::string parameters)
{
    int64_t timestamp = getTimestamp();
    KF_LOG_INFO(logger,"timestamp="<<timestamp);
    string Message = std::to_string(timestamp) + "DELETE" + method_url /*+ parameters*/;

    KF_LOG_INFO(logger,"Message="<<Message);
    std::string signature = hmac_sha256(unit.secret_key, Message);
    KF_LOG_INFO(logger,"signature ="<<signature);
    
    string url = unit.baseUrl + method_url;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    auto response = cpr::Delete(Url{url}, Header{{"FTX-KEY", unit.api_key}, 
                                        {"FTX-TS", std::to_string(timestamp)},
                                        {"FTX-SIGN", signature}},
                                        Timeout{30000});
    lock.unlock();
    //if(response.text.length()<500){
    KF_LOG_INFO(logger, "[DELETE] (url) " << url << " (response.status_code) " << response.status_code <<
                                        " (response.error.message) " << response.error.message <<
                                        " (response.text) " << response.text.c_str());
    //}
    return response;
}
cpr::Response TDEngineFtx::Post(const std::string& method_url,const std::string& body, AccountUnitFtx& unit)
{
    int64_t timestamp = getTimestamp();
    KF_LOG_INFO(logger,"timestamp="<<timestamp);
    string Message = std::to_string(timestamp) + "POST" +method_url+ body;
    KF_LOG_INFO(logger,"Message="<<Message);
    std::string signature =  hmac_sha256( unit.secret_key, Message);
    KF_LOG_INFO(logger,"signature="<<signature);
    
    string url = unit.baseUrl + method_url;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    Header headers{{"FTX-KEY", unit.api_key},
                                        {"FTX-TS", std::to_string(timestamp)},
                                        {"FTX-SIGN", signature},
                                        {"Content-Type", "application/json"}};
    auto response = cpr::Post(Url{url},headers ,Body{body}, Timeout{30000});
    lock.unlock();
    for(auto head : headers)
    {
        KF_LOG_INFO(logger,"key:"<<head.first << ",value:" << head.second << ";");
    }
    KF_LOG_INFO(logger, "[POST] (url) " << url <<" (body) "<< body<< " (response.status_code) " << response.status_code <<
                                        " (response.error.message) " << response.error.message <<
                                        " (response.text) " << response.text.c_str());
    return response;
}
void TDEngineFtx::init()
{
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineFtx::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineFtx::resize_accounts(int account_num)
{
    //account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineFtx::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    //string api_key = j_config["APIKey"].get<string>();
    //string secret_key = j_config["SecretKey"].get<string>();
    
    if(j_config.find("is_margin") != j_config.end()) {
        isMargin = j_config["is_margin"].get<bool>();
    }
    //https://api.Ftx.pro
    string baseUrl = j_config["baseUrl"].get<string>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

    if(j_config.find("max_rest_retry_times") != j_config.end()) {
        max_rest_retry_times = j_config["max_rest_retry_times"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (max_rest_retry_times)" << max_rest_retry_times);

    int thread_pool_size = 0;
    if(j_config.find("thread_pool_size") != j_config.end()) {
        thread_pool_size = j_config["thread_pool_size"].get<int>();
    }
    if(thread_pool_size > 0)
    {
        m_ThreadPoolPtr = new ThreadPool(thread_pool_size);
    }
    KF_LOG_INFO(logger, "[load_account] (thread_pool_size)" << thread_pool_size);
    if(j_config.find("retry_interval_milliseconds") != j_config.end()) {
        retry_interval_milliseconds = j_config["retry_interval_milliseconds"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (retry_interval_milliseconds)" << retry_interval_milliseconds);


    auto iter = j_config.find("users");
    if (iter != j_config.end() && iter.value().size() > 0)
    { 
        for (auto& j_account: iter.value())
        {
            AccountUnitFtx unit;

            unit.api_key = j_account["APIKey"].get<string>();
            unit.secret_key = j_account["SecretKey"].get<string>();
            unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
            //unit.coinPairWhiteList.Debug_print();
            unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
            //unit.positionWhiteList.Debug_print();
            unit.baseUrl = baseUrl;

            KF_LOG_INFO(logger, "[load_account] (api_key)" << unit.api_key << " (baseUrl)" << unit.baseUrl);
            if(unit.coinPairWhiteList.Size() == 0) {
                //display usage:
                KF_LOG_ERROR(logger, "TDEngineBinance::load_account: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
                KF_LOG_ERROR(logger, "\"whiteLists\":{");
                KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
                KF_LOG_ERROR(logger, "    \"btc_usdt\": \"BTCUSDT\",");
                KF_LOG_ERROR(logger, "     \"etc_eth\": \"ETCETH\"");
                KF_LOG_ERROR(logger, "},");
            }
            account_units.emplace_back(unit);
        }
    }
    getServerTime();
    // set up
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, account_units[0].api_key.c_str(), 16);
    strncpy(account.Password, account_units[0].secret_key.c_str(), 21);
    //web socket登陆
    //login(0);
    return account;
}

void TDEngineFtx::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitFtx& unit = account_units[idx];
        //unit.logged_in = true;
        KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
        if (!unit.logged_in)
        {
            unit.logged_in = true;
        }
    }
}

size_t current_account_idx = -1;
AccountUnitFtx& TDEngineFtx::get_current_account()
{
    current_account_idx++;
    current_account_idx %= account_units.size();
    return account_units[current_account_idx];
}

//{"op":"login","args":["<api_key>","<passphrase>","<timestamp>","<sign>"]}
void TDEngineFtx::ftxAuth(AccountUnitFtx& unit){
    KF_LOG_INFO(logger, "[FtxAuth] auth");

    int64_t now = getTimestamp();
    //std::string strTimestamp = "1557246346499";
    std::string strTimestamp = std::to_string(now);
    KF_LOG_INFO(logger,"strTimestamp="<<strTimestamp);

    //std::string method_url = "/users/self/verify";
    string Message = strTimestamp + "websocket_login";
    std::string signature =  hmac_sha256( unit.secret_key, Message );
    KF_LOG_INFO(logger,"signature ="<<signature);

    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("args");
    writer.StartObject();
    writer.Key("key");
    writer.String(unit.api_key.c_str()); 
    writer.Key("sign");
    writer.String(signature.c_str()); 
    writer.Key("time");
    writer.Int64(now);
    writer.EndObject();
    writer.Key("op");
    writer.String("login");
    writer.EndObject();
    std::string strSubscribe = sbUpdate.GetString();
    KF_LOG_INFO(logger, "[FtxAuth] auth success...");
}

void TDEngineFtx::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[TDEngineFtx::login]");
    connect(timeout_nsec);
}

void TDEngineFtx::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineFtx::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineFtx::is_logged_in() const{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineFtx::is_connected() const{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}


std::string TDEngineFtx::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "buy";
    } else if (LF_CHAR_Sell == input) {
        return "sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineFtx::GetDirection(std::string input) {
    if ("buy" == input ) {
        return LF_CHAR_Buy;
    } else if ("sell" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineFtx::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "market";
    } else {
        return "";
    }
}

LfOrderPriceTypeType TDEngineFtx::GetPriceType(std::string input) {
    if ("buy-limit" == input||"sell-limit" == input) {
        return LF_CHAR_LimitPrice;
    } else if ("buy-market" == input||"sell-market" == input) {
        return LF_CHAR_AnyPrice;
    } else {
        return '0';
    }
}
//订单状
LfOrderStatusType TDEngineFtx::GetOrderStatus(std::string state) {

    if(state == "cancelled")
    {
        return LF_CHAR_Canceled;
    }
    else if(state == "open")
    {
        return LF_CHAR_NotTouched;
    }
    else if(state == "filled")
    {
        return LF_CHAR_AllTraded;
    }
    return LF_CHAR_AllTraded;
}

/**
 * req functions
 * 查询账户持仓
 */
void TDEngineFtx::req_investor_position(const LFQryPositionField* data, int account_index, int requestId){
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitFtx& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key << " (InstrumentID) " << data->InstrumentID);
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_FTX, 1, requestId);
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

    int errorId = 0;
    std::string errorMsg = "";
    Document d;
    get_account(unit, d);
    KF_LOG_INFO(logger, "[req_investor_position] get_account");
    
    if(d.IsObject() && d.HasMember("error_message"))
    {
        //auto& error = d["error"];
        errorId =  std::stoi(d["error_code"].GetString());
        errorMsg = d["error_message"].GetString();
        KF_LOG_ERROR(logger, "[req_investor_position] failed!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    
    std::map<std::string,LFRspPositionField> tmp_map;
    if(!d.HasParseError() && d.HasMember("success") && d["success"].GetBool() && d.HasMember("result"))
    {
        size_t len = d["result"].Size();
        KF_LOG_INFO(logger, "[req_investor_position] (accounts.length)" << len);
        for(size_t i = 0; i < len; i++)
        {
            std::string symbol = d["result"].GetArray()[i]["coin"].GetString();
            KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol);
            std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
            KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (ticker) " << ticker);
            if(ticker.length() > 0) 
            {            
                uint64_t nPosition = std::round(d["result"].GetArray()[i]["free"].GetDouble() * scale_offset);   
                auto it = tmp_map.find(ticker);
                if(it == tmp_map.end())
                {
                    it = tmp_map.insert(std::make_pair(ticker,pos)).first;
                    strncpy(it->second.InstrumentID, ticker.c_str(), sizeof(it->second.InstrumentID));      
                }
                it->second.Position += nPosition;
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << it->second.Position);
            }
        }
    }

    //send the filtered position
    int position_count = tmp_map.size();
    if(position_count > 0) {
        for (auto it =  tmp_map.begin() ; it != tmp_map.end() ;  ++it) {
            --position_count;
            on_rsp_position(&it->second, position_count == 0, requestId, 0, errorMsg.c_str());
        }
    }
    else
    {
        KF_LOG_INFO(logger, "[req_investor_position] (!findSymbolInResult) (requestId)" << requestId);
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineFtx::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineFtx::dealPriceVolume(AccountUnitFtx& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,std::string& nDealPrice,std::string& nDealVolume,bool isbuy){
    KF_LOG_INFO(logger,"dealPriceVolume");
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(symbol);
    double price = nPrice * 1.0 / scale_offset;
    double volume = nVolume * 1.0 / scale_offset;
    auto it = precision_map.find(ticker);
    if(it != precision_map.end()){        
        if(isbuy){
            price = (floor(nPrice* 1.0/it->second.price_filter))*it->second.price_filter/scale_offset;
        }else{
            price = (ceil(nPrice* 1.0/it->second.price_filter))*it->second.price_filter/scale_offset;
        }
        volume = (floor(nVolume* 1.0/it->second.lot_size))*it->second.lot_size/scale_offset;
    }
    char strPrice[20]={};
    sprintf(strPrice,"%.4lf",price);
    char strVolume[20]={};
    sprintf(strVolume,"%.4lf",volume);
    nDealPrice = strPrice;
    nDealVolume = strVolume;
}
void get_option_info_from_instrument_id(const char* instrumentID,std::string& underlying,std::string& strike,std::string& type)
{
    char strUnderlying[20]={};
    char strType[20]={};
    char strStrike[20]={};
    double dStrike;
    sscanf(instrumentID,"%3s_%lf_%s",strUnderlying,&dStrike,strType);
    sprintf(strStrike,"%.0lf",dStrike);
    underlying = strUnderlying;
    type = strType;
    strike = strStrike;
}

//多线程发单
void TDEngineFtx::send_order_thread(AccountUnitFtx* unit,string ticker,const LFInputOrderField data,int requestId,int errorId,std::string errorMsg)
{
    KF_LOG_DEBUG(logger, "[send_order_thread] current thread is:" <<std::this_thread::get_id());
    bool isbuy;
    if(GetSide(data.Direction) == "buy"){
        isbuy = true;
    }else{
        isbuy = false;
    }
    Document d;
    std::string fixedPrice;
    std::string fixedVolume;
    dealPriceVolume(*unit,data.InstrumentID,data.LimitPrice,data.Volume,fixedPrice,fixedVolume,isbuy);
    KF_LOG_INFO(logger,"fixedPrice="<<fixedPrice<<" fixedVolume="<<fixedVolume);
    if(fixedVolume == "0"){
        KF_LOG_DEBUG(logger, "[req_order_insert] fixed Volume error of " << ticker);
        errorId = 200;
        errorMsg = data.InstrumentID;
        errorMsg += " : no such ticker";
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        return;
    }
    LFRtnOrderField rtn_order;
    memset(&rtn_order, 0, sizeof(LFRtnOrderField));
    rtn_order.OrderStatus = LF_CHAR_Unknown;
    rtn_order.VolumeTraded = 0;
    strcpy(rtn_order.ExchangeID, "ftx");
    strncpy(rtn_order.UserID, unit->api_key.c_str(), 16);
    strncpy(rtn_order.InstrumentID, data.InstrumentID, 31);
    rtn_order.Direction = data.Direction;
    rtn_order.RequestID = requestId;
    //No this setting on Ftx
    rtn_order.TimeCondition = data.TimeCondition;
    rtn_order.OrderPriceType = data.OrderPriceType;
    strncpy(rtn_order.OrderRef, data.OrderRef, sizeof(rtn_order.OrderRef));
    rtn_order.VolumeTotalOriginal = round(stod(fixedVolume) * scale_offset);
    rtn_order.LimitPrice = round(stod(fixedPrice) * scale_offset);
    rtn_order.VolumeTotal = round(stod(fixedVolume) * scale_offset);
    std::string underlying,strike,type;
    int64_t nExpiry = formatISO8601_to_timestamp(data.Expiry);
    int64_t nRequestExpiry = data.TimeCondition == LF_CHAR_GTD ? 0 : formatISO8601_to_timestamp(data.OrderExpiry);
    get_option_info_from_instrument_id(ticker.c_str(),underlying,strike,type);
    if(data.OrderPriceType == LF_CHAR_AnyPrice)
    {//市价单
        fixedPrice = "";
    }
    send_order(*unit, underlying,type,strike, GetSide(data.Direction),nExpiry,fixedPrice,fixedVolume,data.OrderPriceType == LF_CHAR_HideLimitPrice,nRequestExpiry,d);
    
    if(!d.IsObject()){
        errorId = 100;
        errorMsg = "send_order http response has parse error or is not json. please check the log";
        KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
                                                                           errorId << " (errorMsg) " << errorMsg);
    } 
    else
    {
        bool result = false;
        if(d.HasMember("success") && d["success"].GetBool() && d.HasMember("result"))
        {
            auto& result = d["result"];
            int64_t remoteOrderId = result["id"].GetInt64();
            std::unique_lock<std::mutex> lck(order_mutex);
            localOrderRefRemoteOrderId[std::string(data.OrderRef)] = remoteOrderId;
            KF_LOG_INFO(logger, "[req_order_insert] after send  (rid)" << requestId << " (OrderRef) " <<
                                                                           data.OrderRef << " (remoteOrderId) "
                                                                           << remoteOrderId);
            sprintf(rtn_order.BusinessUnit,"%lld",remoteOrderId);
            rtn_order.OrderStatus = LF_CHAR_NotTouched;
            on_rtn_order(&rtn_order);
            QuoteReuqest request;
            request.data = rtn_order;
            order_map.insert(make_pair(remoteOrderId, request));
            lck.unlock();
            return;
            
        }
        else
        {
            errorId = 100;
            errorMsg = d["error"].GetString();
        }
    }
    
    //unlock
    if(errorId != 0)
    {
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
    }


}

//发单
void TDEngineFtx::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time){

    AccountUnitFtx& unit = get_current_account();
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef
                                              << " (Expiry)" << data->Expiry);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_FTX, 1/*ISLAST*/, requestId);

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
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);
   
    if(nullptr == m_ThreadPoolPtr)
    {
        send_order_thread(&unit,ticker,*data,requestId,errorId,errorMsg);
        
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineFtx::send_order_thread,this,&unit,ticker,*data,requestId,errorId,errorMsg)); 
        KF_LOG_DEBUG(logger, "[req_order_insert] [left thread count ]: ] "<< m_ThreadPoolPtr->idlCount());
    }

}

void TDEngineFtx::action_order_thread(AccountUnitFtx* unit,string ticker,const LFOrderActionField data,int requestId,int64_t remoteOrderId,int errorId,std::string errorMsg)
{
    Document d;
    cancel_order(*unit, ticker, remoteOrderId, d);

    if(d.IsObject() && d.HasMember("success") && d["success"].GetBool() && d.HasMember("result") )
    {
        auto& result = d["result"];
        char status = GetOrderStatus(result["status"].GetString());
        if(status == LF_CHAR_Canceled)
        {
            std::unique_lock<std::mutex> lck(order_mutex);
            auto it = order_map.find(remoteOrderId);
            if(it != order_map.end())
            {
                it->second.data.OrderStatus = LF_CHAR_Canceled;
                on_rtn_order(&it->second.data);
            }
            order_map.erase(it);
            lck.unlock();
        }
    }
    else if(d.IsObject() && d.HasMember("success") && !d["success"].GetBool() && d.HasMember("error"))
    {
            errorId = 201;
            errorMsg = d["error"].GetString();
            KF_LOG_ERROR(logger, "[req_order_action]: " << requestId << " (errorId)" <<errorId << " (errorMsg) " << errorMsg);
    }
    else
    {
        errorId = 2;
        errorMsg = "unknown error";
        KF_LOG_ERROR(logger, "[req_order_action] cancel_order failed!" << " (rid)" << requestId
                                                                       << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    if(errorId != 0)
    {
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());

    } else {
        KF_LOG_INFO(logger,"req success");
    }

}

void TDEngineFtx::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time){
    
    int errorId = 0;
    std::string errorMsg = "";
    
    AccountUnitFtx& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_FTX, 1, requestId);
    
   
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);
    std::unique_lock<std::mutex> lck(order_mutex);
    auto itr = localOrderRefRemoteOrderId.find(data->OrderRef);
    int64_t remoteOrderId;
    if(itr == localOrderRefRemoteOrderId.end()) {
        errorId = 1;
        std::stringstream ss;
        ss << "[req_order_action] not found in localOrderRefRemoteOrderId map (orderRef) " << data->OrderRef;
        errorMsg = ss.str();
        KF_LOG_ERROR(logger, "[req_order_action] not found in localOrderRefRemoteOrderId map. "
                << " (rid)" << requestId << " (orderRef)" << data->OrderRef << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        return;
    } else {
        remoteOrderId = itr->second;
        KF_LOG_DEBUG(logger, "[req_order_action] found in localOrderRefRemoteOrderId map (orderRef) "
                << data->OrderRef << " (remoteOrderId) " << remoteOrderId);
    }
    lck.unlock();
    if(nullptr == m_ThreadPoolPtr)
    {
        action_order_thread(&unit,ticker,*data,requestId,remoteOrderId,errorId,errorMsg);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineFtx::action_order_thread,this,&unit,ticker,*data,requestId,remoteOrderId,errorId,errorMsg));
    }

}

/** insert quote */
void TDEngineFtx::req_quote_insert(const LFInputQuoteField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitFtx& unit = get_current_account();
    KF_LOG_DEBUG(logger, "[req_quote_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Price)" << data->Price
                                              << " (OrderRef)" << data->OrderRef);
    int errorId = 0;
    std::string errorMsg = "";
    on_rsp_quote_insert(data, requestId, errorId, errorMsg.c_str());
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_quote_insert]: not in WhiteList, ignore it  (rid)" << requestId <<
                                                                                      " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_quote_insert(data, requestId, errorId, errorMsg.c_str());
        return;
    }
    std::string fixedPrice;
    std::string fixedVolume;
    dealPriceVolume(unit,data->InstrumentID,data->Price,0,fixedPrice,fixedVolume,false);
    //
    string methodUrl = "/api/options/requests/"+ std::to_string(data->QuoteRequestID)+"/quotes";
    string body="{\"price\":"+ fixedPrice +"}";
    auto response = Post(methodUrl,body,unit);
    KF_LOG_INFO(logger, "[req_quote_insert] " << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );
    Document json;
    getResponse(response.status_code, response.text, response.error.message, json);
    if(!json.IsObject()){
        errorId = 100;
        errorMsg = "send_order http response has parse error or is not json. please check the log";
        KF_LOG_ERROR(logger, "[req_quote_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
                                                                           errorId << " (errorMsg) " << errorMsg);
    } 
    if(json.HasMember("success") && json["success"].GetBool() && json.HasMember("result"))
    {
        auto& result = json["result"];
        int64_t remoteOrderId = result["id"].GetInt64();
        std::unique_lock<std::mutex> lck(order_mutex);
        KF_LOG_INFO(logger, "[req_quote_insert] after send  (rid)" << requestId << " (OrderRef) " <<
                                                                           data->OrderRef << " (remoteOrderId) "
                                                                           << remoteOrderId);
        LFRtnQuoteField quote;
        strncpy(quote.InstrumentID,data->InstrumentID,sizeof(quote.InstrumentID));
        quote.ID = remoteOrderId;
        strncpy(quote.OrderRef,data->OrderRef,sizeof(quote.OrderRef));
        strncpy(quote.ExchangeID,data->ExchangeID,sizeof(quote.ExchangeID));
        quote.RequestID = requestId;
        quote.Price = std::round(result["price"].GetDouble()*scale_offset);
        quote.Volume = std::round(result["size"].GetDouble()*scale_offset);
        quote.QuoteRequestID = result["requestId"].GetInt64();
        quote.OrderStatus = GetOrderStatus(result["status"].GetString());
        quote.Direction = GetDirection(result["quoterSide"].GetString());
        on_rtn_quote(&quote);
        quote_map.insert(make_pair(remoteOrderId, quote));
        localOrderRefRemoteOrderId[std::string(data->OrderRef)] = remoteOrderId;
        lck.unlock();
    }
    else
    {
        errorId = 101;
        errorMsg = json["error"].GetString();
        KF_LOG_ERROR(logger, "[req_quote_insert]: (rid)" << requestId <<" (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_quote_insert(data, requestId, errorId, errorMsg.c_str());
    }

}
/** request cancel quote*/
void TDEngineFtx::req_quote_action(const LFQuoteActionField* data, int account_index, int requestId, long rcv_time)
{
    int errorId = 0;
    std::string errorMsg = "";
    
    AccountUnitFtx& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_quote_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);
    if(data->ActionFlag == '0')
    {//cancel
        std::unique_lock<std::mutex> lck(order_mutex);
        auto it = localOrderRefRemoteOrderId.find(data->OrderRef);
        if(it == localOrderRefRemoteOrderId.end())
        {
            errorId = 404;
            errorMsg = "not found in quote map (orderRef) " +  std::string(data->OrderRef);
            KF_LOG_ERROR(logger, "(rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
            on_rsp_quote_action(data, requestId, errorId, errorMsg.c_str());
            return;
        }
        auto it_ref = quote_map.find(it->second);
        if(it_ref == quote_map.end())
        {
            errorId = 404;
            errorMsg = "not found in localOrderRefRemoteOrderId (remote order if) " +  std::to_string(it->second);
            KF_LOG_ERROR(logger, "(rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
            on_rsp_quote_action(data, requestId, errorId, errorMsg.c_str());
            return;
        }
        auto& rtn_quote = it_ref->second;
        //{"result":{"id":157000274,"option":{"expiry":"2020-08-28T03:00:00+00:00","strike":6000.0,"type":"put","underlying":"BTC"},"requestExpiry":"2020-07-26T16:11:29.301170+00:00","side":"buy","size":1.0,"status":"cancelled","time":"2020-07-26T16:06:29.300853+00:00"},"success":true}
        string methodUrl = "/api/options/quotes/"+std::to_string(rtn_quote.ID);
        auto response  = Delete(methodUrl,unit,"");
        Document json;
        getResponse(response.status_code, response.text, response.error.message, json);
        KF_LOG_INFO(logger, "[cancel_quote] " <<  " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );
        if(json.HasMember("success") && json["success"].GetBool() && json.HasMember("result"))
        {
            auto& result = json["result"];
            rtn_quote = it_ref->second;
            rtn_quote.OrderStatus = GetOrderStatus(result["status"].GetString());
            on_rtn_quote(&rtn_quote);
        }
        else
        {
            errorId = 201;
            errorMsg = json["error"].GetString();
            KF_LOG_ERROR(logger, "[req_quote_action]: " << requestId << " (errorId)" <<errorId << " (errorMsg) " << errorMsg);
        }
    }
    else if(data->ActionFlag == '1')
    {//accept
        string methodUrl = "/api/options/quotes/"+std::to_string(data->QuoteID)+"/accept";
        auto response =  Post(methodUrl,"",unit);
        Document json;
        getResponse(response.status_code, response.text, response.error.message, json);
        KF_LOG_INFO(logger, "[accept_quote] " <<  " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );
        if(json.HasMember("success") && json["success"].GetBool())
        {
            auto& result = json["result"];
            int64_t quote_request_id = result["requestId"].GetInt64();
            std::unique_lock<std::mutex> lck(order_mutex);
            auto it  = order_map.find(quote_request_id);
            if(it != order_map.end())
            {
                char status = GetOrderStatus(result["status"].GetString());
                if(status != it->second.data.OrderStatus)
                {
                    it->second.data.OrderStatus = status;
                    on_rtn_order(&it->second.data);
                }
                if(isClosed(status))
                {
                    localOrderRefRemoteOrderId.erase(it->second.data.OrderRef);
                    order_map.erase(it);
                }
            }
            
        }
        else
        {
            errorId = 202;
            errorMsg = json["error"].GetString();
            KF_LOG_ERROR(logger, "[req_quote_action]: " << requestId << " (errorId)" <<errorId << " (errorMsg) " << errorMsg);
        }
    }
    on_rsp_quote_action(data, requestId, errorId, errorMsg.c_str());
}


void TDEngineFtx::req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw)
{
    
}
void TDEngineFtx::req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId)
{
    
}

void TDEngineFtx::req_inner_transfer(const LFTransferField* data, int account_index, int requestId)
{
    
}

void TDEngineFtx::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineFtx::loop");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineFtx::loop, this)));


}
void TDEngineFtx::getQuoteFills(AccountUnitFtx& unit)
{///api/options/fills
    std::string methodUrl = "/api/options/fills";
    auto response = Get(methodUrl,unit,"");
    KF_LOG_INFO(logger, "[getQuoteFills] "  << " (response.status_code) " << response.status_code <<
                                                        " (response.error.message) " << response.error.message <<
                                                        " (response.text) " << response.text.c_str() );
    Document json;
    getResponse(response.status_code, response.text, response.error.message, json);
    if(json.HasMember("success") && json["success"].GetBool() && json.HasMember("result"))
    {
        int size = json["result"].Size();
        auto& result = json["result"];
        for(int i = 0; i < size; ++i)
        {
            auto& quote = result[i];
            if (!quote["quoteId"].IsInt64()) // int or null
                continue;
            int64_t id = quote["quoteId"].GetInt64();
            std::unique_lock<std::mutex> lck(order_mutex);
            auto it = quote_map.find(id);
            if(it != quote_map.end())
            {
                char status = LF_CHAR_AllTraded;
                if(status != it->second.OrderStatus)
                {
                    it->second.OrderStatus = status;
                    on_rtn_quote(&it->second);
                }
                quote_map.erase(it);
                localOrderRefRemoteOrderId.erase(it->second.OrderRef);
            }
            if(quote_map.empty())
            {
                break;
            }
        }
    }
}

void TDEngineFtx::getQuoteStatus(AccountUnitFtx& unit)
{
    ///api/options/requests/"+ std::to_string(it->second.QuoteRequestID) +"/quotes
    std::unique_lock<std::mutex> lck(order_mutex);
    for(auto it = quote_map.begin(); it != quote_map.end(); )
    {
        std::string methodUrl = "/api/options/requests/"+ std::to_string(it->second.QuoteRequestID) +"/quotes";
        auto response = Get(methodUrl,unit,"");
        KF_LOG_INFO(logger, "[getQuoteStatus] "  << " (response.status_code) " << response.status_code <<
                                                        " (response.error.message) " << response.error.message <<
                                                        " (response.text) " << response.text.c_str() );
        Document json;
        getResponse(response.status_code, response.text, response.error.message, json);
        if(json.HasMember("success") && json["success"].GetBool() && json.HasMember("result"))
        {
            int size = json["result"].Size();
            auto& result = json["result"];
            for(int i = 0; i < size; ++i)
            {
                auto& quote = result[i];
                int64_t id = quote["id"].GetInt64();
                if(it->second.ID == id)
                {
                    char status = GetOrderStatus(quote["status"].GetString());
                    if(status != it->second.OrderStatus)
                    {
                        it->second.OrderStatus = status;
                        on_rtn_quote(&it->second);
                    }
                    if(it->second.OrderStatus == LF_CHAR_AllTraded || it->second.OrderStatus == LF_CHAR_Canceled)
                    {
                        it = quote_map.erase(it);
                        if(it != quote_map.begin())
                        {//
                            --it;
                        }
                        localOrderRefRemoteOrderId.erase(it->second.OrderRef);
                    }
                    break;
                }
            }
        }
        ++it;
    }
}
void TDEngineFtx::getQuoteRequestStatus(AccountUnitFtx& unit)
{
    
    std::string methodUrl = "/api/options/my_requests";
    auto response = Get(methodUrl,unit,"");
    KF_LOG_INFO(logger, "[getQuoteRequestStatus] "  << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );
    Document json;
    getResponse(response.status_code, response.text, response.error.message, json);
    if(json.HasMember("success") && json["success"].GetBool() && json.HasMember("result"))
    {
        std::unique_lock<std::mutex> lck(order_mutex);
        int size = json["result"].Size();
        for(int i =0;i <size; ++i)
        {
            int64_t id = json["result"][i]["id"].GetInt64();
            auto it = order_map.find(id);
            if(it != order_map.end())
            {
                auto& quote_request = it->second.data;
                char status = GetOrderStatus(json["result"][i]["status"].GetString());
                if(status != quote_request.OrderStatus)
                    quote_request.OrderStatus = status;
                if(quote_request.OrderStatus == LF_CHAR_AllTraded || quote_request.OrderStatus == LF_CHAR_Canceled)
                {
                    order_map.erase(it);
                    localOrderRefRemoteOrderId.erase(quote_request.OrderRef);
                }
                else
                {//更新 quote
                    auto& quotes = json["result"][i]["quotes"];
                    int quote_size = quotes.Size();
                    for(int j = 0; j < quote_size;++j)
                    {
                        int64_t quote_id = quotes[j]["id"].GetInt64();
                        char status = GetOrderStatus(quotes[j]["status"].GetString());
                        auto it_quote = it->second.mapQuote.find(quote_id);
                        if(it_quote == it->second.mapQuote.end())
                        {//new quote
                            LFRtnQuoteField quote;
                            quote.ID = quote_id;
                            strncpy(quote.InstrumentID,quote_request.InstrumentID,sizeof(quote.InstrumentID));
                            strncpy(quote.OrderRef,quote_request.OrderRef,sizeof(quote.OrderRef));
                            strncpy(quote.ExchangeID,quote_request.ExchangeID,sizeof(quote.ExchangeID));
                            quote.Price = std::round(quotes[j]["price"].GetDouble()*scale_offset);
                            quote.Volume = quote_request.VolumeTotalOriginal;
                            quote.QuoteRequestID = id;
                            quote.RequestID = quote_request.RequestID;
                            quote.Direction = quote_request.Direction == LF_CHAR_Buy ? LF_CHAR_Sell : LF_CHAR_Buy;
                            quote.OrderStatus = status;
                            on_rtn_quote(&quote);
                            it->second.mapQuote.insert(std::make_pair(quote_id,quote));
                        }
                        else 
                        {
                            if(status != it_quote->second.OrderStatus)
                            {
                                it_quote->second.OrderStatus = status;
                                on_rtn_quote(&it_quote->second);
                            }
                            if(status != LF_CHAR_NotTouched)
                            {
                                KF_LOG_INFO(logger, quote_id << " is closed");
                                it->second.mapQuote.erase(quote_id);
                            }
                        }
                        
                    }
                    
                }
                
            }
        }
    }
}

void TDEngineFtx::loop()
{
    int64_t nLastTime = getTimestamp();
    while(isRunning)
    {
        int64_t nNowTime = getTimestamp();
        if(nNowTime - nLastTime >= rest_get_interval_ms)
        {
            for (size_t idx = 0; idx < account_units.size(); idx++)
            {
                AccountUnitFtx& unit = account_units[idx];
                //unit.logged_in = true;
                //KF_LOG_INFO(logger, "[loop] (api_key)" << unit.api_key);
                getQuoteFills(unit);
                getQuoteRequestStatus(unit);
            }
            //KF_LOG_INFO(logger, "[loop] last time = " <<  nLastTime << ",now time = " << nNowTime);
            nLastTime = nNowTime;
        }
            
    }
}


void TDEngineFtx::getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json)
{
    if(http_status_code >= HTTP_RESPONSE_OK && http_status_code <= 299)
    {
        json.Parse(responseText.c_str());
    } 
    else
    {
        KF_LOG_INFO(logger, "[getResponse] (err) (responseText)" << responseText.c_str());
        if(responseText.empty())
        {
            json.SetObject();
            Document::AllocatorType& allocator = json.GetAllocator();
            json.AddMember("code", http_status_code, allocator);

            rapidjson::Value val;
            val.SetString(errorMsg.c_str(), errorMsg.length(), allocator);
            json.AddMember("error", val, allocator);
        }
        else
        {
            json.Parse(responseText.c_str());
        }
        
    }
}

void TDEngineFtx::get_account(AccountUnitFtx& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_account]");

    std::string requestPath="/api/wallet/balances";
    const auto response = Get(requestPath,unit,"");
    json.Parse(response.text.c_str());
    KF_LOG_INFO(logger, "[get_account] (account info) "<<response.text.c_str());
    return ;
}
std::string TDEngineFtx::createQuoteRequestString(std::string underlying,std::string type,std::string side,std::string strike,
    int64_t expiry,std::string price,std::string volume,bool is_hide,int64_t requestExpiry)
{
    std::stringstream ss;
    ss << "{\"underlying\":\"" << underlying << "\",\"type\":\""<<type << "\",\"strike\":" << strike << ",\"expiry\":"<<expiry <<
        ",\"side\":\"" << side << "\",\"size\":" << volume ;
    if(!is_hide)
    {
       ss << ",\"hideLimitPrice\":false";
    }
    if(requestExpiry > 0)
    {
        ss<< ",\"requestExpiry\":" << requestExpiry;
    }
    if(price.empty())
    {
        ss << "}";
    }
    else
    {
       ss << ",\"limitPrice\":" << price << "}";
    }
    
    return ss.str();
}

void TDEngineFtx::send_order(AccountUnitFtx& unit, std::string underlying, std::string type,std::string strike,
                                 std::string side,int64_t expiry, std::string price, std::string volume,bool is_hide,int64_t requestExpiry, Document& json){
    KF_LOG_INFO(logger, "[send_order]  "<<"(price)"<<price<<"(volume)"<<volume);

    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    std::string requestPath = "/api/options/requests";
    response = Post(requestPath,createQuoteRequestString(underlying,type,side,strike,expiry,price,volume,is_hide,requestExpiry),unit);
    KF_LOG_INFO(logger, "[send_order] (url) " << requestPath << " (response.status_code) " << response.status_code 
                                                  << " (response.error.message) " << response.error.message 
                                                  <<" (response.text) " << response.text.c_str() << " (retry_times)" << retry_times);
    //json.Clear();
    getResponse(response.status_code, response.text, response.error.message, json);

}

void TDEngineFtx::cancel_order(AccountUnitFtx& unit, std::string code,int64_t orderId, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_order]");

    int retry_times = 0;
    cpr::Response response;
    std::string requestPath="/api/options/requests/" + std::to_string(orderId);
    response = Delete(requestPath,unit,"");
    getResponse(response.status_code, response.text, response.error.message, json);
    KF_LOG_INFO(logger, "[cancel_order] " << retry_times << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );

}



int64_t TDEngineFtx::getTimestamp(){
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp + timeDiff;
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))
BOOST_PYTHON_MODULE(libftxtd){
    using namespace boost::python;
    class_<TDEngineFtx, boost::shared_ptr<TDEngineFtx> >("Engine")
     .def(init<>())
        .def("init", &TDEngineFtx::initialize)
        .def("start", &TDEngineFtx::start)
        .def("stop", &TDEngineFtx::stop)
        .def("logout", &TDEngineFtx::logout)
        .def("wait_for_stop", &TDEngineFtx::wait_for_stop);
}

