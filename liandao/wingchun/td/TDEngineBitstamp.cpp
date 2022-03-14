#include "TDEngineBitstamp.h"
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
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/bio.h>
#include <algorithm>
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
using utils::crypto::hmac_sha256;
using utils::crypto::hmac_sha256_byte;
using utils::crypto::base64_encode;
using utils::crypto::base64_url_encode;
USING_WC_NAMESPACE
std::mutex mutex_precision;
std::mutex mutex_order;
std::mutex mutex_local;

typedef char char_64[64];

TDEngineBitstamp::TDEngineBitstamp(): ITDEngine(SOURCE_BITSTAMP)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.Bitstamp");
    KF_LOG_INFO(logger, "[TDEngineBitstamp]");

    mutex_order_and_trade = new std::mutex();
    mutex_response_order_status = new std::mutex();
    mutex_orderaction_waiting_response = new std::mutex();
    m_ThreadPoolPtr = nullptr;
}

TDEngineBitstamp::~TDEngineBitstamp()
{
    if(mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if(mutex_response_order_status != nullptr) delete mutex_response_order_status;
    if(mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
    if(m_ThreadPoolPtr != nullptr) delete m_ThreadPoolPtr;
}
void TDEngineBitstamp::writeInfoLog(std::string strInfo){
    KF_LOG_INFO(logger,strInfo);
}
void TDEngineBitstamp::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}

int64_t TDEngineBitstamp::getMSTime(){
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  timestamp;
}
// helper function to compute SHA256:
std::vector<unsigned char> TDEngineBitstamp::sha256(string& data){
   std::vector<unsigned char> digest(SHA256_DIGEST_LENGTH);
   SHA256_CTX ctx;
   SHA256_Init(&ctx);
   SHA256_Update(&ctx, data.c_str(), data.length());
   SHA256_Final(digest.data(), &ctx);

   return digest;
}
//cys edit from kraken api
std::mutex g_httpMutex;
cpr::Response TDEngineBitstamp::Get(const std::string& method_url,const std::string& body, std::string postData,AccountUnitBitstamp& unit)
{
    string url = unit.baseUrl + method_url+"?"+postData;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    const auto response = cpr::Get(Url{url},
                                   Header{{}}, Timeout{10000} );
    lock.unlock();
    //if(response.text.length()<500){
    KF_LOG_INFO(logger, "[Get] (url) " << url << " (response.status_code) " << response.status_code <<
        " (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str());
    //}
    return response;
}
//cys edit
cpr::Response TDEngineBitstamp::Post(const std::string& method_url,const std::string& body,std::string postData, AccountUnitBitstamp& unit)
{
    /*int64_t nonce = getTimestamp();
    string nonceStr=std::to_string(nonce);
    KF_LOG_INFO(logger,"[Post] (nonce) "<<nonceStr);
    string s1="nonce=";
    postData=s1+nonceStr+"&"+postData;
    string path = method_url;
    string strSignature=getBitstampSignature(path,nonceStr,postData,unit);
    KF_LOG_INFO(logger,"[Post] (strSignature) "<<strSignature);*/


    string url = method_url;
    //std::unique_lock<std::mutex> lock(g_httpMutex);
    auto response = cpr::Post(Url{url}, 
                                /*Header{
                                {"API-Key", unit.api_key},
                                {"API-Sign",strSignature}},*/
                                Body{body},Timeout{30000});
    //lock.unlock();
    //if(response.text.length()<500){
    KF_LOG_INFO(logger, "[POST] (url) " << url <<" (body) "<< body<< " \n(response.status_code) " << response.status_code
        <<" (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str());
    //}
    return response;
}
void TDEngineBitstamp::init()
{
    genUniqueKey();
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineBitstamp::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineBitstamp::resize_accounts(int account_num)
{
    account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineBitstamp::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    string api_key = j_config["APIKey"].get<string>();
    string secret_key = j_config["SecretKey"].get<string>();
    //string passphrase = j_config["passphrase"].get<string>();
    //https://api.kraken.pro
    //string baseUrl = j_config["baseUrl"].get<string>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

    int thread_pool_size = 0;
    if(j_config.find("thread_pool_size") != j_config.end()) {
        thread_pool_size = j_config["thread_pool_size"].get<int>();
    }
    if(thread_pool_size > 0)
    {
        m_ThreadPoolPtr = new ThreadPool(thread_pool_size);
    }
    KF_LOG_INFO(logger, "[load_account] (thread_pool_size)" << thread_pool_size);

    if(j_config.find("orderaction_max_waiting_seconds") != j_config.end()) {
        orderaction_max_waiting_seconds = j_config["orderaction_max_waiting_seconds"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (orderaction_max_waiting_seconds)" << orderaction_max_waiting_seconds);

    if(j_config.find("max_rest_retry_times") != j_config.end()) {
        max_rest_retry_times = j_config["max_rest_retry_times"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (max_rest_retry_times)" << max_rest_retry_times);


    if(j_config.find("retry_interval_milliseconds") != j_config.end()) {
        retry_interval_milliseconds = j_config["retry_interval_milliseconds"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (retry_interval_milliseconds)" << retry_interval_milliseconds);

    AccountUnitBitstamp& unit = account_units[idx];
    unit.api_key = api_key;
    unit.secret_key = secret_key;
    /*unit.passphrase = passphrase;
    unit.baseUrl = baseUrl;

    KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl 
                                                   << " (spotAccountId) "<<unit.spotAccountId
                                                   << " (marginAccountId) "<<unit.marginAccountId);*/


    unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    unit.coinPairWhiteList.Debug_print();

    unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
    unit.positionWhiteList.Debug_print();

    //display usage:
    if(unit.coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "TDEngineBitstamp::load_account: please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }
    //test
    Document json;
    get_account(unit, json);
    getPrecision();
    //printResponse(json);
    //cancel_order(unit,"code","OCITZY-JMMFG-AT2MB3",json);
    //printResponse(json);
    //getPriceVolumePrecision(unit);
    // set up
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, api_key.c_str(), 16);
    strncpy(account.Password, secret_key.c_str(), 21);
    //web socket登陆
    //login(0);
    return account;
}

void TDEngineBitstamp::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitBitstamp& unit = account_units[idx];
        //unit.logged_in = true;
        KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
        Document doc;
        cancel_all_orders(unit, doc);
        //get_account(unit,doc);
        if (!unit.logged_in)
        {
            //KF_LOG_INFO(logger, "[connect] (account id) "<<unit.spotAccountId<<" login.");
            //lws_login(unit, 0);
            //set true to for let the kungfuctl think td is running.
            unit.logged_in = true;
        }
    }
}

void TDEngineBitstamp::getPrecision()
{
    KF_LOG_INFO(logger,"[getPrecision]");
    std::string url = "www.bitstamp.net/api/v2/trading-pairs-info/";
    const auto response = cpr::Get(Url{url}, Timeout{10000} );
    KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
                                                " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
    Document json;
    json.Parse(response.text.c_str());
    std::unique_lock<std::mutex> lck(mutex_precision);
    if(json.IsArray()){
        int size = json.Size();
        for(int i=0;i<size;i++){
            std::string name = json.GetArray()[i]["name"].GetString();     
            int base_decimals = json.GetArray()[i]["base_decimals"].GetInt();
            int counter_decimals = json.GetArray()[i]["counter_decimals"].GetInt();
            int precision = std::min(base_decimals,counter_decimals);
            int flag = name.find("/");
            std::string s1 = name.substr(0,flag);
            std::string s2 = name.substr(flag+1,name.length());
            std::string symbol = s1 + s2;
            transform(symbol.begin(),symbol.end(),symbol.begin(),::tolower);
            precision_map.insert(make_pair(symbol,precision));
        }
    }
    std::map<std::string, int>::iterator it;
    for(it=precision_map.begin();it!=precision_map.end();it++){
        KF_LOG_INFO(logger,"symbol:"<<it->first<<" precision="<<it->second); 
    }
    lck.unlock();
}

void TDEngineBitstamp::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[TDEngineBitstamp::login]");
    connect(timeout_nsec);
}

void TDEngineBitstamp::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineBitstamp::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineBitstamp::is_logged_in() const{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineBitstamp::is_connected() const{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}


std::string TDEngineBitstamp::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "buy";
    } else if (LF_CHAR_Sell == input) {
        return "sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineBitstamp::GetDirection(std::string input) {
    if ("buy" == input) {
        return LF_CHAR_Buy;
    } else if ("sell" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineBitstamp::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "market";
    } else {
        return "";
    }
}

LfOrderPriceTypeType TDEngineBitstamp::GetPriceType(std::string input) {
    if ("limit" == input) {
        return LF_CHAR_LimitPrice;
    } else if ("market" == input) {
        return LF_CHAR_AnyPrice;
    } else {
        return '0';
    }
}
//订单状态，pending 提交, open 部分成交, closed , open 成交, canceled 已撤销,expired 失效
LfOrderStatusType TDEngineBitstamp::GetOrderStatus(std::string state) {

    if(state == "canceled"){
        return LF_CHAR_Canceled;
    }else if(state == "pending"){
        return LF_CHAR_NotTouched;
    }else if(state == "closed"){
        return LF_CHAR_Error;
    }else if(state == "expired"){
        return LF_CHAR_Error;
    }else if(state == "open"){
        return LF_CHAR_NoTradeQueueing;
    }
    return LF_CHAR_Unknown;
}

void TDEngineBitstamp::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_investor_position]");

    AccountUnitBitstamp& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key);

    // User Balance
    Document d;
    get_account(unit, d);
    KF_LOG_INFO(logger, "[req_investor_position] get_account");
    printResponse(d);

    int errorId = 0;
    std::string errorMsg = "";
    if(d.HasParseError() )
    {
        errorId=100;
        errorMsg= "get_account http response has parse error. please check the log";
        KF_LOG_ERROR(logger, "[req_investor_position] get_account error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }

    if(!d.HasParseError() && d.IsObject() && d.HasMember("code") && d["code"].IsNumber())
    {
        errorId = d["code"].GetInt();
        if(d.HasMember("msg") && d["msg"].IsString())
        {
            errorMsg = d["msg"].GetString();
        }

        KF_LOG_ERROR(logger, "[req_investor_position] get_account failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_BITSTAMP, 1, requestId);

    LFRspPositionField pos;
    memset(&pos, 0, sizeof(LFRspPositionField));
    strncpy(pos.BrokerID, data->BrokerID, 11);
    strncpy(pos.InvestorID, data->InvestorID, 19);
    strncpy(pos.InstrumentID, data->InstrumentID, 31);
    pos.PosiDirection = LF_CHAR_Long;
    pos.Position = 0;

    std::vector<LFRspPositionField> tmp_vector;

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
    if(!d.HasParseError())
    {
        if ( d.HasMember("bch_reserved") ) {
            std::string symbol = "bch";
            if(symbol.length() > 0) {
                strncpy(pos.InstrumentID, symbol.c_str(), 31);
                pos.Position = std::round(stod(d["bch_reserved"].GetString()) * scale_offset);
                tmp_vector.push_back(pos);
                KF_LOG_INFO(logger,  "[connect] (symbol)" << symbol);
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
            }
        }
        else if ( d.HasMember("btc_reserved") ) {
            std::string symbol = "btc";
            if(symbol.length() > 0) {
                strncpy(pos.InstrumentID, symbol.c_str(), 31);
                pos.Position = std::round(stod(d["btc_reserved"].GetString()) * scale_offset);
                tmp_vector.push_back(pos);
                KF_LOG_INFO(logger,  "[connect] (symbol)" << symbol);
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
            }
        }
        else if ( d.HasMember("eth_reserved") ) {
            std::string symbol = "eth";
            if(symbol.length() > 0) {
                strncpy(pos.InstrumentID, symbol.c_str(), 31);
                pos.Position = std::round(stod(d["eth_reserved"].GetString()) * scale_offset);
                tmp_vector.push_back(pos);
                KF_LOG_INFO(logger,  "[connect] (symbol)" << symbol);
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
            }
        }
        else if ( d.HasMember("eur_reserved") ) {
            std::string symbol = "eur";
            if(symbol.length() > 0) {
                strncpy(pos.InstrumentID, symbol.c_str(), 31);
                pos.Position = std::round(stod(d["eur_reserved"].GetString()) * scale_offset);
                tmp_vector.push_back(pos);
                KF_LOG_INFO(logger,  "[connect] (symbol)" << symbol);
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
            }
        }
        else if ( d.HasMember("ltc_reserved") ) {
            std::string symbol = "ltc";
            if(symbol.length() > 0) {
                strncpy(pos.InstrumentID, symbol.c_str(), 31);
                pos.Position = std::round(stod(d["ltc_reserved"].GetString()) * scale_offset);
                tmp_vector.push_back(pos);
                KF_LOG_INFO(logger,  "[connect] (symbol)" << symbol);
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
            }
        }
        else if ( d.HasMember("usd_reserved") ) {
            std::string symbol = "usd";
            if(symbol.length() > 0) {
                strncpy(pos.InstrumentID, symbol.c_str(), 31);
                pos.Position = std::round(stod(d["usd_reserved"].GetString()) * scale_offset);
                tmp_vector.push_back(pos);
                KF_LOG_INFO(logger,  "[connect] (symbol)" << symbol);
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
            }
        }
        else if ( d.HasMember("xrp_reserved") ) {
            std::string symbol = "xrp";
            if(symbol.length() > 0) {
                strncpy(pos.InstrumentID, symbol.c_str(), 31);
                pos.Position = std::round(stod(d["xrp_reserved"].GetString()) * scale_offset);
                tmp_vector.push_back(pos);
                KF_LOG_INFO(logger,  "[connect] (symbol)" << symbol);
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << pos.Position);
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
        raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_BITSTAMP, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineBitstamp::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineBitstamp::dealPriceVolume(AccountUnitBitstamp& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,
            std::string& nDealPrice,std::string& nDealVolume){
    KF_LOG_DEBUG(logger, "[dealPriceVolume] (symbol)" << symbol);
    KF_LOG_DEBUG(logger, "[dealPriceVolume] (price)" << nPrice);
    KF_LOG_DEBUG(logger, "[dealPriceVolume] (volume)" << nVolume);
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(symbol);
    auto it = unit.mapPriceVolumePrecision.find(ticker);
    if(it == unit.mapPriceVolumePrecision.end())
    {
        KF_LOG_INFO(logger, "[dealPriceVolume] symbol not find :" << ticker);
        nDealVolume = "0";
        return ;
    }
    else
    {
        KF_LOG_INFO(logger,"[dealPriceVolume] (deal price and volume precision)");
        int pPrecision=it->second.pricePrecision;
        int vPrecision=it->second.amountPrecision;
        KF_LOG_INFO(logger,"[dealPriceVolume] (pricePrecision) "<<pPrecision<<" (amountPrecision) "<<vPrecision);
        double tDealPrice=nPrice*1.0/scale_offset;
        double tDealVolume=nVolume*1.0/scale_offset;
        KF_LOG_INFO(logger,"[dealPriceVolume] (tDealPrice) "<<tDealPrice<<" (tDealVolume) "<<tDealVolume);
        char chP[16],chV[16];
        sprintf(chP,"%.8lf",nPrice*1.0/scale_offset);
        sprintf(chV,"%.8lf",nVolume*1.0/scale_offset);
        nDealPrice=chP;
        KF_LOG_INFO(logger,"[dealPriceVolume] (chP) "<<chP<<" (nDealPrice) "<<nDealPrice);
        nDealPrice=nDealPrice.substr(0,nDealPrice.find(".")+(pPrecision==0?pPrecision:(pPrecision+1)));
        nDealVolume=chV;
         KF_LOG_INFO(logger,"[dealPriceVolume]  (chP) "<<chV<<" (nDealVolume) "<<nDealVolume);
        nDealVolume=nDealVolume.substr(0,nDealVolume.find(".")+(vPrecision==0?vPrecision:(vPrecision+1)));
    }
    KF_LOG_INFO(logger, "[dealPriceVolume]  (symbol)" << ticker << " (Volume)" << nVolume << " (Price)" << nPrice
                                                      << " (FixedVolume)" << nDealVolume << " (FixedPrice)" << nDealPrice);
}
//发单
void TDEngineBitstamp::dealnum(string pre_num,string& fix_num)
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

void TDEngineBitstamp::handle_order_insert(AccountUnitBitstamp& unit,const LFInputOrderField data,int requestId,const std::string& ticker)
{
    KF_LOG_DEBUG(logger, "[handle_order_insert]" << " (current thread)" << std::this_thread::get_id());
    int errorId = 0;
    std::string errorMsg = "";

    int precision;
    std::unique_lock<std::mutex> lck(mutex_precision);
    auto it = precision_map.find(ticker);
    if(it != precision_map.end()){
        precision = it->second;
    }
    lck.unlock();
    KF_LOG_INFO(logger,"precision="<<precision);
    Document d;
    string price = to_string(data.LimitPrice);
    string volume = to_string(data.Volume);
    string priceStr=" ";
    string sizeStr=" ";
    dealnum(price,priceStr);
    dealnum(volume,sizeStr);
    if(precision!=8){
        if(GetSide(data.Direction)=="buy"){//price过滤
            priceStr = priceStr.substr(0,priceStr.length()-(8-precision));
        }else{
            std::string b=priceStr.substr(0,priceStr.length()-(7-precision));
            double d= stod(b.substr(0,b.length()-1));
            d=d+pow(10,-precision);     
            priceStr=to_string(d);
            priceStr=priceStr.substr(0,priceStr.length()-(6-precision));
        }
    }
    KF_LOG_INFO(logger,"priceStr="<<priceStr<<"sizeStr="<<sizeStr);

    if((GetSide(data.Direction)=="buy")&&(GetType(data.OrderPriceType)=="limit")) {
        send_buylimitorder(unit, unit.userref, ticker, GetSide(data.Direction),GetType(data.OrderPriceType), sizeStr, priceStr, d);
    }
    else if((GetSide(data.Direction)=="buy")&&(GetType(data.OrderPriceType)=="market")) {
        send_buymarketorder(unit, unit.userref, ticker, GetSide(data.Direction),GetType(data.OrderPriceType), sizeStr, priceStr, d);
    }
    else if((GetSide(data.Direction)=="sell")&&(GetType(data.OrderPriceType)=="limit")) { 
        send_selllimitorder(unit, unit.userref, ticker, GetSide(data.Direction),GetType(data.OrderPriceType), sizeStr, priceStr, d);
    }
    else if((GetSide(data.Direction)=="sell")&&(GetType(data.OrderPriceType)=="market")) {
        send_sellmarketorder(unit, unit.userref, ticker, GetSide(data.Direction),GetType(data.OrderPriceType), sizeStr, priceStr, d);
    }

    /*std::map<std::string, std::string>::iterator itr = localOrderRefRemoteOrderId.find(data->OrderRef);
    std::string remoteOrderId;
    if(itr == localOrderRefRemoteOrderId.end()) {
        return;
    } else {
        remoteOrderId = itr->second;
        KF_LOG_DEBUG(logger, " [order_status](remoteOrderId) " << remoteOrderId);
    }*/

    //Document d;
    //order_status(unit, ticker, remoteOrderId, d);

    //not expected response
    string errorstr="error";
    if(!d.IsObject()){
        errorId = 100;
        errorMsg = "send_order http response has parse error or is not json. please check the log";
        KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
                                                                           errorId << " (errorMsg) " << errorMsg);
    }
    if(d.HasMember("status")&&d["status"].GetString()==errorstr){
        KF_LOG_INFO(logger,"[req_order_insert] (error) ");
        errorId = 100;
        errorMsg = d["reason"]["__all__"].GetArray()[0].GetString();
        if(errorMsg==""&&d["reason"].HasMember("price")){
            errorMsg = d["reason"]["price"].GetArray()[0].GetString();
        }
    }
    else {
            //if send successful and the exchange has received ok, then add to  pending query order list
            //std::string remoteOrderId = result["txid"].GetArray()[0].GetString();
            //fix defect of use the old value
            /*localOrderRefRemoteOrderId[std::string(data->OrderRef)] = remoteOrderId;
            KF_LOG_INFO(logger, "[req_order_insert] after send  (rid)" << requestId << " (OrderRef) " <<
                                                                       data->OrderRef << " (remoteOrderId) "
                                                                       << remoteOrderId);*/
            std::string remoteOrderId = d["id"].GetString();
            std::unique_lock<std::mutex> lck1(mutex_local);
            localOrderRefRemoteOrderId[std::string(data.OrderRef)] = remoteOrderId;
            lck1.unlock();
            PendingOrderStatus pOrderStatus;
            //初始化
            memset(&pOrderStatus, 0, sizeof(PendingOrderStatus));
            LFRtnOrderField *rtn_order = &pOrderStatus.rtn_order;
            strncpy(rtn_order->BusinessUnit,remoteOrderId.c_str(),21);
            rtn_order->OrderStatus = LF_CHAR_NotTouched;
            rtn_order->VolumeTraded = 0;
            rtn_order->VolumeTotalOriginal = data.Volume;
            rtn_order->LimitPrice = data.LimitPrice;
            
            strcpy(rtn_order->ExchangeID, "bitstamp");
            strncpy(rtn_order->UserID, unit.api_key.c_str(), 16);
            strncpy(rtn_order->InstrumentID, data.InstrumentID, 31);
            rtn_order->Direction = data.Direction;
            //No this setting on Bitstamp
            rtn_order->TimeCondition = LF_CHAR_GTC;
            rtn_order->OrderPriceType = data.OrderPriceType;
            strncpy(rtn_order->OrderRef, data.OrderRef, 13);
            rtn_order->VolumeTotalOriginal = data.Volume;
            rtn_order->LimitPrice = data.LimitPrice;
            rtn_order->VolumeTotal = data.Volume;

            on_rtn_order(rtn_order);
            raw_writer->write_frame(rtn_order, sizeof(LFRtnOrderField),
                                    source_id, MSG_TYPE_LF_RTN_TRADE_BITSTAMP,
                                    1, (rtn_order->RequestID > 0) ? rtn_order->RequestID : -1);
            std::unique_lock<std::mutex> lck2(mutex_order);
            order_map.insert(std::make_pair(remoteOrderId,*rtn_order));
            lck2.unlock();
            KF_LOG_DEBUG(logger, "[req_order_insert] (addNewQueryOrdersAndTrades)" );
            pOrderStatus.averagePrice = 0;
            //addNewQueryOrdersAndTrades(unit, pOrderStatus, remoteOrderId);

            raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITSTAMP, 1,
                                          requestId, errorId, errorMsg.c_str());
            KF_LOG_DEBUG(logger, "[req_order_insert] success" );
            //return;
        }
    //unlock
    if(errorId != 0)
    {
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITSTAMP, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineBitstamp::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time){
    //on_rtn_order(NULL);
    AccountUnitBitstamp& unit = account_units[account_index];
    unit.userref=std::to_string(requestId);
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITSTAMP, 1, requestId);

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
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITSTAMP, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);

    if(nullptr == m_ThreadPoolPtr)
    {
        handle_order_insert(unit,*data,requestId,ticker);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineBitstamp::handle_order_insert,this,unit,*data,requestId,ticker));
    }

}

void TDEngineBitstamp::handle_order_action(AccountUnitBitstamp& unit,const LFOrderActionField data, int requestId,const std::string& ticker)
{
    KF_LOG_DEBUG(logger, "[handle_order_action]" << " (current thread)" << std::this_thread::get_id());
    int errorId = 0;
    std::string errorMsg = ""; 

    std::unique_lock<std::mutex> lck1(mutex_local);
    std::map<std::string, std::string>::iterator itr = localOrderRefRemoteOrderId.find(data.OrderRef);
    std::string remoteOrderId;
    if(itr == localOrderRefRemoteOrderId.end()) {
        errorId = 1;
        std::stringstream ss;
        ss << "[req_order_action] not found in localOrderRefRemoteOrderId map (orderRef) " << data.OrderRef;
        errorMsg = ss.str();
        KF_LOG_ERROR(logger, "[req_order_action] not found in localOrderRefRemoteOrderId map. "
                << " (rid)" << requestId << " (orderRef)" << data.OrderRef << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITSTAMP, 1, requestId, errorId, errorMsg.c_str());
        return;
    } else {
        remoteOrderId = itr->second;
        KF_LOG_DEBUG(logger, "[req_order_action] found in localOrderRefRemoteOrderId map (orderRef) "
                << data.OrderRef << " (remoteOrderId) " << remoteOrderId);
        Document d;
        cancel_order(unit, ticker, remoteOrderId, d);
        if(!d.HasParseError()&&d.HasMember("id")){
            //iscancel_map.insert(std::make_pair(remoteOrderId,std::string(data.OrderRef)));
            std::unique_lock<std::mutex> lck2(mutex_order);
            auto it = order_map.find(remoteOrderId);
            if(it != order_map.end()){
                orderIsCanceled(unit,&(it->second));
                order_map.erase(it);
            }
            lck2.unlock(); 
            
            auto it2 = localOrderRefRemoteOrderId.find(std::string(data.OrderRef));
            if(it2 != localOrderRefRemoteOrderId.end()){
                KF_LOG_INFO(logger,"erase it2");
                localOrderRefRemoteOrderId.erase(it2);
            }
        }
        if(errorId != 0)
        {
            on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
            raw_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITSTAMP, 1, requestId, errorId, errorMsg.c_str());

        } else {
            //itr1++;
            KF_LOG_INFO(logger,"[req_order_action] cancel order success");
        }
    }
    lck1.unlock();

}

void TDEngineBitstamp::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time){
    AccountUnitBitstamp& unit = account_units[account_index];
    unit.userref=std::to_string(requestId);
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITSTAMP, 1, requestId);

    int errorId = 0;
    std::string errorMsg = "";

    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITSTAMP, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);

    if(nullptr == m_ThreadPoolPtr)
    {
        handle_order_action(unit,*data,requestId,ticker);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineBitstamp::handle_order_action,this,unit,*data,requestId,ticker));
    }

}
//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
void TDEngineBitstamp::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId){
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;
    newOrderActionSent.sentNameTime = getTimestamp();
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    remoteOrderIdOrderActionSentTime[remoteOrderId] = newOrderActionSent;
}
//cys no use
void TDEngineBitstamp::GetAndHandleOrderTradeResponse(){
     KF_LOG_INFO(logger, "[GetAndHandleOrderTradeResponse]" );
    //every account
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitBitstamp& unit = account_units[idx];
        if (!unit.logged_in)
        {
            continue;
        }
        //将新订单放到提交缓存中
        //moveNewOrderStatusToPending(unit);
        retrieveOrderStatus(unit);
    }//end every account
}

void TDEngineBitstamp::retrieveOrderStatus(AccountUnitBitstamp& unit){
    string finishedstr = "Finished";
    string openstr = "Open";
    string queuestr = "In Queue";
    std::map<std::string, LFRtnOrderField>::iterator it;
    std::unique_lock<std::mutex> lck2(mutex_order);
    for(it = order_map.begin();it != order_map.end();){
        std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(it->second.InstrumentID));
        KF_LOG_INFO(logger,"ticker="<<ticker);
        string tickerstr=ticker.substr(0,3);
        char_64 tickerchar;
        strcpy(tickerchar,tickerstr.c_str());
        std::string remoteOrderId = it->first;

        Document d;
        query_order(unit, ticker,remoteOrderId, d);
        if(d.HasMember("transactions")&&d["transactions"].IsArray()&&(d["transactions"].Size()!=0)&&(d["status"].GetString()==finishedstr||d["status"].GetString()==openstr))
        {
            Value &node=d["transactions"];
            int size = d["transactions"].Size();
            for(int i=0;i<size;i++){
                bool need_update = true;
                int64_t price = std::round(std::stod(node.GetArray()[i]["price"].GetString()) * scale_offset);
                uint64_t volume = std::round(std::stod(node.GetArray()[i][tickerchar].GetString()) * scale_offset);
                int64_t tid = node.GetArray()[i]["tid"].GetInt64();
                std::vector<TradeMsg>::iterator itr;
                for(itr = TradeMsg_vec.begin();itr != TradeMsg_vec.end();itr++){
                    if(itr->orderid == remoteOrderId && itr->tid == tid && itr->price == price && itr->volume == volume){
                        KF_LOG_INFO(logger,"no need");
                        need_update = false;
                        break;
                    }
                }
                if(need_update){
                    TradeMsg trademsg;
                    trademsg.orderid = remoteOrderId;
                    trademsg.tid = tid;
                    trademsg.price = price;
                    trademsg.volume = volume;
                    TradeMsg_vec.push_back(trademsg);
                    it->second.VolumeTraded += volume;
                    it->second.VolumeTotal = it->second.VolumeTotalOriginal - it->second.VolumeTraded;
                    if(it->second.VolumeTraded == it->second.VolumeTotalOriginal){
                        it->second.OrderStatus = LF_CHAR_AllTraded;
                    }else{
                        it->second.OrderStatus = LF_CHAR_PartTradedQueueing;
                    }

                    on_rtn_order(&(it->second));
                    raw_writer->write_frame(&(it->second), sizeof(LFRtnOrderField),
                                            source_id, MSG_TYPE_LF_RTN_ORDER_BITSTAMP, 1, -1);

                    LFRtnTradeField rtn_trade;
                    memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
                    strcpy(rtn_trade.ExchangeID,it->second.ExchangeID);
                    strncpy(rtn_trade.UserID, it->second.UserID,sizeof(rtn_trade.UserID));
                    strncpy(rtn_trade.InstrumentID, it->second.InstrumentID, sizeof(rtn_trade.InstrumentID));
                    strncpy(rtn_trade.OrderRef, it->second.OrderRef, sizeof(rtn_trade.OrderRef));
                    rtn_trade.Direction = it->second.Direction;
                    //strncpy(rtn_trade.OrderSysID,strOrderId.c_str(),sizeof(rtn_trade.OrderSysID));
                    rtn_trade.Volume = volume;
                    rtn_trade.Price = price;
                    on_rtn_trade(&rtn_trade);
                    raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
                                            source_id, MSG_TYPE_LF_RTN_TRADE_BITSTAMP, 1, -1);
                }
            }

            if(it->second.OrderStatus == LF_CHAR_AllTraded)
            {
                std::unique_lock<std::mutex> lck1(mutex_local);
                auto it_id = localOrderRefRemoteOrderId.find(it->second.OrderRef);
                if(it_id != localOrderRefRemoteOrderId.end())
                {
                    localOrderRefRemoteOrderId.erase(it_id);
                }
                it = order_map.erase(it);
                lck1.unlock();
            }else{
                it++;
            }
            
        }
        else{
            it++;
        }
    } 
    lck2.unlock();
}
//订单状态cys not use
/*void TDEngineBitstamp::retrieveOrderStatus(AccountUnitBitstamp& unit){
    KF_LOG_INFO(logger,"retrieveOrderStatus");

    std::lock_guard<std::mutex> guard_mutex(*mutex_response_order_status);
    KF_LOG_INFO(logger,"retrieveOrderStatus1");
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    std::vector<PendingOrderStatus>::iterator orderStatusIterator;
    KF_LOG_INFO(logger,"size="<<unit.pendingOrderStatus.size());
    for(orderStatusIterator = unit.pendingOrderStatus.begin(); orderStatusIterator != unit.pendingOrderStatus.end();)
    {

        std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(orderStatusIterator->rtn_order.InstrumentID));
        if(ticker.length() == 0) {
            KF_LOG_INFO(logger, "[retrieveOrderStatus]: not in WhiteList , ignore it:" );
 
            orderStatusIterator = unit.pendingOrderStatus.erase(orderStatusIterator);
            KF_LOG_INFO(logger,"size1="<<unit.pendingOrderStatus.size());
            continue;
        }

        string remoteOrderId = orderStatusIterator->rtn_order.BusinessUnit;
        auto iter = order_map.find(remoteOrderId);
        if(iter!=order_map.end()){
            Document d;
            query_order(unit, ticker,remoteOrderId, d);
            //订单状态，pending 提交, open 成交, canceled 已撤销, expired已失效, closed 
            if(d.HasParseError()) {
                //HasParseError, skip
                KF_LOG_ERROR(logger, "[retrieveOrderStatus] get_order response HasParseError " << " (symbol)" << orderStatusIterator->rtn_order.InstrumentID
                                                                                               << " (orderRef)" << orderStatusIterator->rtn_order.OrderRef
                                                                                               << " (remoteOrderId) " << remoteOrderId);
                continue;
            }
            KF_LOG_INFO(logger, "[retrieveOrderStatus] query_order:");
            string finishedstr = "Finished";
            string openstr = "Open";
            string queuestr = "In Queue";
            
            string tickerstr=ticker.substr(0,3);
            char_64 tickerchar;
            strcpy(tickerchar,tickerstr.c_str());
            if(d.HasMember("transactions")&&d["transactions"].IsArray()&&(d["transactions"].Size()!=0)&&(d["status"].GetString()==finishedstr))
            {
                    Value &node=d["transactions"]; 

                    KF_LOG_INFO(logger, "[retrieveOrderStatus] (query success)");
                    ResponsedOrderStatus responsedOrderStatus;
                    responsedOrderStatus.ticker = ticker;

                    //平均价格
                    responsedOrderStatus.averagePrice = std::round(std::stod(node.GetArray()[0]["price"].GetString()) * scale_offset);
                    //累计成交价格
                    responsedOrderStatus.PriceTraded = std::round(std::stod(node.GetArray()[0]["price"].GetString()) * scale_offset);
                    //总量
                    responsedOrderStatus.volume = std::round(orderStatusIterator->rtn_order.VolumeTotalOriginal);
                    //累计成交数量
                    responsedOrderStatus.VolumeTraded = std::round(std::stod(node.GetArray()[0][tickerchar].GetString()) * scale_offset);
                    //未成交数量
                    responsedOrderStatus.openVolume =  responsedOrderStatus.volume - orderStatusIterator->rtn_order.VolumeTraded;
                    //订单状态
                    //responsedOrderStatus.OrderStatus = LF_CHAR_AllTraded;

                    int len = d["transactions"].Size();
                    if(len>1){
                        for(int i=1;i<len;i++){
                            //累计成交价格
                            responsedOrderStatus.PriceTraded += std::round(std::stod(node.GetArray()[i]["price"].GetString()) * scale_offset);
                            //累计成交数量
                            responsedOrderStatus.VolumeTraded += std::round(std::stod(node.GetArray()[i][tickerchar].GetString()) * scale_offset);
                            //未成交数量
                            responsedOrderStatus.openVolume =  responsedOrderStatus.volume - orderStatusIterator->rtn_order.VolumeTraded;                                        
                        }
                    }
                    KF_LOG_ERROR(logger, "[volume and VolumeTraded]  " << " (Volume)" << responsedOrderStatus.volume
                                                                       << " (VolumeTraded)" << responsedOrderStatus.VolumeTraded);
                                                                                      
                    //订单状态
                    if(responsedOrderStatus.volume==responsedOrderStatus.VolumeTraded){
                        responsedOrderStatus.OrderStatus = LF_CHAR_AllTraded;
                    }
                    else if(responsedOrderStatus.volume>responsedOrderStatus.VolumeTraded){
                        responsedOrderStatus.OrderStatus = LF_CHAR_Canceled;
                    }
                    //订单信息处理
                    handlerResponseOrderStatus(unit, orderStatusIterator, responsedOrderStatus);

                    //OrderAction发出以后，有状态回来，就清空这次OrderAction的发送状态，不必制造超时提醒信息
                    remoteOrderIdOrderActionSentTime.erase(orderStatusIterator->rtn_order.BusinessUnit);
                
            } 
            else if(d.HasMember("transactions")&&d["transactions"].IsArray()&&(d["transactions"].Size()!=0)&&(d["status"].GetString()==openstr))
            {
                    Value &node=d["transactions"]; 

                    KF_LOG_INFO(logger, "[retrieveOrderStatus] (query success)");
                    ResponsedOrderStatus responsedOrderStatus;
                    responsedOrderStatus.ticker = ticker;
                    //平均价格
                    responsedOrderStatus.averagePrice = std::round(std::stod(node.GetArray()[0]["price"].GetString()) * scale_offset);
                    //累计成交价格
                    responsedOrderStatus.PriceTraded = std::round(std::stod(node.GetArray()[0]["price"].GetString()) * scale_offset);
                    //总量
                    responsedOrderStatus.volume = std::round(orderStatusIterator->rtn_order.VolumeTotalOriginal);
                    //累计成交数量
                    responsedOrderStatus.VolumeTraded = std::round(std::stod(node.GetArray()[0][tickerchar].GetString()) * scale_offset);
                    //未成交数量
                    responsedOrderStatus.openVolume =  responsedOrderStatus.volume - orderStatusIterator->rtn_order.VolumeTraded;
                    //订单状态
                    responsedOrderStatus.OrderStatus = LF_CHAR_PartTradedQueueing;

                    int len = d["transactions"].Size();
                    if(len>1){
                        for(int i=1;i<len;i++){
                            //累计成交价格
                            responsedOrderStatus.PriceTraded += std::round(std::stod(node.GetArray()[i]["price"].GetString()) * scale_offset);
                            //累计成交数量
                            responsedOrderStatus.VolumeTraded += std::round(std::stod(node.GetArray()[i][tickerchar].GetString()) * scale_offset);
                            //未成交数量
                            responsedOrderStatus.openVolume =  responsedOrderStatus.volume - orderStatusIterator->rtn_order.VolumeTraded;                                        
                        }
                    }
                    //订单信息处理
                    handlerResponseOrderStatus(unit, orderStatusIterator, responsedOrderStatus);

                    //OrderAction发出以后，有状态回来，就清空这次OrderAction的发送状态，不必制造超时提醒信息
                    remoteOrderIdOrderActionSentTime.erase(orderStatusIterator->rtn_order.BusinessUnit);
                
            } 
            else if(d.HasMember("error")){
                KF_LOG_INFO(logger, "[retrieveOrderStatus] (notfind)");
                //orderStatusIterator = unit.pendingOrderStatus.erase(orderStatusIterator);
                auto it = iscancel_map.find(remoteOrderId);
                if(it!=iscancel_map.end()){
                    orderIsCanceled(unit,&(orderStatusIterator->rtn_order));
                    iscancel_map.erase(it);
                }
            }
        }


        //remove order when finish
        if(orderStatusIterator->rtn_order.OrderStatus == LF_CHAR_AllTraded  || orderStatusIterator->rtn_order.OrderStatus == LF_CHAR_Canceled
           || orderStatusIterator->rtn_order.OrderStatus == LF_CHAR_Error)
        {
            KF_LOG_INFO(logger, "[retrieveOrderStatus] remove a pendingOrderStatus.");
            orderStatusIterator = unit.pendingOrderStatus.erase(orderStatusIterator);
            KF_LOG_INFO(logger,"erase orderStatusIterator");
            auto itr = order_map.find(remoteOrderId);
            if(itr!=order_map.end()){
                KF_LOG_INFO(logger,"erase order_map");
                order_map.erase(itr);
            }
        } else {
            ++orderStatusIterator;
        }
    }
}*/

void TDEngineBitstamp::addNewQueryOrdersAndTrades(AccountUnitBitstamp& unit, PendingOrderStatus pOrderStatus, std::string& remoteOrderId){
    KF_LOG_DEBUG(logger, "[addNewQueryOrdersAndTrades]" );
    //add new orderId for GetAndHandleOrderTradeResponse
    std::lock_guard<std::mutex> guard_mutex(*mutex_order_and_trade);

    unit.newOrderStatus.push_back(pOrderStatus);

    KF_LOG_INFO(logger, "[addNewQueryOrdersAndTrades] (InstrumentID) " << pOrderStatus.rtn_order.InstrumentID
                                                                       << " (OrderRef) " << pOrderStatus.rtn_order.OrderRef
                                                                       << " (remoteOrderId) " << pOrderStatus.rtn_order.BusinessUnit
                                                                       << "(VolumeTraded)" << pOrderStatus.rtn_order.VolumeTraded);
}


void TDEngineBitstamp::moveNewOrderStatusToPending(AccountUnitBitstamp& unit)
{
    std::lock_guard<std::mutex> pending_guard_mutex(*mutex_order_and_trade);
    std::lock_guard<std::mutex> response_guard_mutex(*mutex_response_order_status);


    std::vector<PendingOrderStatus>::iterator newOrderStatusIterator;
    for(newOrderStatusIterator = unit.newOrderStatus.begin(); newOrderStatusIterator != unit.newOrderStatus.end();)
    {
        unit.pendingOrderStatus.push_back(*newOrderStatusIterator);
        newOrderStatusIterator = unit.newOrderStatus.erase(newOrderStatusIterator);
    }
}
//cys no use
void TDEngineBitstamp::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineBitstamp::loop");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBitstamp::loop, this)));

    KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineBitstamp::loopOrderActionNoResponseTimeOut");
    orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBitstamp::loopOrderActionNoResponseTimeOut, this)));
}
//cys no use
void TDEngineBitstamp::loop()
{
    KF_LOG_INFO(logger, "[loop] (isRunning) " << isRunning);
    while(isRunning)
    {
        using namespace std::chrono;
        auto current_ms = duration_cast< milliseconds>(system_clock::now().time_since_epoch()).count();
        if(last_rest_get_ts != 0 && (current_ms - last_rest_get_ts) < rest_get_interval_ms)
        {
            continue;
        }

        last_rest_get_ts = current_ms;
        GetAndHandleOrderTradeResponse();
    }
}


void TDEngineBitstamp::loopOrderActionNoResponseTimeOut()
{
    KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        orderActionNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void TDEngineBitstamp::orderActionNoResponseTimeOut(){
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut]");
    int errorId = 100;
    std::string errorMsg = "OrderAction has none response for a long time(" + std::to_string(orderaction_max_waiting_seconds) + " s), please send OrderAction again";

    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    int64_t currentNano = getTimestamp();
    int64_t timeBeforeNano = currentNano - orderaction_max_waiting_seconds * 1000;
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (currentNano)" << currentNano << " (timeBeforeNano)" << timeBeforeNano);
    std::map<std::string, OrderActionSentTime>::iterator itr;
    for(itr = remoteOrderIdOrderActionSentTime.begin(); itr != remoteOrderIdOrderActionSentTime.end();)
    {
        if(itr->second.sentNameTime < timeBeforeNano)
        {
            KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.erase remoteOrderId)" << itr->first );
            on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
            itr = remoteOrderIdOrderActionSentTime.erase(itr);
        } else {
            ++itr;
        }
    }
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}

void TDEngineBitstamp::printResponse(const Document& d){
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
}

void TDEngineBitstamp::getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json)
{
    if(http_status_code >= HTTP_RESPONSE_OK && http_status_code <= 299)
    {
        json.Parse(responseText.c_str());
    } else if(http_status_code == 0)
    {
        json.SetObject();
        Document::AllocatorType& allocator = json.GetAllocator();
        int errorId = 1;
        json.AddMember("code", errorId, allocator);
        KF_LOG_INFO(logger, "[getResponse] (errorMsg)" << errorMsg);
        rapidjson::Value val;
        val.SetString(errorMsg.c_str(), errorMsg.length(), allocator);
        json.AddMember("msg", val, allocator);
    } else
    {
        Document d;
        d.Parse(responseText.c_str());
        KF_LOG_INFO(logger, "[getResponse] (err) (responseText)" << responseText.c_str());
        json.SetObject();
        Document::AllocatorType& allocator = json.GetAllocator();
        json.AddMember("code", http_status_code, allocator);

        rapidjson::Value val;
        val.SetString(errorMsg.c_str(), errorMsg.length(), allocator);
        json.AddMember("msg", val, allocator);
    }
}

void TDEngineBitstamp::get_account(AccountUnitBitstamp& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_account]");
    std::unique_lock<std::mutex> lock(g_httpMutex);
    string path="https://www.bitstamp.net/api/v2/balance/";
    int64_t nonce = getTimestamp();
    string nonceStr=std::to_string(nonce);
    string Message = nonceStr + unit.customer_id + unit.api_key;
    std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
    transform(signature.begin(),signature.end(),signature.begin(),::toupper);
    std::string queryString= "";
    queryString.append( "key=" );
    queryString.append( unit.api_key );
        queryString.append( "&signature=" );
    queryString.append( signature );
        queryString.append( "&nonce=" );
    queryString.append( nonceStr );    

    std::string body1 = queryString;        

    const auto response = Post(path,body1,"",unit);
    lock.unlock();
    json.Parse(response.text.c_str());
    //KF_LOG_INFO(logger, "[get_account] (account info) "<<response.text.c_str());
    return ;
}
std::string TDEngineBitstamp::createInsertOrdertring(string pair,string type,string ordertype,string price,string volume,
        string oflags,string userref){
    string s="";
    s=s+"pair="+pair+"&"+
        "type="+type+"&"+
        "ordertype="+ordertype+"&"+
        "price="+price+"&"+
        "volume="+volume+"&"+
        "userref="+userref;

    return s;
}
void TDEngineBitstamp::send_buylimitorder(AccountUnitBitstamp& unit, string userref, string code,
                        string side, string type, string volume, string price, Document& json){
    KF_LOG_INFO(logger, "[send_buylimitorder]");

    KF_LOG_INFO(logger, "[send_order] (code) "<<code);
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        std::unique_lock<std::mutex> lock(g_httpMutex);
        should_retry = false;
        string path = "https://www.bitstamp.net/api/v2/buy/";
        string url = path + code + "/";
        //string postData=createInsertOrdertring(code, side, type, price,volume,"",userref);
        int64_t nonce = getTimestamp();
        string nonceStr=std::to_string(nonce);
        string Message = nonceStr + unit.customer_id + unit.api_key;
        std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
        transform(signature.begin(),signature.end(),signature.begin(),::toupper);
        std::string queryString= "";
        queryString.append( "key=" );
        queryString.append( unit.api_key );
            queryString.append( "&signature=" );
        queryString.append( signature );
            queryString.append( "&nonce=" );
        queryString.append( nonceStr );
            queryString.append( "&amount=" );
        queryString.append( volume );
            queryString.append( "&price=" );
        queryString.append( price );
           /* queryString.append( "&limit_price=" );
        queryString.append( price );*/     

        std::string body1 = queryString;      

        response = Post(url,body1,"",unit);
        lock.unlock();

        KF_LOG_INFO(logger, "[send_order] (url) " << url << " (response.status_code) " << response.status_code 
                                                  << " (response.error.message) " << response.error.message 
                                                  << " (retry_times)" << retry_times);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);
        if(shouldRetry(json)) {
            should_retry = true;
            retry_times++;
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[send_buylimitorder] out_retry (response.status_code) " << response.status_code <<" (response.error.message) " 
                                                                        << response.error.message << " (response.text) " << response.text.c_str() );

    //getResponse(response.status_code, response.text, response.error.message, json);
}
void TDEngineBitstamp::send_buymarketorder(AccountUnitBitstamp& unit, string userref, string code,
                        string side, string type, string volume, string price, Document& json){
    KF_LOG_INFO(logger, "[send_buymarketorder]");
    KF_LOG_INFO(logger, "[send_order] (code) "<<code);
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        std::unique_lock<std::mutex> lock(g_httpMutex);
        should_retry = false;
        string path = "https://www.bitstamp.net/api/v2/buy/market/";
        string url = path + code + "/";
        //string postData=createInsertOrdertring(code, side, type, price,volume,"",userref);
        int64_t nonce = getTimestamp();
        string nonceStr=std::to_string(nonce);
        string Message = nonceStr + unit.customer_id + unit.api_key;
        std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
        transform(signature.begin(),signature.end(),signature.begin(),::toupper);
        std::string queryString= "";
        queryString.append( "key=" );
        queryString.append( unit.api_key );
            queryString.append( "&signature=" );
        queryString.append( signature );
            queryString.append( "&nonce=" );
        queryString.append( nonceStr );
            queryString.append( "&amount=" );
        queryString.append( volume );

        std::string body1 = queryString;      

        response = Post(url,body1,"",unit);
        lock.unlock();

        KF_LOG_INFO(logger, "[send_order] (url) " << url << " (response.status_code) " << response.status_code 
                                                  << " (response.error.message) " << response.error.message 
                                                  << " (retry_times)" << retry_times);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);
        if(shouldRetry(json)) {
            should_retry = true;
            retry_times++;
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[send_buymarketorder] out_retry (response.status_code) " << response.status_code <<" (response.error.message) " 
                                                                        << response.error.message << " (response.text) " << response.text.c_str() );

    //getResponse(response.status_code, response.text, response.error.message, json);
}
void TDEngineBitstamp::send_selllimitorder(AccountUnitBitstamp& unit, string userref, string code,
                        string side, string type, string volume, string price, Document& json){
    KF_LOG_INFO(logger, "[send_selllimitorder]");
    KF_LOG_INFO(logger, "[send_order] (code) "<<code);
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        std::unique_lock<std::mutex> lock(g_httpMutex);
        should_retry = false;
        string path = "https://www.bitstamp.net/api/v2/sell/";
        string url = path + code + "/";
        //string postData=createInsertOrdertring(code, side, type, price,volume,"",userref);
        int64_t nonce = getTimestamp();
        string nonceStr=std::to_string(nonce);
        string Message = nonceStr + unit.customer_id + unit.api_key;
        std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
        transform(signature.begin(),signature.end(),signature.begin(),::toupper);
        std::string queryString= "";
        queryString.append( "key=" );
        queryString.append( unit.api_key );
            queryString.append( "&signature=" );
        queryString.append( signature );
            queryString.append( "&nonce=" );
        queryString.append( nonceStr );
            queryString.append( "&amount=" );
        queryString.append( volume );
            queryString.append( "&price=" );
        queryString.append( price );
           /* queryString.append( "&limit_price=" );
        queryString.append( price ); */    

        std::string body1 = queryString;      

        response = Post(url,body1,"",unit);
        lock.unlock();

        KF_LOG_INFO(logger, "[send_selllimitorder] (url) " << url << " (response.status_code) " << response.status_code 
                                                  << " (response.error.message) " << response.error.message 
                                                  << " (retry_times)" << retry_times);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);
        if(shouldRetry(json)) {
            should_retry = true;
            retry_times++;
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[send_order] out_retry (response.status_code) " << response.status_code <<" (response.error.message) " 
                                                                        << response.error.message << " (response.text) " << response.text.c_str() );

    //getResponse(response.status_code, response.text, response.error.message, json);
}
void TDEngineBitstamp::send_sellmarketorder(AccountUnitBitstamp& unit, string userref, string code,
                        string side, string type, string volume, string price, Document& json){
    KF_LOG_INFO(logger, "[send_sellmarketorder]");
    KF_LOG_INFO(logger, "[send_order] (code) "<<code);
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        std::unique_lock<std::mutex> lock(g_httpMutex);
        should_retry = false;
        string path = "https://www.bitstamp.net/api/v2/sell/market/";
        string url = path + code + "/";
        //string postData=createInsertOrdertring(code, side, type, price,volume,"",userref);
        int64_t nonce = getTimestamp();
        string nonceStr=std::to_string(nonce);
        string Message = nonceStr + unit.customer_id + unit.api_key;
        std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
        transform(signature.begin(),signature.end(),signature.begin(),::toupper);
        std::string queryString= "";
        queryString.append( "key=" );
        queryString.append( unit.api_key );
            queryString.append( "&signature=" );
        queryString.append( signature );
            queryString.append( "&nonce=" );
        queryString.append( nonceStr );
            queryString.append( "&amount=" );
        queryString.append( volume );

        std::string body1 = queryString;      

        response = Post(url,body1,"",unit);
        lock.unlock();

        KF_LOG_INFO(logger, "[send_sellmarketorder] (url) " << url << " (response.status_code) " << response.status_code 
                                                  << " (response.error.message) " << response.error.message 
                                                  << " (retry_times)" << retry_times);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);
        if(shouldRetry(json)) {
            should_retry = true;
            retry_times++;
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[send_sellmarketorder] out_retry (response.status_code) " << response.status_code <<" (response.error.message) " 
                                                                        << response.error.message << " (response.text) " << response.text.c_str() );

    //getResponse(response.status_code, response.text, response.error.message, json);
}
void TDEngineBitstamp::order_status(AccountUnitBitstamp& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[order_status]");

    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        std::unique_lock<std::mutex> lock(g_httpMutex);
        should_retry = false;
        std::string path="https://www.bitstamp.net/api/order_status/";
        int64_t nonce = getTimestamp();
        string nonceStr=std::to_string(nonce);
        string Message = nonceStr + unit.customer_id + unit.api_key;
        std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
        transform(signature.begin(),signature.end(),signature.begin(),::toupper);
        std::string queryString= "";
        queryString.append( "key=" );
        queryString.append( unit.api_key );
            queryString.append( "&signature=" );
        queryString.append( signature );
            queryString.append( "&nonce=" );
        queryString.append( nonceStr );
            queryString.append( "&id=" );
        queryString.append( orderId );       

        std::string body1 = queryString; 

        response = Post(path,body1,"",unit);
        lock.unlock();

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);
        //has error and find the 'error setting certificate verify locations' error, should retry
        if(shouldRetry(json)) {
            should_retry = true;
            retry_times++;
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);


    KF_LOG_INFO(logger, "[order_status] out_retry " << retry_times << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );

    //getResponse(response.status_code, response.text, response.error.message, json);
}
bool TDEngineBitstamp::shouldRetry(Document& doc)
{
    bool ret = false;
    int errLen = 0;
    /*if(doc.HasMember("error"))
    {
        errLen = doc["error"].Size();
    }*/
    KF_LOG_INFO(logger, "[shouldRetry] ret = " << ret << ", errLen = " << errLen);
    return ret;
}

void TDEngineBitstamp::cancel_order(AccountUnitBitstamp& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[]");

    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        std::unique_lock<std::mutex> lock(g_httpMutex);
        should_retry = false;
        std::string path="https://www.bitstamp.net/api/v2/cancel_order/";
        int64_t nonce = getTimestamp();
        string nonceStr=std::to_string(nonce);
        string Message = nonceStr + unit.customer_id + unit.api_key;
        std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
        transform(signature.begin(),signature.end(),signature.begin(),::toupper);
        std::string queryString= "";
        queryString.append( "key=" );
        queryString.append( unit.api_key );
            queryString.append( "&signature=" );
        queryString.append( signature );
            queryString.append( "&nonce=" );
        queryString.append( nonceStr );
            queryString.append( "&id=" );
        queryString.append( orderId );       

        std::string body1 = queryString; 

        response = Post(path,body1,"",unit);
        lock.unlock();

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);
        //has error and find the 'error setting certificate verify locations' error, should retry
        if(shouldRetry(json)) {
            should_retry = true;
            retry_times++;
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);


    KF_LOG_INFO(logger, "[cancel_order] out_retry " << retry_times << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );

    //getResponse(response.status_code, response.text, response.error.message, json);
}
void TDEngineBitstamp::cancel_all_orders(AccountUnitBitstamp& unit, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_all_orders]");
    std::unique_lock<std::mutex> lock(g_httpMutex);
    string path="https://www.bitstamp.net/api/cancel_all_orders/";
    int64_t nonce = getTimestamp();
    string nonceStr=std::to_string(nonce);
    string Message = nonceStr + unit.customer_id + unit.api_key;
    std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
    transform(signature.begin(),signature.end(),signature.begin(),::toupper);
    std::string queryString= "";
    queryString.append( "key=" );
    queryString.append( unit.api_key );
        queryString.append( "&signature=" );
    queryString.append( signature );
        queryString.append( "&nonce=" );
    queryString.append( nonceStr );    

    std::string body1 = queryString;        

    const auto response = Post(path,body1,"",unit);
    lock.unlock();
    json.Parse(response.text.c_str());
    //KF_LOG_INFO(logger, "[get_account] (account info) "<<response.text.c_str());
    return ;
}

void TDEngineBitstamp::query_order(AccountUnitBitstamp& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[query_order] start");
    std::unique_lock<std::mutex> lock(g_httpMutex);
    //Bitstamp查询订单详情
    string getPath = "https://www.bitstamp.net/api/order_status/";
    int64_t nonce = getTimestamp();
    string nonceStr=std::to_string(nonce);
    string Message = nonceStr + unit.customer_id + unit.api_key;
    std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
    transform(signature.begin(),signature.end(),signature.begin(),::toupper);
    std::string queryString= "";
    queryString.append( "key=" );
    queryString.append( unit.api_key );
        queryString.append( "&signature=" );
    queryString.append( signature );
        queryString.append( "&nonce=" );
    queryString.append( nonceStr );    
        queryString.append( "&id=" );
    queryString.append( orderId );    
    std::string body1 = queryString;        

    auto response = Post(getPath,body1,"",unit);
    lock.unlock();
    json.Parse(response.text.c_str());
    KF_LOG_INFO(logger, "[query_order] end");
    return;
}
void TDEngineBitstamp::orderIsCanceled(AccountUnitBitstamp& unit, LFRtnOrderField* rtn_order){
    rtn_order->OrderStatus = LF_CHAR_Canceled;
    //累计成交数量
    //rtn_order.VolumeTraded;
    //剩余未成交数量
    //rtn_order->VolumeTotal = rtn_order.VolumeTotalOriginal-rtn_order->VolumeTraded;
    on_rtn_order(rtn_order);
    raw_writer->write_frame(&(*rtn_order), sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_TRADE_BITSTAMP,1, 
            (rtn_order->RequestID > 0) ? rtn_order->RequestID: -1);
}
void TDEngineBitstamp::handlerResponseOrderStatus(AccountUnitBitstamp& unit, std::vector<PendingOrderStatus>::iterator itr,
         ResponsedOrderStatus& responsedOrderStatus)
{
    KF_LOG_INFO(logger, "[handlerResponseOrderStatus]");
    LfOrderStatusType orderStatus = responsedOrderStatus.OrderStatus;
        /*if(responsedOrderStatus.VolumeTraded == responsedOrderStatus.volume){
            orderStatus = LF_CHAR_AllTraded;
        }else if(responsedOrderStatus.VolumeTraded == 0){
            orderStatus = LF_CHAR_NotTouched;
        }else if(responsedOrderStatus.VolumeTraded < responsedOrderStatus.volume){
            orderStatus = LF_CHAR_PartTradedQueueing;
        }*/
                    
    
    if(orderStatus == itr->rtn_order.OrderStatus&&responsedOrderStatus.VolumeTraded == itr->rtn_order.VolumeTraded){//no change
        KF_LOG_INFO(logger,"[handlerResponseOrderStatus] order status not change, return nothing.");
        return;
    }
    itr->rtn_order.OrderStatus = orderStatus;
    //单次成交量
    uint64_t singleVolume = responsedOrderStatus.VolumeTraded - itr->rtn_order.VolumeTraded;
    //单次成交价
    double oldAmount = itr->rtn_order.VolumeTraded/(scale_offset*1.0) * itr->averagePrice/(scale_offset*1.0);
    double newAmount = responsedOrderStatus.VolumeTraded/(scale_offset*1.0) * responsedOrderStatus.averagePrice/(scale_offset*1.0);
    double singlePrice = responsedOrderStatus.averagePrice;
    uint64_t oldVolumeTraded = itr->rtn_order.VolumeTraded;
    //累计成交数量
    itr->rtn_order.VolumeTraded=responsedOrderStatus.VolumeTraded;
    //剩余未成交数量
    itr->rtn_order.VolumeTotal = itr->rtn_order.VolumeTotalOriginal-itr->rtn_order.VolumeTraded;
    itr->averagePrice = responsedOrderStatus.averagePrice;
    on_rtn_order(&(itr->rtn_order));
    raw_writer->write_frame(&(itr->rtn_order), sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_TRADE_BITSTAMP,1, (itr->rtn_order.RequestID > 0) ? itr->rtn_order.RequestID: -1);

    if(oldVolumeTraded!=itr->rtn_order.VolumeTraded){
        //send OnRtnTrade
        LFRtnTradeField rtn_trade;
        memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
        strcpy(rtn_trade.ExchangeID, "bitstamp");
        strncpy(rtn_trade.UserID, unit.api_key.c_str(), 16);
        strncpy(rtn_trade.InstrumentID, itr->rtn_order.InstrumentID, 31);
        strncpy(rtn_trade.OrderRef, itr->rtn_order.OrderRef, 13);
        rtn_trade.Direction = itr->rtn_order.Direction;
        //单次成交数量
        rtn_trade.Volume = singleVolume;
        //单次成交价格
        rtn_trade.Price = std::round(singlePrice);
        strncpy(rtn_trade.OrderSysID,itr->rtn_order.BusinessUnit,31);
        on_rtn_trade(&rtn_trade);

        raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
            source_id, MSG_TYPE_LF_RTN_TRADE_BITSTAMP, 1, -1);

        KF_LOG_INFO(logger, "[on_rtn_trade 1] (InstrumentID)" << rtn_trade.InstrumentID << "(Direction)" << rtn_trade.Direction
                << "(Volume)" << rtn_trade.Volume << "(Price)" <<  rtn_trade.Price);
    }

}

std::string TDEngineBitstamp::parseJsonToString(Document &d){
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


inline int64_t TDEngineBitstamp::getTimestamp(){
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void TDEngineBitstamp::genUniqueKey(){
    struct tm cur_time = getCurLocalTime();
    //SSMMHHDDN
    char key[11]{0};
    snprintf((char*)key, 11, "%02d%02d%02d%02d%1s", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_engineIndex.c_str());
    m_uniqueKey = key;
}
//clientid =  m_uniqueKey+orderRef
std::string TDEngineBitstamp::genClinetid(const std::string &orderRef){
    static int nIndex = 0;
    return m_uniqueKey + orderRef + std::to_string(nIndex++);
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))
BOOST_PYTHON_MODULE(libbitstamptd){
    using namespace boost::python;
    class_<TDEngineBitstamp, boost::shared_ptr<TDEngineBitstamp> >("Engine")
     .def(init<>())
        .def("init", &TDEngineBitstamp::initialize)
        .def("start", &TDEngineBitstamp::start)
        .def("stop", &TDEngineBitstamp::stop)
        .def("logout", &TDEngineBitstamp::logout)
        .def("wait_for_stop", &TDEngineBitstamp::wait_for_stop);
}




