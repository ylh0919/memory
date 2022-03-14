#include "TDEngineBitflyer.h"
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
#include <map>
#include <stdio.h>
#include <cmath>
#include <assert.h>
#include <mutex>
#include <chrono>
#include "../../utils/crypto/openssl_util.h"

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
USING_WC_NAMESPACE

std::atomic<int> rest5times (0);

std::mutex http_mutex;
std::mutex mutex_limit;
std::atomic<int> remain(-1);
std::atomic<int64_t> reset(0);


TDEngineBitflyer::TDEngineBitflyer() : ITDEngine(SOURCE_BITFLYER)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.Bitflyer");
    KF_LOG_INFO(logger, "[TDEngineBitflyer]");

    mutex_order_and_trade = new std::mutex();
    mutex_map_new_order=new std::mutex();
    mutex_order_map=new std::mutex();
    mutex_cancel_map=new std::mutex();
    //mutex_response_order_status = new std::mutex();
    //mutex_orderaction_waiting_response = new std::mutex();
}

TDEngineBitflyer::~TDEngineBitflyer()
{
    if (mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if (mutex_response_order_status != nullptr) delete mutex_response_order_status;
    if (mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
    if(m_ThreadPoolPtr!=nullptr) delete m_ThreadPoolPtr;
    if(mutex_map_new_order!=nullptr) delete mutex_map_new_order;
    if(mutex_order_map!=nullptr) delete mutex_order_map;
    if(mutex_cancel_map!=nullptr) delete mutex_cancel_map;
    
}


inline int64_t TDEngineBitflyer::get_timestamp_ms()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

inline int64_t TDEngineBitflyer::get_timestamp_s()
{
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}


/*
queryString格式 “/v1/”+query
/v1/getchats
/v1/getchats/usa
/v1/getchats/eu
*/
cpr::Response TDEngineBitflyer::rest_withoutAuth(string& method, string& path, string& body)
{
    int64_t Timestamp = get_timestamp_ms();
   // int64_t cur_time = get_timestamp_s();
    //string method = "GET";
    //string path = "/v1/getchats/usa"; please append the version /v1/
    //string body="";
    /*baseUrl:https://api.bitflyer.com*/
    string url = "https://api.bitflyer.com";
    url += path;
    cpr::Response response;
    if (!strcmp(method.c_str(), "GET"))
    {
       // limit(cur_time);
        std::unique_lock<std::mutex> lck(http_mutex);
        response = cpr::Get(
            Url{ url },
            Body{ body },
            Timeout{ 10000 }
        );
        lck.unlock();
    }
    else if (!strcmp(method.c_str(), "POST"))
    {
       // limit(cur_time);
        std::unique_lock<std::mutex> lck(http_mutex);
        response = cpr::Post(
            Url{ url },
            Body{ body },
            Timeout{ 10000 }
        );
        lck.unlock();
    }
    else
    {
        KF_LOG_ERROR(logger, "request method error");
        response.error.message = "request method error";
        response.status_code = 404;
        return response;
    }
    
    

    auto& header = response.header;
    if(response.status_code == HTTP_RESPONSE_OK)
    {
        auto iter = header.find("X-RateLimit-Remaining");
        if(iter != header.end())
            remain = atoi(iter->second.c_str());
        iter = header.find("X-RateLimit-Reset");
        if(iter != header.end())
            reset = atoll(iter->second.c_str());
    }

    int64_t cur_time;
    if(remain <= 0)
    {
        cur_time = get_timestamp_s();
        int sleep_time = reset - cur_time + 1;
        KF_LOG_INFO(logger,"TDEngineBitflyer::rest_withoutAuth sleep" << sleep_time << "s");
        std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
    }

//    size_t undealed_order_num = unit.map_new_order.size();
 //   cur_time = get_timestamp_s();
  //  enable_call_num = remain - undealed_order_num * (2 * (reset - cur_time) * 1000 / rest_get_interval_ms + 1);

    KF_LOG_INFO(logger, "[" << method << "] (url) " << url <<
        " (body) " << body <<
        " (timestamp) " << Timestamp <<
        " (response.status_code) " << response.status_code <<
        " (response.error.message) " << response.error.message <<
        " (response.text) " << response.text.c_str());
    return response;
}

cpr::Response TDEngineBitflyer::rest_withAuth(AccountUnitBitflyer& unit, string& method, string& path, string& body)
{
    int64_t Timestamp = get_timestamp_ms();
//    int64_t cur_time = get_timestamp_s();
    //string method = "GET";
    //string path = "/v1/getchats/usa"; please append the version /v1/
    //string body="";
    /*baseUrl:https://api.bitflyer.com*/
    string key=unit.api_key;
    string secret=unit.secret_key;
    string url = unit.baseUrl + path;
    string text = std::to_string(Timestamp) + method + path + body;
    string sign=hmac_sha256(secret.c_str(), text.c_str());
    cpr::Response response;
    if (!strcmp(method.c_str(), "POST"))
    {
       // limit(cur_time);
        std::unique_lock<std::mutex> lck(http_mutex);
        response = cpr::Post(
            Url{ url },
            Body{ body },
            Header{{"ACCESS-KEY", key},
            {"ACCESS-TIMESTAMP",std::to_string(Timestamp)},
            {"ACCESS-SIGN",sign},
            {"Content-Type", "application/json"}
            },
            Timeout{ 10000 }
        );
        lck.unlock();
    }
    else if (!strcmp(method.c_str(), "GET"))
    {
        //limit(cur_time);
        std::unique_lock<std::mutex> lck(http_mutex);
        response = cpr::Get(  
            Url{ url },
            Body{ body },
            Header{{"ACCESS-KEY", key},
            {"ACCESS-TIMESTAMP",std::to_string(Timestamp)},
            {"ACCESS-SIGN",sign},
            {"Content-Type", "application/json"}
            },
            Timeout{ 10000 }

        );
        lck.unlock();
    }
    else
    {
        KF_LOG_ERROR(logger, "request method error");
        response.error.message = "request method error";
        response.status_code = 404;
        return response;
    }

    auto& header = response.header;
    if(response.status_code == HTTP_RESPONSE_OK)
    {
        auto iter = header.find("X-RateLimit-Remaining");
        if(iter != header.end())
            remain = atoi(iter->second.c_str());
        iter = header.find("X-RateLimit-Reset");
        if(iter != header.end())
            reset = atoll(iter->second.c_str());
    }
    int64_t cur_time;
    if(remain <= 0)
    {
        cur_time = get_timestamp_s();
        int sleep_time = reset - cur_time + 1;
        KF_LOG_INFO(logger,"TDEngineBitflyer::rest_withAuth sleep" << sleep_time << "s");
        std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
    }

    size_t undealed_order_num = unit.map_new_order.size();
    cur_time = get_timestamp_s();
    enable_call_num = remain - undealed_order_num * (2 * (reset - cur_time) * 1000 / rest_get_interval_ms + 1);    
    
    KF_LOG_INFO(logger,"TDEngineBitflyer::rest_withAuth begin"); 
    KF_LOG_INFO(logger,"TDEngineBitflyer::rest_withAuth enable_call_num" << enable_call_num);   
    KF_LOG_INFO(logger,"TDEngineBitflyer::rest_withAuth cur_time" << cur_time);
    KF_LOG_INFO(logger,"TDEngineBitflyer::rest_withAuth undealed_order_num" << undealed_order_num);
    KF_LOG_INFO(logger,"TDEngineBitflyer::rest_withAuth remain" << remain);
    KF_LOG_INFO(logger,"TDEngineBitflyer::rest_withAuth reset" << reset);
    KF_LOG_INFO(logger,"TDEngineBitflyer::rest_withAuth end");
   
    KF_LOG_INFO(logger, "[" << method << "] (url) " << url <<
        " (body) " << body <<
        " (timestamp) " << Timestamp <<
        " (text) "<<text<<
        " (sign) "<<sign<<
        " (response.status_code) " << response.status_code <<
        " (response.error.message) " << response.error.message <<
        " (response.text) " << response.text.c_str()) ;
    return response;
}

cpr::Response TDEngineBitflyer::chat()
{
    cpr::Response r;
    string method = "GET";
    string path = "/v1/getchats/usa";
    string body = "";
    r = rest_withoutAuth(method, path, body);
    return r;
}

cpr::Response TDEngineBitflyer::get_order(std::string requestId, int type)
{
    string method = "GET";
    string path;
    //string path0 = "/v1/me/getchildorders";
    string path1 = "/v1/me/getchildorders";
    string path2 = "/v1/me/getexecutions";
    //string path3 = "/v1/me/getchildorders";
    //string path4 = "/v1/me/getchildorders";
    string body = "";
    cpr::Response r,r0,r1,r2,r3,r4;
    AccountUnitBitflyer& unit = account_units[0];
    //if (type == 0) 
    //{
        map<std::string, OrderInfo>::iterator it;
       
        it = unit.map_new_order.find(requestId);
        if (it == unit.map_new_order.end())
        {
            //KF_LOG_ERROR(logger, "we do not find this order's child order id by requestId");
            r.status_code = 200;//need edit,,,,,
            r.error.message = "we do not find this order's child order id by requestId";
            KF_LOG_ERROR(logger, "[get_order]: " << r.error.message);
            return r;
        }
       
        OrderInfo& stOrderInfo = it->second;
        body = "";
        //path = path+"?child_order_acceptance_id="+stOrderInfo.child_order_acceptance_id;
        //path = path + "?product_code=" + stOrderInfo.product_code + "&child_order_acceptance_id="+stOrderInfo.child_order_acceptance_id+ "&child_order_state=ACTIVE";
        //r = rest_withAuth(account_units[0], method, path, body);
        //path0 = path0 + "?product_code=" + stOrderInfo.product_code + "&child_order_state=ACTIVE";
        //r0 = rest_withAuth(account_units[0], method, path0, body);
        /*path1 = path1 + "?product_code=" + stOrderInfo.product_code + "&child_order_acceptance_id="+stOrderInfo.child_order_acceptance_id;
        r1 = rest_withAuth(account_units[0], method, path1, body);
        path2 = path2 + "?product_code=" + stOrderInfo.product_code + "&child_order_acceptance_id="+stOrderInfo.child_order_acceptance_id;
        r2 = rest_withAuth(account_units[0], method, path2, body);*/
        //path3 = path3 + "?product_code=" + stOrderInfo.product_code + "&child_order_state=COMPLETED";
        //r3 = rest_withAuth(account_units[0], method, path3, body);
        //path4 = path4 + "?product_code=" + stOrderInfo.product_code + "&child_order_state=EXPIRED";
        //r4 = rest_withAuth(account_units[0], method, path4, body);        
    //}
    if(type==1){
        path = path1 + "?product_code=" + stOrderInfo.product_code + "&child_order_acceptance_id="+stOrderInfo.child_order_acceptance_id;
        r = rest_withAuth(account_units[0], method, path, body);
    }
    else{
        path = path2 + "?product_code=" + stOrderInfo.product_code + "&child_order_acceptance_id="+stOrderInfo.child_order_acceptance_id;
        r = rest_withAuth(account_units[0], method, path, body);
    }
    return r;
}

void TDEngineBitflyer::init()
{
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineBitflyer::pre_load(const json& j_config)
{
    //占位
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineBitflyer::resize_accounts(int account_num)
{
    //占位
    account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineBitflyer::load_account(int idx, const json & j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    AccountUnitBitflyer& unit = account_units[0];
    //加载必要参数
    string api_key = j_config["APIKey"].get<string>();
    string secret_key = j_config["SecretKey"].get<string>();
    string baseUrl = j_config["baseUrl"].get<string>();
    
    //币对白名单设置
    unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    unit.coinPairWhiteList.Debug_print();
    //持仓白名单设置
    unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
    unit.positionWhiteList.Debug_print();

    //可选配置参数如下设立
    if (j_config.find("rest_get_interval_ms") != j_config.end()) {
        rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (rest_get_interval_ms)" << rest_get_interval_ms);
    int thread_pool_size = 0;
    if(j_config.find("thread_pool_size") != j_config.end()) {
        thread_pool_size = j_config["thread_pool_size"].get<int>();
    }
    if(thread_pool_size > 0)
    {
        m_ThreadPoolPtr = new ThreadPool(thread_pool_size);
    }
    KF_LOG_INFO(logger, "[load_account] (thread_pool_size)" << thread_pool_size);
    //账户设置
    unit.api_key = api_key;
    unit.secret_key = secret_key;
    unit.baseUrl = baseUrl;
    unit.positionHolder.clear();
    KF_LOG_INFO(logger, "[load_account] (baseUrl)" << unit.baseUrl);

    //系统账户信息
    TradeAccount account = {};
    strncpy(account.UserID, api_key.c_str(), 16);
    strncpy(account.Password, secret_key.c_str(), 21);
    //simply for rest api test
    chat();
    /*string body = "";
    string path = "/v1/me/getbalance";
    string method = "GET";
    cpr::Response r = rest_withAuth(account_units[0], method, path, body);//获得账户余额消息
    KF_LOG_DEBUG(logger, "[getbalance](status_code)" << r.status_code <<
            "(response.text)" << r.text <<
            "(response.error.text)" << r.error.message);

    string body1 = "{\"product_code\":\"ETH_BTC\"}";
    string path1 = "/v1/me/cancelallchildorders";
    string method1 = "POST";
    cpr::Response r1 = rest_withAuth(account_units[0], method1, path1, body1);*/

    //test ends here
    return account;
}

void TDEngineBitflyer::connect(long timeout_nesc)
{
    //占位
    KF_LOG_INFO(logger, "[connect]");
    for(auto& unit:account_units)
    {
        account_units[0].logged_in=true;//maybe we need to add some "if" here
    }
}

void TDEngineBitflyer::login(long timeout_nesc)
{
    //占位
    KF_LOG_INFO(logger, "[login]");
    connect(timeout_nesc);
}

void TDEngineBitflyer::logout()
{
    //占位
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineBitflyer::release_api()
{
    //占位
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineBitflyer::is_connected() const
{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}

bool TDEngineBitflyer::is_logged_in() const
{
    //占位
    KF_LOG_INFO(logger, "[is_logged_in]");
    /*for(auto &unit:account_units)
    {
        if(!unit.logged_in)
            return false;
    }*/
    return true;
}

int TDEngineBitflyer::get_response_parsed_position(cpr::Response r)
{
    auto js = json::parse(r.text);
    AccountUnitBitflyer& unit = account_units[0];
    PositionSetting ps;
    /*
    [
        {
            "currency_code": "JPY",
            "amount" : 1024078,
            "available" : 508000
        },
        {
            "currency_code": "BTC",
            "amount" : 10.24,
            "available" : 4.12
        },
        {
            "currency_code": "ETH",
            "amount" : 20.48,
            "available" : 16.38
        }
    ]
    */
    if (js.is_array())//用于判断能否解析
    {
        for (int i = 0; i < js.size(); i++)
        {
            auto object = js[i];
            ps.ticker = object["currency_code"].get<string>();
            ps.amount = object["amount"].get<double>();
            if (ps.amount > 0)
                ps.isLong = true;
            else ps.isLong = false;
            unit.positionHolder.push_back(ps);
        }
        //TODO: maybe we need a debug print
        return 1;
    }
    return 0;
}

void TDEngineBitflyer::req_investor_position(const LFQryPositionField * data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitBitflyer& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_investor_position]" << "(InstrumentID) " << data->InstrumentID);
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_BITFLYER, 1, requestId);
    int errorId = 0;
    std::string errorMsg = "";

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

    /*实现一个函数获得balance信息，一个函数解析并存储到positionHolder里面去*/
    rest5times += 1;
    string body = "";
    string path = "/v1/me/getbalance";
    string method = "GET";
    cpr::Response r = rest_withAuth(unit, method, path, body);//获得账户余额消息
    KF_LOG_DEBUG(logger, "[getbalance](status_code)" << r.status_code <<
        "(response.text)" << r.text <<
        "(response.error.text)" << r.error.message);
    json js;
    while (true)
    {
        if (r.status_code == 200)//获得余额信息成功
        {
            if (get_response_parsed_position(r))
            {
                /*解析response*/
                /*若是解析成功则退出*/
                break;
            }
            else
            {
                KF_LOG_ERROR(logger, "get balance response parsed error,quit");
                return ;
            }
        }
        else
        {
            KF_LOG_ERROR(logger, "get balance failed,retry");
            rest5times += 1;
            r = rest_withAuth(unit, method, path, body);
            KF_LOG_DEBUG(logger, "[getbalance](status_code)" << r.status_code <<
                "(response.text)" << r.text <<
                "(response.error.text)" << r.error.message);
        }
    }
    bool findSymbolInResult = false;
    //send the filtered position
    int position_count = unit.positionHolder.size();
    for (int i = 0; i < position_count; i++)
    {
        pos.PosiDirection = LF_CHAR_Long;
        strncpy(pos.InstrumentID, unit.positionHolder[i].ticker.c_str(), 31);
        if (unit.positionHolder[i].isLong) {
            pos.PosiDirection = LF_CHAR_Long;
        }
        else {
            pos.PosiDirection = LF_CHAR_Short;
        }
        pos.Position = unit.positionHolder[i].amount;
        on_rsp_position(&pos, i == (position_count - 1), requestId, errorId, errorMsg.c_str());
        findSymbolInResult = true;
    }

    if (!findSymbolInResult)
    {
        KF_LOG_INFO(logger, "[req_investor_position] (!findSymbolInResult) (requestId)" << requestId);
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
    }

    if (errorId != 0)
    {
        raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_BITFLYER, 1, requestId, errorId, errorMsg.c_str());
    }

}

void TDEngineBitflyer::req_qry_account(const LFQryAccountField * data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

string TDEngineBitflyer::get_order_type(LfOrderPriceTypeType type)
{
    if (type == LF_CHAR_LimitPrice)
    {
        return "LIMIT";
    }
    else if (type == LF_CHAR_AnyPrice)//need check 哪一个order price type对应market类型
    {
        return "MARKET";
    }
    return "false";
}
string TDEngineBitflyer::get_order_side(LfDirectionType type)
{
    if (type == LF_CHAR_Buy)
    {
        return "BUY";
    }
    else if (type == LF_CHAR_Sell)
    {
        return "SELL";
    }
    return "false";
}

void TDEngineBitflyer::dealnum(string pre_num,string& fix_num)
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

void TDEngineBitflyer::dealprice(string pre_num,string& fix_num,string Side,string ticker)
{
    int size = pre_num.size();
    int iseth=ticker.find("ETH");
    int isbch=ticker.find("BCH");
    if(iseth>-1||isbch>-1){
        if(size>8){
            string s1 = pre_num.substr(0,size-8);
            s1.append(".");
            string s2 = pre_num.substr(size-8,size);
            string s3=s2.substr(0,5);
            if(Side=="SELL"){
                int inum = stoi(s3);
                inum = inum+1;
                s3 = to_string(inum);
            }
            fix_num = s1 + s3;
        }
        else if(size>3&&size<9){
            string s1 = pre_num.substr(0,size-3);
            if(Side=="SELL"){
                int inum = stoi(s1);
                inum = inum+1;
                s1 = to_string(inum);
            }  
            string s2 = "0.";
            for(int i=0;i<8-size;i++){
                s2.append("0");
            }         
            fix_num = s2 + s1;
        }
        else{
            string s1 = "0";
            if(Side=="SELL"){
                s1 = "0.00001";
            }           
            fix_num = s1;            
        }
    }
    else{
        if(size>8){
            string s1 = pre_num.substr(0,size-8);
            /*s1.append(".");
            string s2 = pre_num.substr(size-8,size);*/
            if(Side=="SELL"){
                int inum = stoi(s1);
                inum = inum+1;
                s1 = to_string(inum);
            }
            fix_num = s1;
        }
        else{
            string s1 = "0";
            /*for(int i=0; i<8-size; i++){
                s1.append("0");
            }*/
            if(Side=="SELL"){
                int inum = stoi(s1);
                inum = inum+1;
                s1 = to_string(inum);
            }           
            fix_num = s1;
        }
    }
    KF_LOG_INFO(logger,"pre_num:"<<pre_num<<"fix_num:"<<fix_num);
}


void TDEngineBitflyer::send_order_thread(AccountUnitBitflyer* unit,string product_code,const LFInputOrderField data,int requestId,int errorId,std::string errorMsg)
{
    limit_place(to_string(data.Volume));

    std::stringstream ss;
    string method = "POST";
    string path = "/v1/me/sendchildorder";
    string body;
    string child_order_type = get_order_type(data.OrderPriceType);
    string side = get_order_side(data.Direction);
    string price_str,size_str;
    string price = to_string(data.LimitPrice);
    string volume = to_string(data.Volume);
    dealprice(price,price_str,side,product_code);

    KF_LOG_INFO(logger,"product_code:"<<product_code);
    dealnum(volume,size_str);
    int iseth=product_code.find("ETH");
    if(iseth>-1){
        KF_LOG_INFO(logger,"findETH");
        int flag = size_str.find(".");
        string s1 = size_str.substr(0,flag+1);
        string s2 = size_str.substr(flag+1,size_str.length());
        if(s2.length()==8){
            s2=s2.substr(0,7);
        }
        size_str = s1 + s2;
        KF_LOG_INFO(logger,"size_str:"<<size_str);
    }
    double dvolume = stod(size_str);
    int isjpy=product_code.find("JPY");
    int isfx=product_code.find("FX");
    if(isjpy>-1&&isfx==-1){
        if(dvolume<0.001){
            errorId = 100;
            errorMsg = "min size is 0.001";
            on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        }
    }
    else{
        if(dvolume<0.01){
            errorId = 100;
            errorMsg = "min size is 0.01";
            on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        }        
    }
    std::stringstream s;
    int minute_to_expire = 2;
    s.flush();
    s << minute_to_expire;
    string minute_to_expire_str = s.str();
    string time_in_force = "GTC";

    body = R"({"product_code":")" + product_code + R"(",)"
        + R"("child_order_type":")" + child_order_type + R"(",)"
        + R"("side":")" + side + R"(",)"
        + R"("price":")" + price_str + R"(",)"
        + R"("size":")" + size_str + R"(",)"
        + R"("minute_to_expire":")" + minute_to_expire_str + R"(",)"
        + R"("time_in_force":")" + time_in_force + R"("})";
    cpr::Response r;
    rest5times += 1;
    r = rest_withAuth(*unit, method, path, body);//下单
    if (r.status_code == 200)//成功
    {
        Document json;
        json.Parse(r.text.c_str());  
        string child_order_acceptance_id = json["child_order_acceptance_id"].GetString();
        KF_LOG_INFO(logger,"child_order_acceptance_id:"<<child_order_acceptance_id);     

        vector<std::string> request;
        request.push_back(std::string(data.OrderRef));
        //KF_LOG_INFO(logger,"vector");
        OrderInfo stOrderInfo;//构建一个map方便日后查订单
        stOrderInfo.requestId = requestId;
        stOrderInfo.child_order_acceptance_id = child_order_acceptance_id;
        stOrderInfo.timestamp = get_timestamp_ms();
        stOrderInfo.product_code = product_code;

        std::unique_lock<std::mutex> map_new_order_mutex(*mutex_map_new_order);
        unit->map_new_order.insert(std::make_pair(std::string(data.OrderRef), stOrderInfo));//插入orderinfo，方便日后寻找订单创建时的相关信息
        //KF_LOG_INFO(logger,"map_new_order");
        size_t undealed_order_num = unit->map_new_order.size();
        int64_t cur_time = get_timestamp_s();
        enable_call_num = remain - undealed_order_num * (2 * (reset - cur_time) * 1000 / rest_get_interval_ms + 1);

        KF_LOG_INFO(logger,"TDEngineBitflyer::send_order_thread begin"); 
        KF_LOG_INFO(logger,"TDEngineBitflyer::send_order_thread enable_call_num" << enable_call_num);   
        KF_LOG_INFO(logger,"TDEngineBitflyer::send_order_thread cur_time" << cur_time);
        KF_LOG_INFO(logger,"TDEngineBitflyer::send_order_thread undealed_order_num" << undealed_order_num);
        KF_LOG_INFO(logger,"TDEngineBitflyer::send_order_thread remain" << remain);
        KF_LOG_INFO(logger,"TDEngineBitflyer::send_order_thread reset" << reset);
        KF_LOG_INFO(logger,"TDEngineBitflyer::send_order_thread end");
        map_new_order_mutex.unlock();

        LFRtnOrderField rtn_order;//返回order信息
        memset(&rtn_order, 0, sizeof(LFRtnOrderField));
        rtn_order.OrderStatus = LF_CHAR_NotTouched;
        strcpy(rtn_order.ExchangeID, "bitflyer");
        strncpy(rtn_order.UserID, unit->api_key.c_str(), 16);
        rtn_order.Direction = data.Direction;
        rtn_order.TimeCondition = LF_CHAR_GTC;
        rtn_order.OrderPriceType = data.OrderPriceType;
        strncpy(rtn_order.OrderRef, data.OrderRef, 13);
        strcpy(rtn_order.InstrumentID, data.InstrumentID);
        rtn_order.VolumeTraded = 0;
        rtn_order.VolumeTotalOriginal = data.Volume;
        rtn_order.VolumeTotal = data.Volume;
        rtn_order.LimitPrice = data.LimitPrice;
        rtn_order.RequestID = requestId;
        strncpy(rtn_order.BusinessUnit, child_order_acceptance_id.c_str(), 25);
        std::unique_lock<std::mutex> order_map_mutex(*mutex_order_map);
        ordermap.insert(std::make_pair(child_order_acceptance_id, rtn_order));
        order_map_mutex.unlock();
        //KF_LOG_INFO(logger,"ordermap");

        on_rtn_order(&rtn_order);
        raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
            source_id, MSG_TYPE_LF_RTN_ORDER_BITFLYER,
            1/*islast*/, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);
        //KF_LOG_INFO(logger,"raw_writer");
    }

    else
    {
        Document d;
        d.Parse(r.text.c_str());
        errorId = r.status_code;
        errorMsg = d["error_message"].GetString();
        KF_LOG_ERROR(logger, "req_order_insert error");
    }
    on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());

}
void TDEngineBitflyer::req_order_insert(const LFInputOrderField * data, int account_index, int requestId, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    AccountUnitBitflyer& unit = account_units[account_index];
    /*
    Request

    POST /v1/me/sendchildorder

    Body parameters
    {
        "product_code": "BTC_JPY",
        "child_order_type": "LIMIT",
        "side": "BUY",
        "price": 30000,
        "size": 0.1,
        "minute_to_expire": 10000,
        "time_in_force": "GTC"
    }
    */
    int errorId = 0;
    string errorMsg;
    on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
    string product_code = unit.coinPairWhiteList.GetValueByKey(string(data->InstrumentID));//就是ticker
    if (product_code.length() == 0)
    {
        errorId = 200;
        errorMsg = string(data->InstrumentID) + " not in WhiteList, ignored";
        KF_LOG_ERROR(logger, "[req_order_insert]: not in WhiteList, ignore it  " <<
            "(rid)" << requestId <<
            " (errorId)" << errorId <<
            " (errorMsg) " << errorMsg);
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITFLYER, 1, requestId, errorId, errorMsg.c_str());
        return;
    }

    if(nullptr == m_ThreadPoolPtr)
    {
        send_order_thread(&unit,product_code,*data,requestId,errorId,errorMsg);
        
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineBitflyer::send_order_thread,this,&unit,product_code,*data,requestId,errorId,errorMsg)); 
        KF_LOG_DEBUG(logger, "[req_order_insert] [left thread count ]: ] "<< m_ThreadPoolPtr->idlCount());
    }

}
void TDEngineBitflyer::action_order_thread(AccountUnitBitflyer* unit,string product_code,const LFOrderActionField data,int requestId,int errorId,std::string errorMsg)
{
    string method = "POST";
    string path = "/v1/me/cancelchildorder";
    string body;
    std::unique_lock<std::mutex> map_new_order_mutex(*mutex_map_new_order);
    map<std::string, OrderInfo>::iterator it;
    it = unit->map_new_order.find(std::string(data.OrderRef));
    if (it == unit->map_new_order.end())
    {
        KF_LOG_ERROR(logger, "we do not find this order's child order id by requestId");
        errorId = 200;//need edit,,,,,
        errorMsg = "we do not find this order's child order id by requestId";
        KF_LOG_ERROR(logger, "[req_order_action]: " << errorMsg);
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFLYER, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    map_new_order_mutex.unlock();
    string child_order_id = it->second.child_order_acceptance_id;
    rest5times += 1;
    body = R"({"product_code":")" + product_code + R"(",)"
        + R"("child_order_acceptance_id":")" + child_order_id + R"("})";
    cpr::Response r = rest_withAuth(*unit, method, path, body);
    if (r.status_code == 200)//成功
    {
        errorId = 0;
        errorMsg = "";
        KF_LOG_ERROR(logger, "[req_order_action] cancel_order succeeded (requestId)" << requestId );
        KF_LOG_INFO(logger,"cancel_id:"<<child_order_id);
        std::unique_lock<std::mutex> cancel_order_mutex(*mutex_cancel_map);
        cancel_map.insert(make_pair(child_order_id,std::string(data.OrderRef)));
        cancel_order_mutex.unlock();
    }
    else
    {
        errorId = r.status_code;
        errorMsg = r.error.message;
        KF_LOG_ERROR(logger, "[req_order_action] cancel_order failed (requestId)" << requestId << "(errorId)" << errorId << "(errorMsg)" << errorMsg);  
    }
    on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());

}
void TDEngineBitflyer::req_order_action(const LFOrderActionField * data, int account_index, int requestId, long rcv_time)
{
    AccountUnitBitflyer& unit=account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
        << " (APIKey)" << unit.api_key
        << " (Iid)" << data->InvestorID
        << " (OrderRef)" << data->OrderRef
        << " (KfOrderID)" << data->KfOrderID);
    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFLYER, 1, requestId);
    int errorId = 0;
    std::string errorMsg = "";

    std::string product_code = unit.coinPairWhiteList.GetValueByKey(string(data->InstrumentID));
    if (product_code.length() == 0)
    {
        errorId = 200;
        errorMsg = string(data->InstrumentID) + "not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
            errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFLYER, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << product_code);


    if(nullptr == m_ThreadPoolPtr)
    {
        action_order_thread(&unit,product_code,*data,requestId,errorId,errorMsg);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineBitflyer::action_order_thread,this,&unit,product_code,*data,requestId,errorId,errorMsg));
    }

    
}

void TDEngineBitflyer::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineBitflyer::loop");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBitflyer::loop, this)));

    /*KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineBitflyer::loopOrderAction");
    orderaction_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBitflyer::loopOrderAction, this)));*/
}
//cys no use
void TDEngineBitflyer::loop()
{
    KF_LOG_INFO(logger, "[loop] (isRunning) " << isRunning);
    while(isRunning)
    {
        
        using namespace std::chrono;
        auto current_ms = duration_cast< milliseconds>(system_clock::now().time_since_epoch()).count();
        /*
        if(last_5_ts == 0){
            last_5_ts = current_ms;
        }
        else if(last_5_ts != 0 && (current_ms - last_5_ts) >= 300000){
            KF_LOG_INFO(logger,"rest5times=0");
            rest5times = 0;
            last_5_ts = current_ms;
        }
        //KF_LOG_INFO(logger,"rest5times="<<rest5times);
        int rest_times = 480 - rest5times;//剩余次数 
        if(rest_times > 0){
            uint64_t rest_ms = ceil((300000-(current_ms - last_5_ts))/rest_times);
            if(rest_ms > rest_get_interval_ms){
                period_ms = rest_ms;
            }else{
                period_ms = rest_get_interval_ms;
            }
        }else{
            period_ms = 300000;
        }
        //KF_LOG_INFO(logger,"period_ms="<<period_ms);
        
        if(last_rest_get_ts != 0 && (current_ms - last_rest_get_ts) < rest_get_interval_ms)
        {
            continue;
        }
    
        last_rest_get_ts = current_ms;
        */
        GetAndHandleOrderTradeResponse();
        std::this_thread::sleep_for(std::chrono::milliseconds(rest_get_interval_ms));

    }
}
/*
void TDEngineBitflyer::loopOrderAction()
{
    KF_LOG_INFO(logger, "[loopOrderAction] (isRunning) " << isRunning);
    while(isRunning)
    {
        DealorderAction();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void TDEngineBitflyer::DealorderAction()
{
    std::map<std::string,std::string>::iterator itr;
    for(itr = cancel_map.begin();itr != cancel_map.end();){
        std::string orderref = itr->second;
        cpr::Response r1 = get_order(orderref,1);
        cpr::Response r2 = get_order(orderref,2);
        Document json;
        Document json2;
        json.Parse(r1.text.c_str());
        json2.Parse(r2.text.c_str());
        KF_LOG_INFO(logger,"r1.text:"<<r1.text);
        KF_LOG_INFO(logger,"r2.text:"<<r2.text);
        int size = 0;
        if(json.IsArray()){
            size = json.Size();//orderid
        }
        int size2 = 0;
        if(json2.IsArray()){
            size2 = json2.Size();//traded
        }

        if(size2==0&&size==0){//cancel
            string orderid = itr->first;
            KF_LOG_INFO(logger,"orderid="<<orderid);
            auto it1 = ordermap.find(orderid);
            if(it1 != ordermap.end()){
                it1->second.OrderStatus = LF_CHAR_Canceled;
                on_rtn_order(&(it1->second));
                ordermap.erase(it1);
                itr = cancel_map.erase(itr);
            }
        }
        else{
            itr++;
        }
    }
}*/
void TDEngineBitflyer::GetAndHandleOrderTradeResponse(){
    //KF_LOG_INFO(logger, "[GetAndHandleOrderTradeResponse]" );
    //KF_LOG_INFO(logger,"account_units.size():"<<account_units.size());
    //every account
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitBitflyer& unit = account_units[idx];
        /*if (!unit.logged_in)
        {
            continue;
        }*/
        //将新订单放到提交缓存中
        //moveNewOrderStatusToPending(unit);
        retrieveOrderStatus(unit);
    }//end every account
}

void TDEngineBitflyer::retrieveOrderStatus(AccountUnitBitflyer& unit){
    KF_LOG_INFO(logger,"[TDEngineBitflyer::retrieveOrderStatus]");
    //std::vector<std::string>::iterator it;
    std::unique_lock<std::mutex> map_new_order_mutex(*mutex_map_new_order);
    map<std::string, OrderInfo>::iterator itr;
    for(itr = unit.map_new_order.begin(); itr != unit.map_new_order.end();){
        std::string orderref = itr->first;
        KF_LOG_INFO(logger,"orderref:"<<orderref);
        rest5times += 2;
        cpr::Response r1 = get_order(orderref,1);
        cpr::Response r2 = get_order(orderref,2);
        Document json;
        Document json2;
        json.Parse(r1.text.c_str());
        json2.Parse(r2.text.c_str());
        KF_LOG_INFO(logger,"r1.text:"<<r1.text);
        KF_LOG_INFO(logger,"r2.text:"<<r2.text);
        int size = 0;
        if(json.IsArray()){
            size = json.Size();//orderid
        }
        int size2 = 0;
        if(json2.IsArray()){
            size2 = json2.Size();//traded
        }
        //if(json.IsArray()){
        //    int size = json.Size();
        //    if(size > 0){
        //        int i = 0;
        size_t undealed_order_num;
        int64_t cur_time;
        if(size2>0){//trade        
            for(int i=0;i<size2;i++){
                    string child_order_acceptance_id = json2.GetArray()[i]["child_order_acceptance_id"].GetString();
                    KF_LOG_INFO(logger,"child_order_acceptance_id1="<<child_order_acceptance_id);

                    double price = json2.GetArray()[i]["price"].GetDouble();
                    price = std::round(price*scale_offset);
                    int64_t uprice = price;
                    KF_LOG_INFO(logger,"price="<<price<<"uprice="<<uprice);
                    double executed_size = json2.GetArray()[i]["size"].GetDouble();
                    executed_size = std::round(executed_size*scale_offset);
                    uint64_t uvolume = executed_size;
                    KF_LOG_INFO(logger,"volume="<<executed_size<<"uvolume="<<uvolume);
                    /*LfOrderStatusType status = LF_CHAR_NotTouched;
                    if(child_order_state=="COMPLETED"){
                        status = LF_CHAR_AllTraded;
                    }
                    else if(child_order_state=="CANCELED"){
                        status = LF_CHAR_Canceled;
                    }
                    else if(child_order_state=="ACTIVE" && uvolume>0){
                        status = LF_CHAR_PartTradedQueueing;
                    }*/
                    std::unique_lock<std::mutex> order_map_mutex(*mutex_order_map);
                    std::map<string,LFRtnOrderField>::iterator it;
                    it = ordermap.find(child_order_acceptance_id);
                    if (it != ordermap.end()){
                        it->second.VolumeTraded += uvolume;
                        it->second.VolumeTotal = it->second.VolumeTotalOriginal - it->second.VolumeTraded;
                        if(it->second.VolumeTraded == it->second.VolumeTotalOriginal){
                            it->second.OrderStatus = LF_CHAR_AllTraded;
                        }else{
                            it->second.OrderStatus = LF_CHAR_PartTradedQueueing;
                        }
                            on_rtn_order(&(it->second));
                            raw_writer->write_frame(&(it->second), sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_TRADE_BITFLYER,1, (it->second.RequestID > 0) ? it->second.RequestID: -1);
                        
                        //if(uvolume>0){
                            LFRtnTradeField rtn_trade;
                            memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
                            strcpy(rtn_trade.ExchangeID, "bitflyer");
                            strncpy(rtn_trade.UserID, unit.api_key.c_str(), 16);
                            strncpy(rtn_trade.InstrumentID, it->second.InstrumentID, 31);
                            strncpy(rtn_trade.OrderRef, it->second.OrderRef, 13);
                            rtn_trade.Direction = it->second.Direction;
                            rtn_trade.Volume = uvolume;
                            rtn_trade.Price = uprice;
                            strncpy(rtn_trade.OrderSysID,it->second.BusinessUnit,31);
                            on_rtn_trade(&rtn_trade);

                            raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
                                source_id, MSG_TYPE_LF_RTN_TRADE_BITFLYER, 1, -1);                        
                        //}
                        if(it->second.OrderStatus == LF_CHAR_AllTraded){
                            KF_LOG_INFO(logger,"delete map");
                            ordermap.erase(it);
                            itr = unit.map_new_order.erase(itr);
                        }
                    }
                    undealed_order_num = unit.map_new_order.size();
                    cur_time = get_timestamp_s();
                    enable_call_num = remain - undealed_order_num * (2 * (reset - cur_time) * 1000 / rest_get_interval_ms + 1);
                    order_map_mutex.unlock();
            }
            KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus trade begin"); 
            KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus enable_call_num" << enable_call_num);   
            KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus cur_time" << cur_time);
            KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus undealed_order_num" << undealed_order_num);
            KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus remain" << remain);
            KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus reset" << reset);
            KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus trade end");
        }
        else if(size2==0&&size==0){//cancel
            string orderid = itr->second.child_order_acceptance_id;
            KF_LOG_INFO(logger,"orderid="<<orderid);
            std::unique_lock<std::mutex> cancel_order_mutex(*mutex_cancel_map);
            auto it2 = cancel_map.find(orderid);
            
            if(it2 != cancel_map.end()){
                cancel_order_mutex.unlock();
                std::unique_lock<std::mutex> order_map_mutex(*mutex_order_map);
                KF_LOG_INFO(logger,"cancel_map in");
                auto it1 = ordermap.find(orderid);
                if(it1 != ordermap.end()){
                    KF_LOG_INFO(logger,"deal LF_CHAR_Canceled");
                    it1->second.OrderStatus = LF_CHAR_Canceled;
                    on_rtn_order(&(it1->second));
                    ordermap.erase(it1);
                    itr = unit.map_new_order.erase(itr);
                }else{
                    itr++;
                }
                undealed_order_num = unit.map_new_order.size();
                cur_time = get_timestamp_s();
                enable_call_num = remain - undealed_order_num * (2 * (reset - cur_time) * 1000 / rest_get_interval_ms + 1);
                KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus cancel begin"); 
                KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus enable_call_num" << enable_call_num);   
                KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus cur_time" << cur_time);
                KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus undealed_order_num" << undealed_order_num);
                KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus remain" << remain);
                KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus reset" << reset);
                KF_LOG_INFO(logger,"TDEngineBitflyer::retrieveOrderStatus cancel end");
                order_map_mutex.unlock();
            }
            else{
                itr++;
            }

        }
        else{
            itr++;
        }
            /*else if(size == 0){
                string id = itr->second.child_order_acceptance_id;
                KF_LOG_INFO(logger,"id:"<<id);
                std::map<string,LFRtnOrderField>::iterator it1;
                it1 = ordermap.find(id);
                if (it1 != ordermap.end()){
                    it1->second.OrderStatus = LF_CHAR_Canceled;
                    on_rtn_order(&(it1->second));
                    raw_writer->write_frame(&(it1->second), sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_TRADE_BITFLYER,1, (it1->second.RequestID > 0) ? it1->second.RequestID: -1);
                    ordermap.erase(it1);
                    itr = unit.map_new_order.erase(itr);
                }                
            }*/
        
    }

    map_new_order_mutex.unlock();
}
void TDEngineBitflyer::limit_place(string data_volume)
{

    std::unique_lock<std::mutex> limit_mutex(mutex_limit);
   
    KF_LOG_INFO(logger,"TDEngineBitflyer::limit_place begin");
    int64_t cur_time = get_timestamp_s();
    int enable_num = -1;
    if(remain != -1 && cur_time < reset)
    {
        while(enable_num < 0)
        {
            cur_time = get_timestamp_s();
            enable_num = enable_call_num - (2 * (reset - cur_time) * 1000 / rest_get_interval_ms + 1);
            KF_LOG_INFO(logger,"TDEngineBitflyer::limit_place enable_call_num:"<< enable_call_num); 
            KF_LOG_INFO(logger,"TDEngineBitflyer::limit_place enable_num:"<< enable_num);            
            KF_LOG_INFO(logger,"TDEngineBitflyer::limit_place cur_time:"<<cur_time);
            KF_LOG_INFO(logger,"TDEngineBitflyer::limit_place remain:"<< remain);
            KF_LOG_INFO(logger,"TDEngineBitflyer::limit_place reset:"<< reset);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    double volume = stod(data_volume);
    KF_LOG_INFO(logger,"TDEngineBitflyer::limit_place volume:"<< volume);
    if(volume <= 10000000)
    {
        cur_time = get_timestamp_s();
        int cur_minute = cur_time / 60;
        while(!call_queue.empty()&&(call_queue.front() / 60) != cur_minute)
            call_queue.pop();
        int size = call_queue.size();
        KF_LOG_INFO(logger,"TDEngineBitflyer::limit_place size:"<<size);
        if(size >= 100)
        {
            int64_t sleep_time = 60 - cur_time % 60 + 1;
            KF_LOG_INFO(logger,"TDEngineBitflyer::limit_place sleep:"<< sleep_time << "s");
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        }
        cur_time = get_timestamp_s();
        call_queue.push(cur_time);
    }

    KF_LOG_INFO(logger,"TDEngineBitflyer::limit_place end");
    limit_mutex.unlock();
}
/*void TDEngineBitflyer::limit(int64_t call_time)
{
    std::unique_lock<std::mutex> limit_mutex(mutex_limit);
    if(call_num != 0)
    {
        int64_t time_interval[minute_num];
        for(int i = 0;i < minute_num;i++)
            time_interval[i] = reset - 60 * (minute_num - i);  //根据得到的重置时间来设置相应的时间区间

        int64_t sleep_time;
        if(remain > 1) //剩余调用数大于1
        {
            int i;
            for(i = 0;i < minute_num;i++)
            {
                int64_t time_gap = reset - call_time;
                double rate = remain*1.0 / time_gap; //得到当前平均每秒调用的限制速率rate
                if(call_time < time_interval[0]) //将当前时间调整到第一个区间内
                {
                    sleep_time = time_interval[0] - call_time + 1;
                    std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
                    call_time = get_timestamp_s();
                }
                else if(call_time > time_interval[minute_num-1] + 59) //无法调整，直接跳出
                {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    break;
                }
                if(call_time >= time_interval[i] && call_time <= (time_interval[i]+59))
                {
                    double remain_interval = rate * (time_interval[i] + 59 - call_time);//获取当前区间内的剩余调用数
                    if(remain_interval > 1.0)//若剩余调用数大于1 则直接跳出
                        break;
                    else //否则将时间sleep到下一区间
                    {
                        sleep_time = time_interval[i] + 59 - call_time + 1;
                        std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
                        call_time = get_timestamp_s();
                    }
               }
           }
        }
        else  //若调用数达到限制则直接sleep到下一个测量时段
        {
            sleep_time = reset - call_time + 1;
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        }
    }
    call_num++;
    limit_mutex.unlock();
}
*/
BOOST_PYTHON_MODULE(libbitflyertd)
{
    using namespace boost::python;
    class_<TDEngineBitflyer, boost::shared_ptr<TDEngineBitflyer> >("Engine")
        .def(init<>())
        .def("init", &TDEngineBitflyer::initialize)
        .def("start", &TDEngineBitflyer::start)
        .def("stop", &TDEngineBitflyer::stop)
        .def("logout", &TDEngineBitflyer::logout)
        .def("wait_for_stop", &TDEngineBitflyer::wait_for_stop);
}






