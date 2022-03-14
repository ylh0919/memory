#include "TDEngineKumex.h"
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
#include <functional>
#include <atomic>
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

std::atomic<bool> flag(false);
std::atomic<bool> ubsub(true);
std::atomic<bool> m_isSubL3(false);

TDEngineKumex::TDEngineKumex(): ITDEngine(SOURCE_KUMEX)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.Kumex");
    KF_LOG_INFO(logger, "[TDEngineKumex]");

    m_mutexOrder = new std::mutex();
    mutex_order_and_trade = new std::mutex();
    mutex_response_order_status = new std::mutex();
    mutex_orderaction_waiting_response = new std::mutex();
    mutex_marketorder_waiting_response = new std::mutex();
    mutex_web_connect=new std::mutex();
    m_ThreadPoolPtr = nullptr;
    m_LwsThreadPoolPtr = nullptr;
}

TDEngineKumex::~TDEngineKumex()
{
    if(m_mutexOrder != nullptr) delete m_mutexOrder;
    if(mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if(mutex_response_order_status != nullptr) delete mutex_response_order_status;
    if(mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
    if(mutex_marketorder_waiting_response != nullptr) delete mutex_marketorder_waiting_response;
    if(m_ThreadPoolPtr != nullptr) delete m_ThreadPoolPtr;
    if(mutex_web_connect!=nullptr) delete mutex_web_connect;
}

static TDEngineKumex* global_md = nullptr;

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
            //global_md->writeErrorLog(ss.str());
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
        case LWS_CALLBACK_CLIENT_CLOSED:
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

std::string TDEngineKumex::getId()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  std::to_string(timestamp);
}
AccountUnitKumex& TDEngineKumex::findAccountUnitKumexByWebsocketConn(struct lws * websocketConn){

    for (size_t idx = 0; idx < account_units.size(); idx++) {
        AccountUnitKumex &unit = account_units[idx];

        std::unique_lock<std::mutex> web_socket_connect_mutex(* mutex_web_connect);
        if(unit.webSocketConn == websocketConn) {
        KF_LOG_INFO(logger, "findAccountUnitHuobiByWebsocketConn unit " << unit.webSocketConn << ","<< idx);
        KF_LOG_INFO(logger,"api_key="<<unit.api_key);
        return unit;
        }

    }
    return account_units[0];
}
 void TDEngineKumex::onPong(struct lws* conn)
 {
     Ping(conn);
 }

 void TDEngineKumex::Ping(struct lws* conn)
 {
     m_shouldPing = false;
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
    KF_LOG_INFO(logger, "TDEngineKumex::lws_write_ping: " << strPing.c_str() << " ,len = " << length);
    strncpy((char *)msg+LWS_PRE, strPing.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
 }

 void TDEngineKumex::onOrder(const PendingOrderStatus& stPendingOrderStatus)
 {
            LFRtnOrderField rtn_order;
            memset(&rtn_order, 0, sizeof(LFRtnOrderField));
            rtn_order.RequestID = stPendingOrderStatus.nRequestID;
            rtn_order.OrderStatus = stPendingOrderStatus.OrderStatus;
            rtn_order.VolumeTraded = stPendingOrderStatus.VolumeTraded;

            //first send onRtnOrder about the status change or VolumeTraded change
            strcpy(rtn_order.ExchangeID, "kumex");
            strncpy(rtn_order.UserID, stPendingOrderStatus.strUserID.c_str(), sizeof(rtn_order.UserID));
            strncpy(rtn_order.InstrumentID, stPendingOrderStatus.InstrumentID, sizeof(rtn_order.InstrumentID));
            rtn_order.Direction = stPendingOrderStatus.Direction;
            //No this setting on Kumex
            rtn_order.TimeCondition = LF_CHAR_GTC;
            rtn_order.OrderPriceType = stPendingOrderStatus.OrderPriceType;
            strncpy(rtn_order.OrderRef, stPendingOrderStatus.OrderRef, sizeof(rtn_order.OrderRef));
            rtn_order.VolumeTotal = stPendingOrderStatus.nVolume - rtn_order.VolumeTraded;
            rtn_order.LimitPrice = stPendingOrderStatus.nPrice;
            rtn_order.VolumeTotalOriginal = stPendingOrderStatus.nVolume;
            strncpy(rtn_order.BusinessUnit,stPendingOrderStatus.remoteOrderId.c_str(),sizeof(rtn_order.BusinessUnit));

            on_rtn_order(&rtn_order);

            raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
                                    source_id, MSG_TYPE_LF_RTN_ORDER_KUMEX,
                                    1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);
            
            KF_LOG_INFO(logger, "[on_rtn_order] (InstrumentID)" << rtn_order.InstrumentID << "(OrderStatus)" <<  rtn_order.OrderStatus
                        << "(Volume)" << rtn_order.VolumeTotalOriginal << "(VolumeTraded)" << rtn_order.VolumeTraded);


 }

void TDEngineKumex::onTrade(const PendingOrderStatus& stPendingOrderStatus,int64_t nSize,int64_t nPrice,std::string& strTradeId,std::string& strTime)
{
            LFRtnTradeField rtn_trade;
            memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
            strcpy(rtn_trade.ExchangeID, "kumex");
            strncpy(rtn_trade.UserID, stPendingOrderStatus.strUserID.c_str(), sizeof(rtn_trade.UserID));
            strncpy(rtn_trade.InstrumentID, stPendingOrderStatus.InstrumentID, sizeof(rtn_trade.InstrumentID));
            strncpy(rtn_trade.OrderRef, stPendingOrderStatus.OrderRef, sizeof(rtn_trade.OrderRef));
            rtn_trade.Direction = stPendingOrderStatus.Direction;
            //calculate the volumn and price (it is average too)
            rtn_trade.Volume = nSize;
            rtn_trade.Price = nPrice;
            strncpy(rtn_trade.OrderSysID,stPendingOrderStatus.remoteOrderId.c_str(),sizeof(rtn_trade.OrderSysID));
            strncpy(rtn_trade.TradeID, strTradeId.c_str(), sizeof(rtn_trade.TradeID));
            strncpy(rtn_trade.TradeTime, strTime.c_str(), sizeof(rtn_trade.TradeTime));
            strncpy(rtn_trade.ClientID, stPendingOrderStatus.strClientId.c_str(), sizeof(rtn_trade.ClientID));
            on_rtn_trade(&rtn_trade);
            raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
                                    source_id, MSG_TYPE_LF_RTN_TRADE_KUMEX, 1, -1);

             KF_LOG_INFO(logger, "[on_rtn_trade 1] (InstrumentID)" << rtn_trade.InstrumentID << "(Direction)" << rtn_trade.Direction 
                        << "(Volume)" << rtn_trade.Volume << "(Price)" <<  rtn_trade.Price);
}
std::mutex mutex_sequence;
bool TDEngineKumex::matchSequence(Document& json,std::string& msg)
{
    bool ret = true;
    if(json.HasMember("data"))
    {
        auto& data = json["data"];
        if(data.HasMember("sequence") && data.HasMember("symbol"))
        {
            std::string strSymbol = data["symbol"].GetString();
            int64_t nSequence = data["sequence"].GetInt64();
            std::lock_guard<std::mutex> lck(mutex_sequence);
            auto it = mapSequence.find(strSymbol);
            if(it == mapSequence.end())
            {
                SequenceInfo info;
                info.nSequence = nSequence;
                mapSequence.insert(std::make_pair(strSymbol,info));
            }
            else
            {
                if(it->second.nSequence + 1 < nSequence)
                {
                    it->second.mapMessage.insert(std::make_pair(nSequence,msg));
                    KF_LOG_INFO(logger, "matchSequence: cached " << msg << ",local sequence:" << it->second.nSequence <<" < " << nSequence << " @" << std::this_thread::get_id());
                    ret = false;
                }
                else
                {
                    it->second.nSequence= nSequence;
                }
            }
        }
    }
    return ret;
}
std::string TDEngineKumex::getNextSequence(std::string& symbol)
{
    std::lock_guard<std::mutex> lck(mutex_sequence);
    auto it = mapSequence.find(symbol);
    if(it == mapSequence.end())
        return "";
    auto it_sec = it->second.mapMessage.find(it->second.nSequence+1);
    if(it_sec == it->second.mapMessage.end())
        return "";
    ++it->second.nSequence;
    std::string msg = it_sec->second;
    it->second.mapMessage.erase(it_sec);
    KF_LOG_INFO(logger, "getNextSequence: " << msg << " @" << std::this_thread::get_id());
    return msg;
}
void TDEngineKumex::onOrderChange(Document& d,bool match_sequence)
 {
        if(d.HasMember("data"))
        {
            auto& data = d["data"];

            if(data.HasMember("type"))
            {
                std::string strType = data["type"].GetString();
                
                if(strType == "done" && data["reason"].GetString() == std::string("canceled"))
                {
                    std::string strOrderId = data["orderId"].GetString();
                    std::lock_guard<std::mutex> lck(*m_mutexOrder); 
                    auto it = m_mapOrder.find(strOrderId);
                    if(it != m_mapOrder.end())
                    {
                        
                        std::unique_lock<std::mutex> lck1(*mutex_orderaction_waiting_response);
                        auto it3=remoteOrderIdOrderActionSentTime.find(strOrderId);
                        if(it3!=remoteOrderIdOrderActionSentTime.end())
                        {
                            KF_LOG_INFO(logger,"zafit3");
                            remoteOrderIdOrderActionSentTime.erase(it3);
                        }
                        KF_LOG_INFO(logger,"remoteOrderIdOrderActionSentTime:" << strOrderId);
                        for(auto& item:remoteOrderIdOrderActionSentTime)
                        {
                            KF_LOG_INFO(logger,"id:" << item.first);
                        }
                        lck1.unlock();

                        it->second.OrderStatus = LF_CHAR_Canceled;
                        onOrder( it->second);

                        auto it2 = localOrderRefRemoteOrderId.find(it->second.OrderRef);
                        if(it2 != localOrderRefRemoteOrderId.end())
                        {
                            KF_LOG_INFO(logger,"zaf-erase:"<< it2->second);
                            localOrderRefRemoteOrderId.erase(it2);
                        }
                        m_mapOrder.erase(it);
                    }

                }
                else if(strType == "match")
                {
                    std::string strOrderId = data["takerOrderId"].GetString();
                    std::lock_guard<std::mutex> lck(*m_mutexOrder); 
                    auto it = m_mapOrder.find(strOrderId);
                    if(it != m_mapOrder.end())
                    {
                        //int64_t nSize = std::round(std::stod(data["size"].GetString()) * scale_offset);
                        int64_t nPrice = std::round(std::stod(data["price"].GetString()) * scale_offset);
                        int64_t nSize = (data["matchSize"].GetInt64()) * scale_offset;
                        std::string strTradeId = data["tradeId"].GetString();
                        //std::string strTime = data["time"].GetString();
                        int64_t ts = data["ts"].GetInt64();
                        std::string strTime = std::to_string(ts);
                        it->second.VolumeTraded += nSize;
                        it->second.OrderStatus =  it->second.VolumeTraded ==  it->second.nVolume ? LF_CHAR_AllTraded : LF_CHAR_PartTradedQueueing;
                        onOrder( it->second);
                        onTrade(it->second,nSize,nPrice,strTradeId,strTime);
                        if( it->second.OrderStatus == LF_CHAR_AllTraded)
                        {
                            auto it2 = localOrderRefRemoteOrderId.find(it->second.OrderRef);
                            if(it2 != localOrderRefRemoteOrderId.end())
                            {
                                KF_LOG_INFO(logger,"zaf-erase:"<< it2->second);
                                localOrderRefRemoteOrderId.erase(it2);
                            }
                            m_mapOrder.erase(it);
                        }
                    }

                    strOrderId = data["makerOrderId"].GetString();
                    auto it1 = m_mapOrder.find(strOrderId);
                    if(it1 != m_mapOrder.end())
                    {
                        int64_t nPrice = std::round(std::stod(data["price"].GetString()) * scale_offset);
                        int64_t nSize = (data["matchSize"].GetInt64()) * scale_offset;
                        std::string strTradeId = data["tradeId"].GetString();
                        //std::string strTime = data["time"].GetString();
                        int64_t ts = data["ts"].GetInt64();
                        std::string strTime = std::to_string(ts);
                        it1->second.VolumeTraded += nSize;
                        it1->second.OrderStatus =  it1->second.VolumeTraded ==  it1->second.nVolume ? LF_CHAR_AllTraded : LF_CHAR_PartTradedQueueing;
                        onOrder( it1->second);
                        onTrade(it1->second,nSize,nPrice,strTradeId,strTime);
                        if( it1->second.OrderStatus == LF_CHAR_AllTraded)
                        {
                            auto it3 = localOrderRefRemoteOrderId.find(it1->second.OrderRef);
                            if(it3 != localOrderRefRemoteOrderId.end())
                            {
                                KF_LOG_INFO(logger,"zaf-erase:"<< it3->second);
                                localOrderRefRemoteOrderId.erase(it3);
                            }
                            m_mapOrder.erase(it1);
                        }
                    }

                }
                else if(strType == "received" && data.HasMember("clientOid"))
                {
                    std::string strClientID = data["clientOid"].GetString();
                    std::string strOrderId = data["orderId"].GetString();
                    std::lock_guard<std::mutex> lck(*m_mutexOrder); 
                    auto it = m_mapOrder.find(strOrderId);
                    auto it2 = m_mapNewOrder.find(strClientID);
                    if(it == m_mapOrder.end() && it2 != m_mapNewOrder.end())
                    {
                        it2->second.OrderStatus = LF_CHAR_NotTouched;
                        it2->second.remoteOrderId = strOrderId;
                        m_mapOrder.insert(std::make_pair(strOrderId,it2->second));
                        localOrderRefRemoteOrderId.insert(std::make_pair(it2->second.OrderRef,strOrderId));
                        KF_LOG_INFO(logger,"zaf-insert:" << strOrderId);
                        onOrder(it2->second);
                        m_mapNewOrder.erase(it2);
                    }
                }
            }
            if(match_sequence && data.HasMember("symbol"))
            {
                std::string strSymbol = data["symbol"].GetString();
                std::string next_msg= getNextSequence(strSymbol);
                while(!next_msg.empty())
                {
                    Document json;
                    json.Parse(next_msg.c_str());
                    onOrderChange(json,false);
                    next_msg= getNextSequence(strSymbol);
                }
            }
            KF_LOG_INFO(logger, "TDEngineKumex::onOrderChange: data end,match_sequence:" << match_sequence);
        }
        
 }


void TDEngineKumex::handle_lws_data(struct lws* conn,std::string data)
{
    //std::string strData = dealDataSprit(data);
	KF_LOG_INFO(logger, "TDEngineKumex::handle_lws_data: " << data);
    Document json;
    json.Parse(data.c_str());

    if(!json.HasParseError() && json.IsObject() && json.HasMember("type") && json["type"].IsString())
    {
        if(strcmp(json["type"].GetString(), "welcome") == 0)
        {
            KF_LOG_INFO(logger, "MDEngineKumex::handle_lws_data: welcome");
            lws_callback_on_writable(conn);
        }
        else if(strcmp(json["type"].GetString(), "pong") == 0)
        {
            KF_LOG_INFO(logger, "MDEngineKumex::handle_lws_data: pong");
            m_isPong = true;
            m_conn = conn;
        }
        else if(strcmp(json["type"].GetString(), "message") == 0 && matchSequence(json,data))
        {
            KF_LOG_INFO(logger, "TDEngineKumex::handle_lws_data: order");
            onOrderChange(json);
        }   
    } 
    else 
    {
        KF_LOG_ERROR(logger, "MDEngineKumex::on_lws_data . parse json error: " << data);
    }
}
void TDEngineKumex::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    //handle_lws_data(conn,std::string(data));
    
    //if(nullptr == m_ThreadPoolPtr)
    {
        handle_lws_data(conn,std::string(data));
    }
    //else
    //{
    //    m_ThreadPoolPtr->commit(std::bind(&TDEngineKumex::handle_lws_data,this,conn,std::string(data)));
    //}
	
}

std::string TDEngineKumex::makeSubscribeL3Update(const std::map<std::string,int>& mapAllSymbols)
{
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("id");
    writer.String(getId().c_str());
    writer.Key("type");
    writer.String("subscribe");
    writer.Key("topic");
    std::string strTopic = "/contractMarket/level3:";
    for(const auto&  pair : mapAllSymbols)
    {
        strTopic += pair.first + ",";
    }
    strTopic.pop_back();
    writer.String(strTopic.c_str());
    writer.Key("privateChannel");
    writer.String("false");
    writer.Key("response");
    writer.String("true");
    writer.EndObject();
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}

std::string TDEngineKumex::makeunSubscribeL3Update(const std::map<std::string,int>& mapAllSymbols)
{
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("id");
    writer.String(getId().c_str());
    writer.Key("type");
    writer.String("unsubscribe");
    writer.Key("topic");
    std::string strTopic = "/contractMarket/level3:";
    for(const auto&  pair : mapAllSymbols)
    {
        strTopic += pair.first + ",";
    }
    strTopic.pop_back();
    writer.String(strTopic.c_str());
    writer.Key("privateChannel");
    writer.String("false");
    writer.Key("response");
    writer.String("true");
    writer.EndObject();
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}

int TDEngineKumex::lws_write_subscribe(struct lws* conn)
{
    KF_LOG_INFO(logger, "TDEngineKumex::lws_write_subscribe:" );
    
    int ret = 0;

    if(!ubsub)
    {
        ubsub = true;

        std::map<std::string,int> mapAllSymbols;
        for(auto& unit : account_units)
        {
            for(auto& pair :  unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList())
            {
                mapAllSymbols[pair.second] = 0;
            }
        }

        std::string strSubscribe = makeunSubscribeL3Update(mapAllSymbols);
        unsigned char msg[1024];
        memset(&msg[LWS_PRE], 0, 1024-LWS_PRE);
        int length = strSubscribe.length();
        KF_LOG_INFO(logger, "TDEngineKumex::lws_write_subscribe: " << strSubscribe.c_str() << " ,len = " << length);
        strncpy((char *)msg+LWS_PRE, strSubscribe.c_str(), length);
        ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
        lws_callback_on_writable(conn);  
    }
    else if(!m_isSubL3)
    {
        KF_LOG_INFO(logger,"in l3");
         m_isSubL3 = true;

        std::map<std::string,int> mapAllSymbols;
        for(auto& unit : account_units)
        {
            for(auto& pair :  unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList())
            {
                mapAllSymbols[pair.second] = 0;
            }
        }

        std::string strSubscribe = makeSubscribeL3Update(mapAllSymbols);
        unsigned char msg[1024];
        memset(&msg[LWS_PRE], 0, 1024-LWS_PRE);
        int length = strSubscribe.length();
        KF_LOG_INFO(logger, "TDEngineKumex::lws_write_subscribe: " << strSubscribe.c_str() << " ,len = " << length);
        strncpy((char *)msg+LWS_PRE, strSubscribe.c_str(), length);
        ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
        lws_callback_on_writable(conn);  
    }
    else
    {
        if(m_shouldPing)
        {
            m_isPong = false;
            Ping(conn);
        }
    }
    
    return ret;
}

/*void TDEngineKumex::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "TDEngineKumex::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    m_isPong = false;
    m_shouldPing = true;
    //no use it
    long timeout_nsec = 0;
    //reset sub
    m_isSubL3 = false;
    login(timeout_nsec);
}*/
void TDEngineKumex::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_INFO(logger,"[on_lws_connection_error] on_lws_connection_error,threadid="<<std::this_thread::get_id());
    KF_LOG_ERROR(logger, "TDEngineKumex::on_lws_connection_error. login again.");
    AccountUnitKumex& unit=findAccountUnitKumexByWebsocketConn(conn);     
    unit.is_connecting = false; 
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    m_isPong = false;
    m_shouldPing = true;
    //no use it
    long timeout_nsec = 0;
    //reset sub
    m_isSubL3 = false;
    context = NULL;
    if(rest_thread != nullptr && rest_thread->joinable())
    {
        rest_thread->join();
        rest_thread = nullptr;
        KF_LOG_ERROR(logger, "TDEngineKumex::on_lws_connection_error. release loop");
    }
    //sleep(40);
    if(lws_login(unit, timeout_nsec))
    {
        Document json;
        for(auto& unit:account_units)
        {
            cancel_all_orders(unit, json);
            std::lock_guard<std::mutex> lck(*m_mutexOrder); 
            int64_t endTime = getTimestamp();
            int64_t startTime = endTime;
            int64_t mflag = 0;
            int64_t rflag = mflag ;
            for(auto& order : m_mapNewOrder)
            {
                if(order.second.nSendTime > 0)
                {
                    if(startTime > order.second.nSendTime)
                    {
                        startTime = order.second.nSendTime;
                        mflag++;
                    }
                }
            }
            for(auto& orders : m_mapOrder)
            {
                if(orders.second.nSendTime > 0)
                {
                    if(startTime > orders.second.nSendTime)
                    {
                        startTime = orders.second.nSendTime;
                        rflag++;
                    }
                }
            }

            if(m_mapOrder.size() != 0 || m_mapNewOrder.size() != 0)
                check_orders(unit,startTime,endTime,mflag,rflag,0);
            startTime = getTimestamp();
            mflag = rflag = 0;

            for(auto& order : m_mapNewOrder)
            {
                if(order.second.nSendTime > 0)
                {
                    if(startTime > order.second.nSendTime)
                    {
                        startTime = order.second.nSendTime;
                        mflag++;
                    }
                }
            }
            for(auto& orders : m_mapOrder)
            {
                if(orders.second.nSendTime > 0)
                {
                    if(startTime > orders.second.nSendTime)
                    {
                        startTime = orders.second.nSendTime;
                        rflag++;
                    }
                }
            }

            if(m_mapOrder.size() != 0 || m_mapNewOrder.size() != 0)
                check_orders(unit,startTime,endTime,mflag,rflag,1);

            for(auto itr = m_mapNewOrder.begin() ; itr != m_mapNewOrder.end() ; itr++)
            {
                KF_LOG_INFO(logger,"[on_lws_connection_error] insert failed");
                int errorId=100;
                std::string errorMsg="error,the order was insert failed";
                on_rsp_order_insert(&itr->second.data, itr->second.nRequestID, errorId, errorMsg.c_str());
            }
        }
        m_mapOrder.clear();
        m_mapNewOrder.clear();
        localOrderRefRemoteOrderId.clear();
        std::unique_lock<std::mutex> lck2(*mutex_marketorder_waiting_response);
        remoteOrderIdOrderActionSentTime.clear(); 
        lck2.unlock();
    }
    KF_LOG_ERROR(logger, "TDEngineKumex::on_lws_connection_error. login end.");
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

void TDEngineKumex::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}

bool TDEngineKumex::getToken(Document& d) 
{
    int nTryCount = 0;
    cpr::Response response;
    std::string url;
    do{
        //url = "https://api.kumex.com/api/v1/bullet-private";
        //response = cpr::Post(Url{url.c_str()}, Parameters{}); 
        url="/api/v1/bullet-private";
        response=Post(url,"",account_units[0]);
       
    }while(++nTryCount < max_rest_retry_times && response.status_code != 200);

    KF_LOG_INFO(logger, "[getToken] (url) " << url << " (response.status_code) " << response.status_code <<
                                               " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());

    if(response.status_code != 200)
    {
        KF_LOG_ERROR(logger, "TDEngineKumex::login::getToken Error");
        return false;
    }

    KF_LOG_INFO(logger, "TDEngineKumex::getToken: " << response.text.c_str());

    d.Parse(response.text.c_str());
    return true;
}


bool TDEngineKumex::getServers(Document& d)
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
        KF_LOG_ERROR(logger, "TDEngineKumex::login::getServers Error");
        return false;
    }
    return true;
}

int64_t TDEngineKumex::getMSTime()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  timestamp;
}

std::mutex g_httpMutex;
cpr::Response TDEngineKumex::Get(const std::string& method_url,const std::string& body, AccountUnitKumex& unit)
{
    KF_LOG_INFO(logger,"api_key"<<unit.api_key<<"secret_key:"<<unit.secret_key);
    string url = unit.baseUrl + method_url;
    std::string strTimestamp = std::to_string(getTimestamp());
    std::string strSign = strTimestamp + "GET" + method_url;
    //KF_LOG_INFO(logger, "strSign = " << strSign );
    unsigned char strHmac[EVP_MAX_MD_SIZE]={0};
    hmac_sha256_byte(unit.secret_key.c_str(),strSign.c_str(),strHmac,EVP_MAX_MD_SIZE);
    //KF_LOG_INFO(logger, "strHmac = " << strHmac );
    std::string strSignatrue = base64_encode(strHmac,32);
    cpr::Header mapHeader = cpr::Header{{"KC-API-SIGN",strSignatrue},
                                        {"KC-API-TIMESTAMP",strTimestamp},
                                        {"KC-API-KEY",unit.api_key},
                                        {"KC-API-PASSPHRASE",unit.passphrase}};
    //KF_LOG_INFO(logger, "KC-API-SIGN = " << strSignatrue << ", KC-API-TIMESTAMP = " << strTimestamp << ", KC-API-KEY = " << unit.api_key << ", KC-API-PASSPHRASE = " << unit.passphrase);
    std::unique_lock<std::mutex> lock(g_httpMutex);
    auto response= cpr::Get(Url{url}, Header{mapHeader}, Timeout{10000} );
	//auto response = future.get();
    KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
                                               " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
    return response;
}

cpr::Response TDEngineKumex::Delete(const std::string& method_url,const std::string& body, AccountUnitKumex& unit)
{
    KF_LOG_INFO(logger,"api_key"<<unit.api_key<<"secret_key:"<<unit.secret_key);
    string url = unit.baseUrl + method_url + body;
    std::string strTimestamp = std::to_string(getTimestamp());
    std::string strSign =  strTimestamp + "DELETE" + method_url + body;
    //KF_LOG_INFO(logger, "strSign = " << strSign );
    unsigned char strHmac[EVP_MAX_MD_SIZE]={0};
    hmac_sha256_byte(unit.secret_key.c_str(),strSign.c_str(),strHmac,EVP_MAX_MD_SIZE);
    //KF_LOG_INFO(logger, "strHmac = " << strHmac );
    std::string strSignatrue = base64_encode(strHmac,32);
    cpr::Header mapHeader = cpr::Header{{"KC-API-SIGN",strSignatrue},
                                        {"KC-API-TIMESTAMP",strTimestamp},
                                        {"KC-API-KEY",unit.api_key},
                                        {"KC-API-PASSPHRASE",unit.passphrase}};
    //KF_LOG_INFO(logger, "KC-API-SIGN = " << strSignatrue << ", KC-API-TIMESTAMP = " << strTimestamp << ", KC-API-KEY = " << unit.api_key << ", KC-API-PASSPHRASE = " << unit.passphrase);  
    std::unique_lock<std::mutex> lock(g_httpMutex);
    auto response = cpr::Delete(Url{url},Header{mapHeader}, Timeout{10000} );
	//auto response = future.get();
    KF_LOG_INFO(logger, "[delete] (url) " << url << " (response.status_code) " << response.status_code <<
                                               " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
    return response;
}

cpr::Response TDEngineKumex::Post(const std::string& method_url,const std::string& body, AccountUnitKumex& unit)
{
    KF_LOG_INFO(logger,"api_key"<<unit.api_key<<"secret_key:"<<unit.secret_key);
    std::string strTimestamp = std::to_string(getTimestamp());
    std::string strSign =  strTimestamp + "POST" + method_url + body;
    // KF_LOG_INFO(logger, "strSign = " << strSign );
    unsigned char strHmac[EVP_MAX_MD_SIZE]={0};
    hmac_sha256_byte(unit.secret_key.c_str(),strSign.c_str(),strHmac,EVP_MAX_MD_SIZE);
    std::string strSignatrue = base64_encode(strHmac,32);
    //KF_LOG_INFO(logger, "strHmac = " << strHmac );
    cpr::Header mapHeader = cpr::Header{{"KC-API-SIGN",strSignatrue},
                                        {"KC-API-TIMESTAMP",strTimestamp},
                                        {"KC-API-KEY",unit.api_key},
                                        {"KC-API-PASSPHRASE",unit.passphrase},
                                        {"Content-Type", "application/json"}};

    KF_LOG_INFO(logger, "[post] (TO-SIGN) "<< strSign <<" (SIGNED)"<<strSignatrue << " (TIMESTAMP) " << strTimestamp << " (KEY) " << unit.api_key);
    string url = unit.baseUrl + method_url;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    auto response = cpr::Post(Url{url}, Header{mapHeader},Body{body},Timeout{30000});
	//auto response = future.get();

    KF_LOG_INFO(logger, "[post] (url) " << url <<"(body) "<< body<< " (response.status_code) " << response.status_code <<
                                       " (response.error.message) " << response.error.message <<
                                       " (response.text) " << response.text.c_str());
    return response;
}

void TDEngineKumex::init()
{
    
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineKumex::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineKumex::resize_accounts(int account_num)
{
    account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineKumex::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    string api_key = j_config["APIKey"].get<string>();
    string secret_key = j_config["SecretKey"].get<string>();
    string passphrase = j_config["passphrase"].get<string>();
    string baseUrl = j_config["baseUrl"].get<string>();
    
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

    if(j_config.find("leverage") != j_config.end()) {
        leverage = j_config["leverage"].get<string>();
    }
    KF_LOG_INFO(logger, "[load_account] (leverage)" << leverage);

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

    if(j_config.find("current_td_index") != j_config.end()) {
        m_CurrentTDIndex = j_config["current_td_index"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (current_td_index)" << m_CurrentTDIndex);
    genUniqueKey();

    int thread_pool_size = 0;
    if(j_config.find("thread_pool_size") != j_config.end()) {
        thread_pool_size = j_config["thread_pool_size"].get<int>();
    }
    if(thread_pool_size > 0)
    {
        m_ThreadPoolPtr = new ThreadPool(thread_pool_size);
    }
    KF_LOG_INFO(logger, "[load_account] (thread_pool_size)" << thread_pool_size);
    if(j_config.find("no_response_wait_ms") != j_config.end()) {
        no_response_wait_ms = j_config["no_response_wait_ms"].get<int64_t>();
    }
    no_response_wait_ms = std::max(no_response_wait_ms,(int64_t)500);
    AccountUnitKumex& unit = account_units[idx];
    unit.api_key = api_key;
    unit.secret_key = secret_key;
    unit.passphrase = passphrase;
    unit.baseUrl = baseUrl;

    KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl);

//test rs256
  //  std::string data ="{}";
  //  std::string signature =utils::crypto::rsa256_private_sign(data, g_private_key);
   // std::string sign = base64_encode((unsigned char*)signature.c_str(), signature.size());
    //std::cout  << "[TDEngineKumex] (test rs256-base64-sign)" << sign << std::endl;

    //std::string decodeStr = utils::crypto::rsa256_pub_verify(data,signature, g_public_key);
    //std::cout  << "[TDEngineKumex] (test rs256-verify)" << (decodeStr.empty()?"yes":"no") << std::endl;

    unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    unit.coinPairWhiteList.Debug_print();

    unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
    unit.positionWhiteList.Debug_print();

    //display usage:
    if(unit.coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "TDEngineKumex::load_account: please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }

    //test
    Document json;
    //get_account(unit, json);
    printResponse(json);
    cancel_all_orders(unit, json);
    printResponse(json);
    getPriceIncrement(unit);
    // set up
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, api_key.c_str(), 16);
    strncpy(account.Password, secret_key.c_str(), 21);
    return account;
}

void TDEngineKumex::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitKumex& unit = account_units[idx];
        unit.logged_in = true;
        KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
       // Document doc;
        //
        //std::string requestPath = "/key";
       // const auto response = Get(requestPath,"{}",unit);

      //  getResponse(response.status_code, response.text, response.error.message, doc);

       // if ( !unit.logged_in && doc.HasMember("code"))
        //{
         //   int code = doc["code"].GetInt();
         //   unit.logged_in = (code == 0);
        //}
        //login(timeout_nsec);
        lws_login(unit, 0);
    }
}

   void TDEngineKumex::getPriceIncrement(AccountUnitKumex& unit)
   {
        KF_LOG_INFO(logger,"TDEngineKumex::getPriceIncrement");
        auto& coinPairWhiteList = unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList();
        for(auto& pair : coinPairWhiteList)
        {
            Document json;
            const auto response = Get("/api/v1/contracts/" + pair.second,"",unit);
            json.Parse(response.text.c_str());
            const static std::string strSuccesse = "200000";
            if(json.HasMember("code") && json["code"].GetString() == strSuccesse)
            {
                /*auto& data = json["data"];
                PriceIncrement stPriceIncrement;
                stPriceIncrement.nBaseMinSize = std::round(std::stod(data["baseMinSize"].GetString())* scale_offset);
                stPriceIncrement.nPriceIncrement = std::round(std::stod(data["priceIncrement"].GetString()) * scale_offset);
                stPriceIncrement.nQuoteIncrement = std::round(std::stod(data["quoteIncrement"].GetString()) * scale_offset);
                stPriceIncrement.nBaseIncrement = std::round(std::stod(data["baseIncrement"].GetString()) * scale_offset);
                unit.mapPriceIncrement.insert(std::make_pair(pair.first,stPriceIncrement));

                 KF_LOG_INFO(logger, "[getPriceIncrement] (BaseMinSize )" << stPriceIncrement.nBaseMinSize << "(PriceIncrement)" << stPriceIncrement.nPriceIncrement
                                    << "(QuoteIncrement)" << stPriceIncrement.nQuoteIncrement << "(BseIncrement)" << stPriceIncrement.nBaseIncrement);
*/
            }
        }
   }
void TDEngineKumex::check_orders(AccountUnitKumex& unit, int64_t startTime, int64_t endTime, int64_t mflag, int64_t rflag, int64_t flag)
{
    int64_t currentPage = 1 ;
    int64_t totalPage = currentPage ;
    int64_t new_mflag = 0;
    int64_t new_rflag = 0;
    
    while((new_mflag != mflag || new_rflag != rflag) && currentPage <= totalPage)
    {
        std::string url ;
        if(flag == 1)
            url = "/api/v1/orders?status=active";
        else
            url = "/api/v1/orders?status=done";
        url += "&currentPage=" + std::to_string(currentPage++);
        url += "&pageSize=500&startAt=" + std::to_string(startTime-3000);
        url += "&endAt="+std::to_string(endTime+60000);
        Document json;
        const auto response = Get(url.c_str(),"",unit);
        json.Parse(response.text.c_str());
        const static std::string strSuccesse = "200000";
        if(!json.HasParseError() && json.IsObject() &&json.HasMember("code") && json["code"].GetString() == strSuccesse && json.HasMember("data"))
        {
            auto& data = json["data"];
            totalPage = data["totalPage"].GetInt();
            if(data.HasMember("items") && data["items"].IsArray())
            {
                int size = data["items"].Size();
                KF_LOG_ERROR(logger,"size:"<<size);
                for(int i =0;i < size;++i)
                {
                    auto& item = data["items"][i];
                    if(item.HasMember("clientOid") && item.HasMember("id") && item["clientOid"].IsString())
                    {
                        int64_t createdAt = item["createdAt"].GetInt64();
                        std::string strtime = std::to_string(createdAt);
                        std::string strClientId = item["clientOid"].GetString();
                        std::string strOrderId = item["id"].GetString();
                        bool Status = item["isActive"].GetBool();
                        bool cancelExist = item["cancelExist"].GetBool();
                        std::lock_guard<std::mutex> lck(*m_mutexOrder);
                        auto its = m_mapNewOrder.find(strClientId);
                        if(its != m_mapNewOrder.end())
                        {
                            its->second.OrderStatus = LF_CHAR_NotTouched;
                            m_mapOrder.insert(std::make_pair(strOrderId,its->second));
                            localOrderRefRemoteOrderId.insert(std::make_pair(its->second.OrderRef,strOrderId));
                            onOrder(its->second);
                            m_mapNewOrder.erase(its);
                        }
                       
                        auto it = m_mapOrder.find(strOrderId);
                        if(it != m_mapOrder.end())
                        {   
                            if(Status)
                            {
                                std::unique_lock<std::mutex> lck1(*mutex_marketorder_waiting_response); 
                                auto its = remoteOrderIdOrderActionSentTime.find(strOrderId);
                                if(its != remoteOrderIdOrderActionSentTime.end() && endTime - its->second.sentNameTime > no_response_wait_ms)
                                {
                                    KF_LOG_INFO(logger,"order is active");
                                    KF_LOG_INFO(logger,"cancel failed");
                                    int errorId=100;
                                    std::string errorMsg="error,the order was cancel failed";
                                    on_rsp_order_action(&its->second.data, its->second.requestId, errorId, errorMsg.c_str());
                                    remoteOrderIdOrderActionSentTime.erase(its);
                                    m_mapOrder.erase(strOrderId);
                                    new_rflag++;
                                }
                                lck1.unlock();
                            }
                            else
                            {
                                int64_t nSize = std::round((item["dealSize"].GetInt64()) * scale_offset);
                                KF_LOG_INFO(logger,"nSize="<<nSize);
                                //it->second.VolumeTraded += nSize;

                                if(!cancelExist && nSize>0)
                                {
                                    KF_LOG_INFO(logger,"trade in");
                                    it->second.VolumeTraded += nSize;
                                    if(it->second.VolumeTraded == it->second.nVolume){                                    
                                        it->second.OrderStatus = LF_CHAR_AllTraded;
                                    }
                                    else{
                                        it->second.OrderStatus = LF_CHAR_PartTradedQueueing;
                                    }
                                    /*std::string url1 = "/api/v1/fills?orderId="+strOrderId;
                                    Document doc;
                                    const auto response1 = Get(url1.c_str(),"",unit);
                                    doc.Parse(response1.text.c_str());
                                    if(!doc.HasParseError() && doc.IsObject() &&doc.HasMember("code") && doc["code"].GetString() == strSuccesse)
                                    {
                                        Value &node = doc["data"]["items"];
                                        int len = node.Size();
                                        for(int j=0;j<len;j++){
                                            int64_t price = (node.GetArray()[j]["price"].GetInt64())*scale_offset;
                                            int64_t volume = (node.GetArray()[j]["size"].GetInt64())*scale_offset;
                                            onTrade(it->second,volume,price,strOrderId,strtime);
                                        }
                                    }*/
                                    onTrade(it->second,nSize,it->second.nPrice,strOrderId,strtime);
                                }
                                else
                                {
                                    it->second.OrderStatus = LF_CHAR_Canceled;   
                                }

                                onOrder(it->second);
                                auto it2 = localOrderRefRemoteOrderId.find(it->second.OrderRef);
                                if(it2 != localOrderRefRemoteOrderId.end())
                                {
                                    KF_LOG_INFO(logger,"erase local");
                                    localOrderRefRemoteOrderId.erase(it2);
                                }
                       
                                std::unique_lock<std::mutex> lck2(*mutex_marketorder_waiting_response); 
                                auto rit = remoteOrderIdOrderActionSentTime.find(strOrderId);
                                if(rit != remoteOrderIdOrderActionSentTime.end())
                                {
                                    remoteOrderIdOrderActionSentTime.erase(rit);
                                    new_rflag++;
                                }
                                lck2.unlock();
                                m_mapOrder.erase(it);
                            }
                        }
                    }
                }
            }
        }
    }
}
void TDEngineKumex::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[login]");
    //lws_login(timeout_nsec);
}

bool TDEngineKumex::lws_login(AccountUnitKumex& unit, long timeout_nsec)
{
    KF_LOG_INFO(logger,"[login] login,threadid="<<std::this_thread::get_id());
    KF_LOG_INFO(logger, "TDEngineKumex::login:");

    global_md = this;

    Document d;
    if(!getToken(d))
    {
        return false;
    }
    if(!getServers(d))
   {
       return false;
   }
    m_isSubL3 = false;
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
	KF_LOG_INFO(logger, "TDEngineKumex::login: context created.");


	if (context == NULL) {
		KF_LOG_ERROR(logger, "TDEngineKumex::login: context is NULL. return");
		return false;
	}
    if(nullptr == rest_thread && isRunning)
        rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineKumex::loopwebsocket, this)));
	// Set up the client creation info
    auto& stServerInfo = m_vstServerInfos.front();
    std::string strAddress = stServerInfo.strEndpoint;
    size_t nAddressEndPos = strAddress.find_last_of('/');
    std::string strPath = strAddress.substr(nAddressEndPos);
    strPath += "?token=";
    strPath += m_strToken;
    strPath += "&connectId=" +  getId();// +"&acceptUserMessage=true";
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
    std::unique_lock<std::mutex> web_socket_connect_mutex(* mutex_web_connect);
    clientConnectInfo.pwsi = &unit.webSocketConn;

    KF_LOG_INFO(logger, "TDEngineKumex::login: address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);

	unit.webSocketConn = lws_client_connect_via_info(&clientConnectInfo);
	if (unit.webSocketConn == NULL) {
		KF_LOG_ERROR(logger, "TDEngineKumex::login: wsi create error.");
		return false;
	}
	KF_LOG_INFO(logger, "TDEngineKumex::login: wsi create success.");
    unit.is_connecting = true;
    web_socket_connect_mutex.unlock();
    return true;
    //connect(timeout_nsec);
}

void TDEngineKumex::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineKumex::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineKumex::is_logged_in() const
{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineKumex::is_connected() const
{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}


std::string TDEngineKumex::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "buy";
    } else if (LF_CHAR_Sell == input) {
        return "sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineKumex::GetDirection(std::string input) {
    if ("buy" == input) {
        return LF_CHAR_Buy;
    } else if ("sell" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineKumex::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "market";
    } else {
        return "";
    }
}

LfOrderPriceTypeType TDEngineKumex::GetPriceType(std::string input) {
    if ("limit" == input) {
        return LF_CHAR_LimitPrice;
    } else if ("market" == input) {
        return LF_CHAR_AnyPrice;
    } else {
        return '0';
    }
}
//订单状态，﻿open（未成交）、filled（已完成）、canceled（已撤销）、cancel（撤销中）、partially-filled（部分成交）
LfOrderStatusType TDEngineKumex::GetOrderStatus(bool isCancel,int64_t nSize,int64_t nDealSize) {
    
    if(isCancel)
    {
          return LF_CHAR_Canceled; 
    }
    if(nDealSize == 0)
    {
        return LF_CHAR_NotTouched;
    }
    if(nSize > nDealSize)
   {
        return  LF_CHAR_PartTradedQueueing;
   }
    return LF_CHAR_AllTraded;
}

/**
 * req functions
 */
void TDEngineKumex::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitKumex& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key << " (InstrumentID) " << data->InstrumentID);

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
    //get_account(unit, d);
    KF_LOG_INFO(logger, "[req_investor_position] (get_account)" );
    if(d.IsObject() && d.HasMember("code"))
    {
         KF_LOG_INFO(logger, "[req_investor_position] (getcode)" );
        errorId =  std::round(std::stod(d["code"].GetString()));
         KF_LOG_INFO(logger, "[req_investor_position] (errorId)" << errorId);
        if(errorId != 200000) {
            if (d.HasMember("msg") && d["msg"].IsString()) {
                errorMsg = d["msg"].GetString();
            }
            KF_LOG_ERROR(logger, "[req_investor_position] failed!" << " (rid)" << requestId << " (errorId)" << errorId
                                                                   << " (errorMsg) " << errorMsg);
            raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_KUMEX, 1, requestId, errorId, errorMsg.c_str());
        }
    }
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_KUMEX, 1, requestId);
    KF_LOG_INFO(logger,"send_writer");
    
    std::map<std::string,LFRspPositionField> tmp_map;
    /*
    if(!d.HasParseError() && d.HasMember("data"))
    {
        auto& jisonData = d["data"];
        size_t len = jisonData.Size();
        KF_LOG_INFO(logger, "[req_investor_position] (accounts.length)" << len);
        for(size_t i = 0; i < len; i++)
        {
            std::string symbol = jisonData.GetArray()[i]["currency"].GetString();
             KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol);
            std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
             KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (ticker) " << ticker);
            if(ticker.length() > 0) {            
                uint64_t nPosition = std::round(std::stod(jisonData.GetArray()[i]["balance"].GetString()) * scale_offset);   
                auto it = tmp_map.find(ticker);
                if(it == tmp_map.end())
                {
                     it = tmp_map.insert(std::make_pair(ticker,pos)).first;
                     strncpy(it->second.InstrumentID, ticker.c_str(), 31);      
                }
                it->second.Position += nPosition;
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol << " (position) " << it->second.Position);
            }
        }
    }*/

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

void TDEngineKumex::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineKumex::dealPriceVolume(AccountUnitKumex& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,double& dDealPrice,double& dDealVolume)
{
        KF_LOG_DEBUG(logger, "[dealPriceVolume] (symbol)" << symbol);
        auto it = unit.mapPriceIncrement.find(symbol);
        if(it == unit.mapPriceIncrement.end())
        {
                  KF_LOG_INFO(logger, "[dealPriceVolume] symbol not find :" << symbol);
                  /*dDealVolume = 0;
                  return ;*/
                  dDealPrice = nPrice/scale_offset;
                  dDealVolume = nVolume/scale_offset;
        }
        else
        {
            if(it->second.nBaseMinSize > nVolume)
            {
                KF_LOG_INFO(logger, "[dealPriceVolume] (Volume) "  << nVolume  << " <  (BaseMinSize)  "  << it->second.nBaseMinSize << " (symbol)" << symbol);
                dDealVolume = 0;
                return ;
            }
            int64_t nDealVolume =  it->second.nBaseIncrement  > 0 ? nVolume / it->second.nBaseIncrement * it->second.nBaseIncrement : nVolume;
            int64_t nDealPrice = it->second.nPriceIncrement > 0 ? nPrice / it->second.nPriceIncrement * it->second.nPriceIncrement : nPrice;
            dDealVolume = nDealVolume * 1.0 / scale_offset;
            dDealPrice = nDealPrice * 1.0 / scale_offset;
            char strVolume[64];
            char strPrice[64];
            sprintf(strVolume,"%.8lf",dDealVolume + 0.0000000001);
            sprintf(strPrice,"%.8lf",dDealPrice + 0.0000000001);
            dDealVolume = std::stod(strVolume);
            dDealPrice = std::stod(strPrice);
            KF_LOG_INFO(logger, "[dealPriceVolume]  (symbol)" << symbol << " (Volume)" << nVolume << " (Price)" << nPrice 
                << " (FixedVolume)" << strVolume << " (FixedPrice)" << strPrice);
        }
         
}

void TDEngineKumex::handle_order_insert(AccountUnitKumex& unit,const LFInputOrderField data,int requestId,const std::string& ticker)
{
    KF_LOG_DEBUG(logger, "[handle_order_insert]" << " (current thread)" << std::this_thread::get_id());
    int errorId = 0;
    std::string errorMsg = "";

    double funds = 0;
    Document d;

    double fixedPrice = 0;
    double fixedVolume = 0;
    dealPriceVolume(unit,data.InstrumentID,data.LimitPrice,data.Volume,fixedPrice,fixedVolume);
    KF_LOG_INFO(logger,"fixedPrice="<<fixedPrice<<"fixedVolume="<<fixedVolume);
    
    if(fixedVolume == 0)
    {
        KF_LOG_DEBUG(logger, "[req_order_insert] fixed Volume error" << ticker);
        errorId = 200;
        errorMsg = data.InstrumentID;
        errorMsg += " : quote less than baseMinSize";
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_KUMEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    std::string strClientId = "";//genClinetid(data->OrderRef);
    PendingOrderStatus stPendingOrderStatus;
    stPendingOrderStatus.nVolume = std::round(fixedVolume*scale_offset);
    stPendingOrderStatus.nPrice = std::round(fixedPrice*scale_offset);
    strncpy(stPendingOrderStatus.InstrumentID, data.InstrumentID, sizeof(stPendingOrderStatus.InstrumentID));
    strncpy(stPendingOrderStatus.OrderRef, data.OrderRef, sizeof(stPendingOrderStatus.OrderRef));
    stPendingOrderStatus.strUserID = unit.api_key;
    stPendingOrderStatus.OrderStatus = LF_CHAR_Unknown;
    stPendingOrderStatus.VolumeTraded = 0;
    stPendingOrderStatus.Direction = data.Direction;
    stPendingOrderStatus.OrderPriceType = data.OrderPriceType;
    
    stPendingOrderStatus.nRequestID = requestId;
    stPendingOrderStatus.strClientId = strClientId;
    memcpy(&stPendingOrderStatus.data, &data, sizeof(LFInputOrderField));

    if(!unit.is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        return;
    }
    send_order(unit,stPendingOrderStatus, ticker.c_str(), fixedVolume, fixedPrice,is_post_only(&data),d);
    
    if(!d.IsObject())
    {
        errorId = 100;
        errorMsg = "send_order http response has parse error or is not json. please check the log";
        KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
                                                                           errorId << " (errorMsg) " << errorMsg);
    } else  if(d.HasMember("code"))
    {
        int code =std::round(std::stod(d["code"].GetString()));
        if(code == 200000) {
            //if send successful and the exchange has received ok, then add to  pending query order list
            std::string remoteOrderId = d["data"]["orderId"].GetString();
            //fix defect of use the old value
            KF_LOG_INFO(logger, "[req_order_insert] after send  (rid)" << requestId << " (OrderRef) " <<
                                                                       data.OrderRef << " (remoteOrderId) "
                                                                       << remoteOrderId);      
                                                                       
            std::lock_guard<std::mutex> lck(*m_mutexOrder);   
            auto it = m_mapNewOrder.find(stPendingOrderStatus.strClientId);
            if(it != m_mapNewOrder.end())
            {//websocket信息尚未到达
                it->second.remoteOrderId = remoteOrderId;
                it->second.nSendTime = getTimestamp();//记录当前时间
                KF_LOG_INFO(logger,"zaftime:"<<it->second.nSendTime);
            }
            
            //success, only record raw data
            raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_KUMEX, 1,requestId, errorId, errorMsg.c_str());
            KF_LOG_DEBUG(logger, "[req_order_insert] success" );
            
            KF_LOG_INFO(logger,"[req_order_insert] data.OrderPriceType="<<data.OrderPriceType);          
            return;

        }else {
            errorId = code;
            if(d.HasMember("msg") && d["msg"].IsString())
            {
                errorMsg = d["msg"].GetString();
            }
            KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
    }
    
    if(errorId != 0)
    {
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_KUMEX, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineKumex::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitKumex& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_KUMEX, 1/*ISLAST*/, requestId);
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
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_KUMEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);

    if(nullptr == m_ThreadPoolPtr)
    {
        handle_order_insert(unit,*data,requestId,ticker);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineKumex::handle_order_insert,this,unit,*data,requestId,ticker));
    }
}

//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
void TDEngineKumex::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId, int64_t sentNameTime)
{
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;
    newOrderActionSent.nSendTime = sentNameTime;
    newOrderActionSent.sentNameTime = getTimestamp();
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    KF_LOG_INFO(logger,"zafinsert:"<<remoteOrderId);
    remoteOrderIdOrderActionSentTime[remoteOrderId] = newOrderActionSent;
}

void TDEngineKumex::handle_order_action(AccountUnitKumex& unit,const LFOrderActionField data, int requestId,const std::string& ticker)
{
    int errorId = 0;
    std::string errorMsg = "";
    std::unique_lock<std::mutex> lck(*m_mutexOrder);
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
        raw_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_KUMEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    } else {
        remoteOrderId = itr->second;
        KF_LOG_DEBUG(logger, "[req_order_action] found in localOrderRefRemoteOrderId map (orderRef) "
                << data.OrderRef << " (remoteOrderId) " << remoteOrderId);
    }
    int64_t sentNameTime = getTimestamp();
    auto it = m_mapOrder.begin();
    if(it != m_mapOrder.end())
        sentNameTime = it->second.nSendTime;
    lck.unlock();
    Document d;
    if(!unit.is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        return;
    }
    cancel_order(unit, ticker, remoteOrderId, d);

    std::string strSuccessCode =  "200000";
    if(!d.HasParseError() && d.HasMember("code") && strSuccessCode != d["code"].GetString()) {
        errorId = std::stoi(d["code"].GetString());
        if(d.HasMember("msg") && d["msg"].IsString())
        {
            errorMsg = d["msg"].GetString();
        }
        KF_LOG_ERROR(logger, "[req_order_action] cancel_order failed!" << " (rid)" << requestId
                                                                       << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    /*else{
        auto iter = m_mapOrder.find(remoteOrderId);
        if(iter != m_mapOrder.end()){
            iter->second.OrderStatus = LF_CHAR_Canceled;
            onOrder(iter->second);
            m_mapOrder.erase(iter);
        }
    }*/

    if(errorId==0)
    {
        addRemoteOrderIdOrderActionSentTime(&data,requestId,remoteOrderId,sentNameTime);
    }

    if(errorId != 0)
    {
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_KUMEX, 1, requestId, errorId, errorMsg.c_str());
    } 
}
void TDEngineKumex::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitKumex& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_KUMEX, 1, requestId);

    int errorId = 0;
    std::string errorMsg = "";
    on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_KUMEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);
    if(nullptr == m_ThreadPoolPtr)
    {
        handle_order_action(unit,*data,requestId,ticker);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineKumex::handle_order_action,this,unit,*data,requestId,ticker));
    }
    
}



void TDEngineKumex::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineKumex::loop");
    if(nullptr == rest_thread)
        rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineKumex::loopwebsocket, this)));
    KF_LOG_INFO(logger, "[set_reader_thread] loopwebsocket threadid="<<std::this_thread::get_id());

    KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineKumex::loopOrderActionNoResponseTimeOut");
    orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineKumex::loopOrderActionNoResponseTimeOut, this)));
    KF_LOG_INFO(logger, "[set_reader_thread] loopOrderActionNoResponseTimeOut threadid="<<std::this_thread::get_id());
}

void TDEngineKumex::loopwebsocket()
{
        KF_LOG_INFO(logger, "[loopwebsocket] loopwebsocket threadid="<<std::this_thread::get_id());
        time_t nLastTime = time(0);

        while(isRunning && context != NULL)
        {
             time_t nNowTime = time(0);
            if(m_isPong && (nNowTime - nLastTime>= 30))
            {
                m_isPong = false;
                nLastTime = nNowTime;
                KF_LOG_INFO(logger, "TDEngineKumex::loopwebsocket: last time = " <<  nLastTime << ",now time = " << nNowTime << ",m_isPong = " << m_isPong);
                m_shouldPing = true;
                lws_callback_on_writable(m_conn);  
            }
			lws_service(context, rest_get_interval_ms );
            if(flag.load())
            {
                KF_LOG_INFO(logger,"[loopwebsocket] flag=true");
                flag.store(false);  
                m_isSubL3=false;
                ubsub=false;              
                lws_callback_on_writable(m_conn); 
            }
		}
        KF_LOG_INFO(logger, "[loopwebsocket] loopwebsocket =end");
}


//***********************************************************************************************
//***********************************************************************************************

void TDEngineKumex::loopOrderActionNoResponseTimeOut()
{
   KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] loopOrderActionNoResponseTimeOut threadid="<<std::this_thread::get_id());
    KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        for(auto& unit:account_units)
        {
            
            int64_t endTime = getTimestamp();
            int64_t startTime = endTime;
            int64_t mflag = 0;
            int64_t rflag = mflag ;
            std::unique_lock<std::mutex> lck(*m_mutexOrder);  
            for(auto& order : m_mapNewOrder)
            {
                if(order.second.nSendTime > 0)
                {
                    if(startTime > order.second.nSendTime)
                    {
                        startTime = order.second.nSendTime;
                    }
                    if(endTime - order.second.nSendTime > no_response_wait_ms)
                        mflag++;
                }
            }
            lck.unlock();

            std::unique_lock<std::mutex> lck1(*mutex_orderaction_waiting_response);
            for(auto& orders : remoteOrderIdOrderActionSentTime)
            {
                if(orders.second.sentNameTime > 0)
                {
                    if(startTime > orders.second.sentNameTime)
                    {
                        startTime = orders.second.sentNameTime;
                    } 
                    if(endTime - orders.second.sentNameTime > no_response_wait_ms)
                        rflag++;  
                }
            }
            lck1.unlock();

            if(endTime - startTime > no_response_wait_ms)
            {
                KF_LOG_INFO(logger,"check1");
                check_orders(unit,startTime,endTime,mflag,rflag,1);

                startTime = getTimestamp();
                mflag = rflag = 0;
                lck.lock();
                for(auto& order : m_mapNewOrder)
                {
                    if(order.second.nSendTime > 0)
                    {
                        if(startTime > order.second.nSendTime)
                        {
                            startTime = order.second.nSendTime;
                        }
                    }
                    if(endTime - order.second.nSendTime > no_response_wait_ms)
                    {
                        mflag++;
                    }
                }
                lck.unlock();

                lck1.lock();
                for(auto& orders : remoteOrderIdOrderActionSentTime)
                {
                    if(orders.second.sentNameTime > 0)
                    {
                        if(startTime > orders.second.sentNameTime)
                        {
                            startTime = orders.second.sentNameTime;
                        } 
                        if(endTime - orders.second.sentNameTime > no_response_wait_ms)
                            rflag++;
                    }
                }
                lck1.unlock();

                if(endTime - startTime > no_response_wait_ms)
                {
                    KF_LOG_INFO(logger,"check2");
                    check_orders(unit,startTime,endTime,mflag,rflag,0);
                } 

                lck.lock();
                auto order = m_mapNewOrder.begin();
                while(order != m_mapNewOrder.end())
                {
                    if(endTime - order->second.nSendTime > no_response_wait_ms && order->second.nSendTime != 0)
                    {
                        KF_LOG_INFO(logger,"[req_order_insert_error] insert failed");
                        int errorId=100;
                        std::string errorMsg="error,the order was insert failed";
                        on_rsp_order_insert(&order->second.data, order->second.nRequestID, errorId, errorMsg.c_str());
                        order = m_mapNewOrder.erase(order);
                    }
                    else
                        order++;
                }
                lck.unlock();  
            } 
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void TDEngineKumex::printResponse(const Document& d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
}

void TDEngineKumex::getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json)
{
    if(http_status_code >= HTTP_RESPONSE_OK && http_status_code <= 299)
    {
        json.Parse(responseText.c_str());
    } else if(http_status_code == 0)
    {
        json.SetObject();
        Document::AllocatorType& allocator = json.GetAllocator();
        //int errorId = 1;
        json.AddMember("code", Document::StringRefType("1"), allocator);
        KF_LOG_INFO(logger, "[getResponse] (errorMsg)" << errorMsg);
        rapidjson::Value val;
        val.SetString(errorMsg.c_str(), errorMsg.length(), allocator);
        json.AddMember("msg", val, allocator);
    } else
    {   
        Document d;
        d.Parse(responseText.c_str());
        if(!d.HasParseError() && d.IsObject() && d.HasMember("code") && d.HasMember("msg"))
        {
            json.Parse(responseText.c_str());
        }
        else
        {
            json.SetObject();
            Document::AllocatorType& allocator = json.GetAllocator();
            std::string strErrorID = std::to_string(http_status_code);
            json.AddMember("code", Document::StringRefType(strErrorID.c_str()), allocator);

            rapidjson::Value val;
            if(errorMsg.size() > 0)
            {
                val.SetString(errorMsg.c_str(), errorMsg.length(), allocator);
            }
            else if(responseText.size() > 0)
            {
                val.SetString(responseText.c_str(), responseText.length(), allocator);
            }
            else
            {
                val.SetString("unknown error");
            }
            
            json.AddMember("msg", val, allocator);
        }
        
    }
}

std::string TDEngineKumex::construct_request_body(const AccountUnitKumex& unit,const  std::string& data,bool isget)
{
    std::string pay_load = R"({"uid":")" + unit.api_key + R"(","data":)" + data + R"(})";
    std::string request_body = utils::crypto::jwt_create(pay_load,unit.secret_key);
    //std::cout  << "[construct_request_body] (request_body)" << request_body << std::endl;
    return  isget ? "user_jwt="+request_body:R"({"user_jwt":")"+request_body+"\"}";
}


void TDEngineKumex::get_account(AccountUnitKumex& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_account]");

    std::string requestPath = "/api/v1/accounts";
    //std::string queryString= construct_request_body(unit,"{}");
    //RkTgU1lne1aWSBnC171j0eJe__fILSclRpUJ7SWDDulWd4QvLa0-WVRTeyloJOsjyUtduuF0K0SdkYqXR-ibuULqXEDGCGSHSed8WaNtHpvf-AyCI-JKucLH7bgQxT1yPtrJC6W31W5dQ2Spp3IEpXFS49pMD3FRFeHF4HAImo9VlPUM_bP-1kZt0l9RbzWjxVtaYbx3L8msXXyr_wqacNnIV6X9m8eie_DqZHYzGrN_25PfAFgKmghfpL-jmu53kgSyTw5v-rfZRP9VMAuryRIMvOf9LBuMaxcuFn7PjVJx8F7fcEPBCd0roMTLKhHjFidi6QxZNUO1WKSkoSbRxA
            ;//construct_request_body(unit, "{}");

    //string url = unit.baseUrl + requestPath + queryString;

    const auto response = Get(requestPath,"{}",unit);
    
    json.Parse(response.text.c_str());
    return ;
}

/*
 * {
    "market": "vetusd",
    "side": "buy",
    "volume": 0.25,
    "price": 10,
    "ord_type": "limit"
}
 * */
std::string TDEngineKumex::createInsertOrdertring(const char *code,const char*  strClientId,
                                                    const char *side, const char *type, double& size, double& price,bool isPostOnly)
{
    KF_LOG_INFO(logger, "[TDEngineKumex::createInsertOrdertring]:(price)"<<price << "(volume)" << size);
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("clientOid");
    writer.String(strClientId);

    writer.Key("side");
    writer.String(side);
    writer.Key("symbol");
    writer.String(code);
    writer.Key("type");
    writer.String(type);
    writer.Key("leverage");
    writer.String(leverage.c_str());
    //writer.Key("stp");
    //writer.String("CO");
    
    int iprice = (int)price;
    int isize = (int)size;
    writer.Key("price");
    writer.Int(iprice);
    writer.Key("size");
    writer.Int(isize);
    if(isPostOnly)
    {
        writer.Key("postOnly");
        writer.Bool(isPostOnly);
    }
    writer.EndObject();
    /*std::stringstream ss;
    ss.setf(std::ios::fixed);
    ss.precision(8);
    std::string str = s.GetString();
    str.pop_back();
    ss << str;
     if(strcmp("market",type) != 0)
    {
        ss << ",\"price\":" << price;
    }
    ss << ",\"size\":" << size << "}";
    str = ss.str();
    KF_LOG_INFO(logger, "[TDEngineKumex::createInsertOrdertring]:" << str);
    return str;*/
    KF_LOG_INFO(logger, "[TDEngineKumex::createInsertOrdertring]:" << s.GetString());
    return s.GetString();
}

void TDEngineKumex::send_order(AccountUnitKumex& unit,PendingOrderStatus& stPendingOrderStatus,const char* code,double size,double price, bool isPostOnly,Document& json)
{
    KF_LOG_INFO(logger, "[send_order]");
    auto strSide = GetSide(stPendingOrderStatus.Direction);
    auto strType =GetType(stPendingOrderStatus.OrderPriceType);
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        should_retry = false;

        std::string requestPath = "/api/v1/orders";
        std::string newClientId = genClinetid(stPendingOrderStatus.OrderRef);
        stPendingOrderStatus.strClientId = newClientId;
        std::unique_lock<std::mutex> lck(*m_mutexOrder);
        m_mapNewOrder.insert(std::make_pair(newClientId,stPendingOrderStatus));
        lck.unlock();
        response = Post(requestPath,createInsertOrdertring(code, newClientId.c_str(),strSide.c_str(), strType.c_str(), size, price,isPostOnly),unit);

        KF_LOG_INFO(logger, "[send_order] (url) " << requestPath << " (response.status_code) " << response.status_code <<
                                                  " (response.error.message) " << response.error.message <<
                                                  " (response.text) " << response.text.c_str() << " (retry_times)" << retry_times);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);
        //has error and find the 'error setting certificate verify locations' error, should retry
        if(shouldRetry(json)) {
            should_retry = true;
            retry_times++;
            std::unique_lock<std::mutex> lck(*m_mutexOrder);
            m_mapNewOrder.erase(newClientId);
            lck.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }
    } while(should_retry && retry_times < max_rest_retry_times);



    KF_LOG_INFO(logger, "[send_order] out_retry (response.status_code) " << response.status_code <<
                                                                         " (response.error.message) " << response.error.message <<
                                                                         " (response.text) " << response.text.c_str() );

    //getResponse(response.status_code, response.text, response.error.message, json);
}

bool TDEngineKumex::shouldRetry(Document& doc)
{ 
    bool ret = false;
    std::string strCode="null";
    bool isObJect = doc.IsObject();
    if(isObJect)
    {
        
        if(doc.HasMember("code") && doc["code"].IsString())
        {
            strCode = doc["code"].GetString();
        } 
        if(strCode != "200000")
        {
            ret = true;
        }
    }
    else
    {
        ret = true;
    }
    
    KF_LOG_INFO(logger, "[shouldRetry] isObJect = " << isObJect << ",strCode = " << strCode);

    return ret;
}

void TDEngineKumex::cancel_all_orders(AccountUnitKumex& unit, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_all_orders]");

    std::string requestPath = "/api/v1/orders";
    //std::string queryString= "?user_jwt=RkTgU1lne1aWSBnC171j0eJe__fILSclRpUJ7SWDDulWd4QvLa0-WVRTeyloJOsjyUtduuF0K0SdkYqXR-ibuULqXEDGCGSHSed8WaNtHpvf-AyCI-JKucLH7bgQxT1yPtrJC6W31W5dQ2Spp3IEpXFS49pMD3FRFeHF4HAImo9VlPUM_bP-1kZt0l9RbzWjxVtaYbx3L8msXXyr_wqacNnIV6X9m8eie_DqZHYzGrN_25PfAFgKmghfpL-jmu53kgSyTw5v-rfZRP9VMAuryRIMvOf9LBuMaxcuFn7PjVJx8F7fcEPBCd0roMTLKhHjFidi6QxZNUO1WKSkoSbRxA";//construct_request_body(unit, "{}");

    auto response = Delete(requestPath,"",unit);

    getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineKumex::cancel_order(AccountUnitKumex& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_order]");

    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        should_retry = false;

        std::string requestPath = "/api/v1/orders/" + orderId;
        //std::string queryString= construct_request_body(unit, "{\"id\":" + orderId + "}");
        response = Delete(requestPath,"",unit);

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



std::string TDEngineKumex::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


inline int64_t TDEngineKumex::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void TDEngineKumex::genUniqueKey()
{
    struct tm cur_time = getCurLocalTime();
    //SSMMHHDDN
    char key[11]{0};
    snprintf((char*)key, 11, "%02d%02d%02d%02d%02d", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_CurrentTDIndex);
    m_uniqueKey = key;
}

//clientid =  m_uniqueKey+orderRef
std::atomic<uint64_t> nIndex{0};
std::string TDEngineKumex::genClinetid(const std::string &orderRef)
{
    //static int nIndex = 0;
    return m_uniqueKey + orderRef + std::to_string(nIndex++);
}


#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

BOOST_PYTHON_MODULE(libkumextd)
{
    using namespace boost::python;
    class_<TDEngineKumex, boost::shared_ptr<TDEngineKumex> >("Engine")
            .def(init<>())
            .def("init", &TDEngineKumex::initialize)
            .def("start", &TDEngineKumex::start)
            .def("stop", &TDEngineKumex::stop)
            .def("logout", &TDEngineKumex::logout)
            .def("wait_for_stop", &TDEngineKumex::wait_for_stop);
}

