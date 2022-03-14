#include "TDEngineKuCoin.h"
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

TDEngineKuCoin::TDEngineKuCoin(): ITDEngine(SOURCE_KUCOIN)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.KuCoin");
    KF_LOG_INFO(logger, "[TDEngineKuCoin]");

    m_mutexOrder = new std::mutex();
    mutex_order_and_trade = new std::mutex();
    mutex_response_order_status = new std::mutex();
    mutex_orderaction_waiting_response = new std::mutex();
    mutex_marketorder_waiting_response = new std::mutex();
    mutex_web_connect=new std::mutex();
    mutex_journal_writer =new std::mutex();
    m_ThreadPoolPtr = nullptr;
}

TDEngineKuCoin::~TDEngineKuCoin()
{
    if(m_mutexOrder != nullptr) delete m_mutexOrder;
    if(mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if(mutex_response_order_status != nullptr) delete mutex_response_order_status;
    if(mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
    if(mutex_marketorder_waiting_response != nullptr) delete mutex_marketorder_waiting_response;
    if(m_ThreadPoolPtr != nullptr) delete m_ThreadPoolPtr;
    if(mutex_web_connect!=nullptr) delete mutex_web_connect;
    if(mutex_journal_writer !=nullptr) delete mutex_journal_writer;
}

static TDEngineKuCoin* global_md = nullptr;

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

std::string TDEngineKuCoin::getId()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  std::to_string(timestamp);
}
AccountUnitKuCoin& TDEngineKuCoin::findAccountUnitKucoinByWebsocketConn(struct lws * websocketConn){

    for (size_t idx = 0; idx < account_units.size(); idx++) {
        AccountUnitKuCoin &unit = account_units[idx];

        std::unique_lock<std::mutex> web_socket_connect_mutex(* mutex_web_connect);
        if(unit.webSocketConn == websocketConn) {
        KF_LOG_INFO(logger, "findAccountUnitHuobiByWebsocketConn unit " << unit.webSocketConn << ","<< idx);
        KF_LOG_INFO(logger,"api_key="<<unit.api_key);
        return unit;
        }

    }
    return account_units[0];
}
 void TDEngineKuCoin::onPong(struct lws* conn)
 {
     Ping(conn);
 }

 void TDEngineKuCoin::Ping(struct lws* conn)
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
    KF_LOG_INFO(logger, "TDEngineKuCoin::lws_write_ping: " << strPing.c_str() << " ,len = " << length);
    strncpy((char *)msg+LWS_PRE, strPing.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
 }

 void TDEngineKuCoin::onOrder(const PendingOrderStatus& stPendingOrderStatus)
 {
            LFRtnOrderField rtn_order;
            memset(&rtn_order, 0, sizeof(LFRtnOrderField));
            rtn_order.RequestID = stPendingOrderStatus.nRequestID;
            rtn_order.OrderStatus = stPendingOrderStatus.OrderStatus;
            rtn_order.VolumeTraded = stPendingOrderStatus.VolumeTraded;
            rtn_order.OffsetFlag = stPendingOrderStatus.OffsetFlag;
            //first send onRtnOrder about the status change or VolumeTraded change
            strcpy(rtn_order.ExchangeID, "kucoin");
            strncpy(rtn_order.UserID, stPendingOrderStatus.strUserID.c_str(), sizeof(rtn_order.UserID));
            strncpy(rtn_order.InstrumentID, stPendingOrderStatus.InstrumentID, sizeof(rtn_order.InstrumentID));
            rtn_order.Direction = stPendingOrderStatus.Direction;
            //No this setting on KuCoin
            rtn_order.TimeCondition = LF_CHAR_GTC;
            rtn_order.OrderPriceType = stPendingOrderStatus.OrderPriceType;
            strncpy(rtn_order.OrderRef, stPendingOrderStatus.OrderRef, sizeof(rtn_order.OrderRef));
            rtn_order.VolumeTotal = stPendingOrderStatus.nVolume - rtn_order.VolumeTraded;
            rtn_order.LimitPrice = stPendingOrderStatus.nPrice;
            rtn_order.VolumeTotalOriginal = stPendingOrderStatus.nVolume;
            strncpy(rtn_order.BusinessUnit,stPendingOrderStatus.remoteOrderId.c_str(),sizeof(rtn_order.BusinessUnit));

            std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
            on_rtn_order(&rtn_order);

            send_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
                                    source_id, MSG_TYPE_LF_RTN_ORDER_KUCOIN,
                                    1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);
            writer_lock.unlock();
            
            KF_LOG_INFO(logger, "[on_rtn_order] (InstrumentID)" << rtn_order.InstrumentID << "(OrderStatus)" <<  rtn_order.OrderStatus
                        << "(Volume)" << rtn_order.VolumeTotalOriginal << "(VolumeTraded)" << rtn_order.VolumeTraded);
 }

void TDEngineKuCoin::onTrade(const PendingOrderStatus& stPendingOrderStatus,int64_t nSize,int64_t nPrice,std::string& strTradeId,std::string& strTime)
{
            LFRtnTradeField rtn_trade;
            memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
            strcpy(rtn_trade.ExchangeID, "kucoin");
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

            std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
            on_rtn_trade(&rtn_trade);
            send_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
                                    source_id, MSG_TYPE_LF_RTN_TRADE_KUCOIN, 1, -1);
            writer_lock.unlock();

             KF_LOG_INFO(logger, "[on_rtn_trade 1] (InstrumentID)" << rtn_trade.InstrumentID << "(Direction)" << rtn_trade.Direction 
                        << "(Volume)" << rtn_trade.Volume << "(Price)" <<  rtn_trade.Price);
}

//fixed here
 void TDEngineKuCoin::onOrderChange(Document& d)
 {
     if(d.HasMember("data"))
     {
            auto& data = d["data"];
            if(data.HasMember("type") && data.HasMember("orderId") && data.HasMember("clientOid"))
            {
                std::string strType = data["type"].GetString();
                std::string strOrderId = data["orderId"].GetString();
                std::string strClientID = data["clientOid"].GetString();
                KF_LOG_INFO(logger, "[onOrderChange] order (type)" << strType.c_str() << " (orderId)" << strOrderId.c_str() << " (clientOid)" << strClientID.c_str());

                std::lock_guard<std::mutex> lck(*m_mutexOrder);
                // 在m_mapOrder和m_mapNewOrder中查找订单
                auto it = m_mapOrder.find(strOrderId);
                auto it2 = m_mapNewOrder.find(strClientID);

                // 处理新单
                if (it == m_mapOrder.end() && it2 != m_mapNewOrder.end())
                { //如果在m_mapOrder中找不到，先在m_mapNewOrder查找并插入m_mapOrder中，onOrder后，将订单从m_mapNewOrder中删除
                    KF_LOG_INFO(logger, "[onOrderChange] (orderId)" << strOrderId.c_str() << " found in m_mapNewOrder");
                    it2->second.OrderStatus = LF_CHAR_NotTouched;
                    it2->second.remoteOrderId = strOrderId;

                    m_mapOrder.insert(std::make_pair(strOrderId, it2->second));
                    localOrderRefRemoteOrderId.insert(std::make_pair(it2->second.OrderRef, strOrderId));
                    KF_LOG_INFO(logger, "zaf-insert:" << strOrderId);

                    onOrder(it2->second);
                    m_mapNewOrder.erase(it2);
                    //find again after insert
                    auto it = m_mapOrder.find(strOrderId);
                }

                // type: open/match/filled/canceled/update
                if (strType == "open")
                {   // 已经在前一步的判断中完成对新订单的返回，这里只是检查
                    if (it != m_mapOrder.end())
                        KF_LOG_INFO(logger, "[onOrderChange] just check (orderId)" << strOrderId.c_str() << " (clientOid)" << strClientID.c_str() << " found in m_mapOrder");
                    else 
                        KF_LOG_INFO(logger, "[onOrderChange] error (orderId)" << strOrderId.c_str() << " (clientOid)" << strClientID.c_str() << " not found in m_mapOrder");
                }
                else if(strType == "canceled" || strType == "match" || strType == "filled")
                {
                    if(it != m_mapOrder.end())
                    {   // 在m_mapOrder中能找到，则onOrder，若订单已经结单，将订单从m_mapOrder中删除
                        KF_LOG_INFO(logger, "[onOrderChange] (orderId)" << strOrderId.c_str() << " (clientOid)" << strClientID.c_str() << " found in m_mapOrder");

                        // 总之先检查有没有成交 或者订单状态改变
                        int64_t filledSize = std::round(std::stod(data["filledSize"].GetString()) * scale_offset);
                        int64_t remainSize = std::round(std::stod(data["remainSize"].GetString()) * scale_offset);
                        LfOrderStatusType NewOrderStatus = it->second.OrderStatus;
                        if (strType == "filled")
                            NewOrderStatus = LF_CHAR_AllTraded;
                        else if (strType == "canceled")
                            NewOrderStatus = LF_CHAR_Canceled;
                        else if (remainSize == 0)
                            NewOrderStatus = LF_CHAR_AllTraded;
                        else
                            NewOrderStatus = LF_CHAR_PartTradedQueueing;

                        // 有新成交 或者订单状态改变
                        if (it->second.VolumeTraded < filledSize || NewOrderStatus != it->second.OrderStatus) {
                            int64_t nSize = std::round(std::stod(data["size"].GetString()) * scale_offset);
                            std::string strTime = to_string(data["orderTime"].GetInt64());
                            std::string orderType = data["orderType"].GetString();

                            it->second.VolumeTraded = filledSize;
                            it->second.OrderStatus = NewOrderStatus;

                            int64_t nPrice = 0;
                            if (data.HasMember("price"))
                                nPrice = std::round(std::stod(data["price"].GetString()) * scale_offset);
                            // orderType == "market"时可能没有price
                            else if (data.HasMember("matchPrice"))
                                nPrice = std::round(std::stod(data["matchPrice"].GetString()) * scale_offset);
                            else if (orderType == "market" && (strType == "filled" || strType == "canceled"))
                                nPrice = get_order_price(it->second.strUserID, strOrderId);

                            // 市价单时更改nPrice，但很有可能回报没有price
                            if (orderType == "market" && nPrice != 0)
                                it->second.nPrice = nPrice;

                            // 有新成交,可能会因找不到价格返回it->second.nPrice
                            if (it->second.VolumeTraded < filledSize) {
                                std::string strTradeId = "unknow";
                                //strType == "match"可能才有matchPrice和tradeId，此时修改onTrade价格，但match接收到的时间可能晚于done，收到done不一定会这样填写
                                if (data.HasMember("matchPrice") && data.HasMember("tradeId")) {
                                    strTradeId = data["tradeId"].GetString();
                                    int64_t matchPrice = std::round(std::stod(data["matchPrice"].GetString()) * scale_offset);
                                    onTrade(it->second, nSize, matchPrice, strTradeId, strTime);
                                }
                                else if (nPrice != 0)
                                    onTrade(it->second, nSize, nPrice, strTradeId, strTime);
                                else
                                    onTrade(it->second, nSize, it->second.nPrice, strTradeId, strTime);
                            }

                            //总之都要onOrder
                            onOrder(it->second);

                            if (it->second.OrderStatus == LF_CHAR_AllTraded)
                            {
                                auto it2 = localOrderRefRemoteOrderId.find(it->second.OrderRef);
                                if (it2 != localOrderRefRemoteOrderId.end())
                                {
                                    KF_LOG_INFO(logger, "zaf-erase:" << it2->second);
                                    localOrderRefRemoteOrderId.erase(it2);
                                }
                                m_mapOrder.erase(it);
                            }
                            else if (it->second.OrderStatus == LF_CHAR_Canceled)
                            {
                                //从各map中删去
                                std::unique_lock<std::mutex> lck1(*mutex_orderaction_waiting_response);
                                auto it3 = remoteOrderIdOrderActionSentTime.find(strOrderId);
                                if (it3 != remoteOrderIdOrderActionSentTime.end())
                                {
                                    KF_LOG_INFO(logger, "zafit3");
                                    remoteOrderIdOrderActionSentTime.erase(it3);
                                }
                                lck1.unlock();

                                auto it2 = localOrderRefRemoteOrderId.find(it->second.OrderRef);
                                if (it2 != localOrderRefRemoteOrderId.end())
                                {
                                    KF_LOG_INFO(logger, "zaf-erase:" << it2->second);
                                    localOrderRefRemoteOrderId.erase(it2);
                                }
                                m_mapOrder.erase(it);
                            }
                        }
                    }
                    else
                        KF_LOG_INFO(logger, "[onOrderChange] error, (orderId)" << strOrderId.c_str() << " not found in m_mapOrder");
                }
                //可以修改总数量,一般不用，未经测试
                else if (strType == "update")
                {
                    if (it != m_mapOrder.end())
                    {
                        //在m_mapOrder中能找到，则onOrder
                        int64_t nSize = std::round(std::stod(data["size"].GetString()) * scale_offset);
                        int64_t filledSize = std::round(std::stod(data["filledSize"].GetString()) * scale_offset);
                        int64_t remainSize = std::round(std::stod(data["remainSize"].GetString()) * scale_offset);
                        int64_t oldSize = std::round(std::stod(data["oldSize"].GetString()) * scale_offset);
                        int64_t nPrice = std::round(std::stod(data["price"].GetString()) * scale_offset);
                        std::string strTradeId = data["tradeId"].GetString();
                        //std::string strTime = data["time"].GetString();
                        std::string strTime = data["orderTime"].GetString();

                        //it->second.VolumeTraded += nSize;
                        //it->second.OrderStatus =  it->second.VolumeTraded ==  it->second.nVolume ? LF_CHAR_AllTraded : LF_CHAR_PartTradedQueueing;
                        it->second.VolumeTraded == filledSize;
                        it->second.nVolume == nSize;
                        it->second.OrderStatus = remainSize == 0 ? LF_CHAR_AllTraded : LF_CHAR_PartTradedQueueing;
                        onOrder(it->second);
                        onTrade(it->second, nSize, nPrice, strTradeId, strTime);
                        if (it->second.OrderStatus == LF_CHAR_AllTraded)
                        {
                            auto it2 = localOrderRefRemoteOrderId.find(it->second.OrderRef);
                            if (it2 != localOrderRefRemoteOrderId.end())
                            {
                                KF_LOG_INFO(logger, "zaf-erase:" << it2->second);
                                localOrderRefRemoteOrderId.erase(it2);
                            }
                            m_mapOrder.erase(it);
                        }
                    }
                    else
                        KF_LOG_INFO(logger, "[onOrderChange] error, (orderId)" << strOrderId.c_str() << " not found in m_mapOrder");
                }
            }
            else
                KF_LOG_INFO(logger, "[onOrderChange] error, not found type or orderId or clientOid");
     }
        KF_LOG_INFO(logger, "[onOrderChange] finished");
 }


void TDEngineKuCoin::handle_lws_data(struct lws* conn,std::string data)
{
    //std::string strData = dealDataSprit(data);
	KF_LOG_INFO(logger, "TDEngineKuCoin::on_lws_data: " << data);
    Document json;
    json.Parse(data.c_str());

    if(!json.HasParseError() && json.IsObject() && json.HasMember("type") && json["type"].IsString())
    {
        if(strcmp(json["type"].GetString(), "welcome") == 0)
        {
            //KF_LOG_INFO(logger, "MDEngineKuCoin::on_lws_data: welcome");
            lws_callback_on_writable(conn);
        }
        if(strcmp(json["type"].GetString(), "pong") == 0)
        {
            //KF_LOG_INFO(logger, "MDEngineKuCoin::on_lws_data: pong");
           m_isPong = true;
           m_conn = conn;
        }
        if(strcmp(json["type"].GetString(), "message") == 0)
        {
            onOrderChange(json);
        }   
    } 
    else 
    {
        KF_LOG_ERROR(logger, "MDEngineKuCoin::on_lws_data . parse json error: " << data);
    }
    KF_LOG_INFO(logger, "[handle_lws_data] finished");
}
void TDEngineKuCoin::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    //handle_lws_data(conn,std::string(data));
    
    if(nullptr == m_ThreadPoolPtr)
    {
        handle_lws_data(conn,std::string(data));
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineKuCoin::handle_lws_data,this,conn,std::string(data)));
    }
	
}

// fixed here
// subscribe /spotMarket/tradeOrders
std::string TDEngineKuCoin::makeSubscribeTradeOrders()
{
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("id");
    writer.String(getId().c_str());
    writer.Key("type");
    writer.String("subscribe");
    writer.Key("topic");
    writer.String("/spotMarket/tradeOrders");
    writer.Key("privateChannel");
    writer.String("true");
    writer.Key("response");
    writer.String("true");
    writer.EndObject();
    std::string strUpdate = sbUpdate.GetString();
    return strUpdate;
}


std::string TDEngineKuCoin::makeSubscribeL3Update(const std::map<std::string,int>& mapAllSymbols)
{
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("id");
    writer.String(getId().c_str());
    writer.Key("type");
    writer.String("subscribe");
    writer.Key("topic");
    std::string strTopic = "/market/level3:";
    for(const auto&  pair : mapAllSymbols)
    {
        strTopic += pair.first + ",";
    }
    strTopic.pop_back();
    writer.String(strTopic.c_str());
    writer.Key("privateChannel");
    writer.String("true");
    writer.Key("response");
    writer.String("true");
    writer.EndObject();
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}

std::string TDEngineKuCoin::makeunSubscribeL3Update(const std::map<std::string,int>& mapAllSymbols)
{
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("id");
    writer.String(getId().c_str());
    writer.Key("type");
    writer.String("unsubscribe");
    writer.Key("topic");
    std::string strTopic = "/market/level3:";
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

int TDEngineKuCoin::lws_write_subscribe(struct lws* conn)
{
    KF_LOG_INFO(logger, "TDEngineKuCoin::lws_write_subscribe:" );
    
    int ret = 0;
    
    // fixed here
    // subscribe /spotMarket/tradeOrders
    AccountUnitKuCoin& unit = findAccountUnitKucoinByWebsocketConn(conn);
    if (!unit.is_sub_trade_orders)
    {
        unit.is_sub_trade_orders = true;

        std::string strSubscribe = makeSubscribeTradeOrders();
        unsigned char msg[1024];
        memset(&msg[LWS_PRE], 0, 1024 - LWS_PRE);
        int length = strSubscribe.length();
        KF_LOG_INFO(logger, "TDEngineKuCoin::lws_write_subscribe: " << strSubscribe.c_str() << " ,len = " << length);
        strncpy((char*)msg + LWS_PRE, strSubscribe.c_str(), length);
        ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);
        lws_callback_on_writable(conn);
    }
    else if(!ubsub)
    //if(!ubsub)
    // fixed end 
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
        KF_LOG_INFO(logger, "TDEngineKuCoin::lws_write_subscribe: " << strSubscribe.c_str() << " ,len = " << length);
        strncpy((char *)msg+LWS_PRE, strSubscribe.c_str(), length);
        ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
        lws_callback_on_writable(conn);  
    }
    else if(!m_isSubL3)
    {
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
        KF_LOG_INFO(logger, "TDEngineKuCoin::lws_write_subscribe: " << strSubscribe.c_str() << " ,len = " << length);
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

void TDEngineKuCoin::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_INFO(logger,"[on_lws_connection_error] on_lws_connection_error,threadid="<<std::this_thread::get_id());
    KF_LOG_ERROR(logger, "TDEngineKuCoin::on_lws_connection_error. login again.");
    AccountUnitKuCoin& unit=findAccountUnitKucoinByWebsocketConn(conn);     
    unit.is_connecting = false;    
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    m_isPong = false;
    m_shouldPing = true;
    //no use it
    long timeout_nsec = 0;
    //reset sub
    m_isSubL3 = false;
    //sleep(40);
    if(lws_login(unit, timeout_nsec))
    {
        Document json;
        for(auto& unit:account_units)
        {
            cancel_all_orders(unit, json);
            std::unique_lock<std::mutex> lck(*m_mutexOrder); 
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
            {
                lck.unlock();
                //check_orders(unit,startTime,endTime,mflag,rflag,0);
                //fix here
                check_orders(unit, startTime - rest_get_interval_ms, endTime + rest_get_interval_ms, mflag, rflag, 1);
                lck.lock();
            }
            endTime = startTime = getTimestamp();
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
            {
                lck.unlock();
                //check_orders(unit,startTime,endTime,mflag,rflag,1);
                //fix here
                check_orders(unit, startTime - rest_get_interval_ms, endTime + rest_get_interval_ms, mflag, rflag, 1);
                lck.lock();
            }
            for(auto itr = m_mapNewOrder.begin() ; itr != m_mapNewOrder.end() ; itr++)
            {
                KF_LOG_INFO(logger,"[on_lws_connection_error] insert failed");
                int errorId=100;
                std::string errorMsg="error,the order was insert failed,canceled in relogin";

                std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
                on_rsp_order_insert(&itr->second.data, itr->second.nRequestID, errorId, errorMsg.c_str());
                writer_lock.unlock();
            }
            for(auto it = m_mapOrder.begin(); it != m_mapOrder.end();)
            {
                bool isClosed = check_single_order(unit,it->second);
                if(isClosed)
                {
                    KF_LOG_INFO(logger,"erase order:"<< it->second.remoteOrderId);
                    localOrderRefRemoteOrderId.erase(it->second.OrderRef);
                    remoteOrderIdOrderActionSentTime.erase(it->second.remoteOrderId);
                    it = m_mapOrder.erase(it);
                }
                else
                {
                    KF_LOG_INFO(logger,"order is active:"<< it->second.remoteOrderId);
                    auto it2 = remoteOrderIdOrderActionSentTime.find(it->second.remoteOrderId);
                    if(it2 != remoteOrderIdOrderActionSentTime.end())
                    {
                        int errorId=100;
                        std::string errorMsg="error,the order was cancel failed in relogin";
                        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
                        on_rsp_order_action(&it2->second.data, it2->second.requestId, errorId, errorMsg.c_str());
                        writer_lock.unlock();
                        remoteOrderIdOrderActionSentTime.erase(it2);
                        it = m_mapOrder.erase(it);
                    }
                    else
                    {
                        ++it;
                    }
                }
            }
        }
        std::unique_lock<std::mutex> lck(*m_mutexOrder); 
        //m_mapOrder.clear();
        m_mapNewOrder.clear();
        lck.unlock();
        //localOrderRefRemoteOrderId.clear();
        //std::unique_lock<std::mutex> lck2(*mutex_marketorder_waiting_response);
        //remoteOrderIdOrderActionSentTime.clear(); 
        //lck2.unlock();
    }
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

void TDEngineKuCoin::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}

bool TDEngineKuCoin::getToken(Document& d) 
{
    int nTryCount = 0;
    cpr::Response response;
    std::string url;
    do{
        //url = "https://api.kucoin.com/api/v1/bullet-private";
        //response = cpr::Post(Url{url.c_str()}, Parameters{}); 
        url="/api/v1/bullet-private";
        response=Post(url,"",account_units[0],account_units[0].api_key,account_units[0].secret_key,account_units[0].passphrase);
       
    }while(++nTryCount < max_rest_retry_times && response.status_code != 200);

    KF_LOG_INFO(logger, "[getToken] (url) " << url << " (response.status_code) " << response.status_code <<
                                               " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());

    if(response.status_code != 200)
    {
        KF_LOG_ERROR(logger, "TDEngineKuCoin::login::getToken Error");
        return false;
    }

    KF_LOG_INFO(logger, "TDEngineKuCoin::getToken: " << response.text.c_str());

    d.Parse(response.text.c_str());
    return true;
}


bool TDEngineKuCoin::getServers(Document& d)
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
        KF_LOG_ERROR(logger, "TDEngineKuCoin::login::getServers Error");
        return false;
    }
    return true;
}

int64_t TDEngineKuCoin::getMSTime()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  timestamp;
}

std::mutex g_httpMutex;
cpr::Response TDEngineKuCoin::Get(const std::string& method_url,const std::string& body, AccountUnitKuCoin& unit)
{
    if (rateLimitExceeded) {
        if (getTimestamp() > limitEndTime) {
            KF_LOG_DEBUG(logger, "[Get] set rateLimitExceeded = false, (limitEndTime)" << limitEndTime << " (getTimestamp)" << getTimestamp());
            rateLimitExceeded = false;
        }
        else {
            KF_LOG_DEBUG(logger, "[Get] Too Many Requests, (limitEndTime)" << limitEndTime);
            cpr::Response res;
            return res;
        }
    }

    string url = unit.baseUrl + method_url;
    std::string strTimestamp = std::to_string(getTimestamp());
    std::string strSign = strTimestamp + "GET" + method_url;
    //KF_LOG_INFO(logger, "strSign = " << strSign );
    unsigned char strHmac[EVP_MAX_MD_SIZE]={0};
    hmac_sha256_byte(unit.secret_key.c_str(),strSign.c_str(),strHmac,EVP_MAX_MD_SIZE);
    //KF_LOG_INFO(logger, "strHmac = " << strHmac );
    std::string strSignatrue = base64_encode(strHmac,32);

    //对于V2版的API-KEY，需要将KC-API-KEY-VERSION指定为2，并将passphrase使用API-Secret进行HMAC-sha256加密，再将加密内容通过base64编码后传递
    unsigned char strHmacPass[EVP_MAX_MD_SIZE] = { 0 };
    hmac_sha256_byte(unit.secret_key.c_str(), unit.passphrase.c_str(), strHmacPass, EVP_MAX_MD_SIZE);
    std::string strPassPhrase = base64_encode(strHmacPass, 32);

    cpr::Header mapHeader = cpr::Header{{"KC-API-SIGN",strSignatrue},
                                        {"KC-API-TIMESTAMP",strTimestamp},
                                        {"KC-API-KEY",unit.api_key},
                                        {"KC-API-PASSPHRASE", strPassPhrase},
                                        {"KC-API-KEY-VERSION", to_string(2)} };
    //KF_LOG_INFO(logger, "KC-API-SIGN = " << strSignatrue.c_str() << ", KC-API-TIMESTAMP = " << strTimestamp.c_str() << ", KC-API-KEY = " << unit.api_key.c_str() << ", KC-API-PASSPHRASE = " << unit.passphrase.c_str());
    KF_LOG_INFO(logger, "[get] wait to lock g_httpMutex (method_url)" << method_url.c_str());
    std::unique_lock<std::mutex> lock(g_httpMutex);
    KF_LOG_INFO(logger, "[get] now locking g_httpMutex (method_url)" << method_url.c_str());
    auto response= cpr::Get(Url{url}, Header{mapHeader}, Timeout{10000} );
    lock.unlock();
    KF_LOG_INFO(logger, "[get] unlock g_httpMutex (method_url)" << method_url.c_str());

    KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
                                               " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
    //check limit rate
    if (response.status_code == 429) {
        rateLimitExceeded = true;
        limitEndTime = getTimestamp() + prohibit_order_ms;
        KF_LOG_DEBUG(logger, "[Get] Too Many Requests, http requests will not be sent to server from now until (limitEndTime)" << limitEndTime);
    }

    return response;
}

cpr::Response TDEngineKuCoin::Delete(const std::string& method_url,const std::string& body, AccountUnitKuCoin& unit)
{
    if (rateLimitExceeded) {
        if (getTimestamp() > limitEndTime) {
            KF_LOG_DEBUG(logger, "[Delete] set rateLimitExceeded = false, (limitEndTime)" << limitEndTime << " (getTimestamp)" << getTimestamp());
            rateLimitExceeded = false;
        }
        else {
            KF_LOG_DEBUG(logger, "[Delete] Too Many Requests, (limitEndTime)" << limitEndTime);
            cpr::Response res;
            return res;
        }
    }

    string url = unit.baseUrl + method_url + body;
    std::string strTimestamp = std::to_string(getTimestamp());
    std::string strSign =  strTimestamp + "DELETE" + method_url + body;
    //KF_LOG_INFO(logger, "strSign = " << strSign );
    unsigned char strHmac[EVP_MAX_MD_SIZE]={0};
    hmac_sha256_byte(unit.secret_key.c_str(),strSign.c_str(),strHmac,EVP_MAX_MD_SIZE);
    //KF_LOG_INFO(logger, "strHmac = " << strHmac );
    std::string strSignatrue = base64_encode(strHmac,32);

    //对于V2版的API-KEY，需要将KC-API-KEY-VERSION指定为2，并将passphrase使用API-Secret进行HMAC-sha256加密，再将加密内容通过base64编码后传递
    unsigned char strHmacPass[EVP_MAX_MD_SIZE] = { 0 };
    hmac_sha256_byte(unit.secret_key.c_str(), unit.passphrase.c_str(), strHmacPass, EVP_MAX_MD_SIZE);
    std::string strPassPhrase = base64_encode(strHmacPass, 32);

    cpr::Header mapHeader = cpr::Header{{"KC-API-SIGN",strSignatrue},
                                        {"KC-API-TIMESTAMP",strTimestamp},
                                        {"KC-API-KEY",unit.api_key},
                                        {"KC-API-PASSPHRASE", strPassPhrase},
                                        {"KC-API-KEY-VERSION", to_string(2)} };
    //KF_LOG_INFO(logger, "KC-API-SIGN = " << strSignatrue.c_str() << ", KC-API-TIMESTAMP = " << strTimestamp.c_str() << ", KC-API-KEY = " << unit.api_key.c_str() << ", KC-API-PASSPHRASE = " << unit.passphrase.c_str());
    KF_LOG_INFO(logger, "[delete] wait to lock g_httpMutex (method_url)" << method_url.c_str());
    std::unique_lock<std::mutex> lock(g_httpMutex);
    KF_LOG_INFO(logger, "[delete] now locking g_httpMutex (method_url)" << method_url.c_str());
    auto response = cpr::Delete(Url{url},Header{mapHeader}, Timeout{10000} );
    lock.unlock();
    KF_LOG_INFO(logger, "[delete] unlock g_httpMutex (method_url)" << method_url.c_str());

    KF_LOG_INFO(logger, "[delete] (url) " << url << " (response.status_code) " << response.status_code <<
                                               " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());

    //check limit rate
    if (response.status_code == 429) {
        rateLimitExceeded = true;
        limitEndTime = getTimestamp() + prohibit_order_ms;
        KF_LOG_DEBUG(logger, "[Delete] Too Many Requests, http requests will not be sent to server from now until (limitEndTime)" << limitEndTime);
    }

    return response;
}

cpr::Response TDEngineKuCoin::Post(const std::string& method_url,const std::string& body, AccountUnitKuCoin& unit,std::string api_key,std::string secret_key,std::string passphrase)
{
    if (rateLimitExceeded) {
        if (getTimestamp() > limitEndTime) {
            KF_LOG_DEBUG(logger, "[Post] set rateLimitExceeded = false, (limitEndTime)" << limitEndTime << " (getTimestamp)" << getTimestamp());
            rateLimitExceeded = false;
        }
        else {
            KF_LOG_DEBUG(logger, "[Post] Too Many Requests, (limitEndTime)" << limitEndTime);
            cpr::Response res;
            return res;
        }
    }

    KF_LOG_INFO(logger,"api_key2="<<api_key);
    std::string strTimestamp = std::to_string(getTimestamp());
    std::string strSign =  strTimestamp + "POST" + method_url + body;
    // KF_LOG_INFO(logger, "strSign = " << strSign );
    ///使用 API-Secret 对 {timestamp + method+ endpoint + body} 拼接的字符串进行HMAC-sha256加密。
    ///再将加密内容使用 base64 加密
    unsigned char strHmac[EVP_MAX_MD_SIZE]={0};
    hmac_sha256_byte(secret_key.c_str(),strSign.c_str(),strHmac,EVP_MAX_MD_SIZE);
    std::string strSignatrue = base64_encode(strHmac,32);
    //KF_LOG_INFO(logger, "strHmac = " << strHmac );
    
    //对于V2版的API-KEY，需要将KC-API-KEY-VERSION指定为2，并将passphrase使用API-Secret进行HMAC-sha256加密，再将加密内容通过base64编码后传递
    unsigned char strHmacPass[EVP_MAX_MD_SIZE] = { 0 };
    hmac_sha256_byte(secret_key.c_str(), unit.passphrase.c_str(), strHmacPass, EVP_MAX_MD_SIZE);
    std::string strPassPhrase = base64_encode(strHmacPass, 32);

    cpr::Header mapHeader = cpr::Header{{"KC-API-SIGN",strSignatrue},
                                        {"KC-API-TIMESTAMP",strTimestamp},
                                        {"KC-API-KEY",api_key},
                                        {"KC-API-PASSPHRASE", strPassPhrase},
                                        {"KC-API-KEY-VERSION", to_string(2)},
                                        {"Content-Type", "application/json"}};

    //KF_LOG_INFO(logger, "KC-API-SIGN = " << strSignatrue.c_str() << ", KC-API-TIMESTAMP = " << strTimestamp.c_str() << ", KC-API-KEY = " << unit.api_key.c_str() << ", KC-API-PASSPHRASE = " << unit.passphrase.c_str());
    string url = baseurl + method_url;
    KF_LOG_INFO(logger, "[post] wait to lock g_httpMutex (method_url)" << method_url.c_str());
    std::unique_lock<std::mutex> lock(g_httpMutex);
    KF_LOG_INFO(logger, "[post] now locking g_httpMutex (method_url)" << method_url.c_str());
    auto response = cpr::Post(Url{url}, Header{mapHeader},Body{body},Timeout{30000});
    lock.unlock();
    KF_LOG_INFO(logger, "[post] unlock g_httpMutex (method_url)" << method_url.c_str());

    KF_LOG_INFO(logger, "[post] (url) " << url <<"(body) "<< body<< " (response.status_code) " << response.status_code <<
                                       " (response.error.message) " << response.error.message <<
                                       " (response.text) " << response.text.c_str());

    //check limit rate
    if (response.status_code == 429) {
        rateLimitExceeded = true;
        limitEndTime = getTimestamp() + prohibit_order_ms;
        KF_LOG_DEBUG(logger, "[Post] Too Many Requests, http requests will not be sent to server from now until (limitEndTime)" << limitEndTime);
    }

    return response;
}

void TDEngineKuCoin::init()
{
    
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineKuCoin::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineKuCoin::resize_accounts(int account_num)
{
    account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineKuCoin::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    string api_key = j_config["APIKey"].get<string>();
    string secret_key = j_config["SecretKey"].get<string>();
    string passphrase = j_config["passphrase"].get<string>();
    string baseUrl = j_config["baseUrl"].get<string>();
    baseurl = baseUrl;
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

    if (j_config.find("prohibit_order_ms") != j_config.end()) {
        prohibit_order_ms = j_config["prohibit_order_ms"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (prohibit_order_ms)" << prohibit_order_ms);

    if(j_config.find("orderaction_max_waiting_seconds") != j_config.end()) {
        orderaction_max_waiting_seconds = j_config["orderaction_max_waiting_seconds"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (orderaction_max_waiting_seconds)" << orderaction_max_waiting_seconds);

    if(j_config.find("sub") != j_config.end()){
        nlohmann::json sub = j_config["sub"].get<nlohmann::json>();
        if(sub.is_object()){
            for (nlohmann::json::iterator it = sub.begin(); it != sub.end(); ++it){
                KF_LOG_INFO(logger,"it.key()"<<it.key()<<" it.value()"<<it.value());
                std::string name = it.key();
                char_64 cname;
                strcpy(cname,name.c_str());
                KEY key;
                key.api_key = sub[cname]["APIKey"].get<string>();
                key.secret_key = sub[cname]["SecretKey"].get<string>();
                key.passphrase = sub[cname]["passphrase"].get<string>();
                sub_map.insert(make_pair(name,key));                    
            }
        }
    }

    if(j_config.find("max_rest_retry_times") != j_config.end()) {
        max_rest_retry_times = j_config["max_rest_retry_times"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (max_rest_retry_times)" << max_rest_retry_times);


    if(j_config.find("retry_interval_milliseconds") != j_config.end()) {
        retry_interval_milliseconds = j_config["retry_interval_milliseconds"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (retry_interval_milliseconds)" << retry_interval_milliseconds);

    if(j_config.find("account_type") != j_config.end())
    {
        if((j_config["account_type"].get<string>() == "margin")){
            account_type = j_config["account_type"].get<string>();
        }
    }

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
    AccountUnitKuCoin& unit = account_units[idx];
    unit.api_key = api_key;
    unit.secret_key = secret_key;
    unit.passphrase = passphrase;
    unit.baseUrl = baseUrl;

    KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl);

//test rs256
  //  std::string data ="{}";
  //  std::string signature =utils::crypto::rsa256_private_sign(data, g_private_key);
   // std::string sign = base64_encode((unsigned char*)signature.c_str(), signature.size());
    //std::cout  << "[TDEngineKuCoin] (test rs256-base64-sign)" << sign << std::endl;

    //std::string decodeStr = utils::crypto::rsa256_pub_verify(data,signature, g_public_key);
    //std::cout  << "[TDEngineKuCoin] (test rs256-verify)" << (decodeStr.empty()?"yes":"no") << std::endl;

    unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    unit.coinPairWhiteList.Debug_print();

    unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
    unit.positionWhiteList.Debug_print();

    //display usage:
    if(unit.coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "TDEngineKuCoin::load_account: please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }

    //test
    Document json;
    get_account(unit, json);
    printResponse(json);
    cancel_all_orders(unit, json);
    printResponse(json);
    getPriceIncrement(unit);
    req_currencylist(unit);
    get_sub_user(unit);
    req_withdrawals_list("XRP",1562036459000,1564734404000,"SUCCESS");
    req_deposits_list("XRP",1562036459000,1564734404000,"SUCCESS");
    // set up
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, api_key.c_str(), 16);
    strncpy(account.Password, secret_key.c_str(), 21);
    return account;
}

void TDEngineKuCoin::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitKuCoin& unit = account_units[idx];
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

   void TDEngineKuCoin::getPriceIncrement(AccountUnitKuCoin& unit)
   {
        auto& coinPairWhiteList = unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList();
        for(auto& pair : coinPairWhiteList)
        {
            Document json;
            const auto response = Get("/api/v1/symbols/" + pair.second,"",unit);
            json.Parse(response.text.c_str());
            const static std::string strSuccesse = "200000";
            if(json.HasMember("code") && json["code"].GetString() == strSuccesse)
            {
                auto& data = json["data"];
                PriceIncrement stPriceIncrement;
                stPriceIncrement.nBaseMinSize = std::round(std::stod(data["baseMinSize"].GetString())* scale_offset);
                stPriceIncrement.nPriceIncrement = std::round(std::stod(data["priceIncrement"].GetString()) * scale_offset);
                stPriceIncrement.nQuoteIncrement = std::round(std::stod(data["quoteIncrement"].GetString()) * scale_offset);
                stPriceIncrement.nBaseIncrement = std::round(std::stod(data["baseIncrement"].GetString()) * scale_offset);
                unit.mapPriceIncrement.insert(std::make_pair(pair.first,stPriceIncrement));

                 KF_LOG_INFO(logger, "[getPriceIncrement] (BaseMinSize )" << stPriceIncrement.nBaseMinSize << "(PriceIncrement)" << stPriceIncrement.nPriceIncrement
                                    << "(QuoteIncrement)" << stPriceIncrement.nQuoteIncrement << "(BseIncrement)" << stPriceIncrement.nBaseIncrement);
            }
        }
   }
void TDEngineKuCoin::check_orders(AccountUnitKuCoin& unit, int64_t startTime, int64_t endTime, int64_t mflag, int64_t rflag, int64_t flag)
{
    KF_LOG_INFO(logger, "[check_orders]");

    int64_t currentPage = 1 ;
    int64_t totalPage = currentPage ;
    int64_t new_mflag = 0;
    int64_t new_rflag = 0;
    
    while((new_mflag != mflag || new_rflag != rflag) && currentPage <= totalPage)
    {
        KF_LOG_INFO(logger, "[check_orders] currentPage " << currentPage << " totalPage" << totalPage);

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
            KF_LOG_INFO(logger, "[check_orders] check response, code == strSuccesse");

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
                        std::string strClientId = item["clientOid"].GetString();
                        std::string strOrderId = item["id"].GetString();
                        bool Status = item["isActive"].GetBool();
                        bool cancelExist = item["cancelExist"].GetBool();

                        KF_LOG_INFO(logger, "[check_orders] check response, (strClientId)" << strClientId.c_str() << " (strOrderId)" << strOrderId.c_str());

                        std::lock_guard<std::mutex> lck(*m_mutexOrder);
                        auto its = m_mapNewOrder.find(strClientId);
                        if(its != m_mapNewOrder.end())
                        {
                            its->second.OrderStatus = LF_CHAR_NotTouched;
                            its->second.remoteOrderId = strOrderId;
                            m_mapOrder.insert(std::make_pair(strOrderId,its->second));
                            localOrderRefRemoteOrderId.insert(std::make_pair(its->second.OrderRef,strOrderId));
                            onOrder(its->second);
                            m_mapNewOrder.erase(its);
                            new_mflag++;
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
                                    std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
                                    on_rsp_order_action(&its->second.data, its->second.requestId, errorId, errorMsg.c_str());
                                    writer_lock.unlock();
                                    remoteOrderIdOrderActionSentTime.erase(its);
                                    m_mapOrder.erase(strOrderId);
                                    new_rflag++;
                                }
                                lck1.unlock();
                            }
                            else
                            {
                                if(!cancelExist)
                                {                                    
                                    it->second.OrderStatus = LF_CHAR_AllTraded;
                                }
                                else
                                {
                                    it->second.OrderStatus = LF_CHAR_Canceled;   
                                }
                                int64_t nSize = std::round(std::stod(item["dealSize"].GetString()) * scale_offset);
                                it->second.VolumeTraded = nSize;
                                onOrder(it->second);
                                auto it2 = localOrderRefRemoteOrderId.find(it->second.OrderRef);
                                if(it2 != localOrderRefRemoteOrderId.end())
                                {
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

    KF_LOG_INFO(logger, "[check_orders] finished");
}

bool TDEngineKuCoin::check_single_order(AccountUnitKuCoin& unit,PendingOrderStatus& stPendingOrderStatus)
{
    KF_LOG_INFO(logger,"check_single_order:"<< stPendingOrderStatus.remoteOrderId);
    bool isClosed = false;
    std::string url ="/api/v1/orders/" + stPendingOrderStatus.remoteOrderId;
    Document json;
    const auto response = Get(url.c_str(),"",unit);
    json.Parse(response.text.c_str());
    const static std::string strSuccesse = "200000";
    if(!json.HasParseError() && json.IsObject() &&json.HasMember("code") && json["code"].GetString() == strSuccesse && json.HasMember("data"))
    {
        auto& data = json["data"];
        if(data.HasMember("isActive") && data.HasMember("cancelExist"))
        {
            bool isActive = data["isActive"].GetBool();
            bool cancelExist = data["cancelExist"].GetBool();
            int64_t nDealSize = std::round(std::stod(data["dealSize"].GetString()) * scale_offset);
            //int64_t nSize = std::round(std::stod(data["size"].GetString()) * scale_offset);
            stPendingOrderStatus.VolumeTraded = nDealSize;
            if(!isActive)
            {
                if(stPendingOrderStatus.nVolume == nDealSize)
                {
                    stPendingOrderStatus.OrderStatus = LF_CHAR_AllTraded;
                }
                else
                {
                    stPendingOrderStatus.OrderStatus = LF_CHAR_Canceled;
                }
                onOrder(stPendingOrderStatus);
                isClosed = true;
            }
        }
    }
    return isClosed;
}

//获取单个订单成交价格信息
int64_t TDEngineKuCoin::get_order_price(string api_key, string orderId)
{
    //stPendingOrderStatus.strUserID = unit.api_key;
    int64_t res = 0;
    int i = 0;
    for (i = 0; i < account_units.size(); i++) {
        if (account_units[i].api_key == api_key)
            break;
    }
    if (i == account_units.size())
    {
        KF_LOG_INFO(logger, "[get_order_price] error, can not found account_units by (api_key)" << api_key.c_str());
        return res;
    }

    AccountUnitKuCoin& unit = account_units[i];
    std::string url = "/api/v1/orders/" + orderId;
    Document json;
    const auto response = Get(url.c_str(), "", unit);
    json.Parse(response.text.c_str());

    if (json.HasMember("dealFunds") && json.HasMember("dealSize"))
    {
        double dealFunds = std::stod(json["dealFunds"].GetString());
        double dealSize = std::stod(json["dealSize"].GetString());
        if(dealSize != 0)
            res = std::round(dealFunds / dealSize * scale_offset);
    }
    else
        KF_LOG_INFO(logger, "[get_order_price] error, response is not except (api_key)" << api_key.c_str());

    return res;
}

void TDEngineKuCoin::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[login]");
    //lws_login(timeout_nsec);
}

bool TDEngineKuCoin::lws_login(AccountUnitKuCoin& unit, long timeout_nsec)
{
    KF_LOG_INFO(logger,"[login] login,threadid="<<std::this_thread::get_id());
    KF_LOG_INFO(logger, "TDEngineKuCoin::login:");

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
	KF_LOG_INFO(logger, "TDEngineKuCoin::login: context created.");


	if (context == NULL) {
		KF_LOG_ERROR(logger, "TDEngineKuCoin::login: context is NULL. return");
		return false;
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
    std::unique_lock<std::mutex> web_socket_connect_mutex(* mutex_web_connect);
    clientConnectInfo.pwsi = &unit.webSocketConn;

    KF_LOG_INFO(logger, "TDEngineKuCoin::login: address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);

	unit.webSocketConn = lws_client_connect_via_info(&clientConnectInfo);
	if (unit.webSocketConn == NULL) {
		KF_LOG_ERROR(logger, "TDEngineKuCoin::login: wsi create error.");
		return false;
	}
	KF_LOG_INFO(logger, "TDEngineKuCoin::login: wsi create success.");
    unit.is_connecting = true;
    web_socket_connect_mutex.unlock();
    return true;
    //connect(timeout_nsec);
}

void TDEngineKuCoin::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineKuCoin::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineKuCoin::is_logged_in() const
{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineKuCoin::is_connected() const
{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}


std::string TDEngineKuCoin::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "buy";
    } else if (LF_CHAR_Sell == input) {
        return "sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineKuCoin::GetDirection(std::string input) {
    if ("buy" == input) {
        return LF_CHAR_Buy;
    } else if ("sell" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineKuCoin::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "market";
    } else {
        return "";
    }
}

LfOrderPriceTypeType TDEngineKuCoin::GetPriceType(std::string input) {
    if ("limit" == input) {
        return LF_CHAR_LimitPrice;
    } else if ("market" == input) {
        return LF_CHAR_AnyPrice;
    } else {
        return '0';
    }
}
//订单状态，﻿open（未成交）、filled（已完成）、canceled（已撤销）、cancel（撤销中）、partially-filled（部分成交）
LfOrderStatusType TDEngineKuCoin::GetOrderStatus(bool isCancel,int64_t nSize,int64_t nDealSize) {
    
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


std::string TDEngineKuCoin::createInsertOrdertring_sub_accounts(string subUserId)
{    
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("subUserId");
    writer.String(subUserId.c_str());    
   
    writer.EndObject();
    std::string str = s.GetString();
    return str;
}

void TDEngineKuCoin::get_sub_accounts(string subUserId, AccountUnitKuCoin& unit, Document& json)
{
    string path = "/api/v1/sub-accounts/"+subUserId;
    auto response=Get(path,"{}",unit);
    json.Parse(response.text.c_str());  
}

/**
 * req functions
 */
void TDEngineKuCoin::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{

    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitKuCoin& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key << " (InstrumentID) " << data->InstrumentID);

    LFRspPositionField pos;
    memset(&pos, 0, sizeof(LFRspPositionField));
    strcpy(pos.BrokerID, data->BrokerID);
    strcpy(pos.InvestorID, data->InvestorID);
    strncpy(pos.InstrumentID, data->InstrumentID, 31);
    pos.PosiDirection = LF_CHAR_Long;
    pos.HedgeFlag = LF_CHAR_Speculation;
    pos.Position = 0;
    pos.YdPosition = 0;
    pos.PositionCost = 0;

    int errorId = 0;
    std::string errorMsg = "";

    std::map<std::string,LFRspPositionField> tmp_map;
    KF_LOG_INFO(logger, "[req_investor_position] (BrokerID)"<<data->BrokerID );
    string BrokerID=data->BrokerID;
    
               
    if(strcmp(data->BrokerID,"master-main")==0 || strcmp(data->BrokerID,"master-trade")==0 || strcmp(data->BrokerID,"master-margin")==0)
    {
        Document d;
        get_account(unit, d);
        if(d.IsObject() && !d.HasParseError() && d.HasMember("code"))
        {

            KF_LOG_INFO(logger, "[req_investor_position] (getcode)" );
            errorId =  std::round(std::stod(d["code"].GetString()));
            KF_LOG_INFO(logger, "[req_investor_position] (errorId)" << errorId);
            if(errorId != 200000) 
            {
                if (d.HasMember("msg") && d["msg"].IsString()) 
                {
                    errorMsg = d["msg"].GetString();
                }
                KF_LOG_ERROR(logger, "[req_investor_position] failed!" << " (rid)" << requestId << " (errorId)" << errorId
                                                                   << " (errorMsg) " << errorMsg);

                std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
                send_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
                writer_lock.unlock();
            }
            else if( d.HasMember("data"))
            {
                auto& jisonData = d["data"];
                size_t len = jisonData.Size();
                KF_LOG_INFO(logger, "[req_investor_position] (accounts.length)" << len);
                std::string accountType = data->BrokerID;
                auto position = accountType.find("-");
                std::string type = accountType.substr(position+1);
                //if(strcmp(data->BrokerID,"master-main")==0)
                {
                    KF_LOG_INFO(logger, "[req_investor_position] BrokerID==master-main --> (BrokerID) " << BrokerID );
                    for(size_t i = 0; i < len; i++)
                    {
                        if(jisonData.GetArray()[i]["type"].GetString() == type)
                        {
                            std::string symbol = jisonData.GetArray()[i]["currency"].GetString();
                            KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol);
                            std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
                            KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (ticker) " << ticker);
                            if(ticker.length() > 0)
                            {            
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
                    }
                }
            }
        }
        else
        {
            KF_LOG_ERROR(logger, "[req_investor_position] error,failed!");
        }
    }
    else if(strcmp(data->BrokerID,"sub-main")==0 || strcmp(data->BrokerID,"sub-trade")==0 || strcmp(data->BrokerID,"sub-margin")==0)
    {
        KF_LOG_INFO(logger,"BrokerID2");       
        Document d;
        string subUserId;
        auto itr = unit.mapSubUser.find(data->InvestorID);
        if(itr != unit.mapSubUser.end()){
            KF_LOG_INFO(logger,"find.");
            subUserId=itr->second.userId;
        }

        KF_LOG_INFO(logger, "[req_investor_position] (subUserId)"<<subUserId );
        get_sub_accounts(subUserId,unit,d);

        if(d.IsObject() && !d.HasParseError() && d.HasMember("code"))           
        {
            KF_LOG_INFO(logger, "[req_investor_position] (getcode)");
            errorId =  std::round(std::stod(d["code"].GetString()));
            KF_LOG_INFO(logger, "[req_investor_position] (errorId)" << errorId);
            if(errorId != 200000) 
            {
                if (d.HasMember("msg") && d["msg"].IsString()) 
                {
                    errorMsg = d["msg"].GetString();
                }
                KF_LOG_ERROR(logger, "[req_investor_position] failed!" << " (rid)" << requestId << " (errorId)" << errorId
                                                                   << " (errorMsg) " << errorMsg);

                std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
                send_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
                writer_lock.unlock();
            }
            else if(d.HasMember("data"))
            {
                auto& jisonData = d["data"];
                size_t len;

                string sub_master;

                if(strcmp(data->BrokerID,"sub-main")==0)
                {
                    KF_LOG_INFO(logger, "[req_investor_position] BrokerID==sub-main --> (BrokerID) " << BrokerID );

                    len = jisonData["mainAccounts"].Size();
                    sub_master="mainAccounts";
                }
                else if(strcmp(data->BrokerID,"sub-trade")==0)
                {
                    KF_LOG_INFO(logger, "[req_investor_position] BrokerID==sub-trade --> (BrokerID) " << BrokerID );

                    len = jisonData["tradeAccounts"].Size();
                    sub_master="tradeAccounts";
                }
                else
                {
                    KF_LOG_INFO(logger, "[req_investor_position] BrokerID==sub-margin --> (BrokerID) " << BrokerID );

                    len = jisonData["marginAccounts"].Size();
                    sub_master="marginAccounts";
                }
                
                
                auto accounts = jisonData[sub_master.c_str()].GetArray();
                //Value &data = jisonData[sub_master.c_str()];
                for(int i=0;i<len;i++)
                {
                    rapidjson::Value data = accounts[i].GetObject();

                    std::string symbol =data["currency"].GetString();
                    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol);

                    std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
                    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (ticker) " << ticker);
                    
                    if(ticker.length() > 0)
                    {            
                        uint64_t nPosition = std::round(std::stod(data["balance"].GetString()) * scale_offset);   
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

            }
        }
        else
        {
            KF_LOG_ERROR(logger, "[req_investor_position] error,failed!");
        }

       
    } 
  
                         
//*******************************************************************
    
    //send the filtered position
    int position_count = tmp_map.size();
    if(position_count > 0) {
        for (auto it =  tmp_map.begin() ; it != tmp_map.end() ;  ++it) {
            --position_count;
            std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
            on_rsp_position(&it->second, position_count == 0, requestId, 0, errorMsg.c_str());
            writer_lock.unlock();
        }
    }
    else
    {
        KF_LOG_INFO(logger, "[req_investor_position] (!findSymbolInResult) (requestId)" << requestId);
        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();
    }
}

void TDEngineKuCoin::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineKuCoin::dealPriceVolume(AccountUnitKuCoin& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,double& dDealPrice,double& dDealVolume)
{
        KF_LOG_DEBUG(logger, "[dealPriceVolume] (symbol)" << symbol);
        auto it = unit.mapPriceIncrement.find(symbol);
        if(it == unit.mapPriceIncrement.end())
        {
                  KF_LOG_INFO(logger, "[dealPriceVolume] symbol not find :" << symbol);
                  dDealVolume = 0;
                  return ;
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

void TDEngineKuCoin::handle_order_insert(AccountUnitKuCoin& unit,const LFInputOrderField data,int requestId,const std::string& ticker)
{
    KF_LOG_DEBUG(logger, "[handle_order_insert]" << " (current thread)" << std::this_thread::get_id());
    int errorId = 0;
    std::string errorMsg = "";

    double funds = 0;
    Document d;
    double fixedPrice = 0;
    double fixedVolume = 0;
    if (data.OrderPriceType == LF_CHAR_AnyPrice)
    {
        KF_LOG_DEBUG(logger, "[req_order_insert] market price ->limit price:" << data.ExpectPrice);
        dealPriceVolume(unit,data.InstrumentID,data.ExpectPrice,data.Volume,fixedPrice,fixedVolume);
    }
    else
    {
        dealPriceVolume(unit,data.InstrumentID,data.LimitPrice,data.Volume,fixedPrice,fixedVolume);
    }
    if(fixedVolume == 0)
    {
        KF_LOG_DEBUG(logger, "[req_order_insert] fixed Volume error" << ticker);
        errorId = 200;
        errorMsg = data.InstrumentID;
        errorMsg += " : quote less than baseMinSize";

        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        send_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();
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
    //stPendingOrderStatus.OrderPriceType = LF_CHAR_LimitPrice; //data.OrderPriceType;
    stPendingOrderStatus.OrderPriceType = data.OrderPriceType;
    stPendingOrderStatus.OffsetFlag = data.OffsetFlag;
    //stPendingOrderStatus.remoteOrderId = remoteOrderId;
    stPendingOrderStatus.nRequestID = requestId;
    stPendingOrderStatus.strClientId = strClientId;
    memcpy(&stPendingOrderStatus.data, &data, sizeof(LFInputOrderField));

    if(!unit.is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();
        return;
    }

    if (rateLimitExceeded) {
        if (getTimestamp() > limitEndTime) {
            rateLimitExceeded = false;
        }
        else {
            KF_LOG_DEBUG(logger, "[handle_order_insert] Too Many Requests, (limitEndTime)" << limitEndTime);
            errorId = 203;
            errorMsg = "429 -- Too Many  Requests, please try again later";
            std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
            on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
            writer_lock.unlock();
            return;
        }
    }

    send_order(unit,stPendingOrderStatus, ticker.c_str(), fixedVolume, fixedPrice,is_post_only(&data),d);
    
    if (!d.IsObject() || !d.HasMember("code"))
    {
        errorId = 100;
        errorMsg = "send_order http response has parse error or is not json. please check the log";
        KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
            errorId << " (errorMsg) " << errorMsg);
    }
    else {
        int code = std::round(std::stod(d["code"].GetString()));
        if (d.HasMember("data") && code == 200000)
        {
            //if send successful and the exchange has received ok, then add to  pending query order list
            std::string remoteOrderId = d["data"]["orderId"].GetString();
            //fix defect of use the old value
            KF_LOG_INFO(logger, "[req_order_insert] after send  (rid)" << requestId << " (OrderRef) " <<
                data.OrderRef << " (remoteOrderId) "
                << remoteOrderId);

            std::lock_guard<std::mutex> lck(*m_mutexOrder);
            auto it = m_mapNewOrder.find(stPendingOrderStatus.strClientId);
            if (it != m_mapNewOrder.end())
            {//websocket信息尚未到达
                it->second.nSendTime = getTimestamp();//记录当前时间
                KF_LOG_INFO(logger, "zaftime:" << it->second.nSendTime);
            }

            //success, only record raw data
            //raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_KUCOIN, 1,requestId, errorId, errorMsg.c_str());
            KF_LOG_DEBUG(logger, "[req_order_insert] success");
            KF_LOG_INFO(logger, "[req_order_insert] data.OrderPriceType=" << data.OrderPriceType);
            return;
        }
        else if (d.HasMember("msg") && d["msg"].IsString()) {
            string msg = d["msg"].GetString();
            if (msg != "success") {
                errorId = code;
                errorMsg = d["msg"].GetString();
                KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
            }
        }
    }
    
    if(errorId != 0)
    {
        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        send_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();
    }
}

void TDEngineKuCoin::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitKuCoin& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);

    std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_KUCOIN, 1/*ISLAST*/, requestId);
    writer_lock.unlock();

    int errorId = 0;
    std::string errorMsg = "";
    writer_lock.lock();
    on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
    writer_lock.unlock();
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_insert]: not in WhiteList, ignore it  (rid)" << requestId <<
                                                                                      " (errorId)" << errorId << " (errorMsg) " << errorMsg);

        writer_lock.lock();
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        send_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();

        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);

    if(nullptr == m_ThreadPoolPtr)
    {
        handle_order_insert(unit,*data,requestId,ticker);
    }
    else
    {
        KF_LOG_DEBUG(logger, "[req_order_insert] m_ThreadPoolPtr commit thread, idlThrNum" << m_ThreadPoolPtr->idlCount());
        m_ThreadPoolPtr->commit(std::bind(&TDEngineKuCoin::handle_order_insert,this,unit,*data,requestId,ticker));
    }
}

//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
void TDEngineKuCoin::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId, int64_t sentNameTime)
{
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;
    newOrderActionSent.nSendTime = sentNameTime;
    newOrderActionSent.sentNameTime = getTimestamp();
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    KF_LOG_INFO(logger,"addRemoteOrderIdOrderActionSentTime :"<<remoteOrderId);
    remoteOrderIdOrderActionSentTime[remoteOrderId] = newOrderActionSent;
}

void TDEngineKuCoin::handle_order_action(AccountUnitKuCoin& unit,const LFOrderActionField data, int requestId,const std::string& ticker)
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

        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        send_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();

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
        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();
        return;
    }

    if (rateLimitExceeded) {
        if (getTimestamp() > limitEndTime) {
            rateLimitExceeded = false;
        }
        else {
            KF_LOG_DEBUG(logger, "[handle_order_action] Too Many Requests, (limitEndTime)" << limitEndTime);
            errorId = 203;
            errorMsg = "429 -- Too Many  Requests, please try again later";
            std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
            on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
            writer_lock.unlock();
            return;
        }
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

    if(errorId==0)
    {
        addRemoteOrderIdOrderActionSentTime(&data,requestId,remoteOrderId,sentNameTime);
    }

    if(errorId != 0)
    {
        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        send_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();
    } 
}
void TDEngineKuCoin::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitKuCoin& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_KUCOIN, 1, requestId);
    writer_lock.unlock();

    int errorId = 0;
    std::string errorMsg = "";
    writer_lock.lock();
    on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
    writer_lock.unlock();
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);

        writer_lock.lock();
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        send_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();

        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);

    if(nullptr == m_ThreadPoolPtr)
    {
        handle_order_action(unit,*data,requestId,ticker);
    }
    else
    {
        KF_LOG_DEBUG(logger, "[req_order_action] m_ThreadPoolPtr commit thread, idlThrNum" << m_ThreadPoolPtr->idlCount());
        m_ThreadPoolPtr->commit(std::bind(&TDEngineKuCoin::handle_order_action,this,unit,*data,requestId,ticker));
    }
    
}

///****************************************************
std::string TDEngineKuCoin::createInsertOrdertring_withdraw(string currency, string volume, string address, string tag)
{
    //KF_LOG_INFO(logger, "[TDEngineKuCoin::createInsertOrdertring]:(price)"<<price << "(volume)" << size);
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("currency");
    writer.String(currency.c_str());

    writer.Key("address");
    writer.String(address.c_str());
    writer.Key("amount");
    writer.String(volume.c_str());
    writer.Key("memo");
    writer.String(tag.c_str());
   
    writer.EndObject();
    std::string str = s.GetString();
    return str;
}


void TDEngineKuCoin::withdrawl_currency(string currency, string volume, string address, string tag, Document& json, AccountUnitKuCoin& unit){
    string path = "/api/v1/withdrawals";
    auto response=Post(path,createInsertOrdertring_withdraw(currency,volume,address,tag),unit,unit.api_key,unit.secret_key,unit.passphrase);
    json.Parse(response.text.c_str());
}

void TDEngineKuCoin::req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId){
    AccountUnitKuCoin& unit = account_units[account_index];
    //unit.withdrawl_key = data->Key;
    KF_LOG_DEBUG(logger, "[req_withdraw_currency]"<<"(currency)"<<data->Currency
                                                   <<"(amount)"<<data->Volume
                                                   <<"(address)"<< data->Address
                                                   <<"(memo)"<< data->Tag);

    std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
    send_writer->write_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_KUCOIN, 1, requestId);
    writer_lock.unlock();

    int errorId = 0;
    std::string errorMsg = "";


    std::string currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
    if(currency.length() == 0) 
    {
        errorId = 200;
        errorMsg = std::string(data->Currency) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_withdraw_currency]: not in WhiteList, ignore it  (rid)" << requestId <<
                                                                                      " (errorId)" << errorId << " (errorMsg) " << errorMsg);

        writer_lock.lock();
        on_rsp_withdraw(data, requestId, errorId, errorMsg.c_str());
        send_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();

        return;
    }
    KF_LOG_DEBUG(logger, "[req_withdraw_currency] (exchange_currency)" << currency);

    string address = data->Address, tag = data->Tag;
    if(address == "")
    {
        errorId = 520;
        errorMsg = "address is null";
        KF_LOG_ERROR(logger,"[req_withdraw_currency] address is null");
  
        writer_lock.lock();
        on_rsp_withdraw(data, requestId, errorId, errorMsg.c_str());
        send_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_KUCOIN, 1, 
            requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();

        return;
    }
    Document json;
    withdrawl_currency(currency,std::to_string((data->Volume)/pow(10,8)),address,tag,json,unit);
    if(!json.IsObject())
    {
        errorId = 100;
        //errorMsg = "send_order http response has parse error or is not json. please check the log";
        errorMsg = "req_withdraw_currency http response has parse error or is not json. please check the log";
        KF_LOG_ERROR(logger, "[req_withdraw_currency] req_withdraw_currency error!  (WithdrawalId)" << requestId << " (errorId)" <<errorId << " (errorMsg) " << errorMsg);
    } else  if(json.HasMember("code"))
    {
        int code =std::round(std::stod(json["code"].GetString()));
        if(code == 200000) {
            auto& datas = json["data"];
            std::string withdrawalId = datas["withdrawalId"].GetString();
            KF_LOG_INFO(logger, "[req_withdraw_currency] after send  (WithdrawalId)" << withdrawalId);  
            LFWithdrawField* dataCpy = (LFWithdrawField*)data;
            strncpy(dataCpy->ID,withdrawalId.c_str(),64);
            //success, only record raw data

            writer_lock.lock();
            send_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_KUCOIN, 1,requestId, errorId, errorMsg.c_str());
            writer_lock.unlock();

            KF_LOG_DEBUG(logger, "[req_withdraw_currency] success" );
            //return;

        }else {
            errorId = code;
            if(json.HasMember("msg") && json["msg"].IsString())
            {
                errorMsg = json["msg"].GetString();
            }
            //KF_LOG_ERROR(logger, "[req_withdraw] send_order error!  (WithdrawalId)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
            KF_LOG_ERROR(logger, "[req_withdraw_currency] req_withdraw_currency error!  (WithdrawalId)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
    }
    //if(errorId != 0)
    //{
    writer_lock.lock();
        on_rsp_withdraw(data, requestId, errorId, errorMsg.c_str());
        send_writer->write_error_frame(&data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();
   // }
}


//*****************************************************
std::string TDEngineKuCoin::createInsertOrdertring_inner_transfer(string clientOid,string currency, string from, string to, string amount)
{    
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("clientOid");
    writer.String(clientOid.c_str());
    writer.Key("currency");
    writer.String(currency.c_str());
    writer.Key("from");
    writer.String(from.c_str());
    writer.Key("to");
    writer.String(to.c_str());
    writer.Key("amount");
    writer.String(amount.c_str());
   
    writer.EndObject();
    std::string str = s.GetString();
    return str;
}



void TDEngineKuCoin::transfer_inner(string clientOid, string currency, string from, string to,string amount,
                                    Document& json, AccountUnitKuCoin& unit,std::string api_key,std::string secret_key,std::string passphrase)
{
    string path = "/api/v2/accounts/inner-transfer";
    auto response=Post(path,createInsertOrdertring_inner_transfer(clientOid,currency,from,to,amount),unit,api_key,secret_key,passphrase);
    json.Parse(response.text.c_str());
}



void TDEngineKuCoin::req_transfer_inner(const LFTransferField* data,int account_index,int requestId,string from,string to,std::string api_key,std::string secret_key,std::string passphrase)
{
    AccountUnitKuCoin& unit =account_units[account_index];
    string clientOid=genClinetid("A");
    KF_LOG_DEBUG(logger, "[req_transfer_inner]"<<"(clientOid)"<<clientOid
                                               <<"(currency)"<<data->Currency
                                               <<"(from)"<<from
                                               <<"(to)"<< to
                                               <<"(amount)"<<data->Volume);
    
    //send_writer->write_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_KUCOIN, 1, requestId);
    
    int errorId = 0;
    std::string errorMsg = "";

    std::string currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
    if(currency.length() == 0) 
    {
        errorId = 200;
        errorMsg = std::string(data->Currency) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_transfer_inner]: not in WhiteList, ignore it  (rid)" << requestId <<
                                                                                      " (errorId)" << errorId << " (errorMsg) " << errorMsg);

        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_transfer(data, requestId,errorId, errorMsg.c_str());
        send_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();

        return;
    }
    KF_LOG_DEBUG(logger, "[req_transfer_inner] (exchange_currency)" << currency);

    auto it = unit.mapCurrencyList.find(currency);
    int min_withdrawl_size=it->second.withdrawalMinSize;
    double amount=data->Volume/pow(10,8);
    if(amount <(min_withdrawl_size/pow(10,8)))
    {
        errorId=101;
        errorMsg="The transfer amount is less than withdrawalMinSize";
        KF_LOG_ERROR(logger, "[req_transfer_inner] req_transfer_inner error!  (orderId)" << requestId << " (errorId)" <<errorId << " (errorMsg) " << errorMsg);
    }
    else
    {   
        string strvolume = to_string(data->Volume);
        string fixvolume;
        dealnum(strvolume, fixvolume);
        //int precision=it->second.precision;
        Document json;
        //char volume[50];
        //sprintf(volume,"%8lf",amount);
        transfer_inner(clientOid,currency,from,to,fixvolume,json,unit,api_key,secret_key,passphrase);
        
        if(!json.IsObject())
        {
            errorId = 100;        
            errorMsg = "req_transfer_inner http response has parse error or is not json. please check the log";
            KF_LOG_ERROR(logger, "[req_transfer_inner] req_transfer_inner error!  (orderId)" << requestId << " (errorId)" <<errorId << " (errorMsg) " << errorMsg);
        } 
        else  if(json.HasMember("code"))
        {
            int code =std::round(std::stod(json["code"].GetString()));
            if(code == 200000) 
            {
                if(json.HasMember("msg") && json["msg"].IsString())
                {
                    errorId = code;
                    errorMsg = json["msg"].GetString();
                }
                KF_LOG_INFO(logger, "[req_transfer_inner] after send  (orderId)" << requestId);                                                                           
                KF_LOG_DEBUG(logger, "[req_transfer_inner] success" );


            }
            else 
            {
                errorId = code;
                if(json.HasMember("msg") && json["msg"].IsString())
                {
                   errorMsg = json["msg"].GetString();
                }
            
                KF_LOG_ERROR(logger, "[req_transfer_inner] req_transfer_inner error!  (orderId)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
            }
        }

    }    
    std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
    on_rsp_transfer(data, requestId,errorId, errorMsg.c_str());
    writer_lock.unlock();
   /* if(errorId!=0)     
    {
        send_writer->write_error_frame(&data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
    }*/
    
}

std::string TDEngineKuCoin::createInsertOrdertring_sub_transfer(string clientOid,string currency, string amount, string direction,
                                                               string subAccountType,string subUserId)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("clientOid");
    writer.String(clientOid.c_str());
    writer.Key("currency");
    writer.String(currency.c_str());
    writer.Key("amount");
    writer.String(amount.c_str());
    writer.Key("direction");
    writer.String(direction.c_str());    
    writer.Key("subAccountType");
    writer.String(subAccountType.c_str());
    writer.Key("subUserId");
    writer.String(subUserId.c_str());
   
    writer.EndObject();
    std::string str = s.GetString();
    return str;
}



void TDEngineKuCoin::transfer_sub(string clientOid, string currency, string amount, string direction,
                                 string subAccountType,string subUserId,Document& json, AccountUnitKuCoin& unit,std::string api_key,std::string secret_key,std::string passphrase)
{
    string path = "/api/v1/accounts/sub-transfer";
    auto response=Post(path,createInsertOrdertring_sub_transfer(clientOid,currency,amount,direction,subAccountType,subUserId),unit,api_key,secret_key,passphrase);
    json.Parse(response.text.c_str());
}

void TDEngineKuCoin::dealnum(string pre_num,string& fix_num)
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

void TDEngineKuCoin::req_transfer_sub(const LFTransferField* data,int account_index,int requestId,string direction,string subAccountType,string subUserId,std::string api_key,std::string secret_key,std::string passphrase)
{
    AccountUnitKuCoin& unit =account_units[account_index];
    string clientOid=genClinetid("A");
    KF_LOG_DEBUG(logger, "[req_transfer_sub]"<<"(clientOid)"<<clientOid
                                             <<"(currency)"<<data->Currency
                                             <<"(amount)"<< data->Volume
                                             <<"(direction)"<< direction
                                             <<"(subAccountType)"<<subAccountType
                                             <<"(subUserId)"<<subUserId);
    
    //send_writer->write_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_TRANSFER_SUB_KUCOIN, 1, requestId);
    
    int errorId = 0;
    std::string errorMsg = "";

    std::string currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
    if(currency.length() == 0) 
    {
        errorId = 200;
        errorMsg = std::string(data->Currency) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_transfer_sub]: not in WhiteList, ignore it  (rid)" << requestId <<
                                                                                      " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_transfer(data, requestId,errorId, errorMsg.c_str());
        send_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();
        return;

    }
    KF_LOG_DEBUG(logger, "[req_transfer_sub] (exchange_currency)" << currency);

    auto it = unit.mapCurrencyList.find(currency);
    int min_withdrawl_size=it->second.withdrawalMinSize;
    double amount=data->Volume/pow(10,8);
    if(amount < (min_withdrawl_size/pow(10,8)))
    {
        errorId=101;
        errorMsg="The transfer amount is nless than withdrawalMinSize";
        KF_LOG_ERROR(logger, "[req_transfer_sub] req_transfer_inner error!  (orderId)" << requestId << " (errorId)" <<errorId << " (errorMsg) " << errorMsg);
    }
    else
    {
        string strvolume = to_string(data->Volume);
        string fixvolume;
        dealnum(strvolume, fixvolume);
        Document json;
        //char volume[50];
        //sprintf(volume,"%8lf",amount);       
        transfer_sub(clientOid,currency,fixvolume,direction,subAccountType,subUserId,json,unit,api_key,secret_key,passphrase);
        
        if(!json.IsObject())
        {
            errorId = 100;        
            errorMsg = "req_transfer_sub http response has parse error or is not json. please check the log";
            KF_LOG_ERROR(logger, "[req_transfer_sub] req_transfer_sub error!  (orderId)" << requestId << " (errorId)" <<errorId << " (errorMsg) " << errorMsg);
        } 
        else  if(json.HasMember("code"))
        {
            int code =std::round(std::stod(json["code"].GetString()));
            if(code == 200000) 
            {
                if(json.HasMember("msg") && json["msg"].IsString())
                {
                    errorId = code;
                    errorMsg = json["msg"].GetString();
                }
                KF_LOG_INFO(logger, "[req_transfer_sub] after send  (orderId)" << requestId);      
            
                KF_LOG_DEBUG(logger, "[req_transfer_sub] success" );
            

            }
             else 
            {
                errorId = code;
                if(json.HasMember("msg") && json["msg"].IsString())
                {
                     errorMsg = json["msg"].GetString();
                }
            
                KF_LOG_ERROR(logger, "[req_transfer_sub] req_transfer_sub error!  (orderId)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
            }
        }

    }
    std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
    on_rsp_transfer(data, requestId,errorId, errorMsg.c_str());
    writer_lock.unlock();
   /* if(errorId != 0)
    {        
        send_writer->write_error_frame(&data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_TRANSFER_SUB_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
    }*/
}

void TDEngineKuCoin::req_inner_transfer(const LFTransferField* data,int account_index,int requestId)
{
    KF_LOG_INFO(logger, "[req_inner_transfer] ");
    AccountUnitKuCoin& unit =account_units[account_index];
    int errorId;
    std::string errorMsg = "";

    std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
    send_writer->write_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_KUCOIN, 1, requestId);
    writer_lock.unlock();

    std::string name = data->FromName;
    std::string from_begin = data->From;
    from_begin = from_begin.substr(0,from_begin.find("-"));
    std::string to_begin = data->To;
    to_begin = to_begin.substr(0,to_begin.find("-"));
    std::string from_end = data->From;
    from_end = from_end.substr(from_end.find("-")+1,from_end.length());
    std::string to_end = data->To;
    to_end = to_end.substr(to_end.find("-")+1,to_end.length());
    KF_LOG_INFO(logger,"from_begin:"<<from_begin<<" from_end"<<from_end<<" to_begin"<<to_begin<<" to_end"<<to_end);
    std::string api_key,secret_key,passphrase;
    if(from_begin == "sub" && to_begin == "sub")
    {
        auto itr = sub_map.find(name);
        if(itr == sub_map.end()){
            errorId = 200;
            errorMsg = "not find name";
            std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
            on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
            writer_lock.unlock();
            return;
        }else{
            api_key = itr->second.api_key;
            secret_key = itr->second.secret_key;
            passphrase = itr->second.passphrase;
        }
    }
    else
    {
        api_key = unit.api_key;
        secret_key = unit.secret_key;
        passphrase = unit.passphrase;
    }
    KF_LOG_INFO(logger,"api_key1:"<<api_key);
    //master-main --> sub-main / master-main --> sub-trade 
    if((strcmp(data->From,"master-main")==0) && ((strcmp(data->To,"sub-main")==0) || (strcmp(data->To,"sub-trade")==0) || (strcmp(data->To,"sub-margin")==0)))
    {
        KF_LOG_INFO(logger, "[req_transfer_sub] (from) "<< data->From<<"(to)"<<data->To);
        //AccountUnitKuCoin& unit =account_units[account_index];

        string direction;
        string subAccountType;
        string subUserId;

        direction="OUT";
        if(strcmp(data->To,"sub-main")==0)
        {
           subAccountType="MAIN";
           KF_LOG_INFO(logger, "[req_transfer_sub] (subAccountType) "<< "main");
        }
        else if(strcmp(data->To,"sub-trade")==0)
        {
           subAccountType="TRADE";
           KF_LOG_INFO(logger, "[req_transfer_sub] (subAccountType) "<< "trade");
        }
        else if(strcmp(data->To,"sub-margin")==0)
        {
           subAccountType="MARGIN";
           KF_LOG_INFO(logger, "[req_transfer_sub] (subAccountType) "<< "trade");
        }
        else 
        {
            KF_LOG_INFO(logger, "[req_transfer_sub] (subAccountType) "<< subAccountType);
        }
        auto it = unit.mapSubUser.find(data->ToName);
        if(it!=unit.mapSubUser.end())
        {
            subUserId=it->second.userId;
            KF_LOG_INFO(logger, "[req_transfer_sub] (subUserId) "<< subUserId);
        }        
        else 
        {
            KF_LOG_INFO(logger, "[req_transfer_sub] (subUserId) not found ");
        }

        req_transfer_sub(data,account_index,requestId,direction,subAccountType,subUserId,api_key,secret_key,passphrase);
    }
    //sub-main --> master-main  sub-trade --> master-mian:
    else if(( (strcmp(data->From,"sub-main")==0) || (strcmp(data->From,"sub-trade")==0) || (strcmp(data->From,"sub-margin")==0) ) && (strcmp(data->To,"master-main")==0))
    {
        KF_LOG_INFO(logger, "[req_transfer_sub] (from) "<< data->From<<"(to)"<<data->To);
        //AccountUnitKuCoin& unit =account_units[account_index];

        string direction;
        string subAccountType;
        string subUserId;

        direction="IN";
        if(strcmp(data->From,"sub-main")==0)
        {
            subAccountType="MAIN";
            KF_LOG_INFO(logger, "[req_transfer_sub] (subAccountType) "<< "main");
        }
        else if(strcmp(data->From,"sub-trade")==0)
        {
           subAccountType="TRADE";
           KF_LOG_INFO(logger, "[req_transfer_sub] (subAccountType) "<< "trade");
        }
        else if(strcmp(data->From,"sub-margin")==0)
        {
           subAccountType="MARGIN";
           KF_LOG_INFO(logger, "[req_transfer_sub] (subAccountType) "<< "margin");
        }
        else
        {
            KF_LOG_INFO(logger, "[req_transfer_sub] (subAccountType) "<< subAccountType);
        }
        auto it = unit.mapSubUser.find(data->FromName);
        if(it!=unit.mapSubUser.end())
        {
            subUserId=it->second.userId;
            KF_LOG_INFO(logger, "[req_transfer_sub] (subUserId) "<< subUserId);
        }
        else
        {
            KF_LOG_INFO(logger, "[req_transfer_sub] (subUserId) not found");
        }

        req_transfer_sub(data,account_index,requestId,direction,subAccountType,subUserId,api_key,secret_key,passphrase);
    }
    else if(from_begin == to_begin)
    {
        KF_LOG_INFO(logger,"inner1");
        req_transfer_inner(data,account_index,requestId,from_end,to_end,api_key,secret_key,passphrase);
    }
    //sub-mian --> sub-trade [34m~H~V[34m~@~E sub-trade --> sub-main :[34m~F~E[34m~C�[34m~D[34m~G~Q[34m~H~R转
    /*else if(((strcmp(data->From,"sub-main")==0) && (strcmp(data->To,"sub-trade")==0)) || ((strcmp(data->From,"sub-trade")==0) && (strcmp(data->To,"sub-main")==0)))
    {
        KF_LOG_INFO(logger, "[req_transfer_inner] (from) "<< data->From<<"(to)"<<data->To);
	string from;
        string to;

        if(strcmp(data->From,"sub-main")==0)
        {
            from="main";
            KF_LOG_INFO(logger, "[req_transfer_inner] (from) "<< "main");
        }
        else if(strcmp(data->From,"sub-trade")==0)
        {
            from="trade";
            KF_LOG_INFO(logger, "[req_transfer_inner] (from) "<< "trade");
        }

        if(strcmp(data->To,"sub-trade")==0)
        {
            to="trade";
            KF_LOG_INFO(logger, "[req_transfer_inner] (to) "<< "trade");
        }
        else if(strcmp(data->To,"sub-main")==0)
        {
            to="main";
            KF_LOG_INFO(logger, "[req_transfer_inner] (to) "<< "main");
        }
        req_transfer_inner(data,account_index,requestId,from,to);
    }
    //master-main --> main-trade [34m~H~V[34m~@~E master-trade --> master-main :[34m~F~E[34m~C�[34m~D[34m~G~Q[34m~H~R转
    else if(((strcmp(data->From,"master-main")==0) && (strcmp(data->To,"master-trade")==0)) || ((strcmp(data->From,"master-trade")==0) && (strcmp(data->To,"master-main")==0)))
    {
        KF_LOG_INFO(logger, "[req_transfer_inner] (from) "<< data->From<<"(to)"<<data->To);
    	
	string from;
        string to;

        if(strcmp(data->From,"master-main")==0)
        {
            from="main";
            KF_LOG_INFO(logger, "[req_transfer_inner] (from) "<< "main");
        }
        else if(strcmp(data->From,"master-trade")==0)
        { 
            from="trade";
            KF_LOG_INFO(logger, "[req_transfer_inner] (from) "<< "trade");
        }

        if(strcmp(data->To,"master-trade")==0)
        {
            to="trade";
            KF_LOG_INFO(logger, "[req_transfer_inner] (to) "<< "trade");
        }
        else if(strcmp(data->To,"master-main")==0)
        {
            to="main";
            KF_LOG_INFO(logger, "[req_transfer_inner] (to) "<< "main");
        }
        req_transfer_inner(data,account_index,requestId,from,to);
    }   */
}
/********************************************************************************/

void TDEngineKuCoin::currency_list(Document& json, AccountUnitKuCoin& unit)
{
    string path = "/api/v1/currencies";
    auto response=Get(path,"{}",unit);
    json.Parse(response.text.c_str());
}

void TDEngineKuCoin::req_currencylist(AccountUnitKuCoin& unit)
{
    KF_LOG_DEBUG(logger, "[req_currencylist]");

    Document json;
    currency_list(json,unit);
    const static std::string strSuccesse = "200000";
    if(json.HasMember("code") && (json["code"].GetString()==strSuccesse) && json.HasMember("data") )
    {
        int i;
        int len = json["data"].Size();
        auto datas = json["data"].GetArray();
        for(i=0;i<len;i++)
        {
            rapidjson::Value account=datas[i].GetObject();

            CurrencyList currencylist;
            currencylist.currency=account["currency"].GetString();
            currencylist.name=account["name"].GetString();
            currencylist.fullName=account["fullName"].GetString();
            currencylist.precision=account["precision"].GetInt();
            currencylist.isWithdrawEnabled=account["isWithdrawEnabled"].GetBool();
            currencylist.isDepositEnabled=account["isDepositEnabled"].GetBool();
            currencylist.withdrawalMinFee=std::round(std::stod(account["withdrawalMinFee"].GetString())* scale_offset);
            currencylist.withdrawalMinSize=std::round(std::stod(account["withdrawalMinSize"].GetString())* scale_offset);
        
            unit.mapCurrencyList.insert(std::make_pair(currencylist.currency,currencylist));
            KF_LOG_INFO(logger, "[req_currencylist] (currency )"<<currencylist.currency) ;
        }
    }
    
}

void TDEngineKuCoin::sub_user(Document& json, AccountUnitKuCoin& unit)
{
    string path = "/api/v1/sub/user";
    auto response=Get(path,"{}",unit);
    json.Parse(response.text.c_str());
}

void TDEngineKuCoin::get_sub_user(AccountUnitKuCoin& unit)
{
    KF_LOG_DEBUG(logger, "[get_sub_user]");

    Document json;
    sub_user(json,unit);
    const static std::string strSuccesse = "200000";
    if(json.HasMember("code") && (json["code"].GetString()==strSuccesse) && json.HasMember("data") && json["data"].IsArray())
    {
        int i;
        int len = json["data"].Size();
        auto datas = json["data"].GetArray();
        for(i=0;i<len;i++)
        {
            rapidjson::Value account=datas[i].GetObject();

            SubUser subuser;
            subuser.userId=account["userId"].GetString();
            subuser.subName=account["subName"].GetString();
            //subuser.remarks=account["remarks"].GetString();
        
            unit.mapSubUser.insert(std::make_pair(subuser.subName,subuser));
            KF_LOG_INFO(logger, "[get_sub_user] (userId )"<<subuser.userId);
        }
    }
    
}
//************************************************************************************************************
//************************************************************************************************************
std::string TDEngineKuCoin::createInsertWithdrawals_list(string currency,string status,string startAt,string endAt)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("currency");
    writer.String(currency.c_str());

    writer.Key("status");
    writer.String(status.c_str());
    writer.Key("startAt");
    writer.String(startAt.c_str());
    writer.Key("endAt");
    writer.String(endAt.c_str());
   
    writer.EndObject();
    std::string str = s.GetString();
    return str;
}

int TDEngineKuCoin::get_transfer_status(std::string status)
{
    int local_status =LFTransferStatus::TRANSFER_STRATUS_SUCCESS;
    if(status == "FAILURE")
    {
        local_status = LFTransferStatus::TRANSFER_STRATUS_FAILURE;
    }
    else if(status == "PROCESSING" || status == "WALLET_PROCESSING")
    {
        local_status = LFTransferStatus::TRANSFER_STRATUS_PROCESSING;
    }
    return local_status;
}
std::string TDEngineKuCoin::get_query_transfer_status(int status)
{
    std::string query_status = "SUCCESS";
    switch(status)
    {
        case TRANSFER_STRATUS_PROCESSING:
            query_status = "PROCESSING";
            break;
        case TRANSFER_STRATUS_CANCELED:
        case TRANSFER_STRATUS_FAILURE:
        case TRANSFER_STRATUS_REJECTED:
            query_status = "FAILURE";
            break;
        default:
            break;
    }
    return query_status;
}

void TDEngineKuCoin::withdrawals_list(Document& json,string currency,string status,string startAt,string endAt,AccountUnitKuCoin& unit)
{
    string path = "/api/v1/withdrawals";
    path+=+"?currency="+currency+"&status="+status+"&startAt="+startAt+"&endAt="+endAt;
    auto response=Get(path,"",unit);
    //auto response=Get(path,createInsertWithdrawals_list(currency,status,startAt,endAt),unit);
    json.Parse(response.text.c_str());
}

void TDEngineKuCoin::req_withdrawals_list(string currency,long startAt,long endAt,string status)
{
    KF_LOG_DEBUG(logger, "[req_withdrawals_list]");
    KF_LOG_INFO(logger,"[req_withdrawals_list] (currency)"<<currency<<"(startAt)"<<startAt<<"(endAt)"<<endAt);

    AccountUnitKuCoin& unit=account_units[0];
    Document json;
    string startAt_1=std::to_string(startAt);
    string endAt_1=std::to_string(endAt);
    withdrawals_list(json,currency,status,startAt_1,endAt_1,unit);

    const static std::string strSuccesse = "200000";
    if(json.HasMember("code") && (json["code"].GetString()==strSuccesse) && json.HasMember("data") )
    {
        auto& datas = json["data"];

        WithdrawalsList withdrawalslist;
        withdrawalslist.currentPage=datas["currentPage"].GetInt();
        withdrawalslist.pageSize=datas["pageSize"].GetInt();
        withdrawalslist.totalNum=datas["totalNum"].GetInt();
        withdrawalslist.totalPage=datas["totalPage"].GetInt();

        KF_LOG_INFO(logger, "[req_withdrawals_list] (currentPage)"<<withdrawalslist.currentPage<<"(pageSize)"<<withdrawalslist.pageSize
                                                    <<"(totalNum)"<<withdrawalslist.totalNum<<"(totalPage)"<<withdrawalslist.totalPage);

        auto array=datas["items"].GetArray();
        int len = datas["items"].Size();
        int i;
        for(i=0;i<len;i++)
        {
            auto& account=array[i];
            //KF_LOG_INFO(logger,"[req_withdrawals_list] (account)"<<account);
            
            withdrawalslist.items.address=account["address"].GetString();
            KF_LOG_INFO(logger,"[req_withdrawals_list] (address)"<<withdrawalslist.items.address);

            withdrawalslist.items.memo=account["memo"].GetString();
            KF_LOG_INFO(logger,"[req_withdrawals_list] (memo)"<<withdrawalslist.items.memo);

            withdrawalslist.items.currency=account["currency"].GetString();
            KF_LOG_INFO(logger,"[req_withdrawals_list] (currency)"<<withdrawalslist.items.currency);

            withdrawalslist.items.amount=std::stod(account["amount"].GetString());
            KF_LOG_INFO(logger,"[req_withdrawals_list] (amount)"<<withdrawalslist.items.amount);

            withdrawalslist.items.fee=std::stod(account["fee"].GetString());
            KF_LOG_INFO(logger,"[req_withdrawals_list] (fee)"<<withdrawalslist.items.fee);

            withdrawalslist.items.walletTxId=account["walletTxId"].GetString();
            KF_LOG_INFO(logger,"[req_withdrawals_list] (walletTxId)"<<withdrawalslist.items.walletTxId);

            withdrawalslist.items.isInner=account["isInner"].GetBool();
            KF_LOG_INFO(logger,"[req_withdrawals_list] (isInner)"<<withdrawalslist.items.isInner);

            withdrawalslist.items.status=account["status"].GetString();
            KF_LOG_INFO(logger,"[req_withdrawals_list] (status)"<<withdrawalslist.items.status);

            withdrawalslist.items.createdAt=account["createdAt"].GetInt64();
            KF_LOG_INFO(logger,"[req_withdrawals_list] (createdAt)"<<withdrawalslist.items.createdAt);

            withdrawalslist.items.updatedAt=account["updatedAt"].GetInt64();
            KF_LOG_INFO(logger,"[req_withdrawals_list] (updatedAt)"<<withdrawalslist.items.updatedAt);
        }
        unit.mapWithdrawalsList.insert(std::make_pair(currency,withdrawalslist));    
            
    }


}
//***************************************************************************************************************
//***************************************************************************************************************
std::string TDEngineKuCoin::createInsertDeposits_list(string currency,string status,string startAt,string endAt)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("currency");
    writer.String(currency.c_str());

    writer.Key("status");
    writer.String(status.c_str());
    writer.Key("startAt");
    writer.String(startAt.c_str());
    writer.Key("endAt");
    writer.String(endAt.c_str());
   
    writer.EndObject();
    std::string str = s.GetString();
    return str;
}

void TDEngineKuCoin::deposits_list(Document& json,string currency,string status,string startAt,string endAt,AccountUnitKuCoin& unit)
{
    string path = "/api/v1/deposits";
     path+=+"?currency="+currency+"&status="+status+"&startAt="+startAt+"&endAt="+endAt;
    auto response=Get(path,"",unit);
    //auto response=Get(path,createInsertDeposits_list(currency,status,startAt,endAt),unit);

    json.Parse(response.text.c_str());
}

void TDEngineKuCoin::req_deposits_list(string currency,long startAt,long endAt,string status)
{
    KF_LOG_DEBUG(logger, "[req_deposits_list]");
    KF_LOG_INFO(logger,"[req_deposits_list] (currency)"<<currency<<"(startAt)"<<startAt<<"(endAt)"<<endAt);

    AccountUnitKuCoin& unit=account_units[0];
    Document json;
    string startAt_1=std::to_string(startAt);
    string endAt_1=std::to_string(endAt);
    deposits_list(json,currency,status,startAt_1,endAt_1,unit);

    const static std::string strSuccesse = "200000";
    if(json.HasMember("code") && (json["code"].GetString()==strSuccesse) && json.HasMember("data") )
    {
        auto& datas = json["data"];

        DepositsList depositslist;
        depositslist.currentPage=datas["currentPage"].GetInt();
        depositslist.pageSize=datas["pageSize"].GetInt();
        depositslist.totalNum=datas["totalNum"].GetInt();
        depositslist.totalPage=datas["totalPage"].GetInt();

        KF_LOG_INFO(logger, "[req_deposits_list] (currentPage)"<<depositslist.currentPage<<"(pageSize)"<<depositslist.pageSize
                                                    <<"(totalNum)"<<depositslist.totalNum<<"(totalPage)"<<depositslist.totalPage); 

        auto array=datas["items"].GetArray();
        int len = datas["items"].Size();
        int i;
        for(i=0;i<len;i++)
        {
            auto& account=array[i];
            //KF_LOG_INFO(logger,"[req_deposits_list] (account)"<<account);

            depositslist.items.address=account["address"].GetString();
            KF_LOG_INFO(logger,"[req_deposits_list] (address)"<<depositslist.items.address);

            depositslist.items.memo=account["memo"].GetString();
            KF_LOG_INFO(logger,"[req_deposits_list] (memo)"<<depositslist.items.memo);

            depositslist.items.amount=std::stod(account["amount"].GetString());
            KF_LOG_INFO(logger,"[req_deposits_list] (amount)"<<depositslist.items.amount);

            depositslist.items.fee=std::stod(account["fee"].GetString());
            KF_LOG_INFO(logger,"[req_deposits_list] (fee)"<<depositslist.items.fee);

            depositslist.items.currency=account["currency"].GetString();
            KF_LOG_INFO(logger,"[req_deposits_list] (currency)"<<depositslist.items.currency);

            depositslist.items.isInner=account["isInner"].GetBool();
            KF_LOG_INFO(logger,"[req_deposits_list] (isInner)"<<depositslist.items.isInner);

            depositslist.items.walletTxId=account["walletTxId"].GetString();
            KF_LOG_INFO(logger,"[req_deposits_list] (walletTxId)"<<depositslist.items.walletTxId);

            depositslist.items.status=account["status"].GetString();
            KF_LOG_INFO(logger,"[req_deposits_list] (status)"<<depositslist.items.status);

            depositslist.items.createdAt=account["createdAt"].GetInt64();
            KF_LOG_INFO(logger,"[req_deposits_list] (createdAt)"<<depositslist.items.createdAt);

            depositslist.items.updatedAt=account["updatedAt"].GetInt64();
            KF_LOG_INFO(logger,"[req_deposits_list] (updatedAt)"<<depositslist.items.updatedAt);
        }
        unit.mapDepositsList.insert(std::make_pair(currency,depositslist));       
        
    }
    

}

void TDEngineKuCoin::req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw)
{
    KF_LOG_INFO(logger, "[req_transfer_history]");
    AccountUnitKuCoin& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_transfer_history] (api_key)" << unit.api_key);

    LFTransferHistoryField his;
    memset(&his, 0, sizeof(LFTransferHistoryField));
    strncpy(his.UserID, data->UserID, 64);
    strncpy(his.ExchangeID, data->ExchangeID, 11);
    his.IsWithdraw = isWithdraw;
    strcpy(his.ExchangeID, "kucoin");
    int errorId = 0;
    std::string errorMsg = "";
    KF_LOG_INFO(logger, "[req_transfer_history] (data->UserID)" << data->UserID);
    
    string asset = data->Currency;
    KF_LOG_INFO(logger,"asset:"<<asset);
    string startTime = data->StartTime;
    string endTime = data->EndTime;
    KF_LOG_INFO(logger,"startTime="<<startTime<<"endTime="<<endTime);
    std::string status = get_query_transfer_status(data->Status);
    Document d;
    if(!isWithdraw)
    {
        deposits_list(d,asset,status,startTime,endTime,unit);
        KF_LOG_INFO(logger, "[req_transfer_history] req_deposit_history");
    }
    else
    {
        withdrawals_list(d,asset,status,startTime,endTime,unit);
        KF_LOG_INFO(logger, "[req_transfer_history] req_withdraw_history");
    }
    printResponse(d);

    if(d.HasParseError() )
    {
        errorId=100;
        errorMsg= "req_transfer_history http response has parse error. please check the log";
        KF_LOG_ERROR(logger, "[req_transfer_history] req_transfer_history error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    /*
    deposit
        "address": "0x5f047b29041bcfdbf0e4478cdfa753a336ba6989",
        "memo": "5c247c8a03aa677cea2a251d",   
        "amount": 1,
        "fee": 0.0001,
        "currency": "KCS",
        "isInner": false,
        "walletTxId": "5bbb57386d99522d9f954c5a@test004",
        "status": "SUCCESS",
        "remark": "test",
        "createdAt": 1544178843000,
        "updatedAt": 1544178891000
    withdraw
        "id": "5c2dc64e03aa675aa263f1ac",
        "address": "0x5bedb060b8eb8d823e2414d82acce78d38be7fe9",
        "memo": "",
        "currency": "ETH",
        "amount": 1.0000000,
        "fee": 0.0100000,
        "walletTxId": "3e2414d82acce78d38be7fe9",
        "isInner": false,
        "status": "FAILURE",
        "remark": "test",
        "createdAt": 1546503758000,
        "updatedAt": 1546504603000
    */
    std::vector<LFTransferHistoryField> tmp_vector;
    if(!d.HasParseError() && d.IsObject() && d.HasMember("data")){
        Value &node = d["data"]["items"];
        int len = node.Size();
        for ( int i = 0 ; i < len ; i++ ) {
            //char Currency[32];
            //strncpy(Currency, d["depositList"].GetArray()[i]["asset"].GetString(), 32);
            //std::string ticker = unit.positionWhiteList.GetKeyByValue(Currency);
            
            std::string ticker = node.GetArray()[i]["currency"].GetString();
            ticker = unit.positionWhiteList.GetKeyByValue(ticker);
            if(ticker.empty())
                continue;
            strcpy(his.Currency, ticker.c_str());
            int64_t createdAt = node.GetArray()[i]["createdAt"].GetInt64();
            std::string timestamp = std::to_string(createdAt);
            int64_t updatedAt = node.GetArray()[i]["updatedAt"].GetInt64();
            std::string updatetime = std::to_string(updatedAt);
            strncpy(his.TimeStamp, timestamp.c_str(), 32);
            strncpy(his.StartTime, timestamp.c_str(), 32);
            strncpy(his.EndTime, updatetime.c_str(), 32);
            std::string status = node.GetArray()[i]["status"].GetString();
            his.Status = get_transfer_status(status);
            std::string amount = node.GetArray()[i]["amount"].GetString();
            his.Volume = std::round(stod(amount) * scale_offset);
            std::string address = node.GetArray()[i]["address"].GetString();
            strncpy(his.Address, address.c_str(), 130);
            if(node.GetArray()[i].HasMember("id")){
                std::string id = node.GetArray()[i]["id"].GetString();
                strncpy(his.FromID, id.c_str(), 64 );
            }
            //strncpy(his.Tag, d["depositList"].GetArray()[i]["addressTag"].GetString(), 64);
            std::string walletTxId = node.GetArray()[i]["walletTxId"].GetString();
            strncpy(his.TxId, walletTxId.c_str(), 130);
            tmp_vector.push_back(his);
            KF_LOG_INFO(logger,  "[req_deposit_history] (insertTime)" << his.TimeStamp << " (amount)" <<  his.Volume
                                 << " (asset)" << his.Currency<< " (address)" << his.Address <<" (addressTag)" <<his.Tag
                                 << " (txId)" << his.TxId <<" (status)" <<his.Status <<" (requestId)" << requestId );
            KF_LOG_INFO(logger,  "[req_deposit_history1] (insertTime)" << tmp_vector[i].TimeStamp << " (amount)" <<  tmp_vector[i].Volume
                                 << " (asset)" << tmp_vector[i].Currency<< " (address)" << tmp_vector[i].Address <<" (addressTag)" <<tmp_vector[i].Tag
                                 << " (txId)" << tmp_vector[i].TxId <<" (status)" <<tmp_vector[i].Status <<" (requestId)" << requestId );
           
       }        
    }
    else if(d.IsObject() && d.HasMember("code") && d.HasMember("msg"))
    {
        errorId = atoi(d["code"].GetString());
        errorMsg = d["msg"].GetString();
    }

    bool findSymbolInResult = false;
    
    int history_count = tmp_vector.size();
    if(history_count == 0)
    {
        his.Status = -1;
    }
    for (int i = 0; i < history_count; i++)
    {
        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_transfer_history(&tmp_vector[i], isWithdraw, i == (history_count - 1), requestId,errorId, errorMsg.c_str());
        writer_lock.unlock();
        findSymbolInResult = true;
    }

    if(!findSymbolInResult || errorId != 0)
    {
        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
        on_rsp_transfer_history(&his, isWithdraw, 1, requestId,errorId, errorMsg.c_str());
        send_writer->write_error_frame(&his, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
        writer_lock.unlock();
    }
}

void TDEngineKuCoin::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineKuCoin::loop");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineKuCoin::loopwebsocket, this)));
    KF_LOG_INFO(logger, "[set_reader_thread] loopwebsocket threadid="<<std::this_thread::get_id());

    KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineKuCoin::loopOrderActionNoResponseTimeOut");
    orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineKuCoin::loopOrderActionNoResponseTimeOut, this)));
    KF_LOG_INFO(logger, "[set_reader_thread] loopOrderActionNoResponseTimeOut threadid="<<std::this_thread::get_id());
}

void TDEngineKuCoin::loopwebsocket()
{
        KF_LOG_INFO(logger, "[loopwebsocket] loopwebsocket threadid="<<std::this_thread::get_id());
        time_t nLastTime = time(0);

        while(isRunning)
        {
             time_t nNowTime = time(0);
            if(m_isPong && (nNowTime - nLastTime>= 30))
            {
                m_isPong = false;
                nLastTime = nNowTime;
                KF_LOG_INFO(logger, "TDEngineKuCoin::loopwebsocket: last time = " <<  nLastTime << ",now time = " << nNowTime << ",m_isPong = " << m_isPong);
                m_shouldPing = true;
                lws_callback_on_writable(m_conn);  
            }
			lws_service( context, rest_get_interval_ms );
            if(flag.load())
            {
                KF_LOG_INFO(logger,"[loopwebsocket] flag=true");
                flag.store(false);  
                m_isSubL3=false;
                ubsub=false;              
                lws_callback_on_writable(m_conn); 
            }
		}
}


//***********************************************************************************************
//***********************************************************************************************

void TDEngineKuCoin::loopOrderActionNoResponseTimeOut()
{
   KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] loopOrderActionNoResponseTimeOut threadid="<<std::this_thread::get_id());
    KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        for(auto& unit:account_units)
        {
            KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] loop");

            //设置 startTime 和 endTime 为当前时间戳, 设置 mflag and rflag 为 0
            int64_t endTime = getTimestamp();
            int64_t startTime = endTime;
            int64_t mflag = 0;
            int64_t rflag = mflag ;
            std::unique_lock<std::mutex> lck(*m_mutexOrder);  

            //遍历m_mapNewOrder，应该是下单map，设置startTime为最小的nSendTime，若endTime-nSendTime>no_response_wait_ms, 则使mflag++
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

            //遍历remoteOrderIdOrderActionSentTime，应该是撤单map，设置startTime为最小的sentNameTime，若endTime-sentNameTime>no_response_wait_ms, 则使rflag++
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

            //如果endTime - startTime > no_response_wait_ms，说明之前一定有订单超出响应时间限制
            if(endTime - startTime > no_response_wait_ms)
            {
                //检查订单
                //check_orders(unit,startTime,endTime,mflag,rflag,1);
                //fix here
                check_orders(unit, startTime - rest_get_interval_ms, endTime + rest_get_interval_ms, mflag, rflag, 1);

                //在下面再遍历一遍m_mapNewOrder和remoteOrderIdOrderActionSentTime，设置startTime、mflag、rflag
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
                    //再次检查订单
                    //check_orders(unit,startTime,endTime,mflag,rflag,1);
                    //fix here
                    check_orders(unit, startTime - rest_get_interval_ms, endTime + rest_get_interval_ms, mflag, rflag, 1);
                } 

                lck.lock();
                //第三次检查m_mapNewOrder
                auto order = m_mapNewOrder.begin();
                while(order != m_mapNewOrder.end())
                {
                    if(endTime - order->second.nSendTime > no_response_wait_ms && order->second.nSendTime != 0)
                    {
                        KF_LOG_INFO(logger,"[req_order_insert_error] insert failed");
                        int errorId=100;
                        std::string errorMsg="error,the order was insert failed";
                        std::unique_lock<std::mutex> writer_lock(*mutex_journal_writer);
                        on_rsp_order_insert(&order->second.data, order->second.nRequestID, errorId, errorMsg.c_str());
                        writer_lock.unlock();
                        order = m_mapNewOrder.erase(order);
                    }
                    else
                        order++;
                }
                lck.unlock();  
            } 
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(rest_get_interval_ms));
    }
}

void TDEngineKuCoin::printResponse(const Document& d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
}

void TDEngineKuCoin::getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json)
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

std::string TDEngineKuCoin::construct_request_body(const AccountUnitKuCoin& unit,const  std::string& data,bool isget)
{
    std::string pay_load = R"({"uid":")" + unit.api_key + R"(","data":)" + data + R"(})";
    std::string request_body = utils::crypto::jwt_create(pay_load,unit.secret_key);
    //std::cout  << "[construct_request_body] (request_body)" << request_body << std::endl;
    return  isget ? "user_jwt="+request_body:R"({"user_jwt":")"+request_body+"\"}";
}


void TDEngineKuCoin::get_account(AccountUnitKuCoin& unit, Document& json)
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
std::string TDEngineKuCoin::createInsertOrdertring(const char *code,const char*  strClientId,
                                                    const char *side, const char *type, double& size, double& price,bool isPostOnly)
{
    KF_LOG_INFO(logger, "[TDEngineKuCoin::createInsertOrdertring]:(price)"<<price << "(volume)" << size);
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
    writer.Key("stp");
    writer.String("CO");
    if(account_type == "margin"){
        writer.Key("tradeType");
        writer.String("MARGIN_TRADE");        
    }
   
     //writer.Key("price");
     // writer.Double(price);
    //writer.Key("size");
    //writer.Double(size);
    if(isPostOnly)
    {
        writer.Key("postOnly");
        writer.Bool(isPostOnly);
    }
    writer.EndObject();
    std::stringstream ss;
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
    KF_LOG_INFO(logger, "[TDEngineKuCoin::createInsertOrdertring]:" << str);
    return str;
}

void TDEngineKuCoin::send_order(AccountUnitKuCoin& unit,PendingOrderStatus& stPendingOrderStatus,const char* code,double size,double price, bool isPostOnly,Document& json)
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
        response = Post(requestPath,createInsertOrdertring(code, newClientId.c_str(),strSide.c_str(), strType.c_str(), size, price,isPostOnly),unit,unit.api_key,unit.secret_key,unit.passphrase);

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

bool TDEngineKuCoin::shouldRetry(Document& doc)
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
    
    KF_LOG_INFO(logger, "[shouldRetry] isObJect = " << isObJect << ",strCode = " << strCode << " ,shouldRetry=" << ret);

    return ret;
}

void TDEngineKuCoin::cancel_all_orders(AccountUnitKuCoin& unit, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_all_orders]");

    std::string requestPath = "/api/v1/orders";
    if(account_type == "margin"){
        requestPath = "/api/v1/orders?tradeType=MARGIN_TRADE";
    }

    //std::string queryString= "?user_jwt=RkTgU1lne1aWSBnC171j0eJe__fILSclRpUJ7SWDDulWd4QvLa0-WVRTeyloJOsjyUtduuF0K0SdkYqXR-ibuULqXEDGCGSHSed8WaNtHpvf-AyCI-JKucLH7bgQxT1yPtrJC6W31W5dQ2Spp3IEpXFS49pMD3FRFeHF4HAImo9VlPUM_bP-1kZt0l9RbzWjxVtaYbx3L8msXXyr_wqacNnIV6X9m8eie_DqZHYzGrN_25PfAFgKmghfpL-jmu53kgSyTw5v-rfZRP9VMAuryRIMvOf9LBuMaxcuFn7PjVJx8F7fcEPBCd0roMTLKhHjFidi6QxZNUO1WKSkoSbRxA";//construct_request_body(unit, "{}");

    auto response = Delete(requestPath,"",unit);

    getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineKuCoin::cancel_order(AccountUnitKuCoin& unit, std::string code, std::string orderId, Document& json)
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



std::string TDEngineKuCoin::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


inline int64_t TDEngineKuCoin::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void TDEngineKuCoin::genUniqueKey()
{
    struct tm cur_time = getCurLocalTime();
    //SSMMHHDDN
    char key[11]{0};
    snprintf((char*)key, 11, "%02d%02d%02d%02d%02d", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_CurrentTDIndex);
    m_uniqueKey = key;
}

//clientid =  m_uniqueKey+orderRef
std::atomic<uint64_t> nIndex{0};
std::string TDEngineKuCoin::genClinetid(const std::string &orderRef)
{
    //static int nIndex = 0;
    return m_uniqueKey + orderRef + std::to_string(nIndex++);
}


#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

BOOST_PYTHON_MODULE(libkucointd)
{
    using namespace boost::python;
    class_<TDEngineKuCoin, boost::shared_ptr<TDEngineKuCoin> >("Engine")
            .def(init<>())
            .def("init", &TDEngineKuCoin::initialize)
            .def("start", &TDEngineKuCoin::start)
            .def("stop", &TDEngineKuCoin::stop)
            .def("logout", &TDEngineKuCoin::logout)
            .def("wait_for_stop", &TDEngineKuCoin::wait_for_stop);
}
