#include "TDEngineHuobi.h"
#include "longfist/ctp.h"
#include "longfist/LFUtils.h"
#include "TypeConvert.hpp"
#include <boost/algorithm/string.hpp>

#include <unistd.h>
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

std::mutex update_mutex;

TDEngineHuobi::TDEngineHuobi(): ITDEngine(SOURCE_HUOBI)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.Huobi");
    KF_LOG_INFO(logger, "[TDEngineHuobi]");

    mutex_order_and_trade = new std::mutex();
    mutex_map_pricevolum = new std::mutex();
    mutex_response_order_status = new std::mutex();
    mutex_orderaction_waiting_response = new std::mutex();
    mutex_orderaction_waiting_response1 = new std::mutex();
    mutex_orderinsert_waiting_response = new std::mutex();
    
    mutex_web_orderstatus=new std::mutex();
    mutex_web_connect=new std::mutex();
}

TDEngineHuobi::~TDEngineHuobi()
{
    if(mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if(mutex_response_order_status != nullptr) delete mutex_response_order_status;
    if(mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
    if(mutex_orderaction_waiting_response1 != nullptr) delete mutex_orderaction_waiting_response1;
    if(mutex_orderinsert_waiting_response != nullptr) delete mutex_orderinsert_waiting_response;
    if(nullptr != mutex_map_pricevolum)delete mutex_map_pricevolum;
    if(m_ThreadPoolPtr!=nullptr) delete m_ThreadPoolPtr;
    if(mutex_web_orderstatus!=nullptr) delete mutex_web_orderstatus;
    if(mutex_web_connect!=nullptr) delete mutex_web_connect;
    
}
// gzCompress: do the compressing
int TDEngineHuobi::gzCompress(const char *src, int srcLen, char *dest, int destLen){
	z_stream c_stream;
	int err = 0;
	int windowBits = 15;
	int GZIP_ENCODING = 16;
 
	if(src && srcLen > 0)
	{
		c_stream.zalloc = (alloc_func)0;
		c_stream.zfree = (free_func)0;
		c_stream.opaque = (voidpf)0;
		if(deflateInit2(&c_stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 
                    windowBits | GZIP_ENCODING, 8, Z_DEFAULT_STRATEGY) != Z_OK) return -1;
		c_stream.next_in  = (Bytef *)src;
		c_stream.avail_in  = srcLen;
		c_stream.next_out = (Bytef *)dest;
		c_stream.avail_out  = destLen;
		while (c_stream.avail_in != 0 && c_stream.total_out < destLen) 
		{
			if(deflate(&c_stream, Z_NO_FLUSH) != Z_OK) return -1;
		}
        	if(c_stream.avail_in != 0) return c_stream.avail_in;
		for (;;) {
			if((err = deflate(&c_stream, Z_FINISH)) == Z_STREAM_END) break;
			if(err != Z_OK) return -1;
		}
		if(deflateEnd(&c_stream) != Z_OK) return -1;
		return c_stream.total_out;
	}
	return -1;
}
 
// gzDecompress: do the decompressing
int TDEngineHuobi::gzDecompress(const char *src, int srcLen, const char *dst, int dstLen){
	z_stream strm;
	strm.zalloc=NULL;
	strm.zfree=NULL;
	strm.opaque=NULL;
	 
	strm.avail_in = srcLen;
	strm.avail_out = dstLen;
	strm.next_in = (Bytef *)src;
	strm.next_out = (Bytef *)dst;
	 
	int err=-1, ret=-1;
	err = inflateInit2(&strm, MAX_WBITS+16);
	if (err == Z_OK){
	    err = inflate(&strm, Z_FINISH);
	    if (err == Z_STREAM_END){
	        ret = strm.total_out;
	    }
	    else{
	        inflateEnd(&strm);
	        return err;
	    }
	}
	else{
	    inflateEnd(&strm);
	    return err;
	}
	inflateEnd(&strm);
	return err;
}
static TDEngineHuobi* global_md = nullptr;
//web socket代码
static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{
    std::stringstream ss;
    
    ss << "[ws_service_cb] lws_callback,reason=" << reason << ",";
    
    switch( reason )
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {//lws callback client established
            global_md->on_lws_open(wsi);
            //lws_callback_on_writable( wsi );
            break;
        }
        case LWS_CALLBACK_PROTOCOL_INIT:
        {//lws callback protocol init
            ss << "LWS_CALLBACK_PROTOCOL_INIT.";
            //global_md->writeInfoLog(ss.str());
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {//lws callback client receive
            ss << "LWS_CALLBACK_CLIENT_RECEIVE.";
            //global_md->writeInfoLog(ss.str());
            if(global_md)
            {//统一接收，不同订阅返回数据不同解析
                global_md->on_lws_data(wsi, (char *)in, len);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {//lws callback client writeable
            ss << "LWS_CALLBACK_CLIENT_WRITEABLE.";
            global_md->writeInfoLog(ss.str());
            int ret = 0;
            if(global_md)
            {//统一发送，不同订阅不同请求
                ret = global_md->on_lws_write_subscribe(wsi);
            }
            break;
        }
        case LWS_CALLBACK_CLOSED:
        {//lws callback close
            ss << "LWS_CALLBACK_CLOSED.";
            global_md->on_lws_close(wsi);
            break;
        }
        case LWS_CALLBACK_WSI_DESTROY:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {//lws callback client connection error
            ss << "LWS_CALLBACK_CLIENT_CONNECTION_ERROR.";
            global_md->writeInfoLog(ss.str());
            if(global_md)
            {
                global_md->on_lws_connection_error(wsi);
            }
            break;
        }
        default:
            ss << " default Info.";
            global_md->writeInfoLog(ss.str());
            break;
    }

    return 0;
}
void TDEngineHuobi::on_lws_open(struct lws* wsi){
    KF_LOG_INFO(logger,"[on_lws_open] wsi" << wsi);
    huobiAuth(findAccountUnitHuobiByWebsocketConn(wsi));
    KF_LOG_INFO(logger,"[on_lws_open] finished ");
}
std::mutex account_mutex;
//cys websocket connect
void TDEngineHuobi::Ping(struct lws* conn)
{
    //m_shouldPing = false;
    auto& unit = findAccountUnitHuobiByWebsocketConn(conn);
    StringBuffer sbPing;
    Writer<StringBuffer> writer(sbPing);
    writer.StartObject();
    writer.Key("type");
    writer.String("ping");
    writer.EndObject();
    std::string strPing = sbPing.GetString();
    unit.sendmessage.push(strPing);
    lws_callback_on_writable(conn);
}
/*
void TDEngineHuobi::Pong(struct lws* conn,long long ping){
    KF_LOG_INFO(logger,"[Pong] pong the ping of websocket," << conn);
    auto& unit = findAccountUnitHuobiByWebsocketConn(conn);
    StringBuffer sbPing;
    Writer<StringBuffer> writer(sbPing);
    writer.StartObject();
    writer.Key("action");
    writer.String("pong");
    writer.Key("data");
    writer.StartObject();
    writer.Key("ts");
    writer.Int64(ping);
    writer.EndObject();
    writer.EndObject();
    std::string strPong = sbPing.GetString();
    unit.sendmessage.push(strPong);
    lws_callback_on_writable(conn);
}
*/
/*{
    "action": "pong",
    "data": {
          "ts": 1575537778295 // 使用Ping消息中的ts值
    }
}*/
void TDEngineHuobi::Pong(struct lws* conn, long long ping) {
    KF_LOG_INFO(logger, "[Pong] pong the ping of websocket," << conn);
    auto& unit = findAccountUnitHuobiByWebsocketConn(conn);
    StringBuffer sbPing;
    Writer<StringBuffer> writer(sbPing);
    writer.StartObject();
    writer.Key("action");
    writer.String("pong");
    writer.Key("data");
    writer.StartObject();
    writer.String("ts");
    writer.Int64(ping);
    writer.EndObject();
    writer.EndObject();
    std::string strPong = sbPing.GetString();
    unit.sendmessage.push(strPong);
    lws_callback_on_writable(conn);
}

void TDEngineHuobi::on_lws_receive_orders(struct lws* conn, Document& json) {
    KF_LOG_INFO(logger, "[on_lws_receive_orders]");
    std::lock_guard<std::mutex> guard_mutex(*mutex_response_order_status);
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    AccountUnitHuobi& unit = findAccountUnitHuobiByWebsocketConn(conn);
    //v1 v2都叫data
    rapidjson::Value& node = json["data"];
    KF_LOG_INFO(logger, "[on_lws_receive_orders] receive_order:");

    //事件类型
    std::string eventType = json["data"]["eventType"].GetString();

    //读订单号
    if (node.HasMember("orderId")) {
        KF_LOG_INFO(logger, "[on_lws_receive_orders] (receive success)");
        string remoteOrderId = std::to_string(node["orderId"].GetInt64());
        std::string cid;
        if (node.HasMember("clientOrderId")) {
            cid = node["clientOrderId"].GetString();
        }
        //读restOrderStatusMap时加锁
        std::unique_lock<std::mutex> rest_order_status_mutex(*mutex_order_and_trade);
        std::map<std::string, LFRtnOrderField>::iterator restOrderStatus = unit.restOrderStatusMap.find(remoteOrderId);
        rest_order_status_mutex.unlock();

        //在restOrderStatus找remoteOrderId 找不到
        if (restOrderStatus == unit.restOrderStatusMap.end())
        {
            KF_LOG_INFO(logger, "restOrderStatus not in unit.restOrderStatusMap ");

            bool update = true;
            std::vector<UpdateMsg>::iterator itr;
            //读Updatemsg_vec时加锁
            std::unique_lock<std::mutex> lck1(update_mutex);
            for (itr = Updatemsg_vec.begin(); itr != Updatemsg_vec.end(); itr++) {
                //遍历Updatemsg_vec时若已有remoteOrderId相同的orderId，则update置为false
                if (itr->orderId == remoteOrderId) {
                    KF_LOG_INFO(logger, "old update");
                    update = false;
                    break;
                }
            }

            lck1.unlock();
            //更新
            if (update) {
                //新创建updatemsg，记下orderId，并压入Updatemsg_vec
                UpdateMsg updatemsg;
                updatemsg.orderId = remoteOrderId;
                //更新Updatemsg_vec时加锁
                lck1.lock();
                Updatemsg_vec.push_back(updatemsg);
                lck1.unlock();

                //找到cid位置 本地储存信息 直接写  
                auto it = cid_map.find(cid);
                if (it != cid_map.end()) {
                    localOrderRefRemoteOrderId.insert(make_pair(std::string(it->second.OrderRef), remoteOrderId));

                    LFRtnOrderField rtn_order;
                    memset(&rtn_order, 0, sizeof(LFRtnOrderField));
                    strncpy(rtn_order.BusinessUnit, remoteOrderId.c_str(), 21);
                    rtn_order.OrderStatus = LF_CHAR_NotTouched;
                    rtn_order.VolumeTraded = 0;

                    strcpy(rtn_order.ExchangeID, "huobi");
                    strncpy(rtn_order.UserID, unit.api_key.c_str(), 16);
                    strncpy(rtn_order.InstrumentID, it->second.InstrumentID, 31);
                    rtn_order.Direction = it->second.Direction;
                    //No this setting on Huobi
                    rtn_order.TimeCondition = LF_CHAR_GTC;
                    rtn_order.OrderPriceType = it->second.OrderPriceType;
                    strncpy(rtn_order.OrderRef, it->second.OrderRef, 13);
                    rtn_order.VolumeTotalOriginal = it->second.Volume;
                    rtn_order.LimitPrice = it->second.LimitPrice;
                    rtn_order.VolumeTotal = it->second.Volume;

                    on_rtn_order(&rtn_order);
                    raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
                        source_id, MSG_TYPE_LF_RTN_ORDER_HUOBI,
                        1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);

                    KF_LOG_DEBUG(logger, "[req_order_insert] (addNewOrderToMap)");
                    addNewOrderToMap(unit, rtn_order);
                    KF_LOG_DEBUG(logger, "[req_order_insert] success");
                    std::unique_lock<std::mutex> lck(account_mutex);
                    mapInsertOrders.insert(std::make_pair(it->second.OrderRef, &unit));
                    lck.unlock();
                }
            }
        }

        //找到了
        else {
            handleResponseOrderStatus(unit, restOrderStatus->second, json);
            LfOrderStatusType orderStatus = GetOrderStatus(json["data"]["orderStatus"].GetString());
            //以下3种情况删除
            if (orderStatus == LF_CHAR_AllTraded || orderStatus == LF_CHAR_Canceled
                || orderStatus == LF_CHAR_Error) {
                //edit begin
                if(orderStatus == LF_CHAR_Canceled)
                    KF_LOG_INFO(logger, "LF_CHAR_Canceled");
                //edit end
                KF_LOG_INFO(logger, "[rest addNewOrderToMap] remove a pendingOrderStatus.");
                std::string ref = restOrderStatus->second.OrderRef;
                unit.restOrderStatusMap.erase(remoteOrderId);
                auto it = localOrderRefRemoteOrderId.find(ref);
                if (it != localOrderRefRemoteOrderId.end()) {
                    localOrderRefRemoteOrderId.erase(it);
                    KF_LOG_INFO(logger, "erase local");
                }
                auto it1 = mapInsertOrders.find(ref);
                if (it1 != mapInsertOrders.end()) {
                    mapInsertOrders.erase(it1);
                }
                auto it2 = cid_map.find(cid);
                if (it2 != cid_map.end()) {
                    //edit here
                    if (orderStatus == LF_CHAR_Error) 
                    {
                        auto it3 = requestId_map.find(cid);
                        if (it3 != requestId_map.end()) {
                            int errCode = json["data"]["errCode"].GetInt();
                            std::string errMessage = json["data"]["errMessage"].GetString();
                            on_rsp_order_insert(&it2->second, it3->second, errCode, errMessage.c_str());
                            requestId_map.erase(it3);
                        }
                    }
                    //edit end
                    cid_map.erase(it2);
                    KF_LOG_INFO(logger, "erase cid");
                }
            }
        }
    }
    else
        KF_LOG_INFO(logger, "[on_lws_receive_orders] (reveive failed)");
}
/*
void TDEngineHuobi::on_lws_receive_orders(struct lws* conn,Document& json){
    KF_LOG_INFO(logger,"[on_lws_receive_orders]");
    //加了两个锁
    std::lock_guard<std::mutex> guard_mutex(*mutex_response_order_status);
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);
    //
    AccountUnitHuobi& unit = findAccountUnitHuobiByWebsocketConn(conn);
    rapidjson::Value &node=json["data"];
    KF_LOG_INFO(logger, "[on_lws_receive_orders] receive_order:");
    //读订单号
    if(node.HasMember("order-id")){
        KF_LOG_INFO(logger, "[on_lws_receive_orders] (receive success)");
        std::string type = node["eventType"].GetString();
        string remoteOrderId = std::to_string(node["orderId"].GetInt64());
        std::string cid;
        if(node.HasMember("clientOrderId")){
            cid = node["clientOrderId"].GetString();
        }
        //读restOrderStatusMap时加锁
        std::unique_lock<std::mutex> rest_order_status_mutex(*mutex_order_and_trade);
        std::map<std::string,LFRtnOrderField>::iterator restOrderStatus=unit.restOrderStatusMap.find(remoteOrderId);
        rest_order_status_mutex.unlock();
        if (restOrderStatus == unit.restOrderStatusMap.end()) {
            //web_socket_orderstatus_mutex.unlock();

            bool update = true;
            std::vector<UpdateMsg>::iterator itr;
            std::unique_lock<std::mutex> lck1(update_mutex);
            for (itr = Updatemsg_vec.begin(); itr != Updatemsg_vec.end(); itr++) {
                //遍历Updatemsg_vec时若已有remoteOrderId相同的orderId，则update置为false
                if (itr->orderId == remoteOrderId) {
                    KF_LOG_INFO(logger, "old update");
                    update = false;
                    break;
                }
            }
            lck1.unlock();

            if (update) {
                //新创建updatemsg，记下orderId，并压入Updatemsg_vec
                UpdateMsg updatemsg;
                updatemsg.orderId = remoteOrderId;
                //更新Updatemsg_vec时加锁
                lck1.lock();
                Updatemsg_vec.push_back(updatemsg);
                lck1.unlock();

                //找到cid位置
                auto it = cid_map.find(cid);
                if (it != cid_map.end()) {
                    //
                    localOrderRefRemoteOrderId.insert(make_pair(std::string(it->second.OrderRef), remoteOrderId));

                    LFRtnOrderField rtn_order;
                    memset(&rtn_order, 0, sizeof(LFRtnOrderField));
                    strncpy(rtn_order.BusinessUnit, remoteOrderId.c_str(), 21);
                    rtn_order.OrderStatus = LF_CHAR_NotTouched;
                    rtn_order.VolumeTraded = 0;

                    strcpy(rtn_order.ExchangeID, "huobi");
                    strncpy(rtn_order.UserID, unit.api_key.c_str(), 16);
                    strncpy(rtn_order.InstrumentID, it->second.InstrumentID, 31);
                    rtn_order.Direction = it->second.Direction;
                    //No this setting on Huobi
                    rtn_order.TimeCondition = LF_CHAR_GTC;
                    rtn_order.OrderPriceType = it->second.OrderPriceType;
                    strncpy(rtn_order.OrderRef, it->second.OrderRef, 13);
                    rtn_order.VolumeTotalOriginal = it->second.Volume;
                    rtn_order.LimitPrice = it->second.LimitPrice;
                    rtn_order.VolumeTotal = it->second.Volume;

                    on_rtn_order(&rtn_order);
                    raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
                        source_id, MSG_TYPE_LF_RTN_ORDER_HUOBI,
                        1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);

                    KF_LOG_DEBUG(logger, "[req_order_insert] (addNewOrderToMap)");
                    addNewOrderToMap(unit, rtn_order);
                    KF_LOG_DEBUG(logger, "[req_order_insert] success");
                    std::unique_lock<std::mutex> lck(account_mutex);
                    mapInsertOrders.insert(std::make_pair(it->second.OrderRef, &unit));
                    lck.unlock();
                }
            }

        }
        else {
            handleResponseOrderStatus(unit, restOrderStatus->second, json);
            LfOrderStatusType orderStatus = GetOrderStatus(json["data"]["order-state"].GetString());
            if (orderStatus == LF_CHAR_AllTraded || orderStatus == LF_CHAR_Canceled
                || orderStatus == LF_CHAR_Error) {
                KF_LOG_INFO(logger, "[rest addNewOrderToMap] remove a pendingOrderStatus.");
                std::string ref = restOrderStatus->second.OrderRef;
                unit.restOrderStatusMap.erase(remoteOrderId);
                auto it = localOrderRefRemoteOrderId.find(ref);
                if (it != localOrderRefRemoteOrderId.end()) {
                    localOrderRefRemoteOrderId.erase(it);
                    KF_LOG_INFO(logger, "erase local");
                }
                auto it1 = mapInsertOrders.find(ref);
                if (it1 != mapInsertOrders.end()) {
                    mapInsertOrders.erase(it1);
                }
                auto it2 = cid_map.find(cid);
                if (it2 != cid_map.end()) {
                    cid_map.erase(it2);
                    KF_LOG_INFO(logger, "erase cid");
                }
            }
            if(orderStatus == LF_CHAR_Canceled){
                KF_LOG_INFO(logger, "[remove remoteOrderIdOrderActionSentTime] remove a remoteOrderIdOrderActionSentTime.");
                //int64_t id = json["data"]["order-id"].GetInt64();
                remoteOrderIdOrderActionSentTime.erase(remoteOrderId);
            }
        }
    } 
    else {
        KF_LOG_INFO(logger, "[on_lws_receive_orders] (reveive failed)");
        std::string errorMsg;
        int errorId = json["err-code"].GetInt();
        if (json.HasMember("err-msg") && json["err-msg"].IsString()) {
            errorMsg = json["err-msg"].GetString();
        }
        KF_LOG_ERROR(logger, "[on_lws_receive_orders] get_order fail."<< " (errorId)" << errorId<< " (errorMsg)" << errorMsg);
    }
}
*/
/*
{"op": "sub",
"cid" : "40sG903yz80oDFWr",
"topic" : "orders.htusdt.update"
}
{ "op": "sub",
  "ts": 1489474081631,
  "topic": "orders.htusdt.update",
  "err-code": 0,
  "cid": "40sG903yz80oDFWr"
}*/

//在ws_service_cb被调用
void TDEngineHuobi::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "[on_lws_data] (data) " << data);
    //std::string strData = dealDataSprit(data);
    Document json;
    json.Parse(data);
    //总之查一下json正常不正常
    if (json.HasParseError() || !json.IsObject()) {
        KF_LOG_ERROR(logger, "[cys_on_lws_data] parse to json error ");
        return;
    }

    if (json.HasMember("action"))
    {
        std::string action = json["action"].GetString();
        std::string ch;
        if (json.HasMember("ch"))
            ch = json["ch"].GetString();

        if (action == "req" && ch.empty() == false && ch == "auth") {

            int code = json["code"].GetInt();
            if (code != 200) {
                KF_LOG_ERROR(logger, "[on_lws_data] cys_huobiAuth error. code: " << code);
                return;
            }

            AccountUnitHuobi& unit = findAccountUnitHuobiByWebsocketConn(conn);
            unit.isAuth = huobi_auth;
            KF_LOG_INFO(logger, "[on_lws_data] cys_huobiAuth success.");
        }
        //ping必须返回pong
        if (action == "ping")
        {
            long long ping = json["data"]["ts"].GetInt64();
            Pong(conn, ping);
        }
        //Response 三种情况都会返回 sub 
        if (action == "sub")
        {
            int code = json["code"].GetInt();
            //std::string json_data = json["data"].GetString();
            if(code == 200)
                KF_LOG_INFO(logger, "[on_lws_data] action: " << action << " code: " << code << " ch: " << ch);
            else {
                KF_LOG_ERROR(logger, "[on_lws_data] sub error. code: " << code);
                string message;
                if (json.HasMember("message")) {
                    message = json["message"].GetString();
                    KF_LOG_ERROR(logger, "[on_lws_data] sub error. message: " << message);
                }   
            }
        }

        //订阅订单更新和账户变更后 update action都是push
        if (action == "push") 
        {
            if(ch.empty())
                KF_LOG_ERROR(logger, "[on_lws_data] . action == push  without ch");
            
            //订阅订单变更信息
            if (ch.find("orders") != string::npos)
            {
                KF_LOG_INFO(logger, "[on_lws_data] . action:push  ch:orders  begin");
                on_lws_receive_orders(conn, json);
                KF_LOG_INFO(logger, "[on_lws_data] . action:push  ch:orders  end");
                //换个地方处理了，不管
            }
            
            //订阅账户变更信息 不做
            if (ch.find("accounts") != string::npos)
            {   
                if (ch == "accounts.update#0" || ch == "accounts.update" )
                {
                    KF_LOG_INFO(logger, "ch:accounts.update#0");
                }
                else if (ch == "accounts.update#1")
                {
                    KF_LOG_INFO(logger, "ch:accounts.update#1");
                }
                else
                {
                    KF_LOG_ERROR(logger, "[on_lws_data] . ch:account error");
                }
            }
        }
    }

    //订阅清算后成交及撤单更新 update内容 也不做
    else if (json.HasMember("ch")) {
        std::string ch = json["ch"].GetString();
        if (ch.find("trade.clearing") != string::npos) 
        {
            KF_LOG_INFO(logger, "ch:trade.clearing");
        }
    }

    else
    {
        KF_LOG_ERROR(logger, "[on_lws_data] . parse json error(data): " << data);
    }
}

/*void TDEngineHuobi::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    int l = len*10;
    char* buf = new char[l]{};
    l = gzDecompress(data, len, buf, l);
    KF_LOG_INFO(logger, "[on_lws_data] (cys_buf) " << buf);
    KF_LOG_INFO(logger, "[on_lws_data] (data) " << data);
    //std::string strData = dealDataSprit(data);
    Document json;
    json.Parse(buf);
    //总之查一下json正常不正常
    if(json.HasParseError()||!json.IsObject()){
        KF_LOG_ERROR(logger, "[cys_on_lws_data] parse to json error ");
        delete[] buf;
        return;
    }
    if (json.HasMember("op") || json.HasMember("ping"))
    {
        //状态码不为ok 或者有错误码
        if ((json.HasMember("status") && json["status"].GetString() != "ok") ||
            (json.HasMember("err-code") && json["err-code"].GetInt() != 0)) {
            int errorCode = json["err-code"].GetInt();
            std::string errorMsg = json["err-msg"].GetString();
            KF_LOG_ERROR(logger, "[on_lws_data] (err-code) " << errorCode << " (errMsg) " << errorMsg);
        }
        //有op记op
        else if (json.HasMember("op")) {
            std::string op = json["op"].GetString();
            //判断op种类
            //notify通知 建立订阅后 收到Update example
            if (op == "notify") {
                string topic = json["topic"].GetString();
                if (topic.substr(0, topic.find(".")) == "orders") {
                    on_lws_receive_orders(conn, json);
                }
            }
            //收到ping需要pong返回
            else if (op == "ping") {
                long long ping = json["ts"].GetInt64();
                Pong(conn, ping);
            }
            //鉴权固定值为 auth
            else if (op == "auth") {
                AccountUnitHuobi& unit = findAccountUnitHuobiByWebsocketConn(conn);
                unit.isAuth = huobi_auth;
                int userId = json["data"]["user-id"].GetInt();
                KF_LOG_INFO(logger, "[on_lws_data] cys_huobiAuth success. authed user-id " << userId);
            }
        }
        //收到ch subbed 啥也不干
        else if (json.HasMember("ch")) {}
        //有ping就pong
        else if (json.HasMember("ping")) {
            long long ping = json["ts"].GetInt64();
            Pong(conn, ping);
        }
        else if (json.HasMember("subbed")) {}
    }
    else
    {
        KF_LOG_ERROR(logger, "[on_lws_data] . parse json error(data): " << data);
    }
    delete[] buf;
}
*/
/*{
    "action": "sub",
    "ch": "orders#btcusdt"
}*/
std::string TDEngineHuobi::makeSubscribeOrdersUpdate(AccountUnitHuobi& unit, string ticker) {
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("action");
    writer.String("sub");
    writer.Key("ch");
    string ch = "orders#";
    ch = ch + ticker;
    writer.String(ch.c_str());
    writer.EndObject();
    std::string strUpdate = sbUpdate.GetString();
    return strUpdate;
}
/*{
  "op": "sub",
  "cid": "40sG903yz80oDFWr",
  "topic": "orders.htusdt.update",
}*/
/*std::string TDEngineHuobi::makeSubscribeOrdersUpdate(AccountUnitHuobi& unit, string ticker){
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("op");
    writer.String("sub");
    writer.Key("cid");
    writer.String(unit.spotAccountId.c_str());
    writer.Key("topic");
    string topic = "orders.";
    topic = topic + ticker + ".update";
    writer.String(topic.c_str());
    writer.EndObject();
    std::string strUpdate = sbUpdate.GetString();
    return strUpdate;
}*/

AccountUnitHuobi& TDEngineHuobi::findAccountUnitHuobiByWebsocketConn(struct lws * websocketConn){

    for (size_t idx = 0; idx < account_units.size(); idx++) {
        AccountUnitHuobi &unit = account_units[idx];

        std::unique_lock<std::mutex> web_socket_connect_mutex(* mutex_web_connect);
        if(unit.webSocketConn == websocketConn) {
        KF_LOG_INFO(logger, "findAccountUnitHuobiByWebsocketConn unit " << unit.webSocketConn << ","<< idx);
        KF_LOG_INFO(logger,"api_key="<<unit.api_key);
        return unit;
        }

    }
    return account_units[0];
}

int TDEngineHuobi::subscribeTopic(struct lws* conn,string strSubscribe){
    unsigned char msg[1024];
    memset(&msg[LWS_PRE], 0, 1024-LWS_PRE);
    int length = strSubscribe.length();
    KF_LOG_INFO(logger, "[subscribeTopic] " << strSubscribe.c_str() << " ,len = " << length);
    strncpy((char *)msg+LWS_PRE, strSubscribe.c_str(), length);
    //请求
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
    lws_callback_on_writable(conn);
    return ret;
}

int TDEngineHuobi::on_lws_write_subscribe(struct lws* conn){
    KF_LOG_INFO(logger, "[on_lws_write_subscribe]" );
    int ret = 0;
    AccountUnitHuobi& unit=findAccountUnitHuobiByWebsocketConn(conn);
    if(unit.sendmessage.size()>0)
    {
        ret = subscribeTopic(conn,unit.sendmessage.front());
        unit.sendmessage.pop();
    }
    if(unit.isAuth==huobi_auth&&unit.isOrders != orders_sub){
        unit.isOrders=orders_sub;
        /*std::unordered_map<std::string, std::string>::iterator map_itr;
        map_itr = unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
        if(map_itr == unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()){
            KF_LOG_ERROR(logger,"[on_lws_write_subscribe] whitelist is null, subscribe topic none.");
            return ret;
        }
        map_itr++;
        while(map_itr != unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()){
            string ticker = map_itr->second.c_str();
            KF_LOG_INFO(logger,"[on_lws_write_subscribe] (ticker) "<<ticker);
            string strSubscribe = makeSubscribeOrdersUpdate(unit,ticker);
            ret = subscribeTopic(conn,strSubscribe);
            map_itr++;
        }*/
        string strSubscribe = makeSubscribeOrdersUpdate(unit,"*");
        unit.sendmessage.push(strSubscribe);
        lws_callback_on_writable(conn);
        //ret = subscribeTopic(conn,strSubscribe);
    }
    return ret;
}

void TDEngineHuobi::on_lws_connection_error(struct lws* conn){
    KF_LOG_ERROR(logger, "TDEngineHuobi::on_lws_connection_error. login again:" << conn);
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    m_isPong = false;
    //m_shouldPing = true;
    AccountUnitHuobi& unit=findAccountUnitHuobiByWebsocketConn(conn);     
    unit.is_connecting = false;        
    unit.isAuth = nothing;
    unit.isOrders = nothing;
    //no use it
    long timeout_nsec = 0;
    //reset sub
    //m_isSubL3 = false;
    //AccountUnitHuobi& unit=findAccountUnitHuobiByWebsocketConn(conn);
    //std::this_thread::sleep_for(std::chrono::minutes(5));
    lws_login(unit,0);
    Document d;
    KF_LOG_INFO(logger,"login again and cancel_all_orders");
    cancel_all_orders(unit, "no_use", d);

    std::map<std::string, std::string>::iterator it;
    for(it=localOrderRefRemoteOrderId.begin();it!=localOrderRefRemoteOrderId.end();it++){
        std::string url = "/v1/order/orders/" + it->second;
        KF_LOG_INFO(logger,"(url2)"<<url);
        const auto response = Get(url,"",unit,"");
        Document json;
        json.Parse(response.text.c_str());
        if(!json.HasParseError()){
            if (json.IsObject()){
                std::string state = json["data"]["state"].GetString();
                if(state == "canceled"){
                    string remoteOrderId=std::to_string(json["data"]["id"].GetInt64());
                    std::map<std::string,LFRtnOrderField>::iterator restOrderStatus=unit.restOrderStatusMap.find(remoteOrderId);
                    if(restOrderStatus!=unit.restOrderStatusMap.end()){
                        if(restOrderStatus->second.OrderStatus != LF_CHAR_Canceled){
                            restOrderStatus->second.OrderStatus = LF_CHAR_Canceled;
                            on_rtn_order(&(restOrderStatus->second));
                            //on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
                        }
                    }
                }
            }
        }
    }
}
void TDEngineHuobi::on_lws_close(struct lws* conn){
    AccountUnitHuobi& unit=findAccountUnitHuobiByWebsocketConn(conn);  
    unit.isAuth=nothing;
    unit.isOrders=nothing;
    KF_LOG_INFO(logger,"[websocket close]");
}
static struct lws_protocols protocols[] =
        {
                {
                        "ws",
                        ws_service_cb,
                              0,
                                 65536,
                },
                { NULL, NULL, 0, 0 } /* terminator */
        };
int on_lws_write_subscribe(struct lws* conn);
void on_lws_connection_error(struct lws* conn);

struct session_data {
    int fd;
};
void TDEngineHuobi::writeInfoLog(std::string strInfo){
    KF_LOG_INFO(logger,strInfo);
}
void TDEngineHuobi::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}

int64_t TDEngineHuobi::GetMillsecondByInterval(const std::string& interval)
{
    if (interval == "1min")
        return 60000;
    else if (interval == "5min")
        return 300000;
    else if (interval == "15min")
        return 900000;
    else if (interval == "30min")
        return 1800000;
    else if (interval == "60min")
        return 3600000;
    else if (interval == "4hour")
        return 14400000;
    else if (interval == "1day")
        return 86400000;
    else if (interval == "1mon")
        return 2678400000;
    else if (interval == "1week")
        return 604800000;
    else if (interval == "1year")
        return 31536000000;
    else {
        KF_LOG_DEBUG(logger, "GetMillsecondByInterval error, please check the interval");
        return 0;
    }
}

std::string TDEngineHuobi::getHuobiSignatrue(std::string parameters[],int psize,std::string timestamp,std::string method_url,
                                                std::string reqType,AccountUnitHuobi& unit,std::string parameter){
    std::string strAccessKeyId=unit.api_key;
    std::string strSignatureMethod="HmacSHA256";
    //edit here
    std::string strSignatureVersion="2";
    std::string strSign = reqType+unit.baseUrl+"\n" + method_url+"\n"+
                            "AccessKeyId="+strAccessKeyId+"&"+
                            "SignatureMethod="+strSignatureMethod+"&"+
                            "SignatureVersion="+strSignatureVersion+"&"+
                            "Timestamp="+timestamp+parameter;
    KF_LOG_INFO(logger, "[getHuobiSignatrue] strAccessKeyId = " << strAccessKeyId);
    KF_LOG_INFO(logger, "[getHuobiSignatrue] strSign = " << strSign );
    unsigned char* strHmac = hmac_sha256_byte(unit.secret_key.c_str(),strSign.c_str());
    KF_LOG_INFO(logger, "[getHuobiSignatrue] strHmac = " << strHmac );
    std::string strSignatrue = escapeURL(base64_encode(strHmac,32));
    KF_LOG_INFO(logger, "[getHuobiSignatrue] Signatrue = " << strSignatrue );
    return strSignatrue;
}
char TDEngineHuobi::dec2hexChar(short int n) {
	if (0 <= n && n <= 9) {
		return char(short('0') + n);
	}
	else if (10 <= n && n <= 15) {
		return char(short('A') + n - 10);
	}
	else {
		return char(0);
	}
}
std::string TDEngineHuobi::escapeURL(const string &URL){
	string result = "";
	for (unsigned int i = 0; i < URL.size(); i++) {
		char c = URL[i];
		if (
			('0' <= c && c <= '9') ||
			('a' <= c && c <= 'z') ||
			('A' <= c && c <= 'Z') ||
			c == '/' || c == '.'
			) {
			result += c;
		}
		else {
			int j = (short int)c;
			if (j < 0) {
				j += 256;
			}
			int i1, i0;
			i1 = j / 16;
			i0 = j - i1 * 16;
			result += '%';
			result += dec2hexChar(i1);
			result += dec2hexChar(i0);
		}
	}
	return result;
}
//cys edit from huobi api
std::mutex g_httpMutex;
cpr::Response TDEngineHuobi::Get(const std::string& method_url,const std::string& body, AccountUnitHuobi& unit,std::string parameters)
{
    std::string strTimestamp = getHuobiTime();
    string strSignatrue=getHuobiSignatrue(NULL,0,strTimestamp,method_url,"GET\n",unit,parameters);
    string SignatureVersion = "2";
    string url = unit.baseUrl + method_url+"?"+"AccessKeyId="+unit.api_key+"&"+
                    "SignatureMethod=HmacSHA256&"+
                    //edit here
                    "SignatureVersion="+ SignatureVersion +"&"+
                    "Timestamp="+strTimestamp+"&"+
                    "Signature="+strSignatrue+parameters;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    const auto response = cpr::Get(Url{url},
                                   Header{{"Content-Type", "application/json"}}, Timeout{10000} );
    lock.unlock();
    //if(response.text.length()<500){
        KF_LOG_INFO(logger, "[Get] (url) " << url << " (response.status_code) " << response.status_code <<
                                       " (response.error.message) " << response.error.message <<
                                       " (response.text) " << response.text.c_str());
    //}
    return response;
}
//cys edit
cpr::Response TDEngineHuobi::Post(const std::string& method_url,const std::string& body, AccountUnitHuobi& unit)
{
    std::string strTimestamp = getHuobiTime();
    string strSignatrue=getHuobiSignatrue(NULL,0,strTimestamp,method_url,"POST\n",unit,"");
    string url = unit.baseUrl + method_url+"?"+"AccessKeyId="+unit.api_key+"&"+
                    "SignatureMethod=HmacSHA256&"+
                    "SignatureVersion=2&"+
                    "Timestamp="+strTimestamp+"&"+
                    "Signature="+strSignatrue;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    auto response = cpr::Post(Url{url}, Header{{"Content-Type", "application/json"}},
                              Body{body},Timeout{30000});
    lock.unlock();
    if(response.text.length()<500){
        KF_LOG_INFO(logger, "[POST] (url) " << url <<"(body) "<< body<< " (response.status_code) " << response.status_code <<
                                        " (response.error.message) " << response.error.message <<
                                        " (response.text) " << response.text.c_str());
    }
    return response;
}
void TDEngineHuobi::init()
{
    //genUniqueKey();
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineHuobi::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineHuobi::resize_accounts(int account_num)
{
    //account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineHuobi::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    //string api_key = j_config["APIKey"].get<string>();
    //string secret_key = j_config["SecretKey"].get<string>();
    
    if(j_config.find("is_margin") != j_config.end()) {
        isMargin = j_config["is_margin"].get<bool>();
    }
    //https://api.huobi.pro
    string baseUrl = j_config["baseUrl"].get<string>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

    if(j_config.find("orderaction_max_waiting_seconds") != j_config.end()) {
        orderaction_max_waiting_seconds = j_config["orderaction_max_waiting_seconds"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (orderaction_max_waiting_seconds)" << orderaction_max_waiting_seconds);

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
    genUniqueKey();

    //多账户
    auto iter = j_config.find("users");
    if (iter != j_config.end() && iter.value().size() > 0)
    {
        for (auto& j_account: iter.value())
        {
            AccountUnitHuobi unit;

            unit.api_key = j_account["APIKey"].get<string>();
            unit.secret_key = j_account["SecretKey"].get<string>();
            unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
            //unit.coinPairWhiteList.Debug_print();
            unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
            //unit.positionWhiteList.Debug_print();
            unit.baseUrl = baseUrl;

            KF_LOG_INFO(logger, "[load_account] (api_key)" << unit.api_key << " (baseUrl)" << unit.baseUrl 
                                                   << " (spotAccountId) "<<unit.spotAccountId
                                                   << " (marginAccountId) "<<unit.marginAccountId);
            if(unit.coinPairWhiteList.Size() == 0) {
                //display usage:
                KF_LOG_ERROR(logger, "TDEngineHuobi::load_account: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
                KF_LOG_ERROR(logger, "\"whiteLists\":{");
                KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
                KF_LOG_ERROR(logger, "    \"btc_usdt\": \"BTCUSDT\",");
                KF_LOG_ERROR(logger, "     \"etc_eth\": \"ETCETH\"");
                KF_LOG_ERROR(logger, "},");
            }
            else
            {
                getAccountId(unit);
                //test
                Document json;
                get_account(unit, json);
                //printResponse(json);
                //cancel_order(unit,"code","1",json);
                cancel_all_orders(unit, "btc_usd", json);
                //printResponse(json);
                getPriceVolumePrecision(unit);         
            }
            account_units.emplace_back(unit);
          
        }

    }
    else
    {
        KF_LOG_ERROR(logger, "[load_account] no tarde account info !");
    }
    // set up
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, account_units[0].api_key.c_str(), 16);
    strncpy(account.Password, account_units[0].secret_key.c_str(), 21);
    //web socket登陆
    login(0);
    return account;
}

void TDEngineHuobi::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitHuobi& unit = account_units[idx];
        //unit.logged_in = true;
        KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
        if (!unit.logged_in)
        {
            KF_LOG_INFO(logger, "[connect] (account id) "<<unit.spotAccountId<<" login. idx:" << idx);
            lws_login(unit, 0);
            //set true to for let the kungfuctl think td is running.
            unit.logged_in = true;
        }
    }
}
//火币
/*
字段名称	数据类型	描述
base-currency	string	交易对中的基础币种
quote-currency	string	交易对中的报价币种
price-precision	integer	交易对报价的精度（小数点后位数）
amount-precision	integer	交易对基础币种计数精度（小数点后位数）
symbol-partition	string	交易区，可能值: [main，innovation，bifurcation]
  "data": [
    {
        "base-currency": "btc",
        "quote-currency": "usdt",
        "price-precision": 2,
        "amount-precision": 4,
        "symbol-partition": "main",
        "symbol": "btcusdt"
    }
    {
        "base-currency": "eth",
        "quote-currency": "usdt",
        "price-precision": 2,
        "amount-precision": 4,
        "symbol-partition": "main",
        "symbol": "ethusdt"
    }
  ]
*/


size_t current_account_idx = -1;
AccountUnitHuobi& TDEngineHuobi::get_current_account()
{
    current_account_idx++;
    current_account_idx %= account_units.size();
    return account_units[current_account_idx];
}

void TDEngineHuobi::getPriceVolumePrecision(AccountUnitHuobi& unit){
    KF_LOG_INFO(logger,"[getPriceVolumePrecision]");
    Document json;
    const auto response = Get("/v1/common/symbols","",unit,"");
    json.Parse(response.text.c_str());
    const static std::string strSuccesse = "ok";
    if(json.HasMember("status") && json["status"].GetString() == strSuccesse)
    {
        auto& list=json["data"];
        int n=json["data"].Size();
        std::unique_lock<std::mutex> rest_price_volume_mutex(*mutex_map_pricevolum);
        for(int i=0;i<n;i++){
            PriceVolumePrecision stPriceVolumePrecision;
            stPriceVolumePrecision.symbol=list.GetArray()[i]["symbol"].GetString();
            std::string ticker = unit.coinPairWhiteList.GetKeyByValue(stPriceVolumePrecision.symbol);
            if(ticker.length()==0){
                //KF_LOG_ERROR(logger,"[getPriceVolumePrecision] (No such symbol in whitelist) "<<stPriceVolumePrecision.symbol);
                continue;
            }
            stPriceVolumePrecision.baseCurrency=list.GetArray()[i]["base-currency"].GetString();
            stPriceVolumePrecision.quoteCurrency=list.GetArray()[i]["quote-currency"].GetString();
            stPriceVolumePrecision.pricePrecision=list.GetArray()[i]["price-precision"].GetInt();
            stPriceVolumePrecision.amountPrecision=list.GetArray()[i]["amount-precision"].GetInt();
            stPriceVolumePrecision.symbolPartition=list.GetArray()[i]["symbol-partition"].GetString();
           
            unit.mapPriceVolumePrecision.insert(std::make_pair(stPriceVolumePrecision.symbol,stPriceVolumePrecision));

            KF_LOG_INFO(logger,"[getPriceVolumePrecision] symbol "<<stPriceVolumePrecision.symbol);
        }
        KF_LOG_INFO(logger,"[getPriceVolumePrecision] (map size) "<<unit.mapPriceVolumePrecision.size());
        rest_price_volume_mutex.unlock();
    }
}


/*
{
    "action": "req",
    "ch": "auth",
    "params": {
        "authType":"api",
        "accessKey": "e2xxxxxx-99xxxxxx-84xxxxxx-7xxxx", 
        "signatureMethod": "HmacSHA256",                 
        "signatureVersion": "2.1",                       //这里不同
        "timestamp": "2019-09-01T18:16:16",              
        "signature": "4F65x5A2bLyMWVQj3Aqp+B4w+ivaA7n5Oi2SuYtCJ9o="
    }
}
*/
void TDEngineHuobi::huobiAuth(AccountUnitHuobi& unit) {
    KF_LOG_INFO(logger, "[huobiAuth] auth");
    //时间戳和AccessKey
    std::string strTimestamp = getHuobiTime();
    std::string timestamp = getHuobiNormalTime();
    std::string strAccessKeyId = unit.api_key;
    KF_LOG_INFO(logger, "[huobiAuth] strSign =unit.api_key" << unit.api_key);
    //签名方法和版本
    std::string strSignatureMethod = "HmacSHA256";
    std::string strSignatureVersion = "2.1";
    string reqType = "GET\n";
    //签名开始 也许要改
    /*
    std::string strSign = reqType + "api.huobi.pro\n" + "/ws/v2\n" +
        "AccessKeyId=" + strAccessKeyId + "&" +
        "SignatureMethod=" + strSignatureMethod + "&" +
        "SignatureVersion=" + strSignatureVersion + "&" +
        "Timestamp=" + strTimestamp;
    */
    std::string strSign = reqType + "api.huobi.pro\n" + "/ws/v2\n" +
        "accessKey=" + strAccessKeyId + "&" +
        "signatureMethod=" + strSignatureMethod + "&" +
        "signatureVersion=" + strSignatureVersion + "&" +
        "timestamp=" + strTimestamp;
    KF_LOG_INFO(logger, "[huobiAuth] strSign = " << strSign);
    unsigned char* strHmac = hmac_sha256_byte(unit.secret_key.c_str(), strSign.c_str());
    KF_LOG_INFO(logger, "[huobiAuth] strHmac = " << strHmac);
    std::string strSignatrue = base64_encode(strHmac, 32);
    KF_LOG_INFO(logger, "[huobiAuth] Signatrue = " << strSignatrue);
    //签名结束
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    //edit begin
    writer.StartObject();
    writer.Key("action");
    writer.String("req");
    writer.Key("ch");
    writer.String("auth");
    writer.Key("params");
    writer.StartObject();
    writer.Key("authType");
    writer.String("api");
    writer.Key("accessKey");
    writer.String(unit.api_key.c_str());
    writer.Key("signatureMethod");
    writer.String("HmacSHA256");
    writer.Key("signatureVersion");
    writer.String("2.1");
    writer.Key("timestamp");
    writer.String(timestamp.c_str());
    writer.Key("signature");
    writer.String(strSignatrue.c_str());
    writer.EndObject();
    writer.EndObject();
    //edit end
    std::string strSubscribe = sbUpdate.GetString();
    unsigned char msg[1024];
    memset(&msg[LWS_PRE], 0, 1024 - LWS_PRE);
    int length = strSubscribe.length();
    KF_LOG_INFO(logger, "[huobiAuth] auth data " << strSubscribe.c_str() << " ,len = " << length);
    unit.sendmessage.push(strSubscribe);
    lws_callback_on_writable(unit.webSocketConn);
    KF_LOG_INFO(logger, "[huobiAuth] auth success...");
}
/*
void TDEngineHuobi::huobiAuth(AccountUnitHuobi& unit){
    KF_LOG_INFO(logger, "[huobiAuth] auth");
    std::string strTimestamp = getHuobiTime();
    std::string timestamp = getHuobiNormalTime();
    std::string strAccessKeyId=unit.api_key;
    KF_LOG_INFO(logger, "[huobiAuth] strSign =unit.api_key" << unit.api_key);
    std::string strSignatureMethod="HmacSHA256";
    std::string strSignatureVersion="2.1";
    string reqType="GET\n";
    std::string strSign = reqType+unit.baseUrl + "\n/ws/v2\n"+
                            "accessKey="+strAccessKeyId+"&"+
                            "signatureMethod="+strSignatureMethod+"&"+
                            "signatureVersion="+strSignatureVersion+"&"+
                            "timestamp="+strTimestamp;
    KF_LOG_INFO(logger, "[huobiAuth] strSign = " << strSign );
    unsigned char* strHmac = hmac_sha256_byte(unit.secret_key.c_str(),strSign.c_str());
    KF_LOG_INFO(logger, "[huobiAuth] strHmac = " << strHmac );
    std::string strSignatrue = base64_encode(strHmac,32);
    KF_LOG_INFO(logger, "[huobiAuth] Signatrue = " << strSignatrue );
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("action");
    writer.String("req");
    writer.Key("ch");
    writer.String("auth");
    writer.Key("params");
    writer.StartObject();
    writer.Key("authType");
    writer.String("api");
    writer.Key("accessKey");
    writer.String(unit.api_key.c_str());
    writer.Key("signatureMethod");
    writer.String(strSignatureMethod.c_str());
    writer.Key("signatureVersion");
    writer.String(strSignatureVersion.c_str());
    writer.Key("timestamp");
    writer.String(timestamp.c_str());
    writer.Key("signature");
    writer.String(strSignatrue.c_str());
    writer.EndObject();
    writer.EndObject();
    std::string strSubscribe = sbUpdate.GetString();
    unsigned char msg[1024];
    memset(&msg[LWS_PRE], 0, 1024-LWS_PRE);
    int length = strSubscribe.length();
    KF_LOG_INFO(logger, "[huobiAuth] auth data " << strSubscribe.c_str() << " ,len = " << length);
    unit.sendmessage.push(strSubscribe);
    lws_callback_on_writable(unit.webSocketConn);
    KF_LOG_INFO(logger, "[huobiAuth] auth success...");
}
*/
void TDEngineHuobi::lws_login(AccountUnitHuobi& unit, long timeout_nsec){
    KF_LOG_INFO(logger, "[TDEngineHuobi::lws_login]");
    global_md = this;
    m_isSubL3 = false;
    unit.isAuth = nothing;
    unit.isOrders = nothing;
    global_md = this;
    int inputPort = 443;
    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;

    struct lws_context_creation_info ctxCreationInfo;
    struct lws_client_connect_info clientConnectInfo;
    //struct lws *wsi = NULL;

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

    unit.context = lws_create_context(&ctxCreationInfo);
    KF_LOG_INFO(logger, "[TDEngineHuobi::lws_login] context created.");


    if (unit.context == NULL) {
        KF_LOG_ERROR(logger, "[TDEngineHuobi::lws_login] context is NULL. return");
        return;
    }

    // Set up the client creation info
    static std::string host  = "api.huobi.pro";
    //edit begin
    static std::string path = "/ws/v2";
    //static std::string path = "/ws/v1";
    //edit end
    clientConnectInfo.address = host.c_str();
    clientConnectInfo.path = path.c_str(); // Set the info's path to the fixed up url path
    
    clientConnectInfo.context = unit.context;
    clientConnectInfo.port = 443;
    clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    clientConnectInfo.host = host.c_str();
    clientConnectInfo.origin = "origin";
    clientConnectInfo.ietf_version_or_minus_one = -1;
    clientConnectInfo.protocol = protocols[0].name;
    std::unique_lock<std::mutex> web_socket_connect_mutex(* mutex_web_connect);
    clientConnectInfo.pwsi = &unit.webSocketConn;

    KF_LOG_INFO(logger, "[TDEngineHuobi::login] address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);
    //建立websocket连接
    
    unit.webSocketConn = lws_client_connect_via_info(&clientConnectInfo);
    if (unit.webSocketConn == NULL) {  
        KF_LOG_ERROR(logger, "[TDEngineHuobi::lws_login] wsi create error.");
        //return;
        sleep(10);
        lws_login(unit,0);
    }
    KF_LOG_ERROR(logger, "[TDEngineHuobi::lws_login] unit.webSocketConn." << unit.webSocketConn);
    KF_LOG_INFO(logger, "[TDEngineHuobi::login] wsi create success.");
    unit.is_connecting = true;
    KF_LOG_INFO(logger,"unit.api_key="<<unit.api_key);
    web_socket_connect_mutex.unlock();
}
void TDEngineHuobi::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[TDEngineHuobi::login]");
    connect(timeout_nsec);
}

void TDEngineHuobi::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineHuobi::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineHuobi::is_logged_in() const{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineHuobi::is_connected() const{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}


std::string TDEngineHuobi::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "buy";
    } else if (LF_CHAR_Sell == input) {
        return "sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineHuobi::GetDirection(std::string input) {
    if ("buy-limit" == input || "buy-market" == input) {
        return LF_CHAR_Buy;
    } else if ("sell-limit" == input || "sell-market" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineHuobi::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "market";
    } else {
        return "";
    }
}

LfOrderPriceTypeType TDEngineHuobi::GetPriceType(std::string input) {
    if ("buy-limit" == input||"sell-limit" == input) {
        return LF_CHAR_LimitPrice;
    } else if ("buy-market" == input||"sell-market" == input) {
        return LF_CHAR_AnyPrice;
    } else {
        return '0';
    }
}
//订单状态，submitting , submitted 已提交, partial-filled 部分成交, partial-canceled 部分成交撤销, filled 完全成交, canceled 已撤销
LfOrderStatusType TDEngineHuobi::GetOrderStatus(std::string state) {
    //v2:  rejected submitted     partial-filled  filled    canceled partial-canceled   
    if (state == "canceled" || state == "partial-canceled") {
        return LF_CHAR_Canceled;
    }
    //没有这个状态
    else if (state == "submitting") {
        return LF_CHAR_NotTouched;
    }
    else if (state == "partial-filled") {
        return  LF_CHAR_PartTradedQueueing;
    }
    else if (state == "submitted") {
        return LF_CHAR_NotTouched;
    }
    else if (state == "filled") {
        return LF_CHAR_AllTraded;
    }
    //edit here
    else if (state == "rejected")
        return LF_CHAR_Error;
    return LF_CHAR_AllTraded;
}

/**
 * req functions
 * 查询账户持仓
 */
void TDEngineHuobi::req_investor_position(const LFQryPositionField* data, int account_index, int requestId){
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitHuobi& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key << " (InstrumentID) " << data->InstrumentID);

    LFRspPositionField pos;
    memset(&pos, 0, sizeof(LFRspPositionField));
    strncpy(pos.BrokerID, data->BrokerID, 32);
    strncpy(pos.InvestorID, data->InvestorID, 32);
    strncpy(pos.InstrumentID, data->InstrumentID, 31);
    pos.PosiDirection = LF_CHAR_Long;
    pos.HedgeFlag = LF_CHAR_Speculation;
    pos.Position = 0;
    pos.YdPosition = 0;
    pos.PositionCost = 0;

    int errorId = 0;
    std::string errorMsg = "";
    Document d;
    KF_LOG_INFO(logger, "[req_investor_position] (data->BrokerID)" << data->BrokerID);
    if( !strcmp(data->BrokerID, "master-spot")) 
    {
        get_account(unit, d);
        KF_LOG_INFO(logger, "[req_investor_position] get_spot_account");
    }
    else if(!strcmp(data->BrokerID, "master-margin"))
    {
        get_account(unit, d,true);
        KF_LOG_INFO(logger, "[req_investor_position] get_margin_account");
    }
    else if( !strcmp(data->BrokerID, "sub-spot") || !strcmp(data->BrokerID, "sub-margin"))
    {
        get_sub_account(data->InvestorID, unit, d);
        KF_LOG_INFO(logger, "[req_investor_position] get_sub_account");
    }

    if(!d.IsObject()){
        KF_LOG_INFO(logger,"account3");
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_INFO(logger,"account1");
    if(d.IsObject() && d.HasMember("status"))
    {
        std::string status=d["status"].GetString();
        KF_LOG_INFO(logger, "[req_investor_position] (get status)" );
        //errorId =  std::round(std::stod(d["id"].GetString()));
        errorId = 0;
        KF_LOG_INFO(logger, "[req_investor_position] (status)" << status);
        KF_LOG_INFO(logger, "[req_investor_position] (errorId)" << errorId);
        if(status != "ok") {
            errorId=520;
            if (d.HasMember("err-msg") && d["err-msg"].IsString()) {
                std::string tab="\t";
                errorMsg = d["err-code"].GetString()+tab+d["err-msg"].GetString();
            }
            KF_LOG_ERROR(logger, "[req_investor_position] failed!" << " (rid)" << requestId << " (errorId)" << errorId
                                                                   << " (errorMsg) " << errorMsg);
            raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_HUOBI, 1, requestId, errorId, errorMsg.c_str());
        }
    }
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_HUOBI, 1, requestId);
    /*账户余额
    GET /v1/account/accounts/{account-id}/balance
    {
        "data": {
            "id": 100009,
            "type": "spot",
            "state": "working",
            "list": [
                {
                    "currency": "usdt",
                "type": "trade",
                "balance": "5007.4362872650"
                },
                {
                "currency": "usdt",
                "type": "frozen",
                "balance": "348.1199920000"
                }
            ],
            "user-id": 10000
        }
    }
    */
    /*子账户余额
    {
  "status": "ok",
    "data": [
    {
      "id": 9910049,
      "type": "spot",
      "list": 
      [
        {
          "currency": "btc",
          "type": "trade",
          "balance": "1.00"
        },
        {
          "currency": "eth",
          "type": "trade",
          "balance": "1934.00"
        }
      ]
    },
    {
      "id": 9910050,
      "type": "point",
      "list": []
    }
    ]
}
    */
    KF_LOG_INFO(logger,"account2");
    std::vector<LFRspPositionField> tmp_vector;
    if(!d.HasParseError() && d.HasMember("data"))
    {
        if( !strcmp(data->BrokerID, "master-spot") || !strcmp(data->BrokerID, "master-margin")){
            auto& accounts = d["data"]["list"];
            size_t len = d["data"]["list"].Size();
            KF_LOG_INFO(logger, "[req_investor_position] (accounts.length)" << len);
            for(size_t i = 0; i < len; i++)
            {
                    std::string type = accounts.GetArray()[i]["type"].GetString();
                    if(type != "trade"){//frozen
                        continue;
                    }
                    std::string symbol = accounts.GetArray()[i]["currency"].GetString();
                    std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
                    //if(symbol != "btc"||symbol != "usdt" || symbol !="etc" || symbol != "eos")continue;
                    //KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol);
                    if(ticker.length() > 0){
                        strncpy(pos.InstrumentID, ticker.c_str(), 31);
                        pos.Position = std::round(std::stod(accounts.GetArray()[i]["balance"].GetString()) * scale_offset);
                        tmp_vector.push_back(pos);
                        KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId 
                                    << " (ticker) " << ticker << " (position) " << pos.Position);
                    }
            }
        }
        else if( !strcmp(data->BrokerID, "sub-spot") || !strcmp(data->BrokerID, "sub-margin")){
            //KF_LOG_INFO(logger,"in");
            Value &node = d["data"];
            int len = node.Size();
            for(int i=0; i < len; i++){
                std::string symbol = node.GetArray()[i]["currency"].GetString();
                std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
                if(ticker.length() > 0){
                    strncpy(pos.InstrumentID, ticker.c_str(), 31);
                    pos.Position = std::round(std::stod(node.GetArray()[i]["balance"].GetString()) * scale_offset);
                    tmp_vector.push_back(pos);
                    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId 
                                << " (ticker) " << ticker << " (position) " << pos.Position);
                }               
            }
        }
    }

    //send the filtered position
    int position_count = tmp_vector.size();
    KF_LOG_INFO(logger,"position_count="<<position_count);
    if(position_count > 0) {
        for (int i = 0; i < position_count; i++) {
            on_rsp_position(&tmp_vector[i], i == (position_count - 1), requestId, errorId, errorMsg.c_str());
        }
    }
    else
    {
        KF_LOG_INFO(logger, "[req_investor_position] (!findSymbolInResult) (requestId)" << requestId);
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineHuobi::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineHuobi::dealPriceVolume(AccountUnitHuobi& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,
            std::string& nDealPrice,std::string& nDealVolume){
    KF_LOG_DEBUG(logger, "[dealPriceVolume] (symbol)" << symbol);
    KF_LOG_DEBUG(logger, "[dealPriceVolume] (price)" << nPrice);
    KF_LOG_DEBUG(logger, "[dealPriceVolume] (volume)" << nVolume);
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(symbol);
    std::unique_lock<std::mutex> rest_price_volume_mutex(*mutex_map_pricevolum);
    auto it = unit.mapPriceVolumePrecision.find(ticker);
    if(it == unit.mapPriceVolumePrecision.end())
    {
        KF_LOG_INFO(logger, "[dealPriceVolume] symbol not find :" << ticker);
        nDealVolume = "0";
        rest_price_volume_mutex.unlock();
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

//多线程发单
void TDEngineHuobi::send_order_thread(AccountUnitHuobi* unit,string ticker,const LFInputOrderField data,int requestId,int errorId,std::string errorMsg)
{
    KF_LOG_DEBUG(logger, "[send_order_thread] current thread is:" <<std::this_thread::get_id()<<" current CPU is  "<<sched_getcpu());
    Document d;
    std::string fixedPrice;
    std::string fixedVolume;
    
    //edit begin  必要时可以注释掉
    if (GetSide(data.Direction) == "buy" && GetType(data.OrderPriceType) == "market")
    {
        double dVolume, dPrice, res;
        dVolume = data.Volume * 1.0 / scale_offset;
        dPrice = data.ExpectPrice * 1.0 / scale_offset;
        res = dVolume * dPrice;
        fixedVolume = std::to_string(res);
        //cannot get data.ExpectPrice 
        //wait to be edited
        KF_LOG_INFO(logger, "[send_order_thread] data.Volume: " << data.Volume << " data.ExpectPrice: " << data.ExpectPrice <<
            " dVolume: " << dVolume << " dPrice " << dPrice << " res: " << res << " fixedVolume: " << fixedVolume);
    }
    else
    //edit end
        dealPriceVolume(*unit, data.InstrumentID, data.LimitPrice, data.Volume, fixedPrice, fixedVolume);


    if(fixedVolume == "0"){
        KF_LOG_DEBUG(logger, "[req_order_insert] fixed Volume error (no ticker)" << ticker);
        errorId = 200;
        errorMsg = data.InstrumentID;
        errorMsg += " : no such ticker";
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_HUOBI, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_INFO(logger,"[req_order_insert] cys_ticker "<<ticker.c_str());
    
    //cid_map.insert(make_pair(cid, data));
    if(!unit->is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        return;
    }
    std::string cid;
    //lock
    send_order(*unit, ticker.c_str(), GetSide(data.Direction).c_str(),GetType(data.OrderPriceType).c_str(), fixedVolume, fixedPrice, d, cid, data, is_post_only(&data));

    //not expected response
    if(!d.IsObject()){
        errorId = 100;
        errorMsg = "send_order http response has parse error or is not json. please check the log";
        KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
                                                                           errorId << " (errorMsg) " << errorMsg);
    } 
    else  if(d.HasMember("status"))
    {//发单成功
        std::string status =d["status"].GetString();
        if(status == "ok") 
        {
            //if send successful and the exchange has received ok, then add to  pending query order list
            std::string remoteOrderId = d["data"].GetString();

            bool update = true;
            std::vector<UpdateMsg>::iterator it;
            std::unique_lock<std::mutex> lck1(update_mutex);
            for(it = Updatemsg_vec.begin(); it != Updatemsg_vec.end(); it++){
                if(it->orderId == remoteOrderId){
                    update = false;
                    break;
                }
            }
            lck1.unlock();

            if(update){
                UpdateMsg updatemsg;
                updatemsg.orderId = remoteOrderId;
                lck1.lock();
                Updatemsg_vec.push_back(updatemsg);
                lck1.unlock();

                localOrderRefRemoteOrderId.insert(make_pair(std::string(data.OrderRef), remoteOrderId));
                KF_LOG_INFO(logger, "[req_order_insert] after send  (rid)" << requestId << " (OrderRef) " <<
                                                                           data.OrderRef << " (remoteOrderId) "
                                                                           << remoteOrderId);
                LFRtnOrderField rtn_order;
                memset(&rtn_order, 0, sizeof(LFRtnOrderField));
                strncpy(rtn_order.BusinessUnit,remoteOrderId.c_str(),21);
                rtn_order.OrderStatus = LF_CHAR_NotTouched;
                rtn_order.VolumeTraded = 0;
                
                strcpy(rtn_order.ExchangeID, "huobi");
                strncpy(rtn_order.UserID, unit->api_key.c_str(), 16);
                strncpy(rtn_order.InstrumentID, data.InstrumentID, 31);
                rtn_order.Direction = data.Direction;
                //No this setting on Huobi
                rtn_order.TimeCondition = LF_CHAR_GTC;
                rtn_order.OrderPriceType = data.OrderPriceType;
                strncpy(rtn_order.OrderRef, data.OrderRef, 13);
                rtn_order.VolumeTotalOriginal = data.Volume;
                rtn_order.LimitPrice = data.LimitPrice;
                rtn_order.VolumeTotal = data.Volume;

                on_rtn_order(&rtn_order);
                raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
                                        source_id, MSG_TYPE_LF_RTN_ORDER_HUOBI,
                                        1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);

                KF_LOG_DEBUG(logger, "[req_order_insert] (addNewOrderToMap)" );
                addNewOrderToMap(*unit, rtn_order);
                raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_HUOBI, 1,
                                              requestId, errorId, errorMsg.c_str());
                KF_LOG_DEBUG(logger, "[req_order_insert] success" );
                std::unique_lock<std::mutex> lck(account_mutex);
                mapInsertOrders.insert(std::make_pair(data.OrderRef,unit));
                lck.unlock();
                return;
            }

        }
        else 
        {
            //errorId = std::round(std::stod(d["id"].GetString()));
            errorId=404;
            if(d.HasMember("err-msg") && d["err-msg"].IsString()){
                std::string tab="\t";
                errorMsg = d["err-code"].GetString()+tab+d["err-msg"].GetString();
            }
            KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
                                                                               errorId << " (errorMsg) " << errorMsg);
        }
    }
    
    //unlock
    if(errorId != 0)
    {
        //********************edit begin**********************
        auto tmp_itr = cid_map.find(cid);
        if (tmp_itr != cid_map.end()) 
        {
            cid_map.erase(tmp_itr);
        }
        //*********************edit end***********************
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_HUOBI, 1, requestId, errorId, errorMsg.c_str());
    }
    
    //********************edit begin**********************
    else
        requestId_map.insert(make_pair(cid, requestId));
    //*********************edit end***********************

}

//发单
void TDEngineHuobi::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time){

    AccountUnitHuobi& unit = get_current_account();
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_HUOBI, 1/*ISLAST*/, requestId);

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
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_HUOBI, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);
   
    if(nullptr == m_ThreadPoolPtr)
    {
        send_order_thread(&unit,ticker,*data,requestId,errorId,errorMsg);
        
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineHuobi::send_order_thread,this,&unit,ticker,*data,requestId,errorId,errorMsg)); 
        KF_LOG_DEBUG(logger, "[req_order_insert] [left thread count ]: ] "<< m_ThreadPoolPtr->idlCount());
    }

}




void TDEngineHuobi::action_order_thread(AccountUnitHuobi* unit,string ticker,const LFOrderActionField data,int requestId,std::string remoteOrderId,int errorId,std::string errorMsg)
{
    if(!unit->is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        return;
    }
    Document d;
    addRemoteOrderIdOrderActionSentTime(&data, requestId, std::string(data.OrderRef), *unit ,remoteOrderId);
    cancel_order(*unit, ticker, remoteOrderId, d);

    std::string strSuccessCode =  "ok";
    if(!d.HasParseError() && d.HasMember("status") && strSuccessCode != d["status"].GetString()) {
        errorId = 404;
        if(d.HasMember("err-msg") && d["err-msg"].IsString())
        {
            std::string tab="\t";
            errorMsg = d["err-code"].GetString()+tab+d["err-msg"].GetString();
        }
        KF_LOG_ERROR(logger, "[req_order_action] cancel_order failed!" << " (rid)" << requestId
                                                                       << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    if(errorId != 0)
    {
        on_rsp_order_action(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_HUOBI, 1, 
            requestId, errorId, errorMsg.c_str());

        Document json;
        int isTraded = orderIsTraded(*unit,ticker,remoteOrderId,json);
        if(isTraded == 1){
            KF_LOG_INFO(logger,"[req_order_action] AllTraded or Canceled, can not cancel again.");
            return;
        }
    } 
    else 
    {
         auto it = unit->restOrderStatusMap.find(remoteOrderId);
		if (it == unit->restOrderStatusMap.end())
		{ 
            errorId=120;
            errorMsg= "cancel_order ,no order match,cancleFailed";
            //on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
			KF_LOG_ERROR_FMT(logger, "TDEngineHuobi::onOrder,no order match(%s),cancleFailed",remoteOrderId.c_str());
			return;
		}
    }
}
//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
void TDEngineHuobi::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId, AccountUnitHuobi& unit, std::string orderid){
    std::lock_guard<std::mutex> guard_mutex_order_action1(*mutex_orderaction_waiting_response1);

    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;
    newOrderActionSent.sentNameTime = getTimestamp();
    newOrderActionSent.unit = unit;
    newOrderActionSent.orderid = orderid;
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    remoteOrderIdOrderActionSentTime[orderid] = newOrderActionSent;
}

void TDEngineHuobi::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time){
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
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_HUOBI, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    AccountUnitHuobi& unit = *(it->second);
    lck.unlock();

   // AccountUnitHuobi& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_HUOBI, 1, requestId);
    
   
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_HUOBI, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);

    std::map<std::string, std::string>::iterator itr = localOrderRefRemoteOrderId.find(data->OrderRef);
    std::string remoteOrderId;
    if(itr == localOrderRefRemoteOrderId.end()) {
        errorId = 1;
        std::stringstream ss;
        ss << "[req_order_action] not found in localOrderRefRemoteOrderId map (orderRef) " << data->OrderRef;
        errorMsg = ss.str();
        KF_LOG_ERROR(logger, "[req_order_action] not found in localOrderRefRemoteOrderId map. "
                << " (rid)" << requestId << " (orderRef)" << data->OrderRef << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_HUOBI, 1, requestId, errorId, errorMsg.c_str());
        return;
    } else {
        remoteOrderId = itr->second;
        KF_LOG_DEBUG(logger, "[req_order_action] found in localOrderRefRemoteOrderId map (orderRef) "
                << data->OrderRef << " (remoteOrderId) " << remoteOrderId);
    }
    if(nullptr == m_ThreadPoolPtr)
    {
        action_order_thread(&unit,ticker,*data,requestId,remoteOrderId,errorId,errorMsg);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineHuobi::action_order_thread,this,&unit,ticker,*data,requestId,remoteOrderId,errorId,errorMsg));
    }

}

void TDEngineHuobi::req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId)
{
    AccountUnitHuobi& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_withdraw_currency]" << " (rid) " << requestId
                                              << " (APIKey) " << unit.api_key
                                              << " (Currency) " << data->Currency
                                              << " (Volume) " << data->Volume
                                              << " (Address) " << data->Address
                                              << " (Tag) " << data->Tag);
    send_writer->write_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_HUOBI, 1, requestId);
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
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_HUOBI, 1, requestId, errorId, errorMsg.c_str());
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
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_HUOBI, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }
    string volume = to_string(data->Volume);
    string fixvolume;
    dealnum(volume,fixvolume);
    Document json;
    withdrawl_currency(address, fixvolume, currency, tag,json,unit);
    if (json.HasParseError() || !json.IsObject()){
        errorId = 101;
        errorMsg = "json has parse error.";
        KF_LOG_ERROR(logger,"[withdrawl_currency] json has parse error.");
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_HUOBI, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }
    if(json.HasMember("data") && json["data"].IsNumber()){
        int64_t id = json["data"].GetInt64();
        std::string idstr = std::to_string(id);
        LFWithdrawField* dataCpy = (LFWithdrawField*)data;
        strncpy(dataCpy->ID,idstr.c_str(),64);
        KF_LOG_INFO(logger, "[withdrawl_currency] (id) " << idstr);
        KF_LOG_INFO(logger, "[withdrawl_currency] withdrawl success. no error message");
        on_rsp_withdraw(dataCpy,requestId,errorId,errorMsg.c_str());
    }else if(json.HasMember("err-msg") && json["err-msg"].IsString()){
        string message = json["err-msg"].GetString();
        KF_LOG_INFO(logger, "[withdrawl_currency] (msg) " << message);
        KF_LOG_INFO(logger, "[withdrawl_currency] withdrawl faild!");
        errorId = 102;
        errorMsg = message;

        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_HUOBI, 1, 
            requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineHuobi::withdrawl_currency(string address, string amount, string asset, string addresstag, Document& json, AccountUnitHuobi& unit)
{
    KF_LOG_INFO(logger, "[withdraw_currency]");
    //火币post批量撤销订单
    std::string requestPath = "/v1/dw/withdraw/api/create";
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("address");
    writer.String(address.c_str());
    writer.Key("amount");
    writer.String(amount.c_str());
    writer.Key("currency");
    writer.String(asset.c_str());
    /*if(asset == "USDT-ERC20")
    {
        writer.Key("chain");
        writer.String("usdterc20");
    }
    writer.Key("addr-tag");
    writer.String(addresstag.c_str());*/
    //write Signature
    writer.EndObject();
    KF_LOG_INFO(logger,"s1="<<s.GetString());
    auto response = Post(requestPath,s.GetString(),unit);
    getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineHuobi::dealnum(string pre_num,string& fix_num)
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

void TDEngineHuobi::req_inner_transfer(const LFTransferField* data, int account_index, int requestId)
{
    AccountUnitHuobi& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_inner_transfer]" << " (rid) " << requestId
                                              << " (APIKey) " << unit.api_key
                                              << " (from) " << data->From
                                              << " (Currency) " << data->Currency
                                              << " (Volume) " << data->Volume );
    send_writer->write_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_HUOBI, 1, requestId);
    int errorId = 0;
    std::string errorMsg = "";
    Document json;
    std::string type = "";
    std::string Currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
    if(Currency.length() == 0) {
        errorId = 200;
        errorMsg = "Currency not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_inner_transfer]: Currency not in WhiteList , ignore it. (rid)" << requestId << " (errorId)" <<
                                                                                      errorId << " (errorMsg) " << errorMsg);
        on_rsp_transfer(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_HUOBI, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    std::string volume = std::to_string(data->Volume);
    std::string fixvolume;
    dealnum(volume,fixvolume);
    if ( !strcmp(data->From, "master-spot") &&!strcmp(data->To, "master-margin"))
    {
        type = "0";
        inner_transfer(data->Symbol, Currency, fixvolume,type ,json ,unit);
    }
    else if( (!strcmp(data->From, "master-margin") &&!strcmp(data->To, "master-spot")) )
    {
        type = "1";
        inner_transfer(data->Symbol, Currency, fixvolume,type ,json ,unit);
    }
    else if( (!strcmp(data->From, "sub-spot") &&!strcmp(data->To, "master-spot")) )
    {
        type = "master-transfer-in";
        KF_LOG_INFO(logger, "[sub_inner_transfer] sub to master");
        sub_inner_transfer(data->FromName, Currency, fixvolume, type ,json, unit);
    }
    else if( (!strcmp(data->From, "master-spot") &&!strcmp(data->To, "sub-spot")) )
    {
        type = "master-transfer-out";
        KF_LOG_INFO(logger, "[sub_inner_transfer] master to sub");
        sub_inner_transfer(data->ToName, Currency, fixvolume, type ,json, unit);
    }
    else if( (!strcmp(data->From, "master-point") &&!strcmp(data->To, "sub-point")) )
    {
        type = "master-point-transfer-out";
        KF_LOG_INFO(logger, "[sub_inner_transfer] master point to sub point");
        sub_inner_transfer(data->ToName, Currency, fixvolume, type ,json, unit);
    }
    else if( (!strcmp(data->From, "sub-point") &&!strcmp(data->To, "master-spot")) )
    {
        type = "master-point-transfer-in";
        KF_LOG_INFO(logger, "[sub_inner_transfer] sub point to master point");
        sub_inner_transfer(data->FromName, Currency, fixvolume, type ,json, unit);
    }
    else
    {
        KF_LOG_ERROR(logger,"[req_inner_transfer] error! From or To is error!");
        return ;
    }
    if (json.HasParseError() || !json.IsObject()){
        if(json.HasMember("code"))
        {
            errorId = json["code"].GetInt();
        }
        errorMsg = "json has parse error.";
        KF_LOG_ERROR(logger,"[req_inner_transfer] json has parse error.");
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_HUOBI, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }
    if(json.HasMember("data") && json["data"].IsNumber()){
        
        KF_LOG_INFO(logger, "[req_inner_transfer] inner_transfer success. no error message");
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
    }else if(json.HasMember("err-msg") && json["err-msg"].IsString()){
        string message = json["err-msg"].GetString();
        errorId = 102;
        KF_LOG_INFO(logger, "[req_inner_transfer] (msg) " << message);
        KF_LOG_INFO(logger, "[req_inner_transfer] inner_transfer faild!");
        errorMsg = message;
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_HUOBI, 1, 
            requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineHuobi::inner_transfer(string symbol, string currency, string amount, string type, Document& json, AccountUnitHuobi& unit)
{
    std::string requestPath;
    if(type == "0")
    {
        requestPath = "/v1/cross-margin/transfer-in";//"/v1/dw/transfer-in/margin";
        KF_LOG_INFO(logger, "[inner_transfer] spot to margin");
    }
    else
    {
        requestPath = "/v1/cross-margin/transfer-out";//"/v1/dw/transfer-out/margin";
        KF_LOG_INFO(logger, "[inner_transfer] margin to spot" );
    }
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    //writer.Key("symbol");
    //writer.String(symbol.c_str());
    writer.Key("currency");
    writer.String(currency.c_str());
    writer.Key("amount");
    writer.String(amount.c_str());
    //write Signature
    writer.EndObject();
    KF_LOG_INFO(logger,"s2="<<s.GetString());
    auto response = Post(requestPath,s.GetString(),unit);
    getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineHuobi::sub_inner_transfer(string subuid,string currency,string amount,string type, Document& json, AccountUnitHuobi& unit)
{
    KF_LOG_INFO(logger, "[sub_inner_transfer]");
    //火币post批量撤销订单
    std::string requestPath = "/v1/subuser/transfer";
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("sub-uid");
    writer.String(subuid.c_str());
    writer.Key("currency");
    writer.String(currency.c_str());
    writer.Key("amount");
    writer.String(amount.c_str());
    writer.Key("type");
    writer.String(type.c_str());
    //write Signature
    writer.EndObject();
    KF_LOG_INFO(logger,"s3="<<s.GetString());
    auto response = Post(requestPath,s.GetString(),unit);
    getResponse(response.status_code, response.text, response.error.message, json);
}

int TDEngineHuobi::get_transfer_status(std::string status,bool is_withdraw)
{
    int local_status =LFTransferStatus::TRANSFER_STRATUS_SUCCESS;
    if(status == "confirm-error")
    {
        local_status = LFTransferStatus::TRANSFER_STRATUS_FAILURE;
    }
    else if (status == "wallet-reject" || status == "reject")
    {
        local_status = LFTransferStatus::TRANSFER_STRATUS_REJECTED;
    }
    else if(status == "submitted" || status == "reexamine" || status == "pass" || status == "pre-transfer" || 
        status == "wallet-transfer" || status == "unknown" || status == "confirming" || status == "orphan" ||
        (!is_withdraw && status == "confirmed"))
    {
        local_status = LFTransferStatus::TRANSFER_STRATUS_PROCESSING;
    }
    else if(status == "canceled" || status == "repealed" )
    {
        local_status = LFTransferStatus::TRANSFER_STRATUS_CANCELED;
    }
    
    return local_status;
}

void TDEngineHuobi::req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw)
{
    KF_LOG_INFO(logger, "[req_transfer_history]");
    AccountUnitHuobi& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_transfer_history] (api_key)" << unit.api_key);

    LFTransferHistoryField his;
    memset(&his, 0, sizeof(LFTransferHistoryField));
    strncpy(his.UserID, data->UserID, 64);
    strncpy(his.ExchangeID, "huobi", 11);

    int errorId = 0;
    std::string errorMsg = "";
    KF_LOG_INFO(logger, "[req_transfer_history] (data->UserID)" << data->UserID);

    std::string Currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
    if(Currency.length() == 0) {
        errorId = 200;
        errorMsg = "Currency not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_inner_transfer]: Currency not in WhiteList , ignore it. (rid)" << requestId << " (errorId)" <<
                                                                                      errorId << " (errorMsg) " << errorMsg);
        on_rsp_transfer_history(&his, isWithdraw, 1, requestId,errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_HUOBI, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    
    //string asset = data->Currency;
    string startTime = data->StartTime;
    string endTime = data->EndTime;
    std::string type;
    Document d;
    if(!isWithdraw)
    {
        type = "deposit";
        //get_deposit_history(unit, d, asset, data->Status, startTime, endTime);
        KF_LOG_INFO(logger, "[req_transfer_history] req_deposit_history");
    }
    else
    {
        type = "withdraw";
        //get_withdraw_history(unit, d, asset, data->Status, startTime, endTime);
        KF_LOG_INFO(logger, "[req_transfer_history] req_withdraw_history");
    }
    get_transfer_history(Currency,type,unit,d);
    printResponse(d);

    if(d.HasParseError() )
    {
        errorId=100;
        errorMsg= "req_transfer_history http response has parse error. please check the log";
        KF_LOG_ERROR(logger, "[req_transfer_history] req_transfer_history error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }

    /*if(!d.HasParseError() && d.IsObject() && d.HasMember("code") && d["code"].IsNumber())
    {
        errorId = d["code"].GetInt();
        if(d.HasMember("msg") && d["msg"].IsString())
        {
            errorMsg = d["msg"].GetString();
        }
        KF_LOG_ERROR(logger, "[req_transfer_history] req_transfer_history failed! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    send_writer->write_frame(data, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_HUOBI, 1, requestId);*/

    std::vector<LFTransferHistoryField> tmp_vector;
    if(!d.HasParseError() && d.IsObject() && d.HasMember("data") && d["data"].IsArray()){
        Value &node = d["data"];
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
            std::string tag = node.GetArray()[i]["address-tag"].GetString();
            strncpy(his.Tag, tag.c_str(), 64);
            int64_t createdAt = node.GetArray()[i]["created-at"].GetInt64();
            std::string timestamp = std::to_string(createdAt);
            int64_t updatedAt = node.GetArray()[i]["updated-at"].GetInt64();
            std::string updatetime = std::to_string(updatedAt);
            //strncpy(his.TimeStamp, timestamp.c_str(), 32);
            strncpy(his.StartTime, timestamp.c_str(), 32);
            strncpy(his.EndTime, updatetime.c_str(), 32);
            std::string status = node.GetArray()[i]["state"].GetString();
            his.Status = get_transfer_status(status,isWithdraw);
            double amount = node.GetArray()[i]["amount"].GetDouble();
            his.Volume = std::round(amount * scale_offset);
            std::string address = node.GetArray()[i]["address"].GetString();
            strncpy(his.Address, address.c_str(), 130);
            his.IsWithdraw = isWithdraw;
            //if(node.GetArray()[i].HasMember("id")){
                int64_t id = node.GetArray()[i]["id"].GetInt64();
                std::string idstr = std::to_string(id);
                strncpy(his.FromID, idstr.c_str(), 64 );
            //}
            //strncpy(his.Tag, d["depositList"].GetArray()[i]["addressTag"].GetString(), 64);
            std::string walletTxId = node.GetArray()[i]["tx-hash"].GetString();
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
    else if(d.IsObject() && d.HasMember("err-msg") && d["err-msg"].IsString())
    {
        errorId = 104;
        errorMsg = d["err-msg"].GetString();
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
        raw_writer->write_error_frame(&his, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_HUOBI, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineHuobi::req_get_kline_via_rest(const GetKlineViaRest* data, int account_index, int requestId, long rcv_time)
{
    KF_LOG_INFO(logger, "TDEngineHuobi::req_get_kline_via_rest: (symbol)" << data->Symbol << " (interval)" << data->Interval);
    writer->write_frame(data, sizeof(GetKlineViaRest), source_id, MSG_TYPE_LF_GET_KLINE_VIA_REST, 1/*islast*/, requestId);

    AccountUnitHuobi& unit = account_units[account_index];
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(data->Symbol);
    if (ticker.empty())
    {
        KF_LOG_INFO(logger, "symbol not in white list");
        return;
    }

    int param_limit = data->Limit;
    if (param_limit > 1000)
        param_limit = 1000;
    else if (param_limit < 1)
        param_limit = 1;


    const auto static url = "https://api.huobi.pro/market/history/kline";
    cpr::Response response;
    response = cpr::Get(Url{ url }, Parameters{ {"symbol", ticker},{"period", data->Interval},{"size", to_string(param_limit)} });
    KF_LOG_INFO(logger, "rest response url " << response.url);

    if (response.status_code >= 400) {
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

    /*[{"id": 1499184000, "amount" : 37593.0266, "count" : 0, "open" : 1935.2000, "close" : 1879.0000, "low" : 1856.0000, "high" : 1940.0000, "vol" : 71031537.97866500}]*/
    KF_LOG_INFO(logger, "TDEngineHuobi::req_get_kline_via_rest: parse response" << response.text.c_str());

    Document d;
    d.Parse(response.text.c_str());

    if (d.IsArray()) {
        LFBarSerial1000Field bars;
        memset(&bars, 0, sizeof(bars));
        strncpy(bars.InstrumentID, data->Symbol, 31);
        strcpy(bars.ExchangeID, "huobi");

        for (int i = 0; i < d.Size(); i++) {
            if (!d[i].IsObject()) {
                KF_LOG_INFO(logger, "TDEngineHuobi::req_get_kline_via_rest: response is abnormal" << response.text.c_str());
                break;
            }

            bars.BarSerial[i].StartUpdateMillisec = d[i]["id"].GetInt64() * 1000;
            bars.BarSerial[i].PeriodMillisec      = GetMillsecondByInterval(data->Interval);
            bars.BarSerial[i].EndUpdateMillisec   = bars.BarSerial[i].StartUpdateMillisec + bars.BarSerial[i].PeriodMillisec;
            
            //scale_offset = 1e8
            bars.BarSerial[i].Open           = std::round(d[i]["open"].GetDouble() * scale_offset);
            bars.BarSerial[i].Close          = std::round(d[i]["close"].GetDouble() * scale_offset);
            bars.BarSerial[i].Low            = std::round(d[i]["low"].GetDouble() * scale_offset);
            bars.BarSerial[i].High           = std::round(d[i]["high"].GetDouble() * scale_offset);
            bars.BarSerial[i].Volume         = std::round(d[i]["amount"].GetDouble() * scale_offset);
            bars.BarSerial[i].BusinessVolume = std::round(d[i]["vol"].GetDouble() * scale_offset);

            bars.BarSerial[i].TransactionsNum = d[i]["count"].GetInt();

            bars.BarLevel = i + 1;
        }
        on_bar_serial1000(&bars, data->RequestID);
    }
    else if (!d.IsArray()) {
        KF_LOG_INFO(logger, "TDEngineHuobi::req_get_kline_via_rest: response is abnormal");
    }
}

void TDEngineHuobi::get_transfer_history(std::string currency,std::string type, AccountUnitHuobi& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_transfer_history]");
    std::string requestPath = "/v1/query/deposit-withdraw";
    std::string queryString("");

    queryString.append("&currency=");
    queryString.append(currency);

    queryString.append("&type=");
    queryString.append(type);
    std::string parameters = queryString;
    //requestPath += queryString;

    /*std::string strTimestamp = getHuobiTime();
    string strSignatrue=getHuobiSignatrue(NULL,0,strTimestamp,requestPath,"GET\n",unit);
    string url = unit.baseUrl + requestPath+"?"+"AccessKeyId="+unit.api_key+"&"+
                    "SignatureMethod=HmacSHA256&"+
                    "SignatureVersion=2&"+
                    "Timestamp="+strTimestamp+"&"+
                    "Signature="+strSignatrue +"&"+ queryString;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    const auto response = cpr::Get(Url{url},
                                   Header{{"Content-Type", "application/json"}}, Timeout{10000} );
    KF_LOG_INFO(logger, "[get_transfer_history] (url) " << url << " (response.status_code) " << response.status_code <<
                                       " (response.error.message) " << response.error.message <<
                                       " (response.text) " << response.text.c_str());
    lock.unlock();*/
    /*StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("currency");
    writer.String(currency.c_str());
    writer.Key("type");
    writer.String(type.c_str());
    writer.EndObject();*/
    const auto response = Get(requestPath,"",unit,parameters);
    json.Parse(response.text.c_str());

}

void TDEngineHuobi::get_sub_account(string subuid, AccountUnitHuobi& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_sub_account]");
    //std::string requestPath = "/v1/account/accounts/" + subuid;
    /*std::string requestPath = "/v1/account/accounts";
    std::string parameters = "&sub-uid=" + subuid;
    const auto response = Get(requestPath,"",unit,parameters);*/
    std::string requestPath = "/v1/subuser/aggregate-balance";
    const auto response = Get(requestPath,"",unit,"");
    json.Parse(response.text.c_str());
    //KF_LOG_INFO(logger, "[get_account] (account info) "<<response.text.c_str());
    return ;
}


void TDEngineHuobi::addNewOrderToMap(AccountUnitHuobi& unit, LFRtnOrderField& rtn_order){
    KF_LOG_DEBUG(logger, "[rest addNewOrderToMap]" );
    //add new orderId for GetAndHandleOrderTradeResponse
    std::unique_lock<std::mutex> rest_order_status_mutex(*mutex_order_and_trade);
    string remoteOrderId = rtn_order.BusinessUnit;
    unit.restOrderStatusMap.insert(std::make_pair(remoteOrderId,rtn_order));
    rest_order_status_mutex.unlock();
    KF_LOG_INFO(logger, "[addNewOrderToMap] (InstrumentID) " << rtn_order.InstrumentID
                                                                       << " (OrderRef) " << rtn_order.OrderRef
                                                                       << " (remoteOrderId) " << rtn_order.BusinessUnit
                                                                       << "(VolumeTraded)" << rtn_order.VolumeTraded);

}


void TDEngineHuobi::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineHuobi::loopwebsocket");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineHuobi::loopwebsocket, this)));

    KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineHuobi::loopOrderActionNoResponseTimeOut");
    orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineHuobi::loopOrderActionNoResponseTimeOut, this)));

}

void TDEngineHuobi::loopwebsocket()
{
    time_t nLastTime = time(0);

    while(isRunning)
    {
        time_t nNowTime = time(0);
        if(m_isPong && (nNowTime - nLastTime>= 30))
        {
            m_isPong = false;
            nLastTime = nNowTime;
            KF_LOG_INFO(logger, "[loop] last time = " <<  nLastTime << ",now time = " << nNowTime << ",m_isPong = " << m_isPong);
            //m_shouldPing = true;
            lws_callback_on_writable(m_conn);
        }
        //KF_LOG_INFO(logger, "TDEngineHuobi::loop:lws_service");
        for (size_t idx = 0; idx < account_units.size(); idx++)
        {
            AccountUnitHuobi& unit = account_units[idx];
            lws_service(unit.context, rest_get_interval_ms );
        }
            
    }
}




void TDEngineHuobi::loopOrderActionNoResponseTimeOut()
{
    KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        orderActionNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
}
/*
void TDEngineHuobi::loopOrderInsertNoResponseTimeOut()
{
    KF_LOG_INFO(logger, "[loopOrderInsertNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        orderInsertNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
}
*/
void TDEngineHuobi::orderActionNoResponseTimeOut(){
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut]");
    std::string cancelstr = "canceled";
    int errorId = 100;
    std::string errorMsg = "OrderAction has none response for a long time, please send OrderAction again";

    std::lock_guard<std::mutex> guard_mutex_order_action1(*mutex_orderaction_waiting_response1);

    int64_t currentNano = getTimestamp();
    int64_t timeBeforeNano = currentNano - orderaction_max_waiting_seconds * 1000;
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (currentNano)" << currentNano << " (timeBeforeNano)" << timeBeforeNano);
    std::map<std::string, OrderActionSentTime>::iterator itr;
    for(itr = remoteOrderIdOrderActionSentTime.begin(); itr != remoteOrderIdOrderActionSentTime.end();)
    {
        if(itr->second.sentNameTime < timeBeforeNano)
        {
            KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.erase remoteOrderId)" << itr->first );
            std::string url = "/v1/order/orders/" + itr->second.orderid;
            KF_LOG_INFO(logger,"(url1)"<<url);
            const auto response = Get(url,"",itr->second.unit,"");
            Document json;
            json.Parse(response.text.c_str());
            if (json.IsObject()){
                std::string state = json["data"]["state"].GetString();
                if(state != "canceled"){
                    on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
                    itr=remoteOrderIdOrderActionSentTime.erase(itr);                    
                }
                else if(state == "canceled"){
                    string remoteOrderId=std::to_string(json["data"]["id"].GetInt64());
                    std::map<std::string,LFRtnOrderField>::iterator restOrderStatus=itr->second.unit.restOrderStatusMap.find(remoteOrderId);
                    if(restOrderStatus==itr->second.unit.restOrderStatusMap.end()){
                        KF_LOG_ERROR(logger,"[orderActionNoResponseTimeOut] rest receive no order id");
                        itr=remoteOrderIdOrderActionSentTime.erase(itr);
                    }
                    else{
                        errorMsg = "canceled";
                        restOrderStatus->second.OrderStatus = LF_CHAR_Canceled;
                        on_rtn_order(&(restOrderStatus->second));
                        on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
                        itr=remoteOrderIdOrderActionSentTime.erase(itr);
                    }
                }
            }
            /*on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
            itr = remoteOrderIdOrderActionSentTime.erase(itr);*/
        } else {
            ++itr;
        }
    }
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}
/*
void TDEngineHuobi::orderInsertNoResponseTimeOut(){
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut]");
    int errorId = 100;
    std::string errorMsg = "OrderInsert has none response for a long time, please send OrderAction again";

    //std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    int64_t currentNano = getTimestamp();
    int64_t timeBeforeNano = currentNano - orderinsert_max_waiting_seconds * 1000;
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (currentNano)" << currentNano << " (timeBeforeNano)" << timeBeforeNano);
    std::map<std::string, OrderInsertSentTime>::iterator itr;
    for(itr = remoteOrderIdOrderInsertSentTime.begin(); itr != remoteOrderIdOrderInsertSentTime.end();)
    {
        if(itr->second.sentNameTime < timeBeforeNano)
        {
            KF_LOG_DEBUG(logger, "[orderInsertNoResponseTimeOut] (remoteOrderIdOrderInsertSentTime.erase remoteOrderId)" << itr->first );
            
            std::string url = "/v1/order/orders/" + remoteOrderId;
            const auto response = Get(url,"",unit);
            on_rsp_order_insert(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
            itr = remoteOrderIdOrderInsertSentTime.erase(itr);
        } else {
            ++itr;
        }
    }
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}
*/
void TDEngineHuobi::printResponse(const Document& d){
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
}

void TDEngineHuobi::getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json)
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

void TDEngineHuobi::get_account(AccountUnitHuobi& unit, Document& json,bool is_margin)
{
    KF_LOG_INFO(logger, "[get_account]");
    /*
      账户余额
      查询指定账户的余额，支持以下账户：
      spot：现货账户， margin：杠杆账户，otc：OTC 账户，point：点卡账户
      HTTP 请求
      GET /v1/account/accounts/{account-id}/balance
    */
    std::string getPath="/v1/account/accounts/";
    string accountId=is_margin?unit.marginAccountId:unit.spotAccountId;
    std::string requestPath = getPath+accountId+"/balance";
    const auto response = Get(requestPath,"{}",unit,"");
    json.Parse(response.text.c_str());
    KF_LOG_INFO(logger, "[get_account] (account info) "<<response.text.c_str());
    return ;
}
void TDEngineHuobi::getAccountId(AccountUnitHuobi& unit){
    KF_LOG_DEBUG(logger,"[getAccountID] ");
    std::string getPath="/v1/account/accounts/";
    const auto resp = Get("/v1/account/accounts","{}",unit,"");
    Document j;
    j.Parse(resp.text.c_str());
    //int n=j["data"].Size();
    std::string type="spot";//现货账户
    std::string marginType="super-margin";//现货账户
    string state="working";
    std::string accountId;
    bool isSpot=false,isMyMargin=false;
    if(j.HasMember("data")&&j["data"].IsArray()){
        int n=j["data"].Size();
        for(int i=0;i<n;i++){
            if((!isSpot)&&(type==j["data"].GetArray()[i]["type"].GetString())
                &&(state==j["data"].GetArray()[i]["state"].GetString())){
                unit.spotAccountId=std::to_string(j["data"].GetArray()[i]["id"].GetInt());
                isSpot=true;
            }
            if((!isMyMargin)&&(marginType==j["data"].GetArray()[i]["type"].GetString())
                &&(state==j["data"].GetArray()[i]["state"].GetString())){
                unit.marginAccountId=std::to_string(j["data"].GetArray()[i]["id"].GetInt());
                isMyMargin=true;
            }
            if(isSpot&&isMyMargin)break;
        }
    }
    KF_LOG_DEBUG(logger,"[getAccountID] (spot-account) "<<unit.spotAccountId << ",(margin-account) "<< unit.marginAccountId);
}
std::string TDEngineHuobi::getHuobiTime(){
    time_t t = time(NULL);
    struct tm *local = gmtime(&t);
    char timeBuf[100] = {0};
    sprintf(timeBuf, "%04d-%02d-%02dT%02d%%3A%02d%%3A%02d",
            local->tm_year + 1900,
            local->tm_mon + 1,
            local->tm_mday,
            local->tm_hour,
            local->tm_min,
            local->tm_sec);
    std::string huobiTime=timeBuf;
    return huobiTime;
}
std::string TDEngineHuobi::getHuobiNormalTime(){
    time_t t = time(NULL);
    struct tm *local = gmtime(&t);
    char timeBuf[100] = {0};
    sprintf(timeBuf, "%04d-%02d-%02dT%02d:%02d:%02d",
            local->tm_year + 1900,
            local->tm_mon + 1,
            local->tm_mday,
            local->tm_hour,
            local->tm_min,
            local->tm_sec);
    std::string huobiTime=timeBuf;
    return huobiTime;
}
/*
    {
  "account-id": "100009",
  "amount": "10.1",
  "price": "100.1",
  "source": "api",
  "symbol": "ethusdt",
  "type": "buy-limit"

 * */
std::string TDEngineHuobi::createInsertOrdertring(const char *accountId,
        const char *amount, const char *price, const char *source, const char *symbol,const char *type,std::string cid){
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("account-id");
    writer.String(accountId);
    writer.Key("amount");
    writer.String(amount);
    writer.Key("price");
    writer.String(price);
    writer.Key("source");
    writer.String(source);
    writer.Key("symbol");
    writer.String(symbol);
    writer.Key("type");
    writer.String(type);
    writer.Key("client-order-id");
    writer.String(cid.c_str());
    writer.EndObject();
    return s.GetString();
}
/*火币下单请求参数
    {
        "account-id": "100009",
        "amount": "10.1",
        "price": "100.1",
        "source": "api",
        "symbol": "ethusdt",
        "type": "buy-limit"
    }
*/


//edit std::string& output_cid, const LFInputOrderField data
void TDEngineHuobi::send_order(AccountUnitHuobi& unit, const char *code, const char *side, const char *type, std::string volume,
                                std::string price, Document& json, std::string& output_cid, const LFInputOrderField data, bool isPostOnly)
{
    KF_LOG_INFO(logger, "[send_order]");
    KF_LOG_INFO(logger, "[send_order] (code) "<<code);
    std::string s=side,t=type;
    std::string st=s+"-"+t;
    if(isPostOnly){
        st += "-maker";
    }
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        should_retry = false;
        //火币下单post /v1/order/orders/place
        std::string requestPath = "/v1/order/orders/place";
        string source="api";
        string accountId=unit.spotAccountId;
        //lock
        if(isMargin){
            source="margin-api";
            accountId=unit.marginAccountId;
        }

        //***********edit begin************
        std::string cid = genClinetid(std::string(data.OrderRef));
        KF_LOG_INFO(logger, "[send_order] cid:" << cid);
        cid_map.insert(make_pair(cid, data));

        KF_LOG_INFO(logger, "[send_order] volume: " << volume << " price: " << price);
        //***********edit end**************

        KF_LOG_INFO(logger,"[send_order] (isMargin) "<<isMargin<<" (source) "<<source);
        response = Post(requestPath,createInsertOrdertring(accountId.c_str(), volume.c_str(), price.c_str(),
                        source.c_str(),code,st.c_str(),cid),unit);

        KF_LOG_INFO(logger, "[send_order] (url) " << requestPath << " (response.status_code) " << response.status_code 
                                                  << " (response.error.message) " << response.error.message 
                                                  <<" (response.text) " << response.text.c_str() << " (retry_times)" << retry_times);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);
        //has error and find the 'error setting certificate verify locations' error, should retry
        if(shouldRetry(json)) {
            //**********edit begin************
            auto tmp_itr = cid_map.find(cid);
            if (tmp_itr != cid_map.end()) 
            {
                cid_map.erase(tmp_itr);
            }
            //***********edit end*************

            should_retry = true;
            retry_times++;
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
        }

        //**********edit begin************
        output_cid = cid;
        //***********edit end*************
    } while(should_retry && retry_times < max_rest_retry_times);

    KF_LOG_INFO(logger, "[send_order] out_retry (response.status_code) " << response.status_code <<" (response.error.message) " 
                                                                        << response.error.message << " (response.text) " << response.text.c_str() );

    //getResponse(response.status_code, response.text, response.error.message, json);
}
/*火币返回数据格式
    {
  "status": "ok",
  "ch": "market.btcusdt.kline.1day",
  "ts": 1499223904680,
  "data": // per API response data in nested JSON object
    }
*/
bool TDEngineHuobi::shouldRetry(Document& doc)
{
    bool ret = false;
    std::string strCode ;
    if(doc.HasMember("status"))
    {
        strCode = doc["status"].GetString();
    }
    bool isObJect = doc.IsObject();
    if(!isObJect || strCode != "ok")
    {
        ret = true;
    }
    KF_LOG_INFO(logger, "[shouldRetry] isObJect = " << isObJect << ",strCode = " << strCode);
    return ret;
}

void TDEngineHuobi::cancel_all_orders(AccountUnitHuobi& unit, std::string code, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_all_orders]");
    std::string accountId = unit.spotAccountId;
    if(isMargin)accountId=unit.marginAccountId;
    //火币post批量撤销订单
    std::string requestPath = "/v1/order/orders/batchCancelOpenOrders";
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("account-id");
    writer.String(accountId.c_str());
    /*writer.Key("symbol");
    writer.String("ethbtc");
    writer.Key("side");
    writer.String("buy");*/
    writer.Key("size");
    writer.Int(100);
    //write Signature
    writer.EndObject();
    auto response = Post(requestPath,s.GetString(),unit);
    getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineHuobi::cancel_order(AccountUnitHuobi& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_order]");

    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        should_retry = false;
        //火币post撤单请求
        std::string postPath="/v1/order/orders/";
        std::string requestPath = postPath+ orderId + "/submitcancel";
        response = Post(requestPath,"",unit);

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
int TDEngineHuobi::orderIsTraded(AccountUnitHuobi& unit, std::string code, std::string orderId, Document& json){
    KF_LOG_INFO(logger,"[orderIsCanceled]");
    std::map<std::string,LFRtnOrderField>::iterator itr = unit.restOrderStatusMap.find(orderId);
    if(itr == unit.restOrderStatusMap.end()){
        KF_LOG_INFO(logger,"[orderIsCanceled] order id not exits in restOrderStatusMap!");
        return -1;
    }
    query_order(unit,code,orderId,json);
    if(json.HasParseError()||!json.IsObject()){
        KF_LOG_INFO(logger,"[orderIsCanceled] query order status is faild!");
        return -1;
    }
    LfOrderStatusType orderStatus;
    if(json.HasMember("status") && "ok" ==  json["status"].GetString()){
        KF_LOG_INFO(logger, "[orderIsCanceled] (query success)");
        handleResponseOrderStatus(unit, itr->second, json);
        orderStatus=GetOrderStatus(json["data"]["order-state"].GetString());
        if(orderStatus == LF_CHAR_AllTraded  || orderStatus == LF_CHAR_Canceled
            || orderStatus == LF_CHAR_Error){
            KF_LOG_INFO(logger, "[orderIsCanceled] remove a restOrderStatusMap.");
            std::string ref = itr->second.OrderRef;
            unit.restOrderStatusMap.erase(orderId);
            
            auto it = localOrderRefRemoteOrderId.find(ref);
            if(it != localOrderRefRemoteOrderId.end()){
                localOrderRefRemoteOrderId.erase(it);
                KF_LOG_INFO(logger,"erase local");
            }
            auto it1 = mapInsertOrders.find(ref);
            if(it1 != mapInsertOrders.end()){
                mapInsertOrders.erase(it1);
            }

        }
    }
    if(orderStatus == LF_CHAR_AllTraded || orderStatus == LF_CHAR_Canceled)return 1;
}
void TDEngineHuobi::query_order(AccountUnitHuobi& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[query_order]");
    //火币get查询订单详情
    std::string getPath = "/v1/order/orders/";
    std::string requestPath = getPath + orderId;
    auto response = Get(requestPath,"",unit,"");
    json.Parse(response.text.c_str());
    KF_LOG_DEBUG(logger,"[query_order] response "<<response.text.c_str());
    //getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineHuobi::handleResponseOrderStatus(AccountUnitHuobi& unit, LFRtnOrderField& rtn_order, Document& json)
{
    KF_LOG_INFO(logger, "[handleResponseOrderStatus]");
    
    //有没有数据
    if (!json.HasMember("data")) {
        KF_LOG_ERROR(logger, "[handleResponseOrderStatus] no data segment");
        return;
    }
    auto& data = json["data"];
    //没有其中之一就返回

    string eventType;
    if(data.HasMember("eventType"))
        eventType = data["eventType"].GetString();
    else {
        KF_LOG_ERROR(logger, "[handleResponseOrderStatus] no eventType");
        return;
    }

    KF_LOG_INFO(logger, "[handleResponseOrderStatus] eventType:" << eventType);
    if( eventType == "deletion" || eventType == "trigger")
    {
        KF_LOG_INFO(logger, "[handleResponseOrderStatus] return");
        return;
    }

    //role taker|maker 
    //type buy-market, sell-market, buy-limit, sell-limit, buy-limit-maker, sell-limit-maker, buy-ioc, sell-ioc, buy-limit-fok, sell-limit-fok
    string type = data["type"].GetString();
    string role;
    if (type.find("buy"))
        role = "taker";
    else if (type.find("sell"))
        role = "maker";

    //总量
    int64_t nVolume = rtn_order.VolumeTotalOriginal;
    //报单状态  部分成交2
    LfOrderStatusType orderStatus = GetOrderStatus(data["orderStatus"].GetString());
    std::string remoteOrderId = std::to_string(data["orderId"].GetInt64());

    //单次成交价格和数量
    double dDealSize, dDealPrice;
    //内部格式的单次价格和数量
    int64_t nDealSize, nDealPrice;

    if (eventType == "creation")
    {
        dDealSize = 0;
        dDealPrice = 0;
        nDealSize = 0;
        nDealPrice = 0;
    }
    if (eventType == "trade")
    {
        dDealSize = std::stod(data["tradeVolume"].GetString());
        dDealPrice = std::stod(data["tradePrice"].GetString());
        nDealSize = std::round(dDealSize * scale_offset);
        nDealPrice = std::round(dDealPrice * scale_offset);
    }
    if (eventType == "cancellation")
    {
        dDealSize = 0;
        dDealPrice = 0;
        nDealSize = 0;
        nDealPrice = 0;
    }



    bool update = true;
    std::vector<UpdateMsg>::iterator itr;
    std::unique_lock<std::mutex> lck1(update_mutex);
    //总之遍历Updatemsg_vec
    for (itr = Updatemsg_vec.begin(); itr != Updatemsg_vec.end(); itr++) {
        if (itr->orderId == remoteOrderId && itr->fillamount == dDealSize && itr->status == orderStatus) {
            KF_LOG_INFO(logger, "old update1");
            update = false;
            break;
        }
    }

    lck1.unlock();
    if (update) {
        UpdateMsg updatemsg;
        updatemsg.orderId = remoteOrderId;
        updatemsg.fillamount = dDealSize;
        updatemsg.status = orderStatus;
        lck1.lock();
        Updatemsg_vec.push_back(updatemsg);
        lck1.unlock();

        int64_t volumeTraded = rtn_order.VolumeTraded + nDealSize;
        //订单状态 成交数量 无变化 则返回
        if (orderStatus == rtn_order.OrderStatus && volumeTraded == rtn_order.VolumeTraded) {//no change
            KF_LOG_INFO(logger, "[handleResponseOrderStatus] status is not changed");
            return;
        }
        rtn_order.OrderStatus = orderStatus;
        KF_LOG_INFO(logger, "[handleResponseOrderStatus] (orderStatus) " << rtn_order.OrderStatus);
        uint64_t oldVolumeTraded = rtn_order.VolumeTraded;
        //累计成交数量
        rtn_order.VolumeTraded = volumeTraded;
        //剩余数量
        rtn_order.VolumeTotal = nVolume - rtn_order.VolumeTraded;
        //订单状态 成交数量 有变化 调用on_rtn_order
        on_rtn_order(&rtn_order);
        raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField), source_id, MSG_TYPE_LF_RTN_ORDER_HUOBI,
            1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);

        if (oldVolumeTraded != rtn_order.VolumeTraded) {
            //send OnRtnTrade
            LFRtnTradeField rtn_trade;
            memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
            strcpy(rtn_trade.ExchangeID, "huobi");
            strncpy(rtn_trade.UserID, unit.api_key.c_str(), 16);
            strncpy(rtn_trade.InstrumentID, rtn_order.InstrumentID, 31);
            strncpy(rtn_trade.OrderRef, rtn_order.OrderRef, 13);
            rtn_trade.Direction = rtn_order.Direction;
            //单次成交数量
            rtn_trade.Volume = nDealSize;
            rtn_trade.Price = nDealPrice;//(newAmount - oldAmount)/(rtn_trade.Volume);
            strncpy(rtn_trade.OrderSysID, rtn_order.BusinessUnit, 31);
            //成交数量变化 调用on_rtn_trade
            on_rtn_trade(&rtn_trade);

            raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
                source_id, MSG_TYPE_LF_RTN_TRADE_HUOBI, 1, -1);

            KF_LOG_INFO(logger, "[on_rtn_trade 1] (InstrumentID)" << rtn_trade.InstrumentID << "(Direction)" << rtn_trade.Direction
                << "(Volume)" << rtn_trade.Volume << "(Price)" << rtn_trade.Price);
        }
    }
}
/*
void TDEngineHuobi::handleResponseOrderStatus(AccountUnitHuobi& unit, LFRtnOrderField& rtn_order, Document& json)
{
    KF_LOG_INFO(logger, "[handleResponseOrderStatus]");
    if(!json.HasMember("data")){
        KF_LOG_ERROR(logger,"[handleResponseOrderStatus] no data segment");
        return;
    }
    auto& data=json["data"];
    if(!data.HasMember("filled-cash-amount")||!data.HasMember("filled-amount")||!data.HasMember("unfilled-amount")
        ||!data.HasMember("order-state")||!data.HasMember("order-state")){
        KF_LOG_ERROR(logger,"[handleResponseOrderStatus] no child segment");
        return;
    }
    string role = data["role"].GetString();
    //单次成交总金额
    double dDealFunds = std::stod(data["filled-cash-amount"].GetString());
    //单次成交数量
    double dDealSize = std::stod(data["filled-amount"].GetString());
    //单次成交数量
    int64_t nDealSize = std::round(dDealSize * scale_offset);
    //单次成交价格
    double dDealPrice = std::stod(data["price"].GetString());
    //单次成交数量
    int64_t nDealPrice = std::round(dDealPrice * scale_offset);
    //int64_t averagePrice = dDealSize > 0 ? std::round(dDealFunds / dDealSize * scale_offset): 0;
    //单次未成交数量
    //int64_t nUnfilledAmount = std::round(std::stod(data["unfilled-amount"].GetString()) * scale_offset);
    //总量
    int64_t nVolume = rtn_order.VolumeTotalOriginal;
    //报单状态  部分成交2
    LfOrderStatusType orderStatus=GetOrderStatus(data["order-state"].GetString());

    std::string remoteOrderId = std::to_string(data["order-id"].GetInt64());
    
    bool update = true;
    std::vector<UpdateMsg>::iterator itr;
    std::unique_lock<std::mutex> lck1(update_mutex);
    for(itr = Updatemsg_vec.begin(); itr != Updatemsg_vec.end(); itr++){
        if(itr->orderId == remoteOrderId && itr->fillamount == dDealSize && itr->status == orderStatus){
            KF_LOG_INFO(logger,"old update1");
            update = false;
            break;
        }
    }
    lck1.unlock();
    //if(role == "taker" && (orderStatus == LF_CHAR_AllTraded || orderStatus == LF_CHAR_PartTradedQueueing)){
        //KF_LOG_INFO(logger, "[handleResponseOrderStatus] role is taker");
        //return;
    //}
    if(update){
        UpdateMsg updatemsg;
        updatemsg.orderId = remoteOrderId;
        updatemsg.fillamount = dDealSize;
        updatemsg.status = orderStatus;
        lck1.lock();
        Updatemsg_vec.push_back(updatemsg);
        lck1.unlock();

        int64_t volumeTraded = rtn_order.VolumeTraded+nDealSize;//nVolume-nUnfilledAmount;
        //订单状态 成交数量 无变化 则返回
        if(orderStatus == rtn_order.OrderStatus && volumeTraded == rtn_order.VolumeTraded){//no change
            KF_LOG_INFO(logger, "[handleResponseOrderStatus] status is not changed");
            return;
        }
        rtn_order.OrderStatus = orderStatus;
        KF_LOG_INFO(logger, "[handleResponseOrderStatus] (orderStatus) "<<rtn_order.OrderStatus);
        uint64_t oldVolumeTraded = rtn_order.VolumeTraded;
        //累计成交数量
        rtn_order.VolumeTraded = volumeTraded;
        //剩余数量
        rtn_order.VolumeTotal = nVolume - rtn_order.VolumeTraded;
        //订单状态 成交数量 有变化 调用on_rtn_order
        on_rtn_order(&rtn_order);
        raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_ORDER_HUOBI,
            1, (rtn_order.RequestID > 0) ? rtn_order.RequestID: -1);

        if(oldVolumeTraded != rtn_order.VolumeTraded){
            //send OnRtnTrade
            LFRtnTradeField rtn_trade;
            memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
            strcpy(rtn_trade.ExchangeID, "huobi");
            strncpy(rtn_trade.UserID, unit.api_key.c_str(), 16);
            strncpy(rtn_trade.InstrumentID, rtn_order.InstrumentID, 31);
            strncpy(rtn_trade.OrderRef, rtn_order.OrderRef, 13);
            rtn_trade.Direction = rtn_order.Direction;
            //单次成交数量
            rtn_trade.Volume = nDealSize;
            rtn_trade.Price =nDealPrice;//(newAmount - oldAmount)/(rtn_trade.Volume);
            strncpy(rtn_trade.OrderSysID,rtn_order.BusinessUnit,31);
            //成交数量变化 调用on_rtn_trade
            on_rtn_trade(&rtn_trade);

            raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
                source_id, MSG_TYPE_LF_RTN_TRADE_HUOBI, 1, -1);

            KF_LOG_INFO(logger, "[on_rtn_trade 1] (InstrumentID)" << rtn_trade.InstrumentID << "(Direction)" << rtn_trade.Direction
                    << "(Volume)" << rtn_trade.Volume << "(Price)" <<  rtn_trade.Price);  
        }
    }
}
*/
std::string TDEngineHuobi::parseJsonToString(Document &d){
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


inline int64_t TDEngineHuobi::getTimestamp(){
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void TDEngineHuobi::genUniqueKey(){
    struct tm cur_time = getCurLocalTime();
    //SSMMHHDDN
    char key[11]{0};
    snprintf((char*)key, 11, "%02d%02d%02d%02d%1s", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_engineIndex.c_str());
    m_uniqueKey = key;
}
//clientid =  m_uniqueKey+orderRef
std::string TDEngineHuobi::genClinetid(const std::string &orderRef){
    static int nIndex = 0;
    return "BVID"+m_uniqueKey + orderRef + std::to_string(nIndex++);
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))
BOOST_PYTHON_MODULE(libhuobitd){
    using namespace boost::python;
    class_<TDEngineHuobi, boost::shared_ptr<TDEngineHuobi> >("Engine")
     .def(init<>())
        .def("init", &TDEngineHuobi::initialize)
        .def("start", &TDEngineHuobi::start)
        .def("stop", &TDEngineHuobi::stop)
        .def("logout", &TDEngineHuobi::logout)
        .def("wait_for_stop", &TDEngineHuobi::wait_for_stop);
}

