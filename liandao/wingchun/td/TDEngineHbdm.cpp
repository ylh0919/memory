#include "TDEngineHbdm.h"
#include "longfist/ctp.h"
#include "longfist/LFUtils.h"
#include "TypeConvert.hpp"
#include "../../utils/common/ld_utils.h"
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

TDEngineHbdm::TDEngineHbdm(): ITDEngine(SOURCE_HBDM)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.Hbdm");
    KF_LOG_INFO(logger, "[TDEngineHbdm]");

    mutex_order_and_trade = new std::mutex();
    mutex_response_order_status = new std::mutex();
    mutex_orderaction_waiting_response = new std::mutex();
}

TDEngineHbdm::~TDEngineHbdm()
{
    if(mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if(mutex_response_order_status != nullptr) delete mutex_response_order_status;
    if(mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
    KF_LOG_INFO(logger, "TDEngineHbdm deconstruct");
}
// gzCompress: do the compressing
int TDEngineHbdm::gzCompress(const char *src, int srcLen, char *dest, int destLen){
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
int TDEngineHbdm::gzDecompress(const char *src, int srcLen, const char *dst, int dstLen){
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
static TDEngineHbdm* global_md = nullptr;
//web socket代码
/*static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
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
            //global_md->writeInfoLog(ss.str());
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
            //global_md->writeInfoLog(ss.str());
            if(global_md)
            {
                global_md->on_lws_connection_error(wsi);
            }
            break;
        }
        default:
            ss << "default Info.";
            //global_md->writeInfoLog(ss.str());
            break;
    }

    return 0;
}*/
/*void TDEngineHbdm::on_lws_open(struct lws* wsi){
    KF_LOG_INFO(logger,"[on_lws_open] ");
    hbdmAuth(findAccountUnitHbdmByWebsocketConn(wsi));
    KF_LOG_INFO(logger,"[on_lws_open] finished ");
}*/

void TDEngineHbdm::OnConnected(const common::CWebsocket* instance)
{
    KF_LOG_INFO(logger,"OnConnected");

    hbdmAuth(findAccountUnitHbdmByWebsocketConn(instance->m_connection), instance);
    KF_LOG_INFO(logger,"[OnConnected] finished ");
}

void TDEngineHbdm::OnReceivedMessage(const common::CWebsocket* instance,const std::string& msg)
{
    KF_LOG_INFO(logger,"OnReceivedMessage");
    auto dataJson = ldutils::gzip_decompress(msg);
    KF_LOG_INFO(logger,"on_lws_data:"<<dataJson);
    /*char buf[4096] = {0};
    int l = 4096;
    const char* data = msg.data();
    l = gzDecompress(data, strlen(data), buf, l);
    KF_LOG_INFO(logger, "[on_lws_data] (cys_buf) " << buf);
    KF_LOG_INFO(logger, "[on_lws_data] (data) " << data);*/
    //std::string strData = dealDataSprit(data);
    Document json;
    json.Parse(dataJson.c_str());
    if(json.HasParseError()||!json.IsObject()){
        KF_LOG_ERROR(logger, "[cys_on_lws_data] parse to json error ");
        return;
    }
    if(json.HasMember("op")||json.HasMember("ping"))
    {
        /*if ((json.HasMember("status") && json["status"].GetString()!="ok")||      
              (json.HasMember("err-code")&&json["err-code"].GetInt()!=0) ) {
            int errorCode = json["err-code"].GetInt();
            std::string errorMsg = json["err-msg"].GetString();
            KF_LOG_ERROR(logger, "[on_lws_data] (err-code) "<<errorCode<<" (errMsg) " << errorMsg);
        } else*/ if (json.HasMember("op")) {
            std::string op = json["op"].GetString();
            if (op == "notify") {
                string topic=json["topic"].GetString();
                if(topic.substr(0,topic.find("."))=="orders"){
                    //on_lws_receive_orders(conn,json);
                    onOrderChange(json);
                }
            } else if (op == "ping") {
                std::string pingstr=json["ts"].GetString();
                long long ping = stoll(pingstr);
                Pong(instance,ping);
            } else if (op == "auth") {
                isAuth=hbdm_auth;
                std::string userId=json["data"]["user-id"].GetString();
                KF_LOG_INFO(logger,"[on_lws_data] cys_hbdmAuth success. authed user-id "<<userId);
                subscribeTopic(instance);
            }
        } else if (json.HasMember("ch")) {

        } else if (json.HasMember("ping")) {
            long long ping=json["ts"].GetInt64();
            Pong(instance,ping);
        } else if (json.HasMember("subbed")) {

        }
    } else
    {
        KF_LOG_ERROR(logger, "[on_lws_data] . parse json error(data): " << dataJson);
    }
}

void TDEngineHbdm::OnDisconnected(const common::CWebsocket* instance)
{
    KF_LOG_ERROR(logger, "TDEngineHbdm::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    m_isPong = false;
    //m_shouldPing = true;
    isAuth = nothing;isOrders=nothing;
    //no use it
    long timeout_nsec = 0;
    //reset sub
    //m_isSubL3 = false;
    AccountUnitHbdm& unit=findAccountUnitHbdmByWebsocketConn(instance->m_connection);
    unit.is_connecting = false;
    for(auto it = unit.ws_msg_vec.begin(); it != unit.ws_msg_vec.end(); it++){
        if(it->websocket.m_connection == instance->m_connection)
        {
            it->is_ws_disconnectd = true;
        }
    }
    //lws_login(unit,0);
    KF_LOG_INFO(logger,"login again and cancel_all_orders");
    Document d;
    cancel_all_orders(unit, "", d);
    std::string status = d["status"].GetString();
    if(status == "ok"){
        std::string successes = d["data"]["successes"].GetString();
        successes = successes+",";
        int n = count(successes.begin(),successes.end(),',');
        for(int i=0;i<n;i++){
            int flag = successes.find(",");
            std::string orderId = successes.substr(0,flag);
            successes = successes.substr(flag+1,successes.length());
            KF_LOG_INFO(logger,"orderId="<<orderId);
            DealCancel(orderId);
        }
    }
}

//cys websocket connect
void TDEngineHbdm::Ping(struct lws* conn)
{
    //m_shouldPing = false;
    StringBuffer sbPing;
    Writer<StringBuffer> writer(sbPing);
    writer.StartObject();
    writer.Key("type");
    writer.String("ping");
    writer.EndObject();
    std::string strPing = sbPing.GetString();
    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512-LWS_PRE);
    int length = strPing.length();
    KF_LOG_INFO(logger, "TDEngineHbdm::lws_write_ping: " << strPing.c_str() << " ,len = " << length);
    strncpy((char *)msg+LWS_PRE, strPing.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
}
void TDEngineHbdm::Pong(const common::CWebsocket* instance,long long ping){
    KF_LOG_INFO(logger,"[Pong] pong the ping of websocket");
    StringBuffer sbPing;
    Writer<StringBuffer> writer(sbPing);
    writer.StartObject();
    writer.Key("op");
    writer.String("pong");
    writer.Key("ts");
    writer.Int64(ping);
    writer.EndObject();
    std::string strPong = sbPing.GetString();
    AccountUnitHbdm& unit=findAccountUnitHbdmByWebsocketConn(instance->m_connection);
    for(auto it = unit.ws_msg_vec.begin(); it != unit.ws_msg_vec.end(); it++){
        if(it->websocket.m_connection == instance->m_connection)
        {
            KF_LOG_INFO(logger,"send_pong");

            it->websocket.SendMessage(strPong);
        }
    }
}

void TDEngineHbdm::DealCancel(std::string order_id_str)
{
    KF_LOG_INFO(logger,"TDEngineHbdm::DealCancel");
    auto it = m_mapOrder.find(order_id_str);
    if(it != m_mapOrder.end()){
        it->second.OrderStatus = LF_CHAR_Canceled;
        on_rtn_order(&(it->second));
        auto it_id = localOrderRefRemoteOrderId.find(it->second.OrderRef);
        if(it_id != localOrderRefRemoteOrderId.end())
        {
            KF_LOG_INFO(logger,"earse-local");
            localOrderRefRemoteOrderId.erase(it_id);
        }
        m_mapOrder.erase(it);         
    }
    auto it1 = remoteOrderIdOrderActionSentTime.find(order_id_str);
    if(it1 != remoteOrderIdOrderActionSentTime.end()){
        remoteOrderIdOrderActionSentTime.erase(it1);
    }
}

void TDEngineHbdm::DealTrade(std::string order_id_str,int64_t price,uint64_t volume)
{
    auto it = m_mapOrder.find(order_id_str);
    if(it != m_mapOrder.end())
    {
        KF_LOG_DEBUG(logger, "straction=filled in");
        it->second.VolumeTraded += volume;
        it->second.VolumeTotal = it->second.VolumeTotalOriginal - it->second.VolumeTraded;
        if(it->second.VolumeTraded==it->second.VolumeTotalOriginal){
            it->second.OrderStatus = LF_CHAR_AllTraded;
        }
        else{
            it->second.OrderStatus = LF_CHAR_PartTradedQueueing;
        }
        on_rtn_order(&(it->second));
        raw_writer->write_frame(&(it->second), sizeof(LFRtnOrderField),
                                source_id, MSG_TYPE_LF_RTN_ORDER_HBDM, 1, -1);

        LFRtnTradeField rtn_trade;
        memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
        strcpy(rtn_trade.ExchangeID,it->second.ExchangeID);
        strncpy(rtn_trade.UserID, it->second.UserID,sizeof(rtn_trade.UserID));
        strncpy(rtn_trade.InstrumentID, it->second.InstrumentID, sizeof(rtn_trade.InstrumentID));
        strncpy(rtn_trade.OrderRef, it->second.OrderRef, sizeof(rtn_trade.OrderRef));
        rtn_trade.Direction = it->second.Direction;
        strncpy(rtn_trade.OrderSysID,order_id_str.c_str(),sizeof(rtn_trade.OrderSysID));
        rtn_trade.Volume = volume;
        rtn_trade.Price = price;
        on_rtn_trade(&rtn_trade);
        raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
                                source_id, MSG_TYPE_LF_RTN_TRADE_HBDM, 1, -1);

        if(it->second.OrderStatus == LF_CHAR_AllTraded)
        {
            auto it_id = localOrderRefRemoteOrderId.find(it->second.OrderRef);
            if(it_id != localOrderRefRemoteOrderId.end())
            {
                KF_LOG_INFO(logger,"earse local");
                localOrderRefRemoteOrderId.erase(it_id);
            }
            m_mapOrder.erase(it);

            auto it1 = remoteOrderIdOrderActionSentTime.find(order_id_str);
            if(it1 != remoteOrderIdOrderActionSentTime.end()){
                remoteOrderIdOrderActionSentTime.erase(it1); 
            }        
        }
    }
}

void TDEngineHbdm::onOrderChange(Document& json)
{
    KF_LOG_INFO(logger,"TDEngineHbdm::onOrderChange");
    int status = json["status"].GetInt();
    std::string order_id_str = json["order_id_str"].GetString();
    int size = json["trade"].Size();
    if(status == 5 || status == 7){
        DealCancel(order_id_str);
    }else if(size>0){
        for(int i=0;i<size;i++){
            double trade_price = json["trade"].GetArray()[i]["trade_price"].GetDouble();
            int64_t trade_volume = json["trade"].GetArray()[i]["trade_volume"].GetInt64();
            trade_price = std::round(trade_price * scale_offset);
            int64_t price = trade_price;
            trade_volume = std::round(trade_volume * scale_offset);
            uint64_t volume = trade_volume;
            DealTrade(order_id_str,price,volume);
        }
    }
}

std::string TDEngineHbdm::makeSubscribeOrdersUpdate(AccountUnitHbdm& unit, string ticker){
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("op");
    writer.String("sub");
    //writer.Key("cid");
    //writer.String(unit.spotAccountId.c_str());
    writer.Key("topic");
    string topic = "orders.";
    topic = topic + ticker;
    writer.String(topic.c_str());
    writer.EndObject();
    std::string strUpdate = sbUpdate.GetString();
    return strUpdate;
}
AccountUnitHbdm& TDEngineHbdm::findAccountUnitHbdmByWebsocketConn(struct lws * websocketConn){
    for (size_t idx = 0; idx < account_units.size(); idx++) {
        AccountUnitHbdm &unit = account_units[idx];
        for(auto it = unit.ws_msg_vec.begin(); it != unit.ws_msg_vec.end(); it++){
            if(it->websocket.m_connection == websocketConn) {
                return unit;
            }
        }
    }
    return account_units[0];
}
void TDEngineHbdm::subscribeTopic(const common::CWebsocket* instance)
{
    AccountUnitHbdm& unit=findAccountUnitHbdmByWebsocketConn(instance->m_connection);
    if(isAuth==hbdm_auth&&isOrders != orders_sub){
        isOrders=orders_sub;

        string strSubscribe = makeSubscribeOrdersUpdate(unit,"*");
        for(auto it = unit.ws_msg_vec.begin(); it != unit.ws_msg_vec.end(); it++){
            KF_LOG_INFO(logger,"subscribeTopic");
            it->websocket.SendMessage(strSubscribe);
        }
    }    
}
/*int TDEngineHbdm::subscribeTopic(struct lws* conn,string strSubscribe){
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
int TDEngineHbdm::on_lws_write_subscribe(struct lws* conn){
    //KF_LOG_INFO(logger, "[on_lws_write_subscribe]" );
    int ret = 0;
    AccountUnitHbdm& unit=findAccountUnitHbdmByWebsocketConn(conn);
    if(isAuth==hbdm_auth&&isOrders != orders_sub){
        isOrders=orders_sub;

        string strSubscribe = makeSubscribeOrdersUpdate(unit,"*");
        ret = subscribeTopic(conn,strSubscribe);
    }
    return ret;
}*/
/*
void TDEngineHbdm::on_lws_connection_error(struct lws* conn){
    KF_LOG_ERROR(logger, "TDEngineHbdm::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    m_isPong = false;
    //m_shouldPing = true;
    isAuth = nothing;isOrders=nothing;
    //no use it
    long timeout_nsec = 0;
    //reset sub
    //m_isSubL3 = false;
    AccountUnitHbdm& unit=findAccountUnitHbdmByWebsocketConn(conn);
    unit.is_connecting = false;
    lws_login(unit,0);
    KF_LOG_INFO(logger,"login again and cancel_all_orders");
    Document d;
    cancel_all_orders(unit, "", d);
    std::string status = d["status"].GetString();
    if(status == "ok"){
        std::string successes = d["data"]["successes"].GetString();
        successes = successes+",";
        int n = count(successes.begin(),successes.end(),',');
        for(int i=0;i<n;i++){
            int flag = successes.find(",");
            std::string orderId = successes.substr(0,flag);
            successes = successes.substr(flag+1,successes.length());
            KF_LOG_INFO(logger,"orderId="<<orderId);
            DealCancel(orderId);
        }
    }
}*/

struct session_data {
    int fd;
};
void TDEngineHbdm::writeInfoLog(std::string strInfo){
    KF_LOG_INFO(logger,strInfo);
}
void TDEngineHbdm::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}

int64_t TDEngineHbdm::getMSTime(){
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  timestamp;
}

int64_t TDEngineHbdm::GetMillsecondByInterval(const std::string& interval)
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

std::string TDEngineHbdm::getHbdmSignatrue(std::string parameters[],int psize,std::string timestamp,std::string method_url,
                                                std::string reqType,AccountUnitHbdm& unit){
    std::string strAccessKeyId=unit.api_key;
    std::string strSignatureMethod="HmacSHA256";
    std::string strSignatureVersion="2";
    std::string strSign = reqType+"api.hbdm.com\n" + method_url+"\n"+
                            "AccessKeyId="+strAccessKeyId+"&"+
                            "SignatureMethod="+strSignatureMethod+"&"+
                            "SignatureVersion="+strSignatureVersion+"&"+
                            "Timestamp="+timestamp;
    KF_LOG_INFO(logger, "[getHbdmSignatrue] strSign = " << strSign );
    unsigned char* strHmac = hmac_sha256_byte(unit.secret_key.c_str(),strSign.c_str());
    KF_LOG_INFO(logger, "[getHbdmSignatrue] strHmac = " << strHmac );
    std::string strSignatrue = escapeURL(base64_encode(strHmac,32));
    KF_LOG_INFO(logger, "[getHbdmSignatrue] Signatrue = " << strSignatrue );
    return strSignatrue;
}
char TDEngineHbdm::dec2hexChar(short int n) {
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
std::string TDEngineHbdm::escapeURL(const string &URL){
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
//cys edit from hbdm api
std::mutex g_httpMutex;
cpr::Response TDEngineHbdm::Get(const std::string& method_url,const std::string& body, AccountUnitHbdm& unit)
{
    std::string strTimestamp = getHbdmTime();
    string strSignatrue=getHbdmSignatrue(NULL,0,strTimestamp,method_url,"GET\n",unit);
    string url = unit.baseUrl + method_url+"?"+"AccessKeyId="+unit.api_key+"&"+
                    "SignatureMethod=HmacSHA256&"+
                    "SignatureVersion=2&"+
                    "Timestamp="+strTimestamp+"&"+
                    "Signature="+strSignatrue;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    const auto response = cpr::Get(Url{url},
                                   Header{{"Content-Type", "application/json"}}, Timeout{10000} );
    lock.unlock();
    if(response.text.length()<500){
        KF_LOG_INFO(logger, "[Get] (url) " << url << " (response.status_code) " << response.status_code <<
                                       " (response.error.message) " << response.error.message <<
                                       " (response.text) " << response.text.c_str());
    }
    return response;
}
//cys edit
cpr::Response TDEngineHbdm::Post(const std::string& method_url,const std::string& body, AccountUnitHbdm& unit)
{
    std::string strTimestamp = getHbdmTime();
    string strSignatrue=getHbdmSignatrue(NULL,0,strTimestamp,method_url,"POST\n",unit);
    string url = unit.baseUrl + method_url+"?"+"AccessKeyId="+unit.api_key+"&"+
                    "SignatureMethod=HmacSHA256&"+
                    "SignatureVersion=2&"+
                    "Timestamp="+strTimestamp+"&"+
                    "Signature="+strSignatrue;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    auto response = cpr::Post(Url{url}, Header{{"Content-Type", "application/json"}},
                              Body{body},Timeout{10000});
    lock.unlock();
    //if(response.text.length()<500){
        KF_LOG_INFO(logger, "[POST] (url) " << url <<"(body) "<< body<< " (response.status_code) " << response.status_code <<
                                        " (response.error.message) " << response.error.message <<
                                        " (response.text) " << response.text.c_str());
    //}
    return response;
}
void TDEngineHbdm::init()
{
    //genUniqueKey();
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineHbdm::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineHbdm::resize_accounts(int account_num)
{
    account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineHbdm::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    string api_key = j_config["APIKey"].get<string>();
    string secret_key = j_config["SecretKey"].get<string>();
    string passphrase = j_config["passphrase"].get<string>();
    if(j_config.find("is_margin") != j_config.end()) {
        isMargin = j_config["is_margin"].get<bool>();
    }
    //https://api.hbdm.pro
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

    if(j_config.find("lever_rate") != j_config.end()) {
        lever_rate = j_config["lever_rate"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (lever_rate)" << lever_rate);

    if(j_config.find("retry_interval_milliseconds") != j_config.end()) {
        retry_interval_milliseconds = j_config["retry_interval_milliseconds"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (retry_interval_milliseconds)" << retry_interval_milliseconds);
    genUniqueKey();

    AccountUnitHbdm& unit = account_units[idx];
    unit.api_key = api_key;
    unit.secret_key = secret_key;
    unit.passphrase = passphrase;
    unit.baseUrl = baseUrl;

    WsMsg wsmsg;
    wsmsg.is_swap = true;
    wsmsg.ws_url = "api.hbdm.com/swap-notification";
    unit.ws_msg_vec.push_back(wsmsg);
    wsmsg.is_swap = false;
    wsmsg.ws_url = "api.hbdm.com/notification";
    unit.ws_msg_vec.push_back(wsmsg);

    KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl 
                                                   << " (spotAccountId) "<<unit.spotAccountId
                                                   << " (marginAccountId) "<<unit.marginAccountId);

    //test rs256
    //  std::string data ="{}";
    //  std::string signature =utils::crypto::rsa256_private_sign(data, g_private_key);
    // std::string sign = base64_encode((unsigned char*)signature.c_str(), signature.size());
    //std::cout  << "[TDEngineHbdm] (test rs256-base64-sign)" << sign << std::endl;

    //std::string decodeStr = utils::crypto::rsa256_pub_verify(data,signature, g_public_key);
    //std::cout  << "[TDEngineHbdm] (test rs256-verify)" << (decodeStr.empty()?"yes":"no") << std::endl;

    unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    unit.coinPairWhiteList.Debug_print();

    unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
    unit.positionWhiteList.Debug_print();

    //display usage:
    if(unit.coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "TDEngineHbdm::load_account: please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }
    //getAccountId(unit);
    //test
    Document json;
    //get_account(unit, json);
    //printResponse(json);
    //cancel_order(unit,"code","1",json);
    //cancel_all_orders(unit, "", json);
    //printResponse(json);
    getPriceVolumePrecision(unit);
    for(auto it = precision_map.begin(); it != precision_map.end(); it++){
        cancel_all_orders(unit, it->first, json);
    }
    // set up
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, api_key.c_str(), 16);
    strncpy(account.Password, secret_key.c_str(), 21);
    //web socket登陆
    login(0);
    return account;
}

void TDEngineHbdm::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitHbdm& unit = account_units[idx];
        //unit.logged_in = true;
        KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
        if (!unit.logged_in)
        {
            KF_LOG_INFO(logger, "[connect] (account id) "<<unit.spotAccountId<<" login.");
            //lws_login(unit, 0);
            global_md = this;
            m_isSubL3 = false;
            isAuth=nothing;
            isOrders=nothing;
            for(auto it = unit.ws_msg_vec.begin(); it != unit.ws_msg_vec.end(); it++){
                KF_LOG_INFO(logger,"it->ws_url:"<<it->ws_url);
                it->websocket.RegisterCallBack(this);
                it->logged_in = it->websocket.Connect(it->ws_url);
                KF_LOG_INFO(logger, "TDEngineHbdm::login " << (it->logged_in ? "Success" : "Failed"));
            }
            //set true to for let the kungfuctl think td is running.
            unit.logged_in = true;
            unit.is_connecting = true;
        }
    }
}

void TDEngineHbdm::getPriceVolumePrecision(AccountUnitHbdm& unit){
    KF_LOG_INFO(logger,"[getPriceVolumePrecision]");
    std::string url = unit.baseUrl + "/api/v1/contract_contract_info";
    const auto response = cpr::Get(Url{url}, Timeout{10000} );
        KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
                                               " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
    Document json;
    json.Parse(response.text.c_str());
    Value &node = json["data"];
    int size = node.Size();
    for(int i=0;i<size;i++){
        std::string contract_code = node.GetArray()[i]["contract_code"].GetString();
        double price_tick = std::round(node.GetArray()[i]["price_tick"].GetDouble()*scale_offset);
        precision_map.insert(std::make_pair(contract_code,price_tick));
    }

    url = unit.baseUrl + "/swap-api/v1/swap_contract_info";
    const auto response2 = cpr::Get(Url{url}, Timeout{10000} );
        KF_LOG_INFO(logger, "[get] (url) " << url << " (response2.status_code) " << response2.status_code <<
                                               " (response2.error.message) " << response2.error.message <<
                                               " (response2.text) " << response2.text.c_str());
    json.Parse(response2.text.c_str());
    Value &node2 = json["data"];
    size = node2.Size();
    for(int i=0;i<size;i++){
        std::string contract_code = node2.GetArray()[i]["contract_code"].GetString();
        double price_tick = std::round(node2.GetArray()[i]["price_tick"].GetDouble()*scale_offset);
        precision_map.insert(std::make_pair(contract_code,price_tick));
    }

    std::map<std::string, double>::iterator it;
    for(it=precision_map.begin();it!=precision_map.end();it++){
        KF_LOG_INFO(logger,"contract_code="<<it->first<<"price_tick="<<it->second);
    }
}
void TDEngineHbdm::hbdmAuth(AccountUnitHbdm& unit,const common::CWebsocket* instance){
    KF_LOG_INFO(logger, "[hbdmAuth] auth");
    bool is_swap;
    for(auto it = unit.ws_msg_vec.begin(); it != unit.ws_msg_vec.end(); it++){
        if(it->websocket.m_connection == instance->m_connection){
            is_swap = it->is_swap;
        }
    }

    std::string strTimestamp = getHbdmTime();
    std::string timestamp = getHbdmNormalTime();
    std::string strAccessKeyId=unit.api_key;
    std::string strSignatureMethod="HmacSHA256";
    std::string strSignatureVersion="2";
    string reqType="GET\n";
    std::string notification = "/notification\n";
    if(is_swap){
        notification = "/swap-notification\n";
    }
    std::string strSign = reqType+"api.hbdm.com\n" + notification+
                            "AccessKeyId="+strAccessKeyId+"&"+
                            "SignatureMethod="+strSignatureMethod+"&"+
                            "SignatureVersion="+strSignatureVersion+"&"+
                            "Timestamp="+strTimestamp;
    KF_LOG_INFO(logger, "[hbdmAuth] strSign = " << strSign );
    unsigned char* strHmac = hmac_sha256_byte(unit.secret_key.c_str(),strSign.c_str());
    KF_LOG_INFO(logger, "[hbdmAuth] strHmac = " << strHmac );
    std::string strSignatrue = base64_encode(strHmac,32);
    KF_LOG_INFO(logger, "[hbdmAuth] Signatrue = " << strSignatrue );
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("op");
    writer.String("auth");
    writer.Key("type");
    writer.String("api");
    writer.Key("AccessKeyId");
    writer.String(unit.api_key.c_str());
    writer.Key("SignatureMethod");
    writer.String("HmacSHA256");
    writer.Key("SignatureVersion");
    writer.String("2");
    writer.Key("Timestamp");
    writer.String(timestamp.c_str());
    writer.Key("Signature");
    writer.String(strSignatrue.c_str());

    writer.EndObject();
    std::string strSubscribe = sbUpdate.GetString();
    //KF_LOG_INFO(logger,"strSubscribe="<<strSubscribe);
    /*unsigned char msg[1024];
    memset(&msg[LWS_PRE], 0, 1024-LWS_PRE);
    int length = strSubscribe.length();
    KF_LOG_INFO(logger, "[hbdmAuth] auth data " << strSubscribe.c_str() << " ,len = " << length);
    strncpy((char *)msg+LWS_PRE, strSubscribe.c_str(), length);
    //请求
    int ret = lws_write(unit.webSocketConn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
    lws_callback_on_writable(unit.webSocketConn);*/
    for(auto it = unit.ws_msg_vec.begin(); it != unit.ws_msg_vec.end(); it++){
        if(it->websocket.m_connection == instance->m_connection){
            KF_LOG_INFO(logger,"send_auth"<<is_swap);
            it->websocket.SendMessage(strSubscribe);
        }
    }
    KF_LOG_INFO(logger, "[hbdmAuth] auth success...");
}
/*void TDEngineHbdm::lws_login(AccountUnitHbdm& unit, long timeout_nsec){
    KF_LOG_INFO(logger, "[TDEngineHbdm::lws_login]");
    global_md = this;
    m_isSubL3 = false;
    isAuth=nothing;
    isOrders=nothing;
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

    context = lws_create_context(&ctxCreationInfo);
    KF_LOG_INFO(logger, "[TDEngineHbdm::lws_login] context created.");


    if (context == NULL) {
        KF_LOG_ERROR(logger, "[TDEngineHbdm::lws_login] context is NULL. return");
        return;
    }

    // Set up the client creation info
    static std::string host  = "api.hbdm.com";
    static std::string path = "/notification";
    clientConnectInfo.address = host.c_str();
    clientConnectInfo.path = path.c_str(); // Set the info's path to the fixed up url path
    
    clientConnectInfo.context = context;
    clientConnectInfo.port = 443;
    clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    clientConnectInfo.host = host.c_str();
    clientConnectInfo.origin = "origin";
    clientConnectInfo.ietf_version_or_minus_one = -1;
    clientConnectInfo.protocol = protocols[0].name;
    clientConnectInfo.pwsi = &unit.webSocketConn;

    KF_LOG_INFO(logger, "[TDEngineHbdm::login] address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);
    //建立websocket连接
    unit.webSocketConn = lws_client_connect_via_info(&clientConnectInfo);
    if (unit.webSocketConn == NULL) {
        KF_LOG_ERROR(logger, "[TDEngineHbdm::lws_login] wsi create error.");
        //return;
        sleep(10);
        lws_login(unit,0);
    }
    KF_LOG_INFO(logger, "[TDEngineHbdm::login] wsi create success.");
    unit.is_connecting = true;
}*/
void TDEngineHbdm::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[TDEngineHbdm::login]");
    connect(timeout_nsec);
}

void TDEngineHbdm::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineHbdm::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineHbdm::is_logged_in() const{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineHbdm::is_connected() const{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}


std::string TDEngineHbdm::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "buy";
    } else if (LF_CHAR_Sell == input) {
        return "sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineHbdm::GetDirection(std::string input) {
    if ("buy-limit" == input || "buy-market" == input) {
        return LF_CHAR_Buy;
    } else if ("sell-limit" == input || "sell-market" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineHbdm::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "optimal_20";
    } else {
        return "";
    }
}

LfOrderPriceTypeType TDEngineHbdm::GetPriceType(std::string input) {
    if ("buy-limit" == input||"sell-limit" == input) {
        return LF_CHAR_LimitPrice;
    } else if ("buy-market" == input||"sell-market" == input) {
        return LF_CHAR_AnyPrice;
    } else {
        return '0';
    }
}
//订单状态，submitting , submitted 已提交, partial-filled 部分成交, partial-canceled 部分成交撤销, filled 完全成交, canceled 已撤销
LfOrderStatusType TDEngineHbdm::GetOrderStatus(std::string state) {

    if(state == "canceled"){
        return LF_CHAR_Canceled;
    }else if(state == "submitting"){
        return LF_CHAR_NotTouched;
    }else if(state == "partial-filled"){
        return  LF_CHAR_PartTradedQueueing;
    }else if(state == "submitted"){
        return LF_CHAR_NotTouched;
    }else if(state == "partial-canceled"){
        return LF_CHAR_Canceled;
    }else if(state == "filled"){
        return LF_CHAR_AllTraded;
    }
    return LF_CHAR_AllTraded;
}

/**
 * req functions
 * 查询账户持仓
 */
void TDEngineHbdm::req_investor_position(const LFQryPositionField* data, int account_index, int requestId){
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitHbdm& unit = account_units[account_index];
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
    get_account(unit, d, false);
    KF_LOG_INFO(logger, "[req_investor_position] (get_account)" );
    /*if(d.IsObject() && d.HasMember("status"))
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
            raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_HBDM, 1, requestId, errorId, errorMsg.c_str());
        }
    }*/
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_HBDM, 1, requestId);

    std::vector<LFRspPositionField> tmp_vector;
    if(!d.HasParseError() && d.HasMember("data"))
    {
        //if( !strcmp(data->BrokerID, "master-spot") || !strcmp(data->BrokerID, "master-margin")){
            auto& accounts = d["data"];
            size_t len = d["data"].Size();
            KF_LOG_INFO(logger, "[req_investor_position] (accounts.length)" << len);
            for(size_t i = 0; i < len; i++)
            {
                    std::string symbol = accounts.GetArray()[i]["symbol"].GetString();
                    std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);

                    if(ticker.length() > 0){
                        strncpy(pos.InstrumentID, ticker.c_str(), 31);
                        if(accounts.GetArray()[i]["margin_available"].IsInt64()){
                            pos.Position = std::round(accounts.GetArray()[i]["margin_available"].GetInt64() * scale_offset);
                        }
                        tmp_vector.push_back(pos);
                        KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId 
                                    << " (ticker) " << ticker << " (position) " << pos.Position);
                    }
            }
        //}
    }
    get_account(unit, d, true);
    KF_LOG_INFO(logger, "[req_investor_position] (get_account2)" );
    if(!d.HasParseError() && d.HasMember("data"))
    {
        //if( !strcmp(data->BrokerID, "master-spot") || !strcmp(data->BrokerID, "master-margin")){
            auto& accounts = d["data"];
            size_t len = d["data"].Size();
            KF_LOG_INFO(logger, "[req_investor_position] (accounts.length)" << len);
            for(size_t i = 0; i < len; i++)
            {
                    std::string symbol = accounts.GetArray()[i]["symbol"].GetString();
                    std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);

                    if(ticker.length() > 0){
                        strncpy(pos.InstrumentID, ticker.c_str(), 31);
                        if(accounts.GetArray()[i]["margin_available"].IsDouble()){
                            pos.Position = std::round(accounts.GetArray()[i]["margin_available"].GetDouble() * scale_offset);
                        }
                        tmp_vector.push_back(pos);
                        KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId 
                                    << " (ticker) " << ticker << " (position) " << pos.Position);
                    }
            }
        //}
    }

    //send the filtered position
    int position_count = tmp_vector.size();
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

void TDEngineHbdm::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineHbdm::dealPriceVolume(AccountUnitHbdm& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,
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
void TDEngineHbdm::dealnum(string pre_num,string& fix_num)
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
    //KF_LOG_INFO(logger,"pre_num:"<<pre_num<<"fix_num:"<<fix_num);
}
//发单
void TDEngineHbdm::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time){
    AccountUnitHbdm& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_HBDM, 1/*ISLAST*/, requestId);

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
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_HBDM, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);
    std::string fixedPrice;
    auto it=precision_map.find(ticker);
    if(it != precision_map.end()){
        KF_LOG_INFO(logger,"find in precision_map");
        double precision = it->second;
        int64_t precision_num;
        if(GetSide(data->Direction)=="sell"){
            precision_num = (std::ceil(data->LimitPrice/precision))*precision;
        }else{
            precision_num = (std::floor(data->LimitPrice/precision))*precision;
        }
        dealnum(std::to_string(precision_num),fixedPrice);
        /*int flag=8-log(precision)/log(10);
        fixedPrice=std::to_string(precision_num);
        std::string s1=fixedPrice.substr(0,fixedPrice.length()-flag);
        std::string s2=fixedPrice.substr(fixedPrice.length()-flag,fixedPrice.length());
        fixedPrice=s1+"."+s2;*/
    }else{
        fixedPrice = std::to_string((double)data->LimitPrice/scale_offset);
    }
    Document d;
    //std::string fixedPrice = std::to_string((double)data->LimitPrice/scale_offset);
    std::string fixedVolume = std::to_string((double)data->Volume/scale_offset);
    KF_LOG_INFO(logger,"fixedPrice="<<fixedPrice<<"fixedVolume="<<fixedVolume);
    int64_t ivolume = stoll(fixedVolume);
    KF_LOG_INFO(logger,"ivolume="<<ivolume);
    double dprice = std::stod(fixedPrice);
    int64_t price = std::round(dprice * scale_offset);
    uint64_t volume = ivolume * scale_offset;
    KF_LOG_INFO(logger,"volume="<<volume);

    //lock
    std::string offset;
    if(data->OffsetFlag==LF_CHAR_Open){
        offset="open";
    }else{
        offset="close";
    }
    std::string cid = genClinetid(std::string(data->OrderRef));
    if(!unit.is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        return;
    }
    send_order(unit, ticker.c_str(), GetSide(data->Direction).c_str(),
            GetType(data->OrderPriceType).c_str(), ivolume, fixedPrice, d,offset,cid,is_post_only(data));
    //not expected response
    if(!d.IsObject()){
        errorId = 100;
        errorMsg = "send_order http response has parse error or is not json. please check the log";
        KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
                                                                           errorId << " (errorMsg) " << errorMsg);
    } else  if(d.HasMember("status")){//发单成功
        std::string status =d["status"].GetString();
        if(status == "ok") {
            KF_LOG_INFO(logger,"ok");
            std::string remoteOrderId = d["data"]["order_id_str"].GetString();
            //fix defect of use the old value
            localOrderRefRemoteOrderId[std::string(data->OrderRef)] = remoteOrderId;
            KF_LOG_INFO(logger, "[req_order_insert] after send  (rid)" << requestId << " (OrderRef) " <<
                                                                       data->OrderRef << " (remoteOrderId) "
                                                                       << remoteOrderId);

            LFRtnOrderField rtn_order;
            memset(&rtn_order, 0, sizeof(LFRtnOrderField));
            strncpy(rtn_order.BusinessUnit,remoteOrderId.c_str(),21);
            rtn_order.OrderStatus = LF_CHAR_NotTouched;
            rtn_order.VolumeTraded = 0;
            
            strcpy(rtn_order.ExchangeID, "hbdm");
            strncpy(rtn_order.UserID, unit.api_key.c_str(), 16);
            strncpy(rtn_order.InstrumentID, data->InstrumentID, 31);
            rtn_order.Direction = data->Direction;
            //No this setting on Hbdm
            rtn_order.TimeCondition = LF_CHAR_GTC;
            rtn_order.OrderPriceType = data->OrderPriceType;
            strncpy(rtn_order.OrderRef, data->OrderRef, 13);
            rtn_order.VolumeTotalOriginal = volume;
            rtn_order.LimitPrice = price;
            rtn_order.VolumeTotal = volume;
            rtn_order.RequestID = requestId;
            rtn_order.OffsetFlag = data->OffsetFlag;
            on_rtn_order(&rtn_order);
            raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
                                    source_id, MSG_TYPE_LF_RTN_ORDER_HBDM,
                                    1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);

            KF_LOG_DEBUG(logger, "[req_order_insert] (addNewOrderToMap)" );
            //addNewOrderToMap(unit, rtn_order);
            m_mapOrder.insert(std::make_pair(remoteOrderId,rtn_order));
            raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_HBDM, 1,
                                          requestId, errorId, errorMsg.c_str());
            KF_LOG_DEBUG(logger, "[req_order_insert] success" );
            return;
        }else {
            //errorId = std::round(std::stod(d["id"].GetString()));
            errorId=200;
            if(d.HasMember("err_msg") && d["err_msg"].IsString()){
                errorMsg = d["err_msg"].GetString();
            }
            KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
                                                                               errorId << " (errorMsg) " << errorMsg);
        }
    }
    else if(d.HasMember("code") && d.HasMember("msg"))
    {
        errorId = d["code"].GetInt();
        errorMsg = d["msg"].GetString();
    }
    //unlock
    if(errorId != 0)
    {
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_HBDM, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineHbdm::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time){
    AccountUnitHbdm& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_HBDM, 1, requestId);

    int errorId = 0;
    std::string errorMsg = "";

    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_HBDM, 1, requestId, errorId, errorMsg.c_str());
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
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_HBDM, 1, requestId, errorId, errorMsg.c_str());
        return;
    } else {
        remoteOrderId = itr->second;
        KF_LOG_DEBUG(logger, "[req_order_action] found in localOrderRefRemoteOrderId map (orderRef) "
                << data->OrderRef << " (remoteOrderId) " << remoteOrderId);
    }
    Document d;
    if(!unit.is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        return;
    }
    addRemoteOrderIdOrderActionSentTime(data, requestId, remoteOrderId, unit);
    cancel_order(unit, ticker, remoteOrderId, d);

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
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_HBDM, 1, 
            requestId, errorId, errorMsg.c_str());

        Document json;
        int isTraded = orderIsTraded(unit,ticker,remoteOrderId,json);
        if(isTraded == 1){
            KF_LOG_INFO(logger,"[req_order_action] AllTraded or Canceled, can not cancel again.");
            return;
        }
    } else {
        //addRemoteOrderIdOrderActionSentTime( data, requestId, remoteOrderId);
        // addRemoteOrderIdOrderActionSentTime( data, requestId, remoteOrderId);
        //TODO:   onRtn order/on rtn trade
    }
}

//请求历史k线
void TDEngineHbdm::req_get_kline_via_rest(const GetKlineViaRest* data, int account_index, int requestId, long rcv_time)
{
    KF_LOG_INFO(logger, "TDEngineHbdm::req_get_kline_via_rest: (symbol)" << data->Symbol << " (interval)" << data->Interval);
    writer->write_frame(data, sizeof(GetKlineViaRest), source_id, MSG_TYPE_LF_GET_KLINE_VIA_REST, 1/*islast*/, requestId);

    AccountUnitHbdm& unit = account_units[account_index];
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

    //BTC_CQ   交割合约               /market/history/kline
    //BTC-USD  币本位永续             /swap-ex/market/history/kline
    //BTC-USDT USDT本位永续           /linear-swap-ex/market/history/kline
    //BTC-USDT-200925-C-10000 期权    /option-ex/market/history/kline

    string url = "https://api.hbdm.com";
    if (ticker.find("-USDT-") != -1)
        url += "/option-ex/market/history/kline";
    else if (ticker.find("-USDT") != -1)
        url += "/linear-swap-ex/market/history/kline";
    else if (ticker.find("-USD") != -1)
        url += "/swap-ex/market/history/kline";
    else if (ticker.find("_") != -1)
        url += "/market/history/kline";

    cpr::Response response;
    if (data->IgnoreStartTime)
        response = cpr::Get(Url{ url }, Parameters{ {"symbol", ticker},{"period", data->Interval},{"size", to_string(param_limit)} });
    else
        response = cpr::Get(Url{ url }, Parameters{ {"symbol", ticker},{"period", data->Interval},{"size", to_string(param_limit)},
            {"from", to_string(data->StartTime)}, {"to", to_string(data->EndTime)} });
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
    KF_LOG_INFO(logger, "TDEngineHbdm::req_get_kline_via_rest: parse response" << response.text.c_str());

    Document d;
    d.Parse(response.text.c_str());

    if (d.HasMember("data") && d["data"].IsArray()) {
        LFBarSerial1000Field bars;
        memset(&bars, 0, sizeof(bars));
        strncpy(bars.InstrumentID, data->Symbol, 31);
        strcpy(bars.ExchangeID, "hbdm");

        for (int i = 0; i < d["data"].Size(); i++) {
            if (!d["data"][i].IsObject()) {
                KF_LOG_INFO(logger, "TDEngineHbdm::req_get_kline_via_rest: response is abnormal" << response.text.c_str());
                break;
            }

            bars.BarSerial[i].StartUpdateMillisec = d["data"][i]["id"].GetInt64() * 1000;
            bars.BarSerial[i].EndUpdateMillisec   = bars.BarSerial[i].StartUpdateMillisec;
            bars.BarSerial[i].PeriodMillisec      = GetMillsecondByInterval(data->Interval) + bars.BarSerial[i].EndUpdateMillisec;

            //scale_offset = 1e8
            bars.BarSerial[i].Open                = std::round(d["data"][i]["open"].GetDouble() * scale_offset);
            bars.BarSerial[i].Close               = std::round(d["data"][i]["close"].GetDouble() * scale_offset);
            bars.BarSerial[i].Low                 = std::round(d["data"][i]["low"].GetDouble() * scale_offset);
            bars.BarSerial[i].High                = std::round(d["data"][i]["high"].GetDouble() * scale_offset);
            bars.BarSerial[i].Volume              = std::round(d["data"][i]["amount"].GetDouble() * scale_offset);
            bars.BarSerial[i].BusinessVolume      = std::round(d["data"][i]["vol"].GetDouble() * scale_offset);

            bars.BarSerial[i].TransactionsNum     = d["data"][i]["count"].GetInt();
            bars.BarLevel = i + 1;
        }
        on_bar_serial1000(&bars, data->RequestID);
    }
    else if (!d.IsArray()) {
        KF_LOG_INFO(logger, "TDEngineHbdm::req_get_kline_via_rest: response is abnormal");
    }
}

//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
void TDEngineHbdm::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId,std::string remoteOrderId,AccountUnitHbdm& unit){
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;
    newOrderActionSent.unit = unit;
    newOrderActionSent.sentNameTime = getTimestamp();
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    remoteOrderIdOrderActionSentTime[remoteOrderId] = newOrderActionSent;
}


void TDEngineHbdm::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineHbdm::loop");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineHbdm::loopwebsocket, this)));

    KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineHbdm::loopOrderActionNoResponseTimeOut");
    orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineHbdm::loopOrderActionNoResponseTimeOut, this)));
}
//cys no use
void TDEngineHbdm::loopwebsocket()
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
        //KF_LOG_INFO(logger, "TDEngineHbdm::loop:lws_service");
        //lws_service( context, rest_get_interval_ms );
        for (size_t idx = 0; idx < account_units.size(); idx++)
        {
            AccountUnitHbdm& unit = account_units[idx];
            for(auto it = unit.ws_msg_vec.begin(); it != unit.ws_msg_vec.end(); it++){
                if(it->is_ws_disconnectd)
                {
                    KF_LOG_INFO(logger,"it->ws_url:"<<it->ws_url);
                    it->websocket.RegisterCallBack(this);
                    it->logged_in = it->websocket.Connect(it->ws_url);
                    KF_LOG_INFO(logger, "TDEngineHbdm::relogin " << (it->logged_in ? "Success" : "Failed"));                
                    
                    it->is_ws_disconnectd = false;
                }
            }
        }
    }
}


void TDEngineHbdm::loopOrderActionNoResponseTimeOut()
{
    KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        orderActionNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void TDEngineHbdm::orderActionNoResponseTimeOut(){
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut]");
    int errorId = 100;
    std::string errorMsg = "OrderAction has none response for a long time, please send OrderAction again";

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
            Document json;
            std::string symbol;
            std::string orderId = itr->first;
            auto it = m_mapOrder.find(orderId);
            if(it != m_mapOrder.end()){
                symbol = std::string(it->second.InstrumentID);
            }
            symbol = itr->second.unit.coinPairWhiteList.GetValueByKey(symbol);
            //symbol = symbol.substr(0,3);
            query_order(itr->second.unit, symbol, orderId, json);
            if(json.HasMember("data")){
                Value &node = json["data"];
                int size = node.Size();
                for(int i=0;i<size;i++){
                    int status = node.GetArray()[i]["status"].GetInt();
                    if(status == 3){//open
                        KF_LOG_INFO(logger,"order is open");
                    }else if(status == 5||status == 7){//canceled
                        DealCancel(orderId);
                        //it->second = LF_CHAR_Canceled;
                        //on_rtn_order(&(it->second));
                    }else{//trade
                        double trade_price;
                        if(node.GetArray()[i]["trade_price"].IsNumber()){
                            trade_price = node.GetArray()[i]["trade_price"].GetDouble();
                        }
                        int trade_volume = node.GetArray()[i]["trade_volume"].GetInt();
                        trade_price = std::round(trade_price * scale_offset);
                        int64_t price = trade_price;
                        trade_volume = std::round(trade_volume * scale_offset);
                        uint64_t volume = trade_volume;
                        DealTrade(orderId,price,volume);
                    }
                }
            }
            //on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
            itr = remoteOrderIdOrderActionSentTime.erase(itr);
        } else {
            ++itr;
        }
    }
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}

void TDEngineHbdm::printResponse(const Document& d){
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
}

void TDEngineHbdm::getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json)
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
        if(responseText.empty())
        {
            rapidjson::Value val;
            val.SetString(responseText.c_str(), responseText.length(), allocator);
            json.AddMember("msg", val, allocator);
        }
        else
        {
            rapidjson::Value val;
            val.SetString(errorMsg.c_str(), errorMsg.length(), allocator);
            json.AddMember("msg", val, allocator);
        }
    }
}

void TDEngineHbdm::get_account(AccountUnitHbdm& unit, Document& json, bool is_swap)
{
    KF_LOG_INFO(logger, "[get_account]");
    /*
      账户余额
      查询指定账户的余额，支持以下账户：
      spot：现货账户， margin：杠杆账户，otc：OTC 账户，point：点卡账户
      HTTP 请求
      GET /v1/account/accounts/{account-id}/balance
    */
    std::string getPath = "/api/v1/contract_account_info";
    //std::string getPath="/api/v1/contract_position_info";
    if(is_swap){
        //getPath = "/swap-api/v1/swap_position_info";
        getPath = "/swap-api/v1/swap_account_info";
    }
    //string accountId=isMargin?unit.marginAccountId:unit.spotAccountId;
    //std::string requestPath = getPath+accountId+"/balance";
    const auto response = Post(getPath,"",unit);
    json.Parse(response.text.c_str());
    //KF_LOG_INFO(logger, "[get_account] (account info) "<<response.text.c_str());
    return ;
}
void TDEngineHbdm::getAccountId(AccountUnitHbdm& unit){
    KF_LOG_DEBUG(logger,"[getAccountID] ");
    std::string getPath="/v1/account/accounts/";
    const auto resp = Get("/v1/account/accounts","{}",unit);
    Document j;
    j.Parse(resp.text.c_str());
    int n=j["data"].Size();
    std::string type="spot";//现货账户
    std::string marginType="margin";//现货账户
    string state="working";
    std::string accountId;
    bool isSpot=false,isMyMargin=false;
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
    KF_LOG_DEBUG(logger,"[getAccountID] (accountId) "<<accountId);
}
std::string TDEngineHbdm::getHbdmTime(){
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
    std::string hbdmTime=timeBuf;
    return hbdmTime;
}
std::string TDEngineHbdm::getHbdmNormalTime(){
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
    std::string hbdmTime=timeBuf;
    return hbdmTime;
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
std::string TDEngineHbdm::createInsertOrdertring(const char *accountId,
        int amount, const char *price, const char *source, const char *symbol,const char *side,std::string offset,std::string type,std::string cid,bool isPostOnly){
    string sym = symbol;
    sym=sym.substr(0,3);
    int64_t client_order_id = stoll(cid);
    bool is_swap = false;
    if(string(symbol).find("-") != -1){
        is_swap = true;
    }
    //string volume = amount;
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    if(!is_swap){
        writer.Key("symbol");
        writer.String(sym.c_str());
    }
    writer.Key("volume");
    writer.Int(amount);
    if(type == "limit"){
        writer.Key("price");
        writer.String(price);
    }
    writer.Key("client_order_id");
    writer.Int64(client_order_id);
    writer.Key("contract_code");
    writer.String(symbol);
    writer.Key("direction");
    writer.String(side);
    writer.Key("offset");
    writer.String(offset.c_str());
    writer.Key("lever_rate");
    writer.Int(lever_rate);
    writer.Key("order_price_type");
    if(isPostOnly){
        writer.String("post_only");
    }else{
        writer.String(type.c_str());
    }
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
void TDEngineHbdm::send_order(AccountUnitHbdm& unit, const char *code,
                                 const char *side, const char *type, int volume, std::string price, Document& json,std::string offset,std::string cid,bool isPostOnly){
    KF_LOG_INFO(logger, "[send_order]");
    KF_LOG_INFO(logger, "[send_order] (code) "<<code);
    std::string s=side,t=type;
    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    bool is_swap = false;
    if(string(code).find("-") != -1){
        is_swap = true;
    }
    do {
        should_retry = false;
        //火币下单post /v1/order/orders/place
        std::string requestPath = "/api/v1/contract_order";
        if(is_swap){
            requestPath = "/swap-api/v1/swap_order";
        }
        string source="api";
        string accountId=unit.spotAccountId;
        //lock
        if(isMargin){
            source="margin-api";
            accountId=unit.marginAccountId;
        }
        KF_LOG_INFO(logger,"[send_order] (isMargin) "<<isMargin<<" (source) "<<source);
        response = Post(requestPath,createInsertOrdertring(accountId.c_str(), volume, price.c_str(),
                        source.c_str(),code,s.c_str(),offset,t,cid,isPostOnly),unit);

        KF_LOG_INFO(logger, "[send_order] (url) " << requestPath << " (response.status_code) " << response.status_code 
                                                  << " (response.error.message) " << response.error.message 
                                                  <<" (response.text) " << response.text.c_str() << " (retry_times)" << retry_times);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);
        //has error and find the 'error setting certificate verify locations' error, should retry
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
/*火币返回数据格式
    {
  "status": "ok",
  "ch": "market.btcusdt.kline.1day",
  "ts": 1499223904680,
  "data": // per API response data in nested JSON object
    }
*/
bool TDEngineHbdm::shouldRetry(Document& doc)
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
/*
 响应数据
 参数名称	是否必须	数据类型	描述	取值范围
 success-count	true	int	成功取消的订单数	
 failed-count	true	int	取消失败的订单数	
 next-id	true	long	下一个符合取消条件的订单号	
 */
void TDEngineHbdm::cancel_all_orders(AccountUnitHbdm& unit, std::string code, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_all_orders]");
    std::string symbol = code.substr(0, 3);
    bool is_swap = false;
    if(code.find("-") != -1){
        is_swap = true;
    }
    std::string accountId = unit.spotAccountId;
    if(isMargin)accountId=unit.marginAccountId;
    //火币post批量撤销订单
    std::string requestPath = "/api/v1/contract_cancelall";
    if(is_swap){
        requestPath = "/swap-api/v1/swap_cancelall";
    }
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    if(!is_swap){
        writer.Key("symbol");
        writer.String(symbol.c_str());
    }
    writer.Key("contract_code");
    writer.String(code.c_str());
    //write Signature
    writer.EndObject();
    auto response = Post(requestPath,s.GetString(),unit);
    getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineHbdm::cancel_order(AccountUnitHbdm& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_order]");

    bool is_swap = false;
    if(code.find("-") != -1){
        is_swap = true;
    }

    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;
    do {
        should_retry = false;
        //火币post撤单请求
        std::string postPath="/api/v1/contract_cancel";
        if(is_swap){
            postPath = "/swap-api/v1/swap_cancel";
        }
        std::string requestPath = postPath;
        std::string symbol;
        auto it = m_mapOrder.find(orderId);
        if(it != m_mapOrder.end()){
            symbol = std::string(it->second.InstrumentID);
            //symbol = symbol.substr(0,3);
        }
        symbol = unit.coinPairWhiteList.GetValueByKey(symbol);
        symbol = symbol.substr(0,3);

        StringBuffer s;
        Writer<StringBuffer> writer(s);
        writer.StartObject();
        if(!is_swap){
            writer.Key("symbol");
            writer.String(symbol.c_str());
        }else{
            writer.Key("contract_code");
            writer.String(code.c_str());
        }
        
        writer.Key("order_id");
        writer.String(orderId.c_str());
        writer.EndObject();

        response = Post(requestPath,s.GetString(),unit);

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
int TDEngineHbdm::orderIsTraded(AccountUnitHbdm& unit, std::string code, std::string orderId, Document& json){
    KF_LOG_INFO(logger,"[orderIsCanceled]");
    std::map<std::string,LFRtnOrderField>::iterator itr = unit.restOrderStatusMap.find(orderId);
    if(itr == unit.restOrderStatusMap.end()){
        KF_LOG_INFO(logger,"[orderIsCanceled] order id not exits in restOrderStatusMap!");
        return -1;
    }
    //query_order(unit,code,orderId,json);
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
            unit.restOrderStatusMap.erase(orderId);
        }
    }
    if(orderStatus == LF_CHAR_AllTraded || orderStatus == LF_CHAR_Canceled)return 1;
}
void TDEngineHbdm::query_order(AccountUnitHbdm& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[query_order]");
    bool is_swap = false;
    if(code.find("-") != -1){
        is_swap = true;
    }
    std::string symbol = code.substr(0,3);
    //火币get查询订单详情
    std::string getPath = "/api/v1/contract_order_info";
    if(is_swap){
        getPath = "/swap-api/v1/swap_order_info";
    }
    std::string requestPath = getPath;
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    if(!is_swap){
        writer.Key("symbol");
        writer.String(symbol.c_str());
    }else{
        writer.Key("contract_code");
        writer.String(code.c_str());
    }
    writer.Key("order_id");
    writer.String(orderId.c_str());
    writer.EndObject();    
    auto response = Post(requestPath,s.GetString(),unit);
    json.Parse(response.text.c_str());
    KF_LOG_DEBUG(logger,"[query_order] response "<<response.text.c_str());
    //getResponse(response.status_code, response.text, response.error.message, json);
}


void TDEngineHbdm::handleResponseOrderStatus(AccountUnitHbdm& unit, LFRtnOrderField& rtn_order, Document& json)
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
    
    //if(role == "taker" && (orderStatus == LF_CHAR_AllTraded || orderStatus == LF_CHAR_PartTradedQueueing)){
        //KF_LOG_INFO(logger, "[handleResponseOrderStatus] role is taker");
        //return;
    //}

    int64_t volumeTraded = rtn_order.VolumeTraded+nDealSize;//nVolume-nUnfilledAmount;
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
    on_rtn_order(&rtn_order);
    raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_ORDER_HBDM,
        1, (rtn_order.RequestID > 0) ? rtn_order.RequestID: -1);

    if(oldVolumeTraded != rtn_order.VolumeTraded){
        //send OnRtnTrade
        LFRtnTradeField rtn_trade;
        memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
        strcpy(rtn_trade.ExchangeID, "hbdm");
        strncpy(rtn_trade.UserID, unit.api_key.c_str(), 16);
        strncpy(rtn_trade.InstrumentID, rtn_order.InstrumentID, 31);
        strncpy(rtn_trade.OrderRef, rtn_order.OrderRef, 13);
        rtn_trade.Direction = rtn_order.Direction;
        //单次成交数量
        rtn_trade.Volume = nDealSize;
        rtn_trade.Price =nDealPrice;//(newAmount - oldAmount)/(rtn_trade.Volume);
        strncpy(rtn_trade.OrderSysID,rtn_order.BusinessUnit,31);
        on_rtn_trade(&rtn_trade);

        raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
            source_id, MSG_TYPE_LF_RTN_TRADE_HBDM, 1, -1);

        KF_LOG_INFO(logger, "[on_rtn_trade 1] (InstrumentID)" << rtn_trade.InstrumentID << "(Direction)" << rtn_trade.Direction
                << "(Volume)" << rtn_trade.Volume << "(Price)" <<  rtn_trade.Price);  
    }


}
std::string TDEngineHbdm::parseJsonToString(Document &d){
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


inline int64_t TDEngineHbdm::getTimestamp(){
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void TDEngineHbdm::genUniqueKey(){
    struct tm cur_time = getCurLocalTime();
    //SSMMHHDDN
    char key[11]{0};
    snprintf((char*)key, 11, "%02d%02d%02d%02d%1s", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_engineIndex.c_str());
    m_uniqueKey = key;
}
//clientid =  m_uniqueKey+orderRef
std::string TDEngineHbdm::genClinetid(const std::string &orderRef){
    static int nIndex = 0;
    return m_uniqueKey + orderRef + std::to_string(nIndex++);
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))
BOOST_PYTHON_MODULE(libhbdmtd){
    using namespace boost::python;
    class_<TDEngineHbdm, boost::shared_ptr<TDEngineHbdm> >("Engine")
     .def(init<>())
        .def("init", &TDEngineHbdm::initialize)
        .def("start", &TDEngineHbdm::start)
        .def("stop", &TDEngineHbdm::stop)
        .def("logout", &TDEngineHbdm::logout)
        .def("wait_for_stop", &TDEngineHbdm::wait_for_stop);
}


