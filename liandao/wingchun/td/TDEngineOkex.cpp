#include "TDEngineOkex.h"
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
typedef char char_64[64];
std::mutex order_mutex;

TDEngineOkex::TDEngineOkex(): ITDEngine(SOURCE_OKEX)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.Okex");
    KF_LOG_INFO(logger, "[TDEngineOkex]");

    mutex_order_and_trade = new std::mutex();
    mutex_map_pricevolum = new std::mutex();
    mutex_response_order_status = new std::mutex();
    mutex_orderaction_waiting_response = new std::mutex();
    
    mutex_web_orderstatus=new std::mutex();
    mutex_web_connect=new std::mutex();
}

TDEngineOkex::~TDEngineOkex()
{
    if(mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if(mutex_response_order_status != nullptr) delete mutex_response_order_status;
    if(mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
    if(nullptr != mutex_map_pricevolum)delete mutex_map_pricevolum;
    if(m_ThreadPoolPtr!=nullptr) delete m_ThreadPoolPtr;
    if(mutex_web_orderstatus!=nullptr) delete mutex_web_orderstatus;
    if(mutex_web_connect!=nullptr) delete mutex_web_connect;
    
}
// gzCompress: do the compressing
int TDEngineOkex::gzCompress(const char *src, int srcLen, char *dest, int destLen){
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
 
int TDEngineOkex::gzDecompress(const char *src, int srcLen, const char *dst, int dstLen){
    int err = 0;
    z_stream d_stream = {0}; /* decompression stream */

    static char dummy_head[2] = {
            0x8 + 0x7 * 0x10,
            (((0x8 + 0x7 * 0x10) * 0x100 + 30) / 31 * 31) & 0xFF,
    };

    d_stream.zalloc = NULL;
    d_stream.zfree = NULL;
    d_stream.opaque = NULL;
    d_stream.next_in = (Byte *)src;
    d_stream.avail_in = 0;
    d_stream.next_out = (Byte *)dst;


    if (inflateInit2(&d_stream, -MAX_WBITS) != Z_OK) {
        return -1;
    }

    // if(inflateInit2(&d_stream, 47) != Z_OK) return -1;

    while (d_stream.total_out < dstLen && d_stream.total_in < srcLen) {
        d_stream.avail_in = d_stream.avail_out = 1; /* force small buffers */
        if((err = inflate(&d_stream, Z_NO_FLUSH)) == Z_STREAM_END)
            break;

        if (err != Z_OK) {
            if (err == Z_DATA_ERROR) {
                d_stream.next_in = (Bytef*) dummy_head;
                d_stream.avail_in = sizeof(dummy_head);
                if((err = inflate(&d_stream, Z_NO_FLUSH)) != Z_OK) {
                    return -1;
                }
            } else {
                return -1;
            }
        }
    }

    if (inflateEnd(&d_stream)!= Z_OK)
        return -1;
    dstLen = d_stream.total_out;
    return 0;
}
static TDEngineOkex* global_md = nullptr;
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
void TDEngineOkex::on_lws_open(struct lws* wsi){
    KF_LOG_INFO(logger,"[on_lws_open] wsi" << wsi);
    okexAuth(findAccountUnitOkexByWebsocketConn(wsi));
    KF_LOG_INFO(logger,"[on_lws_open] finished ");
}
std::mutex account_mutex;
//cys websocket connect
void TDEngineOkex::Ping(struct lws* conn)
{
    //m_shouldPing = false;
    auto& unit = findAccountUnitOkexByWebsocketConn(conn);
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
void TDEngineOkex::Pong(struct lws* conn,long long ping){
    KF_LOG_INFO(logger,"[Pong] pong the ping of websocket," << conn);
    auto& unit = findAccountUnitOkexByWebsocketConn(conn);
    StringBuffer sbPing;
    Writer<StringBuffer> writer(sbPing);
    writer.StartObject();
    writer.Key("op");
    writer.String("pong");
    writer.Key("ts");
    writer.Int64(ping);
    writer.EndObject();
    std::string strPong = sbPing.GetString();
    unit.sendmessage.push(strPong);
    lws_callback_on_writable(conn);
}
/*void TDEngineOkex::on_lws_receive_orders(struct lws* conn,Document& json){
    KF_LOG_INFO(logger,"[on_lws_receive_orders]");
    std::lock_guard<std::mutex> guard_mutex(*mutex_response_order_status);
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);
    AccountUnitOkex& unit = findAccountUnitOkexByWebsocketConn(conn);
    rapidjson::Value &data=json["data"];
    KF_LOG_INFO(logger, "[on_lws_receive_orders] receive_order:");
    if(data.HasMember("order-id")){
        KF_LOG_INFO(logger, "[on_lws_receive_orders] (receive success)");
        string remoteOrderId=std::to_string(data["order-id"].GetInt64());
        std::unique_lock<std::mutex> rest_order_status_mutex(*mutex_order_and_trade);
        std::map<std::string,LFRtnOrderField>::iterator restOrderStatus=unit.restOrderStatusMap.find(remoteOrderId);
        rest_order_status_mutex.unlock();
        if(restOrderStatus==unit.restOrderStatusMap.end()){
            std::unique_lock<std::mutex> web_socket_orderstatus_mutex(* mutex_web_orderstatus);
            KF_LOG_ERROR(logger,"[on_lws_receive_orders] rest receive no order id, save int websocketOrderStatusMap");
            unit.websocketOrderStatusMap.push_back(parseJsonToString(json));
            web_socket_orderstatus_mutex.unlock();

        }else{
            handleResponseOrderStatus(unit, restOrderStatus->second, json);
            LfOrderStatusType orderStatus=GetOrderStatus(json["data"]["order-state"].GetString());
            if(orderStatus == LF_CHAR_AllTraded  || orderStatus == LF_CHAR_Canceled
                || orderStatus == LF_CHAR_Error){
                KF_LOG_INFO(logger, "[rest addNewOrderToMap] remove a pendingOrderStatus.");
                unit.restOrderStatusMap.erase(remoteOrderId);
            }
        }
    } else {
        KF_LOG_INFO(logger, "[on_lws_receive_orders] (reveive failed)");
        std::string errorMsg;
        int errorId = json["err-code"].GetInt();
        if(json.HasMember("err-msg") && json["err-msg"].IsString()){
            errorMsg = json["err-msg"].GetString();
        }
        KF_LOG_ERROR(logger, "[on_lws_receive_orders] get_order fail."<< " (errorId)" << errorId<< " (errorMsg)" << errorMsg);
    }
}*/
void TDEngineOkex::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    int l = len*10;
    char* buf = new char[l]{};
    int result = gzDecompress(data, len, buf, l);
    KF_LOG_INFO(logger, "[on_lws_data]: "<< buf);

    Document json;
    json.Parse(buf);
    if(json.HasParseError()||!json.IsObject()){
        KF_LOG_ERROR(logger, "MDEngineOkex::on_lws_data. parse json error: " << buf);
        std::string new_buf = string(buf) + "}";
        json.Parse(new_buf.c_str());
        if(json.HasParseError()){
            KF_LOG_INFO(logger,"will return");
            delete[] buf;
            return;
        }
    }

    std::string loginstr = "login"; std::string tablestr = "spot/order";
    if(json.HasMember("event") && json["event"].GetString() == loginstr && json.HasMember("success") && json["success"].GetBool()){
        KF_LOG_INFO(logger,"login success");
        AccountUnitOkex& unit=findAccountUnitOkexByWebsocketConn(conn);
        unit.isAuth=okex_auth;        
    }else if(json.HasMember("table") && json["table"].GetString() == tablestr){
        auto& data = json["data"].GetArray()[0];
        std::string state = data["state"].GetString();
        std::string orderId = data["order_id"].GetString();
        std::string client_oid = data["client_oid"].GetString();   
        int64_t price = round(stod(data["last_fill_px"].GetString()) * scale_offset);
        uint64_t volume = round(stod(data["last_fill_qty"].GetString()) * scale_offset);
        if(state == "-1"){
            DealCancel(orderId);
        }else if(state == "1" || state == "2"){
            DealTrade(orderId, price, volume, state);
        }else if(state == "0"){
            DealNottouch(client_oid, orderId);
        }
    }
    delete[] buf;

}
void TDEngineOkex::DealNottouch(std::string toncestr,std::string strOrderId)
{
    KF_LOG_INFO(logger,"TDEngineCoinflex::DealNottouch");
    
    /*auto itr = remoteOrderIdOrderInsertSentTime.find(toncestr);
    if(itr != remoteOrderIdOrderInsertSentTime.end()){
        remoteOrderIdOrderInsertSentTime.erase(itr);
    }*/
    std::unique_lock<std::mutex> lck(order_mutex);
    auto iter3 = order_map.find(toncestr);
    if(iter3 != order_map.end())
    {
        KF_LOG_DEBUG(logger, "straction=open in");
        
        localOrderRefRemoteOrderId.insert(std::make_pair(iter3->second.OrderRef,strOrderId));
        order_map.insert(make_pair(strOrderId, iter3->second));
        //if(iter3->second.OrderStatus==LF_CHAR_NotTouched){
        iter3->second.OrderStatus=LF_CHAR_NotTouched;
        on_rtn_order(&(iter3->second));
        //}
        LFRtnOrderField lfrtnorder = iter3->second;
        order_map.erase(iter3);
        order_map.insert(make_pair(strOrderId, lfrtnorder));
    }
    lck.unlock();
                
}
void TDEngineOkex::DealCancel(std::string orderID)
{
    KF_LOG_INFO(logger,"DealCancel,orderID="<<orderID);
    std::unique_lock<std::mutex> lck(order_mutex);
    auto it = order_map.find(orderID);
    if(it != order_map.end())
    {
        it->second.OrderStatus = LF_CHAR_Canceled;
        on_rtn_order(&(it->second));

        auto it_id = localOrderRefRemoteOrderId.find(it->second.OrderRef);
        if(it_id != localOrderRefRemoteOrderId.end())
        {
            KF_LOG_INFO(logger,"erase local");
            localOrderRefRemoteOrderId.erase(it_id);
        }

        auto itr = remoteOrderIdOrderActionSentTime.find(orderID);
        if(itr != remoteOrderIdOrderActionSentTime.end()){
            KF_LOG_INFO(logger,"erase remoteOrderIdOrderActionSentTime");
            remoteOrderIdOrderActionSentTime.erase(itr);
        }

        order_map.erase(it);
    }
    lck.unlock();
}

void TDEngineOkex::DealTrade(std::string orderID,int64_t price,uint64_t lastvolume,std::string state)
{
    std::unique_lock<std::mutex> lck(order_mutex);
    auto it4 = order_map.find(orderID);
    if(it4 != order_map.end())
    {
        KF_LOG_DEBUG(logger, "straction=filled in");

        it4->second.VolumeTraded += lastvolume;
        if(it4->second.VolumeTotalOriginal >= it4->second.VolumeTraded){
            it4->second.VolumeTotal = it4->second.VolumeTotalOriginal - it4->second.VolumeTraded;
        }else{
            it4->second.VolumeTotal = 0;
        }
        if(state == "2"){
            it4->second.OrderStatus = LF_CHAR_AllTraded;
        }
        else{
            it4->second.OrderStatus = LF_CHAR_PartTradedQueueing;
        }
        on_rtn_order(&(it4->second));

        LFRtnTradeField rtn_trade;
        memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
        strcpy(rtn_trade.ExchangeID,it4->second.ExchangeID);
        strncpy(rtn_trade.UserID, it4->second.UserID,sizeof(rtn_trade.UserID));
        strncpy(rtn_trade.InstrumentID, it4->second.InstrumentID, sizeof(rtn_trade.InstrumentID));
        strncpy(rtn_trade.OrderRef, it4->second.OrderRef, sizeof(rtn_trade.OrderRef));
        rtn_trade.Direction = it4->second.Direction;
        strncpy(rtn_trade.OrderSysID,orderID.c_str(),sizeof(rtn_trade.OrderSysID));

        rtn_trade.Volume = lastvolume;
        rtn_trade.Price = price;
        if(rtn_trade.Volume != 0){
            on_rtn_trade(&rtn_trade);
        }

        if(it4->second.OrderStatus == LF_CHAR_AllTraded)
        {
            auto it_id = localOrderRefRemoteOrderId.find(it4->second.OrderRef);
            if(it_id != localOrderRefRemoteOrderId.end())
            {
                KF_LOG_INFO(logger,"earse local");
                localOrderRefRemoteOrderId.erase(it_id);
            }
            order_map.erase(it4);

        }
    }
    lck.unlock();
}

//{"op": "subscribe", "args": ["spot/order:LTC-USDT"]}
std::string TDEngineOkex::makeSubscribeOrdersUpdate(AccountUnitOkex& unit, string ticker){
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("op");
    writer.String("subscribe");
    writer.Key("args");
    writer.StartArray();
    std::string ticker_str = "spot/order:" + ticker;
    writer.String(ticker_str.c_str());
    writer.EndArray();
    writer.EndObject();
    std::string strUpdate = sbUpdate.GetString();
    return strUpdate;
}
AccountUnitOkex& TDEngineOkex::findAccountUnitOkexByWebsocketConn(struct lws * websocketConn){

    for (size_t idx = 0; idx < account_units.size(); idx++) {
        AccountUnitOkex &unit = account_units[idx];

        std::unique_lock<std::mutex> web_socket_connect_mutex(* mutex_web_connect);
        if(unit.webSocketConn == websocketConn) {
        KF_LOG_INFO(logger, "findAccountUnitOkexByWebsocketConn unit " << unit.webSocketConn << ","<< idx);
        return unit;
        }

    }
    return account_units[0];
}
int TDEngineOkex::subscribeTopic(struct lws* conn,string strSubscribe){
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
int TDEngineOkex::on_lws_write_subscribe(struct lws* conn){
    KF_LOG_INFO(logger, "[on_lws_write_subscribe]" );
    int ret = 0;
    //AccountUnitOkex& unit=findAccountUnitOkexByWebsocketConn(conn);
    AccountUnitOkex& unit = account_units[0];
    if(unit.sendmessage.size()>0)
    {
        ret = subscribeTopic(conn,unit.sendmessage.front());
        unit.sendmessage.pop();
    }
    if(unit.isAuth==okex_auth&&unit.isOrders != orders_sub){
        std::unordered_map<std::string, std::string>::iterator map_itr;
        for(map_itr = unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();map_itr != unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end();map_itr++){

            //unit.isOrders=orders_sub;

            string strSubscribe = makeSubscribeOrdersUpdate(unit, map_itr->second);
            unit.sendmessage.push(strSubscribe);
            //lws_callback_on_writable(conn);
        }
        unit.isOrders=orders_sub;
        lws_callback_on_writable(conn);
    }
    return ret;
}

void TDEngineOkex::on_lws_connection_error(struct lws* conn){
    KF_LOG_ERROR(logger, "TDEngineOkex::on_lws_connection_error. login again:" << conn);
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    m_isPong = false;
    //m_shouldPing = true;
    AccountUnitOkex& unit=findAccountUnitOkexByWebsocketConn(conn);             
    unit.isAuth = nothing;
    unit.isOrders = nothing;
    //no use it
    long timeout_nsec = 0;
    //reset sub
    //m_isSubL3 = false;
    //AccountUnitOkex& unit=findAccountUnitOkexByWebsocketConn(conn);
    //std::this_thread::sleep_for(std::chrono::minutes(5));
    lws_login(unit,0);
}
void TDEngineOkex::on_lws_close(struct lws* conn){
    AccountUnitOkex& unit=findAccountUnitOkexByWebsocketConn(conn);  
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
void TDEngineOkex::writeInfoLog(std::string strInfo){
    KF_LOG_INFO(logger,strInfo);
}
void TDEngineOkex::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}

int64_t TDEngineOkex::getMSTime(){
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  timestamp;
}

char TDEngineOkex::dec2hexChar(short int n) {
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
std::string TDEngineOkex::escapeURL(const string &URL){
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

std::mutex g_httpMutex;
cpr::Response TDEngineOkex::Get(const std::string& method_url,const std::string& body, AccountUnitOkex& unit,std::string parameters,bool is_account)
{
    /*std::string time_url = unit.baseUrl + "/api/general/v3/time";
    const auto res = cpr::Get(Url{time_url},Timeout{10000} );
    KF_LOG_INFO(logger, "[get] (url) " << time_url << " (res.status_code) " << res.status_code <<
                                                " (res.error.message) " << res.error.message <<
                                               " (res.text) " << res.text.c_str());
    Document json;
    json.Parse(res.text.c_str());
    std::string strTimestamp = json["iso"].GetString();*/

    std::string strTimestamp = getOkexTime_iso();
    KF_LOG_INFO(logger,"strTimestamp="<<strTimestamp);
    string Message = strTimestamp + "GET" + method_url /*+ parameters*/;
    /*if(is_account){
        Message = "GET" + method_url;
    }*/
    KF_LOG_INFO(logger,"Message="<<Message);
    unsigned char* signature = hmac_sha256_byte(unit.secret_key.c_str(), Message.c_str());
    std::string sign = base64_encode(signature, 32);
    string url = unit.baseUrl + method_url;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    auto response = cpr::Get(Url{url}, Header{{"OK-ACCESS-KEY", unit.api_key}, 
                                        {"OK-ACCESS-SIGN", sign},
                                        {"OK-ACCESS-TIMESTAMP", strTimestamp},
                                        {"OK-ACCESS-PASSPHRASE", unit.passphrase},
                                        {"Content-Type", "application/json"}},
                                        Timeout{30000});
    lock.unlock();
    //if(response.text.length()<500){
        KF_LOG_INFO(logger, "[GET] (url) " << url <<" (body) "<< body<< " (response.status_code) " << response.status_code <<
                                        " (response.error.message) " << response.error.message <<
                                        " (response.text) " << response.text.c_str());
    //}
    return response;
}

cpr::Response TDEngineOkex::Post(const std::string& method_url,const std::string& body, AccountUnitOkex& unit)
{
    /*std::string time_url = unit.baseUrl + "/api/general/v3/time";
    const auto res = cpr::Get(Url{time_url},Timeout{10000} );
    KF_LOG_INFO(logger, "[get] (url) " << time_url << " (res.status_code) " << res.status_code <<
                                                " (res.error.message) " << res.error.message <<
                                               " (res.text) " << res.text.c_str());
    Document json;
    json.Parse(res.text.c_str());
    std::string strTimestamp = json["iso"].GetString();*/

    std::string strTimestamp = getOkexTime_iso();
    KF_LOG_INFO(logger,"strTimestamp="<<strTimestamp);
    string Message = strTimestamp + "POST" +method_url + body;
    //std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
    unsigned char* signature = hmac_sha256_byte(unit.secret_key.c_str(), Message.c_str());
    std::string sign = base64_encode(signature, 32);
    string url = unit.baseUrl + method_url;
    std::unique_lock<std::mutex> lock(g_httpMutex);
    auto response = cpr::Post(Url{url}, Header{{"OK-ACCESS-KEY", unit.api_key}, 
                                        {"OK-ACCESS-SIGN", sign},
                                        {"OK-ACCESS-TIMESTAMP", strTimestamp},
                                        {"OK-ACCESS-PASSPHRASE", unit.passphrase},
                                        {"Content-Type", "application/json"}},
                            Body{body}, Timeout{30000});
    lock.unlock();
    if(response.text.length()<500){
        KF_LOG_INFO(logger, "[POST] (url) " << url <<" (body) "<< body<< " (response.status_code) " << response.status_code <<
                                        " (response.error.message) " << response.error.message <<
                                        " (response.text) " << response.text.c_str());
    }
    return response;
}
void TDEngineOkex::init()
{
    genUniqueKey();
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineOkex::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineOkex::resize_accounts(int account_num)
{
    //account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineOkex::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    //string api_key = j_config["APIKey"].get<string>();
    //string secret_key = j_config["SecretKey"].get<string>();
    
    if(j_config.find("is_margin") != j_config.end()) {
        isMargin = j_config["is_margin"].get<bool>();
    }
    //https://api.Okex.pro
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
        int leverage = j_config["lever_rate"].get<int>();
        lever_rate = std::to_string(leverage);
    }
    if(j_config.find("trade_password") != j_config.end()) {
        trade_password = j_config["trade_password"].get<string>();
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
    if(j_config.find("retry_interval_milliseconds") != j_config.end()) {
        retry_interval_milliseconds = j_config["retry_interval_milliseconds"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (retry_interval_milliseconds)" << retry_interval_milliseconds);

    if(j_config.find("withdraw_fee") != j_config.end()) {
        json j_fee = j_config["withdraw_fee"].get<json>();
        if(j_fee.is_object())
        {
            for (json::iterator it = j_fee.begin(); it != j_fee.end(); ++it)
            {
                std::string currency = it.key();
                std::string fee = it.value();
                KF_LOG_INFO(logger,"currency:"<<currency<<" fee:"<<fee);
                fee_map.insert(make_pair(currency, fee));
            }
        }
    } 
    /*"withdraw_fee":{
                    "btc": "0.0004",
                    "eth": "0.1"
                },*/
   

    InitTime(baseUrl);

    //多账户
    auto iter = j_config.find("users");
    if (iter != j_config.end() && iter.value().size() > 0)
    {
        for (auto& j_account: iter.value())
        {
            AccountUnitOkex unit;

            unit.api_key = j_account["APIKey"].get<string>();
            unit.secret_key = j_account["SecretKey"].get<string>();
            unit.passphrase = j_account["passphrase"].get<string>();
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
                KF_LOG_ERROR(logger, "TDEngineBinance::load_account: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
                KF_LOG_ERROR(logger, "\"whiteLists\":{");
                KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
                KF_LOG_ERROR(logger, "    \"btc_usdt\": \"BTCUSDT\",");
                KF_LOG_ERROR(logger, "     \"etc_eth\": \"ETCETH\"");
                KF_LOG_ERROR(logger, "},");
            }
            else
            {
                Document doc;
                get_fee(unit, doc);
                
                for(auto it = unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); it != unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); it++){
                    Document json;
                    get_open_order(unit, it->second, "", json);
                    if(json.IsArray()){
                        int len = json.Size();
                        for(int i=0; i<len; i++){
                            std::string orderId = json.GetArray()[i]["order_id"].GetString();
                            Document d;
                            cancel_order(unit,it->second,orderId,d);
                        }
                    }
                    
                    if(isMargin){
                        //get_leverage(unit, it->second);
                        set_leverage(unit, it->second);
                        //get_leverage(unit, it->second);
                    }
                }
            }
            account_units.emplace_back(unit);
          
        }

    }
    else
    {
        KF_LOG_ERROR(logger, "[load_account] no tarde account info !");
    }
    genUniqueKey();
    //InitTime(baseUrl);
    getPrecision(baseUrl);
    // set up
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, account_units[0].api_key.c_str(), 16);
    strncpy(account.Password, account_units[0].secret_key.c_str(), 21);
    //web socket登陆
    login(0);
    return account;
}

void TDEngineOkex::getPrecision(std::string baseurl)
{
    KF_LOG_INFO(logger,"[getPrecision]");
    std::string url = baseurl + "/api/spot/v3/instruments";
    const auto response = cpr::Get(Url{url}, Timeout{10000} );
    KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
                                                " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
    Document json;
    json.Parse(response.text.c_str());
    
    if(json.IsArray()){
        int size = json.Size();
        for(int i=0; i<size ;i++){
            std::string name = json.GetArray()[i]["instrument_id"].GetString();     
            Precision precision;
            precision.min_size = round(stod(json.GetArray()[i]["min_size"].GetString()) * scale_offset);
            precision.lot_size = round(stod(json.GetArray()[i]["size_increment"].GetString()) * scale_offset);
            precision.price_filter = round(stod(json.GetArray()[i]["tick_size"].GetString()) * scale_offset);
            precision_map.insert(make_pair(name, precision));
        }
    }
    KF_LOG_INFO(logger,"precision_map.size="<<precision_map.size());
}

void TDEngineOkex::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitOkex& unit = account_units[idx];
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

size_t current_account_idx = -1;
AccountUnitOkex& TDEngineOkex::get_current_account()
{
    current_account_idx++;
    current_account_idx %= account_units.size();
    return account_units[current_account_idx];
}

//{"op":"login","args":["<api_key>","<passphrase>","<timestamp>","<sign>"]}
void TDEngineOkex::okexAuth(AccountUnitOkex& unit){
    KF_LOG_INFO(logger, "[okexAuth] auth");

    /*std::string time_url = "https://www.okex.com/api/general/v3/time";
    const auto res = cpr::Get(Url{time_url},Timeout{10000} );
    KF_LOG_INFO(logger, "[get] (url) " << time_url << " (res.status_code) " << res.status_code <<
                                                " (res.error.message) " << res.error.message <<
                                               " (res.text) " << res.text.c_str());
    Document json;
    json.Parse(res.text.c_str());
    std::string strTimestamp = json["epoch"].GetString();*/
    std::string strTimestamp = getOkexTime_epoch();
    KF_LOG_INFO(logger,"strTimestamp="<<strTimestamp);

    //string Message = strTimestamp + "GET" +method_url + body;
    std::string method_url = "/users/self/verify";
    string Message = strTimestamp + "GET" +method_url /*+ body*/;
    //std::string signature =  hmac_sha256( unit.secret_key.c_str(), Message.c_str() );
    unsigned char* signature = hmac_sha256_byte(unit.secret_key.c_str(), Message.c_str());
    std::string sign = base64_encode(signature, 32);

    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("op");
    writer.String("login");
    writer.Key("args");
    writer.StartArray();
    writer.String(unit.api_key.c_str());
    writer.String(unit.passphrase.c_str());
    writer.String(strTimestamp.c_str());
    writer.String(sign.c_str());
    writer.EndArray();
    writer.EndObject();
    std::string strSubscribe = sbUpdate.GetString();
    unsigned char msg[1024];
    memset(&msg[LWS_PRE], 0, 1024-LWS_PRE);
    int length = strSubscribe.length();
    KF_LOG_INFO(logger, "[OkexAuth] auth data " << strSubscribe.c_str() << " ,len = " << length);
    unit.sendmessage.push(strSubscribe);
    lws_callback_on_writable(unit.webSocketConn);
    KF_LOG_INFO(logger, "[OkexAuth] auth success...");
}
void TDEngineOkex::lws_login(AccountUnitOkex& unit, long timeout_nsec){
    KF_LOG_INFO(logger, "[TDEngineOkex::lws_login]");
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
    KF_LOG_INFO(logger, "[TDEngineOkex::lws_login] context created.");


    if (unit.context == NULL) {
        KF_LOG_ERROR(logger, "[TDEngineOkex::lws_login] context is NULL. return");
        return;
    }

    // Set up the client creation info
    static std::string host  = "real.okex.com";
    static std::string path = "/ws/v3";
    clientConnectInfo.address = host.c_str();
    clientConnectInfo.path = path.c_str(); // Set the info's path to the fixed up url path
    
    clientConnectInfo.context = unit.context;
    clientConnectInfo.port = 8443;
    clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    clientConnectInfo.host = host.c_str();
    clientConnectInfo.origin = "origin";
    clientConnectInfo.ietf_version_or_minus_one = -1;
    clientConnectInfo.protocol = protocols[0].name;
    std::unique_lock<std::mutex> web_socket_connect_mutex(* mutex_web_connect);
    clientConnectInfo.pwsi = &unit.webSocketConn;

    KF_LOG_INFO(logger, "[TDEngineOkex::login] address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);
    //建立websocket连接
    
    unit.webSocketConn = lws_client_connect_via_info(&clientConnectInfo);
    if (unit.webSocketConn == NULL) {  
        KF_LOG_ERROR(logger, "[TDEngineOkex::lws_login] wsi create error.");
        return;
    }
    KF_LOG_ERROR(logger, "[TDEngineOkex::lws_login] unit.webSocketConn." << unit.webSocketConn);
    KF_LOG_INFO(logger, "[TDEngineOkex::login] wsi create success.");
    web_socket_connect_mutex.unlock();
}
void TDEngineOkex::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[TDEngineOkex::login]");
    connect(timeout_nsec);
}

void TDEngineOkex::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineOkex::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineOkex::is_logged_in() const{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineOkex::is_connected() const{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}


std::string TDEngineOkex::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "buy";
    } else if (LF_CHAR_Sell == input) {
        return "sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineOkex::GetDirection(std::string input) {
    if ("buy-limit" == input || "buy-market" == input) {
        return LF_CHAR_Buy;
    } else if ("sell-limit" == input || "sell-market" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineOkex::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "market";
    } else {
        return "";
    }
}

LfOrderPriceTypeType TDEngineOkex::GetPriceType(std::string input) {
    if ("buy-limit" == input||"sell-limit" == input) {
        return LF_CHAR_LimitPrice;
    } else if ("buy-market" == input||"sell-market" == input) {
        return LF_CHAR_AnyPrice;
    } else {
        return '0';
    }
}
//订单状态，submitting , submitted 已提交, partial-filled 部分成交, partial-canceled 部分成交撤销, filled 完全成交, canceled 已撤销
LfOrderStatusType TDEngineOkex::GetOrderStatus(std::string state) {

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
  std::string TDEngineOkex::GetTimeInForce(const LfTimeConditionType& input) {
    if (LF_CHAR_IOC == input || LF_CHAR_FAK == input) {
      return "IOC";
    } else if (LF_CHAR_FOK == input) {
      return "FOK";
    } else {
      return "";
    }
  }
/**
 * req functions
 * 查询账户持仓
 */
void TDEngineOkex::req_investor_position(const LFQryPositionField* data, int account_index, int requestId){
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitOkex& unit = account_units[account_index];
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
    //if( !strcmp(data->BrokerID, "master-spot") || !strcmp(data->BrokerID, "master-margin"))
    //{
        get_account(unit, d);
        KF_LOG_INFO(logger, "[req_investor_position] get_account");
    //}
    /*else if( !strcmp(data->BrokerID, "sub-spot") || !strcmp(data->BrokerID, "sub-margin"))
    {
        //get_sub_account(data->InvestorID, unit, d);
        KF_LOG_INFO(logger, "[req_investor_position] get_sub_account");
    }*/
    if(d.IsObject() && d.HasMember("error_message"))
    {
        //auto& error = d["error"];
        errorId =  std::stoi(d["error_code"].GetString());
        errorMsg = d["error_message"].GetString();
        KF_LOG_ERROR(logger, "[req_investor_position] failed!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_OKEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_OKEX, 1, requestId);

    std::map<std::string,LFRspPositionField> tmp_map;
    if(!d.HasParseError())
    {
        if(!isMargin){
            size_t len = d.Size();
            KF_LOG_INFO(logger, "[req_investor_position] (accounts.length)" << len);
            for(size_t i = 0; i < len; i++)
            {
                std::string symbol = d.GetArray()[i]["currency"].GetString();
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol);
                std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
                KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (ticker) " << ticker);
                if(ticker.length() > 0) {            
                    uint64_t nPosition = std::round(std::stod(d.GetArray()[i]["available"].GetString()) * scale_offset);   
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
        }else{
            int len = d.Size();
            for(int i=0; i<len; i++){
                Value node = d.GetArray()[i].GetObject();
                for(rapidjson::Value::ConstMemberIterator itr = node.MemberBegin();itr != node.MemberEnd(); ++itr){
                    std::string total = itr->name.GetString();
                    KF_LOG_INFO(logger,"total="<<total);
                    char_64 tickerchar;
                    strcpy(tickerchar,total.c_str());
                    std::string symbol = total.erase(0,9);
                    std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
                    KF_LOG_INFO(logger,"ticker:"<<ticker);

                    if(ticker.length() > 0) {            
                        uint64_t nPosition = std::round(std::stod(node[tickerchar]["available"].GetString()) * scale_offset);   
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

void TDEngineOkex::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineOkex::dealPriceVolume(AccountUnitOkex& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,
            std::string& nDealPrice,std::string& nDealVolume,bool isbuy){
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

    nDealPrice = std::to_string(price);
    nDealVolume = std::to_string(volume);
}
//多线程发单
void TDEngineOkex::send_order_thread(AccountUnitOkex* unit,string ticker,const LFInputOrderField data,int requestId,int errorId,std::string errorMsg)
{
    KF_LOG_DEBUG(logger, "[send_order_thread] current thread is:" <<std::this_thread::get_id()<<" current CPU is  "<<sched_getcpu());
    bool isbuy;
    if(GetSide(data.Direction) == "buy"){
        isbuy = true;
    }else{
        isbuy = false;
    }
    Document d;
    std::string fixedPrice;
    std::string fixedVolume;
    //dealnum(std::to_string(data.LimitPrice),fixedPrice);
    //dealnum(std::to_string(data.Volume),fixedVolume);
    dealPriceVolume(*unit,data.InstrumentID,data.LimitPrice,data.Volume,fixedPrice,fixedVolume,isbuy);
    KF_LOG_INFO(logger,"fixedPrice="<<fixedPrice<<" fixedVolume="<<fixedVolume);
    double expect_price = data.ExpectPrice * 1.0 / scale_offset;
    double cost = expect_price * stod(fixedVolume);
    std::string fixedCost = std::to_string(cost);
    KF_LOG_INFO(logger,"fixedCost="<<fixedCost);

    if(fixedVolume == "0"){
        KF_LOG_DEBUG(logger, "[req_order_insert] fixed Volume error (no ticker)" << ticker);
        errorId = 200;
        errorMsg = data.InstrumentID;
        errorMsg += " : no such ticker";
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_OKEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_INFO(logger,"[req_order_insert] cys_ticker "<<ticker.c_str());

                std::string tonce = genClinetid(data.OrderRef);
                tonce = "fa" + tonce;
                LFRtnOrderField rtn_order;
                memset(&rtn_order, 0, sizeof(LFRtnOrderField));
                //strncpy(rtn_order.BusinessUnit,remoteOrderId.c_str(),21);
                rtn_order.OrderStatus = LF_CHAR_Unknown;
                rtn_order.VolumeTraded = 0;
                
                strcpy(rtn_order.ExchangeID, "okex");
                strncpy(rtn_order.UserID, unit->api_key.c_str(), 16);
                strncpy(rtn_order.InstrumentID, data.InstrumentID, 31);
                rtn_order.Direction = data.Direction;
                //No this setting on Okex
                rtn_order.TimeCondition = LF_CHAR_GTC;
                rtn_order.OrderPriceType = data.OrderPriceType;
                strncpy(rtn_order.OrderRef, data.OrderRef, 13);
                rtn_order.VolumeTotalOriginal = round(stod(fixedVolume) * scale_offset);
                rtn_order.LimitPrice = round(stod(fixedPrice) * scale_offset);
                rtn_order.VolumeTotal = round(stod(fixedVolume) * scale_offset);
                std::unique_lock<std::mutex> lck(order_mutex);
                order_map.insert(make_pair(tonce, rtn_order));
                lck.unlock();
    
    send_order(*unit, ticker.c_str(),tonce, GetSide(data.Direction).c_str(),GetType(data.OrderPriceType).c_str(), fixedVolume, fixedPrice,fixedCost,is_post_only(&data),GetTimeInForce(data.TimeCondition), d);
    
    if(!d.IsObject()){
        errorId = 100;
        errorMsg = "send_order http response has parse error or is not json. please check the log";
        KF_LOG_ERROR(logger, "[req_order_insert] send_order error!  (rid)" << requestId << " (errorId)" <<
                                                                           errorId << " (errorMsg) " << errorMsg);
    } 
    else
    {
        bool result = false;
        if(d.HasMember("result")){
            result = d["result"].GetBool();
        }
        if(result){
            std::string remoteOrderId = d["order_id"].GetString();
            std::string client_oid = d["client_oid"].GetString();
            lck.lock();
            auto it = order_map.find(client_oid);
            if(it != order_map.end()){
                localOrderRefRemoteOrderId[std::string(data.OrderRef)] = remoteOrderId;
                KF_LOG_INFO(logger, "[req_order_insert] after send  (rid)" << requestId << " (OrderRef) " <<
                                                                           data.OrderRef << " (remoteOrderId) "
                                                                           << remoteOrderId);

                /*LFRtnOrderField rtn_order;
                memset(&rtn_order, 0, sizeof(LFRtnOrderField));
                strncpy(rtn_order.BusinessUnit,remoteOrderId.c_str(),21);
                rtn_order.OrderStatus = LF_CHAR_NotTouched;
                rtn_order.VolumeTraded = 0;
                
                strcpy(rtn_order.ExchangeID, "okex");
                strncpy(rtn_order.UserID, unit->api_key.c_str(), 16);
                strncpy(rtn_order.InstrumentID, data.InstrumentID, 31);
                rtn_order.Direction = data.Direction;
                //No this setting on Okex
                rtn_order.TimeCondition = LF_CHAR_GTC;
                rtn_order.OrderPriceType = data.OrderPriceType;
                strncpy(rtn_order.OrderRef, data.OrderRef, 13);
                rtn_order.VolumeTotalOriginal = round(stod(fixedVolume) * scale_offset);
                rtn_order.LimitPrice = round(stod(fixedPrice) * scale_offset);
                rtn_order.VolumeTotal = round(stod(fixedVolume) * scale_offset);*/
                it->second.OrderStatus = LF_CHAR_NotTouched;
                on_rtn_order(&it->second);
                raw_writer->write_frame(&it->second, sizeof(LFRtnOrderField),
                                        source_id, MSG_TYPE_LF_RTN_ORDER_OKEX,
                                        1, (it->second.RequestID > 0) ? it->second.RequestID : -1);
                //KF_LOG_INFO(logger,"insert");
                //order_map.insert(make_pair(remoteOrderId, rtn_order));
                LFRtnOrderField lfrtnorder = it->second;
                order_map.erase(it);
                order_map.insert(make_pair(remoteOrderId, lfrtnorder));
                //KF_LOG_INFO(logger,"erase");
            }
            lck.unlock();
            return;
            
        }else{
            errorId = stoi(d["error_code"].GetString());
            errorMsg = d["error_message"].GetString();
        }

    }
    
    //unlock
    if(errorId != 0)
    {
        on_rsp_order_insert(&data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_OKEX, 1, requestId, errorId, errorMsg.c_str());
    }


}

//发单
void TDEngineOkex::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time){

    AccountUnitOkex& unit = get_current_account();
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_OKEX, 1/*ISLAST*/, requestId);

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
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_OKEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);
   
    if(nullptr == m_ThreadPoolPtr)
    {
        send_order_thread(&unit,ticker,*data,requestId,errorId,errorMsg);
        
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&TDEngineOkex::send_order_thread,this,&unit,ticker,*data,requestId,errorId,errorMsg)); 
        KF_LOG_DEBUG(logger, "[req_order_insert] [left thread count ]: ] "<< m_ThreadPoolPtr->idlCount());
    }

}




void TDEngineOkex::action_order_thread(AccountUnitOkex* unit,string ticker,const LFOrderActionField data,int requestId,std::string remoteOrderId,int errorId,std::string errorMsg)
{
    addRemoteOrderIdOrderActionSentTime(&data,requestId,remoteOrderId,*unit,ticker);
    Document d;
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
        raw_writer->write_error_frame(&data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_OKEX, 1, 
            requestId, errorId, errorMsg.c_str());

    } else {
        KF_LOG_INFO(logger,"req success");
    }

}

void TDEngineOkex::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time){
    
    int errorId = 0;
    std::string errorMsg = "";
    
    AccountUnitOkex& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_OKEX, 1, requestId);
    
   
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_OKEX, 1, requestId, errorId, errorMsg.c_str());
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
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_OKEX, 1, requestId, errorId, errorMsg.c_str());
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
        m_ThreadPoolPtr->commit(std::bind(&TDEngineOkex::action_order_thread,this,&unit,ticker,*data,requestId,remoteOrderId,errorId,errorMsg));
    }

}

void TDEngineOkex::dealnum(string pre_num,string& fix_num)
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

//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
void TDEngineOkex::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId,AccountUnitOkex unit,string ticker){
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;
    newOrderActionSent.sentNameTime = getTimestamp();
    newOrderActionSent.unit = unit;
    newOrderActionSent.ticker = ticker;
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    remoteOrderIdOrderActionSentTime[remoteOrderId] = newOrderActionSent;
}

void TDEngineOkex::req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw)
{
    KF_LOG_INFO(logger, "[req_transfer_history]");
    AccountUnitOkex& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_transfer_history] (api_key)" << unit.api_key);

    LFTransferHistoryField his;
    memset(&his, 0, sizeof(LFTransferHistoryField));
    strncpy(his.UserID, data->UserID, 64);
    strncpy(his.ExchangeID, "okex", 11);

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
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_OKEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    
    //string asset = data->Currency;
    //string startTime = data->StartTime;
    //string endTime = data->EndTime;
    //std::string type;
    Document d;
    get_transfer_history(unit,Currency,d,isWithdraw);

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
    send_writer->write_frame(data, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_OKEX, 1, requestId);*/

    std::vector<LFTransferHistoryField> tmp_vector;
    if(!d.HasParseError() && d.IsArray()){
        int len = d.Size();
        for ( int i = 0 ; i < len ; i++ ) {
            std::string ticker = d.GetArray()[i]["currency"].GetString();
            ticker = unit.positionWhiteList.GetKeyByValue(ticker);
            if(ticker.empty())
                continue;
            strcpy(his.Currency, ticker.c_str());
            //std::string tag = d.GetArray()[i]["address-tag"].GetString();
            //strncpy(his.Tag, tag.c_str(), 64);
            
            std::string timestamp = d.GetArray()[i]["timestamp"].GetString();
            //std::string updatetime = std::to_string(updatedAt);
            strncpy(his.TimeStamp, timestamp.c_str(), 32);
            
            std::string status = d.GetArray()[i]["status"].GetString();
            his.Status = get_transfer_status(status,isWithdraw);
            
            his.Volume = std::round(stod(d.GetArray()[i]["amount"].GetString()) * scale_offset);
            std::string from = d.GetArray()[i]["from"].GetString();
            strncpy(his.FromID, from.c_str(), 64);
            std::string address = d.GetArray()[i]["to"].GetString();
            strncpy(his.Address, address.c_str(), 64);
            his.IsWithdraw = isWithdraw;

            //strncpy(his.Tag, d["depositList"].GetArray()[i]["addressTag"].GetString(), 64);
            std::string walletTxId = d.GetArray()[i]["txid"].GetString();
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
    else if(d.IsObject() && d.HasMember("error_message") && d["error_message"].IsString())
    {
        errorId = 104;
        errorMsg = d["error_message"].GetString();
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
        raw_writer->write_error_frame(&his, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_OKEX, 1, requestId, errorId, errorMsg.c_str());
    }
}

int64_t TDEngineOkex::formatISO8601_to_timestamp(std::string time)
{
    //extern long timezone;  
    int year, month, day, hour, min, sec, millsec;
    sscanf(time.c_str(), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", &year, &month, &day, &hour, &min, &sec, &millsec);
    tm utc_time{};
    utc_time.tm_year = year - 1900;
    utc_time.tm_mon = month - 1;
    utc_time.tm_mday = day;
    utc_time.tm_hour = hour;
    utc_time.tm_min = min;
    utc_time.tm_sec = sec;
    time_t timet = mktime(&utc_time);
    tzset();
    KF_LOG_DEBUG_FMT(logger, "formatISO8601_to_timestamp timezone:%lld", timezone);
    return (timet - timezone) * 1000 + millsec;
}

void TDEngineOkex::req_get_kline_via_rest(const GetKlineViaRest* data, int account_index, int requestId, long rcv_time)
{
    // /api/spot        BTC-USDT
    // /api/futures     EOS-USD-190628
    // /api/swap        BTC-USD-SWAP
    // /api/option      BTC-USD-190927-12500-C
    KF_LOG_INFO(logger, "TDEngineOkex::req_get_kline_via_rest: (symbol)" << data->Symbol << " (interval)" << data->Interval);
    writer->write_frame(data, sizeof(GetKlineViaRest), source_id, MSG_TYPE_LF_GET_KLINE_VIA_REST, 1/*islast*/, requestId);

    AccountUnitOkex& unit = account_units[account_index];
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(data->Symbol);
    if (ticker.empty())
    {
        KF_LOG_INFO(logger, "symbol not in white list");
        return;
    }

    string url = unit.baseUrl + "/api/spot/v3/instruments/<instrument_id>/history/candles";
    int param_limit = data->Limit;
    if (param_limit > 300)
        param_limit = 300;
    else if (param_limit < 1)
        param_limit = 1;

    cpr::Response response;
    if (data->IgnoreStartTime)
        response = cpr::Get(Url{ url }, Parameters{ {"instrument_id", ticker},{"granularity", data->Interval},{"limit", to_string(param_limit)} });
    else
        response = cpr::Get(Url{ url }, Parameters{ {"instrument_id", ticker},{"granularity", data->Interval},{"limit", to_string(param_limit)},
            {"start", to_string(data->StartTime)}, {"end", to_string(data->EndTime)} });
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

    // [["2019-03-19T16:00:00.000Z", "3997.3", "4031.9", "3982.5", "3998.7", "26175.21141385"]]
    //  [timestamp,open,high,low,close,volume]
    KF_LOG_INFO(logger, "TDEngineOkex::req_get_kline_via_rest: parse response" << response.text.c_str());

    Document d;
    d.Parse(response.text.c_str());

    if (d.IsArray()) {
        LFBarSerial1000Field bars;
        memset(&bars, 0, sizeof(bars));
        strncpy(bars.InstrumentID, data->Symbol, 31);
        strcpy(bars.ExchangeID, "okex");

        for (int i = 0; i < d.Size(); i++) {
            if (!d[i].IsArray()) {
                KF_LOG_INFO(logger, "TDEngineOkex::req_get_kline_via_rest: response is abnormal" << response.text.c_str());
                break;
            }

            bars.BarSerial[i].StartUpdateMillisec = formatISO8601_to_timestamp(d[i][0].GetString());
            bars.BarSerial[i].PeriodMillisec      = atoll(data->Interval);
            bars.BarSerial[i].EndUpdateMillisec   = bars.BarSerial[i].StartUpdateMillisec + bars.BarSerial[i].PeriodMillisec;

            //scale_offset = 1e8
            bars.BarSerial[i].Open = std::round(std::stod(d[i][1].GetString()) * scale_offset);
            bars.BarSerial[i].Close = std::round(std::stod(d[i][4].GetString()) * scale_offset);
            bars.BarSerial[i].Low = std::round(std::stod(d[i][3].GetString()) * scale_offset);
            bars.BarSerial[i].High = std::round(std::stod(d[i][2].GetString()) * scale_offset);
            bars.BarSerial[i].Volume = std::round(std::stod(d[i][5].GetString()) * scale_offset);

            bars.BarLevel = i + 1;
        }
        on_bar_serial1000(&bars, data->RequestID);
    }
    else if (!d.IsArray()) {
        KF_LOG_INFO(logger, "TDEngineOkex::req_get_kline_via_rest: response is abnormal");
    }
}

int TDEngineOkex::get_transfer_status(std::string status,bool is_withdraw)
{
    int local_status =LFTransferStatus::TRANSFER_STRATUS_SUCCESS;
    //if(!isWithdraw){
        if(status == "0" || status == "1"|| status == "3"|| status == "4"|| status == "5")
        {
            local_status = LFTransferStatus::TRANSFER_STRATUS_PROCESSING;
        }
    //}
    
    return local_status;
}
void TDEngineOkex::req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId)
{
    AccountUnitOkex& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_withdraw_currency]" << " (rid) " << requestId
                                              << " (APIKey) " << unit.api_key
                                              << " (Currency) " << data->Currency
                                              << " (Volume) " << data->Volume
                                              << " (Address) " << data->Address
                                              << " (Tag) " << data->Tag);
    send_writer->write_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_OKEX, 1, requestId);
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
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_OKEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_INFO(logger, "[req_withdraw_currency] (exchange_currency)" << currency);

    string address = data->Address, tag = data->Tag;
    if(address == ""){
        errorId = 100;
        errorMsg = "address/tag is null";
        KF_LOG_ERROR(logger,"[req_withdraw_currency] address is null");
        //on_withdraw(data, requestId, errorId, errorMsg.c_str());
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_OKEX, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }else if(trade_password == ""){
        errorId = 100;
        errorMsg = "trade_password is null";
        KF_LOG_ERROR(logger,"[req_withdraw_currency] trade_password is null");
        
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_OKEX, 1, 
            requestId, errorId, errorMsg.c_str());
        return;        
    }

    string volume = to_string(data->Volume);
    string fixvolume;
    dealnum(volume,fixvolume);
    std::string fee = "";
    auto it = fee_map.find(string(data->Currency));
    if(it != fee_map.end()){
        fee = it->second;
    }else{
        errorId = 100;
        errorMsg = "fee is null";
        KF_LOG_ERROR(logger,"[req_withdraw_currency] fee is null");
        
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_OKEX, 1, 
            requestId, errorId, errorMsg.c_str());
        return;                
    }

    std::string to_address = address;
    if(tag != ""){
        to_address = address + ":" + tag;
    }
    Document json;
    withdrawl_currency(unit, currency, to_address, fixvolume, fee, json);
    if (json.HasParseError() || !json.IsObject()){
        errorId = 101;
        errorMsg = "json has parse error.";
        KF_LOG_ERROR(logger,"[withdrawl_currency] json has parse error.");
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_OKEX, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }
    if(json.HasMember("result") && json["result"].GetBool()){
        std::string id = json["withdrawal_id"].GetString();
        LFWithdrawField* dataCpy = (LFWithdrawField*)data;
        strncpy(dataCpy->ID,id.c_str(),64);
        KF_LOG_INFO(logger, "[withdrawl_currency] withdrawl success. no error message");
        on_rsp_withdraw(dataCpy,requestId,errorId,errorMsg.c_str());
    }else if(json.HasMember("error_message") && json["error_message"].IsString()){
        errorId = 102;
        errorMsg = json["error_message"].GetString();
        KF_LOG_INFO(logger,"error_message:"<<errorMsg);
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_OKEX, 1, 
            requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineOkex::req_inner_transfer(const LFTransferField* data, int account_index, int requestId)
{
    AccountUnitOkex& unit = account_units[account_index];
    LFTransferField* transfer_data = (LFTransferField*) data;
    KF_LOG_DEBUG(logger, "[req_inner_transfer]" << " (rid) " << requestId
                                              << " (APIKey) " << unit.api_key
                                              << " (from) " << data->From
                                              << " (Currency) " << data->Currency
                                              << " (Volume) " << data->Volume );
    send_writer->write_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_OKEX, 1, requestId);
    int errorId = 0;
    std::string errorMsg = "";
    Document json;
    std::string type = ""; std::string from = ""; std::string to = ""; std::string sub_account;
    std::string Currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
    if(Currency.length() == 0) {
        errorId = 200;
        errorMsg = "Currency not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_inner_transfer]: Currency not in WhiteList , ignore it. (rid)" << requestId << " (errorId)" <<
                                                                                      errorId << " (errorMsg) " << errorMsg);
        on_rsp_transfer(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_OKEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    std::string volume = std::to_string(data->Volume);
    std::string fixvolume;
    dealnum(volume,fixvolume);
    if ( !strcmp(data->From, "master-spot") &&!strcmp(data->To, "master-margin"))
    {
        type = "0"; from = "1"; to = "5";
    }
    else if( (!strcmp(data->From, "master-margin") &&!strcmp(data->To, "master-spot")) )
    {
        type = "0"; from = "5"; to = "1";
    }
    else if( (!strcmp(data->From, "sub-spot") &&!strcmp(data->To, "sub-margin")) )
    {
        type = "0"; from = "1"; to = "5";
    }
    else if( (!strcmp(data->From, "sub-margin") &&!strcmp(data->To, "sub-spot")) )
    {
        type = "0"; from = "5"; to = "1";
    }
    else if( (!strcmp(data->From, "sub-spot") &&!strcmp(data->To, "master-spot")) )
    {
        type = "2"; from = "1"; to = "1"; sub_account = string(data->FromName);
    }
    else if( (!strcmp(data->From, "master-spot") &&!strcmp(data->To, "sub-spot")) )
    {
        type = "1"; from = "1"; to = "1"; sub_account = string(data->ToName);
    }
    else if( (!strcmp(data->From, "master-margin") &&!strcmp(data->To, "sub-margin")) )
    {
        type = "1"; from = "5"; to = "5"; sub_account = string(data->ToName);
    }
    else if( (!strcmp(data->From, "sub-margin") &&!strcmp(data->To, "master-margin")) )
    {
        type = "2"; from = "5"; to = "5"; sub_account = string(data->FromName);
    }//margin
    else if ( !strcmp(data->From, "master-spot") &&!strcmp(data->To, "master-futures"))
    {
        type = "0"; from = "1"; to = "3";
    }
    else if( (!strcmp(data->From, "master-futures") &&!strcmp(data->To, "master-spot")) )
    {
        type = "0"; from = "3"; to = "1";
    }
    else if( (!strcmp(data->From, "sub-spot") &&!strcmp(data->To, "sub-futures")) )
    {
        type = "0"; from = "1"; to = "3";
    }
    else if( (!strcmp(data->From, "sub-futures") &&!strcmp(data->To, "sub-spot")) )
    {
        type = "0"; from = "3"; to = "1";
    }
    else if( (!strcmp(data->From, "master-futures") &&!strcmp(data->To, "sub-futures")) )
    {
        type = "1"; from = "3"; to = "3"; sub_account = string(data->ToName);
    }
    else if( (!strcmp(data->From, "sub-futures") &&!strcmp(data->To, "master-futures")) )
    {
        type = "2"; from = "3"; to = "3"; sub_account = string(data->FromName);
    }//futures
    else if ( !strcmp(data->From, "master-spot") &&!strcmp(data->To, "master-swap"))
    {
        type = "0"; from = "1"; to = "9";
    }
    else if( (!strcmp(data->From, "master-swap") &&!strcmp(data->To, "master-spot")) )
    {
        type = "0"; from = "9"; to = "1";
    }
    else if( (!strcmp(data->From, "sub-spot") &&!strcmp(data->To, "sub-swap")) )
    {
        type = "0"; from = "1"; to = "9";
    }
    else if( (!strcmp(data->From, "sub-swap") &&!strcmp(data->To, "sub-spot")) )
    {
        type = "0"; from = "9"; to = "1";
    }
    else if( (!strcmp(data->From, "master-swap") &&!strcmp(data->To, "sub-swap")) )
    {
        type = "1"; from = "9"; to = "9"; sub_account = string(data->ToName);
    }
    else if( (!strcmp(data->From, "sub-swap") &&!strcmp(data->To, "master-swap")) )
    {
        type = "2"; from = "9"; to = "9"; sub_account = string(data->FromName);
    }//swap
    else if ( !strcmp(data->From, "master-spot") &&!strcmp(data->To, "master-option"))
    {
        type = "0"; from = "1"; to = "12";
    }
    else if( (!strcmp(data->From, "master-option") &&!strcmp(data->To, "master-spot")) )
    {
        type = "0"; from = "12"; to = "1";
    }
    else if( (!strcmp(data->From, "sub-spot") &&!strcmp(data->To, "sub-option")) )
    {
        type = "0"; from = "1"; to = "12";
    }
    else if( (!strcmp(data->From, "sub-option") &&!strcmp(data->To, "sub-spot")) )
    {
        type = "0"; from = "12"; to = "1";
    }
    else if( (!strcmp(data->From, "master-option") &&!strcmp(data->To, "sub-option")) )
    {
        type = "1"; from = "12"; to = "12"; sub_account = string(data->ToName);
    }
    else if( (!strcmp(data->From, "sub-option") &&!strcmp(data->To, "master-option")) )
    {
        type = "2"; from = "12"; to = "12"; sub_account = string(data->FromName);
    }//option
    else
    {
        KF_LOG_ERROR(logger,"[req_inner_transfer] error! From or To is error!");
        return ;
    }
    inner_transfer(unit, string(data->Symbol), Currency, fixvolume, type,from,to, sub_account, json);

    if (json.HasParseError() || !json.IsObject()){
        errorId = 200;
        errorMsg = "json has parse error.";
        KF_LOG_ERROR(logger,"[req_inner_transfer] json has parse error.");
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_OKEX, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }
    if(json.HasMember("result") && json["result"].GetBool()){
        std::string transfer_id = json["transfer_id"].GetString();
        strcpy(transfer_data->ID,transfer_id.c_str());
        KF_LOG_INFO(logger, "[req_inner_transfer] inner_transfer success. no error message");
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
    }else if(json.HasMember("error_message") && json["error_message"].IsString()){
        string message = json["error_message"].GetString();
        errorId = 102;
        KF_LOG_INFO(logger, "[req_inner_transfer] inner_transfer faild!");
        errorMsg = message;
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_OKEX, 1, 
            requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineOkex::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineOkex::loopwebsocket");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineOkex::loopwebsocket, this)));

    KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineOkex::loopOrderActionNoResponseTimeOut");
    orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineOkex::loopOrderActionNoResponseTimeOut, this)));


}

void TDEngineOkex::loopwebsocket()
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
        //KF_LOG_INFO(logger, "TDEngineOkex::loop:lws_service");
        for (size_t idx = 0; idx < account_units.size(); idx++)
        {
            AccountUnitOkex& unit = account_units[idx];
            lws_service(unit.context, rest_get_interval_ms );
        }
            
    }
}




void TDEngineOkex::loopOrderActionNoResponseTimeOut()
{
    KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        orderActionNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void TDEngineOkex::orderActionNoResponseTimeOut(){
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
            Document json;
            query_order(itr->second.unit,itr->second.ticker,itr->first,json);
            std::string state = json["state"].GetString();
            if(state == "-1"){
                DealCancel(itr->first);
            }
            else{
                on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
                itr = remoteOrderIdOrderActionSentTime.erase(itr);
            }
        } else {
            ++itr;
        }
    }
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}

void TDEngineOkex::printResponse(const Document& d){
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
}

void TDEngineOkex::getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json)
{
    //if(http_status_code >= HTTP_RESPONSE_OK && http_status_code <= 299)
    //{
        json.Parse(responseText.c_str());
    //} else if(http_status_code == 0)
    /*{
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
    }*/
}

void TDEngineOkex::get_account(AccountUnitOkex& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_account]");

    std::string requestPath="/api/spot/v3/accounts";
    if(isMargin){
        requestPath = "/api/margin/v3/accounts";
    }
    const auto response = Get(requestPath,"",unit,"",false);
    json.Parse(response.text.c_str());
    KF_LOG_INFO(logger, "[get_account] (account info) "<<response.text.c_str());
    return ;
}
/*void TDEngineOkex::getAccountId(AccountUnitOkex& unit){
    KF_LOG_DEBUG(logger,"[getAccountID] ");
    std::string getPath="/v1/account/accounts/";
    const auto resp = Get("/v1/account/accounts","{}",unit,"");
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
}*/
void TDEngineOkex::InitTime(std::string baseUrl)
{
    KF_LOG_INFO(logger,"InitTime");
    std::string time_url = baseUrl + "/api/general/v3/time";
    const auto res = cpr::Get(Url{time_url},Timeout{10000} );
    KF_LOG_INFO(logger, "[get] (url) " << time_url << " (res.status_code) " << res.status_code <<
                                                " (res.error.message) " << res.error.message <<
                                               " (res.text) " << res.text.c_str());
    Document json;
    json.Parse(res.text.c_str());
    std::string strTimestamp = json["epoch"].GetString(); 
    strTimestamp = strTimestamp.substr(0,10) + strTimestamp.substr(11,14);
    int lack_len = 13 - strTimestamp.size();
    for(int i=0; i<lack_len; i++){
        strTimestamp.append("0");
    }    
    int64_t time64 = stoll(strTimestamp);
    int64_t now = getTimestamp();
    check_time = time64 - now;
    KF_LOG_INFO(logger,"check_time="<<check_time);
}
std::string TDEngineOkex::getOkexTime_epoch(){
    int64_t now = getTimestamp() + check_time;
    std::string okextime = std::to_string(now);
    okextime = okextime.substr(0,10) + "." + okextime.substr(10,13);
    return okextime;
}
std::string TDEngineOkex::getOkexTime_iso(){
    int64_t now = getTimestamp() + check_time;
    std::string okextime = timestamp_to_formatISO8601(now);
    return okextime;
}
std::string TDEngineOkex::createInsertOrdertring(std::string cid,std::string type,std::string side,std::string symbol,std::string price,std::string volume,std::string cost,bool is_post_only,std::string tif){
    //std::string client_oid = "fa" + cid;
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("client_oid");
    writer.String(cid.c_str());
    writer.Key("type");
    writer.String(type.c_str());
    writer.Key("side");
    writer.String(side.c_str());
    writer.Key("instrument_id");
    writer.String(symbol.c_str());
    if(type == "limit"){
        writer.Key("price");
        writer.String(price.c_str());
        writer.Key("size");
        writer.String(volume.c_str()); 

        writer.Key("order_type");
        if(is_post_only){
            writer.String("1");
        }else if(tif == "FOK"){
            writer.String("2");
        }else if(tif == "IOC"){
            writer.String("3");
        }else{
            writer.String("0");
        }       
    }else{//market
       if(side == "buy"){
            writer.Key("notional");
            writer.String(cost.c_str());  
       }else{//sell
            writer.Key("size");
            writer.String(volume.c_str());        
       }
    }
    if(isMargin){
        writer.Key("margin_trading");
        writer.String("2");
    }
    writer.EndObject();
    return s.GetString();
}

void TDEngineOkex::send_order(AccountUnitOkex& unit, std::string code, std::string cid,
                                 std::string side, std::string type, std::string volume, std::string price,std::string cost,bool is_post_only,std::string tif, Document& json){
    KF_LOG_INFO(logger, "[send_order] (code) "<<code<<"(price)"<<price<<"(volume)"<<volume);

    int retry_times = 0;
    cpr::Response response;
    bool should_retry = false;

        
        std::string requestPath = "/api/spot/v3/orders";

        if(isMargin){
            requestPath = "/api/margin/v3/orders";
        }
        
        response = Post(requestPath,createInsertOrdertring(cid,type,side,code,price,volume,cost,is_post_only,tif),unit);

        KF_LOG_INFO(logger, "[send_order] (url) " << requestPath << " (response.status_code) " << response.status_code 
                                                  << " (response.error.message) " << response.error.message 
                                                  <<" (response.text) " << response.text.c_str() << " (retry_times)" << retry_times);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);

}

bool TDEngineOkex::shouldRetry(Document& doc)
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

void TDEngineOkex::cancel_all_orders(AccountUnitOkex& unit, std::string code, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_all_orders]");

    std::string requestPath = "/api/spot/v3/cancel_batch_orders";
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartArray();
    writer.StartObject();
    writer.Key("instrument_id");
    writer.String(code.c_str());
    writer.EndObject();
    writer.EndArray();
    auto response = Post(requestPath,s.GetString(),unit);
    getResponse(response.status_code, response.text, response.error.message, json);

    KF_LOG_INFO(logger, "[cancel_all_orders] " << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );    
}

void TDEngineOkex::cancel_order(AccountUnitOkex& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[cancel_order]");

    int retry_times = 0;
    cpr::Response response;

        std::string requestPath="/api/spot/v3/cancel_orders/" + orderId;
        if(isMargin){
            requestPath = "/api/margin/v3/cancel_orders/" + orderId;
        }
        StringBuffer s;
        Writer<StringBuffer> writer(s);
        writer.StartObject();
        writer.Key("instrument_id");
        writer.String(code.c_str());
        writer.EndObject();
        std::string body = s.GetString();
        response = Post(requestPath,body,unit);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);

    KF_LOG_INFO(logger, "[cancel_order] " << retry_times << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );

}

void TDEngineOkex::query_order(AccountUnitOkex& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[query_order]");
    
    std::string getPath = "/api/spot/v3/orders/";
    if(isMargin){
        getPath = "/api/margin/v3/orders/";
    }
    std::string requestPath = getPath + orderId;
    std::string parameters = "?instrument_id=" + code;
    requestPath += parameters; 
    auto response = Get(requestPath,"",unit,parameters,false);
    json.Parse(response.text.c_str());
    //KF_LOG_DEBUG(logger,"[query_order] response "<<response.text.c_str());
    //getResponse(response.status_code, response.text, response.error.message, json);
}

void TDEngineOkex::get_open_order(AccountUnitOkex& unit, std::string code, std::string orderId, Document& json)
{
    KF_LOG_INFO(logger, "[get_open_order]");
    
    std::string getPath = "/api/spot/v3/orders_pending";
    if(isMargin){
        getPath = "/api/margin/v3/orders_pending";
    }
    std::string requestPath = getPath;
    std::string parameters = "?instrument_id=" + code;
    requestPath += parameters; 
    auto response = Get(requestPath,"",unit,parameters,false);
    json.Parse(response.text.c_str());

}

void TDEngineOkex::get_leverage(AccountUnitOkex& unit, std::string code)
{
    KF_LOG_INFO(logger, "[get_leverage]");
    
    std::string getPath = "/api/margin/v3/accounts/" + code + "/leverage";
    
    std::string requestPath = getPath;
    //std::string parameters = "?instrument_id=" + code;
    //requestPath += parameters; 
    auto response = Get(requestPath,"",unit,"",false);
    //json.Parse(response.text.c_str());

}
void TDEngineOkex::get_fee(AccountUnitOkex& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_fee]");
    
    std::string getPath = "/api/account/v3/withdrawal/fee";
    
    std::string requestPath = getPath;
    //std::string parameters = "?instrument_id=" + code;
    //requestPath += parameters; 
    auto response = Get(requestPath,"",unit,"",false);
    json.Parse(response.text.c_str());

}
void TDEngineOkex::set_leverage(AccountUnitOkex& unit, std::string code)
{
    KF_LOG_INFO(logger, "[set_leverage]");
    
    std::string getPath = "/api/margin/v3/accounts/" + code + "/leverage";
    
    std::string requestPath = getPath;
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("leverage");
    writer.String(lever_rate.c_str());
    writer.EndObject();
    std::string body = s.GetString();
    auto response = Post(requestPath,body,unit);

}

void TDEngineOkex::get_transfer_history(AccountUnitOkex& unit, std::string code, Document& json, bool isWithdraw)
{
    KF_LOG_INFO(logger, "[get_transfer_history]");
    
    std::string requestPath;
    if(isWithdraw){
        requestPath = "/api/account/v3/withdrawal/history/" + code;
    }else{
        requestPath = "/api/account/v3/deposit/history/" + code;
    }
    
    auto response = Get(requestPath,"",unit,"",false);
    json.Parse(response.text.c_str());

}

void TDEngineOkex::withdrawl_currency(AccountUnitOkex& unit, std::string code, string address, string amount, string fee,Document& json)
{
    KF_LOG_INFO(logger, "[withdrawl_currency]");
    KF_LOG_INFO(logger," "<<code<<" "<<address<<" "<<amount);

    int retry_times = 0;
    cpr::Response response;

        std::string requestPath="/api/account/v3/withdrawal";
        
        StringBuffer s;
        Writer<StringBuffer> writer(s);
        writer.StartObject();
        writer.Key("currency");
        writer.String(code.c_str());
        writer.Key("amount");
        writer.String(amount.c_str());
        writer.Key("destination");
        writer.String("4");
        writer.Key("to_address");
        writer.String(address.c_str());
        writer.Key("trade_pwd");
        writer.String(trade_password.c_str());
        writer.Key("fee");
        writer.String(fee.c_str());
        writer.EndObject();
        std::string body = s.GetString();
        response = Post(requestPath,body,unit);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);

    KF_LOG_INFO(logger, "[withdrawl_currency] " << retry_times << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );

}

void TDEngineOkex::inner_transfer(AccountUnitOkex& unit, std::string code, string currency, string amount, string type,string from,string to,
                                    string sub_account, Document& json)
{
    KF_LOG_INFO(logger, "[inner_transfer]"<<type<<" "<<from<<" "<<to);
    KF_LOG_INFO(logger," "<<code<<" "<<currency<<" "<<amount<<" "<<sub_account);

    int retry_times = 0;
    cpr::Response response;

        std::string requestPath="/api/account/v3/transfer";
        
        StringBuffer s;
        Writer<StringBuffer> writer(s);
        writer.StartObject();
        writer.Key("currency");
        writer.String(currency.c_str());
        writer.Key("amount");
        writer.String(amount.c_str());
        writer.Key("type");
        writer.String(type.c_str());
        writer.Key("from");
        writer.String(from.c_str());
        writer.Key("to");
        writer.String(to.c_str());
        if(type == "1" || type == "2"){
            writer.Key("sub_account");
            writer.String(sub_account.c_str());
        }
        if(from == "5" || from == "3" || from == "9"){
            writer.Key("instrument_id");
            writer.String(code.c_str());            
        }
        if(to == "5" || to == "3" || to == "9"){
            writer.Key("to_instrument_id");
            writer.String(code.c_str());            
        }
        writer.EndObject();
        std::string body = s.GetString();
        KF_LOG_INFO(logger,"body="<<body);
        response = Post(requestPath,body,unit);

        //json.Clear();
        getResponse(response.status_code, response.text, response.error.message, json);

    KF_LOG_INFO(logger, "[inner_transfer] " << retry_times << " (response.status_code) " << response.status_code <<
                                                    " (response.error.message) " << response.error.message <<
                                                    " (response.text) " << response.text.c_str() );

}

std::string TDEngineOkex::parseJsonToString(Document &d){
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


inline int64_t TDEngineOkex::getTimestamp(){
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}
std::string TDEngineOkex::timestamp_to_formatISO8601(int64_t timestamp)
{
    int ms = timestamp % 1000;
    tm utc_time{};
    time_t time = timestamp/1000;
    gmtime_r(&time, &utc_time);
    char timeStr[50]{};
    sprintf(timeStr, "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", utc_time.tm_year + 1900, utc_time.tm_mon + 1, utc_time.tm_mday, utc_time.tm_hour, utc_time.tm_min, utc_time.tm_sec,ms);
    return std::string(timeStr);
}
void TDEngineOkex::genUniqueKey(){
    struct tm cur_time = getCurLocalTime();
    //SSMMHHDDN
    char key[11]{0};
    snprintf((char*)key, 11, "%02d%02d%02d%02d%02d", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_engineIndex.c_str());
    m_uniqueKey = key;
}
//clientid =  m_uniqueKey+orderRef
std::string TDEngineOkex::genClinetid(const std::string &orderRef){
    static int nIndex = 0;
    return m_uniqueKey + orderRef + std::to_string(nIndex++);
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))
BOOST_PYTHON_MODULE(libokextd){
    using namespace boost::python;
    class_<TDEngineOkex, boost::shared_ptr<TDEngineOkex> >("Engine")
     .def(init<>())
        .def("init", &TDEngineOkex::initialize)
        .def("start", &TDEngineOkex::start)
        .def("stop", &TDEngineOkex::stop)
        .def("logout", &TDEngineOkex::logout)
        .def("wait_for_stop", &TDEngineOkex::wait_for_stop);
}

