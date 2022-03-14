#include "TDEngineDeribit.h"
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
#include "../../utils/crypto/openssl_util.h"

using cpr::Get;
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
using utils::crypto::base64_decode;
USING_WC_NAMESPACE
std::mutex mutex_msg_queue;
std::mutex g_httpMutex;
TDEngineDeribit::TDEngineDeribit(): ITDEngine(SOURCE_DERIBIT)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.DERIBIT");
    KF_LOG_INFO(logger, "[TDEngineDeribit]");

    m_mutexOrder = new std::mutex();
    mutex_order_and_trade = new std::mutex();
    mutex_response_order_status = new std::mutex();
    mutex_orderaction_waiting_response = new std::mutex();
}

TDEngineDeribit::~TDEngineDeribit()
{
    if(m_mutexOrder != nullptr) delete m_mutexOrder;
    if(mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if(mutex_response_order_status != nullptr) delete mutex_response_order_status;
    if(mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
}

static TDEngineDeribit* global_md = nullptr;

static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{
    std::stringstream ss;
    //ss << "lws_callback,reason=" << reason << ",";
	switch( reason )
	{
		case LWS_CALLBACK_CLIENT_ESTABLISHED:
		{
            ss << "LWS_CALLBACK_CLIENT_ESTABLISHED.";
            if(global_md) global_md->writeErrorLog(ss.str());
			lws_callback_on_writable( wsi );
			break;
		}
		case LWS_CALLBACK_PROTOCOL_INIT:
        {
			 ss << "LWS_CALLBACK_PROTOCOL_INIT.";
            if(global_md) global_md->writeErrorLog(ss.str());
			break;
		}
		case LWS_CALLBACK_CLIENT_RECEIVE:
		{
			if(global_md)
			{
				global_md->on_lws_data(wsi, (const char*)in, len);
			}
			break;
		}
		case LWS_CALLBACK_CLIENT_WRITEABLE:
		{
		    ss << "LWS_CALLBACK_CLIENT_WRITEABLE.";
            
            int ret = 0;
			if(global_md)
			{
                global_md->writeErrorLog(ss.str());
				ret = global_md->lws_write_msg(wsi);
			}
			break;
		}
		case LWS_CALLBACK_CLOSED:
        case LWS_CALLBACK_WSI_DESTROY:
		case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
		{
            ss << "lws_callback,reason=" << reason;
            
 			if(global_md)
			{
                global_md->writeErrorLog(ss.str());
				global_md->on_lws_connection_error(wsi);
			}
			break;
		}
		default:
              //if(global_md) global_md->writeErrorLog(ss.str());
			break;
	}

	return 0;
}

std::string TDEngineDeribit::getTimestampStr()
{
    //long long timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  std::to_string(getMSTime());
}


 void TDEngineDeribit::onerror(Document& msg){
        KF_LOG_INFO(logger,"[onerror]");
        std::string orderID = msg["id"].GetString();
        string errmsg=msg["error"]["message"].GetString();
        int requestid;
        auto it1 = m_mapNewOrder.find(orderID);
        if(it1 != m_mapNewOrder.end()){
            requestid=it1->second.RequestID;
            m_mapNewOrder.erase(it1);
        }
        else
        {
            KF_LOG_INFO(logger,"[onerror]A not found order:" << orderID);
        }
        auto it2 = m_mapInputOrder.find(orderID);
        if(it2 != m_mapInputOrder.end())
        {
            on_rsp_order_insert(&(it2->second),requestid,100,errmsg.c_str());
            m_mapInputOrder.erase(it2);
        }
        else
        {
            KF_LOG_INFO(logger,"[onerror]B not found order:" << orderID);
        }
 }
void TDEngineDeribit::onCancel( Document& msg)
{
    Value &node=msg["result"];
    std::string strAction = node["order_state"].GetString();                                
    std::string strOrderId = node["order_id"].GetString();
    KF_LOG_DEBUG(logger, "order_id:"<<strOrderId << ",status:" << strAction);
    if(strAction == "cancelled")
    {
            KF_LOG_DEBUG(logger, "straction=canceled");
            //std::string strOrderId = node["order_id"].GetString();
            //std::lock_guard<std::mutex> lck(*m_mutexOrder); 
            auto it = m_mapOrder.find(strOrderId);
            if(it != m_mapOrder.end())
            {
                it->second.OrderStatus = LF_CHAR_Canceled;
                on_rtn_order(&(it->second));
                auto it_id = localOrderRefRemoteOrderId.find(it->second.OrderRef);
                if(it_id != localOrderRefRemoteOrderId.end())
                {
                    localOrderRefRemoteOrderId.erase(it_id);
                }
                m_mapOrder.erase(it);
            }
            //
            auto it2 = m_mapInputOrder.find(strOrderId);
            if(it2 != m_mapInputOrder.end())
            {
                m_mapInputOrder.erase(it2);
            }
            auto it3 = m_mapOrderAction.find(strOrderId);
            if(it3 != m_mapOrderAction.end())
            {    
                m_mapOrderAction.erase(it3);
            }
    }
}
void TDEngineDeribit::onNewOrder( Document& msg)
{
    Value &node=msg["result"]["order"];
    Value &trades=msg["result"]["trades"];
    std::string orderID = msg["id"].GetString();
    std::string strAction = node["order_state"].GetString();                                
    std::string strOrderId = node["order_id"].GetString();
    if(strAction == "rejected" )
    {
        auto it = m_mapNewOrder.find(orderID);
        auto it2 = m_mapInputOrder.find(orderID);
        if(it2 != m_mapInputOrder.end() && it != m_mapNewOrder.end())
        {
            on_rsp_order_insert(&(it2->second),it->second.RequestID,100,strOrderId.c_str());
            m_mapInputOrder.erase(it2);
            m_mapNewOrder.erase(it);
        }
        return;
    }
    //std::string strClientId = node["client_id"].GetString();
    //std::lock_guard<std::mutex> lck(*m_mutexOrder); 
    auto it = m_mapNewOrder.find(orderID);
    if(it != m_mapNewOrder.end())
    {
        it->second.OrderStatus = LF_CHAR_NotTouched;
        //KF_LOG_INFO(logger,"LF_CHAR_NotTouched");
        strncpy(it->second.BusinessUnit,strOrderId.c_str(),64);
        m_mapOrder.insert(std::make_pair(strOrderId,it->second));
        localOrderRefRemoteOrderId.insert(std::make_pair(it->second.OrderRef,strOrderId));
        on_rtn_order(&(it->second));
        m_mapNewOrder.erase(it);
    }
    else
    {
        KF_LOG_INFO(logger,"NotMatched:" << orderID);
        return;
    }
    auto it2 = m_mapInputOrder.find(orderID);
    if(it2 != m_mapInputOrder.end())
    { 
        auto data = it2->second;
        m_mapInputOrder.erase(it2);
        m_mapInputOrder.insert(std::make_pair(strOrderId,data));
    }
    auto& rtn_order = m_mapOrder[strOrderId];
    int tradeSize = trades.Size();
    if(tradeSize > 0)
    {
        for(int i =0; i < tradeSize;i++)
        {
            string state = trades.GetArray()[i]["state"].GetString();
            if(state != "filled")
            {
                rtn_order.OrderStatus = LF_CHAR_PartTradedQueueing;
            }
            else
            {
                rtn_order.OrderStatus = LF_CHAR_AllTraded;
            }
            int64_t volumeTraded = std::round(trades.GetArray()[i]["amount"].GetDouble()*scale_offset);
            rtn_order.VolumeTraded += volumeTraded;
            rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTraded;
            on_rtn_order(&rtn_order);
            raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_ORDER_DERIBIT, 1, -1);
            LFRtnTradeField rtn_trade;
            memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
            strcpy(rtn_trade.ExchangeID,rtn_order.ExchangeID);
            strncpy(rtn_trade.UserID, rtn_order.UserID,sizeof(rtn_trade.UserID));
            strncpy(rtn_trade.InstrumentID, rtn_order.InstrumentID, sizeof(rtn_trade.InstrumentID));
            strncpy(rtn_trade.OrderRef, rtn_order.OrderRef, sizeof(rtn_trade.OrderRef));
            rtn_trade.Direction = rtn_order.Direction;
            strncpy(rtn_trade.OrderSysID,strOrderId.c_str(),sizeof(rtn_trade.OrderSysID));
            strncpy(rtn_trade.TradeID,trades.GetArray()[i]["trade_id"].GetString(),sizeof(rtn_trade.TradeID));
            rtn_trade.Volume = volumeTraded;
            rtn_trade.Price = std::round(trades.GetArray()[i]["price"].GetDouble()*scale_offset);
            on_rtn_trade(&rtn_trade);
            raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),source_id, MSG_TYPE_LF_RTN_TRADE_DERIBIT, 1, -1);
        }
    }
    if(strAction == "open" )
    {
        localOrderRefRemoteOrderId.insert(std::make_pair(it->second.OrderRef,strOrderId));
    }
    else if(strAction == "filled")
    {
        KF_LOG_DEBUG(logger, "straction=filled");
        if(rtn_order.OrderStatus != LF_CHAR_AllTraded)
        {
            rtn_order.OrderStatus =LF_CHAR_AllTraded;
            rtn_order.VolumeTraded = rtn_order.VolumeTotalOriginal;
            rtn_order.VolumeTotal = 0;
            on_rtn_order(&rtn_order);
        }
        //
        m_mapOrder.erase(strOrderId);
        m_mapInputOrder.erase(strOrderId);                 
    }
    else if(strAction == "cancelled")
    {
        rtn_order.OrderStatus =LF_CHAR_Canceled;
        on_rtn_order(&rtn_order);
        m_mapInputOrder.erase(strOrderId);
        m_mapOrder.erase(strOrderId);
    }
    else
    {
         KF_LOG_DEBUG(logger, "unhandled status:" << strAction);
    }

}
void TDEngineDeribit::onTradeChannel( Document& msg)
{
    Value &trades=msg["params"]["data"];
    if(trades.IsArray() && trades.Size() > 0)
    {   
        for(int i = 0; i < trades.Size();++i)
        {                              
            std::string strOrderId = trades.GetArray()[i]["order_id"].GetString();
            KF_LOG_DEBUG(logger, "[onTardeChannel] OrderId:"<<strOrderId);
            auto it = m_mapOrder.find(strOrderId);
            if(it == m_mapOrder.end())
                continue;
            auto& rtn_order = it->second;
            string state = trades.GetArray()[i]["state"].GetString();
            if(state != "filled")
            {
                rtn_order.OrderStatus = LF_CHAR_PartTradedQueueing;
            }
            else
            {
                rtn_order.OrderStatus = LF_CHAR_AllTraded;
            }
            int64_t volumeTraded = std::round(trades.GetArray()[i]["amount"].GetDouble()*scale_offset);
            rtn_order.VolumeTraded += volumeTraded;
            rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTraded;
            on_rtn_order(&rtn_order);
            raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_ORDER_DERIBIT, 1, -1);
            LFRtnTradeField rtn_trade;
            memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
            strcpy(rtn_trade.ExchangeID,rtn_order.ExchangeID);
            strncpy(rtn_trade.UserID, rtn_order.UserID,sizeof(rtn_trade.UserID));
            strncpy(rtn_trade.InstrumentID, rtn_order.InstrumentID, sizeof(rtn_trade.InstrumentID));
            strncpy(rtn_trade.OrderRef, rtn_order.OrderRef, sizeof(rtn_trade.OrderRef));
            rtn_trade.Direction = rtn_order.Direction;
            strncpy(rtn_trade.OrderSysID,strOrderId.c_str(),sizeof(rtn_trade.OrderSysID));
            strncpy(rtn_trade.TradeID,trades.GetArray()[i]["trade_id"].GetString(),sizeof(rtn_trade.TradeID));
            rtn_trade.Volume = volumeTraded;
            rtn_trade.Price = std::round(trades.GetArray()[i]["price"].GetDouble()*scale_offset);
            on_rtn_trade(&rtn_trade);
            raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),source_id, MSG_TYPE_LF_RTN_TRADE_DERIBIT, 1, -1);
            if(state == "filled")
            {
                localOrderRefRemoteOrderId.find(rtn_order.OrderRef);
                m_mapInputOrder.erase(strOrderId);
                m_mapOrderAction.erase(strOrderId);
                m_mapOrder.erase(strOrderId);    
            }
            else if(state == "cancelled")
            {
                rtn_order.OrderStatus =LF_CHAR_Canceled;
                on_rtn_order(&rtn_order);
                localOrderRefRemoteOrderId.find(rtn_order.OrderRef);
                m_mapInputOrder.erase(strOrderId);
                m_mapOrderAction.erase(strOrderId);
                m_mapOrder.erase(strOrderId);
            }
            else
            {
                KF_LOG_DEBUG(logger, "unhandled status:" << state);
            }
        }
    }
}
void TDEngineDeribit::onOrderChange(Document& msg)
{
    if(msg.HasMember("result")&&msg["result"].HasMember("order_state"))
    {
        onCancel(msg);
    }  
    else if(msg.HasMember("result") && msg["result"].HasMember("order") && msg["result"].HasMember("trades"))
    {
        onNewOrder(msg);
    }
    else
    {
        KF_LOG_INFO(logger, "onOrderChange: no match method" );
    }           
 }

void TDEngineDeribit::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    //std::string strData = dealDataSprit(data);
	KF_LOG_INFO(logger, "TDEngineDeribit::on_lws_data: " << data);
    Document json;
	json.Parse(data);
    string openstr = "open";
    string filledstr = "filled";
    string bearerstr = "bearer";
    string okstr = "ok";
    string teststr="test_request";
    string heartstr="heartbeat";
    string subscriptionstr="subscription";
    string invalidstr="Invalid params";
    string heart_id="A9098";

    if(!json.HasParseError() && json.IsObject())
	{
        if(json.HasMember("params")&&json["params"].IsObject()&&json["params"].HasMember("type")&&(json["params"]["type"].GetString()==teststr||json["params"]["type"].GetString()==heartstr)){
            get_response();
        }
        else if(json.HasMember("id")&&json["id"].GetString()==heart_id&&json["result"].GetString()==okstr){
                m_isSuborderOK = true;
                m_isSubtradeOK = true;
            }
        else if(json.HasMember("error")&&json.HasMember("id")){
            onerror(json);
        }
        else if(json.HasMember("result") && json["result"].IsObject())	
        {
            Value &node=json["result"];
            
            if(node.HasMember("token_type")&&node["token_type"].GetString()==bearerstr){            
                m_isSubOK = true;
                //m_isSuborderOK = true;
                //m_isSubtradeOK = true;
                }
            
            if(node.HasMember("order")||node.HasMember("order_state")){
                onOrderChange(json);
            }
            /*if(node.HasMember("order") && node["order"]["order_state"].GetString()==openstr){
                string orderId = node.GetObject()["order"].GetObject()["order_id"].GetString();
                get_state(orderId);
            }
            else if(node.HasMember("order_state") && node["order_state"].GetString()==openstr){
                string orderId = node["order_id"].GetString();
                get_state(orderId);
            }
            else if(node.HasMember("order") && node["order"]["order_state"].GetString()==filledstr){
                onOrderChange(json);
            }*/
            
        }
        else if(json.HasMember("method")&&json["method"].GetString()==subscriptionstr){
            onTradeChannel(json);
        }

	} else 
    {
		KF_LOG_ERROR(logger, "MDEngineDeribit::on_lws_data . parse json error");
	}
	/*if(m_isSubOK==true){
        get_state();
    }*/
}

/*
{
    "type": "subscribe",
    "contract_codes": [],
    "channels": ["orders"],
    "key": "...",
    "sig": "...",
    "timestamp": "..."
}
*/



std::string TDEngineDeribit::makeSubscribeChannelString(AccountUnitDeribit& unit)
{
    string api_key = unit.api_key;
    string secret_key = unit.secret_key;
    std::string strTime = getTimestampStr();
    StringBuffer sbUpdate;
	Writer<StringBuffer> writer(sbUpdate);
	writer.StartObject();
    writer.Key("jsonrpc");
	writer.String("2.0");
    writer.Key("id");
    writer.String("A9929");    
	writer.Key("method");
    writer.String("public/auth");
    writer.Key("params");
    writer.StartObject();  
    writer.Key("grant_type");
    writer.String("client_credentials");  
    
	writer.Key("client_id");
	writer.String(api_key.c_str());
    
    writer.Key("client_secret");
    writer.String(secret_key.c_str());
    writer.EndObject();
	writer.EndObject();
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}
std::string TDEngineDeribit::makeSubscribeorderString(AccountUnitDeribit& unit)
{
    string api_key = unit.api_key;
    string secret_key = unit.secret_key;
    std::string strTime = getTimestampStr();
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();

    writer.Key("jsonrpc");
    writer.String("2.0");

    writer.Key("method");
    writer.String("private/subscribe");

    writer.Key("id");
    writer.String("A4200");

    writer.Key("params");
    writer.StartObject();

    writer.Key("channels");
    writer.StartArray();
    writer.String("user.orders.BTC-PERPETUAL.raw");
    writer.EndArray();

    writer.EndObject(); 
    writer.EndObject(); 
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}
std::string TDEngineDeribit::makeSubscribetradeString(AccountUnitDeribit& unit)
{
    string api_key = unit.api_key;
    string secret_key = unit.secret_key;
    std::string strTime = getTimestampStr();
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();

    writer.Key("jsonrpc");
    writer.String("2.0");

    writer.Key("method");
    writer.String("private/subscribe");

    writer.Key("id");
    writer.String("A4201");

    writer.Key("params");
    writer.StartObject();

    writer.Key("channels");
    writer.StartArray();
    writer.String("user.trades.any.any.raw");
    writer.EndArray();

    writer.EndObject(); 
    writer.EndObject(); 
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}
std::string TDEngineDeribit::makeheartbeatString(AccountUnitDeribit& unit)
{
    string api_key = unit.api_key;
    string secret_key = unit.secret_key;
    std::string strTime = getTimestampStr();
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();

    writer.Key("jsonrpc");
    writer.String("2.0");

    writer.Key("method");
    writer.String("public/set_heartbeat");

    writer.Key("id");
    writer.String("A9098");

    writer.Key("params");
    writer.StartObject();

    writer.Key("interval");
    //writer.StartArray();
    writer.Int(30);
    //writer.EndArray();

    writer.EndObject(); 
    writer.EndObject(); 
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}
std::string TDEngineDeribit::sign(const AccountUnitDeribit& unit,const std::string& method,const std::string& timestamp,const std::string& endpoint)
 {
    std::string to_sign = timestamp + method + endpoint;
    std::string decode_secret = base64_decode(unit.secret_key);
    unsigned char * strHmac = hmac_sha256_byte(decode_secret.c_str(),to_sign.c_str());
    std::string strSignatrue = base64_encode(strHmac,32);
    return strSignatrue;
 }
int TDEngineDeribit::lws_write_msg(struct lws* conn)
{
	//KF_LOG_INFO(logger, "TDEngineDeribit::lws_write_msg:" );
    
    int ret = 0;
    std::string strMsg = "";
    if (!m_isSub)
    {
        strMsg = makeSubscribeChannelString(account_units[0]);
        m_isSub = true;

    }
    else if(!heartfinish){
        strMsg = makeheartbeatString(account_units[0]);
        heartfinish = true;        
    }
    /*else if(m_isSuborderOK&&!orderfinish){
        strMsg = makeSubscribeorderString(account_units[0]);
        orderfinish = true;        
    }*/
    else if(m_isSubtradeOK&&!tradefinish){
        strMsg = makeSubscribetradeString(account_units[0]);
        tradefinish = true;        
    }
    else if(m_isSubOK)
    {
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        if(m_vstMsg.size() == 0){
            KF_LOG_INFO(logger, "TDEngineDeribit::m_vstMsg.size()=0 " );
            return 0;
        }
        else
        {
            KF_LOG_INFO(logger, "TDEngineDeribit::m_vstMsg" );
            strMsg = m_vstMsg.front();
            m_vstMsg.pop();
        }
    }
    else
    {
        KF_LOG_INFO(logger, "return 0" );
        return 0;
    }
    
    unsigned char msg[1024];
    memset(&msg[LWS_PRE], 0, 1024-LWS_PRE);
    int length = strMsg.length();
    KF_LOG_INFO(logger, "TDEngineDeribit::lws_write_msg: " << strMsg.c_str() << " ,len = " << length);
    strncpy((char *)msg+LWS_PRE, strMsg.c_str(), length);
    ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
    lws_callback_on_writable(conn);  
    return ret;
}

void TDEngineDeribit::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "TDEngineDeribit::on_lws_connection_error. login again.");
    is_connecting = false;
	//no use it
    long timeout_nsec = 0;

    login(timeout_nsec);
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

void TDEngineDeribit::genUniqueKey()
{
    struct tm cur_time = getCurLocalTime();
    //SSMMHHDDN
    char key[11]{0};
    snprintf((char*)key, 11, "%02d%02d%02d%02d%02d", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_CurrentTDIndex);
    m_uniqueKey = key;
}

//clientid =  m_uniqueKey+orderRef
std::string TDEngineDeribit::genClinetid(const std::string &orderRef)
{
    static int nIndex = 0;
    return m_uniqueKey + orderRef + std::to_string(nIndex++);
}

void TDEngineDeribit::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}



int64_t TDEngineDeribit::getMSTime()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  timestamp;
}




void TDEngineDeribit::init()
{
    genUniqueKey();
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineDeribit::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineDeribit::resize_accounts(int account_num)
{
    account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineDeribit::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    string api_key = j_config["APIKey"].get<string>();
    string secret_key = j_config["SecretKey"].get<string>();
    string baseUrl = j_config["baseUrl"].get<string>();
    //string wsUrl = j_config["wsUrl"].get<string>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

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
    if(j_config.find("current_td_index") != j_config.end()) {
        m_CurrentTDIndex = j_config["current_td_index"].get<int>();
    }
    KF_LOG_INFO(logger, "[load_account] (retry_interval_milliseconds)" << retry_interval_milliseconds);

    AccountUnitDeribit& unit = account_units[idx];
    unit.api_key = api_key;
    unit.secret_key = secret_key;
    unit.baseUrl = baseUrl;
    //unit.wsUrl = wsUrl;
    KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl);


    unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    unit.coinPairWhiteList.Debug_print();

    unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
    unit.positionWhiteList.Debug_print();

    //display usage:
    if(unit.coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "TDEngineDeribit::load_account: please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }

    //test
    Document d;
    get_auth(unit,d);
    Document json;
    get_account(unit, json);
    
    //getPriceIncrement(unit);
    // set up
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, api_key.c_str(), 16);
    strncpy(account.Password, secret_key.c_str(), 21);
    return account;
}

void TDEngineDeribit::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitDeribit& unit = account_units[idx];
        unit.logged_in = true;
        KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
        login(timeout_nsec);
    }
	
    cancel_all_orders();
}

   void TDEngineDeribit::getPriceIncrement(AccountUnitDeribit& unit)
   { 
        KF_LOG_INFO(logger, "[getPriceIncrement]");
        std::string requestPath = "/v1/contracts/active";
        string url = unit.baseUrl + requestPath ;
        std::string strTimestamp = getTimestampStr();

        std::string strSignatrue = sign(unit,"GET",strTimestamp,requestPath);
        cpr::Header mapHeader = cpr::Header{{"DERIBIT-ACCESS-SIG",strSignatrue},
                                            {"DERIBIT-ACCESS-TIMESTAMP",strTimestamp},
                                            {"DERIBIT-ACCESS-KEY",unit.api_key}};
        KF_LOG_INFO(logger, "DERIBIT-ACCESS-SIG = " << strSignatrue 
                            << ", DERIBIT-ACCESS-TIMESTAMP = " << strTimestamp 
                            << ", DERIBIT-API-KEY = " << unit.api_key);


        std::unique_lock<std::mutex> lock(g_httpMutex);
        const auto response = cpr::Get(Url{url}, Header{mapHeader}, Timeout{10000} );
        lock.unlock();
        KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
                                                " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
        Document json;
        json.Parse(response.text.c_str());

        if(!json.HasParseError() && json.HasMember("contracts"))
        {
            auto& jisonData = json["contracts"];
            size_t len = jisonData.Size();
            KF_LOG_INFO(logger, "[getPriceIncrement] (accounts.length)" << len);
            for(size_t i = 0; i < len; i++)
            {
                std::string symbol = jisonData.GetArray()[i]["contract_code"].GetString();
                std::string ticker = unit.coinPairWhiteList.GetKeyByValue(symbol);
                KF_LOG_INFO(logger, "[getPriceIncrement] (symbol) " << symbol << " (ticker) " << ticker);
                if(ticker.length() > 0) { 
                    std::string size = jisonData.GetArray()[i]["minimum_price_increment"].GetString(); 
                    PriceIncrement increment;
                    increment.nPriceIncrement = std::round(std::stod(size)*scale_offset);
                    unit.mapPriceIncrement.insert(std::make_pair(ticker,increment));           
                    KF_LOG_INFO(logger, "[getPriceIncrement] (symbol) " << symbol << " (position) " << increment.nPriceIncrement);
                }
            }
        }
        
   }

void TDEngineDeribit::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "TDEngineDeribit::login:");

    global_md = this;

    
    m_isSub = false;
    m_isSubOK = false;
    m_isSuborderOK = false;
    m_isSubtradeOK = false;
    orderfinish=false;
    tradefinish=false;
    heartfinish=false;
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
	KF_LOG_INFO(logger, "TDEngineDeribit::login: context created.");


	if (context == NULL) {
		KF_LOG_ERROR(logger, "TDEngineDeribit::login: context is NULL. return");
		return;
	}

	// Set up the client creation info
	std::string strAddress = "www.deribit.com";
    clientConnectInfo.address = strAddress.c_str();
    clientConnectInfo.path = "/ws/api/v2"; // Set the info's path to the fixed up url path
	clientConnectInfo.context = context;
	clientConnectInfo.port = 443;
	clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
	clientConnectInfo.host =strAddress.c_str();
	clientConnectInfo.origin = strAddress.c_str();
	clientConnectInfo.ietf_version_or_minus_one = -1;
	clientConnectInfo.protocol = protocols[PROTOCOL_TEST].name;
	clientConnectInfo.pwsi = &wsi;

    KF_LOG_INFO(logger, "TDEngineDeribit::login: address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);

	wsi = lws_client_connect_via_info(&clientConnectInfo);
	if (wsi == NULL) {
		KF_LOG_ERROR(logger, "TDEngineDeribit::login: wsi create error.");
		return;
	}
	KF_LOG_INFO(logger, "TDEngineDeribit::login: wsi create success.");
    is_connecting = true;
    m_conn = wsi;
    //connect(timeout_nsec);
}

void TDEngineDeribit::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineDeribit::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineDeribit::is_logged_in() const
{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineDeribit::is_connected() const
{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}


std::string TDEngineDeribit::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "buy";
    } else if (LF_CHAR_Sell == input) {
        return "sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineDeribit::GetDirection(std::string input) {
    if ("buy" == input) {
        return LF_CHAR_Buy;
    } else if ("sell" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineDeribit::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "market";
    } else {
        return "";
    }
}
LfOrderStatusType TDEngineDeribit::GetOrderSatus(const std::string& input)
{
    LfOrderStatusType status = LF_CHAR_Unknown;
    if(input == "open")
    {
        status = LF_CHAR_NotTouched;
    }
    else if (input == "filled")
    {
        status = LF_CHAR_AllTraded;
    }
    else if(input == "rejected")
    {
        status = LF_CHAR_Error;
    }
    else if(input == "cancelled")
    {
        status = LF_CHAR_Canceled;
    }
    else if(input == "untriggered" or input == "archive")
    {
        status = LF_CHAR_NotTouched;
    }
    return status;
}
LfOrderPriceTypeType TDEngineDeribit::GetPriceType(std::string input) 
{
    if ("limit" == input) {
        return LF_CHAR_LimitPrice;
    } else if ("market" == input) {
        return LF_CHAR_AnyPrice;
    } else {
        return '0';
    }
}

/**
 * req functions
 */
void TDEngineDeribit::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitDeribit& unit = account_units[account_index];
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
    get_account(unit, d);
    get_account_summary();
    KF_LOG_INFO(logger, "[req_investor_position] (get_account)" );
    if(d.IsObject() && d.HasMember("code") && d.HasMember("message"))
    {
        errorId =  d["code"].GetInt();
        errorMsg = d["message"].GetString();
        KF_LOG_ERROR(logger, "[req_investor_position] failed!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
    }
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_DERIBIT, 1, requestId);

    std::map<std::string,LFRspPositionField> tmp_map;
    if(!d.HasParseError() && d.HasMember("positions"))
    {
        auto& jisonData = d["positions"];
        size_t len = jisonData.Size();
        KF_LOG_INFO(logger, "[req_investor_position] (accounts.length)" << len);
        for(size_t i = 0; i < len; i++)
        {
            std::string symbol = jisonData.GetArray()[i]["contract_code"].GetString();
             KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol);
            std::string ticker = unit.coinPairWhiteList.GetKeyByValue(symbol);
             KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (ticker) " << ticker);
            if(ticker.length() > 0) {            
                uint64_t nPosition = std::round(std::stod(jisonData.GetArray()[i]["quantity"].GetString()) * scale_offset);   
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

void TDEngineDeribit::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineDeribit::dealPriceVolume(AccountUnitDeribit& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,double& dDealPrice,double& dDealVolume,bool Isbuy)
{
        KF_LOG_DEBUG(logger, "[dealPriceVolume] (symbol)" << symbol);
        //auto it = unit.mapPriceIncrement.find(symbol);
        std::string end=symbol.substr(symbol.length()-2,symbol.length());
        if(end=="-C"||end=="-P")
        {
            //dDealVolume = std::round(nVolume *1.0 / scale_offset);
            dDealVolume = (std::floor(nVolume *1.0 / 1e7))/10;
            if(Isbuy==true){
                dDealPrice = (std::floor(nPrice * 2.0 / 1e5))/1e3/2;
            }
            else{
                dDealPrice = (std::ceil(nPrice * 2.0 / 1e5))/1e3/2;
            }
        }
        else
        {
            dDealVolume = 10*(std::floor(nVolume * 1.0 / 1e9));
            if(Isbuy==true){
                dDealPrice = std::floor(nPrice * 2.0 / scale_offset)/2;
            }
            else{
                dDealPrice = std::ceil(nPrice * 2.0 / scale_offset)/2;
            }            
        }
        KF_LOG_INFO(logger, "[dealPriceVolume]  (symbol)" << symbol << " (Volume)" << nVolume << " (Price)" << nPrice << " (FixedVolume)" << dDealVolume << " (FixedPrice)" << dDealPrice);
}

void TDEngineDeribit::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitDeribit& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_DERIBIT, 1/*ISLAST*/, requestId);
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
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_DERIBIT, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);

    bool isbuy;
    if(GetSide(data->Direction)=="buy"){
        isbuy=true;
    }
    else if(GetSide(data->Direction)=="sell"){
        isbuy=false;
    }
    double fixedPrice = 0;
    double fixedVolume = 0;
    dealPriceVolume(unit,ticker,data->LimitPrice,data->Volume,fixedPrice,fixedVolume,isbuy);
    
    if(fixedVolume == 0)
    {
        KF_LOG_DEBUG(logger, "[req_order_insert] fixed Volume error" << ticker);
        errorId = 200;
        errorMsg = data->InstrumentID;
        errorMsg += " : Volume less than MinSize";
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_DERIBIT, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    std::string strClientId = genClinetid(data->OrderRef);
    string timestamp=getTimestampStr();
    timestamp=timestamp.erase(8,13);
    int insert_id=5275;
    KF_LOG_DEBUG(logger, "client order id:"<<strClientId);
    if(!is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        return;
    }
    {
        //std::lock_guard<std::mutex> lck(*m_mutexOrder);
        m_mapInputOrder.insert(std::make_pair(strClientId,*data));
        LFRtnOrderField order;
        memset(&order, 0, sizeof(LFRtnOrderField));
        order.OrderStatus = LF_CHAR_Unknown;
        order.VolumeTotalOriginal = std::round(fixedVolume*scale_offset);
        order.VolumeTotal = order.VolumeTotalOriginal;
        strncpy(order.OrderRef, data->OrderRef, 21);
        strncpy(order.InstrumentID, data->InstrumentID, 31);
        order.RequestID = requestId;
        strcpy(order.ExchangeID, "Deribit");
        strncpy(order.UserID, unit.api_key.c_str(), 16);
        order.LimitPrice = std::round(fixedPrice*scale_offset);
        order.TimeCondition = data->TimeCondition;
        order.Direction = data->Direction;
        order.OrderPriceType = data->OrderPriceType;
        m_mapNewOrder.insert(std::make_pair(strClientId,order));
    }
    //get_account_summary();
    if(GetSide(data->Direction)=="buy"){
        send_buyorder(ticker.c_str(),strClientId.c_str(), GetSide(data->Direction).c_str(),GetType(data->OrderPriceType).c_str(), fixedVolume, fixedPrice);
    }
    else if(GetSide(data->Direction)=="sell"){
        send_sellorder(ticker.c_str(),strClientId.c_str(), GetSide(data->Direction).c_str(),GetType(data->OrderPriceType).c_str(), fixedVolume, fixedPrice);
    }
   
}

void TDEngineDeribit::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitDeribit& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_DERIBIT, 1, requestId);

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
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_DERIBIT, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);
    //std::lock_guard<std::mutex> lck(*m_mutexOrder);
    std::map<std::string, std::string>::iterator itr = localOrderRefRemoteOrderId.find(data->OrderRef);
    std::string remoteOrderId;
    //cancel_order(id);
    if(itr == localOrderRefRemoteOrderId.end()) {
        errorId = 1;
        std::stringstream ss;
        ss << "[req_order_action] not found in localOrderRefRemoteOrderId map (orderRef) " << data->OrderRef;
        errorMsg = ss.str();
        KF_LOG_ERROR(logger, "[req_order_action] not found in localOrderRefRemoteOrderId map. "
                << " (rid)" << requestId << " (orderRef)" << data->OrderRef << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_DERIBIT, 1, requestId, errorId, errorMsg.c_str());
        return;
    } else {
        remoteOrderId = itr->second;
        KF_LOG_DEBUG(logger, "[req_order_action] found in localOrderRefRemoteOrderId map (orderRef) "
                << data->OrderRef << " (remoteOrderId) " << remoteOrderId);
        if(!is_connecting){
            errorId = 203;
            errorMsg = "websocket is not connecting,please try again later";
            on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
            return;
        }
        {
            //std::lock_guard<std::mutex> lck(*m_mutexOrder);
            m_mapOrderAction.insert(std::make_pair(remoteOrderId,*data));
        }
        
        cancel_order(remoteOrderId);
    }
    
}

//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
void TDEngineDeribit::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId)
{
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;
    newOrderActionSent.sentNameTime = getTimestamp();
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    remoteOrderIdOrderActionSentTime[remoteOrderId] = newOrderActionSent;
}


void TDEngineDeribit::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineDeribit::loop");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineDeribit::loopwebsocket, this)));

    //KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineDeribit::loopOrderActionNoResponseTimeOut");
    //orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineDeribit::loopOrderActionNoResponseTimeOut, this)));
}

void TDEngineDeribit::loopwebsocket()
{
		while(isRunning)
		{
            //KF_LOG_INFO(logger, "TDEngineDeribit::loop:lws_service");
			lws_service( context, rest_get_interval_ms );
            
		}
}



void TDEngineDeribit::loopOrderActionNoResponseTimeOut()
{
    KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        orderActionNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void TDEngineDeribit::orderActionNoResponseTimeOut()
{
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

void TDEngineDeribit::printResponse(const Document& d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
}


void TDEngineDeribit::get_account(AccountUnitDeribit& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_account]");

    //std::string requestPath = "/v1/positions";

    string url = "https://www.deribit.com/api/v2/private/get_account_summary?currency=BTC&extended=true" ;

    /*std::string strTimestamp = getTimestampStr();

    std::string strSignatrue = sign(unit,"GET",strTimestamp,requestPath);
    cpr::Header mapHeader = cpr::Header{{"DERIBIT-ACCESS-SIG",strSignatrue},
                                        {"DERIBIT-ACCESS-TIMESTAMP",strTimestamp},
                                        {"DERIBIT-ACCESS-KEY",unit.api_key}};
     KF_LOG_INFO(logger, "DERIBIT-ACCESS-SIG = " << strSignatrue 
                        << ", DERIBIT-ACCESS-TIMESTAMP = " << strTimestamp 
                        << ", DERIBIT-API-KEY = " << unit.api_key);
    */

    std::unique_lock<std::mutex> lock(g_httpMutex);
    const auto response = cpr::Get(Url{url}, 
                              Header{{"Authorization", "Bearer 7Lsh6H8HGWHAXfZqq2dDWdQGcR991Swt2idyWyv74nWG"}},Timeout{10000} );
    lock.unlock();
    KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
                                               " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
    
    json.Parse(response.text.c_str());
    return ;
}

void TDEngineDeribit::get_auth(AccountUnitDeribit& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_auth]");

    //std::string requestPath = "/v1/positions";

    string url = "https://www.deribit.com/api/v2/public/auth?client_id=6AdW5uJHGNaUJ&client_secret=PNAYVCJCSE7QIOID7T7Y6KOUXTUNFSRA&grant_type=client_credentials" ;

    /*std::string strTimestamp = getTimestampStr();

    std::string strSignatrue = sign(unit,"GET",strTimestamp,requestPath);
    cpr::Header mapHeader = cpr::Header{{"DERIBIT-ACCESS-SIG",strSignatrue},
                                        {"DERIBIT-ACCESS-TIMESTAMP",strTimestamp},
                                        {"DERIBIT-ACCESS-KEY",unit.api_key}};
     KF_LOG_INFO(logger, "DERIBIT-ACCESS-SIG = " << strSignatrue 
                        << ", DERIBIT-ACCESS-TIMESTAMP = " << strTimestamp 
                        << ", DERIBIT-API-KEY = " << unit.api_key);
    */

    //std::unique_lock<std::mutex> lock(g_httpMutex);
    const auto response = cpr::Get(Url{url}, 
                              Timeout{10000} );
    //lock.unlock();
    KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
                                               " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
    
    json.Parse(response.text.c_str());
    return ;
}
/*
 {
  channel: "trading",
  type: "request",
  action: "create-order",
  data: {
    contract_code: "BTCU18",
    client_id: "My Order #10",
    type: "limit",
    side: "buy",
    size: "10.0000",
    price: "6500.00"
  }
}
 */
std::string TDEngineDeribit::createbuyInsertOrderString(const char *code,const char* strClientId,const char *side, const char *type, double& size, double& price)
{
    string limitstr="limit";
    KF_LOG_INFO(logger, "[TDEngineDeribit::createbuyInsertOrdertring]:(price)"<<price << "(volume)" << size);
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("jsonrpc");
    writer.String("2.0");
    writer.Key("id");
    writer.String(strClientId);
    writer.Key("method");
    writer.String("private/buy");
    writer.Key("params");
    writer.StartObject();
    writer.Key("instrument_name");
    writer.String(code);
    writer.Key("amount");
    writer.Double(size);
    writer.Key("type");
    writer.String(type);
    
        writer.Key("price");
        writer.Double(price);
    


    writer.EndObject();
    writer.EndObject();
    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineDeribit::createbuyInsertOrdertring]:" << strOrder);
    return strOrder;
}

std::string TDEngineDeribit::createsellInsertOrderString(const char *code,const char* strClientId,const char *side, const char *type, double& size, double& price)
{
    string limitstr="limit";
    KF_LOG_INFO(logger, "[TDEngineDeribit::createsellInsertOrdertring]:(price)"<<price << "(volume)" << size);
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("jsonrpc");
    writer.String("2.0");
    writer.Key("id");
    writer.String(strClientId);
    writer.Key("method");
    writer.String("private/sell");
    writer.Key("params");
    writer.StartObject();
    writer.Key("instrument_name");
    writer.String(code);
    writer.Key("amount");
    writer.Double(size);
    writer.Key("type");
    writer.String(type);
    
        writer.Key("price");
        writer.Double(price);
    
    /*writer.Key("price");
    writer.Double(price);
    writer.Key("stop_price");
    writer.Double(price);
    writer.Key("trigger");
    writer.String("last_price");*/

    writer.EndObject();
    writer.EndObject();
    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineDeribit::createsellInsertOrdertring]:" << strOrder);
    return strOrder;
}

void TDEngineDeribit::send_buyorder(const char *code,const char* strClientId,const char *side, const char *type, double& size, double& price)
{
    KF_LOG_INFO(logger, "[send_buyorder]");
    {
        std::string new_order = createbuyInsertOrderString(code, strClientId,side, type, size, price);
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        m_vstMsg.push(new_order);
        lws_callback_on_writable(m_conn);
    }
}

void TDEngineDeribit::send_sellorder(const char *code,const char* strClientId,const char *side, const char *type, double& size, double& price)
{
    KF_LOG_INFO(logger, "[send_sellorder]");
    {
        std::string new_order = createsellInsertOrderString(code, strClientId,side, type, size, price);
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        m_vstMsg.push(new_order);
        lws_callback_on_writable(m_conn);
    }
}    


void TDEngineDeribit::cancel_all_orders()
{
    KF_LOG_INFO(logger, "[cancel_all_orders]");

    std::string cancel_allorder = createCancelallOrderString(nullptr);
    std::lock_guard<std::mutex> lck(mutex_msg_queue);
    m_vstMsg.push(cancel_allorder);
    //lws_callback_on_writable(m_conn);
}

/*
void TDEngineDeribit::get_order()
{
    KF_LOG_INFO(logger, "[get_order]");

    std::string get_order = createorderString();
    //std::lock_guard<std::mutex> lck(mutex_msg_queue);
    m_vstMsg.push(get_order);
    //lws_callback_on_writable(m_conn);
}*/


void TDEngineDeribit::get_state(string orderid)
{
    KF_LOG_INFO(logger, "[get_state]");

    std::string get_statestate = createstateString(orderid);
    //std::lock_guard<std::mutex> lck(mutex_msg_queue);
    m_vstMsg.push(get_statestate);
    //lws_callback_on_writable(m_conn);
}

void TDEngineDeribit::get_account_summary()
{
    KF_LOG_INFO(logger, "[get_account_summary]");

    std::string getaccount = createaccountString();
    //std::lock_guard<std::mutex> lck(mutex_msg_queue);
    m_vstMsg.push(getaccount);
    //lws_callback_on_writable(m_conn);
}

void TDEngineDeribit::get_response()
{
    KF_LOG_INFO(logger, "[get_response]");

    std::string getresponse = createresponseString();
    //std::lock_guard<std::mutex> lck(mutex_msg_queue);
    m_vstMsg.push(getresponse);
    //lws_callback_on_writable(m_conn);
}
/*
{
  channel: "trading",
  type: "request",
  action: "cancel-order",
  data: {
    order_id: "58f5435e-02b8-4875-81d4-e3976c5ed68b"
  }
}
*/
std::string TDEngineDeribit::createCancelOrderString(const char* strOrderId)
{
    KF_LOG_INFO(logger, "[TDEngineDeribit::createCancelOrderString]");
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("jsonrpc");
    writer.String("2.0");
    writer.Key("id");
    writer.String("A4214");
    writer.Key("method");
    writer.String("private/cancel");
    writer.Key("params");
    writer.StartObject();
    writer.Key("order_id");
    writer.String(strOrderId);
    writer.EndObject();
    writer.EndObject();
    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineDeribit::createCancelOrderString]:" << strOrder);
    return strOrder;
}

std::string TDEngineDeribit::createstateString(string orderid)
{
    KF_LOG_INFO(logger, "[TDEngineDeribit::createstateString]");
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("jsonrpc");
    writer.String("2.0");
    writer.Key("id");
    writer.String("A4316");
    writer.Key("method");
    writer.String("private/get_order_state");
    writer.Key("params");
    writer.StartObject();
    writer.Key("order_id");
    writer.String(orderid.c_str());
    writer.EndObject();
    writer.EndObject();
    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineDeribit::createstateString]:" << strOrder);
    return strOrder;
}

std::string TDEngineDeribit::createaccountString()
{
    KF_LOG_INFO(logger, "[TDEngineDeribit::createaccountString]");
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("jsonrpc");
    writer.String("2.0");
    writer.Key("id");
    writer.String("A2515");    
    writer.Key("method");
    writer.String("private/get_account_summary");
    writer.Key("params");
    writer.StartObject();    
    
    writer.Key("currency");
    writer.String("BTC");
    
    writer.Key("extended");
    writer.Bool(true);
    writer.EndObject();
    writer.EndObject();

    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineDeribit::createaccountString]:" << strOrder);
    return strOrder;
}

std::string TDEngineDeribit::createresponseString()
{
    KF_LOG_INFO(logger, "[TDEngineDeribit::createresponseString]");
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("jsonrpc");
    writer.String("2.0");
    writer.Key("id");
    writer.String("A8212");    
    writer.Key("method");
    writer.String("public/test");
    writer.Key("params");
    writer.StartObject();    

    writer.EndObject();
    writer.EndObject();

    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineDeribit::createresponseString]:" << strOrder);
    return strOrder;
}

std::string TDEngineDeribit::createCancelallOrderString(const char* strOrderId)
{
    KF_LOG_INFO(logger, "[TDEngineDeribit::createCancelallOrderString]");
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("jsonrpc");
    writer.String("2.0");
    writer.Key("id");
    writer.String("A8748");
    writer.Key("method");
    writer.String("private/cancel_all");
    writer.Key("params");
    writer.StartObject();
    writer.EndObject();
    writer.EndObject();
    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineDeribit::createCancelallOrderString]:" << strOrder);
    return strOrder;
}

void TDEngineDeribit::cancel_order(std::string orderId)
{
    KF_LOG_INFO(logger, "[cancel_order]");
    std::string cancel_order = createCancelOrderString(orderId.c_str());
    std::lock_guard<std::mutex> lck(mutex_msg_queue);
    m_vstMsg.push(cancel_order);
    lws_callback_on_writable(m_conn);
}



std::string TDEngineDeribit::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


inline int64_t TDEngineDeribit::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}



#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

BOOST_PYTHON_MODULE(libderibittd)
{
    using namespace boost::python;
    class_<TDEngineDeribit, boost::shared_ptr<TDEngineDeribit> >("Engine")
            .def(init<>())
            .def("init", &TDEngineDeribit::initialize)
            .def("start", &TDEngineDeribit::start)
            .def("stop", &TDEngineDeribit::stop)
            .def("logout", &TDEngineDeribit::logout)
            .def("wait_for_stop", &TDEngineDeribit::wait_for_stop);
}

