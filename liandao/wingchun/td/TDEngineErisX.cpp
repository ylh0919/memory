#include "TDEngineErisX.h"
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
TDEngineErisX::TDEngineErisX(): ITDEngine(SOURCE_ERISX)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.ERISX");
    KF_LOG_INFO(logger, "[TDEngineErisX]");

    m_mutexOrder = new std::mutex();
    mutex_order_and_trade = new std::mutex();
    mutex_response_order_status = new std::mutex();
    mutex_orderaction_waiting_response = new std::mutex();
}

TDEngineErisX::~TDEngineErisX()
{
    if(m_mutexOrder != nullptr) delete m_mutexOrder;
    if(mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if(mutex_response_order_status != nullptr) delete mutex_response_order_status;
    if(mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
}

static TDEngineErisX* global_md = nullptr;

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

std::string TDEngineErisX::getTimestampStr()
{
    //long long timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  std::to_string(getMSTime());
}

void TDEngineErisX::DealNottouch(std::string clOrdID,std::string orderID,OrderMsg& ordermsg)
{
    KF_LOG_INFO(logger,"TDEngineCoinflex::DealNottouch");
    /*auto itr = remoteOrderIdOrderInsertSentTime.find(clOrdID);
    if(itr != remoteOrderIdOrderInsertSentTime.end()){
        remoteOrderIdOrderInsertSentTime.erase(itr);
    }*/

    auto iter3 = m_mapOrder.find(clOrdID);
    if(iter3 != m_mapOrder.end())
    {
        KF_LOG_DEBUG(logger, "straction=open in");
        localOrderRefRemoteOrderId.insert(std::make_pair(iter3->second.OrderRef,orderID));
        ordermsg_map.insert(std::make_pair(orderID,ordermsg));
        //if(iter3->second.OrderStatus==LF_CHAR_NotTouched){
        iter3->second.OrderStatus=LF_CHAR_NotTouched;
        on_rtn_order(&(iter3->second));
        //}
    }             
}

void TDEngineErisX::DealCancel(std::string clOrdID,std::string orderID)
{
    KF_LOG_INFO(logger,"clOrdID="<<clOrdID);
    auto it = m_mapOrder.find(clOrdID);
    if(it != m_mapOrder.end())
    {
        it->second.OrderStatus = LF_CHAR_Canceled;
        on_rtn_order(&(it->second));

        auto it_id = localOrderRefRemoteOrderId.find(it->second.OrderRef);
        if(it_id != localOrderRefRemoteOrderId.end())
        {
            localOrderRefRemoteOrderId.erase(it_id);
        }
        auto it1 = ordermsg_map.find(orderID);
        if(it1 != ordermsg_map.end())
        {
            ordermsg_map.erase(it1);
        }
        m_mapOrder.erase(it);
    }
}

void TDEngineErisX::DealTrade(std::string clOrdID,std::string orderID,int64_t price,uint64_t cumvolume,uint64_t lastvolume)
{
    auto it4 = m_mapOrder.find(clOrdID);
    if(it4 != m_mapOrder.end())
    {
        KF_LOG_DEBUG(logger, "straction=filled in");

        it4->second.VolumeTraded = cumvolume;
        it4->second.VolumeTotal = it4->second.VolumeTotalOriginal - it4->second.VolumeTraded;
        if(it4->second.VolumeTraded==it4->second.VolumeTotalOriginal){
            it4->second.OrderStatus = LF_CHAR_AllTraded;
        }
        else{
            it4->second.OrderStatus = LF_CHAR_PartTradedQueueing;
        }
        on_rtn_order(&(it4->second));
        raw_writer->write_frame(&(it4->second), sizeof(LFRtnOrderField),
                                source_id, MSG_TYPE_LF_RTN_ORDER_COINFLEX, 1, -1);

        LFRtnTradeField rtn_trade;
        memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
        strcpy(rtn_trade.ExchangeID,it4->second.ExchangeID);
        strncpy(rtn_trade.UserID, it4->second.UserID,sizeof(rtn_trade.UserID));
        strncpy(rtn_trade.InstrumentID, it4->second.InstrumentID, sizeof(rtn_trade.InstrumentID));
        strncpy(rtn_trade.OrderRef, it4->second.OrderRef, sizeof(rtn_trade.OrderRef));
        rtn_trade.Direction = it4->second.Direction;
        strncpy(rtn_trade.OrderSysID,orderID.c_str(),sizeof(rtn_trade.OrderSysID));
        /*uint64_t volume = cumvolume - lastvolume;
        if(volume==0){
            rtn_trade.Volume = lastvolume;
        }
        else{
            rtn_trade.Volume = volume;
        }*/
        rtn_trade.Volume = lastvolume;
        rtn_trade.Price = price;
        on_rtn_trade(&rtn_trade);
        raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
                                source_id, MSG_TYPE_LF_RTN_TRADE_COINFLEX, 1, -1);

        if(it4->second.OrderStatus == LF_CHAR_AllTraded)
        {
            auto it_id = localOrderRefRemoteOrderId.find(it4->second.OrderRef);
            if(it_id != localOrderRefRemoteOrderId.end())
            {
                KF_LOG_INFO(logger,"earse local");
                localOrderRefRemoteOrderId.erase(it_id);
            }
            m_mapOrder.erase(it4);

            auto it1 = ordermsg_map.find(orderID);
            if(it1 != ordermsg_map.end())
            {    
                ordermsg_map.erase(it1);
            }               
        }
    }
}

void TDEngineErisX::onOrderNew(Document& msg)
{
    OrderMsg ordermsg;
    ordermsg.clOrdID = msg["clOrdID"].GetString();
    ordermsg.origClOrdID = msg["origClOrdID"].GetString();
    ordermsg.orderID = msg["orderID"].GetString();
    ordermsg.symbol = msg["symbol"].GetString();
    ordermsg.side = msg["side"].GetString();
    ordermsg.currency = msg["currency"].GetString();
    DealNottouch(ordermsg.clOrdID,ordermsg.orderID,ordermsg);
}

void TDEngineErisX::onOrderCancel(Document& msg)
{
    std::string orderID = msg["orderID"].GetString();
    std::string clOrdID = msg["clOrdID"].GetString();
    DealCancel(clOrdID,orderID);
}

void TDEngineErisX::onOrderTrade(Document& msg)
{
    KF_LOG_INFO(logger,"onOrderTrade");
    std::string clOrdID = msg["clOrdID"].GetString();
    std::string orderID = msg["orderID"].GetString();
    double avgPrice = msg["avgPrice"].GetDouble();
    double cumQty = msg["cumQty"].GetDouble();
    double lastQty = msg["lastQty"].GetDouble();
    int64_t price = std::round(avgPrice*scale_offset);
    uint64_t cumvolume = cumQty*scale_offset;
    uint64_t lastvolume = lastQty*scale_offset;
    DealTrade(clOrdID,orderID,price,cumvolume,lastvolume);
}

void TDEngineErisX::onerror(Document& msg){
        KF_LOG_INFO(logger,"[onerror]");
        string errmsg;
        if(msg.HasMember("message")){
            errmsg = msg["message"].GetString();
        }else if(msg.HasMember("error")){
            errmsg = msg["error"].GetString();
        }
        std::string cid=msg["correlation"].GetString();
        //std::string cidstr = std::to_string(cid);
        int requestid = 100;

        auto it1 = errormap1.find(cid);
        if(it1 != errormap1.end()){
            KF_LOG_INFO(logger,"[errormap1]");
            requestid=it1->second.RequestID;
            errormap1.erase(it1);
        }

        auto it = errormap.find(cid);
        if(it != errormap.end())
        {
            KF_LOG_INFO(logger,"[errormap]");           
            on_rsp_order_insert(&(it->second),requestid,201,errmsg.c_str());
            errormap.erase(it);
        }
 }

void TDEngineErisX::on_cancelerror(Document& msg){
        KF_LOG_INFO(logger,"[on_cancelerror]");
        string errmsg;
        if(msg.HasMember("message")){
            errmsg = msg["message"].GetString();
        }else if(msg.HasMember("error")){
            errmsg = msg["error"].GetString();
        }else if(msg.HasMember("text")){
            errmsg = msg["text"].GetString();
        }
        std::string cid=msg["correlation"].GetString();
        //std::string cidstr = std::to_string(cid);
        int requestid = 100;

        auto it = error_cancelmap.find(cid);
        if(it != error_cancelmap.end())
        {
            KF_LOG_INFO(logger,"[error_cancelmap]");           
            on_rsp_order_action(&(it->second.data),it->second.requestid,201,errmsg.c_str());
            error_cancelmap.erase(it);
        }
 }

void TDEngineErisX::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    std::string substr = "-1";
    std::string partystr = "party";
    std::string symbolstr = "symbol";
    std::string cancelstr = "cancel";
    std::string cancelstr1 = "CANCELED";
    KF_LOG_INFO(logger, "TDEngineErisX::on_lws_data: " << data);
    Document json;
	json.Parse(data);

    if(!json.HasParseError() && json.IsObject())
	{
        if(json.HasMember("ordStatus")){
            std::string ordStatus = json["ordStatus"].GetString();
            if(ordStatus=="NEW"){
                onOrderNew(json);
            }
            else if(ordStatus=="CANCELED"){
                onOrderCancel(json);
            }
            else if(ordStatus=="PARTIALLY_FILLED"||ordStatus=="FILLED"){
                onOrderTrade(json);
            }
            else if(ordStatus=="REJECTED"){
                on_cancelerror(json);
            }
        }
        else if(json.HasMember("rejectTime") || json.HasMember("error") && json.HasMember("correlation")){
            std::string correlation = json["correlation"].GetString();
            if(correlation.substr(0,4) == "send"){
                onerror(json);
            }else if(correlation.substr(0,4) == "canc"){
                on_cancelerror(json);
            }
        }
        else if(json.HasMember("correlation") && json["correlation"].GetString()==substr && json.HasMember("success"))	
        {
            m_isSubOK = true;
        }
        else if(json.HasMember("correlation") && json["correlation"].GetString()==partystr)  
        {
            KF_LOG_INFO(logger,"ispartyok");
            ispartyok = true;
            partyid = json["partyIds"].GetArray()[0].GetString();
        }
        else if(json.HasMember("correlation") && json["correlation"].GetString()==symbolstr)  
        {
            KF_LOG_INFO(logger,"issymbolok");
            issymbolok = true;
            SaveIncrement(json);
        }       
	} else 
    {
		KF_LOG_ERROR(logger, "MDEngineErisX::on_lws_data . parse json error");
	}
	
}

void TDEngineErisX::SaveIncrement(Document& msg)
{
   Value &node = msg["securities"];
   int size = node.Size();
   for(int i=0;i<size;i++){
        std::string symbol = node.GetArray()[i]["symbol"].GetString();
        PriceIncrement PI;
        PI.minTradeVol = (node.GetArray()[i]["minTradeVol"].GetDouble())*scale_offset;
        PI.roundLot = (node.GetArray()[i]["roundLot"].GetDouble())*scale_offset;
        PI.minPriceIncrement = node.GetArray()[i]["minPriceIncrement"].GetDouble()*scale_offset;
        //PI.minPriceIncrement = (std::stod(PriceIncrement))*scale_offset;
        mapPriceIncrement[symbol] = PI;
        KF_LOG_INFO(logger,"symbol:"<<symbol<<" "<<PI.minTradeVol<<" "<<PI.roundLot<<" "<<PI.minPriceIncrement);
   } 
}

std::string TDEngineErisX::makeSubscribeChannelString(AccountUnitErisX& unit)
{
    int64_t intime = getTimestamp();
    std::string timestr = std::to_string(intime);
    KF_LOG_INFO(logger,"timestr"<<timestr);
    std::string strPayLoad;
        strPayLoad = R"({"sub":")" + api_key + R"(","iat":")" + timestr
            //+ R"(","query":")" + strQuery
            + R"("})";
    KF_LOG_INFO(logger,"strPayLoad:"<<strPayLoad);

    std::string strJWT = utils::crypto::jwt_hs256_create_erisx(strPayLoad,secret_key);

    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();

    writer.Key("correlation");
    writer.String("-1");

    writer.Key("type");
    writer.String("AuthenticationRequest");

    writer.Key("token");
    writer.String(strJWT.c_str());

    writer.EndObject(); 
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}

std::string TDEngineErisX::makePartyString()
{
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();

    writer.Key("correlation");
    writer.String("party");

    writer.Key("type");
    writer.String("PartyListRequest");

    writer.EndObject(); 
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}

std::string TDEngineErisX::makeSymbolString()
{
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();

    writer.Key("correlation");
    writer.String("symbol");

    writer.Key("type");
    writer.String("SecurityList");

    writer.EndObject(); 
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}

std::string TDEngineErisX::sign(const AccountUnitErisX& unit,const std::string& method,const std::string& timestamp,const std::string& endpoint)
 {
    std::string to_sign = timestamp + method + endpoint;
    std::string decode_secret = base64_decode(unit.secret_key);
    unsigned char * strHmac = hmac_sha256_byte(decode_secret.c_str(),to_sign.c_str());
    std::string strSignatrue = base64_encode(strHmac,32);
    return strSignatrue;
 }
int TDEngineErisX::lws_write_msg(struct lws* conn)
{
    //KF_LOG_INFO(logger, "TDEngineCoinflex::lws_write_msg:" );
    
    int ret = 0;
    std::string strMsg = "";

    if (!m_isSub)
    {
        strMsg = makeSubscribeChannelString(account_units[0]);
        m_isSub = true;
    }
    else if(m_isSubOK&&!isparty){
        strMsg = makePartyString();
        isparty = true;
    }
    else if(m_isSubOK&&ispartyok&&!issymbol){
        strMsg = makeSymbolString();
        issymbol = true;
    }
    else if(m_isSubOK && ispartyok && issymbolok)
    {
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        if(m_vstMsg.size() == 0){
            KF_LOG_INFO(logger, "TDEngineErisX::m_vstMsg.size()=0 " );
            return 0;
        }
        else
        {
            KF_LOG_INFO(logger, "TDEngineErisX::m_vstMsg" );
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
    KF_LOG_INFO(logger, "TDEngineErisX::lws_write_msg: " << strMsg.c_str() << " ,len = " << length);
    strncpy((char *)msg+LWS_PRE, strMsg.c_str(), length);
    ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
    lws_callback_on_writable(conn);  
    return ret;
}

void TDEngineErisX::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "TDEngineErisX::on_lws_connection_error. login again.");
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

void TDEngineErisX::genUniqueKey()
{
    struct tm cur_time = getCurLocalTime();
    //SSMMHHDDN
    char key[11]{0};
    snprintf((char*)key, 11, "%02d%02d%02d%02d%02d", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_CurrentTDIndex);
    m_uniqueKey = key;
}

//clientid =  m_uniqueKey+orderRef
std::string TDEngineErisX::genClinetid(const std::string &orderRef)
{
    static int nIndex = 0;
    return m_uniqueKey + orderRef + std::to_string(nIndex++);
}

void TDEngineErisX::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}



int64_t TDEngineErisX::getMSTime()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  timestamp;
}




void TDEngineErisX::init()
{
    //genUniqueKey();
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineErisX::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineErisX::resize_accounts(int account_num)
{
    account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineErisX::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    api_key = j_config["APIKey"].get<string>();
    secret_key = j_config["SecretKey"].get<string>();
    baseUrl = j_config["baseUrl"].get<string>();
    //string wsUrl = j_config["wsUrl"].get<string>();
    if(j_config.find("isfuture") != j_config.end()) {
        isfuture = j_config["isfuture"].get<int>();
    }
    else 
        isfuture = 0;
    
    if(j_config.find("accountType") != j_config.end() && j_config.find("custOrderCapacity") != j_config.end() && j_config.find("senderLocationId") != j_config.end() && j_config.find("senderSubId") != j_config.end()) {
        futureisconfig = 1;
        accountType = j_config["accountType"].get<int>();
        custOrderCapacity = j_config["custOrderCapacity"].get<int>();
        senderLocationId = j_config["senderLocationId"].get<string>();
        senderSubId = j_config["senderSubId"].get<string>();
    }
    else
        futureisconfig = 0;



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

    AccountUnitErisX& unit = account_units[idx];
    unit.api_key = api_key;
    unit.secret_key = secret_key;
    unit.baseUrl = baseUrl;
    //unit.wsUrl = wsUrl;
    KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl);
    genUniqueKey();

    unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    unit.coinPairWhiteList.Debug_print();

    unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
    unit.positionWhiteList.Debug_print();

    KF_LOG_INFO(logger, "Debug_print success");


    //display usage:
    if(unit.coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "TDEngineErisX::load_account: please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }

    //test
    Document json;
    //get_account(unit, json);
    
    //getPriceIncrement(unit);
    // set up
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, api_key.c_str(), 16);
    strncpy(account.Password, secret_key.c_str(), 21);
    return account;
}

void TDEngineErisX::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitErisX& unit = account_units[idx];
        unit.logged_in = true;
        KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
        login(timeout_nsec);
    }
	
    //cancel_all_orders();
}
/*
   void TDEngineErisX::getPriceIncrement(AccountUnitErisX& unit)
   { 
        //KF_LOG_INFO(logger, "[getPriceIncrement]");
        std::string requestPath = "/v1/contracts/active";
        string url = unit.baseUrl + requestPath ;
        std::string strTimestamp = getTimestampStr();

        std::string strSignatrue = sign(unit,"GET",strTimestamp,requestPath);
        cpr::Header mapHeader = cpr::Header{{"ERISX-ACCESS-SIG",strSignatrue},
                                            {"ERISX-ACCESS-TIMESTAMP",strTimestamp},
                                            {"ERISX-ACCESS-KEY",unit.api_key}};


        std::unique_lock<std::mutex> lock(g_httpMutex);
        const auto response = cpr::Get(Url{url}, Header{mapHeader}, Timeout{10000} );
        lock.unlock();
        //KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<" (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str());
        Document json;
        json.Parse(response.text.c_str());

        if(!json.HasParseError() && json.HasMember("contracts"))
        {
            auto& jisonData = json["contracts"];
            size_t len = jisonData.Size();
            //KF_LOG_INFO(logger, "[getPriceIncrement] (accounts.length)" << len);
            for(size_t i = 0; i < len; i++)
            {
                std::string symbol = jisonData.GetArray()[i]["contract_code"].GetString();
                std::string ticker = unit.coinPairWhiteList.GetKeyByValue(symbol);
                //KF_LOG_INFO(logger, "[getPriceIncrement] (symbol) " << symbol << " (ticker) " << ticker);
                if(ticker.length() > 0) { 
                    std::string size = jisonData.GetArray()[i]["minimum_price_increment"].GetString(); 
                    PriceIncrement increment;
                    increment.nPriceIncrement = std::round(std::stod(size)*scale_offset);
                    unit.mapPriceIncrement.insert(std::make_pair(ticker,increment));           
                    //KF_LOG_INFO(logger, "[getPriceIncrement] (symbol) " << symbol << " (position) " << increment.nPriceIncrement);
                }
            }
        }
        
   }*/

void TDEngineErisX::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "TDEngineErisX::login:");

    global_md = this;

    isparty = false;
    ispartyok = false;
    m_isSub = false;
    m_isSubOK = false;
    issymbol = false;
    issymbolok = false;
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
	KF_LOG_INFO(logger, "TDEngineErisX::login: context created.");


	if (context == NULL) {
		KF_LOG_ERROR(logger, "TDEngineErisX::login: context is NULL. return");
		return;
	}

	// Set up the client creation info
	std::string strAddress = baseUrl;
    clientConnectInfo.address = strAddress.c_str();
    clientConnectInfo.path = "/"; // Set the info's path to the fixed up url path
	clientConnectInfo.context = context;
	clientConnectInfo.port = 443;
	clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
	clientConnectInfo.host =strAddress.c_str();
	clientConnectInfo.origin = strAddress.c_str();
	clientConnectInfo.ietf_version_or_minus_one = -1;
	clientConnectInfo.protocol = protocols[PROTOCOL_TEST].name;
	clientConnectInfo.pwsi = &wsi;

    KF_LOG_INFO(logger, "TDEngineErisX::login: address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);

	wsi = lws_client_connect_via_info(&clientConnectInfo);
	if (wsi == NULL) {
		KF_LOG_ERROR(logger, "TDEngineErisX::login: wsi create error.");
		return;
	}
	KF_LOG_INFO(logger, "TDEngineErisX::login: wsi create success.");
    is_connecting = true;
    m_conn = wsi;
    KF_LOG_INFO(logger,"wsi end");
    //connect(timeout_nsec);
}

void TDEngineErisX::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineErisX::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineErisX::is_logged_in() const
{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineErisX::is_connected() const
{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}


std::string TDEngineErisX::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "buy";
    } else if (LF_CHAR_Sell == input) {
        return "sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineErisX::GetDirection(std::string input) {
    if ("buy" == input) {
        return LF_CHAR_Buy;
    } else if ("sell" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineErisX::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "market";
    } else {
        return "";
    }
}

LfOrderPriceTypeType TDEngineErisX::GetPriceType(std::string input) 
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
void TDEngineErisX::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitErisX& unit = account_units[account_index];
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
    KF_LOG_INFO(logger, "[req_investor_position] (get_account)" );
    if(d.IsObject() && d.HasMember("code") && d.HasMember("message"))
    {
        errorId =  d["code"].GetInt();
        errorMsg = d["message"].GetString();
        KF_LOG_ERROR(logger, "[req_investor_position] failed!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_KUCOIN, 1, requestId, errorId, errorMsg.c_str());
    }
    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_ERISX, 1, requestId);

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
    KF_LOG_INFO(logger,"posend");
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

void TDEngineErisX::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineErisX::dealPriceVolume(AccountUnitErisX& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,double& dDealPrice,double& dDealVolume,bool isbuy)
{
        KF_LOG_DEBUG(logger, "[dealPriceVolume] (symbol)" << symbol);
        auto it = mapPriceIncrement.find(symbol);
        if(it == mapPriceIncrement.end())
        {
            KF_LOG_INFO(logger, "[dealPriceVolume] symbol not find :" << symbol);
            dDealVolume = nVolume * 1.0 / scale_offset;
            dDealPrice = nPrice * 1.0 / scale_offset;
        }
        else
        {
            KF_LOG_INFO(logger, "[dealPriceVolume] symbol find :" << symbol);

            dDealVolume = (floor(nVolume/it->second.roundLot))*it->second.roundLot/scale_offset;
            if(isbuy==true){
                dDealPrice=(floor(nPrice/it->second.minPriceIncrement))*it->second.minPriceIncrement/scale_offset;
            }
            else{
                dDealPrice=(ceil(nPrice/it->second.minPriceIncrement))*it->second.minPriceIncrement/scale_offset;
            }
        }

}

void TDEngineErisX::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitErisX& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId<< " (APIKey)" << unit.api_key<< " (Tid)" << data->InstrumentID<< " (Volume)" << data->Volume<< " (LimitPrice)" << data->LimitPrice<< " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_ERISX, 1/*ISLAST*/, requestId);
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
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_ERISX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    else if(isfuture == 1 && futureisconfig == 0)
    {
        errorId = 66;
        errorMsg = "future is not config in kungfu.json";
        KF_LOG_ERROR(logger, "[req_order_insert]: "<< "(errorId) " << errorId << " (errorMsg) " << errorMsg);        
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        return;
    }
    //KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);
    bool isbuy;
    if(GetSide(data->Direction)=="buy"){
        isbuy=true;
    }
    else if(GetSide(data->Direction)=="sell"){
        isbuy=false;
    }

    double fixedPrice = 0;
    double fixedVolume = 0;
    /*fixedPrice = (data->LimitPrice/scale_offset);
    fixedVolume = ((double)data->Volume/scale_offset);*/
    dealPriceVolume(unit,ticker,data->LimitPrice,data->Volume,fixedPrice,fixedVolume,isbuy);
    KF_LOG_INFO(logger,"fixedPrice="<<fixedPrice<<"fixedVolume"<<fixedVolume);
    
    if(fixedVolume == 0)
    {
        KF_LOG_DEBUG(logger, "[req_order_insert] fixed Volume error" << ticker);
        errorId = 200;
        errorMsg = data->InstrumentID;
        errorMsg += " : quote less than baseMinSize";
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_ERISX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    std::string strClientId1 = genClinetid(data->OrderRef);
    std::string strClientId = partyid + "-" + strClientId1;
    if(!is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        return;
    }
    {
        std::lock_guard<std::mutex> lck(*m_mutexOrder);
        //m_mapInputOrder.insert(std::make_pair(strClientId,*data));
        LFRtnOrderField order;
        memset(&order, 0, sizeof(LFRtnOrderField));
        order.OrderStatus = LF_CHAR_Unknown;
        order.VolumeTotalOriginal = std::round(fixedVolume*scale_offset);
        order.VolumeTotal = order.VolumeTotalOriginal;
        strncpy(order.OrderRef, data->OrderRef, 21);
        strncpy(order.InstrumentID, data->InstrumentID, 31);
        order.RequestID = requestId;
        strcpy(order.ExchangeID, "ErisX");
        strncpy(order.UserID, unit.api_key.c_str(), 16);
        order.LimitPrice = std::round(fixedPrice*scale_offset);
        order.TimeCondition = data->TimeCondition;
        order.Direction = data->Direction;
        order.OrderPriceType = data->OrderPriceType;
        m_mapOrder.insert(std::make_pair(strClientId,order));
        errormap["send"+strClientId1]=*data;
        errormap1["send"+strClientId1]=order;
    }

    std::string type = GetType(data->OrderPriceType);
    if(type == "market"){
        errorId = 100;
        errorMsg = "not suppose market order";
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        return;        
    }else{
        send_order(ticker.c_str(),strClientId.c_str(), GetSide(data->Direction).c_str(),GetType(data->OrderPriceType).c_str(), fixedVolume, fixedPrice,is_post_only(data));   
    }
}

void TDEngineErisX::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitErisX& unit = account_units[account_index];
    //KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId << " (APIKey)" << unit.api_key<< " (Iid)" << data->InvestorID<< " (OrderRef)" << data->OrderRef<< " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_ERISX, 1, requestId);

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
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_ERISX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    //KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);
    std::lock_guard<std::mutex> lck(*m_mutexOrder);
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
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_ERISX, 1, requestId, errorId, errorMsg.c_str());
        return;
    } 
    else {
        remoteOrderId = itr->second;
        /*{
            //std::lock_guard<std::mutex> lck(*m_mutexOrder);
            m_mapOrderAction.insert(std::make_pair(remoteOrderId,*data));
        }*/
        if(!is_connecting){
            errorId = 203;
            errorMsg = "websocket is not connecting,please try again later";
            on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
            return;
        }
        auto it = ordermsg_map.find(remoteOrderId);
        if(it != ordermsg_map.end()){
            KF_LOG_INFO(logger,"cancel_order");
            CancelError cancelerror;
            cancelerror.data = *data;
            cancelerror.requestid = requestId;
            int flag = it->second.clOrdID.find("-");
            std::string correlation = it->second.clOrdID.substr(flag+1,it->second.clOrdID.length());
            error_cancelmap.insert(make_pair("canc"+correlation, cancelerror));
            cancel_order(remoteOrderId,it->second.clOrdID,it->second.origClOrdID,it->second.currency,it->second.side,it->second.symbol);
        }
    }
    
}

//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
void TDEngineErisX::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId)
{
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;
    newOrderActionSent.sentNameTime = getTimestamp();
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    remoteOrderIdOrderActionSentTime[remoteOrderId] = newOrderActionSent;
}


void TDEngineErisX::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineErisX::loop");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineErisX::loopwebsocket, this)));

    //KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineErisX::loopOrderActionNoResponseTimeOut");
    //orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineErisX::loopOrderActionNoResponseTimeOut, this)));
}

void TDEngineErisX::loopwebsocket()
{
		while(isRunning)
		{
            //KF_LOG_INFO(logger, "TDEngineErisX::loop:lws_service");
			lws_service( context, rest_get_interval_ms );
            //延时返回撤单回报
            std::lock_guard<std::mutex> lck(*m_mutexOrder); 
            for(auto canceled_order = m_mapCanceledOrder.begin();canceled_order != m_mapCanceledOrder.end();++canceled_order)
            {
                if(getMSTime() - canceled_order->second >= 1000)
                {// 撤单成功超过1秒时，回报205
                    auto it = m_mapOrder.find(canceled_order->first);
                    if(it != m_mapOrder.end())
                    {
                        on_rtn_order(&(it->second));
                        m_mapOrder.erase(it);
                    }
                    canceled_order = m_mapCanceledOrder.erase(canceled_order);
                    if(canceled_order == m_mapCanceledOrder.end())
                    {
                        break;
                    }
                }
            }
		}
}



void TDEngineErisX::loopOrderActionNoResponseTimeOut()
{
    //KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        orderActionNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void TDEngineErisX::orderActionNoResponseTimeOut()
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
            //KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.erase remoteOrderId)" << itr->first );
            on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
            itr = remoteOrderIdOrderActionSentTime.erase(itr);
        } else {
            ++itr;
        }
    }
//    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}

void TDEngineErisX::printResponse(const Document& d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    //KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
}


void TDEngineErisX::get_account(AccountUnitErisX& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_account]");

    std::string requestPath = "/v1/positions";

    string url = unit.baseUrl + requestPath ;

    std::string strTimestamp = getTimestampStr();

    std::string strSignatrue = sign(unit,"GET",strTimestamp,requestPath);
    cpr::Header mapHeader = cpr::Header{{"ERISX-ACCESS-SIG",strSignatrue},
                                        {"ERISX-ACCESS-TIMESTAMP",strTimestamp},
                                        {"ERISX-ACCESS-KEY",unit.api_key}};
     KF_LOG_INFO(logger, "ERISX-ACCESS-SIG = " << strSignatrue 
                        << ", ERISX-ACCESS-TIMESTAMP = " << strTimestamp 
                        << ", ERISX-API-KEY = " << unit.api_key);


    std::unique_lock<std::mutex> lock(g_httpMutex);
    const auto response = cpr::Get(Url{url}, 
                             Header{mapHeader}, Timeout{10000} );
    lock.unlock();
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
std::string TDEngineErisX::createInsertOrderString(const char *code,const char* strClientId,const char *side, const char *type, double& size, double& price,bool is_post_only)
{
    //KF_LOG_INFO(logger, "[TDEngineErisX::createInsertOrdertring]:(price)"<<price << "(volume)" << size);
    std::string currency = code;
    int flag = currency.find("/");
    currency = currency.substr(0,flag);
    std::string correlation = strClientId;
    int flag1 = correlation.find("-");
    correlation = "send" + correlation.substr(flag1+1,correlation.length());
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("correlation");
    writer.String(correlation.c_str());
    writer.Key("type");
    writer.String("NewLimitOrderSingle");
    writer.Key("clOrdID");
    writer.String(strClientId);
    writer.Key("currency");
    writer.String(currency.c_str());
    writer.Key("side");
    writer.String(side);
    writer.Key("symbol");
    writer.String(code);
    writer.Key("partyID");
    writer.String(partyid.c_str());
    writer.Key("orderQty");
    writer.Double(size);
    writer.Key("ordType");
    writer.String("LIMIT");
    writer.Key("timeInForce");
    writer.String("GoodTillCancel");

    if(isfuture == 1)
    {
        writer.Key("targetLocationId");
        writer.String(senderLocationId.c_str());
        writer.Key("custOrderCapacity");
        writer.Int(custOrderCapacity);
        writer.Key("accountType");
        writer.Int(accountType);
        writer.Key("targetSubId");
        writer.String(senderSubId.c_str());        
    }
    writer.EndObject();
    std::string strOrder = s.GetString();
    std::stringstream ss;
    strOrder.pop_back();
    ss << strOrder;
    ss << ",\"price\":" << price << "}";
    strOrder = ss.str();
    //KF_LOG_INFO(logger, "[TDEngineErisX::createInsertOrdertring]:" << strOrder);
    return strOrder;
}

void TDEngineErisX::send_order(const char *code,const char* strClientId,const char *side, const char *type, double& size, double& price,bool is_post_only)
{
    KF_LOG_INFO(logger, "[send_order]");
    {
        std::string new_order = createInsertOrderString(code, strClientId,side, type, size, price,is_post_only);
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        m_vstMsg.push(new_order);
        lws_callback_on_writable(m_conn);
    }
}
    
/*
void TDEngineErisX::cancel_all_orders()
{
    KF_LOG_INFO(logger, "[cancel_all_orders]");

    std::string cancel_order = createCancelOrderString(nullptr);
    //std::lock_guard<std::mutex> lck(mutex_msg_queue);
    m_vstMsg.push(cancel_order);
    //lws_callback_on_writable(m_conn);
}*/
std::string TDEngineErisX::createCancelOrderString(std::string strOrderId,std::string clOrdID,std::string origClOrdID,std::string currency,std::string side,std::string symbol)
{
    //KF_LOG_INFO(logger, "[TDEngineErisX::createCancelOrderString]");
    int flag = clOrdID.find("-");
    std::string correlation = clOrdID.substr(flag+1,clOrdID.length());
    correlation = "canc" + correlation;
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("correlation");
    writer.String(correlation.c_str());
    writer.Key("type");
    writer.String("CancelLimitOrderSingleRequest");
    writer.Key("partyID");
    writer.String(partyid.c_str());
    writer.Key("clOrdID");
    writer.String(clOrdID.c_str());
    writer.Key("origClOrdID");
    writer.String(origClOrdID.c_str());
    writer.Key("orderID");
    writer.String(strOrderId.c_str());
    writer.Key("currency");
    writer.String(currency.c_str());
    writer.Key("side");
    writer.String(side.c_str());
    writer.Key("symbol");
    writer.String(symbol.c_str());

    writer.EndObject();
    std::string strOrder = s.GetString();
    //KF_LOG_INFO(logger, "[TDEngineErisX::createCancelOrderString]:" << strOrder);
    return strOrder;
}

void TDEngineErisX::cancel_order(std::string orderId,std::string clOrdID,std::string origClOrdID,std::string currency,std::string side,std::string symbol)
{
    //KF_LOG_INFO(logger, "[cancel_order]");
    std::string cancel_order = createCancelOrderString(orderId,clOrdID,origClOrdID,currency,side,symbol);
    std::lock_guard<std::mutex> lck(mutex_msg_queue);
    m_vstMsg.push(cancel_order);
    lws_callback_on_writable(m_conn);
}



std::string TDEngineErisX::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


inline int64_t TDEngineErisX::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}



#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

BOOST_PYTHON_MODULE(liberisxtd)
{
    using namespace boost::python;
    class_<TDEngineErisX, boost::shared_ptr<TDEngineErisX> >("Engine")
            .def(init<>())
            .def("init", &TDEngineErisX::initialize)
            .def("start", &TDEngineErisX::start)
            .def("stop", &TDEngineErisX::stop)
            .def("logout", &TDEngineErisX::logout)
            .def("wait_for_stop", &TDEngineErisX::wait_for_stop);
}



