#include "TDEngineCoinflex.h"
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
using cpr::Delete;

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
using utils::crypto::hmac_sha224;
using utils::crypto::hmac_sha224_byte;
using utils::crypto::base64_encode;
using utils::crypto::base64_decode;
using utils::crypto::b2a_hex;
using utils::crypto::sha224digest;
USING_WC_NAMESPACE

std::mutex mutex_msg_queue;
std::mutex g_httpMutex;
std::mutex order_mutex;//1
std::mutex local_mutex;
std::mutex idtance_mutex;
std::mutex insert_time_mutex;
std::mutex insert_time1_mutex;
std::mutex action_time_mutex;
std::mutex action_time1_mutex;
std::mutex wsmsg_mutex;
TDEngineCoinflex::TDEngineCoinflex(): ITDEngine(SOURCE_COINFLEX)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.COINFLEX");
    KF_LOG_INFO(logger, "[TDEngineCoinflex]");

    m_mutexOrder = new std::mutex();
    mutex_order_and_trade = new std::mutex();
    mutex_response_order_status = new std::mutex();
    mutex_orderaction_waiting_response = new std::mutex();
    mutex_orderinsert_waiting_response = new std::mutex();
}

TDEngineCoinflex::~TDEngineCoinflex()
{
    if(m_mutexOrder != nullptr) delete m_mutexOrder;
    if(mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if(mutex_response_order_status != nullptr) delete mutex_response_order_status;
    if(mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
    if(mutex_orderinsert_waiting_response != nullptr) delete mutex_orderinsert_waiting_response;
}

static TDEngineCoinflex* global_md = nullptr;

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

std::string TDEngineCoinflex::getTimestampStr()
{
    //long long timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  std::to_string(getMSTime());
}


 void TDEngineCoinflex::onerror(Document& msg){
        KF_LOG_INFO(logger,"[onerror]");
        string errmsg=msg["error_msg"].GetString();
        int64_t cid=msg["tag"].GetInt64();
        std::string cidstr = std::to_string(cid);
        int requestid;
        std::unique_lock<std::mutex> lck4(insert_time_mutex);
        auto itr = remoteOrderIdOrderInsertSentTime.find(cidstr);
        if(itr != remoteOrderIdOrderInsertSentTime.end()){
            remoteOrderIdOrderInsertSentTime.erase(itr);
        }
        lck4.unlock();

        auto it1 = error_requestid_map.find(cid);
        if(it1 != error_requestid_map.end()){
            KF_LOG_INFO(logger,"[error_requestid_map]");
            requestid=it1->second.RequestID;
            error_requestid_map.erase(it1);
        }

        auto it = errormap.find(cid);
        if(it != errormap.end())
        {
            KF_LOG_INFO(logger,"[errormap]");           
            on_rsp_order_insert(&(it->second),requestid,100,errmsg.c_str());
            errormap.erase(it);
        }
 }

void TDEngineCoinflex::DealNottouch(std::string toncestr,std::string strOrderId)
{
    KF_LOG_INFO(logger,"TDEngineCoinflex::DealNottouch");
    std::unique_lock<std::mutex> lck4(insert_time_mutex);
    auto itr = remoteOrderIdOrderInsertSentTime.find(toncestr);
    if(itr != remoteOrderIdOrderInsertSentTime.end()){
        remoteOrderIdOrderInsertSentTime.erase(itr);
    }
    lck4.unlock();
    std::unique_lock<std::mutex> lck1(order_mutex);
    auto iter3 = m_mapOrder.find(toncestr);
    if(iter3 != m_mapOrder.end())
    {
        KF_LOG_DEBUG(logger, "straction=open in");
        std::unique_lock<std::mutex> lck2(local_mutex);
        localOrderRefRemoteOrderId.insert(std::make_pair(iter3->second.OrderRef,strOrderId));
        lck2.unlock();
        //if(iter3->second.OrderStatus==LF_CHAR_NotTouched){
        iter3->second.OrderStatus=LF_CHAR_NotTouched;
        on_rtn_order(&(iter3->second));
        //}
    }
    lck1.unlock();             
}

void TDEngineCoinflex::DealTrade(std::string toncestr,std::string idstr,int64_t price,uint64_t volume,int type)
{
    //int64_t price=msg["price"].GetInt64();
    //uint64_t volume=msg["quantity"].GetInt();
    volume=volume*1e4;
    std::unique_lock<std::mutex> lck1(order_mutex);
    auto it4 = m_mapOrder.find(toncestr);
    if(it4 != m_mapOrder.end())
    {
        KF_LOG_DEBUG(logger, "straction=filled in");
        //it4->second.OrderStatus = LF_CHAR_AllTraded;
                            
        it4->second.VolumeTraded += volume;
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
        strncpy(rtn_trade.OrderSysID,idstr.c_str(),sizeof(rtn_trade.OrderSysID));
        rtn_trade.Volume = std::round(it4->second.VolumeTraded);
        rtn_trade.Price = (price*1e4);
        if(type==1){
            WSmsg wsmsg;
            wsmsg.price = rtn_trade.Price;
            wsmsg.volume = rtn_trade.Volume;
            wsmsg.orderid = idstr;
            std::unique_lock<std::mutex> lck8(wsmsg_mutex);
            WSmsg_vec.push_back(wsmsg);
            lck8.unlock();
        }
        on_rtn_trade(&rtn_trade);
        raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
                                source_id, MSG_TYPE_LF_RTN_TRADE_COINFLEX, 1, -1);

        if(it4->second.OrderStatus == LF_CHAR_AllTraded)
        {
            std::unique_lock<std::mutex> lck2(local_mutex);
            auto it_id = localOrderRefRemoteOrderId.find(it4->second.OrderRef);
            if(it_id != localOrderRefRemoteOrderId.end())
            {
                KF_LOG_INFO(logger,"earse local");
                localOrderRefRemoteOrderId.erase(it_id);
            }
            lck2.unlock();
            m_mapOrder.erase(it4);
            std::unique_lock<std::mutex> lck6(action_time_mutex);
            auto it5 = remoteOrderIdOrderActionSentTime.find(idstr);
            if(it5 != remoteOrderIdOrderActionSentTime.end())
            {    
                remoteOrderIdOrderActionSentTime.erase(it5);
            }
            lck6.unlock();
            std::unique_lock<std::mutex> lck3(idtance_mutex);
            auto it1 = idtonce_map.find(idstr);
            if(it1 != idtonce_map.end())
            {    
                idtonce_map.erase(it1);
            }  
            lck3.unlock();             
        }
    }
    lck1.unlock();
}

void TDEngineCoinflex::onOrderOpen(Document& msg)
{
    KF_LOG_INFO(logger,"TDEngineCoinflex::onOrderOpen");
    int64_t tonce;
    if(msg["tonce"].IsNumber()){
        tonce = msg["tonce"].GetInt64();
    }
    std::string toncestr = std::to_string(tonce);
    int64_t id = msg["id"].GetInt64();
    std::string idstr = std::to_string(id);
    std::unique_lock<std::mutex> lck3(idtance_mutex);
    idtonce_map.insert(std::make_pair(idstr,toncestr));
    lck3.unlock();
    DealNottouch(toncestr,idstr);
}

void TDEngineCoinflex::onLimitOrderMatch(Document& msg)
{
    KF_LOG_INFO(logger,"TDEngineCoinflex::onLimitOrderMatch");
    int64_t id,tonce;
    std::string idstr,toncestr;
    if(msg.HasMember("ask_tonce")&&msg["ask_tonce"].IsNumber()){
        id = msg["ask"].GetInt64();        
        tonce = msg["ask_tonce"].GetInt64();        
    }
    else if(msg.HasMember("bid_tonce")&&msg["bid_tonce"].IsNumber()){
        id = msg["bid"].GetInt64();
        tonce = msg["bid_tonce"].GetInt64();
    }
    idstr = std::to_string(id);
    toncestr = std::to_string(tonce);
    int64_t price = msg["price"].GetInt64();
    uint64_t volume = msg["quantity"].GetInt64();
    DealTrade(toncestr,idstr,price,volume,1);
}

void TDEngineCoinflex::onMarketOrderMatch(Document& msg)
{
    KF_LOG_INFO(logger,"TDEngineCoinflex::onMarketOrderMatch");
    int64_t id,tonce;
    std::string idstr,toncestr;
    if(msg.HasMember("ask_tonce")&&msg["ask_tonce"].IsNumber()){        
        tonce = msg["ask_tonce"].GetInt64();        
    }
    else if(msg.HasMember("bid_tonce")&&msg["bid_tonce"].IsNumber()){
        tonce = msg["bid_tonce"].GetInt64();
    }
    toncestr = std::to_string(tonce);
    DealNottouch(toncestr,idstr);
    int64_t price = msg["price"].GetInt64();
    uint64_t volume = msg["quantity"].GetInt64();
    DealTrade(toncestr,idstr,price,volume,1);    
}

void TDEngineCoinflex::onOrderClose(Document& msg)
{
    KF_LOG_INFO(logger,"TDEngineCoinflex::onOrderClose");
    int64_t tonce;
    if(msg["tonce"].IsNumber()){
        tonce = msg["tonce"].GetInt64(); 
    } 
    std::string toncestr = std::to_string(tonce);
    int64_t id = msg["id"].GetInt64();
    std::string idstr = std::to_string(id);
    std::unique_lock<std::mutex> lck1(order_mutex);
    auto itr = m_mapOrder.find(toncestr);
    if(itr != m_mapOrder.end()){
        if(itr->second.OrderStatus==LF_CHAR_NotTouched||itr->second.OrderStatus==LF_CHAR_PartTradedQueueing){
            itr->second.OrderStatus=LF_CHAR_Canceled;
            on_rtn_order(&(itr->second));
            std::unique_lock<std::mutex> lck2(local_mutex);
            auto it_id = localOrderRefRemoteOrderId.find(itr->second.OrderRef);
            if(it_id != localOrderRefRemoteOrderId.end())
            {
                localOrderRefRemoteOrderId.erase(it_id);
            }
            lck2.unlock();
            m_mapOrder.erase(itr);
            std::unique_lock<std::mutex> lck6(action_time_mutex);
            auto it = remoteOrderIdOrderActionSentTime.find(idstr);
            if(it != remoteOrderIdOrderActionSentTime.end()){
                remoteOrderIdOrderActionSentTime.erase(it);
            }
            lck6.unlock();
            std::unique_lock<std::mutex> lck3(idtance_mutex);
            auto it1 = idtonce_map.find(idstr);
            if(it1 != idtonce_map.end()){
                idtonce_map.erase(it1);
            }   
            lck3.unlock();                     
        }                                               
    } 
    lck1.unlock();   
}

void TDEngineCoinflex::onOrderAction(Document& msg)
{
                KF_LOG_INFO(logger,"300001");
                int flag = 0;
                Value &node = msg["orders"];
                int size = node.Size();
                for(int i=0;i<size;i++){
                    int64_t id = node.GetArray()[i]["id"].GetInt64();
                    std::string idstr = std::to_string(id);
                    std::unique_lock<std::mutex> lck7(action_time1_mutex);
                    auto it = remoteOrderIdOrderActionSentTime1.find(idstr);
                    if(it != remoteOrderIdOrderActionSentTime1.end()){
                        flag = 1;
                        int errorId = 100;
                        std::string errorMsg="OrderAction has none response for a long time, please send OrderAction again";
                        on_rsp_order_action(&it->second.data, it->second.requestId, errorId, errorMsg.c_str());
                        remoteOrderIdOrderActionSentTime1.erase(it);
                    }
                    lck7.unlock();
                }
                if(flag == 0){
                    int flag1 = 0;
                    std::string url = "https://webapi.coinflex.com/trades/?limit=25";
                    string apikey = api_key;
                    //string Message = "348/"+apikey+":wMH%7y$@r";
                    string Message = userid+"/"+apikey+":"+passphrase;
                    string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
                    const auto response = Get(Url{url}, 
                                            Header{{"Authorization","Basic "+payload},
                                                   {"Content-Type", "application/json"}},Timeout{10000}  );
                    KF_LOG_INFO(logger, "[Get] (url) " << url << " (response.status_code) " << response.status_code <<
                        " (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str());
                    Document json;
                    json.Parse(response.text.c_str());   
                    int len = json.Size();
                    KF_LOG_INFO(logger,"len="<<len);
                    for(int i=0;i<len;i++){
                        int64_t id;
                        if(json.GetArray()[i]["order_id"].IsNumber()){
                            id = json.GetArray()[i]["order_id"].GetInt64();
                        }
                        //KF_LOG_INFO(logger,"idtrue");
                        std::string idstr = std::to_string(id);
                        int64_t price=json.GetArray()[i]["price"].GetInt64();
                        KF_LOG_INFO(logger,"pricetrue");
                        uint64_t volume=json.GetArray()[i]["quantity"].GetInt64();
                        KF_LOG_INFO(logger,"volumetrue");
                        if(volume<0){
                            volume=-volume;
                        }
                        std::vector<WSmsg>::iterator wsit;
                        std::unique_lock<std::mutex> lck8(wsmsg_mutex);
                        for(wsit=WSmsg_vec.begin();wsit!=WSmsg_vec.end();){
                            if(wsit->orderid==idstr&&wsit->price==price*1e4&&wsit->volume==volume*1e4){
                                KF_LOG_INFO(logger,"erase wsit");
                                wsit = WSmsg_vec.erase(wsit);
                            }
                            else{
                                std::unique_lock<std::mutex> lck7(action_time1_mutex);
                                auto it1 = remoteOrderIdOrderActionSentTime1.find(idstr);
                                if(it1 != remoteOrderIdOrderActionSentTime1.end()){
                                    //trade
                                    flag1 = 1;
                                    //std::string oid = it1->first;

                                    std::string toncestr;
                                    std::unique_lock<std::mutex> lck3(idtance_mutex);
                                    auto it2 = idtonce_map.find(idstr);
                                    if(it2 != idtonce_map.end()){
                                        toncestr = it2->second;
                                    }
                                    lck3.unlock();
                                    KF_LOG_INFO(logger,"toncestr="<<toncestr);
                                    DealTrade(toncestr,idstr,price,volume,2);                            
                                    remoteOrderIdOrderActionSentTime1.erase(it1);
                                }
                                lck7.unlock();
                                wsit++;
                            }
                        }
                        lck8.unlock();
                    }
                    if(flag1 == 0){//canceled
                        KF_LOG_INFO(logger,"canceled in");
                        std::map<std::string, OrderActionSentTime>::iterator itr;
                        std::unique_lock<std::mutex> lck7(action_time1_mutex);
                        for(itr = remoteOrderIdOrderActionSentTime1.begin(); itr != remoteOrderIdOrderActionSentTime1.end();){
                            std::string oid = itr->first;
                            std::string toncestr;
                            std::unique_lock<std::mutex> lck3(idtance_mutex);
                            auto it3 = idtonce_map.find(oid);
                            if(it3 != idtonce_map.end()){
                                toncestr = it3->second;
                                idtonce_map.erase(it3);
                            }
                            lck3.unlock();
                            KF_LOG_INFO(logger,"toncestr="<<toncestr);
                            std::unique_lock<std::mutex> lck1(order_mutex);
                            auto it4 = m_mapOrder.find(toncestr);
                            if(it4 != m_mapOrder.end())
                            {
                                KF_LOG_INFO(logger,"LF_CHAR_Canceled"<<it4->second.BusinessUnit);
                                it4->second.OrderStatus = LF_CHAR_Canceled;
                                on_rtn_order(&(it4->second));
                                std::unique_lock<std::mutex> lck2(local_mutex);
                                auto it_id = localOrderRefRemoteOrderId.find(it4->second.OrderRef);
                                if(it_id != localOrderRefRemoteOrderId.end())
                                {
                                    localOrderRefRemoteOrderId.erase(it_id);
                                }
                                lck2.unlock();
                                m_mapOrder.erase(it4);
                            }
                            lck1.unlock();
                            
                            itr = remoteOrderIdOrderActionSentTime1.erase(itr);                            
                        }
                        lck7.unlock();
                    }                
                }

}

void TDEngineCoinflex::onOrderInsert(Document& msg)
{
                KF_LOG_INFO(logger,"300002");
                int flag = 0;
                Value &node = msg["orders"];
                int size = node.Size();
                for(int i=0;i<size;i++){
                    int64_t tonce;
                    if(node.GetArray()[i]["tonce"].IsNumber()){
                        tonce = node.GetArray()[i]["tonce"].GetInt64();
                    }
                    std::string toncestr = std::to_string(tonce);
                    int64_t id = node.GetArray()[i]["id"].GetInt64();
                    std::string strOrderId=std::to_string(id);
                    std::unique_lock<std::mutex> lck5(insert_time1_mutex);
                    auto it = remoteOrderIdOrderInsertSentTime1.find(toncestr);
                    if(it != remoteOrderIdOrderInsertSentTime1.end()){
                        flag = 1;
                        KF_LOG_INFO(logger,"nottouch");
                        DealNottouch(toncestr,strOrderId);                    
                        remoteOrderIdOrderInsertSentTime1.erase(it);
                    }
                    lck5.unlock();
                }
                if(flag == 0){
                    int flag1 = 0;
                    std::string url = "https://webapi.coinflex.com/trades/?limit=25";
                    string apikey = api_key;
                    //string Message = "348/"+apikey+":wMH%7y$@r";
                    string Message = userid+"/"+apikey+":"+passphrase;
                    string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
                    const auto response = Get(Url{url}, 
                                            Header{{"Authorization","Basic "+payload},
                                                   {"Content-Type", "application/json"}},Timeout{10000}  );
                    KF_LOG_INFO(logger, "[Get] (url) " << url << " (response.status_code) " << response.status_code <<
                        " (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str());
                    Document json;
                    json.Parse(response.text.c_str());   
                    int len = json.Size();
                    KF_LOG_INFO(logger,"len="<<len);
                    for(int i=0;i<len;i++){
                        int64_t id;
                        if(json.GetArray()[i]["order_id"].IsNumber()){
                            id = json.GetArray()[i]["order_id"].GetInt64();
                        }
                        //KF_LOG_INFO(logger,"idtrue");
                        std::string idstr = std::to_string(id);
                        std::string toncestr2;
                        if(json.GetArray()[i].HasMember("tonce")&&json.GetArray()[i]["tonce"].IsNumber()){
                            int64_t tonce = json.GetArray()[i]["tonce"].GetInt64();
                            toncestr2 = std::to_string(tonce);
                        }
                        std::unique_lock<std::mutex> lck5(insert_time1_mutex);
                        auto it1 = remoteOrderIdOrderInsertSentTime1.find(toncestr2);
                        if(it1 != remoteOrderIdOrderInsertSentTime1.end()){
                            //trade
                            flag1 = 1;
                            DealNottouch(toncestr2,idstr);                          
                            //std::string oid = it1->first;
                            int64_t price=json.GetArray()[i]["price"].GetInt64();
                            KF_LOG_INFO(logger,"pricetrue");
                            uint64_t volume=json.GetArray()[i]["quantity"].GetInt64();
                            KF_LOG_INFO(logger,"volumetrue");
                            if(volume<0){
                                volume=-volume;
                            }
                            DealTrade(toncestr2,idstr,price,volume,2);                            
                            remoteOrderIdOrderInsertSentTime1.erase(it1);
                        }
                        lck5.unlock();
                    }
                    if(flag1 == 0){//canceled
                        KF_LOG_INFO(logger,"canceled in");
                        std::unique_lock<std::mutex> lck5(insert_time1_mutex);
                        KF_LOG_INFO(logger,"sent11="<<remoteOrderIdOrderInsertSentTime1.size());
                        std::map<std::string, OrderInsertSentTime>::iterator itr;
                        for(itr = remoteOrderIdOrderInsertSentTime1.begin(); itr != remoteOrderIdOrderInsertSentTime1.end();){
                            std::string toncestr = itr->first;
                            std::string idstr;
                            std::map<std::string, std::string>::iterator itr1;
                            std::unique_lock<std::mutex> lck3(idtance_mutex);
                            for(itr1 = idtonce_map.begin();itr1 != idtonce_map.end();){
                                if(itr1->second == toncestr){
                                    idstr = itr1->first;
                                    itr1 = idtonce_map.erase(itr1);
                                    break;
                                }else{
                                    itr1++;
                                }
                            }
                            /*auto it5 = idtonce_map.find(idstr);
                            if(it5 != idtonce_map.end()){
                                idtonce_map.erase(it5);
                            }*/
                            lck3.unlock();
                            KF_LOG_INFO(logger,"idstr="<<idstr);
                            DealNottouch(toncestr,idstr);
                            std::unique_lock<std::mutex> lck1(order_mutex);
                            auto it4 = m_mapOrder.find(toncestr);
                            if(it4 != m_mapOrder.end())
                            {
                                KF_LOG_INFO(logger,"LF_CHAR_Canceled"<<it4->second.BusinessUnit);
                                it4->second.OrderStatus = LF_CHAR_Canceled;
                                on_rtn_order(&(it4->second));
                                std::unique_lock<std::mutex> lck2(local_mutex);
                                auto it_id = localOrderRefRemoteOrderId.find(it4->second.OrderRef);
                                if(it_id != localOrderRefRemoteOrderId.end())
                                {
                                    localOrderRefRemoteOrderId.erase(it_id);
                                }
                                lck2.unlock();
                                m_mapOrder.erase(it4);
                            }
                            lck1.unlock();

                            itr = remoteOrderIdOrderInsertSentTime1.erase(itr);                            
                        }
                        lck5.unlock();
                    }                
                }    
}

void TDEngineCoinflex::onOrderCut(Document& msg)
{
    KF_LOG_INFO(logger,"300003");
    int flag = 0;
    Value &node = msg["orders"];
    int size = node.Size();
    for(int i=0;i<size;i++){
        int64_t tonce;
        if(node.GetArray()[i]["tonce"].IsNumber()){
            tonce = node.GetArray()[i]["tonce"].GetInt64();
        }
        std::string toncestr = std::to_string(tonce);
        std::unique_lock<std::mutex> lck1(order_mutex);
        auto it = m_mapOrder.find(toncestr);
        if(it != m_mapOrder.end()){
            flag = 1;
            KF_LOG_INFO(logger,"is open");
        } 
        lck1.unlock();       
    } 
    if(flag == 0){
        int flag1 = 0;
        std::string url = "https://webapi.coinflex.com/trades/?limit=25";
        string apikey = api_key;
        //string Message = "348/"+apikey+":wMH%7y$@r";
        string Message = userid+"/"+apikey+":"+passphrase;
        string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
        const auto response = Get(Url{url}, 
                                Header{{"Authorization","Basic "+payload},
                                       {"Content-Type", "application/json"}},Timeout{10000}  );
        KF_LOG_INFO(logger, "[Get] (url) " << url << " (response.status_code) " << response.status_code <<
            " (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str());
        if (response.status_code == 200){
            Document json;
            json.Parse(response.text.c_str()); 
            if(json.IsArray()){            
                int len = json.Size();
                KF_LOG_INFO(logger,"len="<<len);
                for(int i=0;i<len;i++){
                    int64_t id;
                    if(json.GetArray()[i]["order_id"].IsNumber()){
                        id = json.GetArray()[i]["order_id"].GetInt64();
                    }
                    //KF_LOG_INFO(logger,"idtrue");
                    std::string idstr = std::to_string(id);
                    std::string toncestr2;
                    if(json.GetArray()[i].HasMember("tonce")&&json.GetArray()[i]["tonce"].IsNumber()){
                        int64_t tonce = json.GetArray()[i]["tonce"].GetInt64();
                        toncestr2 = std::to_string(tonce);
                    }
                    int64_t price=json.GetArray()[i]["price"].GetInt64();
                    KF_LOG_INFO(logger,"pricetrue");
                    uint64_t volume=json.GetArray()[i]["quantity"].GetInt64();
                    KF_LOG_INFO(logger,"volumetrue");
                    if(volume<0){
                        volume=-volume;
                    }
                    std::vector<WSmsg>::iterator wsit;
                    std::unique_lock<std::mutex> lck8(wsmsg_mutex);
                    for(wsit=WSmsg_vec.begin();wsit!=WSmsg_vec.end();){
                        if(wsit->orderid==idstr&&wsit->price==price*1e4&&wsit->volume==volume){
                            wsit = WSmsg_vec.erase(wsit);
                        }
                        else{                  
                            auto it1 = m_mapOrder.find(toncestr2);
                            if(it1 != m_mapOrder.end()){
                                //trade
                                flag1 = 1;

                                DealTrade(toncestr2,idstr,price,volume,2);                            
                                m_mapOrder.erase(it1);
                            }
                        
                            wsit++;
                        }
                    }
                    lck8.unlock();
                }
            }
        }
        if(flag1 == 0){//canceled
            KF_LOG_INFO(logger,"canceled in");
            std::map<std::string, LFRtnOrderField>::iterator itr;
            std::unique_lock<std::mutex> lck1(order_mutex);
            KF_LOG_INFO(logger,"size="<<m_mapOrder.size());
            for(itr = m_mapOrder.begin(); itr != m_mapOrder.end();){
                std::string toncestr = itr->first;
                std::string idstr;
                std::map<std::string, std::string>::iterator itr1;
                std::unique_lock<std::mutex> lck3(idtance_mutex);
                for(itr1 = idtonce_map.begin();itr1 != idtonce_map.end();){
                    if(itr1->second == toncestr){
                        idstr = itr1->first;
                        itr1 = idtonce_map.erase(itr1);
                        break;
                    }else{
                        itr1++;
                    }
                }
                KF_LOG_INFO(logger,"idstr="<<idstr);
                /*auto it5 = idtonce_map.find(idstr);
                if(it5 != idtonce_map.end()){
                    idtonce_map.erase(it5);
                }*/
                lck3.unlock();

                itr->second.OrderStatus = LF_CHAR_Canceled;
                on_rtn_order(&(itr->second));
                std::unique_lock<std::mutex> lck2(local_mutex);
                auto it_id = localOrderRefRemoteOrderId.find(itr->second.OrderRef);
                if(it_id != localOrderRefRemoteOrderId.end())
                {
                    localOrderRefRemoteOrderId.erase(it_id);
                }
                lck2.unlock();
                itr = m_mapOrder.erase(itr);                            
            }
            lck1.unlock();
        }                
    }          
}

void TDEngineCoinflex::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    //std::string strData = dealDataSprit(data);
    KF_LOG_INFO(logger, "TDEngineCoinflex::on_lws_data: " << data);
    Document json;
    json.Parse(data);
    string openstr="OrderOpened";
    string matchstr="OrdersMatched";
    string closestr="OrderClosed";
    if(!json.HasParseError() && json.IsObject())
    {
        if(json.HasMember("nonce")&&json.HasMember("notice")){
            recnonce=json["nonce"].GetString();
            //get_auth(nonce);
            welcome=true;
        }
        else if(json.HasMember("error_code") && json["error_code"].GetInt()==0 && json["tag"].GetInt64()==100000000)   
        {
            m_isSubOK = true;
            KF_LOG_INFO(logger,"m_isSubOK");
        }
        else if(json.HasMember("error_code") && json["error_code"].GetInt()!=0 && json["tag"].GetInt64()<100000000)
        {
            onerror(json);
        }
        else if(json.HasMember("error_code") && json["error_code"].GetInt()==0 && json["tag"].GetInt64()==300000001)
        {
           //KF_LOG_INFO(logger,"into onOrderChange");
           onOrderAction(json);
        }
        else if(json.HasMember("error_code") && json["error_code"].GetInt()==0 && json["tag"].GetInt64()==300000002)
        {
           //KF_LOG_INFO(logger,"into onOrderChange");
           onOrderInsert(json);
        }
        else if(json.HasMember("error_code") && json["error_code"].GetInt()==0 && json["tag"].GetInt64()==300000003)
        {
           //KF_LOG_INFO(logger,"into onOrderChange");
           onOrderCut(json);
        }
        else if(json.HasMember("notice")&&json["notice"].GetString()==openstr){
            onOrderOpen(json);
        }
        else if(json.HasMember("notice")&&json["notice"].GetString()==matchstr){
            if(json.HasMember("ask")&&json.HasMember("bid")){
                onLimitOrderMatch(json);
            }
            else{
                onMarketOrderMatch(json);
            }
        }
        else if(json.HasMember("notice")&&json["notice"].GetString()==closestr){
            onOrderClose(json);
        }
    } else 
    {
        KF_LOG_ERROR(logger, "MDEngineCoinflex::on_lws_data . parse json error");
    }
    
}

std::string TDEngineCoinflex::makeSubscribetradeString(AccountUnitCoinflex& unit)
{
    string api_key = unit.api_key;
    string secret_key = unit.secret_key;
    std::string strTime = getTimestampStr();
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("tag");
    writer.Int64(600000000);
    writer.Key("method");
    writer.String("WatchOrders");
    writer.Key("base");
    writer.Int(51207);
    writer.Key("counter");
    writer.Int(51887);
    writer.Key("watch");
    writer.Bool(true);
    writer.EndObject(); 
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}

std::string TDEngineCoinflex::makeSubscribeChannelString(AccountUnitCoinflex& unit)
{
    int iuserid = stoi(userid);
    std::string strTime = getTimestampStr();
    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();
    writer.Key("tag");
    writer.Int64(100000000);
    writer.Key("method");
    writer.String("Authenticate");
    writer.Key("user_id");
    writer.Int(iuserid);
    writer.Key("cookie");
    writer.String(api_key.c_str());
    writer.Key("nonce");
    writer.String("8IyYyvH9gujOqYJdv/BP0A==");
    writer.Key("signature");
    writer.StartArray();
    unsigned char * keySignatrue = key_sign();
    unsigned char * msgSignatrue = tomsg_sign(recnonce);
    std::string strSignatrue1;
    std::string strSignatrue2;

    mp_limb_t r[MP_NLIMBS(29)], s[MP_NLIMBS(29)], d[MP_NLIMBS(29)], z[MP_NLIMBS(29)];
    uint8_t rb[28], sb[28], db[28], zb[28];
    unsigned char d1[28], z1[28], r1[28], s1[28];
    //char r1[28], s1[28];
    for(int i=0;i<28;i++){
       d1[i]=*keySignatrue;
       z1[i]=*msgSignatrue;
       //KF_LOG_INFO(logger, "d[" << i<<"]"<<d1[i]);
       keySignatrue++;
       msgSignatrue++;
    }

    for(int i=0;i<28;i++){
        db[i]=(uint8_t)d1[i];
        zb[i]=(uint8_t)z1[i];
    }

#if GMP_LIMB_BITS == 32
    z[sizeof *z / sizeof z - 1] = d[sizeof *d / sizeof d - 1] = 0;
#endif

    bytes_to_mpn(d, db, sizeof db);
    bytes_to_mpn(z, zb, sizeof zb);
    ecp_sign(r, s, secp224k1_p, secp224k1_a, *secp224k1_G, secp224k1_n, d, z, MP_NLIMBS(29));
    mpn_to_bytes(rb, r, sizeof rb);
    mpn_to_bytes(sb, s, sizeof sb);
  
    for(int i=0;i<28;i++){
        r1[i]=(unsigned char)rb[i];
        s1[i]=(unsigned char)sb[i];
    }
    unsigned char* sign1=r1;
    unsigned char* sign2=s1;

    strSignatrue1=base64_encode(sign1,28);
    strSignatrue2=base64_encode(sign2,28);        

    writer.String(strSignatrue1.c_str());
    writer.String(strSignatrue2.c_str());

    writer.EndArray();
    writer.EndObject();
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}

unsigned char * TDEngineCoinflex::tomsg_sign(const std::string& re_nonce)
 {
    int iuserid = stoi(userid);
    string decode_nonce = base64_decode(re_nonce);
    std::string random = "8IyYyvH9gujOqYJdv/BP0A==";
    random=base64_decode(random);
    char* ran=(char*)random.data();
    string hex_random=b2a_hex((char*)ran,16);
    KF_LOG_ERROR(logger, "random:"<<hex_random);
    char client[16]; 
    memcpy(client, ran, sizeof(client));   

    char* recnonce=(char*)decode_nonce.data();
    int decode_nonce_size=decode_nonce.size();
    std::string hex_decode_nonce=b2a_hex((char*)recnonce,16);
    char server[16];
    memcpy(server, recnonce, sizeof(server));
    KF_LOG_INFO(logger,"decode_nonce_size"<<decode_nonce_size);

    uint64_t user_id=iuserid;
    if(is_big_endian()==false){
        user_id=swap_uint64(user_id);
    }
    KF_LOG_INFO(logger,"user_id"<<user_id);
    char buf[8];
    memset(buf,0,sizeof(buf));
    memcpy(buf,&user_id,sizeof(user_id));

    char msgbuf[40];
    for(int i=0;i<8;i++){
        msgbuf[i]=buf[i];
    }
    for(int i=8;i<24;i++){
        msgbuf[i]=server[i-8];
    }
    for(int i=24;i<40;i++){
        msgbuf[i]=client[i-24];
    }
    char* msgchar=msgbuf;
    KF_LOG_ERROR(logger, "msgbuf[8]:"<<msgbuf[8]);

    int leng=100;
    int keychar_len=40;
    unsigned char * masage = new unsigned char[leng];
    sha224digest(msgchar,keychar_len,masage,leng);
    std::string msgdigest=b2a_hex((char*)masage,28);
    KF_LOG_ERROR(logger, "msgdigest:"<<msgdigest);
 
    return masage;
 }

unsigned char * TDEngineCoinflex::key_sign()
 {
    //std::string hex_passphrase="wMH%7y$@r";
    int iuserid = stoi(userid);
    char * passphrase1=(char*)passphrase.data();
    char pass[9];
    memcpy(pass, passphrase1, sizeof(pass));
    uint64_t user_id=iuserid;
    if(is_big_endian()==false){
        user_id=swap_uint64(user_id);
    }
    KF_LOG_INFO(logger,"user_id"<<user_id);
    char buf[8];
    memset(buf,0,sizeof(buf));
    memcpy(buf,&user_id,sizeof(user_id));

    char keybuf[17];
    for(int i=0;i<8;i++){
        keybuf[i]=buf[i];
    }
    for(int i=8;i<17;i++){
        keybuf[i]=pass[i-8];
    }
    char* keychar=keybuf;
    KF_LOG_ERROR(logger, "keybuf[8]:"<<keybuf[8]);

    int leng=100;
    int keychar_len=17;
    unsigned char * key = new unsigned char[leng];
    sha224digest(keychar,keychar_len,key,leng);
    std::string keydigest=b2a_hex((char*)key,28);
    KF_LOG_ERROR(logger, "keydigest:"<<keydigest);

    return key;
 }

uint64_t TDEngineCoinflex::swap_uint64( uint64_t val ) {
    val = ((val << 8) & 0xFF00FF00FF00FF00ULL ) | ((val >> 8) & 0x00FF00FF00FF00FFULL );
    val = ((val << 16) & 0xFFFF0000FFFF0000ULL ) | ((val >> 16) & 0x0000FFFF0000FFFFULL );
    return (val << 32) | (val >> 32);
}

bool TDEngineCoinflex::is_big_endian()
{
    unsigned short test = 0x1234;
    if(*( (unsigned char*) &test ) == 0x12)
        return true;
    else
        return false;
}

int TDEngineCoinflex::lws_write_msg(struct lws* conn)
{
    //KF_LOG_INFO(logger, "TDEngineCoinflex::lws_write_msg:" );
    
    int ret = 0;
    std::string strMsg = "";

    if (welcome&&!m_isSub)
    {
        strMsg = makeSubscribeChannelString(account_units[0]);
        m_isSub = true;
    }
    /*else if(m_isSubOK&&!istrade){
        strMsg = makeSubscribetradeString(account_units[0]);
        istrade = true;        
    }*/
    else if(m_isSubOK)
    {
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        if(m_vstMsg.size() == 0){
            KF_LOG_INFO(logger, "TDEngineCoinflex::m_vstMsg.size()=0 " );
            return 0;
        }
        else
        {
            KF_LOG_INFO(logger, "TDEngineCoinflex::m_vstMsg" );
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
    KF_LOG_INFO(logger, "TDEngineCoinflex::lws_write_msg: " << strMsg.c_str() << " ,len = " << length);
    strncpy((char *)msg+LWS_PRE, strMsg.c_str(), length);
    ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
    lws_callback_on_writable(conn);  
    return ret;
}

void TDEngineCoinflex::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "TDEngineCoinflex::on_lws_connection_error. login again.");
    is_connecting = false;
    //no use it
    long timeout_nsec = 0;

    login(timeout_nsec);

    string url ="https://webapi.coinflex.com/orders/";
    //string Message = "348/dKPxKc/5NEJ8BPwZzae+SoB0Lng=:wMH%7y$@r";
    string Message = userid+"/"+api_key+":"+passphrase;
    string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
    const auto response = Delete(Url{url}, 
                            Header{{"Authorization","Basic "+payload},
                                    {"Content-Type", "application/json"}},Timeout{10000}  );
    KF_LOG_INFO(logger, "[Delete] (url) " << url << " (response.status_code) " << response.status_code <<
                " (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str());
    Document json;
    json.Parse(response.text.c_str());
    if(json.IsArray()){
        int size = json.Size();
        for(int i=0;i<size;i++){
            int64_t id64 = json.GetArray()[i]["id"].GetInt64();
            std::string id = std::to_string(id64);
            KF_LOG_INFO(logger,"id="<<id);
            std::string toncestr;
            std::unique_lock<std::mutex> lck3(idtance_mutex);
            auto it = idtonce_map.find(id);
            if(it != idtonce_map.end()){
                toncestr = it->second;
                idtonce_map.erase(it);
            }
            lck3.unlock();
            std::unique_lock<std::mutex> lck1(order_mutex);
            auto it1 = m_mapOrder.find(toncestr);
            if(it1 != m_mapOrder.end())
            {
                KF_LOG_INFO(logger,"m_mapOrder");
                it1->second.OrderStatus = LF_CHAR_Canceled;
                //撤单回报延时返回
                //m_mapCanceledOrder.insert(std::make_pair(id,getMSTime()));
                on_rtn_order(&(it1->second));
                std::unique_lock<std::mutex> lck2(local_mutex);
                auto it_id = localOrderRefRemoteOrderId.find(it1->second.OrderRef);
                if(it_id != localOrderRefRemoteOrderId.end())
                {
                    localOrderRefRemoteOrderId.erase(it_id);
                }
                lck2.unlock();
                m_mapOrder.erase(it1);
            }
            lck1.unlock();          
        }
    }
    get_orders(3);    
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

void TDEngineCoinflex::genUniqueKey()
{
    struct tm cur_time = getCurLocalTime();
    //SSMMHHDDN
    char key[11]{0};
    snprintf((char*)key, 11, "%02d%02d%02d%02d%02d", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_CurrentTDIndex);
    m_uniqueKey = key;
}

//clientid =  m_uniqueKey+orderRef
std::string TDEngineCoinflex::genClinetid(const std::string &orderRef)
{
    static int nIndex = 0;
    KF_LOG_INFO(logger,"m_CurrentTDIndex="<<m_CurrentTDIndex);
    return m_uniqueKey + orderRef;// + std::to_string(nIndex++);
}

void TDEngineCoinflex::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}



int64_t TDEngineCoinflex::getMSTime()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  timestamp;
}




void TDEngineCoinflex::init()
{
    //genUniqueKey();
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineCoinflex::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineCoinflex::resize_accounts(int account_num)
{
    account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineCoinflex::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    api_key = j_config["APIKey"].get<string>();
    secret_key = j_config["SecretKey"].get<string>();
    passphrase = j_config["passphrase"].get<string>();
    userid = j_config["userid"].get<string>();
    string baseUrl = j_config["baseUrl"].get<string>();
    string wsUrl = j_config["wsUrl"].get<string>();
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
    genUniqueKey();

    AccountUnitCoinflex& unit = account_units[idx];
    unit.api_key = api_key;
    unit.secret_key = secret_key;
    unit.baseUrl = baseUrl;
    unit.wsUrl = wsUrl;
    KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl);


    unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    unit.coinPairWhiteList.Debug_print();

    unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
    unit.positionWhiteList.Debug_print();

    //display usage:
    if(unit.coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "TDEngineCoinflex::load_account: please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }

    //test
    //Document json;
    //get_account(unit, json);
    
    getPriceIncrement(unit);
    // set up
    TradeAccount account = {};
    //partly copy this fields
    strncpy(account.UserID, api_key.c_str(), 16);
    strncpy(account.Password, secret_key.c_str(), 21);
    return account;
}

void TDEngineCoinflex::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    for (size_t idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitCoinflex& unit = account_units[idx];
        unit.logged_in = true;
        KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
        login(timeout_nsec);
    }
    
    //cancel_all_orders();
}

   void TDEngineCoinflex::getPriceIncrement(AccountUnitCoinflex& unit)
   { 
        KF_LOG_INFO(logger, "[getPriceIncrement]");
        std::string requestPath = "webapi.coinflex.com/markets/";
        string url = "webapi.coinflex.com/markets/";
        std::string strTimestamp = getTimestampStr();

        std::string strSignatrue = "a";
        cpr::Header mapHeader = cpr::Header{{"COINFLEX-ACCESS-SIG",strSignatrue},
                                            {"COINFLEX-ACCESS-TIMESTAMP",strTimestamp},
                                            {"COINFLEX-ACCESS-KEY",unit.api_key}};
        KF_LOG_INFO(logger, "COINFLEX-ACCESS-SIG = " << strSignatrue 
                            << ", COINFLEX-ACCESS-TIMESTAMP = " << strTimestamp 
                            << ", COINFLEX-API-KEY = " << unit.api_key);


        std::unique_lock<std::mutex> lock(g_httpMutex);
        const auto response = cpr::Get(Url{url},Timeout{10000} );
        lock.unlock();
        KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
                                                " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
        Document json;
        json.Parse(response.text.c_str());

        if(!json.HasParseError())
        {
            size_t len = json.Size();
            KF_LOG_INFO(logger, "[getPriceIncrement] (accounts.length)" << len);
            for(size_t i = 0; i < len; i++)
            {
                int basenum = json.GetArray()[i]["base"].GetInt();
                int counternum = json.GetArray()[i]["counter"].GetInt();
                std::string base=std::to_string(basenum);
                std::string counter=std::to_string(counternum);
                std::string symbol=base+"_"+counter;
                std::string ticker = unit.coinPairWhiteList.GetKeyByValue(symbol);
                KF_LOG_INFO(logger, "[getPriceIncrement] (symbol) " << symbol << " (ticker) " << ticker);
                if(ticker.length() > 0) {
                    int tick= json.GetArray()[i]["tick"].GetInt();
                    std::string size = std::to_string(tick);
                    PriceIncrement increment;
                    increment.nPriceIncrement = tick;
                    unit.mapPriceIncrement.insert(std::make_pair(ticker,increment));           
                    KF_LOG_INFO(logger, "[getPriceIncrement] (symbol) " << symbol << " (position) " << increment.nPriceIncrement);
                }
            }
        }
        
   }

void TDEngineCoinflex::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "TDEngineCoinflex::login:");
    context = nullptr;

    global_md = this;

    welcome = false;
    m_isSub = false;
    m_isSubOK = false;
    //istrade = false;
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
    KF_LOG_INFO(logger, "TDEngineCoinflex::login: context created.");


    if (context == NULL) {
        KF_LOG_ERROR(logger, "TDEngineCoinflex::login: context is NULL. return");
        return;
    }

    // Set up the client creation info
    std::string strAddress = "api.coinflex.com";
    clientConnectInfo.address = strAddress.c_str();
    clientConnectInfo.path = "/v1"; // Set the info's path to the fixed up url path
    clientConnectInfo.context = context;
    clientConnectInfo.port = 443;
    clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    clientConnectInfo.host =strAddress.c_str();
    clientConnectInfo.origin = strAddress.c_str();
    clientConnectInfo.ietf_version_or_minus_one = -1;
    clientConnectInfo.protocol = protocols[PROTOCOL_TEST].name;
    clientConnectInfo.pwsi = &wsi;

    KF_LOG_INFO(logger, "TDEngineCoinflex::login: address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);

    wsi = lws_client_connect_via_info(&clientConnectInfo);
    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "TDEngineCoinflex::login: wsi create error.");
        sleep(10);
        login(0);
        //return;
    }
    KF_LOG_INFO(logger, "TDEngineCoinflex::login: wsi create success.");
    is_connecting = true;
    if(wsi!=NULL){
        m_conn = wsi;
    }
    //connect(timeout_nsec);
    cancel_all_orders();
}

void TDEngineCoinflex::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineCoinflex::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineCoinflex::is_logged_in() const
{
    KF_LOG_INFO(logger, "[is_logged_in]");
    for (auto& unit: account_units)
    {
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineCoinflex::is_connected() const
{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}


std::string TDEngineCoinflex::GetSide(const LfDirectionType& input) {
    if (LF_CHAR_Buy == input) {
        return "buy";
    } else if (LF_CHAR_Sell == input) {
        return "sell";
    } else {
        return "";
    }
}

LfDirectionType TDEngineCoinflex::GetDirection(std::string input) {
    if ("buy" == input) {
        return LF_CHAR_Buy;
    } else if ("sell" == input) {
        return LF_CHAR_Sell;
    } else {
        return LF_CHAR_Buy;
    }
}

std::string TDEngineCoinflex::GetType(const LfOrderPriceTypeType& input) {
    if (LF_CHAR_LimitPrice == input) {
        return "limit";
    } else if (LF_CHAR_AnyPrice == input) {
        return "market";
    } else {
        return "";
    }
}

LfOrderPriceTypeType TDEngineCoinflex::GetPriceType(std::string input) 
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
void TDEngineCoinflex::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitCoinflex& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key << " (InstrumentID) " << data->InstrumentID);

    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_COINFLEX, 1, requestId);
    int errorId = 0;
    std::string errorMsg = "";
    get_account_();

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

    bool findSymbolInResult = false;
    //send the filtered position
    int position_count = positionHolder.size();
    for (int i = 0; i < position_count; i++)
    {
        pos.PosiDirection = LF_CHAR_Long;
        strncpy(pos.InstrumentID, positionHolder[i].ticker.c_str(), 31);
        if (positionHolder[i].isLong) {
            pos.PosiDirection = LF_CHAR_Long;
        }
        else {
            pos.PosiDirection = LF_CHAR_Short;
        }
        pos.Position = positionHolder[i].amount;
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
        raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_COINFLEX, 1, requestId, errorId, errorMsg.c_str());
    }
}

void TDEngineCoinflex::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineCoinflex::dealPriceVolume(AccountUnitCoinflex& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,double& dDealPrice,double& dDealVolume,bool Isbuy)
{
        KF_LOG_DEBUG(logger, "[dealPriceVolume] (symbol)" << symbol);
        auto it = unit.mapPriceIncrement.find(symbol);
        if(it == unit.mapPriceIncrement.end())
        {
            KF_LOG_INFO(logger, "[dealPriceVolume] symbol not find :" << symbol);
            int64_t intvolume=nVolume/1e4;
            dDealVolume = intvolume;
            int64_t intprice=nPrice/1e4;
            dDealPrice = intprice; 
            if(Isbuy==true){ 
                dDealPrice=(floor(dDealPrice/it->second.nPriceIncrement))*it->second.nPriceIncrement;
            }
            else{
                dDealPrice=(ceil(dDealPrice/it->second.nPriceIncrement))*it->second.nPriceIncrement;
            }
        }
        else
        {
            KF_LOG_INFO(logger, "[dealPriceVolume]");
            /*int64_t nDealVolume =  it->second.nQuoteIncrement  > 0 ? nVolume / it->second.nQuoteIncrement * it->second.nQuoteIncrement : nVolume;
            int64_t nDealPrice = it->second.nPriceIncrement > 0 ? nPrice / it->second.nPriceIncrement * it->second.nPriceIncrement : nPrice;*/
            int64_t intvolume=nVolume/1e4;
            dDealVolume = intvolume;
            int64_t intprice=nPrice/1e4;
            dDealPrice = intprice;
            if(Isbuy==true){ 
                dDealPrice=(floor(dDealPrice/it->second.nPriceIncrement))*it->second.nPriceIncrement;
            }
            else{
                dDealPrice=(ceil(dDealPrice/it->second.nPriceIncrement))*it->second.nPriceIncrement;
            }
        }

        KF_LOG_INFO(logger, "[dealPriceVolume]  (symbol)" << symbol << " (Volume)" << nVolume << " (Price)" << nPrice << " (FixedVolume)" << dDealVolume << " (FixedPrice)" << dDealPrice);
}

void TDEngineCoinflex::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitCoinflex& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_COINFLEX, 1/*ISLAST*/, requestId);
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
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_COINFLEX, 1, requestId, errorId, errorMsg.c_str());
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
    dealPriceVolume(unit,data->InstrumentID,data->LimitPrice,data->Volume,fixedPrice,fixedVolume,isbuy);
    
    if(fixedVolume == 0)
    {
        KF_LOG_DEBUG(logger, "[req_order_insert] fixed Volume error" << ticker);
        errorId = 200;
        errorMsg = data->InstrumentID;
        errorMsg += " : quote less than baseMinSize";
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_COINFLEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    std::string tonce = genClinetid(data->OrderRef);
    for(int i=0;i<tonce.length();i++){
        if(tonce.find("0")==0){
            tonce=tonce.erase(0,1);
        }
    }
    KF_LOG_INFO(logger,"tonce:"<<tonce);
    /*std::stringstream ss1;
    ss1<<tonce;
    int64_t itonce;
    ss1>>itonce;*/
    int64_t itonce = stoll(tonce);
    KF_LOG_INFO(logger,"itonce:"<<itonce);
    std::string strClientId = std::string(data->OrderRef);
    KF_LOG_INFO(logger,"strClientId"<<strClientId);
    std::stringstream ss;
    ss<<strClientId;
    int64_t cid;
    ss>>cid;
    KF_LOG_INFO(logger,"(cid)"<<cid);

    if(!is_connecting){
        errorId = 203;
        errorMsg = "websocket is not connecting,please try again later";
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        return;
    }

    {
        //std::lock_guard<std::mutex> lck(*m_mutexOrder);
        //errormap.insert(std::make_pair(4,*data));
        errormap[cid]=*data;
        //m_mapInputOrder.insert(std::make_pair(strClientId,*data));
        LFRtnOrderField order;
        memset(&order, 0, sizeof(LFRtnOrderField));
        order.OrderStatus = LF_CHAR_Unknown;
        order.VolumeTotalOriginal = std::round(fixedVolume*1e4);
        order.VolumeTotal = order.VolumeTotalOriginal;
        strncpy(order.OrderRef, data->OrderRef, 21);
        strncpy(order.InstrumentID, data->InstrumentID, 31);
        order.RequestID = requestId;
        strcpy(order.ExchangeID, "Coinflex");
        strncpy(order.UserID, unit.api_key.c_str(), 16);
        order.LimitPrice = std::round(fixedPrice*1e4);
        order.TimeCondition = data->TimeCondition;
        order.Direction = data->Direction;
        order.OrderPriceType = data->OrderPriceType;
        std::unique_lock<std::mutex> lck1(order_mutex);
        m_mapOrder.insert(std::make_pair(tonce,order));
        lck1.unlock();
       // error_requestid_map.insert(std::make_pair(4,order));
        error_requestid_map[cid]=order;
    }
    addRemoteOrderIdOrderInsertSentTime(data, requestId, tonce);
    send_order(ticker.c_str(),strClientId.c_str(), GetSide(data->Direction).c_str(),GetType(data->OrderPriceType).c_str(), fixedVolume, fixedPrice,cid,itonce);

   
}

void TDEngineCoinflex::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitCoinflex& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (APIKey)" << unit.api_key
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_COINFLEX, 1, requestId);

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
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_COINFLEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);
    //std::lock_guard<std::mutex> lck(*m_mutexOrder);
    std::unique_lock<std::mutex> lck2(local_mutex);    
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
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_COINFLEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    else if(itr != localOrderRefRemoteOrderId.end()){
        remoteOrderId = itr->second;
        KF_LOG_DEBUG(logger, "[req_order_action] found in localOrderRefRemoteOrderId map (orderRef) "
                << data->OrderRef << " (remoteOrderId) " << remoteOrderId);
        /*{
            //std::lock_guard<std::mutex> lck(*m_mutexOrder);
            m_mapOrderAction.insert(std::make_pair(remoteOrderId,*data));
        }*/
        if(!is_connecting){
            errorId = 203;
            errorMsg = "websocket is not connecting,please try again later";
            on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        }else{
            addRemoteOrderIdOrderActionSentTime(data, requestId, remoteOrderId);
            cancel_order(itr->first,remoteOrderId);
        }
    }
    lck2.unlock();
    
}

//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
void TDEngineCoinflex::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId)
{
    //std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;
    newOrderActionSent.sentNameTime = getTimestamp();
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    std::unique_lock<std::mutex> lck6(action_time_mutex);
    remoteOrderIdOrderActionSentTime[remoteOrderId] = newOrderActionSent;
    lck6.unlock();
}

void TDEngineCoinflex::addRemoteOrderIdOrderInsertSentTime(const LFInputOrderField* data, int requestId, const std::string& remoteOrderId){
    //std::lock_guard<std::mutex> guard_mutex_order_insert(*mutex_orderinsert_waiting_response);

    OrderInsertSentTime newOrderInsertSent;
    newOrderInsertSent.requestId = requestId;
    newOrderInsertSent.sentNameTime = getTimestamp();
    memcpy(&newOrderInsertSent.data, data, sizeof(LFInputOrderField));
    std::unique_lock<std::mutex> lck4(insert_time_mutex);
    remoteOrderIdOrderInsertSentTime[remoteOrderId] = newOrderInsertSent;
    lck4.unlock();
}

void TDEngineCoinflex::set_reader_thread()
{
    ITDEngine::set_reader_thread();

    KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineCoinflex::loop");
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineCoinflex::loopwebsocket, this)));

    KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineCoinflex::loopOrderActionNoResponseTimeOut");
    orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineCoinflex::loopOrderActionNoResponseTimeOut, this)));

    KF_LOG_INFO(logger, "[set_reader_thread] orderinsert_timeout_thread start on TDEngineCoinflex::loopOrderInsertNoResponseTimeOut");
    orderinsert_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineCoinflex::loopOrderInsertNoResponseTimeOut, this))); 
}

void TDEngineCoinflex::loopwebsocket()
{
        while(isRunning)
        {
            //KF_LOG_INFO(logger, "TDEngineCoinflex::loop:lws_service");
            lws_service( context, rest_get_interval_ms );
            //延时返回撤单回报
            //std::lock_guard<std::mutex> lck(*m_mutexOrder); 
            for(auto canceled_order = m_mapCanceledOrder.begin();canceled_order != m_mapCanceledOrder.end();++canceled_order)
            {
                if(getMSTime() - canceled_order->second >= 1000)
                {// 撤单成功超过1秒时，回报205
                    std::unique_lock<std::mutex> lck1(order_mutex);
                    auto it = m_mapOrder.find(canceled_order->first);
                    if(it != m_mapOrder.end())
                    {
                        on_rtn_order(&(it->second));
                        m_mapOrder.erase(it);
                    }
                    lck1.unlock();
                    canceled_order = m_mapCanceledOrder.erase(canceled_order);
                    if(canceled_order == m_mapCanceledOrder.end())
                    {
                        break;
                    }
                }
            }
        }
}



void TDEngineCoinflex::loopOrderActionNoResponseTimeOut()
{
    KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        orderActionNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
}

void TDEngineCoinflex::loopOrderInsertNoResponseTimeOut()
{
    KF_LOG_INFO(logger, "[loopOrderInsertNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        //KF_LOG_INFO(logger,"insert in");
        orderInsertNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
}

void TDEngineCoinflex::orderActionNoResponseTimeOut()
{
//    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut]");
    int errorId = 100;
    std::string errorMsg = "OrderAction has none response for a long time, please send OrderAction again";

    //std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    int64_t currentNano = getTimestamp();
    int64_t timeBeforeNano = currentNano - orderaction_max_waiting_seconds * 1000;
//    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (currentNano)" << currentNano << " (timeBeforeNano)" << timeBeforeNano);
    std::map<std::string, OrderActionSentTime>::iterator itr;
    std::unique_lock<std::mutex> lck6(action_time_mutex);
    for(itr = remoteOrderIdOrderActionSentTime.begin(); itr != remoteOrderIdOrderActionSentTime.end();)
    {
        if(itr->second.sentNameTime < timeBeforeNano)
        {
            KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.erase remoteOrderId)" << itr->first );
            get_orders(1);
            std::unique_lock<std::mutex> lck7(action_time1_mutex);
            remoteOrderIdOrderActionSentTime1.insert(std::make_pair(itr->first,itr->second));
            lck7.unlock();
            itr = remoteOrderIdOrderActionSentTime.erase(itr);
            KF_LOG_INFO(logger,"action-out");
            /*std::string url = "https://webapi.coinflex.com/orders/"+itr->first;
            string apikey = "dKPxKc/5NEJ8BPwZzae+SoB0Lng=";
            string apikey64 = base64_encode((const unsigned char*)apikey.c_str(), apikey.length());
            //string Message = "348/"+apikey64+":JdG2+b8QNAI5os41akoI/NkMqC/nYwQxyFJWCw==";
            string Message = "348/"+apikey+":wMH%7y$@r";
            KF_LOG_INFO(logger,"Message:"<<Message);
            string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
            const auto response = Get(Url{url}, 
                                    Header{{"Authorization","Basic "+payload},
                                           {"Content-Type", "application/json"}},Timeout{10000}  );
            KF_LOG_INFO(logger, "[Get] (url) " << url << " (response.status_code) " << response.status_code <<
                " (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str());
            Document json;
            json.Parse(response.text.c_str());
            //int size = json.Size();
            if(response.status_code==200){
                //int64_t id64 = json.GetArray[0]["id"].GetInt64();
                //std::string id = std::to_string(id64);
                on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
                itr = remoteOrderIdOrderActionSentTime.erase(itr);
            }
            else if(response.status_code==404){
                errorMsg = "OrderAction has none response for a long time and can't find by rest";
                on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
                itr = remoteOrderIdOrderActionSentTime.erase(itr);                
            }*/
            /*else if(response.status_code==404){
                        std::string id = itr->first;
                        auto it = m_mapOrder.find(id);
                        if(it != m_mapOrder.end())
                        {
                            it->second.OrderStatus = LF_CHAR_Canceled;
                            //撤单回报延时返回
                            m_mapCanceledOrder.insert(std::make_pair(id,getMSTime()));
                            //on_rtn_order(&(it->second));
                            auto it_id = localOrderRefRemoteOrderId.find(it->second.OrderRef);
                            if(it_id != localOrderRefRemoteOrderId.end())
                            {
                                localOrderRefRemoteOrderId.erase(it_id);
                            }
                            //m_mapOrder.erase(it);
                        }
                        //
                        auto it2 = m_mapInputOrder.find(id);
                        if(it2 != m_mapInputOrder.end())
                        {
                            m_mapInputOrder.erase(it2);
                        }
                        auto it3 = m_mapOrderAction.find(id);
                        if(it3 != m_mapOrderAction.end())
                        {    
                            m_mapOrderAction.erase(it3);
                        }
                        itr = remoteOrderIdOrderActionSentTime.erase(itr);
            }*/
        } else {
            ++itr;
        }
    }
    lck6.unlock();
//    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}

void TDEngineCoinflex::orderInsertNoResponseTimeOut(){
    //KF_LOG_DEBUG(logger, "[orderInsertNoResponseTimeOut]");
    int errorId = 100;
    std::string errorMsg = "OrderInsert has none response for a long time, please send OrderInsert again";

    //std::lock_guard<std::mutex> guard_mutex_order_insert(*mutex_orderinsert_waiting_response);

    int64_t currentNano = getTimestamp();
    int64_t timeBeforeNano = currentNano - orderinsert_max_waiting_seconds * 1000;
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (currentNano)" << currentNano << " (timeBeforeNano)" << timeBeforeNano);
    std::map<std::string, OrderInsertSentTime>::iterator itr;
    std::unique_lock<std::mutex> lck4(insert_time_mutex);
    int size = remoteOrderIdOrderInsertSentTime.size();
    if(size > 0){
        for(itr = remoteOrderIdOrderInsertSentTime.begin(); itr != remoteOrderIdOrderInsertSentTime.end();)
        {
            if(itr->second.sentNameTime < timeBeforeNano)
            {
                KF_LOG_DEBUG(logger, "[orderInsertNoResponseTimeOut] (remoteOrderIdOrderInsertSentTime.erase remoteOrderId)" << itr->first );
                std::unique_lock<std::mutex> lck5(insert_time1_mutex);
                remoteOrderIdOrderInsertSentTime1.insert(std::make_pair(itr->first,itr->second));
                auto it = remoteOrderIdOrderInsertSentTime1.find(itr->first);
                if(it != remoteOrderIdOrderInsertSentTime1.end()){
                    it->second.sentNameTime = getTimestamp();
                }
                KF_LOG_INFO(logger,"sent1="<<remoteOrderIdOrderInsertSentTime1.size());
                lck5.unlock();
                //get_orders(2);
                //remoteOrderIdOrderInsertSentTime1.insert(std::make_pair(itr->first,itr->second));
                //on_rsp_order_insert(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
                itr = remoteOrderIdOrderInsertSentTime.erase(itr);

            } else {
                ++itr;
            }
        }
        get_orders(2);
    }
    
    lck4.unlock();
    std::map<std::string, OrderInsertSentTime>::iterator it1;
    for(it1 = remoteOrderIdOrderInsertSentTime1.begin(); it1 != remoteOrderIdOrderInsertSentTime1.end();){
        if(currentNano - it1->second.sentNameTime > orderinsert_max_waiting_seconds * 2000){
            errorMsg = "get_orders time out";
            on_rsp_order_insert(&it1->second.data, it1->second.requestId, 201, errorMsg.c_str());
            it1 = remoteOrderIdOrderInsertSentTime1.erase(it1);
        }else{
            it1++;
        }
    }
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}

void TDEngineCoinflex::printResponse(const Document& d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
}

/*
void TDEngineCoinflex::get_account(AccountUnitCoinflex& unit, Document& json)
{
    KF_LOG_INFO(logger, "[get_account]");
    std::string requestPath = "/v1/positions";
    string url = unit.baseUrl + requestPath ;
    std::string strTimestamp = getTimestampStr();
    std::string strSignatrue = sign(unit,"GET",strTimestamp,requestPath);
    cpr::Header mapHeader = cpr::Header{{"COINFLEX-ACCESS-SIG",strSignatrue},
                                        {"COINFLEX-ACCESS-TIMESTAMP",strTimestamp},
                                        {"COINFLEX-ACCESS-KEY",unit.api_key}};
     KF_LOG_INFO(logger, "COINFLEX-ACCESS-SIG = " << strSignatrue 
                        << ", COINFLEX-ACCESS-TIMESTAMP = " << strTimestamp 
                        << ", COINFLEX-API-KEY = " << unit.api_key);
    std::unique_lock<std::mutex> lock(g_httpMutex);
    const auto response = cpr::Get(Url{url}, 
                             Header{mapHeader}, Timeout{10000} );
    lock.unlock();
    KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
                                               " (response.error.message) " << response.error.message <<
                                               " (response.text) " << response.text.c_str());
    
    json.Parse(response.text.c_str());
    return ;
}*/

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
std::string TDEngineCoinflex::createInsertOrderString(const char *code,const char* strClientId,const char *side, const char *type, double& size, double& price,int64_t Cid,int64_t tonce)
{
    KF_LOG_INFO(logger, "[TDEngineCoinflex::createInsertOrdertring]:(price)"<<price << "(volume)" << size<<"(type)"<<type);
    int volume=size;
    string limitstr="limit";
    string buystr="buy";
    string codestr=code;
    string basestr=codestr.substr(0,5);
    string counterstr=codestr.substr(6,11);
    int base=atoi(basestr.c_str());
    int counter=atoi(counterstr.c_str());
    int intprice=price;
    if(side==buystr){
        volume=size;
    }
    else{
        volume=-size;
    }
    //string strvolume=std::to_string(volume);
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("tag");
    writer.Int64(Cid);
    writer.Key("tonce");
    writer.Int64(tonce);    
    writer.Key("method");
    writer.String("PlaceOrder");
    writer.Key("base");
    writer.Int(base);    
    writer.Key("counter");
    writer.Int(counter);   
    writer.Key("quantity");
    writer.Int(volume);
    if(type==limitstr){
        writer.Key("price");
        writer.Int(intprice);
    }
    
    writer.EndObject();
    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineCoinflex::createInsertOrdertring]:" << strOrder);
    return strOrder;
}

void TDEngineCoinflex::send_order(const char *code,const char* strClientId,const char *side, const char *type, double& size, double& price,int64_t Cid,int64_t tonce)
{
    KF_LOG_INFO(logger, "[send_order]");
    {
        std::string new_order = createInsertOrderString(code, strClientId,side, type, size, price, Cid, tonce);
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        m_vstMsg.push(new_order);
        //lws_callback_on_writable(m_conn);
    }
    lws_callback_on_writable(m_conn);
}
    
void TDEngineCoinflex::get_orders(int type)
{
    KF_LOG_INFO(logger, "[get_orders]");
    {
        std::string cancel_allorder = creategetOrderString(type);
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        m_vstMsg.push(cancel_allorder);
    }
    lws_callback_on_writable(m_conn);
}

void TDEngineCoinflex::cancel_all_orders()
{
    KF_LOG_INFO(logger, "[cancel_all_orders]");
    {
        std::string cancel_allorder = createCancelallOrderString();
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        m_vstMsg.push(cancel_allorder);
    }
    lws_callback_on_writable(m_conn);
}

void TDEngineCoinflex::get_account_()
{
    KF_LOG_INFO(logger, "[get_account_]");
    {
        std::string get_account_ = createaccountString();
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        m_vstMsg.push(get_account_);
    }
    lws_callback_on_writable(m_conn);
}

std::string TDEngineCoinflex::creategetOrderString(int type)
{
    KF_LOG_INFO(logger, "[TDEngineCoinflex::creategetOrderString]");
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("tag");
    if(type==1){
        writer.Int64(300000001);
    }
    else if(type == 2){
        writer.Int64(300000002);
    }
    else{
        writer.Int64(300000003);
    }    
    writer.Key("method");
    writer.String("GetOrders");
    
    writer.EndObject();
    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineCoinflex::creategetOrderString]:" << strOrder);
    return strOrder;
}

std::string TDEngineCoinflex::createCancelOrderString(std::string ref,const char* strOrderId_)
{
    KF_LOG_INFO(logger, "[TDEngineCoinflex::createCancelOrderString]");
    string Id = strOrderId_;
    int64_t id = atoi(Id.c_str());
    int64_t ref64 = atoi(ref.c_str());
    ref64 = -ref64;
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("tag");
    writer.Int64(200000000);
    writer.Key("method");
    writer.String("CancelOrder");
    writer.Key("id");
    writer.Int64(id);
    
    writer.EndObject();
    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineCoinflex::createCancelOrderString]:" << strOrder);
    return strOrder;
}

std::string TDEngineCoinflex::createCancelallOrderString()
{
    KF_LOG_INFO(logger, "[TDEngineCoinflex::createCancelallOrderString]");
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("tag");
    writer.Int64(300000000);
    writer.Key("method");
    writer.String("CancelAllOrders");
    
    writer.EndObject();
    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineCoinflex::createCancelallOrderString]:" << strOrder);
    return strOrder;
}

std::string TDEngineCoinflex::createaccountString()
{
    KF_LOG_INFO(logger, "[TDEngineCoinflex::createaccountString]");
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("tag");
    writer.Int64(500000000);
    writer.Key("method");
    writer.String("GetBalances");
    
    writer.EndObject();
    std::string strOrder = s.GetString();
    KF_LOG_INFO(logger, "[TDEngineCoinflex::createaccountString]:" << strOrder);
    return strOrder;
}

void TDEngineCoinflex::cancel_order(std::string ref,std::string orderId)
{
    KF_LOG_INFO(logger, "[cancel_order]");
    {
        std::string cancel_order = createCancelOrderString(ref,orderId.c_str());
        std::lock_guard<std::mutex> lck(mutex_msg_queue);
        m_vstMsg.push(cancel_order);
    }
    lws_callback_on_writable(m_conn);
}

std::string TDEngineCoinflex::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


inline int64_t TDEngineCoinflex::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}



#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

BOOST_PYTHON_MODULE(libcoinflextd)
{
    using namespace boost::python;
    class_<TDEngineCoinflex, boost::shared_ptr<TDEngineCoinflex> >("Engine")
            .def(init<>())
            .def("init", &TDEngineCoinflex::initialize)
            .def("start", &TDEngineCoinflex::start)
            .def("stop", &TDEngineCoinflex::stop)
            .def("logout", &TDEngineCoinflex::logout)
            .def("wait_for_stop", &TDEngineCoinflex::wait_for_stop);
}



