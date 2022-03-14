//
// Created by wang on 10/20/18.
//
#include "MDEngineHbdm.h"
#include "../../utils/common/ld_utils.h"
#include <stringbuffer.h>
#include <writer.h>
#include <document.h>
#include <libwebsockets.h>
#include <algorithm>
#include <stdio.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <string>
#include <cctype>
#include <cpr/cpr.h>
using cpr::Get;
using cpr::Url;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;
using rapidjson::Document;
using namespace rapidjson;
using namespace kungfu;
using namespace std;
#define  SCALE_OFFSET 1e8
WC_NAMESPACE_START

std::mutex trade_mutex;
std::mutex book_mutex;
std::mutex kline_mutex;

MDEngineHbdm* MDEngineHbdm::m_instance = nullptr;
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;
std::mutex websocket_vec_mutex;

MDEngineHbdm::MDEngineHbdm(): IMDEngine(SOURCE_HBDM)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Hbdm");
    KF_LOG_DEBUG(logger, "MDEngineHbdm construct");
    timer = getTimestamp();/*edited by zyy*/
    m_ThreadPoolPtr = nullptr;
}

MDEngineHbdm::~MDEngineHbdm()
{
    if (m_thread)
    {
        if(m_thread->joinable())
        {
            m_thread->join();
        }
    }
    if (rest_thread)
    {
        if(rest_thread->joinable())
        {
            rest_thread->join();
        }
    }
    if(m_ThreadPoolPtr != nullptr) delete m_ThreadPoolPtr;
    KF_LOG_DEBUG(logger, "MDEngineHbdm deconstruct");
}

void MDEngineHbdm::set_reader_thread()
{
    IMDEngine::set_reader_thread();
    m_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineHbdm::lwsEventLoop, this)));
    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineHbdm::rest_loop, this)));
    check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineHbdm::check_loop, this)));
    KF_LOG_INFO(logger,"set_reader_thread end");
}

void MDEngineHbdm::load(const json& config)
{
    KF_LOG_INFO(logger, "load config start");
    try
    {
        /*edited by zyy,starts here*/
        if(config.find("level_threshold") != config.end()) 
        {
            level_threshold = config["level_threshold"].get<int>();
        }
        if(config.find("refresh_normal_check_book_s") != config.end()) 
        {
            refresh_normal_check_book_s = config["refresh_normal_check_book_s"].get<int>();
        }
        if(config.find("rest_get_interval_ms") != config.end()) 
        {
            rest_get_interval_ms = config["rest_get_interval_ms"].get<int>();
        }
        if(config.find("refresh_normal_check_trade_s") != config.end()) 
        {
            refresh_normal_check_trade_s = config["refresh_normal_check_trade_s"].get<int>();
        }
        if(config.find("refresh_normal_check_kline_s") != config.end()) 
        {
            refresh_normal_check_kline_s = config["refresh_normal_check_kline_s"].get<int>();
        }
        /*edited by zyy ends here*/
        m_priceBookNum = config["book_depth_count"].get<int>();
        if(config.find("snapshot_check_s") != config.end()) {
            snapshot_check_s = config["snapshot_check_s"].get<int>();
        }
        if(config.find("exchange_url") != config.end()) 
        {
            std::string ws_url = config["exchange_url"].get<string>();
            KF_LOG_INFO(logger,"ws_url1="<<ws_url);
            WsMsg wsmsg;
            wsmsg.ws_url = ws_url;
            wsmsg.is_swap = false;
            std::unique_lock<std::mutex> lck(websocket_vec_mutex);
            ws_msg_vec.push_back(wsmsg);
            lck.unlock();
            /*if (!parseAddress(config["exchange_url"].get<std::string>(), false))
            {
                return;
            }*/
        }
        if(config.find("swap_url") != config.end()) 
        {
            std::string ws_url = config["swap_url"].get<string>();
            KF_LOG_INFO(logger,"ws_url2="<<ws_url);
            WsMsg wsmsg;
            wsmsg.ws_url = ws_url;
            wsmsg.is_swap = true;
            std::unique_lock<std::mutex> lck(websocket_vec_mutex);
            ws_msg_vec.push_back(wsmsg);
            lck.unlock();
            /*if (!parseAddress(config["swap_url"].get<std::string>(), true))
            {
                return;
            }*/
        }
        int thread_pool_size = 0;
        if(config.find("thread_pool_size") != config.end()) {
            thread_pool_size = config["thread_pool_size"].get<int>();
        }
        KF_LOG_INFO(logger, "thread_pool_size:" << thread_pool_size);
        if(thread_pool_size > 0)
        {
            //m_ThreadPoolPtr = new ThreadPool(thread_pool_size);
        }
        m_whiteList.ReadWhiteLists(config, "whiteLists");
        m_whiteList.Debug_print();
        genSubscribeString();

        int64_t nowTime = getTimestamp();
	    std::unordered_map<std::string, std::string>::iterator it;
	    for(it = m_whiteList.GetKeyIsStrategyCoinpairWhiteList().begin();it != m_whiteList.GetKeyIsStrategyCoinpairWhiteList().end();it++)
	    {
		    std::unique_lock<std::mutex> lck(trade_mutex);
		    control_trade_map.insert(make_pair(it->first, nowTime));
		    lck.unlock();

		    std::unique_lock<std::mutex> lck1(book_mutex);
		    control_book_map.insert(make_pair(it->first, nowTime));
		    lck1.unlock();

            std::unique_lock<std::mutex> lck2(kline_mutex);
		    control_kline_map.insert(make_pair(it->first, nowTime));
		    lck2.unlock();
	    }
    }
    catch (const std::exception& e)
    {
        KF_LOG_INFO(logger, "load config exception,"<<e.what());
    }
    KF_LOG_INFO(logger, "load config end");
}
void MDEngineHbdm::makeWebsocketSubscribeJsonString(const common::CWebsocket* instance)
{
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = m_whiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    while (map_itr != m_whiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);
        std::vector<WsMsg>::iterator it;
        std::unique_lock<std::mutex> lck(websocket_vec_mutex);
        for(it = ws_msg_vec.begin(); it != ws_msg_vec.end(); it++){
            int swap_symbol = map_itr->second.find("-");
            if(it->websocket.m_connection == instance->m_connection){
                if(it->is_swap && swap_symbol != -1){
                    std::string jsonBookString = genDepthString(map_itr->second);
                    it->websocket.SendMessage(jsonBookString);

                    std::string jsonTradeString = genTradeString(map_itr->second);
                    it->websocket.SendMessage(jsonTradeString);

                    std::string jsonKlineString = genKlineString(map_itr->second);
                    it->websocket.SendMessage(jsonKlineString);           
                    KF_LOG_INFO(logger, "swap_sendmessage "<< jsonBookString <<" "<<jsonTradeString<<" "<<jsonKlineString);
                }else if(!it->is_swap && swap_symbol == -1){
                    std::string jsonBookString = genDepthString(map_itr->second);
                    it->websocket.SendMessage(jsonBookString);

                    std::string jsonTradeString = genTradeString(map_itr->second);
                    it->websocket.SendMessage(jsonTradeString);

                    std::string jsonKlineString = genKlineString(map_itr->second);
                    it->websocket.SendMessage(jsonKlineString);           
                    KF_LOG_INFO(logger, "!swap_sendmessage "<< jsonBookString <<" "<<jsonTradeString<<" "<<jsonKlineString);                
                }
            }
        }
        lck.unlock();
        map_itr++;
    }
}
void MDEngineHbdm::genSubscribeString()
{
    auto& symbol_map = m_whiteList.GetKeyIsStrategyCoinpairWhiteList();
    for(const auto& var : symbol_map)
    {
        /*
        auto var_new = ldutils::split(var.second, "_");
        if(var_new[1] == "FUTURE")
        {
            std::string var_cw = var_new[0]+"_CW";
            std::string var_nw = var_new[0]+"_NW";
            std::string var_cq = var_new[0]+"_CQ"; 
            m_subcribeJsons.push_back(genDepthString(var_cw));
            m_subcribeJsons.push_back(genTradeString(var_cw));
            m_subcribeJsons.push_back(genKlineString(var_cw));

            m_subcribeJsons.push_back(genDepthString(var_nw));
            m_subcribeJsons.push_back(genTradeString(var_nw));
            m_subcribeJsons.push_back(genKlineString(var_nw));
            
            m_subcribeJsons.push_back(genDepthString(var_cq));
            m_subcribeJsons.push_back(genTradeString(var_cq));
            m_subcribeJsons.push_back(genKlineString(var_cq));

        }
        else
        */
        
        {
            m_subcribeJsons.push_back(genDepthString(var.second));
            m_subcribeJsons.push_back(genTradeString(var.second));
            m_subcribeJsons.push_back(genKlineString(var.second));
        }
        
    }
    
    if(m_subcribeJsons.empty())
    {
        KF_LOG_INFO(logger, "genSubscribeString failed, {error:has no white list}");
        exit(0);
    }
}

std::string MDEngineHbdm::genKlineString(const std::string& symbol)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("sub");
    std::string sub_value ("market.");
    sub_value += symbol+".kline.1min";
    writer.String(sub_value.c_str());
    writer.Key("id");
    writer.String(std::to_string(m_id++).c_str());
    writer.EndObject();
    return buffer.GetString();
}

std::string MDEngineHbdm::genDepthString(const std::string& symbol)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("sub");
    std::string sub_value ("market.");
    sub_value += symbol+".depth.step0";
    writer.String(sub_value.c_str());
    writer.Key("id");
    writer.String(std::to_string(m_id++).c_str());
    writer.EndObject();
    return buffer.GetString();
}

std::string MDEngineHbdm::genTradeString(const std::string& symbol)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("sub");
    std::string sub_value("market.");
    sub_value += symbol+".trade.detail";
    writer.String(sub_value.c_str());
    writer.Key("id");
    writer.String(std::to_string(m_id++).c_str());
    writer.EndObject();
    return buffer.GetString();
}

 std::string MDEngineHbdm::genPongString(const std::string& pong)
 {
    int64_t pong64 = stoll(pong);
     rapidjson::StringBuffer buffer;
     rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
     writer.StartObject();
     writer.Key("pong");
     writer.Int64(pong64);
     writer.EndObject();
     return buffer.GetString();

 }

void MDEngineHbdm::connect(long)
{
    m_connected = true;
}
void MDEngineHbdm::OnConnected(const common::CWebsocket* instance)
{
    KF_LOG_INFO(logger,"OnConnected");
    makeWebsocketSubscribeJsonString(instance);
}

void MDEngineHbdm::OnReceivedMessage(const common::CWebsocket* instance,const std::string& msg)
{
    KF_LOG_INFO(logger,"OnReceivedMessage");
    auto dataJson = ldutils::gzip_decompress(msg);
    //KF_LOG_INFO(logger,"dataJson="<<dataJson);
    if(nullptr == m_ThreadPoolPtr)
    {
        handle_lws_data(instance,dataJson);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&MDEngineHbdm::handle_lws_data,this,instance,dataJson));
    }
}

void MDEngineHbdm::OnDisconnected(const common::CWebsocket* instance)
{
    KF_LOG_ERROR(logger, "MDEngineHbdm::on_lws_connection_error.");
    //market logged_in false;
    //logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineHbdm::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    //priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    std::unique_lock<std::mutex> lck(websocket_vec_mutex);
    for(auto it = ws_msg_vec.begin(); it != ws_msg_vec.end(); it++){
        if(it->websocket.m_connection == instance->m_connection)
        {
            it->is_ws_disconnectd = true;
        }
    }
    lck.unlock();
    //login(timeout_nsec);
}

void MDEngineHbdm::login(long)
{
    KF_LOG_DEBUG(logger, "create context start");
    std::vector<WsMsg>::iterator it;
    std::unique_lock<std::mutex> lck(websocket_vec_mutex);
    for(it = ws_msg_vec.begin(); it != ws_msg_vec.end(); it++){
        KF_LOG_INFO(logger,"it->ws_url:"<<it->ws_url);
        it->websocket.RegisterCallBack(this);
        it->logged_in = it->websocket.Connect(it->ws_url);
        KF_LOG_INFO(logger, "MDEngineHbdm::login " << (it->logged_in ? "Success" : "Failed"));
    }
    lck.unlock();
    m_logged_in = true;
}


void MDEngineHbdm::logout()
{
    
    m_logged_in = false;
    KF_LOG_INFO(logger, "logout");
}

void MDEngineHbdm::lwsEventLoop()
{
    KF_LOG_INFO(logger,"lwsEventLoop");
    while( isRunning)
    {

        std::unique_lock<std::mutex> lck0(websocket_vec_mutex);
        for(auto it = ws_msg_vec.begin(); it != ws_msg_vec.end(); it++){
            if(it->is_ws_disconnectd)
            {
                KF_LOG_INFO(logger,"it->ws_url:"<<it->ws_url);
                it->websocket.RegisterCallBack(this);
                it->logged_in = it->websocket.Connect(it->ws_url);
                KF_LOG_INFO(logger, "MDEngineHbdm::relogin " << (it->logged_in ? "Success" : "Failed"));                
                //login(0);
                it->is_ws_disconnectd = false;
            }
        }
        lck0.unlock();

        int errorId = 0;
		string errorMsg = "";
        /*quest3 edited by fxw starts here*/
        /*判断是否在设定时间内更新与否，*/
        int64_t now = getTimestamp();

        std::unique_lock<std::mutex> lck(trade_mutex);
        std::map<std::string,int64_t>::iterator it;
        for(it = control_trade_map.begin(); it != control_trade_map.end(); it++){
            if((now - it->second) > refresh_normal_check_trade_s * 1000){
	            errorId = 115;
	            errorMsg = it->first + " trade max refresh wait time exceeded";
	            KF_LOG_INFO(logger,"115"<<errorMsg); 
	            write_errormsg(errorId,errorMsg);
	            it->second = now;           		
            }
        }
        lck.unlock();

        std::unique_lock<std::mutex> lck1(book_mutex);
        std::map<std::string,int64_t>::iterator it1;
        for(it1 = control_book_map.begin(); it1 != control_book_map.end(); it1++){
            if((now - it1->second) > refresh_normal_check_book_s * 1000){
	            errorId = 114;
	            errorMsg = it1->first + " orderbook max refresh wait time exceeded";
	            KF_LOG_INFO(logger,"114"<<errorMsg); 
	            write_errormsg(errorId,errorMsg);
	            it1->second = now;           		
            }
        } 
        lck1.unlock();

        std::unique_lock<std::mutex> lck2(kline_mutex);
        std::map<std::string,int64_t>::iterator it2;
        for(it2 = control_kline_map.begin(); it2 != control_kline_map.end(); it2++){
            if((now - it2->second) > refresh_normal_check_kline_s * 1000){
	            errorId = 116;
	            errorMsg = it2->first + " kline max refresh wait time exceeded";
	            KF_LOG_INFO(logger,"116"<<errorMsg); 
	            write_errormsg(errorId,errorMsg);
	            it2->second = now;           		
            }
        }    
        lck2.unlock();

    }
    KF_LOG_INFO(logger,"lwsEventLoop end");
}

void MDEngineHbdm::handle_lws_data(const common::CWebsocket* instance, std::string data)
{
    KF_LOG_DEBUG(logger, "received data from hbdm start");
    try
    {
        if(!isRunning)
        {
            return;
        }
        Document json;
        json.Parse(data.c_str());
        KF_LOG_DEBUG(logger, "handle_lws_data:"<< data);
        if(json.HasParseError())
        {
            KF_LOG_ERROR(logger, "received data from hbdm failed,json parse error");
            return;
        }
        if(json.HasMember("ping"))
        {
            parsePingMsg(json,instance);
        }
        else if(json.HasMember("id"))
        {
            //rsp sub;
            parseRspSubscribe(json);
        }
        else if(json.HasMember("ch"))
        {
            //rtn sub
            parseSubscribeData(json);
        }
    }
    catch(const std::exception& e)
    {
        KF_LOG_ERROR(logger, "received data from hbdm exception,{error:" << e.what() << "}");
    }
    catch(...)
    {
        KF_LOG_ERROR(logger, "received data from hbdm system exception");
    }
    KF_LOG_DEBUG(logger, "received data from hbdm end");
}

void MDEngineHbdm::get_snapshot_via_rest()
{
    {
        auto& symbol_map = m_whiteList.GetKeyIsStrategyCoinpairWhiteList();
        for(const auto& item : symbol_map)
        {
            int swap_symbol = item.second.find("-");
            std::string url;
            if(swap_symbol == -1){
                url = "https://api.hbdm.com/market/depth?type=step0&symbol=";
            }else{
                url = "https://api.hbdm.com/swap-ex/market/depth?type=step0&contract_code=";
            }
            url+=item.second;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            //"status":"ok"
            if(d.IsObject() && d.HasMember("status") && d["status"].GetString() == std::string("ok") && d.HasMember("tick"))
            {
                auto& tick = d["tick"];
                LFPriceBook20Field priceBook {0};
                strncpy(priceBook.ExchangeID, "hbdm", sizeof(priceBook.ExchangeID)-1);
                strncpy(priceBook.InstrumentID, item.first.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, item.first.size()));
                if(tick.HasMember("bids") && tick["bids"].IsArray())
                {
                    auto& bids = tick["bids"];
                    int len = std::min((int)bids.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.BidLevels[i].price = std::round(bids[i][0].GetDouble() * SCALE_OFFSET);
                        priceBook.BidLevels[i].volume = std::round(bids[i][1].GetDouble() * SCALE_OFFSET);
                    }
                    priceBook.BidLevelCount = len;
                }
                if (tick.HasMember("asks") && tick["asks"].IsArray())
                {
                    auto& asks = tick["asks"];
                    int len = std::min((int)asks.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.AskLevels[i].price = std::round(asks[i][0].GetDouble() * SCALE_OFFSET);
                        priceBook.AskLevels[i].volume = std::round(asks[i][1].GetDouble() * SCALE_OFFSET);
                    }
                    priceBook.AskLevelCount = len;
                }
                if(tick.HasMember("mrid"))
                {
                    priceBook.UpdateMicroSecond =tick["mrid"].GetInt64();
                }

                BookMsg bookmsg;
                bookmsg.InstrumentID = item.first;
                bookmsg.sequence = priceBook.UpdateMicroSecond;
                bookmsg.time = getTimestamp();
                std::unique_lock<std::mutex> lck_rest_book(rest_book_mutex);
                rest_book_vec.push_back(bookmsg);
                lck_rest_book.unlock();

                on_price_book_update_from_rest(&priceBook);
            }
        }
    }
    

}
void MDEngineHbdm::check_snapshot()
{
    std::vector<BookMsg>::iterator rest_it;
    std::unique_lock<std::mutex> lck_rest_book(rest_book_mutex);
    for(rest_it = rest_book_vec.begin();rest_it != rest_book_vec.end();){
        int64_t now = getTimestamp();
        std::unique_lock<std::mutex> lck_ws_book(ws_book_mutex);
        auto map_itr = ws_book_map.find(rest_it->InstrumentID);
        if(map_itr != ws_book_map.end()){
            std::map<uint64_t, int64_t>::iterator ws_it;
            for(ws_it = map_itr->second.begin(); ws_it != map_itr->second.end();){
                //if(now - ws_it->second > 10000){
                if(ws_it->first < rest_it->sequence){
                    KF_LOG_INFO(logger,"erase old");
                    ws_it = map_itr->second.erase(ws_it);
                    continue;
                }else if(ws_it->first == rest_it->sequence){
                    if(ws_it->second - rest_it->time > snapshot_check_s * 1000){
                        KF_LOG_INFO(logger, "ws snapshot is later than rest snapshot");
                        string errorMsg = "ws snapshot is later than rest snapshot";
                        write_errormsg(115,errorMsg);
                    }
                    KF_LOG_INFO(logger, "same_book:"<<rest_it->InstrumentID);
                    KF_LOG_INFO(logger,"ws_time="<<ws_it->second<<" rest_time="<<rest_it->time);                    
                }
                ws_it++;
            }
        }
        lck_ws_book.unlock();
        rest_it = rest_book_vec.erase(rest_it);
    }
    lck_rest_book.unlock();
}

int64_t last_rest_time = 0;
void MDEngineHbdm::rest_loop()
{
    
        while(isRunning)
        {
            //KF_LOG_INFO(logger,"rest_loop run");
            int64_t now = getTimestamp();
            if((now - last_rest_time) >= rest_get_interval_ms)
            {
                last_rest_time = now;
                get_snapshot_via_rest();
            }
        }
    KF_LOG_INFO(logger,"rest_loop end");
}
int64_t last_check_time = 0;
void MDEngineHbdm::check_loop()
{
    
        while(isRunning)
        {
            int64_t now = getTimestamp();
            if((now - last_check_time) >= rest_get_interval_ms)
            {
                last_check_time = now;
                check_snapshot();
            }
        }
}
void MDEngineHbdm::onClose(struct lws* conn)
{
    KF_LOG_INFO(logger,"onClose");
    if(isRunning)
    {
        reset();
        login(0);
    }
    if(!m_logged_in)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    }

}

void MDEngineHbdm::reset()
{
    m_subcribeIndex = 0;
    m_logged_in     = false;
}

 void MDEngineHbdm::parsePingMsg(const rapidjson::Document& json,const common::CWebsocket* instance)
 {
     //{"pong": 18212553000}
    auto pong = genPongString(std::to_string(json["ping"].GetInt64()));
    KF_LOG_DEBUG(logger, "send pong msg to server,{ pong:" << pong << " }");
    std::unique_lock<std::mutex> lck(websocket_vec_mutex);
    for(auto it = ws_msg_vec.begin(); it != ws_msg_vec.end(); it++){
        if(it->websocket.m_connection == instance->m_connection)
        {
            KF_LOG_INFO(logger,"send_pong");

            it->websocket.SendMessage(pong);
        }
    }
    lck.unlock();
 }

/*
 * right subcribe rsp
{
"id": "id1",
"status": "ok",
"subbed": "market.btcusdt.kline.1min",
"ts": 1489474081631
}
*/

/*
 * error subcribe rsp
{
"id": "id2",
"status": "error",
"err-code": "bad-request",
"err-msg": "invalid topic market.invalidsymbol.kline.1min",
"ts": 1494301904959
}
 */
void MDEngineHbdm::parseRspSubscribe(const rapidjson::Document& json)
{
    if (0 == strcmp(json["status"].GetString(), "error"))
    {
         //ignore failed subcribe
        KF_LOG_INFO(logger, "subscribe sysmbol error");
        return;
    }
    KF_LOG_DEBUG(logger, "subscribe {sysmbol:"<< json["subbed"].GetString()<<"}");
}

void MDEngineHbdm::parseSubscribeData(const rapidjson::Document& json)
{
    KF_LOG_DEBUG(logger, "parseSubscribeData start");
     auto ch = ldutils::split(json["ch"].GetString(), ".");
     if(ch.size() != 4)
     {
         KF_LOG_INFO(logger, "parseSubscribeData [ch] split error");
        return;
     }
     //auto instruments = ldutils::split(ch[1], "_");
     //transform(instruments[0].begin(),instruments[0].end(),instruments[0].begin(),::tolower);
     //transform(instruments[1].begin(),instruments[1].end(),instruments[1].begin(),::tolower);
     //std::string instrument = instruments[0] + "_" + instruments[1] ;
     auto instrument = m_whiteList.GetKeyByValue(ch[1]);
     if(instrument.empty())
     {
         KF_LOG_DEBUG(logger, "parseSubscribeData whiteList has no this {symbol:"<<instrument<<"}");
         return;
     }
     if (ch[2] == "depth")
     {
         doDepthData(json, instrument);
         return;
     }
     else if (ch[2] == "trade")
     {
         doTradeData(json, instrument);
         return;
     }
     else if (ch[2] == "kline")
     {
         doKlineData(json, instrument);
         return;        
     }
}

void MDEngineHbdm::doKlineData(const rapidjson::Document& json, const std::string& instrument)
{
    std::string ticker = m_whiteList.GetKeyByValue(instrument);
     try
     {
         KF_LOG_DEBUG(logger, "doKilneData start");
         if (!json.HasMember("tick"))
         {
             return;
         }
         auto& data = json["tick"];
         if(data.IsArray())
         {
             return;
         }
         auto& tickKLine = mapKLines[instrument];
         LFBarMarketDataField market;
         memset(&market, 0, sizeof(market));
         strcpy(market.InstrumentID, instrument.c_str());
         strcpy(market.ExchangeID, "hbdm");

         ///注意time检查
         time_t now = time(0);
         struct tm tradeday = *localtime(&now);
         strftime(market.TradingDay, 9, "%Y%m%d", &tradeday);

         
         int64_t nStartTime =data["id"].GetInt64();//( (json["ts"].GetInt64() - 60000)/ 60000 ) * 60000;
         int64_t nEndTime = nStartTime + 59;
         market.StartUpdateMillisec = nStartTime*1000;
         struct tm start_tm = *localtime((time_t*)(&nStartTime));
         sprintf(market.StartUpdateTime,"%02d:%02d:%02d.000", start_tm.tm_hour,start_tm.tm_min,start_tm.tm_sec);

         market.EndUpdateMillisec = market.StartUpdateMillisec+59999;
         struct tm end_tm = *localtime((time_t*)(&nEndTime));
         sprintf(market.EndUpdateTime,"%02d:%02d:%02d.999", end_tm.tm_hour,end_tm.tm_min,end_tm.tm_sec);
         market.PeriodMillisec = 60000;
         market.Open = std::round(data["open"].GetDouble() * SCALE_OFFSET);
         market.Close = std::round(data["close"].GetDouble() * SCALE_OFFSET);
         market.Low = std::round(data["low"].GetDouble() * SCALE_OFFSET);
         market.High = std::round(data["high"].GetDouble() * SCALE_OFFSET);      
         market.Volume = std::round(data["amount"].GetDouble() * SCALE_OFFSET);
         auto it = tickKLine.find(nStartTime);
         if(tickKLine.size() == 0 || it != tickKLine.end())
         {
            tickKLine[nStartTime]=market;
            KF_LOG_INFO(logger, "doKlineData(cached): StartUpdateMillisec "<< market.StartUpdateMillisec << " StartUpdateTime "<<market.StartUpdateTime << " EndUpdateMillisec "<< market.EndUpdateMillisec << " EndUpdateTime "<<market.EndUpdateTime
                << "Open" << market.Open << " Close " <<market.Close << " Low " <<market.Low << " Volume " <<market.Volume); 
         }
         else
         {
            on_market_bar_data(&(tickKLine.begin()->second));
            tickKLine.clear();
            tickKLine[nStartTime]=market;
            KF_LOG_INFO(logger, "doKlineData: StartUpdateMillisec "<< market.StartUpdateMillisec << " StartUpdateTime "<<market.StartUpdateTime << " EndUpdateMillisec "<< market.EndUpdateMillisec << " EndUpdateTime "<<market.EndUpdateTime
                << "Open" << market.Open << " Close " <<market.Close << " Low " <<market.Low << " Volume " <<market.Volume);
         }
         
        std::unique_lock<std::mutex> lck2(kline_mutex);
	    auto it1 = control_kline_map.find(ticker);
	    if(it1 != control_kline_map.end())
	    {
		    it1->second = getTimestamp();
	    }
	    lck2.unlock();
             
     }
     catch (const std::exception& e)
     {
         KF_LOG_INFO(logger, "doKlineData,{error:"<< e.what()<<"}");
     }
     KF_LOG_DEBUG(logger, "doKlineData end");
}

void MDEngineHbdm::doDepthData(const rapidjson::Document& json, const std::string& instrument)
{
    std::string ticker = m_whiteList.GetKeyByValue(instrument);
    try
    {
        KF_LOG_DEBUG(logger, "doDepthData start");
        if (!json.HasMember("tick"))
        {
            return;
        }
        auto& tick = json["tick"];
        if (!tick.HasMember("bids"))
        {
            return;
        }
        auto& bids = tick["bids"];

        if (!tick.HasMember("asks"))
        {
            return;
        }
        auto& asks = tick["asks"];
        LFPriceBook20Field priceBook {0};
        strncpy(priceBook.ExchangeID, "hbdm", std::min<size_t>(sizeof(priceBook.ExchangeID)-1, 5));
        strncpy(priceBook.InstrumentID, instrument.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, instrument.size()));
        if(bids.IsArray())
        {
            int i = 0;
            for(i = 0; i < std::min((int)bids.Size(),m_priceBookNum); ++i)
            {
                priceBook.BidLevels[i].price = std::round(bids[i][0].GetDouble() * SCALE_OFFSET);
                priceBook.BidLevels[i].volume = std::round(bids[i][1].GetDouble() * SCALE_OFFSET);
            }
            priceBook.BidLevelCount = i;
        }
        if (asks.IsArray())
        {
            int i = 0;
            for(i = 0; i < std::min((int)asks.Size(),m_priceBookNum); ++i)
            {
                priceBook.AskLevels[i].price = std::round(asks[i][0].GetDouble() * SCALE_OFFSET);
                priceBook.AskLevels[i].volume = std::round(asks[i][1].GetDouble() * SCALE_OFFSET);
            }
            priceBook.AskLevelCount = i;
        }
        if(tick.HasMember("mrid"))
        {
            priceBook.UpdateMicroSecond =tick["mrid"].GetInt64();
        }
        /*edited by zyy,starts here*/
        KF_LOG_INFO(logger, "MDEngineHbdm::onBook: BidLevelCount="<<priceBook.BidLevelCount<<",AskLevelCount="<<priceBook.AskLevelCount<<",level_threshold="<<level_threshold);
        timer = getTimestamp();
        if(priceBook.BidLevelCount < level_threshold || priceBook.AskLevelCount < level_threshold)
        {
            string errorMsg = "orderbook level below threshold";
            write_errormsg(112,errorMsg);
            on_price_book_update(&priceBook);
        }
        else if(priceBook.BidLevels[0].price <=0 || priceBook.AskLevels[0].price <=0 || priceBook.BidLevels[0].price > priceBook.AskLevels[0].price)
        {
            string errorMsg = "orderbook crossed";
            write_errormsg(113,errorMsg);
        }
        /*edited by zyy ends here*/
        else
        {
            int64_t now = getTimestamp();
            std::unique_lock<std::mutex> lck_ws_book(ws_book_mutex);
            auto itr = ws_book_map.find(instrument);
            if(itr == ws_book_map.end()){
                std::map<uint64_t, int64_t> bookmsg_map;
                bookmsg_map.insert(std::make_pair(priceBook.UpdateMicroSecond, now));
                ws_book_map.insert(std::make_pair(instrument, bookmsg_map));
                KF_LOG_INFO(logger,"insert:"<<instrument);
            }else{
                itr->second.insert(std::make_pair(priceBook.UpdateMicroSecond, now));
            }
            lck_ws_book.unlock();

            on_price_book_update(&priceBook);
        }
        std::unique_lock<std::mutex> lck1(book_mutex);
	    auto it = control_book_map.find(ticker);
	    if(it != control_book_map.end())
	    {
		    it->second = getTimestamp();
	    }
	    lck1.unlock();
    }
    catch (const std::exception& e)
    {
        KF_LOG_INFO(logger, "doDepthData,{error:"<< e.what()<<"}");
    }
     KF_LOG_DEBUG(logger, "doDepthData end");
}

void MDEngineHbdm::doTradeData(const rapidjson::Document& json, const std::string& instrument)
{
    std::string ticker = m_whiteList.GetKeyByValue(instrument);
    try
    {
        KF_LOG_DEBUG(logger, "doTradeData start");
        if (!json.HasMember("tick"))
        {
            return;
        }
        auto& tick = json["tick"];
        if (!tick.HasMember("data"))
        {
            return;
        }
        auto& data = tick["data"];
        if(data.Empty())
        {
            return;
        }
        LFL2TradeField trade{0};
        strncpy(trade.ExchangeID, "hbdm", std::min((int)sizeof(trade.ExchangeID)-1, 5));
        strncpy(trade.InstrumentID, instrument.c_str(),std::min(sizeof(trade.InstrumentID)-1, instrument.size()));
        auto& first_data = data[0];
        trade.Volume =  std::round(first_data["amount"].GetDouble() * SCALE_OFFSET);
        trade.Price  =  std::round(first_data["price"].GetDouble() * SCALE_OFFSET);
        std::string TradeID = std::to_string(first_data["id"].GetInt64());
        trade.TimeStamp = first_data["ts"].GetInt64();
        string strTime  = timestamp_to_formatISO8601(trade.TimeStamp);
        trade.TimeStamp*=1000000;
        strncpy(trade.TradeID , TradeID.c_str() , sizeof(trade.TradeID));
        strncpy(trade.TradeTime , strTime.c_str() , sizeof(trade.TradeTime));
        std::string side = first_data["direction"].GetString();
        trade.OrderBSFlag[0] = side == "buy" ? 'B' : 'S';

        std::unique_lock<std::mutex> lck(trade_mutex);
	    auto it = control_trade_map.find(ticker);
	    if(it != control_trade_map.end())
	    {
		    it->second = getTimestamp();
	    }
	    lck.unlock();

        on_trade(&trade);
    }
    catch (const std::exception& e)
    {
        KF_LOG_INFO(logger, "doTradeData,{error:"<< e.what()<<"}");
    }
    KF_LOG_DEBUG(logger, "doTradeData end");
}



/*edited by zyy,starts here*/
inline int64_t MDEngineHbdm::getTimestamp()
{   /*返回的是毫秒*/
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}
/*edited by zyy ends here*/

BOOST_PYTHON_MODULE(libhbdmmd)
{
    using namespace boost::python;
    class_<MDEngineHbdm, boost::shared_ptr<MDEngineHbdm> >("Engine")
            .def(init<>())
            .def("init", &MDEngineHbdm::initialize)
            .def("start", &MDEngineHbdm::start)
            .def("stop", &MDEngineHbdm::stop)
            .def("logout", &MDEngineHbdm::logout)
            .def("wait_for_stop", &MDEngineHbdm::wait_for_stop);
}

WC_NAMESPACE_END