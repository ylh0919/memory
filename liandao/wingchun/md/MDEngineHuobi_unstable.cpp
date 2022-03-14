//
// Created by wang on 10/20/18.
//
#include "MDEngineHuobi.h"
#include "../../utils/common/ld_utils.h"
#include <stringbuffer.h>
#include <writer.h>
#include <document.h>
#include <libwebsockets.h>
#include <algorithm>
#include <cpr/cpr.h>
#include <stdio.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
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
//lws event function

std::mutex book_mutex;
std::mutex trade_mutex;

static int lwsEventCallback( struct lws *conn, enum lws_callback_reasons reason, void *user, void* data, size_t len );
static  struct lws_protocols  lwsProtocols [] {{"md-protocol", lwsEventCallback, 0, 65536,}, { NULL, NULL, 0, 0 }};

MDEngineHuobi* MDEngineHuobi::m_instance = nullptr;
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;

MDEngineHuobi::MDEngineHuobi(): IMDEngine(SOURCE_HUOBI)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Huobi");
    KF_LOG_DEBUG(logger, "MDEngineHuobi construct");
    timer = getTimestamp();/*edited by zyy*/
}

MDEngineHuobi::~MDEngineHuobi()
{
    if (m_thread)
    {
        if(m_thread->joinable())
        {
            m_thread->join();
        }
    }
    KF_LOG_DEBUG(logger, "MDEngineHuobi deconstruct");
}

void MDEngineHuobi::set_reader_thread()
{
    IMDEngine::set_reader_thread();
    m_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineHuobi::lwsEventLoop, this)));
    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineHuobi::rest_loop, this)));
    check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineHuobi::check_loop, this)));
}

void MDEngineHuobi::load(const json& config)
{
    KF_LOG_INFO(logger, "load config start");
    try
    {
        m_priceBookNum = config["book_depth_count"].get<int>();
        if (!parseAddress(config["exchange_url"].get<std::string>()))
        {
            return;
        }
        m_whiteList.ReadWhiteLists(config, "whiteLists");
        m_whiteList.Debug_print();
        genSubscribeString();
    }
    catch (const std::exception& e)
    {
        KF_LOG_INFO(logger, "load config exception,"<<e.what());
    }
    /*edited by zyy,starts here*/
    if(config.find("level_threshold") != config.end()) {
        level_threshold = config["level_threshold"].get<int>();
    }
    if(config.find("refresh_normal_check_book_s") != config.end()) {
        refresh_normal_check_book_s = config["refresh_normal_check_book_s"].get<int>();
    }
    if(config.find("refresh_normal_check_trade_s") != config.end()) {
        refresh_normal_check_trade_s = config["refresh_normal_check_trade_s"].get<int>();
    }
    int64_t nowTime = getTimestamp();
    std::unordered_map<std::string, std::string>::iterator it;
    for(it = m_whiteList.GetKeyIsStrategyCoinpairWhiteList().begin();it != m_whiteList.GetKeyIsStrategyCoinpairWhiteList().end();it ++)
    {
        std::unique_lock<std::mutex> lck(trade_mutex);
        control_trade_map.insert(make_pair(it->first, nowTime));
        lck.unlock();

        std::unique_lock<std::mutex> lck1(book_mutex);
        control_book_map.insert(make_pair(it->first, nowTime));
        lck1.unlock();
    }
    /*edited by zyy ends here*/
    if(config.find("snapshot_check_s") != config.end()) {
        snapshot_check_s = config["snapshot_check_s"].get<int>();
    }
    if(config.find("rest_get_interval_ms") != config.end()) {
        rest_get_interval_ms = config["rest_get_interval_ms"].get<int>();
    }
    KF_LOG_INFO(logger, "load config end");
}

void MDEngineHuobi::genSubscribeString()
{
    auto& symbol_map = m_whiteList.GetKeyIsStrategyCoinpairWhiteList();
    for(const auto& var : symbol_map)
    {
        //#m_subcribeJsons.push_back(genDepthString(var.second));
        //#m_subcribeJsons.push_back(genTradeString(var.second));
    }
    /*
    if(m_subcribeJsons.empty())
    {
        KF_LOG_INFO(logger, "genSubscribeString failed, {error:has no white list}");
        exit(0);
    }*/
}

std::string MDEngineHuobi::genDepthString(const std::string& symbol)
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

std::string MDEngineHuobi::genTradeString(const std::string& symbol)
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

 std::string MDEngineHuobi::genPongString(const std::string& pong)
 {
     rapidjson::StringBuffer buffer;
     rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
     writer.StartObject();
     writer.Key("pong");
     writer.String(pong.c_str());
     writer.EndObject();
     return buffer.GetString();

 }

void MDEngineHuobi::connect(long)
{
    m_connected = true;
}

void MDEngineHuobi::login(long)
{
    KF_LOG_DEBUG(logger, "create context start");
    m_instance = this;
    struct lws_context_creation_info creation_info;
    memset(&creation_info, 0x00, sizeof(creation_info));
    creation_info.port = CONTEXT_PORT_NO_LISTEN;
    creation_info.protocols = lwsProtocols;
    creation_info.gid = -1;
    creation_info.uid = -1;
    creation_info.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    creation_info.max_http_header_pool = 1024;
    creation_info.fd_limit_per_thread = 1024;
    m_lwsContext = lws_create_context( &creation_info );
    if (!m_lwsContext)
    {
        KF_LOG_ERROR(logger, "create context error");
        return;
    }
    KF_LOG_INFO(logger, "create context success");
    createConnection();
}

void MDEngineHuobi::createConnection()
{
    KF_LOG_DEBUG(logger, "create connect start");
    struct lws_client_connect_info conn_info = { 0 };
    //parse uri
    conn_info.context 	= m_lwsContext;
    conn_info.protocol = m_exchUrl.protocol.c_str();
    conn_info.address = m_exchUrl.ip.c_str();
    conn_info.port = m_exchUrl.port;
    conn_info.path 	= m_exchUrl.path.c_str();
    conn_info.host 	= conn_info.address;
    conn_info.origin = conn_info.address;
    conn_info.ietf_version_or_minus_one = -1;
    conn_info.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    m_lwsConnection = lws_client_connect_via_info(&conn_info);
    if(!m_lwsConnection)
    {
        KF_LOG_INFO(logger, "create connect error");
        return ;
    }
    KF_LOG_INFO(logger, "connect to "<< conn_info.protocol<< "://" << conn_info.address <<conn_info.path<< ":"<< conn_info.port <<" success");
    m_logged_in = true;
}

void MDEngineHuobi::logout()
{
    lws_context_destroy(m_lwsContext);
    m_logged_in = false;
    KF_LOG_INFO(logger, "logout");
}

void MDEngineHuobi::get_snapshot_via_rest()
{
    {
        std::unordered_map<std::string, std::string>::iterator map_itr;
        for(map_itr = m_whiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); map_itr != m_whiteList.GetKeyIsStrategyCoinpairWhiteList().end(); map_itr++)
        {
            std::string url = "https://api.huobi.pro/market/depth?depth=20&type=step0&symbol=";
            url+=map_itr->second;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            //"code":"200000"
            if(d.IsObject() && d.HasMember("tick"))
            {
                //uint64_t sequence = d["ts"].GetUint64();

                auto& tick = d["tick"];
                uint64_t sequence = tick["ts"].GetUint64();
                
                LFPriceBook20Field priceBook {0};
                strcpy(priceBook.ExchangeID, "huobi");
                strncpy(priceBook.InstrumentID, map_itr->first.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, map_itr->first.size()));
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

                BookMsg bookmsg;
                bookmsg.InstrumentID = map_itr->first;
                bookmsg.sequence = sequence;
                bookmsg.time = getTimestamp();
                std::unique_lock<std::mutex> lck_rest_book(rest_book_mutex);
                rest_book_vec.push_back(bookmsg);
                lck_rest_book.unlock();

                on_price_book_update_from_rest(&priceBook);
            }
        }
    }
    

}

void MDEngineHuobi::check_snapshot()
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
void MDEngineHuobi::rest_loop()
{
        while(isRunning)
        {
            int64_t now = getTimestamp();
            if((now - last_rest_time) >= rest_get_interval_ms)
            {
                last_rest_time = now;
                get_snapshot_via_rest();
            }
        }
}
int64_t last_check_time = 0;
void MDEngineHuobi::check_loop()
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

void MDEngineHuobi::lwsEventLoop()
{
    while( isRunning)
    {
        /*edited by zyy,starts here*/
        int errorId = 0;
        std::string errorMsg = "";
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
        
        /*edited by zyy ends here*/
        lws_service(m_lwsContext, 500);
    }
}

void MDEngineHuobi::sendMessage(std::string&& msg)
{
    msg.insert(msg.begin(),  LWS_PRE, 0x00);
    lws_write(m_lwsConnection, (uint8_t*)(msg.data() + LWS_PRE), msg.size() - LWS_PRE, LWS_WRITE_TEXT);
}

void MDEngineHuobi::onMessage(struct lws* conn, char* data, size_t len)
{
    KF_LOG_DEBUG(logger, "received data from huobi start");
    try
    {
        if(!isRunning)
        {
            return;
        }
        Document json;
        auto dataJson = ldutils::gzip_decompress(std::string(data,len));
        json.Parse(dataJson.c_str());
        KF_LOG_DEBUG(logger, "received data from huobi,{msg:"<< dataJson<< "}");
        if(json.HasParseError())
        {
            KF_LOG_ERROR(logger, "received data from huobi failed,json parse error");
            return;
        }
        if(json.HasMember("ping"))
        {
            parsePingMsg(json);
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
        KF_LOG_ERROR(logger, "received data from huobi exception,{error:" << e.what() << "}");
    }
    catch(...)
    {
        KF_LOG_ERROR(logger, "received data from huobi system exception");
    }
    KF_LOG_DEBUG(logger, "received data from huobi end");
}

void MDEngineHuobi::onClose(struct lws* conn)
{
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

void MDEngineHuobi::reset()
{
    m_subcribeIndex = 0;
    m_logged_in     = false;
}

void MDEngineHuobi::onWrite(struct lws* conn)
{
    if(!isRunning)
    {
        return;
    }
    KF_LOG_DEBUG(logger, "subcribe start");
    if (m_subcribeJsons.empty() || m_subcribeIndex == -1)
    {
        KF_LOG_DEBUG(logger, "subcribe ignore");
        return;
    }
    auto symbol = m_subcribeJsons[m_subcribeIndex++];
    KF_LOG_DEBUG(logger, "req subcribe " << symbol);
    sendMessage(std::move(symbol));
    if(m_subcribeIndex >= m_subcribeJsons.size())
    {
        m_subcribeIndex = -1;
        KF_LOG_DEBUG(logger, "subcribe end");
        return;
    }
    if(isRunning)
    {
        lws_callback_on_writable(conn);
    }
    KF_LOG_DEBUG(logger, "subcribe continue");
}

 void MDEngineHuobi::parsePingMsg(const rapidjson::Document& json)
 {
     //{"pong": 18212553000}
     try
     {
         auto pong = genPongString(std::to_string(json["ping"].GetInt64()));
         KF_LOG_DEBUG(logger, "send pong msg to server,{ pong:" << pong << " }");
         sendMessage(std::move(pong));
     }
     catch (const std::exception& e)
     {
         KF_LOG_INFO(logger, "parsePingMsg,{error:"<< e.what()<<"}");
     }
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
 void MDEngineHuobi::parseRspSubscribe(const rapidjson::Document& json)
 {
     if (0 == strcmp(json["status"].GetString(), "error"))
     {
         //ignore failed subcribe
         KF_LOG_INFO(logger, "subscribe sysmbol error");
         return;
     }
     KF_LOG_DEBUG(logger, "subscribe {sysmbol:"<< json["subbed"].GetString()<<"}");
 }

 void MDEngineHuobi::parseSubscribeData(const rapidjson::Document& json)
 {
     KF_LOG_DEBUG(logger, "parseSubscribeData start");
     auto ch = ldutils::split(json["ch"].GetString(), ".");
     if(ch.size() != 4)
     {
         KF_LOG_INFO(logger, "parseSubscribeData [ch] split error");
        return;
     }
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
     if (ch[2] == "trade")
     {
         doTradeData(json, instrument);
         return;
     }
 }

 void MDEngineHuobi::doDepthData(const rapidjson::Document& json, const std::string& instrument)
 {
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
        uint64_t sequence;
        if(json.HasMember("ts")){
            sequence = json["ts"].GetUint64();
        }
        auto& asks = tick["asks"];
        LFPriceBook20Field priceBook {0};
        strncpy(priceBook.ExchangeID, "huobi", std::min<size_t>(sizeof(priceBook.ExchangeID)-1, 5));
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
        /*edited by zyy,starts here*/
        //timer = getTimestamp();
        std::unique_lock<std::mutex> lck1(book_mutex);
        auto it = control_book_map.find(priceBook.InstrumentID);
        if(it != control_book_map.end())
        {
            it->second = getTimestamp();
        }
        lck1.unlock();

        if(priceBook.BidLevelCount < level_threshold || priceBook.AskLevelCount < level_threshold)
        {
            KF_LOG_INFO(logger, "onBook: failed,level count < level threshold :"<<priceBook.BidLevelCount<<" "<<priceBook.AskLevelCount<<" "<<level_threshold);
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
                bookmsg_map.insert(std::make_pair(sequence, now));
                ws_book_map.insert(std::make_pair(instrument, bookmsg_map));
                KF_LOG_INFO(logger,"insert:"<<instrument);
            }else{
                itr->second.insert(std::make_pair(sequence, now));
            }
            lck_ws_book.unlock();

            on_price_book_update(&priceBook);
        }
    }
    catch (const std::exception& e)
    {
        KF_LOG_INFO(logger, "doDepthData,{error:"<< e.what()<<"}");
    }
     KF_LOG_DEBUG(logger, "doDepthData end");
 }

 void MDEngineHuobi::doTradeData(const rapidjson::Document& json, const std::string& instrument)
 {
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
         strncpy(trade.ExchangeID, "huobi", std::min((int)sizeof(trade.ExchangeID)-1, 5));
         strncpy(trade.InstrumentID, instrument.c_str(),std::min(sizeof(trade.InstrumentID)-1, instrument.size()));
         auto& first_data = data[0];
         trade.TimeStamp = first_data["ts"].GetInt64();
         string strTime  = timestamp_to_formatISO8601(trade.TimeStamp);
         strncpy(trade.TradeTime, strTime.c_str(),sizeof(trade.TradeTime));
         trade.TimeStamp*=1000000;
         trade.Volume =  std::round(first_data["amount"].GetDouble() * SCALE_OFFSET);
         trade.Price  =  std::round(first_data["price"].GetDouble() * SCALE_OFFSET);
         std::string side = first_data["direction"].GetString();
         trade.OrderBSFlag[0] = side == "buy" ? 'B' : 'S';

        std::unique_lock<std::mutex> lck(trade_mutex);
        auto it = control_trade_map.find(trade.InstrumentID);
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

bool MDEngineHuobi::parseAddress(const std::string& exch_url)
{
    try
    {
        //url format is xxx.xxx.xxx/xxx
        std::vector<std::string> result;
        boost::split(result, exch_url, boost::is_any_of("/"));
        if (result.size() != 2)
        {
            KF_LOG_INFO(logger, "parse exchange url error, must be xxx://xxx.xxx.xxx:xxx/");
            return false;
        }
        m_exchUrl.protocol = "wss";
        m_exchUrl.ip = result[0];
        m_exchUrl.port = 443;
        m_exchUrl.path = "/" + result[1];
        return  true;
    }
    catch (std::exception& e)
    {
        KF_LOG_INFO(logger, "parseAddress, exception:"<< e.what());
    }
    return false;
}

int lwsEventCallback( struct lws *conn, enum lws_callback_reasons reason, void *, void *data , size_t len )
{
    switch( reason )
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            lws_callback_on_writable( conn );
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {
            if(MDEngineHuobi::m_instance)
            {
                MDEngineHuobi::m_instance->onMessage(conn, (char*)data, len);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {
            if(MDEngineHuobi::m_instance)
            {
                MDEngineHuobi::m_instance->onWrite(conn);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_CLOSED:
        case LWS_CALLBACK_CLOSED:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
            if(MDEngineHuobi::m_instance)
            {
                MDEngineHuobi::m_instance->onClose(conn);
            }
            break;
        }
        default:
            break;
    }
    return 0;
}

/*edited by zyy,starts here*/
inline int64_t MDEngineHuobi::getTimestamp()
{   /*返回的是毫秒*/
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}
/*edited by zyy ends here*/

BOOST_PYTHON_MODULE(libhuobimd)
{
    using namespace boost::python;
    class_<MDEngineHuobi, boost::shared_ptr<MDEngineHuobi> >("Engine")
            .def(init<>())
            .def("init", &MDEngineHuobi::initialize)
            .def("start", &MDEngineHuobi::start)
            .def("stop", &MDEngineHuobi::stop)
            .def("logout", &MDEngineHuobi::logout)
            .def("wait_for_stop", &MDEngineHuobi::wait_for_stop);
}

WC_NAMESPACE_END