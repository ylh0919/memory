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
//lws event function
static int lwsEventCallback( struct lws *conn, enum lws_callback_reasons reason, void *user, void* data, size_t len );
static  struct lws_protocols  lwsProtocols [] {{"md-protocol", lwsEventCallback, 0, 65536,}, { NULL, NULL, 0, 0 }};

MDEngineHbdm* MDEngineHbdm::m_instance = nullptr;

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
}

void MDEngineHbdm::load(const json& config)
{
    KF_LOG_INFO(logger, "load config start");
    try
    {
        /*edited by zyy,starts here*/
        if (config.find("level_threshold") != config.end()) {
            level_threshold = config["level_threshold"].get<int>();
        }
        if (config.find("refresh_normal_check_book_s") != config.end()) {
            refresh_normal_check_book_s = config["refresh_normal_check_book_s"].get<int>();
        }
        if (config.find("rest_get_interval_ms") != config.end()) {
            rest_get_interval_ms = config["rest_get_interval_ms"].get<int>();
        }
        /*edited by zyy ends here*/

        //subscribe two kline
        /*if (config.find("kline_a_interval") != config.end())
            kline_a_interval = config["kline_a_interval"].get<string>();
        if (config.find("kline_b_interval") != config.end())
            kline_b_interval = config["kline_b_interval"].get<string>();*/
        if (config.find("kline_a_interval_s") != config.end())
            kline_a_interval = getIntervalStr(config["kline_a_interval_s"].get<int64_t>());
        if (config.find("kline_b_interval_s") != config.end())
            kline_b_interval = getIntervalStr(config["kline_b_interval_s"].get<int64_t>());

        m_priceBookNum = config["book_depth_count"].get<int>();
        if (!parseAddress(config["exchange_url"].get<std::string>()))
        {
            return;
        }
        int thread_pool_size = 0;
        if (config.find("thread_pool_size") != config.end()) {
            thread_pool_size = config["thread_pool_size"].get<int>();
        }
        KF_LOG_INFO(logger, "thread_pool_size:" << thread_pool_size);
        if (thread_pool_size > 0)
        {
            m_ThreadPoolPtr = new ThreadPool(thread_pool_size);
        }
        m_whiteList.ReadWhiteLists(config, "whiteLists");
        m_whiteList.Debug_print();
        genSubscribeString();
    }
    catch (const std::exception& e)
    {
        KF_LOG_INFO(logger, "load config exception," << e.what());
    }
    KF_LOG_INFO(logger, "load config end");
}

void MDEngineHbdm::genSubscribeString()
{
    auto& symbol_map = m_whiteList.GetKeyIsStrategyCoinpairWhiteList();
    for (const auto& var : symbol_map)
    {
        {
            m_subcribeJsons.push_back(genDepthString(var.second));
            m_subcribeJsons.push_back(genTradeString(var.second));
            //m_subcribeJsons.push_back(genKlineString(var.second));
            //subscribe two kline
            if (kline_a_interval != kline_b_interval) {
                m_subcribeJsons.push_back(genKlineString(var.second, kline_a_interval));
                m_subcribeJsons.push_back(genKlineString(var.second, kline_b_interval));
            }
            else
                m_subcribeJsons.push_back(genKlineString(var.second, kline_a_interval));
        }
    }
    if (m_subcribeJsons.empty())
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

std::string MDEngineHbdm::genKlineString(const std::string& symbol, const std::string& interval)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("sub");
    std::string sub_value("market.");
    sub_value += symbol + ".kline." + interval;
    writer.String(sub_value.c_str());
    writer.Key("id");
    writer.String(std::to_string(m_id++).c_str());
    writer.EndObject();
    return buffer.GetString();
}

std::string MDEngineHbdm::getIntervalStr(int64_t sec)
{
    switch (sec) {
    case 60:
        return "1min";
    case 300:
        return "5min";
    case 900:
        return "15min";
    case 1800:
        return "30min";
    case 3600:
        return "60min";
    case 14400:
        return "4hour";
    case 86400:
        return "1day";
    case 604800:
        return "1week";
    case 2678400:
        return "1mon";
    case 31536000:
        return "1year";
    }
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

void MDEngineHbdm::login(long)
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

void MDEngineHbdm::createConnection()
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

void MDEngineHbdm::logout()
{
    lws_context_destroy(m_lwsContext);
    m_logged_in = false;
    KF_LOG_INFO(logger, "logout");
}

void MDEngineHbdm::lwsEventLoop()
{
    while( isRunning)
    {
        /*edited by zyy,starts here*/
        int64_t now = getTimestamp();
        KF_LOG_INFO(logger,"now = "<<now<<",timer = "<<timer<<", refresh_normal_check_book_s="<<refresh_normal_check_book_s);
        if ((now - timer) > refresh_normal_check_book_s * 1000)
        {
            KF_LOG_INFO(logger, "failed price book update");
            write_errormsg(114,"orderbook max refresh wait time exceeded");
            timer = now;
        }
        /*edited by zyy ends here*/
        lws_service(m_lwsContext, 500);
    }
}

void MDEngineHbdm::sendMessage(std::string&& msg)
{
    msg.insert(msg.begin(),  LWS_PRE, 0x00);
    lws_write(m_lwsConnection, (uint8_t*)(msg.data() + LWS_PRE), msg.size() - LWS_PRE, LWS_WRITE_TEXT);
}

void MDEngineHbdm::onMessage(struct lws* conn, char* data, size_t len)
{
    auto dataJson = ldutils::gzip_decompress(std::string(data,len));
    if(nullptr == m_ThreadPoolPtr)
    {
        handle_lws_data(conn,dataJson);
    }
    else
    {
        m_ThreadPoolPtr->commit(std::bind(&MDEngineHbdm::handle_lws_data,this,conn,dataJson));
    }
}
void MDEngineHbdm::handle_lws_data(struct lws* conn, std::string data)
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
        KF_LOG_DEBUG(logger, "received data from hbdm,{msg:"<< data<< "}");
        if(json.HasParseError())
        {
            KF_LOG_ERROR(logger, "received data from hbdm failed,json parse error");
            return;
        }
        if(json.HasMember("ping"))
        {
            parsePingMsg(json,conn);
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
             std::string url = "https://api.hbdm.com/market/depth?type=step0&symbol=";
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
                on_price_book_update_from_rest(&priceBook);
            }
        }
    }
    

}
int64_t last_rest_time = 0;
void MDEngineHbdm::rest_loop()
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

void MDEngineHbdm::onWrite(struct lws* conn)
{
    if(!isRunning)
    {
        return;
    }
    KF_LOG_DEBUG(logger, "subcribe start");
    if ((m_subcribeJsons.empty() || m_subcribeIndex == -1) && pong_queue.empty())
    {
        KF_LOG_DEBUG(logger, "subcribe ignore");
        return;
    }
    if(pong_queue.size() > 0){
        auto symbol = pong_queue.front();
        pong_queue.pop();
        KF_LOG_DEBUG(logger, "req pong " << symbol);
        sendMessage(std::move(symbol));
    }
    else if(m_subcribeJsons.size() > 0){
        auto symbol = m_subcribeJsons[m_subcribeIndex++];
        KF_LOG_DEBUG(logger, "req subcribe " << symbol);
        sendMessage(std::move(symbol));        
    }

    if(m_subcribeIndex >= m_subcribeJsons.size())
    {
        m_subcribeIndex = -1;
        KF_LOG_DEBUG(logger, "subcribe end");
        //return;
    }
    if(isRunning)
    {
        lws_callback_on_writable(conn);
    }
    KF_LOG_DEBUG(logger, "subcribe continue");
}

 void MDEngineHbdm::parsePingMsg(const rapidjson::Document& json,struct lws* conn)
 {
     //{"pong": 18212553000}
     try
     {
         auto pong = genPongString(std::to_string(json["ping"].GetInt64()));
         KF_LOG_DEBUG(logger, "send pong msg to server,{ pong:" << pong << " }");
         pong_queue.push(pong);
         lws_callback_on_writable(conn);
         //sendMessage(std::move(pong));
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
         //doKlineData(json, instrument);
         doKlineData(json, instrument, ch[3]);
         return;        
     }
 }

void MDEngineHbdm::doKlineData(const rapidjson::Document& json, const std::string& instrument)
 {
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
     }
     catch (const std::exception& e)
     {
         KF_LOG_INFO(logger, "doKlineData,{error:"<< e.what()<<"}");
     }
     KF_LOG_DEBUG(logger, "doKlineData end");
 }

void MDEngineHbdm::doKlineData(const rapidjson::Document& json, const std::string& instrument, const std::string& interval)
{
    try
    {
        KF_LOG_DEBUG(logger, "doKilneData start");
        if (!json.HasMember("tick"))
        {
            return;
        }
        auto& data = json["tick"];
        if (data.IsArray())
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


        int64_t nStartTime = data["id"].GetInt64();//( (json["ts"].GetInt64() - 60000)/ 60000 ) * 60000;
        //int64_t nEndTime = nStartTime + 59;
        market.StartUpdateMillisec = nStartTime * 1000;
        struct tm start_tm = *localtime((time_t*)(&nStartTime));
        sprintf(market.StartUpdateTime, "%02d:%02d:%02d.000", start_tm.tm_hour, start_tm.tm_min, start_tm.tm_sec);

        //market.EndUpdateMillisec = market.StartUpdateMillisec + 59999;
        //struct tm end_tm = *localtime((time_t*)(&nEndTime));
        //sprintf(market.EndUpdateTime, "%02d:%02d:%02d.999", end_tm.tm_hour, end_tm.tm_min, end_tm.tm_sec);
        //market.PeriodMillisec = 60000;
        market.PeriodMillisec = GetMillsecondByInterval(interval);
        market.EndUpdateMillisec = market.StartUpdateMillisec + market.PeriodMillisec - 1;
        int64_t nEndTime = nStartTime + market.PeriodMillisec / 1000 - 1;
        struct tm end_tm = *localtime((time_t*)(&nEndTime));
        sprintf(market.EndUpdateTime, "%02d:%02d:%02d.999", end_tm.tm_hour, end_tm.tm_min, end_tm.tm_sec);

        market.Open = std::round(data["open"].GetDouble() * SCALE_OFFSET);
        market.Close = std::round(data["close"].GetDouble() * SCALE_OFFSET);
        market.Low = std::round(data["low"].GetDouble() * SCALE_OFFSET);
        market.High = std::round(data["high"].GetDouble() * SCALE_OFFSET);
        market.Volume = std::round(data["amount"].GetDouble() * SCALE_OFFSET);
        auto it = tickKLine.find(nStartTime);
        if (tickKLine.size() == 0 || it != tickKLine.end())
        {
            tickKLine[nStartTime] = market;
            KF_LOG_INFO(logger, "doKlineData(cached): StartUpdateMillisec " << market.StartUpdateMillisec << " StartUpdateTime " << market.StartUpdateTime << " EndUpdateMillisec " << market.EndUpdateMillisec << " EndUpdateTime " << market.EndUpdateTime
                << "Open" << market.Open << " Close " << market.Close << " Low " << market.Low << " Volume " << market.Volume);
        }
        else
        {
            on_market_bar_data(&(tickKLine.begin()->second));
            tickKLine.clear();
            tickKLine[nStartTime] = market;
            KF_LOG_INFO(logger, "doKlineData: StartUpdateMillisec " << market.StartUpdateMillisec << " StartUpdateTime " << market.StartUpdateTime << " EndUpdateMillisec " << market.EndUpdateMillisec << " EndUpdateTime " << market.EndUpdateTime
                << "Open" << market.Open << " Close " << market.Close << " Low " << market.Low << " Volume " << market.Volume);
        }
    }
    catch (const std::exception& e)
    {
        KF_LOG_INFO(logger, "doKlineData,{error:" << e.what() << "}");
    }
    KF_LOG_DEBUG(logger, "doKlineData end");
}

//此函数一月30日计，一年365日计，未必与火币一致
//1min, 5min, 15min, 30min, 60min, 4hour, 1day, 1mon, 1week, 1year
int64_t MDEngineHbdm::GetMillsecondByInterval(const std::string& interval)
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

void MDEngineHbdm::doDepthData(const rapidjson::Document& json, const std::string& instrument)
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
            on_price_book_update(&priceBook);
        }
    }
    catch (const std::exception& e)
    {
        KF_LOG_INFO(logger, "doDepthData,{error:"<< e.what()<<"}");
    }
     KF_LOG_DEBUG(logger, "doDepthData end");
 }

 void MDEngineHbdm::doTradeData(const rapidjson::Document& json, const std::string& instrument)
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
         on_trade(&trade);
     }
     catch (const std::exception& e)
     {
         KF_LOG_INFO(logger, "doTradeData,{error:"<< e.what()<<"}");
     }
     KF_LOG_DEBUG(logger, "doTradeData end");
 }

bool MDEngineHbdm::parseAddress(const std::string& exch_url)
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
    std::cout<<"reason="<<reason;
    switch( reason )
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            lws_callback_on_writable( conn );
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {
            if(MDEngineHbdm::m_instance)
            {
                MDEngineHbdm::m_instance->onMessage(conn, (char*)data, len);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {
            if(MDEngineHbdm::m_instance)
            {
                MDEngineHbdm::m_instance->onWrite(conn);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_CLOSED:
        case LWS_CALLBACK_CLOSED:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
            if(MDEngineHbdm::m_instance)
            {
                MDEngineHbdm::m_instance->onClose(conn);
            }
            break;
        }
        default:
            break;
    }
    return 0;
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