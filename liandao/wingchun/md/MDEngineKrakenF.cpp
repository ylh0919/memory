#include "MDEngineKrakenF.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/LFDataStruct.h"

#include <writer.h>
#include <stringbuffer.h>
#include <document.h>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <time.h>
#include <string>
#include <cpr/cpr.h>
#include <chrono>
#include <mutex>

using cpr::Get;
using cpr::Url;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;

using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using std::string;
using std::to_string;
using std::stod;
using std::stoi;

USING_WC_NAMESPACE

std::mutex book_mutex;
std::mutex trade_mutex;

static MDEngineKrakenF* global_md = nullptr;

static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{

    switch( reason )
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            lws_callback_on_writable( wsi );
            break;
        }
        case LWS_CALLBACK_PROTOCOL_INIT:
        {
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
        case LWS_CALLBACK_CLIENT_CLOSED:
        {
            if(global_md) {
                std::cout << "LWS_CALLBACK_CLIENT_CLOSED 2,  (call on_lws_connection_error)  reason = " << reason << std::endl;
                global_md->on_lws_connection_error(wsi);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE_PONG:
        {
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {
            if(global_md)
            {
                global_md->lws_write_subscribe(wsi);
            }
            break;
        }
	    case LWS_CALLBACK_TIMER:
        {
            break;
	    }
        case LWS_CALLBACK_CLOSED:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
            std::cout << "LWS_CALLBACK_CLOSED/LWS_CALLBACK_CLIENT_CONNECTION_ERROR writeable, reason = " << reason << std::endl;
            if(global_md)
            {
                global_md->on_lws_connection_error(wsi);
            }
            break;
        }
        default:
            break;
    }

    return 0;
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

MDEngineKrakenF::MDEngineKrakenF(): IMDEngine(SOURCE_KRAKENF)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.KrakenF");
    timer = getTimestamp();
}

void MDEngineKrakenF::load(const json& j_config)
{
    baseUrl = j_config["baseUrl"].get<string>();
    book_depth_count = j_config["book_depth_count"].get<int>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    if(j_config.find("refresh_normal_check_trade_s") != j_config.end()) {
        refresh_normal_check_trade_s = j_config["refresh_normal_check_trade_s"].get<int>();
    }

    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineKrakenF::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"pi_bchusd\": \"pi_bchusd\",");
        KF_LOG_ERROR(logger, "     \"pv_xrpxbt\": \"pv_xrpxbt\"");
        KF_LOG_ERROR(logger, "},");
    }

    makeWebsocketSubscribeJsonString();
    debug_print(websocketSubscribeJsonString);

    priceBook20Assembler.SetLevel(book_depth_count);

    KF_LOG_INFO(logger, "MDEngineKrakenF::load:  book_depth_count: "
            << book_depth_count <<" baseUrl: " << baseUrl << "level_threshold: "<<
             level_threshold << "refresh_normal_check_book_s: "<<refresh_normal_check_book_s);

    int64_t nowTime = getTimestamp();
    std::unordered_map<std::string, std::string>::iterator it;
    for(it = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();it != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end();it ++)
    {
        std::unique_lock<std::mutex> lck(trade_mutex);
        control_trade_map.insert(make_pair(it->first, nowTime));
        lck.unlock();

        std::unique_lock<std::mutex> lck1(book_mutex);
        control_book_map.insert(make_pair(it->first, nowTime));
        lck1.unlock();
    }
}

void MDEngineKrakenF::makeWebsocketSubscribeJsonString()
{
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    if(map_itr == coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()){
        return;
    }
    map_itr++;
    while(map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) "<< map_itr->second);

        std::string jsonBookString = createBookJsonString(map_itr->second);
        websocketSubscribeJsonString.push_back(jsonBookString);

        std::string jsonTradeString = createTradeJsonString(map_itr->second);
        websocketSubscribeJsonString.push_back(jsonTradeString);

        map_itr++;
    }
}

void MDEngineKrakenF::debug_print(std::vector<std::string> &subJsonString)
{
    size_t count = subJsonString.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
    }
}

void MDEngineKrakenF::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineKrakenF::connect:");
    connected = true;
}

void MDEngineKrakenF::login(long timeout_nsec) {
    KF_LOG_INFO(logger, "MDEngineKrakenF::login:");
    global_md = this;

    struct lws_context_creation_info info;
    memset( &info, 0, sizeof(info) );

    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.iface = NULL;
    info.ssl_cert_filepath = NULL;
    info.ssl_private_key_filepath = NULL;
    info.extensions = NULL;
    info.gid = -1;
    info.uid = -1;
    info.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    info.max_http_header_pool = 1024;
    info.fd_limit_per_thread = 1024;
    info.ws_ping_pong_interval = 10;
    info.ka_time = 10;
    info.ka_probes = 10;
    info.ka_interval = 10;

    context = lws_create_context( &info );
    KF_LOG_INFO(logger, "MDEngineKrakenF::login: context created.");

    if (context == NULL) {
        KF_LOG_ERROR(logger, "MDEngineKrakenF::login: context is NULL. return");
        return;
    }

    struct lws_client_connect_info ccinfo;
    memset(&ccinfo, 0, sizeof(ccinfo));
    struct lws *wsi = NULL;
    std::string host = baseUrl;
    std::string path = "/ws/v1/";

    ccinfo.context 	= context;
    ccinfo.address 	= host.c_str();
    ccinfo.port 	= 443;
    ccinfo.path 	= path.c_str();
    ccinfo.host 	= host.c_str();
    ccinfo.origin 	= host.c_str();
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    ccinfo.ietf_version_or_minus_one = -1;

    wsi = lws_client_connect_via_info(&ccinfo);
    KF_LOG_INFO(logger, "MDEngineKrakenF::login: Connecting to " <<  ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "MDEngineKrakenF::login: wsi create error.");
        return;
    }
    KF_LOG_INFO(logger, "MDEngineKrakenF::login: test login #7 " );
    KF_LOG_INFO(logger, "MDEngineKrakenF::login: wsi create success.");

    logged_in = true;
}

void MDEngineKrakenF::set_reader_thread()
{
    IMDEngine::set_reader_thread();

    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKrakenF::loop, this)));
}

void MDEngineKrakenF::logout()
{
    KF_LOG_INFO(logger, "MDEngineKrakenF::logout:");
}

void MDEngineKrakenF::release_api()
{
    KF_LOG_INFO(logger, "MDEngineKrakenF::release_api:");
}

void MDEngineKrakenF::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEngineKrakenF::subscribeMarketData:");
}

int MDEngineKrakenF::lws_write_subscribe(struct lws* conn)
{
    //KF_LOG_INFO(logger, "MDEngineKrakenF::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    //有待发送的数据，先把待发送的发完，在继续订阅逻辑。  ping?
    if(websocketPendingSendMsg.size() > 0) {
        unsigned char msg[512];
        memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

        std::string jsonString = websocketPendingSendMsg[websocketPendingSendMsg.size() - 1];
        websocketPendingSendMsg.pop_back();
        KF_LOG_INFO(logger, "MDEngineKrakenF::lws_write_subscribe: websocketPendingSendMsg" << jsonString.c_str());
        int length = jsonString.length();

        strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
        int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

        if(websocketPendingSendMsg.size() > 0)
        {    //still has pending send data, emit a lws_callback_on_writable()
            lws_callback_on_writable( conn );
            KF_LOG_INFO(logger, "MDEngineKrakenF::lws_write_subscribe: (websocketPendingSendMsg,size)" << websocketPendingSendMsg.size());
        }
        return ret;
    }

    if(websocketSubscribeJsonString.size() == 0) return 0;
    //sub depth
    if(subscribe_index >= websocketSubscribeJsonString.size())
    {
        //subscribe_index = 0;
        //KF_LOG_INFO(logger, "MDEngineKrakenF::lws_write_subscribe: (none reset subscribe_index = 0, just return 0)");
	    return 0;
    }

    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

    std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

    KF_LOG_INFO(logger, "MDEngineKrakenF::lws_write_subscribe: " << jsonString.c_str() << " ,after ++, (subscribe_index)" << subscribe_index);
    int length = jsonString.length();

    strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

    if(subscribe_index < websocketSubscribeJsonString.size())
    {
        lws_callback_on_writable( conn );
        KF_LOG_INFO(logger, "MDEngineKrakenF::lws_write_subscribe: (subscribe_index < websocketSubscribeJsonString) call lws_callback_on_writable");
    }

    return ret;
}

void MDEngineKrakenF::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "MDEngineKrakenF::on_lws_data: " << data);
    Document json;
    json.Parse(data);

    if(json.HasParseError()) {
        KF_LOG_ERROR(logger, "MDEngineKrakenF::on_lws_data. parse json error: " << data);
        return;
    }

    if(json.IsObject() && json.HasMember("feed") && !json.HasMember("event"))
    {
        if(strcmp(json["feed"].GetString(),"book_snapshot")==0 || strcmp(json["feed"].GetString(),"book")==0)
        {
            KF_LOG_INFO(logger, "MDEngineKrakenF::on_lws_data: is book");
            onBook(json);
        }
        else if(strcmp(json["feed"].GetString(),"trade")==0)
        {
            KF_LOG_INFO(logger, "MDEngineKrakenF::on_lws_data: is trade");
            onTrade(json);
        }
        else
        {
            KF_LOG_INFO(logger, "MDEngineKrakenF::on_lws_data: unknown array data: " << data);
        }
    }
    else if(json.IsObject() && json.HasMember("event"))
    {
        if(strcmp(json["event"].GetString(),"error")==0)
        {
             KF_LOG_ERROR(logger, "MDEngineKrakenF::on_lws_data: subscribe data error,errorMsg is "
              << json["message"].GetString());
        }
    }
}

void MDEngineKrakenF::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "MDEngineKrakenF::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineKrakenF::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;

    login(timeout_nsec);
}


int64_t MDEngineKrakenF::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void MDEngineKrakenF::onTrade(Document& json)
{
    KF_LOG_INFO(logger, "MDEngineKrakenF::onTrade: trade " << parseJsonToString(json));

    std::string ticker = coinPairWhiteList.GetKeyByValue(json["product_id"].GetString());
    if(ticker.length() == 0) {
        KF_LOG_INFO(logger, "MDEngineKrakenF::onTrade: ticker is empty,please check the whitelist");
        return;
    }
    KF_LOG_INFO(logger, "MDEngineKrakenF::onTrade: ticker" << ticker);

	LFL2TradeField trade;
	memset(&trade, 0, sizeof(trade));
	strcpy(trade.InstrumentID, ticker.c_str());
	strcpy(trade.ExchangeID, "Kraken Futures");

	trade.Price = std::round(json["price"].GetDouble() * scale_offset);
	trade.Volume = std::round(json["qty"].GetDouble() * scale_offset);
    strcpy(trade.Sequence,to_string(json["seq"].GetInt()).c_str());
    std::string uid = json["uid"].GetString();
    strcpy(trade.TradeID,uid.c_str());

    time_t t;
    struct tm *p;
    t = (time_t)(json["time"].GetInt64() / 1000);
    int t_ms = json["time"].GetInt64() - (json["time"].GetInt64() / 1000)*1000;
    p=gmtime(&t);
    char s[80];
    strftime(s, 80, "%Y-%m-%dT%H:%M:%S", p);
    sprintf(s,"%s.%sZ",s,to_string(t_ms).c_str());
    strcpy(trade.TradeTime,s);

	if(strcmp(json["side"].GetString(), "buy") == 0)
    {
		trade.OrderBSFlag[0] = 'B';
	}
	else if(strcmp(json["side"].GetString(), "sell") == 0)
	{
		trade.OrderBSFlag[0] = 'S';
	}

    std::unique_lock<std::mutex> lck(trade_mutex);
    auto it = control_trade_map.find(ticker);
    if(it != control_trade_map.end())
    {
        it->second = getTimestamp();
    }
    lck.unlock();

	KF_LOG_INFO(logger, "MDEngineKrakenF::[onTrade] (InstrumentID)"<< trade.InstrumentID
        <<" (ExchangeID)"<<trade.ExchangeID <<" (Price)" << trade.Price <<" (Volume)" << trade.Volume
        <<" (Sequence)"<<trade.Sequence<<" (TradeID)"<<trade.TradeID<<" (TradeTime)"<<trade.TradeTime <<" (OrderBSFlag)" <<trade.OrderBSFlag[0]);
    KF_LOG_INFO(logger, "MDEngineKrakenF::onTrade: on_trade success");
	on_trade(&trade);
}

void MDEngineKrakenF::onBook(Document& json)
{
    KF_LOG_INFO(logger, "MDEngineKrakenF::onBook: (coinpair) " << json["product_id"].GetString());

    std::string ticker = coinPairWhiteList.GetKeyByValue(json["product_id"].GetString());
    if(ticker.length() == 0) {
        KF_LOG_INFO(logger, "MDEngineKrakenF::onBook: ticker is empty,please check the whitelist");
        return;
    }

    KF_LOG_INFO(logger, "MDEngineKrakenF::onBook: (ticker) " << ticker);

    if(strcmp(json["feed"].GetString(),"book_snapshot")==0 && json["bids"].IsArray() && json["asks"].IsArray())
    {
        auto bids = json["bids"].GetArray();
        auto asks = json["asks"].GetArray();
        int lenb = bids.Size();
        int lena = asks.Size();
        priceBook20Assembler.clearPriceBook(ticker);
        for(int i = 0;i < lena;i++)
        {
             double price = std::round(asks[i]["price"].GetDouble() * scale_offset);
             double volume = std::round(asks[i]["qty"].GetDouble() * scale_offset);
             KF_LOG_INFO(logger, "MDEngineKrakenF::onBook(book_snapshot_asks): (ticker)"<<ticker<<" (price)" << price<<" (volume)" << volume);
             priceBook20Assembler.UpdateAskPrice(ticker, price, volume);            
        }
        for(int i = 0; i < lenb;i++)
         {
             double price = std::round(bids[i]["price"].GetDouble() * scale_offset);
             double volume = std::round(bids[i]["qty"].GetDouble() * scale_offset);
             KF_LOG_INFO(logger, "MDEngineKrakenF::onBook(book_snapshot_bids): (ticker)"<<ticker<<" (price)" << price<<" (volume)" << volume);
             priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
         }
    }
    else if(strcmp(json["feed"].GetString(),"book")==0)
    {
        if(strcmp(json["side"].GetString(),"buy")==0)
        {
            double price = std::round(json["price"].GetDouble() * scale_offset);
            double volume = std::round(json["qty"].GetDouble() * scale_offset);
            if (volume == 0)
            {
             
                KF_LOG_INFO(logger, "MDEngineKrakenF::onBook(erase_bid_price): (ticker)"<< ticker<<" (price)" << price<<" (volume)" << volume);
                priceBook20Assembler.EraseBidPrice(ticker, price);
            }
            else
            {
                KF_LOG_INFO(logger, "MDEngineKrakenF::onBook(update_bid_price): (ticker)"<< ticker<<" (price)" << price<<" (volume)" << volume);
                priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
            }           

        }
        else if(strcmp(json["side"].GetString(),"sell")==0)
        {
            double price = std::round(json["price"].GetDouble() * scale_offset);
            double volume = std::round(json["qty"].GetDouble() * scale_offset);
            if (volume == 0)
            {
             
                KF_LOG_INFO(logger, "MDEngineKrakenF::onBook(erase_ask_price): (ticker)"<< ticker<<" (price)" << price<<" (volume)" << volume);
                priceBook20Assembler.EraseAskPrice(ticker, price);
            }
            else
            {
                KF_LOG_INFO(logger, "MDEngineKrakenF::onBook(update_ask_price): (ticker)"<< ticker<<" (price)" << price<<" (volume)" << volume);
                priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
            }           

        }
    }

    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));
    if(priceBook20Assembler.Assembler(ticker, md)) 
    {     
        strcpy(md.ExchangeID, "Kraken Futures");
        priceBook[ticker] = md;
        //timer = getTimestamp();
        std::unique_lock<std::mutex> lck1(book_mutex);
        auto it = control_book_map.find(ticker);
        if(it != control_book_map.end())
        {
            it->second = getTimestamp();
        }
        lck1.unlock();

        if(md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold)
        {
            KF_LOG_INFO(logger, "MDEngineKrakenF::onBook: failed,level count < level threshold :"<<md.BidLevelCount<<" "<<md.AskLevelCount<<" "<<level_threshold);
            string errorMsg = "orderbook level below threshold";
            write_errormsg(112,errorMsg);
            on_price_book_update(&md);
        }
        else if(md.BidLevels[0].price <=0 || md.AskLevels[0].price <=0 || md.BidLevels[0].price > md.AskLevels[0].price)
        {
            KF_LOG_INFO(logger, "MDEngineKrakenF::onBook: orderbook crossed");
            string errorMsg = "orderbook crossed";
            write_errormsg(113,errorMsg);
        }
        else
        {
            KF_LOG_INFO(logger, "MDEngineKrakenF::onBook: on_price_book_update success");
            on_price_book_update(&md);
        }
    }

}

std::string MDEngineKrakenF::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


/*
{  
    "event":"subscribe ",
    "feed":"book",
    "product_ids":[  
        "FI_XBTUSD_180921"
    ]
}
*/
std::string MDEngineKrakenF::createBookJsonString(std::string exchange_coinpair)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();

    writer.Key("event");
    writer.String("subscribe");

    writer.Key("feed");
    writer.String("book");

    writer.Key("product_ids");
    writer.StartArray();
    writer.String(exchange_coinpair.c_str());
    writer.EndArray();

    writer.EndObject();
    return s.GetString();
}

/*
{  
    "event":"subscribe ",
    "feed":"trade",
    "product_ids":[  
        "FI_XBTUSD_180921"
    ]
}
*/
std::string MDEngineKrakenF::createTradeJsonString(std::string exchange_coinpair)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();

    writer.Key("event");
    writer.String("subscribe");

    writer.Key("feed");
    writer.String("trade");

    writer.Key("product_ids");
    writer.StartArray();
    writer.String(exchange_coinpair.c_str());
    writer.EndArray();

    writer.EndObject();
    return s.GetString();
}

void MDEngineKrakenF::loop()
{
    while(isRunning)
    {
        int64_t now = getTimestamp();
        int errorId = 0;
        std::string errorMsg = "";

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

        int n = lws_service( context, rest_get_interval_ms );
    }
}

BOOST_PYTHON_MODULE(libkrakenfmd)
{
    using namespace boost::python;
    class_<MDEngineKrakenF, boost::shared_ptr<MDEngineKrakenF> >("Engine")
            .def(init<>())
            .def("init", &MDEngineKrakenF::initialize)
            .def("start", &MDEngineKrakenF::start)
            .def("stop", &MDEngineKrakenF::stop)
            .def("logout", &MDEngineKrakenF::logout)
            .def("wait_for_stop", &MDEngineKrakenF::wait_for_stop);
}
