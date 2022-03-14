#include "MDEngineUpbit.h"
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
#include <string>
#include <cpr/cpr.h>
#include <chrono>

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
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;
std::mutex ping_mutex;

std::mutex book_mutex;
std::mutex trade_mutex;

static MDEngineUpbit* global_md = nullptr;

static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{

    switch( reason )
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            std::cout << "3.1415926 LWS_CALLBACK_CLIENT_ESTABLISHED callback client established, reason = " << reason << std::endl;
            lws_callback_on_writable( wsi );
            break;
        }
        case LWS_CALLBACK_PROTOCOL_INIT:
        {
            std::cout << "3.1415926 LWS_CALLBACK_PROTOCOL_INIT init, reason = " << reason << std::endl;
            break;
        }
        case LWS_CALLBACK_RECEIVE_PONG:
        {
            std::cout << "3.1415926 LWS_CALLBACK_RECEIVE_PONG init, reason = " << reason << std::endl;
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {
            std::cout << "3.1415926 LWS_CALLBACK_CLIENT_RECEIVE on data, reason = " << reason << std::endl;
            if(global_md)
            {
                global_md->on_lws_data(wsi, (const char*)in, len);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_CLOSED:
        {
            std::cout << "3.1415926 LWS_CALLBACK_CLIENT_CLOSED, reason = " << reason << std::endl;
            if(global_md) {
                std::cout << "3.1415926 LWS_CALLBACK_CLIENT_CLOSED 2,  (call on_lws_connection_error)  reason = " << reason << std::endl;
                global_md->on_lws_connection_error(wsi);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE_PONG:
        {
            std::cout << "3.1415926 LWS_CALLBACK_CLIENT_RECEIVE_PONG, reason = " << reason << std::endl;
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {
            std::cout << "3.1415926 LWS_CALLBACK_CLIENT_WRITEABLE writeable, reason = " << reason << std::endl;
            if(global_md)
            {
                global_md->lws_write_subscribe(wsi);
            }
            break;
        }
        case LWS_CALLBACK_TIMER:
        {
            std::cout << "3.1415926 LWS_CALLBACK_TIMER, reason = " << reason << std::endl;
            break;
        }
        case LWS_CALLBACK_CLOSED:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
            std::cout << "3.1415926 LWS_CALLBACK_CLOSED/LWS_CALLBACK_CLIENT_CONNECTION_ERROR writeable, reason = " << reason << std::endl;
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

enum protocolList 
{
    PROTOCOL_TEST,

    PROTOCOL_LIST_COUNT
};

struct session_data 
{
    int fd;
};

MDEngineUpbit::MDEngineUpbit(): IMDEngine(SOURCE_UPBIT)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Upbit");
    timer = getTimestamp();/*edited by zyy*/
}

void MDEngineUpbit::load(const json& j_config)
{
    book_depth_count = j_config["book_depth_count"].get<int>();
    trade_count = j_config["trade_count"].get<int>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    if(j_config.find("snapshot_check_s") != j_config.end()) {
        snapshot_check_s = j_config["snapshot_check_s"].get<int>();
    }
    /*edited by zyy,starts here*/
    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    if(j_config.find("refresh_normal_check_trade_s") != j_config.end()) {
        refresh_normal_check_trade_s = j_config["refresh_normal_check_trade_s"].get<int>();
    }
    /*edited by zyy ends here*/

    //get kline from rest
    if (j_config.find("need_get_kline_via_rest") != j_config.end())
        need_get_kline_via_rest = j_config["need_get_kline_via_rest"].get<bool>();
    if (need_get_kline_via_rest) {
        if (j_config.find("kline_a_interval_s") != j_config.end())
            kline_a_interval_min = j_config["kline_a_interval_s"].get<int>() / 60;
        if (j_config.find("kline_b_interval_s") != j_config.end())
            kline_b_interval_min = j_config["kline_b_interval_s"].get<int>() / 60;
        if (j_config.find("get_kline_wait_ms") != j_config.end())
            get_kline_wait_ms = j_config["get_kline_wait_ms"].get<int>();
        if (j_config.find("get_kline_via_rest_count") != j_config.end())
            get_kline_via_rest_count = j_config["get_kline_via_rest_count"].get<int>();
        KF_LOG_INFO(logger, "MDEngineBinanceF::load (need_get_kline_via_rest)" << need_get_kline_via_rest <<
            " (get_kline_wait_ms)"          << get_kline_wait_ms          <<
            " (kline_a_interval_min)"       << kline_a_interval_min       <<
            " (kline_b_interval_min)"       << kline_b_interval_min       <<
            " (get_kline_via_rest_count)"   << get_kline_via_rest_count
        );
    }

    priceBook20Assembler.SetLevel(book_depth_count);
    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    //getBaseQuoteFromWhiteListStrategyCoinPair();
    
    makeWebsocketSubscribeJsonString();
    debug_print(websocketSubscribeJsonString);

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineUpbit::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }

    int64_t nowTime = getTimestamp();
    std::unordered_map<std::string, std::string>::iterator it;
    for(it = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); it != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); it++)
    {
        std::unique_lock<std::mutex> lck(book_mutex);
        control_book_map.insert(make_pair(it->first, nowTime));
        lck.unlock();

        std::unique_lock<std::mutex> lck1(trade_mutex);
        control_trade_map.insert(make_pair(it->first, nowTime));
        lck1.unlock();
    }
}

void MDEngineUpbit::getBaseQuoteFromWhiteListStrategyCoinPair()
{
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    while(map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end())
    {
        std::cout << "[getBaseQuoteFromWhiteListStrategyCoinPair] keyIsExchangeSideWhiteList (strategy_coinpair) "
                  << map_itr->first << " (exchange_coinpair) "<< map_itr->second << std::endl;

        // strategy_coinpair 转换成大写字母
        std::string coinpair = map_itr->first;
        std::transform(coinpair.begin(), coinpair.end(), coinpair.begin(), ::toupper);

        CoinBaseQuote baseQuote;
        split(coinpair, "_", baseQuote);
        std::cout << "[readWhiteLists] getBaseQuoteFromWhiteListStrategyCoinPair (base) " << baseQuote.base << " (quote) " << baseQuote.quote << std::endl;

        if(baseQuote.base.length() > 0)
        {
            //get correct base_quote config
            coinBaseQuotes.push_back(baseQuote);
        }
        map_itr++;
    }
}

//example: btc_usdt
void MDEngineUpbit::split(std::string str, std::string token, CoinBaseQuote& sub)
{
    if (str.size() > 0)
    {
        size_t index = str.find(token);
        if (index != std::string::npos)
        {
            sub.base = str.substr(0, index);
            sub.quote = str.substr(index + token.size());
        }
        else {
            //not found, do nothing
        }
    }
}

void MDEngineUpbit::makeWebsocketSubscribeJsonString()
{
    std::vector<std::string> vecPairs;
    auto map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    while(map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end())
    {
        vecPairs.push_back(map_itr->second);
        map_itr++;
    }
    //get ready websocket subscrube json strings
    std::string jsonOrderBookString = createOrderBookJsonString(vecPairs);
    websocketSubscribeJsonString.push_back(jsonOrderBookString);
    //std::string jsonTradeString = createTradeJsonString(vecPairs);
    //websocketSubscribeJsonString.push_back(jsonTradeString);
}

void MDEngineUpbit::debug_print(std::vector<std::string> &subJsonString)
{
    size_t count = subJsonString.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
    }
}

void MDEngineUpbit::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineUpbit::connect:");
    connected = true;
}

void MDEngineUpbit::login(long timeout_nsec) {
    KF_LOG_INFO(logger, "MDEngineUpbit::login:");
    global_md = this;

    char inputURL[300] = "wss://api.upbit.com/websocket/v1";

    const char *urlProtocol, *urlTempPath;
    char urlPath[300];
    //int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
    struct lws_client_connect_info clientConnectInfo;
    memset(&clientConnectInfo, 0, sizeof(clientConnectInfo));
    clientConnectInfo.port = 443;
    if (lws_parse_uri(inputURL, &urlProtocol, &clientConnectInfo.address, &clientConnectInfo.port, &urlTempPath)) {
        KF_LOG_ERROR(logger,
                     "MDEngineUpbit::connect: Couldn't parse URL. Please check the URL and retry: " << inputURL);
        return;
    }

    // Fix up the urlPath by adding a / at the beginning, copy the temp path, and add a \0     at the end
    urlPath[0] = '/';
    strncpy(urlPath + 1, urlTempPath, sizeof(urlPath) - 2);
    urlPath[sizeof(urlPath) - 1] = '\0';
    clientConnectInfo.path = urlPath; // Set the info's path to the fixed up url path

    KF_LOG_INFO(logger, "MDEngineUpbit::login:" << "urlProtocol=" << urlProtocol <<
                                                  "address=" << clientConnectInfo.address <<
                                                  "urlTempPath=" << urlTempPath <<
                                                  "urlPath=" << urlPath);
    if (context == NULL) {
        struct lws_protocols protocol;
        protocol.name = protocols[PROTOCOL_TEST].name;
        protocol.callback = &ws_service_cb;
        protocol.per_session_data_size = sizeof(struct session_data);
        protocol.rx_buffer_size = 0;
        protocol.id = 0;
        protocol.user = NULL;

        struct lws_context_creation_info ctxCreationInfo;
        memset(&ctxCreationInfo, 0, sizeof(ctxCreationInfo));
        ctxCreationInfo.port = CONTEXT_PORT_NO_LISTEN;
        ctxCreationInfo.iface = NULL;
        ctxCreationInfo.protocols = &protocol;
        ctxCreationInfo.ssl_cert_filepath = NULL;
        ctxCreationInfo.ssl_private_key_filepath = NULL;
        ctxCreationInfo.extensions = NULL;
        ctxCreationInfo.gid = -1;
        ctxCreationInfo.uid = -1;
        ctxCreationInfo.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
        ctxCreationInfo.fd_limit_per_thread = 1024;
        ctxCreationInfo.max_http_header_pool = 1024;
        ctxCreationInfo.ws_ping_pong_interval = 1;
        ctxCreationInfo.ka_time = 10;
        ctxCreationInfo.ka_probes = 10;
        ctxCreationInfo.ka_interval = 10;

        context = lws_create_context(&ctxCreationInfo);
        KF_LOG_INFO(logger, "MDEngineUpbit::login: context created.");
    }

    if (context == NULL) {
        KF_LOG_ERROR(logger, "MDEngineUpbit::login: context is NULL. return");
        return;
    }

    struct lws *wsi = NULL;
    // Set up the client creation info
    clientConnectInfo.context = context;
    clientConnectInfo.port = 443;
    clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    clientConnectInfo.host = clientConnectInfo.address;
    clientConnectInfo.origin = clientConnectInfo.address;
    clientConnectInfo.ietf_version_or_minus_one = -1;
    clientConnectInfo.protocol = protocols[PROTOCOL_TEST].name;

    KF_LOG_INFO(logger, "MDEngineUpbit::login:" << "Connecting to " << urlProtocol << ":" <<
                                                  clientConnectInfo.host << ":" <<
                                                  clientConnectInfo.port << ":" << urlPath);

    wsi = lws_client_connect_via_info(&clientConnectInfo);
    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "MDEngineUpbit::login: wsi create error.");
        return;
    }
    ws_wsi = wsi;
    ping_time = getTimestamp();
    KF_LOG_INFO(logger,"ws_wsi="<<ws_wsi<<" "<<wsi);
    KF_LOG_INFO(logger, "MDEngineUpbit::login: wsi create success.");
    logged_in = true;
}

void MDEngineUpbit::set_reader_thread()
{
    IMDEngine::set_reader_thread();
    onKline_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineUpbit::onKline_loop, this)));
    ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineUpbit::loop, this)));
    //rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineUpbit::rest_loop, this)));
    //check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineUpbit::check_loop, this)));
    if(need_get_kline_via_rest)
        get_kline_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineUpbit::get_kline_loop, this)));
}

void MDEngineUpbit::logout()
{
    KF_LOG_INFO(logger, "MDEngineUpbit::logout:");
}

void MDEngineUpbit::release_api()
{
    KF_LOG_INFO(logger, "MDEngineUpbit::release_api:");
}

void MDEngineUpbit::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEngineUpbit::subscribeMarketData:");
}

int MDEngineUpbit::lws_write_subscribe(struct lws* conn)
{
    KF_LOG_INFO(logger, "MDEngineUpbit::lws_write_subscribe: (subscribe_index)" << subscribe_index);
    int ret = 0;
    /*if (websocketSubscribeJsonString.size() == 0) return 0;
    //sub depth
    if (subscribe_index >= websocketSubscribeJsonString.size())
    {
        return 0;
    }

    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512 - LWS_PRE);

    std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

    KF_LOG_INFO(logger, "MDEngineUpbit::lws_write_subscribe: " << jsonString.c_str());
    int length = jsonString.length();

    strncpy((char *)msg + LWS_PRE, jsonString.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);

    if (subscribe_index < websocketSubscribeJsonString.size())
    {
        lws_callback_on_writable(conn);
    }*/
    std::unique_lock<std::mutex> lck_ping(ping_mutex);
    if ((websocketSubscribeJsonString.empty() || subscribe_index == -1) && ping_queue.empty())
    {
        KF_LOG_INFO(logger, "subcribe ignore");
        return ret;
    }
    if(ping_queue.size() > 0){
        int length = ping_queue.front();
        ping_queue.pop();
        KF_LOG_INFO(logger, "send_ping");
        unsigned char pingbuf[LWS_PRE + length];
        ret = lws_write(conn, pingbuf + LWS_PRE, length, LWS_WRITE_PING);
    }
    else if(websocketSubscribeJsonString.size() > 0){
        unsigned char msg[512];
        memset(&msg[LWS_PRE], 0, 512 - LWS_PRE);
        std::string jsonString = websocketSubscribeJsonString[subscribe_index++];
        KF_LOG_INFO(logger, "MDEngineUpbit::lws_write_subscribe: " << jsonString.c_str());
        int length = jsonString.length();

        strncpy((char *)msg + LWS_PRE, jsonString.c_str(), length);
        ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);     
    }
    lck_ping.unlock();

    if(subscribe_index >= websocketSubscribeJsonString.size())
    {
        subscribe_index = -1;
        KF_LOG_INFO(logger, "subcribe end");
        return ret;
    }
    lws_callback_on_writable(conn);

    return ret;
}

void MDEngineUpbit::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "MDEngineUpbit::on_lws_data: " << data);
    Document json;
    json.Parse(data);
    
    if (json.HasParseError()) {
        KF_LOG_ERROR(logger, "MDEngineUpbit::on_lws_data  parse json error: " << data);
        return;
    }
    if (json.HasMember("type")) {
        if (strcmp(json["type"].GetString(), "orderbook") == 0) {
            KF_LOG_INFO(logger, "MDEngineUpbit::on_lws_data:onBook");
            onBook(json);
        }
        else if (strcmp(json["type"].GetString(), "trade") == 0) {
            KF_LOG_INFO(logger, "MDEngineUpbit::on_lws_data:onTrade");
            onTrade(json);
        }
        else {
            KF_LOG_INFO(logger, "MDEngineUpbit::on_lws_data: unknown type: " << data);
        };
    }

}

void MDEngineUpbit::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "MDEngineUpbit::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineUpbit::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;

    login(timeout_nsec);
}

/*
{
    "type":"orderbook",
    "code":"BTC-BAY",
    "timestamp":1565766379878,
    "total_ask_size":78438.9853932,
    "total_bid_size":8932732.3274329,
    "orderbook_units":
    [
        {
        "ask_price":2.1E-7,
        "bid_price":2.0E-7,
        "ask_size":32432.3243252,
        "bid_size":3243.54324
        },
        {}
    ],
    "stream_type":"SNAPSHOT/REALTIME"
}
*/
void MDEngineUpbit::onBook(Document& json)
{
    //PriceBook20Assembler priceBook20Assembler;
    KF_LOG_INFO(logger, "MDEngineUpbit::onBook: " << parseJsonToString(json));
    std::string coinpair; int64_t ask_price,bid_price; uint64_t ask_volume,bid_volume;
    if (json.HasMember("code")) {
        coinpair = coinPairWhiteList.GetKeyByValue(json["code"].GetString());
        KF_LOG_INFO(logger, "MDEngineUpbit::onOrderBook: (symbol) " << json["code"].GetString());
        KF_LOG_INFO(logger, "MDEngineUpbit::onOrderBook: (coinpair) " << coinpair);
    }
    if (coinpair.length() == 0)
    {
        KF_LOG_ERROR(logger, "MDEngineUpbit::onOrderBook:get coinpair error");
        return;
    }
    uint64_t sequence;
    if (json.HasMember("orderbook_units") && json["orderbook_units"].IsArray()) {
        /*std::string stream_type = json["stream_type"].GetString();
        if(stream_type == "SNAPSHOT"){
            KF_LOG_INFO(logger,"clearPriceBook");
            priceBook20Assembler.clearPriceBook(coinpair);
        }*/
        priceBook20Assembler.clearPriceBook(coinpair);
        //orderbook update
        int length = json["orderbook_units"].Size();
        auto &orderbook = json["orderbook_units"];

        sequence = json["timestamp"].GetInt64();
        
        for (int i = 0; i < length; i++) {
            ask_price = std::round(orderbook.GetArray()[i]["ask_price"].GetDouble()* scale_offset);
            ask_volume = std::round(orderbook.GetArray()[i]["ask_size"].GetDouble() * scale_offset);
            bid_price = std::round(orderbook.GetArray()[i]["bid_price"].GetDouble() * scale_offset);
            bid_volume = std::round(orderbook.GetArray()[i]["bid_size"].GetDouble() * scale_offset);
            
            /*if (ask_volume == 0) {
                priceBook20Assembler.EraseAskPrice(coinpair, ask_price);
                //KF_LOG_INFO(logger, "MDEngineUpbit::onBook:EraseAskPrice():coinpair" << coinpair);
                //KF_LOG_INFO(logger, "MDEngineUpbit::onBook:EraseAskPrice():price" << ask_price);
            }
            else {*/
                priceBook20Assembler.UpdateAskPrice(coinpair, ask_price, ask_volume);
                //KF_LOG_INFO(logger, "MDEngineUpbit::onBook:UpdateAskPrice():coinpair" << coinpair);
                //KF_LOG_INFO(logger, "MDEngineUpbit::onBook:UpdateAskPrice():price" << ask_price);
                //KF_LOG_INFO(logger, "MDEngineUpbit::onBook:UpdateAskPrice():volume" << ask_volume);
            //}
            /*if (bid_volume == 0) {
                priceBook20Assembler.EraseBidPrice(coinpair, bid_price);
                //KF_LOG_INFO(logger, "MDEngineUpbit::onBook:EraseBidPrice():coinpair" << coinpair);
                //KF_LOG_INFO(logger, "MDEngineUpbit::onBook:EraseBidPrice():price" << bid_price);
            }
            else {*/
                priceBook20Assembler.UpdateBidPrice(coinpair, bid_price, bid_volume);
                //KF_LOG_INFO(logger, "MDEngineUpbit::onBook:UpdateBidPrice():coinpair" << coinpair);
                //KF_LOG_INFO(logger, "MDEngineUpbit::onBook:UpdateBidPrice():price" << bid_price);
                //KF_LOG_INFO(logger, "MDEngineUpbit::onBook:UpdateBidPrice():volume" << bid_volume);
            //}
        }
            
    }

    //has any update
    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));
    if (priceBook20Assembler.Assembler(coinpair, md)) {
        strcpy(md.ExchangeID, "Upbit");
        KF_LOG_INFO(logger, "MDEngineUpbit::onBook: onOrderBook_update");
        KF_LOG_INFO(logger,"BidLevelCount="<<md.BidLevelCount<<",AskLevelCount="<<md.AskLevelCount<<",level_threshold="<<level_threshold);
        /*edited by zyy,starts here*/
        //timer = getTimestamp();
        std::unique_lock<std::mutex> lck(book_mutex);
        auto it = control_book_map.find(coinpair);
        if(it != control_book_map.end())
        {
            it->second = getTimestamp();
        } 
        lck.unlock();

        if (md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold)
        {
            md.Status = 2;
            string errorMsg = "orderbook level below threshold";
            write_errormsg(112,errorMsg);
            on_price_book_update(&md);
        }  
        else if (md.BidLevels[0].price <=0 || md.AskLevels[0].price <=0 || md.BidLevels[0].price > md.AskLevels[0].price)
        {
            KF_LOG_INFO(logger,"bid="<<md.BidLevels[0].price<<"ask="<<md.AskLevels[0].price);
            md.Status = 1;
            string errorMsg = "orderbook crossed";
            write_errormsg(113,errorMsg);
        }    
        /*edited by zyy ends here*/
        else
        {
            /*
            int64_t now = getTimestamp();
            std::unique_lock<std::mutex> lck_ws_book(ws_book_mutex);
            auto itr = ws_book_map.find(coinpair);
            if(itr == ws_book_map.end()){
                std::map<uint64_t, int64_t> bookmsg_map;
                bookmsg_map.insert(std::make_pair(sequence, now));
                ws_book_map.insert(std::make_pair(coinpair, bookmsg_map));
            }else{
                itr->second.insert(std::make_pair(sequence, now));
            }
            lck_ws_book.unlock();
            */
            md.Status = 0;
            on_price_book_update(&md);
            //timer = getTimestamp();
            KF_LOG_DEBUG(logger, "MDEngineUpbit onOrderBook_update successed");
        }
    }
    else
    {
        //timer = getTimestamp();
        //GetSnapShotAndRtn(ticker);
        KF_LOG_DEBUG(logger, "MDEngineUpbit onOrderBook_update,priceBook20Assembler.Assembler(coinpair, md) failed\n(coinpair)" << coinpair);
    }
}

/*
{
    "type":"trade",
    "code":"BTC-ION",
    "timestamp":125424442,
    "trade_date":"2019-08-14",
    "trade_time":"06:27:32",
    "trade_timestamp":13243543,
    "trade_price":0.0000043,
    "trade_volume":297,
    "ask_bid":"ASK/BID",
    "prev_colsing_price":0.0000459,
    "change":"RISE",
    "change_price":2.4E-7,
    "sequential_id":156576405290000,
    "stream_type":"SNAPSHOT/REALTIME"
}
*/
void MDEngineUpbit::onTrade(Document& json) {
    //KF_LOG_INFO(logger, "MDEngineUpbit::onTrade: " << parseJsonToString(json));
    int64_t price; uint64_t volume; std::string side; int64_t sequent; std::string coinpair,date,time;
    if (json.HasMember("code")) {
        coinpair = coinPairWhiteList.GetKeyByValue(json["code"].GetString());
        //KF_LOG_INFO(logger, "MDEngineUpbit::onTrade: (symbol) " << json["code"].GetString());
        //KF_LOG_INFO(logger, "MDEngineUpbit::onTrade: (coinpair) " << coinpair);
    }
    if (coinpair.length() == 0)
    {
        KF_LOG_ERROR(logger, "MDEngineUpbit::onTrade:get coinpair error");
        return;
    }
    if (strcmp(json["stream_type"].GetString(), "REALTIME") == 0) {
        //KF_LOG_INFO(logger, "MDEngineUpbit::onTrade:trade_update(coinpair): " << coinpair);
        LFL2TradeField trade;
        memset(&trade, 0, sizeof(trade));
        strcpy(trade.InstrumentID, coinpair.c_str());
        //KF_LOG_INFO(logger, "MDEngineUpbit::onTrade: InstrumentID " << coinpair);

        strcpy(trade.ExchangeID, "Upbit");
        //KF_LOG_INFO(logger, "MDEngineUpbit::onTrade: ExchangeID:Upbit ");

        trade.Price = std::round(json["trade_price"].GetDouble() * scale_offset);
        //KF_LOG_INFO(logger, "MDEngineUpbit::onTrade: price " << price);

        trade.Volume = std::round(json["trade_volume"].GetDouble() * scale_offset);
        //KF_LOG_INFO(logger, "MDEngineUpbit::onTrade: volume " << volume);

        sequent = json["sequential_id"].GetInt64();
        sprintf(trade.Sequence , "%lld", sequent);
        //KF_LOG_INFO(logger, "MDEngineUpbit::onTrade: sequence " << sequent);

        side = json["ask_bid"].GetString();
        //KF_LOG_INFO(logger, "MDEngineHitBTC::onTrades: side " << side);
        if (side == "ASK")
            strcpy(trade.OrderBSFlag, "S");
        else if (side == "BID")
            strcpy(trade.OrderBSFlag, "B");
        else KF_LOG_ERROR(logger, "MDEngineHItBTC::onTrade:get trade.OrderBSFlag error");
        
        date = json["trade_date"].GetString();
        time = json["trade_time"].GetString();
        if(time.size() < 8)
        {
            time+=":00";
        }
        time = date + "T" + time+".000Z";
        strcpy(trade.TradeTime, time.c_str());
        trade.TimeStamp = formatISO8601_to_timestamp(trade.TradeTime)*1000000;
        //KF_LOG_INFO(logger, "MDEngineUpbit::[onTrade] (coinpair)" << coinpair <<" (Price)" << trade.Price <<
        //    " (trade.Volume)" << trade.Volume << "(trade.OrderBSFlag):" << trade.OrderBSFlag <<
        //    " (trade.TradeID): " << trade.Sequence << "(trade.TradeTime)" << trade.TradeTime);
        std::unique_lock<std::mutex> lck1(trade_mutex);
        auto it = control_trade_map.find(coinpair);
        if(it != control_trade_map.end())
        {
            it->second = getTimestamp();
        }
        lck1.unlock();
        on_trade(&trade);
    }
    else {
        KF_LOG_INFO(logger, "MDEngineUpbit::onTrade:trade_snapshot(coinpair): " << coinpair);
    }
}

std::string MDEngineUpbit::parseJsonToString(const char* in)
{
    Document d;
    d.Parse(reinterpret_cast<const char*>(in));

    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}

std::string MDEngineUpbit::parseJsonToString(Document& json)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    json.Accept(writer);

    return buffer.GetString();
}

std::string MDEngineUpbit::getUUID()
{
    /*uuid_t uuid;
    //The UUID is 16 bytes (128 bits) long
    uuid_generate(uuid);
    return string((char*)uuid);
    */
    const std::string CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string uuid = std::string(36, ' ');
    int rnd = 0;
    int r = 0;

    uuid[8] = '-';
    uuid[13] = '-';
    uuid[18] = '-';
    uuid[23] = '-';

    uuid[14] = '4';

    for (int i = 0; i < 36; i++) {
        if (i != 8 && i != 13 && i != 18 && i != 14 && i != 23) {
            if (rnd <= 0x02) {
                rnd = 0x2000000 + (std::rand() * 0x1000000) | 0;
            }
            rnd >>= 4;
            uuid[i] = CHARS[(i == 19) ? ((rnd & 0xf) & 0x3) | 0x8 : rnd & 0xf];
        }
    }
    return uuid;
}

/*
[
    {
    "ticket":UUID
    },
    {
    "type":"orderbook",
    "codes" : ["KRW-BTC","USDT-BCH"]
    }
]
*/
std::string MDEngineUpbit::createOrderBookJsonString(std::vector<std::string>& base_quote)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartArray();
    writer.StartObject();
    writer.Key("ticket");
    writer.String(getUUID().c_str());
    writer.EndObject();
    //orderbook
    writer.StartObject();
    writer.Key("type");
    writer.String("orderbook");
    writer.Key("codes");
    writer.StartArray();
    for(auto& item:base_quote)
    {
        writer.String(item.c_str());
    }
    writer.EndArray();
    writer.EndObject();
    //trade
    writer.StartObject();
    writer.Key("type");
    writer.String("trade");
    writer.Key("codes");
    writer.StartArray();
    for(auto& item:base_quote)
    {
        writer.String(item.c_str());
    }
    writer.EndArray();
    writer.EndObject();

    writer.EndArray();
    return s.GetString();
}

/*
[
    {
    "ticket":UUID
    },
    {
    "type":"trade",
    "codes" : ["KRW-BTC","USDT-BCH"]
    }
]
*/
std::string MDEngineUpbit::createTradeJsonString(std::vector<std::string>& base_quote)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartArray();
    writer.StartObject();
    writer.Key("ticket");
    writer.String(getUUID().c_str());
    writer.EndObject();
    writer.StartObject();
    writer.Key("type");
    writer.String("trade");
    writer.Key("codes");
    writer.StartArray();
    for(auto& item:base_quote)
    {
        writer.String(item.c_str());
    }
    writer.EndArray();
    writer.EndObject();
    writer.EndArray();
    return s.GetString();
}

//https://api.upbit.com/v1/candles/minutes/5?market=USDT-ETH&count=10
/*
[{  "market":"USDT-ETH",
    "candle_date_time_utc":"2021-05-26T07:30:00",
    "candle_date_time_kst":"2021-05-26T16:30:00",
    "opening_price":2890.79499000,
    "high_price":2890.79499000,
    "low_price":2838.87849000,
    "trade_price":2838.87849000,
    "timestamp":1622014350059,
    "candle_acc_trade_price":446.35219109,
    "candle_acc_trade_volume":0.15527297,
    "unit":5}
]
*/
void MDEngineUpbit::get_kline_via_rest(string symbol, int minutes, int count)
{
    string ticker = coinPairWhiteList.GetKeyByValue(symbol);
    KF_LOG_INFO(logger, "get_kline_via_rest (symbol)" << symbol << "(ticker)" << ticker);

    string baseUrl = "https://api.upbit.com";
    string url = baseUrl + "/v1/candles/minutes/" + to_string(minutes) + "?market=" + symbol + "&count=" + to_string(count);

    cpr::Response response = Get(Url{ url.c_str() }, Parameters{});
    Document d;
    d.Parse(response.text.c_str());
    KF_LOG_INFO(logger, "get_kline_via_rest get(" << url << "):" << response.text);

    if (d.IsArray()) {
        for (int i = 0; i < d.Size(); i++) {
            LFBarMarketDataField market;
            memset(&market, 0, sizeof(market));
            strcpy(market.InstrumentID, ticker.c_str());
            strcpy(market.ExchangeID, "Upbit");

            market.PeriodMillisec = minutes * 60 * 1000;
            string startStr = d[i]["candle_date_time_utc"].GetString();
            startStr += ".000dZ";
            int64_t nStartTime = formatISO8601_to_timestamp(startStr);
            int64_t nEndTime = nStartTime + market.PeriodMillisec;
            market.StartUpdateMillisec = nStartTime;
            market.EndUpdateMillisec = nEndTime;

            struct tm cur_tm, start_tm, end_tm;
            int ms = nStartTime % 1000;
            nStartTime /= 1000;
            start_tm = *localtime((time_t*)(&nStartTime));
            sprintf(market.StartUpdateTime, "%02d:%02d:%02d.%03d", start_tm.tm_hour, start_tm.tm_min, start_tm.tm_sec, ms);
            ms = nEndTime % 1000;
            nEndTime /= 1000;
            end_tm = *localtime((time_t*)(&nEndTime));
            sprintf(market.EndUpdateTime, "%02d:%02d:%02d.%03d", end_tm.tm_hour, end_tm.tm_min, end_tm.tm_sec, ms);

            market.Open   = std::round(d[i]["opening_price"].GetDouble() * scale_offset);
            market.Close  = std::round(d[i]["trade_price"].GetDouble() * scale_offset);
            market.Low    = std::round(d[i]["low_price"].GetDouble() * scale_offset);
            market.High   = std::round(d[i]["high_price"].GetDouble() * scale_offset);
            //market.Volume = std::round(d[i]["candle_acc_trade_volume"].GetDouble() * scale_offset);

            on_market_bar_data(&market);
        }
    }
}

void MDEngineUpbit::get_snapshot_via_rest()
{
    {
        std::unordered_map<std::string, std::string>::iterator map_itr;
        for(map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); map_itr++)
        {
            std::string url = "https://api.upbit.com/v1/orderbook?markets=";
            url+=map_itr->second;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            //"code":"200000"
            if(d.IsArray())
            {
                auto& orderbook_units = d.GetArray()[0]["orderbook_units"];
                uint64_t sequence = d.GetArray()[0]["timestamp"].GetInt64();
                int len = std::min((int)orderbook_units.Size(),20);

                LFPriceBook20Field priceBook {0};
                strcpy(priceBook.ExchangeID, "upbit");
                strncpy(priceBook.InstrumentID, map_itr->first.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, map_itr->first.size()));

                for(int i = 0; i < len; ++i)
                {
                    priceBook.BidLevels[i].price = std::round(orderbook_units.GetArray()[i]["bid_price"].GetDouble() * scale_offset);
                    priceBook.BidLevels[i].volume = std::round(orderbook_units.GetArray()[i]["bid_size"].GetDouble() * scale_offset);
                    priceBook.AskLevels[i].price = std::round(orderbook_units.GetArray()[i]["ask_price"].GetDouble() * scale_offset);
                    priceBook.AskLevels[i].volume = std::round(orderbook_units.GetArray()[i]["ask_size"].GetDouble() * scale_offset);                    
                }
                priceBook.BidLevelCount = priceBook.AskLevelCount = len;

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

void MDEngineUpbit::check_snapshot()
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
void MDEngineUpbit::rest_loop()
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
void MDEngineUpbit::check_loop()
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
int64_t last_kline_time = 0;
void MDEngineUpbit::onKline_loop()
{
	KF_LOG_INFO(logger,"onKline Thread start "); 
	while(isRunning)
	{
		int64_t now = getTimestamp();
		if((now - last_kline_time) >= get_kline_wait_ms)
		{
			last_kline_time = now;
			std::unordered_map<std::string, std::string>::iterator map_itr;
            		for (map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); map_itr++) 
			{
				onKline(map_itr->second, 1, 1);
            		}

		}
	}
}

void MDEngineUpbit::get_kline_loop()
{
    while (isRunning)
    {
        int64_t now = getTimestamp();
        if ((now - last_rest_time) >= get_kline_wait_ms)
        {
            last_rest_time = now;
            std::unordered_map<std::string, std::string>::iterator map_itr;
            for (map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); map_itr++) {
                get_kline_via_rest(map_itr->second, kline_a_interval_min, get_kline_via_rest_count);
                if(kline_a_interval_min != kline_b_interval_min)
                    get_kline_via_rest(map_itr->second, kline_b_interval_min, get_kline_via_rest_count);
            }
        }
    }
}

void MDEngineUpbit::loop()
{
    while(isRunning)
    {
        /*edited by zyy,starts here*/
        int64_t now = getTimestamp();
        int errorId = 0;
        std::string errorMsg = "";

        std::unique_lock<std::mutex> lck1(trade_mutex);
        std::map<std::string,int64_t>::iterator it;
        for(it = control_trade_map.begin(); it != control_trade_map.end(); it++){
            if((now - it->second) > refresh_normal_check_trade_s * 1000){
                errorId = 115;
                errorMsg = it->first + " trade max refresh wait time exceeded";
                KF_LOG_INFO(logger,"115 "<<errorMsg); 
                write_errormsg(errorId,errorMsg);
                it->second = now;                   
            }
        }
        lck1.unlock();

        std::unique_lock<std::mutex> lck(book_mutex);
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
        lck.unlock();
        /*KF_LOG_INFO(logger,"now = "<<now<<",timer = "<<timer<<", refresh_normal_check_book_s="<<refresh_normal_check_book_s);
        if ((now - timer) > refresh_normal_check_book_s * 1000)
        {
            KF_LOG_INFO(logger, "failed price book update");
            write_errormsg(114,"orderbook max refresh wait time exceeded");
            timer = now;
        }*/
        /*edited by zyy ends here*/
        if ((now - ping_time) >= 60 * 1000)
        {
            KF_LOG_INFO(logger, "will send ping");
            std::unique_lock<std::mutex> lck_ping(ping_mutex);
            ping_queue.push(1);
            lck_ping.unlock();
            lws_callback_on_writable(ws_wsi);
            ping_time = now;
        }        
        lws_service( context, rest_get_interval_ms );
    }
}

void MDEngineUpbit::onKline(string coinpair, int minutes, int count) {
	string ticker = coinPairWhiteList.GetKeyByValue(coinpair);
	string baseUrl = "https://api.upbit.com";
	string url = baseUrl + "/v1/candles/minutes/" + to_string(minutes) + "?market=" + coinpair + "&count=" + to_string(count);
	cpr::Response response = Get(Url{ url.c_str() }, Parameters{});
	KF_LOG_INFO(logger, "onKline get url : " << url << response.text.c_str());
	KF_LOG_INFO(logger, "response : "<<response.text);
	Document d;
	d.Parse(response.text.c_str());
	if (d.IsArray()) {
		for (int i = 0; i < d.Size(); i++) {
			LFBarMarketDataField market;
			memset(&market, 0, sizeof(market));

			strcpy(market.InstrumentID, ticker.c_str());
			strcpy(market.ExchangeID, "Upbit");
			market.PeriodMillisec = minutes * 60 * 1000;
			string startStr = d[i]["candle_date_time_utc"].GetString();
			startStr += ".000dZ";
			int64_t nStartTime = formatISO8601_to_timestamp(startStr);
			int64_t nEndTime = nStartTime + market.PeriodMillisec - 1;
			market.StartUpdateMillisec = nStartTime;
			market.EndUpdateMillisec = nEndTime;

			struct tm cur_tm, start_tm, end_tm;
			int ms = nStartTime % 1000;
			nStartTime /= 1000;
			start_tm = *localtime((time_t*)(&nStartTime));
			sprintf(market.StartUpdateTime, "%02d:%02d:%02d.%03d", start_tm.tm_hour, start_tm.tm_min, start_tm.tm_sec, ms);
			ms = nEndTime % 1000;
			nEndTime /= 1000;
			end_tm = *localtime((time_t*)(&nEndTime));
			sprintf(market.EndUpdateTime, "%02d:%02d:%02d.%03d", end_tm.tm_hour, end_tm.tm_min, end_tm.tm_sec, ms);

			market.Open   = std::round(d[i]["opening_price"].GetDouble() * scale_offset);
			market.Close  = std::round(d[i]["trade_price"].GetDouble() * scale_offset);
			market.Low    = std::round(d[i]["low_price"].GetDouble() * scale_offset);
			market.High   = std::round(d[i]["high_price"].GetDouble() * scale_offset);
			market.Volume = std::round(d[i]["candle_acc_trade_volume"].GetDouble() * scale_offset);

			on_market_bar_data(&market);
		}
	}

}

/*edited by zyy,starts here*/
inline int64_t MDEngineUpbit::getTimestamp()
{   /*返回的是毫秒*/
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}
/*edited by zyy ends here*/

BOOST_PYTHON_MODULE(libupbitmd)
{
    using namespace boost::python;
    class_<MDEngineUpbit, boost::shared_ptr<MDEngineUpbit> >("Engine")
            .def(init<>())
            .def("init", &MDEngineUpbit::initialize)
            .def("start", &MDEngineUpbit::start)
            .def("stop", &MDEngineUpbit::stop)
            .def("logout", &MDEngineUpbit::logout)
            .def("wait_for_stop", &MDEngineUpbit::wait_for_stop);
}
