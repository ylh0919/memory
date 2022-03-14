#include "MDEngineBitstamp.h"
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
#include <atomic>


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

std::mutex trade_mutex;
std::mutex book_mutex;

std::atomic<int64_t> timestamp (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
static MDEngineBitstamp* global_md = nullptr;
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;

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
                std::cout << "3.1415926 LWS_CALLBACK_CLIENT_CLOSED 2,  (call on_lws_connection_error)  reason = " << reason << std::endl;
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


enum protocolList {
    PROTOCOL_TEST,

    PROTOCOL_LIST_COUNT
};

struct session_data {
    int fd;
};

MDEngineBitstamp::MDEngineBitstamp(): IMDEngine(SOURCE_BITSTAMP)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Bitstamp");
}

void MDEngineBitstamp::load(const json& j_config) 
{
    book_depth_count = j_config["book_depth_count"].get<int>();
   
    trade_count = j_config["trade_count"].get<int>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    KF_LOG_INFO(logger, "MDEngineBitstamp:: rest_get_interval_ms: " << rest_get_interval_ms);
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    if(j_config.find("refresh_normal_check_trade_s") != j_config.end()) {
        refresh_normal_check_trade_s = j_config["refresh_normal_check_trade_s"].get<int>();
    }
    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("snapshot_check_s") != j_config.end()) {
        snapshot_check_s = j_config["snapshot_check_s"].get<int>();
    }
    priceBook20Assembler.SetLeastLevel(level_threshold);
    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    makeWebsocketSubscribeJsonString();
    debug_print(websocketSubscribeJsonString);

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineBitstamp::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"tBTCUSDT\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
        KF_LOG_ERROR(logger, "},");
    }

    KF_LOG_INFO(logger, "MDEngineBitstamp::load:  book_depth_count: "
            << book_depth_count << " trade_count: " << trade_count << " rest_get_interval_ms: " << rest_get_interval_ms);

    int64_t nowTime = getTimestamp();
    std::unordered_map<std::string, std::string>::iterator it;
    for(it = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();it != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end();it++)
    {
        std::unique_lock<std::mutex> lck(trade_mutex);
        control_trade_map.insert(make_pair(it->first, nowTime));
        lck.unlock();

        std::unique_lock<std::mutex> lck1(book_mutex);
        control_book_map.insert(make_pair(it->first, nowTime));
        lck1.unlock();
    }
}


// {
//     "event": "bts:subscribe",
//     "data": {
//         "channel": "live_orders_btcusd"
//     }
// }
std::string MDEngineBitstamp::createOrderJsonString(std::string exchange_coinpair)
{
    std::string data = "order_book_"+exchange_coinpair;
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("event");
    writer.String("bts:subscribe");
    writer.Key("data");
    writer.StartObject();
    writer.Key("channel");
    writer.String(data.c_str());
    writer.EndObject();
    
    writer.EndObject();
    return s.GetString();
}

// {
//     "event": "bts:subscribe",
//     "data": {
//         "channel": "live_trades_btcusd"
//     }
// }
std::string MDEngineBitstamp::createTradeJsonString(std::string exchange_coinpair)
{
    std::string data = "live_trades_"+exchange_coinpair;
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("event");
    writer.String("bts:subscribe");
    writer.Key("data");
    writer.StartObject();
    writer.Key("channel");
    writer.String(data.c_str());
    writer.EndObject();
    
    writer.EndObject();
    return s.GetString();
}

void MDEngineBitstamp::makeWebsocketSubscribeJsonString()//创建请求
{
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    while(map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) "<< map_itr->second);

        std::string jsonBookString = createOrderJsonString(map_itr->second);
        websocketSubscribeJsonString.push_back(jsonBookString);

        std::string jsonTradeString = createTradeJsonString(map_itr->second);
        websocketSubscribeJsonString.push_back(jsonTradeString);
        
        map_itr++;
    }
}

void MDEngineBitstamp::debug_print(std::vector<std::string> &subJsonString)
{
    size_t count = subJsonString.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
    }
}

void MDEngineBitstamp::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineBitstamp::connect:");
    connected = true;
}

void MDEngineBitstamp::login(long timeout_nsec) 
{//连接到服务器
    KF_LOG_INFO(logger, "MDEngineBitstamp::login:");
    global_md = this;

    if (context == NULL) {
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
        KF_LOG_INFO(logger, "MDEngineBitstamp::login: context created.");
    }

    if (context == NULL) {
        KF_LOG_ERROR(logger, "MDEngineBitstamp::login: context is NULL. return");
        return;
    }

    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
    lws_set_log_level(logs, NULL);

    struct lws_client_connect_info ccinfo = {0};

    static std::string host  = "ws.bitstamp.net";
    static std::string path = "/";
    static int port = 443;

    ccinfo.context     = context;
    ccinfo.address     = host.c_str();
    ccinfo.port     = port;
    ccinfo.path     = path.c_str();
    ccinfo.host     = host.c_str();
    ccinfo.origin     = host.c_str();
    ccinfo.ietf_version_or_minus_one = -1;
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

    struct lws* wsi = lws_client_connect_via_info(&ccinfo);
    KF_LOG_INFO(logger, "MDEngineBitstamp::login: Connecting to " <<  ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "MDEngineBitstamp::login: wsi create error.");
        return;
    }
    KF_LOG_INFO(logger, "MDEngineBitstamp::login: wsi create success.");

    logged_in = true;
}

void MDEngineBitstamp::set_reader_thread()
{
    IMDEngine::set_reader_thread();

    ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBitstamp::loop, this)));
    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBitstamp::rest_loop, this)));
    check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBitstamp::check_loop, this)));
}

void MDEngineBitstamp::logout()
{
    KF_LOG_INFO(logger, "MDEngineBitstamp::logout:");
}

void MDEngineBitstamp::release_api()
{
    KF_LOG_INFO(logger, "MDEngineBitstamp::release_api:");
}

void MDEngineBitstamp::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEngineBitstamp::subscribeMarketData:");
}

int MDEngineBitstamp::lws_write_subscribe(struct lws* conn)
{
    KF_LOG_INFO(logger, "MDEngineBitstamp::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    if (websocketSubscribeJsonString.size() == 0) return 0;
    //sub depth
    if (subscribe_index >= websocketSubscribeJsonString.size())
    {
        subscribe_index = 0;
    }

    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512 - LWS_PRE);

    std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

    KF_LOG_INFO(logger, "MDEngineBitstamp::lws_write_subscribe: " << jsonString.c_str());
    int length = jsonString.length();

    strncpy((char *)msg + LWS_PRE, jsonString.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);

    if (subscribe_index < websocketSubscribeJsonString.size())
    {
        lws_callback_on_writable(conn);
    }

    return ret;
}

void MDEngineBitstamp::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "MDEngineBitstamp::on_lws_data: " << data);
    Document json;
    json.Parse(data);

    if(json.HasParseError()) {
        KF_LOG_ERROR(logger, "MDEngineBitstamp::on_lws_data. parse json error: " << data);
        return;
    }
    if (json.HasMember("event")) {
        std::string channel = json["channel"].GetString();
        if (strcmp(json["event"].GetString(), "bts:subscription_succeeded") == 0) {//处理订阅成功信息    
            if (channel.find("order_book") != channel.npos) {
                KF_LOG_INFO(logger, "MDEngineBitstamp::on_lws_data: subscript order book succeeded");
            }
            else if (channel.find("live_trades") != channel.npos) {
                KF_LOG_INFO(logger, "MDEngineBitstamp::on_lws_data: subscript trades succeeded");
            }
            else KF_LOG_INFO(logger, "MDEngineBitstamp::on_lws_data: deal with bts:subscription error " << parseJsonToString(json));
        }
        else if(strcmp(json["event"].GetString(), "data") == 0){//处理订阅orderbook返回数据
            if (channel.find("order_book") != channel.npos) {
                onBook(json);
            }
            else KF_LOG_INFO(logger, "MDEngineBitstamp::on_lws_data:deal with subscription orderbook data error " << parseJsonToString(json));
        }
        else if (strcmp(json["event"].GetString(), "trade") == 0) {//处理订阅trade的返回数据
            if (channel.find("live_trades") != channel.npos) {
                onTrade(json);
            }
            else KF_LOG_INFO(logger, "MDEngineBitstamp::on_lws_data:subscription trade error ");
        }

        else KF_LOG_INFO(logger, "MDEngineBitstamp::on_lws_data:subscription error " << parseJsonToString(json));
    }
    else KF_LOG_INFO(logger, "MDEngineBitstamp::on_lws_data:get event error: " << parseJsonToString(json));
}

void MDEngineBitstamp::on_lws_connection_error(struct lws* conn) //liu
{
    KF_LOG_ERROR(logger, "MDEngineBitstamp::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineBitstamp::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;

    login(timeout_nsec);
}

int64_t MDEngineBitstamp::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void MDEngineBitstamp::debug_print(std::vector<SubscribeChannel> &websocketSubscribeChannel)
{
    size_t count = websocketSubscribeChannel.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeChannel (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeChannel (subType) "
                            << websocketSubscribeChannel[i].subType <<
                            " (exchange_coinpair)" << websocketSubscribeChannel[i].exchange_coinpair <<
                            " (channelId)" << websocketSubscribeChannel[i].channelId);
    }
}

  /*//     {
    //     \data\: {
    //         \microtimestamp\: \1557712345770094\,
    //         \amount\: 0.013745220000000001,
    //         \buy_order_id\: 3274515232,//type = 0 ->LFDataStruct.h中LFL2TradeField的makerorderID,否则为takerOrderID
    //         \sell_order_id\: 3274511591,
    //         \amount_str\: \0.01374522\,
    //         \price_str\: \7030.80\,
    //         \timestamp\: \1557712345\,
    //         \price\: 7030.8000000000002,
    //         \type\: 0,  (0 - buy; 1 - sell)
    //         \id\: 87447785
    //         },
    //     \event\: \trade\,
    //     \channel\: \live_trades_btcusd\
    // }*/
void MDEngineBitstamp::onTrade(Document& json) 
{
    //"{"data": {"buy_order_id": 1201513345032192, "amount_str": "0.00541916", "timestamp": "1582173193", "microtimestamp": "1582173193869000", "id": 105595374, "amount": 0.00541916, "sell_order_id": 1201513345892352, "price_str": "9552.34", "type": 1, "price": 9552.34}, "event": "trade", "channel": "live_trades_btcusd"}"
    KF_LOG_INFO(logger, "MDEngineBitstamp::onTrade: " << parseJsonToString(json));
    std::string coinpair, channel;
    int64_t price; uint64_t volume; int type; int64_t tradeID,buy_order_id,sell_order_id;

    channel = json["channel"].GetString();
    auto found = channel.find_last_of("_");
    coinpair = channel.substr(found + 1);//get coinpair from channel
    //KF_LOG_INFO(logger, "MDEngineBitstamp::onTrade: (symbol): " << coinpair);
    coinpair = coinPairWhiteList.GetKeyByValue(coinpair);
    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook: (coinpair): " << coinpair);
    if (coinpair.length() == 0)
    {
        KF_LOG_ERROR(logger, "MDEngineBitstamp::onTrade:get coinpair error");
        return;
    }
    if (json.HasMember("data") && json["data"].IsObject()) {
        //get trade data
        LFL2TradeField trade;
        memset(&trade, 0, sizeof(trade));
        strcpy(trade.InstrumentID, coinpair.c_str());
        //KF_LOG_INFO(logger, "MDEngineBitstamp::onTrade: InstrumentID: " << coinpair);
        
        strcpy(trade.ExchangeID, "Bitstamp");
        //KF_LOG_INFO(logger, "MDEngineBitstamp::onTrade: ExchangeID: Bitstamp ");

        trade.Price =std::round(json["data"]["price"].GetDouble()*scale_offset);
        //KF_LOG_INFO(logger, "MDEngineBitstamp::onTrade: price: " << trade.Price);

        trade.Volume =std::round(json["data"]["amount"].GetDouble()*scale_offset);
        //KF_LOG_INFO(logger, "MDEngineBitstamp::onTrade: Volume: " << trade.Volume);

        tradeID = json["data"]["id"].GetInt64();
        sprintf(trade.TradeID, "%lld", tradeID);
        //KF_LOG_INFO(logger, "MDEngineBitstamp::onTrade: TradeID " << tradeID);

        type = json["data"]["type"].GetInt();
        //KF_LOG_INFO(logger, "MDEngineBitstamp::onTrade: Tradetpye " << type);
        if (type == 0) {
            strcpy(trade.OrderBSFlag, "B");
            buy_order_id = json["data"]["buy_order_id"].GetInt64();
            sprintf(trade.MakerOrderID, "%lld", buy_order_id);
            //KF_LOG_INFO(logger, "MDEngineBitstamp::onTrade: MakerOrderID:  " << buy_order_id);
        }
        else if (type == 1) {
            strcpy(trade.OrderBSFlag, "S");
            sell_order_id = json["data"]["sell_order_id"].GetInt64();
            sprintf(trade.TakerOrderID, "%lld", sell_order_id);
            //KF_LOG_INFO(logger, "MDEngineBitstamp::onTrade: TakerOrderID " << sell_order_id);
        }
        else KF_LOG_ERROR(logger, "MDEngineHItBTC::onTrade:get trade.OrderBSFlag error");

        trade.TimeStamp = std::stoll(json["data"]["microtimestamp"].GetString())*1000;
        string strTime  = timestamp_to_formatISO8601(trade.TimeStamp/1000000);
        strncpy(trade.TradeTime, strTime.c_str(),sizeof(trade.TradeTime));

        std::unique_lock<std::mutex> lck(trade_mutex);
        auto it = control_trade_map.find(coinpair);
        if(it != control_trade_map.end())
        {
            it->second = getTimestamp();
        }
        lck.unlock();

        on_trade(&trade);
    }
    else KF_LOG_ERROR(logger, "MDEngineBitstamp::onTrade:json[data].IsObject() error");
}


 // {
        // "data": {
        //         "timestamp": "1557731190", 
        //         "microtimestamp": "1557731190857523", 
        //         "bids": [["7055.00", "0.00720000"], ["7052.76", "2.00000000"], ["7051.05", "0.02000000"]],
        //         "asks": [["7057.17", "2.00000000"],  ["7180.00", "2.00000000"], ["7184.58", "0.20000000"]]
        //         }, 
        // "event": "data", 
        // "channel": "order_book_btcusd"
        // }
void MDEngineBitstamp::onBook(Document& json) 
{
    KF_LOG_INFO(logger, "MDEngineBitstamp::onBook");
    std::string coinpair, channel;
    int64_t price;
    uint64_t volume;
    channel = json["channel"].GetString();
    auto found = channel.find_last_of("_");
    coinpair = channel.substr(found + 1);//get coinpair from channel
    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook: (symbol) " << coinpair);
    coinpair = coinPairWhiteList.GetKeyByValue(coinpair);
    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook: (coinpair) " << coinpair);
    if (coinpair.length() == 0)
    {
        KF_LOG_ERROR(logger, "MDEngineBitstamp::onOrderBook:get coinpair error");
        return;
    }
    uint64_t sequence;
    if(json["data"].HasMember("timestamp")){
        sequence = std::stoll(json["data"]["timestamp"].GetString());
    }
    if (json.HasMember("data") && json["data"].IsObject()) {
        priceBook20Assembler.clearPriceBook(coinpair);
        //ask update
        if (json["data"].HasMember("asks") && json["data"]["asks"].IsArray()) {
            int len = json["data"]["asks"].Size();
            auto &asks = json["data"]["asks"];
            for (int i = 0; i < len; i++) {
                price = std::round(stod(asks.GetArray()[i][0].GetString()) * scale_offset);
                volume = std::round(stod(asks.GetArray()[i][1].GetString()) * scale_offset);

                /*if (volume == 0) {
                    priceBook20Assembler.EraseAskPrice(coinpair, price);
                    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook:EraseAskPrice():coinpair: " << coinpair);
                    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook:EraseAskPrice():price: " << price);
                }
                else {*/
                    priceBook20Assembler.UpdateAskPrice(coinpair, price, volume);
                    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook:UpdateAskPrice():coinpair: " << coinpair);
                    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook:UpdateAskPrice():price: " << price);
                    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook:UpdateAskPrice():volume: " << volume);
                //}
            }
        }

        //bid update
        if (json["data"].HasMember("bids") && json["data"]["bids"].IsArray()) {
            int len = json["data"]["bids"].Size();
            auto &bid = json["data"]["bids"];
            for (int i = 0; i < len; i++) {
                int64_t price = std::round(stod(bid.GetArray()[i][0].GetString()) * scale_offset);
                uint64_t volume = std::round(stod(bid.GetArray()[i][1].GetString()) * scale_offset);
                /*if (volume == 0) {
                    priceBook20Assembler.EraseBidPrice(coinpair, price);
                    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook:EraseBidPrice():coinpair: " << coinpair);
                    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook:EraseBidPrice():price: " << price);
                }
                else {*/
                    priceBook20Assembler.UpdateBidPrice(coinpair, price, volume);
                    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook:UpdateBidPrice():coinpair: " << coinpair);
                    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook:UpdateBidPrice():price: " << price);
                    //KF_LOG_INFO(logger, "MDEngineBitstamp::onBook:UpdateBidPrice():volume:  " << volume);
                //}
            }
        }
    }

    //has any update
    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));
    if (priceBook20Assembler.Assembler(coinpair, md))
    {
        std::unique_lock<std::mutex> lck1(book_mutex);
        auto it = control_book_map.find(coinpair);
        if(it != control_book_map.end())
        {
            it->second = getTimestamp();
        }
        lck1.unlock();

        strcpy(md.ExchangeID, "BitStamp");
        if (md.AskLevelCount < level_threshold || md.BidLevelCount < level_threshold)
        {
            string errorMsg = "orderbook level below threshold";
            write_errormsg(112,errorMsg);
            KF_LOG_INFO(logger, "MDEngineDerbit::onbook: " << errorMsg);
            on_price_book_update(&md);
        }
        else if (md.BidLevels[0].price <= 0 || md.AskLevels[0].price <= 0 || md.BidLevels[0].price > md.AskLevels[0].price){
            string errorMsg = "orderbook crossed";
            write_errormsg(113,errorMsg);
            KF_LOG_INFO(logger, "MDEngineDerbit::onbook: " << errorMsg);
        }
        else
        {
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

            on_price_book_update(&md);
        }
    }
    else
    {
        //timer = getTimestamp();
        //GetSnapShotAndRtn(ticker);
        KF_LOG_DEBUG(logger, "MDEngineBitStamp onBook_update,priceBook20Assembler.Assembler(coinpair, md) failed\n(coinpair)" << coinpair);
    }
}

std::string MDEngineBitstamp::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}

// vector<string> split(const string &s, const string &seperator){  //字符串分割
//   vector<string> result;
//   typedef string::size_type string_size;
//   string_size i = 0;
  
//   while(i != s.size()){
//     //找到字符串中首个不等于分隔符的字母；
//     int flag = 0;
//     while(i != s.size() && flag == 0){
//       flag = 1;
//       for(string_size x = 0; x < seperator.size(); ++x)
//     　　if(s[i] == seperator[x]){
//       　　++i;
//       　　flag = 0;
//      　　 break;
//     　　}
//     }
    
//     //找到又一个分隔符，将两个分隔符之间的字符串取出；
//     flag = 0;
//     string_size j = i;
//     while(j != s.size() && flag == 0){
//       for(string_size x = 0; x < seperator.size(); ++x)
//     　　if(s[j] == seperator[x]){
//       　　flag = 1;
//      　　 break;
//     　　}
//       if(flag == 0) 
//     　　++j;
//     }
//     if(i != j){
//       result.push_back(s.substr(i, j-i));
//       i = j;
//     }
//   }
//   return result;
// }

void MDEngineBitstamp::get_snapshot_via_rest()
{
    {
        std::unordered_map<std::string, std::string>::iterator map_itr;
        for(map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); map_itr++)
        {
            std::string url = "https://www.bitstamp.net/api/v2/order_book/";
            url+=map_itr->second;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            //"code":"200000"
            if(d.IsObject() && d.HasMember("timestamp"))
            {
                uint64_t sequence = std::stoll(d["timestamp"].GetString());

                LFPriceBook20Field priceBook {0};
                strcpy(priceBook.ExchangeID, "BitStamp");
                strncpy(priceBook.InstrumentID, map_itr->first.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, map_itr->first.size()));
                if(d.HasMember("bids") && d["bids"].IsArray())
                {
                    auto& bids = d["bids"];
                    int len = std::min((int)bids.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.BidLevels[i].price = std::round(stod(bids[i][0].GetString()) * scale_offset);
                        priceBook.BidLevels[i].volume = std::round(stod(bids[i][1].GetString()) * scale_offset);
                    }
                    priceBook.BidLevelCount = len;
                }
                if (d.HasMember("asks") && d["asks"].IsArray())
                {
                    auto& asks = d["asks"];
                    int len = std::min((int)asks.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.AskLevels[i].price = std::round(stod(asks[i][0].GetString()) * scale_offset);
                        priceBook.AskLevels[i].volume = std::round(stod(asks[i][1].GetString()) * scale_offset);
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

void MDEngineBitstamp::check_snapshot()
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
void MDEngineBitstamp::rest_loop()
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
void MDEngineBitstamp::check_loop()
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

void MDEngineBitstamp::loop()
{
    while(isRunning)
    {
        int n = lws_service( context, rest_get_interval_ms );
        std::cout << " 3.1415 loop() lws_service (n)" << n << std::endl;
        int errorId = 0;
        string errorMsg = "";
        /*quest3 edited by fxw starts here*/
        /*判断是否在设定时间内更新与否，*/
         //114错误
        int64_t now = getTimestamp();
        //KF_LOG_INFO(logger, "quest3: update check ");
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
    }
}

BOOST_PYTHON_MODULE(libbitstampmd)
{
    using namespace boost::python;
    class_<MDEngineBitstamp, boost::shared_ptr<MDEngineBitstamp> >("Engine")
            .def(init<>())
            .def("init", &MDEngineBitstamp::initialize)
            .def("start", &MDEngineBitstamp::start)
            .def("stop", &MDEngineBitstamp::stop)
            .def("logout", &MDEngineBitstamp::logout)
            .def("wait_for_stop", &MDEngineBitstamp::wait_for_stop);
}
