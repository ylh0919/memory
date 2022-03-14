#include "MDEnginebitFlyer.h"
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

std::mutex trade_mutex;
std::mutex book_mutex;

static MDEnginebitFlyer* global_md = nullptr;
std::map<std::string,std::string> alias_name;
//std::vector<std::string> product_alias_name;

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

MDEnginebitFlyer::MDEnginebitFlyer(): IMDEngine(SOURCE_BITFLYER)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.bitFlyer");
}

void MDEnginebitFlyer::load(const json& j_config)
{
    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    if(j_config.find("refresh_normal_check_trade_s") != j_config.end()) {
        refresh_normal_check_trade_s = j_config["refresh_normal_check_trade_s"].get<int>();
    }
    book_depth_count = j_config["book_depth_count"].get<int>();
    trade_count = j_config["trade_count"].get<int>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    KF_LOG_INFO(logger, "MDEnginebitFlyer:: rest_get_interval_ms: " << rest_get_interval_ms);


    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    makeWebsocketSubscribeJsonString();
    debug_print(websocketSubscribeJsonString);

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEnginebitFlyer::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"tBTCUSDT\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
        KF_LOG_ERROR(logger, "},");
    }

    KF_LOG_INFO(logger, "MDEnginebitFlyer::load:  book_depth_count: "
            << book_depth_count << " trade_count: " << trade_count << " rest_get_interval_ms: " << rest_get_interval_ms);

    int64_t nowTime = getTimestamp();
	std::unordered_map<std::string, std::string>::iterator it;
    for(it = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();it != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end();it++)
    {
        std::unique_lock<std::mutex> lck(trade_mutex);
        control_trade_map.insert(make_pair(it->first,nowTime));
        lck.unlock();

        std::unique_lock<std::mutex> lck1(book_mutex);
        control_book_map.insert(make_pair(it->first,nowTime));
        lck1.unlock();
    }
}

void MDEnginebitFlyer::makeWebsocketSubscribeJsonString()
{
    alias_name.clear();
    string url1="https://api.bitflyer.com/v1/getmarkets";
    const auto response = Get(Url{url1});
    Document d;
    d.Parse(response.text.c_str());
    if(!d.HasParseError()){
        KF_LOG_INFO(logger,"d.Parse run");
        for(int i=0;i<d.Size();i++)
        {
            auto& temp=d.GetArray()[i];
            if(temp.HasMember("alias"))
            {
                alias_name.insert(std::make_pair(temp["alias"].GetString(),temp["product_code"].GetString()));
                KF_LOG_DEBUG(logger, "product_code: " << temp["product_code"].GetString()<< ", alias: " << temp["alias"].GetString());
            }
        }
    }
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    while(map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) "<< map_itr->second);
        std::string pairkey=map_itr->first;
        std::string pairvalue=map_itr->second;
        /*std::string jsonBookString = createBookJsonString(map_itr->second);
        websocketSubscribeJsonString.push_back(jsonBookString);*/
        if(pairkey.find("_futures") != -1)
        {
            KF_LOG_INFO(logger,"find the _futures");
            for(auto alias_str:alias_name){
                    std::string aliasvalue=alias_str.first;
                    std::string productvalue=alias_str.second;
                    if(aliasvalue.find(pairvalue)!=-1)
                    {
                        std::string jsonSnapshotString = createSnapshotJsonString(productvalue);
                        websocketSubscribeJsonString.push_back(jsonSnapshotString);

                        std::string jsonTradeString = createTradeJsonString(productvalue);
                        websocketSubscribeJsonString.push_back(jsonTradeString);   
                        //break;
                    }
            }
        }
        else {
            std::string jsonSnapshotString = createSnapshotJsonString(pairvalue);
            websocketSubscribeJsonString.push_back(jsonSnapshotString);
            std::string jsonTradeString = createTradeJsonString(pairvalue);
            websocketSubscribeJsonString.push_back(jsonTradeString);        
        }
        map_itr++;
    }
    ///
}

void MDEnginebitFlyer::debug_print(std::vector<std::string> &subJsonString)
{
    size_t count = subJsonString.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
    }
}

void MDEnginebitFlyer::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEnginebitFlyer::connect:");
    connected = true;
}

void MDEnginebitFlyer::login(long timeout_nsec) 
{
    KF_LOG_INFO(logger, "MDEnginebitFlyer::login:");
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
        KF_LOG_INFO(logger, "MDEnginebitFlyer::login: context created.");
    }

    if (context == NULL) {
        KF_LOG_ERROR(logger, "MDEnginebitFlyer::login: context is NULL. return");
        return;
    }

    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
    lws_set_log_level(logs, NULL);

    struct lws_client_connect_info ccinfo = {0};

    static std::string host  = "ws.lightstream.bitflyer.com";
    static std::string path = "/json-rpc";
    static int port = 443;

    ccinfo.context 	= context;
    ccinfo.address 	= host.c_str();
    ccinfo.port 	= port;
    ccinfo.path 	= path.c_str();
    ccinfo.host 	= host.c_str();
    ccinfo.origin 	= host.c_str();
    ccinfo.ietf_version_or_minus_one = -1;
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

    struct lws* wsi = lws_client_connect_via_info(&ccinfo);
    KF_LOG_INFO(logger, "MDEnginebitFlyer::login: Connecting to " <<  ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "MDEnginebitFlyer::login: wsi create error.");
        return;
    }
    KF_LOG_INFO(logger, "MDEnginebitFlyer::login: wsi create success.");

    logged_in = true;
    timer = getTimestamp();
}

void MDEnginebitFlyer::set_reader_thread()
{
    IMDEngine::set_reader_thread();

    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEnginebitFlyer::loop, this)));
}

void MDEnginebitFlyer::logout()
{
    KF_LOG_INFO(logger, "MDEnginebitFlyer::logout:");
}

void MDEnginebitFlyer::release_api()
{
    KF_LOG_INFO(logger, "MDEnginebitFlyer::release_api:");
}

void MDEnginebitFlyer::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEnginebitFlyer::subscribeMarketData:");
}

int MDEnginebitFlyer::lws_write_subscribe(struct lws* conn)
{
    KF_LOG_INFO(logger, "MDEnginebitFlyer::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    //有待发送的数据，先把待发送的发完，在继续订阅逻辑。  ping?
    if(websocketPendingSendMsg.size() > 0) {
        unsigned char msg[512];
        memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

        std::string jsonString = websocketPendingSendMsg[websocketPendingSendMsg.size() - 1];
        websocketPendingSendMsg.pop_back();
        KF_LOG_INFO(logger, "MDEnginebitFlyer::lws_write_subscribe: websocketPendingSendMsg" << jsonString.c_str());
        int length = jsonString.length();

        strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
        int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

        if(websocketPendingSendMsg.size() > 0)
        {    //still has pending send data, emit a lws_callback_on_writable()
            lws_callback_on_writable( conn );
            KF_LOG_INFO(logger, "MDEnginebitFlyer::lws_write_subscribe: (websocketPendingSendMsg,size)" << websocketPendingSendMsg.size());
        }
        return ret;
    }

    if(websocketSubscribeJsonString.size() == 0) return 0;
    //sub depth
    if(subscribe_index >= websocketSubscribeJsonString.size())
    {
        //subscribe_index = 0;
        KF_LOG_INFO(logger, "MDEnginebitFlyer::lws_write_subscribe: (none reset subscribe_index = 0, just return 0)");
	    return 0;
    }

    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

    std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

    KF_LOG_INFO(logger, "MDEnginebitFlyer::lws_write_subscribe: " << jsonString.c_str() << " ,after ++, (subscribe_index)" << subscribe_index);
    int length = jsonString.length();

    strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

    if(subscribe_index < websocketSubscribeJsonString.size())
    {
        lws_callback_on_writable( conn );
        KF_LOG_INFO(logger, "MDEnginebitFlyer::lws_write_subscribe: (subscribe_index < websocketSubscribeJsonString) call lws_callback_on_writable");
    }

    return ret;
}

void MDEnginebitFlyer::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "MDEnginebitFlyer::on_lws_data: " << data);
    Document json;
    json.Parse(data);

    if(json.HasParseError()) {
        KF_LOG_ERROR(logger, "MDEnginebitFlyer::on_lws_data. parse json error: " << data);
        return;
    }

        string channame = json["params"].GetObject()["channel"].GetString();;
        KF_LOG_INFO(logger, "MDEnginebitFlyer::on_lws_data: (channame)" << channame);
        if (channame.empty()) {
            KF_LOG_ERROR(logger, "MDEnginebitFlyer::on_lws_data: EMPTY_CHANNEL (channame)" << channame);
        } else 
        {
            if (channame.find("lightning_board_snapshot_") != std::string::npos) {
                KF_LOG_INFO(logger, "MDEnginebitFlyer::on_lws_data: is snapshot");
                onBook(json,channame.erase(0,25),true);
            } 
            else if (channame.find("lightning_board_") != std::string::npos) 
            {
                KF_LOG_INFO(logger, "MDEnginebitFlyer::on_lws_data: is update");
                onBook(json,channame.erase(0,16),false);
            } 
            else if(channame.find("lightning_executions_") != std::string::npos) {
                KF_LOG_INFO(logger, "MDEnginebitFlyer::on_lws_data: is trade");
                onTrade(json,channame.erase(0,21));               
            }
            else {
                KF_LOG_INFO(logger, "MDEnginebitFlyer::on_lws_data: unknown array data: " << data);
            }
        }
}

void MDEnginebitFlyer::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "MDEnginebitFlyer::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEnginebitFlyer::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;

    login(timeout_nsec);
}

int64_t MDEnginebitFlyer::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

/*
 * // request
{
   "event":"ping",
   "cid": 1234
}

// response
{
   "event":"pong",
   "ts": 1511545528111,
   "cid": 1234
}
 * */
/*
void MDEnginebitFlyer::onPing(struct lws* conn, Document& json)
{
    KF_LOG_INFO(logger, "MDEnginebitFlyer::onPing: " << parseJsonToString(json));
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("method");
    writer.String("pong");

    writer.Key("ts");
    writer.Int64(getTimestamp());

    writer.Key("cid");
    writer.Int(json["cid"].GetInt());

    writer.EndObject();

    std::string result = s.GetString();
    KF_LOG_INFO(logger, "MDEnginebitFlyer::onPing: (Pong)" << result);
    websocketPendingSendMsg.push_back(result);

    //emit a callback
    lws_callback_on_writable( conn );
}
*/

/*
 * #1
 * {
   "event":"info",
   "code": CODE,
   "msg": MSG
}
#2
 {
   "event": "info",
   "version":  VERSION,
   "platform": {
      "status": 1
   }
}
 * */
/*
void MDEnginebitFlyer::onInfo(Document& json)
{
    KF_LOG_INFO(logger, "MDEnginebitFlyer::onInfo: " << parseJsonToString(json));
}
*/

//{\event\:\subscribed\,\channel\:\book\,\chanId\:56,\symbol\:\tETCBTC\,\prec\:\P0\,\freq\:\F0\,\len\:\25\,\pair\:\ETCBTC\}
//{\event\:\subscribed\,\channel\:\trades\,\chanId\:2337,\symbol\:\tETHBTC\,\pair\:\ETHBTC\}
//{\event\:\subscribed\,\channel\:\trades\,\chanId\:1,\symbol\:\tBTCUSD\,\pair\:\BTCUSD\}
/*
void MDEnginebitFlyer::onSubscribed(Document& json)
{
    KF_LOG_INFO(logger, "MDEnginebitFlyer::onSubscribed: " << parseJsonToString(json));

    if(json.HasMember("params")) {
        string channame=json["params"].GetObject()["channel"].GetString();

        if(channame=="lightning_board_snapshot_BTC_JPY"||channame=="lightning_board_snapshot_FX_BTC_JPY"||channame=="lightning_board_snapshot_ETH_BTC") {
            SubscribeChannel newChannel;
            newChannel.subType = book_channel;
            newChannel.channelname = channame;
            websocketSubscribeChannel.push_back(newChannel);
        }

        if(channame=="lightning_board_BTC_JPY"||channame=="lightning_board_FX_BTC_JPY"||channame=="lightning_board_ETH_BTC") {
            SubscribeChannel newChannel;
            newChannel.subType = book_channel;
            newChannel.channelname = channame;
            websocketSubscribeChannel.push_back(newChannel);
        }
        
        if(channame=="lightning_executions_BTC_JPY"||channame=="lightning_executions_FX_BTC_JPY"||channame=="lightning_executions_ETH_BTC") {
            SubscribeChannel newChannel;
            newChannel.subType = trade_channel;
            newChannel.channelname = channame;
            websocketSubscribeChannel.push_back(newChannel);
        }        
      
    }

    debug_print(websocketSubscribeChannel);
}
*/

void MDEnginebitFlyer::debug_print(std::vector<SubscribeChannel> &websocketSubscribeChannel)
{
    size_t count = websocketSubscribeChannel.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeChannel (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeChannel (subType) "
                            << websocketSubscribeChannel[i].subType <<
                            " (channelname)" << websocketSubscribeChannel[i].channelname);
    }
}

/*SubscribeChannel MDEnginebitFlyer::findByChannelID(int channelId)
{
    size_t count = websocketSubscribeChannel.size();

    for (size_t i = 0; i < count; i++)
    {
        if(channelId == websocketSubscribeChannel[i].channelId) {
            return websocketSubscribeChannel[i];
        }
    }
    return EMPTY_CHANNEL;
}*/
/*
SubscribeChannel MDEnginebitFlyer::findByChannelNAME(string channelname)
{
    size_t count = websocketSubscribeChannel.size();

    for (size_t i = 0; i < count; i++)
    {
        if(channelname == websocketSubscribeChannel[i].channelname) {
            return websocketSubscribeChannel[i];
        }
    }
    return EMPTY_CHANNEL;
}
*/

//[1,[[279619183,1534151022575,0.05404775,6485.1],[279619171,1534151022010,-1.04,6485],[279619170,1534151021847,-0.02211732,6485],......]
//[1,"te",[279619192,1534151024181,-0.05678467,6485]]
void MDEnginebitFlyer::onTrade(Document& json,std::string exchange_coinpair)
{
    /*    KF_LOG_INFO(logger, "MDEnginebitFlyer::onTrade: (symbol) " << channel.exchange_coinpair);

    std::string ticker = coinPairWhiteList.GetKeyByValue(channel.exchange_coinpair);
    if(ticker.length() == 0) {
        return;
    }*/

    std::string ticker;
    Value &node1=json["params"];
    ticker = coinPairWhiteList.GetKeyByValue(exchange_coinpair);
    KF_LOG_INFO(logger,"ticker="<<ticker);
    if(ticker.empty())
    {
        ticker = exchange_coinpair;
    }
    Value &node=node1["message"];
    if(node.IsArray()) {

        std::unique_lock<std::mutex> lck(trade_mutex);
        auto it = control_trade_map.find(ticker);
        if(it != control_trade_map.end())
        {
            it->second = getTimestamp();
        }
        lck.unlock();

        int len = node.GetArray().Size();
        if(len == 0) {
            return;
        }
        for(int i = 0; i < len; i++) {
            LFL2TradeField trade;
            memset(&trade, 0, sizeof(trade));
            strcpy(trade.InstrumentID, ticker.c_str());
            strcpy(trade.ExchangeID, "bitflyer");

            trade.Price = std::round(node.GetArray()[i]["price"].GetDouble() * scale_offset);
            trade.Volume = std::round(node.GetArray()[i]["size"].GetDouble() * scale_offset);
            std::string side = node.GetArray()[i]["side"].GetString();
            trade.OrderBSFlag[0] = side == "BUY" ? 'B' : 'S';    
            std::string tridetime;
            tridetime = node.GetArray()[i]["exec_date"].GetString();
            strcpy(trade.TradeTime, tridetime.c_str());  
            trade.TimeStamp = formatISO8601_to_timestamp(tridetime)*1000000;
            // std::string tradeid;
            int64_t tradeid = node.GetArray()[i]["id"].GetInt64();
            strcpy(trade.TradeID,std::to_string(tradeid).c_str());
 
            std::string buyid = node.GetArray()[i]["buy_child_order_acceptance_id"].GetString();
            std::string sellid = node.GetArray()[i]["sell_child_order_acceptance_id"].GetString();
            std::string big;
            std::string small;
            if(buyid > sellid) {
                big = buyid;
                small = sellid;
            }
            else{
                big = sellid;
                small = buyid;
            }
            strcpy(trade.TakerOrderID,big.c_str());
            strcpy(trade.MakerOrderID,small.c_str());
            on_trade(&trade);
        }
    }
    
}


void MDEnginebitFlyer::onBook(Document& json,std::string exchange_coinpair,bool is_snapshot)
{
//    KF_LOG_INFO(logger, "MDEnginebitFlyer::onBook: (symbol) " << channel.exchange_coinpair);
//    KF_LOG_INFO(logger, "MDEnginebitFlyer::onBook: (ticker) " << ticker);

    Value &node1=json["params"];
    std::string ticker = coinPairWhiteList.GetKeyByValue(exchange_coinpair);  
    KF_LOG_INFO(logger,"ticker==="<<ticker);
    if(ticker.empty()){
        ticker = exchange_coinpair;
        KF_LOG_INFO(logger,"ticker2==="<<ticker);
    }
    KF_LOG_INFO(logger, "MDEnginebitFlyer::onBook: (ticker) " << ticker);
    if(is_snapshot) 
    {
        priceBook20Assembler.clearPriceBook(ticker);
        Value &node=node1["message"];    
        int len = node["bids"].GetArray().Size(); 
        for (int i = 0; i < len; i++) {
            int64_t price = std::round(node["bids"].GetArray()[i]["price"].GetDouble() * scale_offset);
            uint64_t volume = std::round(node["bids"].GetArray()[i]["size"].GetDouble() * scale_offset);
            if(volume == 0) {
                priceBook20Assembler.EraseBidPrice(ticker, price);
            } else {
                priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
            }
        }
        int len1 = node["asks"].GetArray().Size();  
        for (int i = 0; i < len1; i++) {
            int64_t price = std::round(node["asks"].GetArray()[i]["price"].GetDouble() * scale_offset);
            uint64_t volume = std::round(node["asks"].GetArray()[i]["size"].GetDouble() * scale_offset);
            if(volume == 0) {
                priceBook20Assembler.EraseAskPrice(ticker, price);
            } else {
                priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
            }
        }
    }
    else{
        Value &node=node1["message"];    
        int len = node["bids"].GetArray().Size();  
        for (int i = 0; i < len; i++) {
            int64_t price = std::round(node["bids"].GetArray()[i]["price"].GetDouble() * scale_offset);
            uint64_t volume = std::round(node["bids"].GetArray()[i]["size"].GetDouble() * scale_offset);
            if(volume == 0) {
                priceBook20Assembler.EraseBidPrice(ticker, price);
            } else {
                priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
            }
        }
        int len1 = node["asks"].GetArray().Size();
        for (int i = 0; i < len1; i++) {
            int64_t price = std::round(node["asks"].GetArray()[i]["price"].GetDouble() * scale_offset);
            uint64_t volume = std::round(node["asks"].GetArray()[i]["size"].GetDouble() * scale_offset);
            if(volume == 0) {
                priceBook20Assembler.EraseAskPrice(ticker, price);
            } else {
                priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
            }
        }            
    }
    
    // has any update
    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));
    if(priceBook20Assembler.Assembler(ticker, md)) {
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
            KF_LOG_INFO(logger,"failed ,level count < level threshold");
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"bids["<<i<<"]:"<<md.BidLevels[i].price);
            }
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"asks["<<i<<"]:"<<md.AskLevels[i].price);
            }
            string errorMsg = "orderbook level below threshold";
            md.Status = 1;
            write_errormsg(112,errorMsg);
        }
        else if(md.BidLevels[0].price > md.AskLevels[0].price)
        {
            KF_LOG_INFO(logger,"failed ,orderbook crossed");
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"bids["<<i<<"]:"<<md.BidLevels[i].price);
            }
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"asks["<<i<<"]:"<<md.AskLevels[i].price);
            }
            string errorMsg = "orderbook crossed";
            md.Status = 2;
            write_errormsg(113,errorMsg);
        }
        else{
            strcpy(md.ExchangeID, "bitflyer");

            KF_LOG_INFO(logger, "MDEnginebitFlyer::onDepth: on_price_book_update");
            on_price_book_update(&md);
        }
    }
}


std::string MDEnginebitFlyer::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}

//{ "event": "subscribe", "channel": "book",  "symbol": "tBTCUSD" }
std::string MDEnginebitFlyer::createBookJsonString(std::string exchange_coinpair)
{
    std::string strChannel="lightning_board_";
    strChannel+=exchange_coinpair.c_str(); 
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();

    writer.Key("jsonrpc");
    writer.String("2.0");

    writer.Key("method");
    writer.String("subscribe");

    writer.Key("params");
    writer.StartObject();
    
    writer.Key("channel");
    writer.String(strChannel.c_str());

    writer.EndObject();

    writer.EndObject();
    return s.GetString();
}

//{ "event": "subscribe", "channel": "trades",  "symbol": "tETHBTC" }
std::string MDEnginebitFlyer::createSnapshotJsonString(std::string exchange_coinpair)
{
    std::string strChannel="lightning_board_snapshot_";
    strChannel+=exchange_coinpair.c_str(); 
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();

    writer.Key("jsonrpc");
    writer.String("2.0");

    writer.Key("method");
    writer.String("subscribe");

    writer.Key("params");
    writer.StartObject();
    
    writer.Key("channel");
    writer.String(strChannel.c_str());

    writer.EndObject();

    writer.EndObject();
    return s.GetString();
}

std::string MDEnginebitFlyer::createTradeJsonString(std::string exchange_coinpair)
{
    std::string strChannel="lightning_executions_";
    strChannel+=exchange_coinpair.c_str(); 
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();

    writer.Key("jsonrpc");
    writer.String("2.0");

    writer.Key("method");
    writer.String("subscribe");

    writer.Key("params");
    writer.StartObject();
    
    writer.Key("channel");
    writer.String(strChannel.c_str());

    writer.EndObject();

    writer.EndObject();
    return s.GetString();
}

void MDEnginebitFlyer::loop()
{
    while(isRunning)
    {
        int errorId = 0;
        std::string errorMsg = "";

        int64_t now = getTimestamp();
        std::unique_lock<std::mutex> lck(trade_mutex);
        std::map<std::string, int64_t>::iterator it;
        for(it = control_trade_map.begin();it != control_trade_map.end();it++)
        {
            if((now - it->second) > refresh_normal_check_trade_s * 1000)
            {
                errorId = 115;
				errorMsg = it->first + " trade max refresh wait time exceeded";
	            KF_LOG_INFO(logger,"115"<<errorMsg); 
	            write_errormsg(errorId,errorMsg);
	            it->second = now;
            }
        }
        lck.unlock();

        std::unique_lock<std::mutex> lck1(book_mutex);
        std::map<std::string, int64_t>::iterator it1;
        for(it1 = control_book_map.begin();it1 != control_book_map.end();it1++)
        {
            if((now - it1->second) > refresh_normal_check_book_s * 1000)
            {
                errorId = 114;
				errorMsg = it1->first + " book max refresh wait time exceeded";
	            KF_LOG_INFO(logger,"114"<<errorMsg); 
	            write_errormsg(errorId,errorMsg);
	            it1->second = now;
            }
        }
        lck1.unlock();

        int n = lws_service( context, rest_get_interval_ms );
        std::cout << " 3.1415 loop() lws_service (n)" << n << std::endl;
    }
}

BOOST_PYTHON_MODULE(libbitflyermd)
{
    using namespace boost::python;
    class_<MDEnginebitFlyer, boost::shared_ptr<MDEnginebitFlyer> >("Engine")
            .def(init<>())
            .def("init", &MDEnginebitFlyer::initialize)
            .def("start", &MDEnginebitFlyer::start)
            .def("stop", &MDEnginebitFlyer::stop)
            .def("logout", &MDEnginebitFlyer::logout)
            .def("wait_for_stop", &MDEnginebitFlyer::wait_for_stop);
}


