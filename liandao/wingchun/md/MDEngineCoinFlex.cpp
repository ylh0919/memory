#include "MDEngineCoinFlex.h"
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
#include <stdlib.h>


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

static MDEngineCoinFlex* global_md = nullptr;
static std::string snapshot_data;
time_t reset_time = time(NULL);
static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{
    std::cout<<"ws_service_cb";
    std::cout<<"reason="<<reason << ",wsi" << wsi << std::endl;
    switch( reason )
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            std::cout<<"LWS_CALLBACK_CLIENT_ESTABLISHED";
            reset_time = time(NULL);
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
    std::cout<<"return 0";
    return 0;
}

static struct lws_protocols protocols[] =
        {
                {
                        "md-protocol",
                        ws_service_cb,
                              0,
                        1024*1024,
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

MDEngineCoinFlex::MDEngineCoinFlex(): IMDEngine(SOURCE_COINFLEX)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.CoinFlex");
    timer = getTimestamp();/*edited by zyy*/
}

void MDEngineCoinFlex::load(const json& j_config)
{
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
    if(j_config.find("cross_times") != j_config.end()) {
        cross_times = j_config["cross_times"].get<int>();
    }
    KF_LOG_INFO(logger, "MDEngineCoinFlex:: cross_times: " << cross_times);
    book_depth_count = j_config["book_depth_count"].get<int>();
    trade_count = j_config["trade_count"].get<int>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    if(j_config.find("reset_interval_s") != j_config.end()) {
        reset_interval_s = j_config["reset_interval_s"].get<int>();
    }
    KF_LOG_INFO(logger, "MDEngineCoinFlex:: rest_get_interval_ms: " << rest_get_interval_ms);


    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    makeWebsocketSubscribeJsonString();
    debug_print(websocketSubscribeJsonString);

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineCoinFlex::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"tBTCUSDT\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
        KF_LOG_ERROR(logger, "},");
    }

    KF_LOG_INFO(logger, "MDEngineCoinFlex::load:  book_depth_count: "
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

void MDEngineCoinFlex::makeWebsocketSubscribeJsonString()
{
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    int num = 1;
    while(map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) "<< map_itr->second);

        std::string coinpair = map_itr->second;
        CoinBaseQuote baseQuote;
        split(coinpair, "_", baseQuote);

        std::string jsonBookString = createBookJsonString(num, baseQuote.base, baseQuote.quote);
        websocketSubscribeJsonString.push_back(jsonBookString);
        currency[num] = baseQuote;
        num++;
        map_itr++;
    }
}

//zaf add
void MDEngineCoinFlex::split(std::string str, std::string token, CoinBaseQuote& sub)
{
    if (str.size() > 0)
    {
        size_t index = str.find(token);
        if (index != std::string::npos)
        {
            std::string str1  = str.substr(0, index);
            std::string str2 = str.substr(index + token.size());
            sub.base = atoi(str1.c_str());
            sub.quote = atoi(str2.c_str());
        }
        else {
            //not found, do nothing
        }
    }
}


void MDEngineCoinFlex::debug_print(std::vector<std::string> &subJsonString)
{
    size_t count = subJsonString.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
    }
}

void MDEngineCoinFlex::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineCoinFlex::connect:");
    connected = true;
}



void MDEngineCoinFlex::login(long timeout_nsec) {
    KF_LOG_INFO(logger, "MDEngineCoinFlex::login:");
    global_md = this;

    if (context == nullptr) {
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
        KF_LOG_INFO(logger, "MDEngineCoinFlex::login: context created.");
    }

    if (context == NULL) {
        KF_LOG_ERROR(logger, "MDEngineCoinFlex::login: context is NULL. return");
        return;
    }

    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
    lws_set_log_level(logs, NULL);

    struct lws_client_connect_info ccinfo = {0};

    static std::string host  = "api.coinflex.com";
    static std::string path = "/v1";
    static int port = 443;

    ccinfo.context 	= context;
    ccinfo.address 	= host.c_str();    //
    ccinfo.port 	= port;
    ccinfo.path 	= path.c_str();    //
    ccinfo.host 	= host.c_str();
    ccinfo.origin 	= host.c_str();
    ccinfo.ietf_version_or_minus_one = -1;
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

    struct lws* wsi = lws_client_connect_via_info(&ccinfo);
    KF_LOG_INFO(logger, "MDEngineCoinFlex::login: Connecting to " <<  ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "MDEngineCoinFlex::login: wsi create error.");
        sleep(10);
        wsi_times += 1;
        if(wsi_times <= 3){
            login(0);
        }else{
            KF_LOG_INFO(logger,"error 3 times");
            int errorId = 115;
            string errorMsg = "wsi create error";  
            write_errormsg(errorId,errorMsg);
        }
        //return;
    }
    KF_LOG_INFO(logger, "MDEngineCoinFlex::login: wsi create success."<<"wsi:"<<wsi<<"context2:"<<context);
    wsi_times = 0;

    logged_in = true;
}

void MDEngineCoinFlex::set_reader_thread()
{
    IMDEngine::set_reader_thread();
    KF_LOG_INFO(logger,"into set_reader_thread");
    ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineCoinFlex::loop, this)));
    reset_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineCoinFlex::reset_loop, this)));
}

void MDEngineCoinFlex::logout()
{
    KF_LOG_INFO(logger, "MDEngineCoinFlex::logout:");
}

void MDEngineCoinFlex::release_api()
{
    KF_LOG_INFO(logger, "MDEngineCoinFlex::release_api:");
}

void MDEngineCoinFlex::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEngineCoinFlex::subscribeMarketData:");
}

int MDEngineCoinFlex::lws_write_subscribe(struct lws* conn)
{
    KF_LOG_INFO(logger, "MDEngineCoinFlex::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    //有待发送的数据，先把待发送的发完，在继续订阅逻辑。  ping?
    if(websocketPendingSendMsg.size() > 0) {
        unsigned char msg[512];
        memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

        std::string jsonString = websocketPendingSendMsg[websocketPendingSendMsg.size() - 1];
        websocketPendingSendMsg.pop_back();
        KF_LOG_INFO(logger, "MDEngineCoinFlex::lws_write_subscribe: websocketPendingSendMsg" << jsonString.c_str());
        int length = jsonString.length();

        strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
        int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

        if(websocketPendingSendMsg.size() > 0)
        {    //still has pending send data, emit a lws_callback_on_writable()
            lws_callback_on_writable( conn );
            KF_LOG_INFO(logger, "MDEngineCoinFlex::lws_write_subscribe: (websocketPendingSendMsg,size)" << websocketPendingSendMsg.size());
        }
        return ret;
    }

    if(websocketSubscribeJsonString.size() == 0) return 0;

    if(subscribe_index >= websocketSubscribeJsonString.size())
    {
        //subscribe_index = 0;
        KF_LOG_INFO(logger, "MDEngineCoinFlex::lws_write_subscribe: (none reset subscribe_index = 0, just return 0)");
	    return 0;
    }

    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

    std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

    KF_LOG_INFO(logger, "MDEngineCoinFlex::lws_write_subscribe: " << jsonString.c_str() << " ,after ++, (subscribe_index)" << subscribe_index);
    int length = jsonString.length();

    strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

    if(subscribe_index < websocketSubscribeJsonString.size())
    {
        lws_callback_on_writable( conn );
        KF_LOG_INFO(logger, "MDEngineCoinFlex::lws_write_subscribe: (subscribe_index < websocketSubscribeJsonString) call lws_callback_on_writable");
    }

    return ret;
}

void MDEngineCoinFlex::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "MDEngineCoinFlex::on_lws_data: " << data);
    //
    if (isRunning) writer->writeStr(data);

    Document json;
    json.Parse(data);

    if (json.HasParseError() && len >= 4096) 
    {
        snapshot_data += data;
    }
    else if (json.HasParseError()) 
    {
        snapshot_data += data;
        json.Parse(snapshot_data.c_str());
        KF_LOG_INFO(logger, "MDEngineCoinfloor::on_lws_data: snapshot_data: " << snapshot_data);
        if (json.HasParseError()) 
        {
            KF_LOG_INFO(logger, "MDEngineCoinfloor::on_lws_data:error snapshot_data: " << snapshot_data);
            snapshot_data = "";
        }       
    }
    

    if(!json.HasParseError() && json.IsObject() && ((json.HasMember("tag")  && json["tag"].IsInt()) 
                                                      || (json.HasMember("notice") && json["notice"].IsString())))
    {
        snapshot_data = "";
        if(json.HasMember("tag") && json["tag"].IsInt())
        {  
            int num =  json["tag"].GetInt();
            Document::AllocatorType& allocator = json.GetAllocator();
            Value value(3);
            value.SetInt(currency[num].base);
            json.AddMember("base",value,allocator);
            value.SetInt(currency[num].quote);
            json.AddMember("counter",value,allocator);
        }

        if(json.HasMember("notice") && json["notice"].IsString() && strcmp(json["notice"].GetString(), "OrdersMatched") == 0)
        {
            KF_LOG_INFO(logger, "MDEngineCoinmex::on_lws_data: is trade");
            onTrade(json);
        }

        KF_LOG_INFO(logger, "MDEngineCoinFlex::on_lws_data: is onbook");
        onBook(json);
    }
    
    
}


void MDEngineCoinFlex::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "MDEngineCoinFlex::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineCoinFlex::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;
    context = nullptr;

    PriceId.clear();
    login(timeout_nsec);
}

void MDEngineCoinFlex::onTrade(Document& json)
{
    std::string base = "";
    if(json.HasMember("base") && json["base"].IsInt()) {
        base = std::to_string(json["base"].GetInt());
    }
    std::string counter = "";
    if(json.HasMember("counter") && json["counter"].IsInt()) {
        counter = std::to_string(json["counter"].GetInt());
    }

    KF_LOG_INFO(logger, "MDEngineCoinFlex::onTrade:" << "base : " << base << "  counter: " << counter);
    std::string base_counter = base + "_" +  counter;
    std::transform(base_counter.begin(), base_counter.end(), base_counter.begin(), ::toupper);
    std::string ticker = coinPairWhiteList.GetKeyByValue(base_counter);
    if(ticker.length() == 0) {
        KF_LOG_INFO(logger, "MDEngineCoinFlex::onTrade: not in WhiteList , ignore it:" << "base : " << base << "  counter: " << counter);
        return;
    } 
    int64_t price = json["price"].GetInt64() ;
    price *= scale_offset;
    int64_t volume = json["quantity"].GetInt64() ;
    volume *= scale_offset;
    LFL2TradeField trade_ask,trade_bid;
    memset(&trade_ask, 0, sizeof(trade_ask));
    strcpy(trade_ask.InstrumentID, ticker.c_str());
    strcpy(trade_ask.ExchangeID, "coinflex");
    memset(&trade_bid, 0, sizeof(trade_bid));
    strcpy(trade_bid.InstrumentID, ticker.c_str());
    strcpy(trade_bid.ExchangeID, "coinflex");
    trade_ask.Price = trade_bid.Price = price;
    trade_ask.Volume = trade_bid.Volume = volume;
    trade_ask.OrderBSFlag[0] = 'S';
    trade_bid.OrderBSFlag[0] = 'B';
    KF_LOG_INFO(logger, "MDEngineCoinmex::[ontrade_ask] (ticker)" << ticker <<
                                                                  " (Price)" << trade_ask.Price <<
                                                                  " (trade.Volume)" << trade_ask.Volume);
    KF_LOG_INFO(logger, "MDEngineCoinmex::[ontrade_bid] (ticker)" << ticker <<
                                                                  " (Price)" << trade_bid.Price <<
                                                                  " (trade.Volume)" << trade_bid.Volume);
    
    std::unique_lock<std::mutex> lck(trade_mutex);
    auto it = control_trade_map.find(ticker);
    if(it != control_trade_map.end())
    {
        it->second = getTimestamp();
    }
    lck.unlock();
    on_trade(&trade_ask);
    on_trade(&trade_bid);
}


void MDEngineCoinFlex::onBook(Document& json)
{

    std::string base = "";
    if(json.HasMember("base") && json["base"].IsInt()) {
        base = std::to_string(json["base"].GetInt());
    }
    std::string counter = "";
    if(json.HasMember("counter") && json["counter"].IsInt()) {
        counter = std::to_string(json["counter"].GetInt());
    }
    std::string base_counter = base + "_" +  counter;
    std::transform(base_counter.begin(), base_counter.end(), base_counter.begin(), ::toupper);
    std::string ticker = coinPairWhiteList.GetKeyByValue(base_counter);
    if(ticker.length() == 0) {
        KF_LOG_INFO(logger, "MDEngineCoinFlex::onBook: not in WhiteList , ignore it:" << "base : " << base << "  counter: " << counter);
        return;
    }

    std::map<int64_t, PriceAndVolume>::iterator it;
    KF_LOG_INFO(logger, "MDEngineCoinFlex::onBook:" << "(ticker) " << ticker);
    //make depth map
    if(json.HasMember("orders") && json["orders"].IsArray())
    {
        priceBook20Assembler.clearPriceBook(ticker);
        size_t len = json["orders"].Size();
        auto& vum = json["orders"];
        for(size_t i = 0 ; i < len; i++)
        {
            auto& object = vum[i];
            if(object.IsObject())
            {
                PriceAndVolume pv;
                pv.price = object["price"].GetInt64();
                if(object["quantity"].GetInt64() < 0) 
                {
                    int64_t price = object["price"].GetInt64() ;
                    price *= scale_offset;
                    uint64_t volume = abs(object["quantity"].GetInt64()) ;
                    pv.volume = volume;
                    volume *= scale_offset;
                    PriceId.insert(std::make_pair(object["id"].GetInt64(), pv));
                    priceBook20Assembler.UpdateAskPrice(ticker, price, volume, 1);
                }

                if(object["quantity"].GetInt64() > 0)
                {
                    int64_t price = object["price"].GetInt64() ;
                    price *= scale_offset;
                    uint64_t volume = abs(object["quantity"].GetInt64()) ;
                    pv.volume = volume;
                    volume *= scale_offset;
                    PriceId.insert(std::make_pair(object["id"].GetInt64(), pv));
                    priceBook20Assembler.UpdateBidPrice(ticker, price, volume, 1);
                }
            }            
        }
    }

    if(json.HasMember("notice") && json["notice"].IsString() && json.HasMember("price")  && json["price"].IsInt())
    {
        int64_t price = json["price"].GetInt64() ;
        int64_t volume , volume1 ;
        volume = abs(json["quantity"].GetInt64()) ;
        volume1 = volume ;
        volume *= scale_offset;
            
        if(strcmp(json["notice"].GetString(), "OrdersMatched") == 0 )
        {
            if(json.HasMember("ask") && json["ask"].IsInt())
            {
                int64_t asknum = json["ask"].GetInt64();
                it = PriceId.find(asknum);
                if(it != PriceId.end())
                {
                    price = it->second.price ;
                    price *= scale_offset;
                    priceBook20Assembler.EraseAskPrice(ticker, price, it->second.volume*scale_offset, 1);
                    it->second.volume = abs(json["ask_rem"].GetInt64());
                    if(it->second.volume > 0)
                        priceBook20Assembler.UpdateAskPrice(ticker, price, it->second.volume*scale_offset, 1);
                }
            }
            if(json.HasMember("bid") && json["bid"].IsInt())
            {
                int64_t bidnum = json["bid"].GetInt64();
                it = PriceId.find(bidnum);
                if(it != PriceId.end())
                {
                    price = it->second.price ;
                    price *= scale_offset;
                    priceBook20Assembler.EraseBidPrice(ticker, price, it->second.volume*scale_offset, 1);
                    it->second.volume = abs(json["bid_rem"].GetInt64());
                    if(it->second.volume > 0)
                        priceBook20Assembler.UpdateBidPrice(ticker, price, it->second.volume*scale_offset, 1);
                }
            }
        }

        //open 订单操作
        if(strcmp(json["notice"].GetString(), "OrderOpened") == 0 && json.HasMember("id") && json["id"].IsInt())
        {
            PriceAndVolume pv ;
            pv.price = price ;
            pv.volume = volume1 ;
            PriceId.insert(std::make_pair(json["id"].GetInt64(), pv));    //记录新加订单
            price *= scale_offset;
            if(json["quantity"].GetInt64() < 0)
                priceBook20Assembler.UpdateAskPrice(ticker, price, volume, 1); 
            if(json["quantity"].GetInt64() > 0)
                priceBook20Assembler.UpdateBidPrice(ticker, price, volume, 1);
        }

        //closed 订单操作 
        if(strcmp(json["notice"].GetString(), "OrderClosed") == 0 )
        {
            it = PriceId.find(json["id"].GetInt64());
            price *= scale_offset ;
            if(it != PriceId.end())
            {
                PriceId.erase(it);
                int quantiy = json["quantity"].GetInt64();
                if( quantiy == 0)
                    volume = 0 ;
                else if(quantiy <= 0)
                { 
                    priceBook20Assembler.EraseAskPrice(ticker, price, volume, 1);
                    KF_LOG_INFO(logger, "MDEngineCoinFlex::onBook: ##bidsPriceAndVolume volume == 0## price:" << price<<  "  volume:"<< volume);
                }
                else if(quantiy >= 0)
                {
                priceBook20Assembler.EraseBidPrice(ticker, price, volume, 1);
                KF_LOG_INFO(logger, "MDEngineCoinFlex::onBook: ##asksPriceAndVolume volume == 0## price:" << price<<  "  volume:"<< volume);
                }
            }
        }

        //modified 操作
        if(strcmp(json["notice"].GetString(), "OrderModified") == 0 && json.HasMember("id") && json["id"].IsInt())
        {
            int64_t idnum = json["id"].GetInt64();
            it = PriceId.find(idnum);
            if(it != PriceId.end())
            {   
                int tem = 0 ;
                if(it->second.volume  < volume1)
                {
                    volume = (volume1 - it->second.volume) * scale_offset;
                    tem = 1 ;
                }
                if(it->second.volume  > volume1)
                {
                    volume = (it->second.volume  - volume1) * scale_offset ;
                    tem = 2 ;
                }
                if(it->second.volume  == volume1 && price == it->second.price)
                {
                    tem = 1;
                    volume = 0;
                }
                //if(price != it->second.price)
                {
                    tem = 1 ;
                    int64_t volume_1 = it->second.volume * scale_offset ;
                    int64_t price_1 = it->second.price * scale_offset ;
                    if(json["quantity"].GetInt64() <= 0)
                    { 
                        priceBook20Assembler.EraseAskPrice(ticker, price_1, volume_1, 1) ;
                        KF_LOG_INFO(logger, "MDEngineCoinFlex::onBook: ##bidsPriceAndVolume volume == 0## price:" << price_1 <<  "  volume:"<< volume_1);
                    }
                    if(json["quantity"].GetInt64() >= 0)
                    {
                       priceBook20Assembler.EraseBidPrice(ticker, price_1, volume_1, 1) ;
                       KF_LOG_INFO(logger, "MDEngineCoinFlex::onBook: ##asksPriceAndVolume volume == 0## price:" << price_1 <<  "  volume:"<< volume_1);
                    }
                    it->second.price = price ;
                    volume = volume1 ;
                    volume *= scale_offset;
                }
                
                it->second.volume = volume1 ;
                price *= scale_offset ;
                if(json["quantity"].GetInt64() < 0)
                    priceBook20Assembler.UpdateAskPrice(ticker, price, volume, tem);
                if(json["quantity"].GetInt64() > 0)
                    priceBook20Assembler.UpdateBidPrice(ticker, price, volume, tem);
            }
        }
    }

    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));
    if(priceBook20Assembler.Assembler(ticker, md))
    {
        strcpy(md.ExchangeID, "CoinFlex");
        /*edited by zyy,starts here*/
        timer = getTimestamp();
        if(md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold)
        {
            cross_count = 0;
            KF_LOG_INFO(logger, "MDEngineCoinFlex::onBook: failed,level count < level threshold :"<<md.BidLevelCount<<" "<<md.AskLevelCount<<" "<<level_threshold);
            string errorMsg = "orderbook level below threshold";
            write_errormsg(112,errorMsg);
            on_price_book_update(&md);
        }
        else if(md.BidLevels[0].price <=0 || md.AskLevels[0].price <=0 || md.BidLevels[0].price > md.AskLevels[0].price)
        {
            string errorMsg = "orderbook crossed";
            cross_count += 1;
            if(cross_count >= cross_times){
                cross_count = 0;
                write_errormsg(113,errorMsg);
            }
        }
        /*edited by zyy ends here*/
        else
        {
            cross_count = 0;
            KF_LOG_INFO(logger, "MDEngineCoinFlex::onBook: on_price_book_update");
            on_price_book_update(&md);
            for(int i = 0; i < 20; i++)
            {
                KF_LOG_DEBUG_FMT(logger, "[%ld, %lu || %ld, %lu]",
                md.BidLevels[i].price,
                md.BidLevels[i].volume,
                md.AskLevels[i].price,
                md.AskLevels[i].volume);
            }
        }
    }
    else
    {
        cross_count = 0;
        KF_LOG_INFO(logger, "MDEngineCoinFlex::onBook: on_price_book_update error");
        for(int i = 0; i < 20; i++)
        {
            KF_LOG_DEBUG_FMT(logger, "[%ld, %lu || %ld, %lu]",
            md.BidLevels[i].price,
            md.BidLevels[i].volume,
            md.AskLevels[i].price,
            md.AskLevels[i].volume);
        }
    }
    std::unique_lock<std::mutex> lck1(book_mutex);
    auto it1 = control_book_map.find(ticker);
    if(it1 != control_book_map.end())
    {
        it1->second = getTimestamp();
    }
    lck1.unlock();

}

std::string MDEngineCoinFlex::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


//{ "event": "subscribe", "channel": "book",  "symbol": "tBTCUSD" }
std::string MDEngineCoinFlex::createBookJsonString(int num, int base, int quote)
{ 
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("tag");
    writer.Int(num);

    writer.Key("method");
    writer.String("WatchOrders");

    writer.Key("base");
    writer.Int(base);

    writer.Key("counter");
    writer.Int(quote);

    writer.Key("watch");
    writer.Bool(true);

    writer.EndObject();
    return s.GetString();
}

bool destroy_flag = false;
void MDEngineCoinFlex::loop()
{
    while(isRunning)
    {
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
       
        //KF_LOG_INFO(logger,"MDEngineCoinFlex::loop");
        if(context != nullptr && !destroy_flag)
        {
            //KF_LOG_INFO(logger,"context3="<<context);
            lws_service( context, rest_get_interval_ms );
        }
        else if(context != nullptr && destroy_flag && logged_in)
        {
            KF_LOG_INFO(logger, "MDEngineCoinFlex::loop lws_context_destroy");
            KF_LOG_INFO(logger,"context="<<context);
            lws_context_destroy(context);
            destroy_flag = false;
            //context = nullptr;
            //on_lws_connection_error(nullptr);
        }
        else{
            KF_LOG_INFO(logger,"context6="<<context<<" destroy_flag="<<destroy_flag);
        }
        //KF_LOG_INFO(logger,"context5="<<context<<" destroy_flag="<<destroy_flag);
    }
    KF_LOG_INFO(logger,"context4="<<context<<" destroy_flag="<<destroy_flag);
}

void MDEngineCoinFlex::reset_loop()
{
    while(isRunning)
    {
        //KF_LOG_INFO(logger,"MDEngineCoinFlex::reset_loop");
        time_t now_time = time(NULL);
        //KF_LOG_INFO(logger,"now_time="<<now_time<<"reset_time="<<reset_time<<"reset_interval_s="<<reset_interval_s);
        if(context != nullptr && now_time - reset_time > reset_interval_s && !destroy_flag)
        {
            destroy_flag = true;
            KF_LOG_INFO(logger, "MDEngineCoinFlex::reset_loop, time to reset");
            reset_time = time(NULL);
        }
        else
        {
            //KF_LOG_INFO(logger, "MDEngineCoinFlex::reset_loop, time count:" << now_time - reset_time);
        }
        
    }
}

/*edited by zyy,starts here*/
inline int64_t MDEngineCoinFlex::getTimestamp()
{   /*返回的是毫秒*/
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}
/*edited by zyy ends here*/

BOOST_PYTHON_MODULE(libcoinflexmd)
{
    using namespace boost::python;
    class_<MDEngineCoinFlex, boost::shared_ptr<MDEngineCoinFlex> >("Engine")
            .def(init<>())
            .def("init", &MDEngineCoinFlex::initialize)
            .def("start", &MDEngineCoinFlex::start)
            .def("stop", &MDEngineCoinFlex::stop)
            .def("logout", &MDEngineCoinFlex::logout)
            .def("wait_for_stop", &MDEngineCoinFlex::wait_for_stop);
}


