#include "MDEngineBitmex.h"

#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/LFDataStruct.h"

#include <writer.h>
#include <ctype.h>
#include <string.h>
#include <stringbuffer.h>
#include <document.h>
#include <cctype>
#include <algorithm>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <string>
#include <cpr/cpr.h>
#include <chrono>
#include <utility>
#include <atomic>
#include "../../utils/crypto/openssl_util.h"


using cpr::Get;
using cpr::Url;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;
using cpr::Body;
using cpr::Timeout;

using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using utils::crypto::base64_encode;
using std::string;
using std::to_string;
using std::stod;
using std::stoi;
using namespace std;

USING_WC_NAMESPACE

static MDEngineBitmex* md_instance = nullptr;
std::map<std::string,std::string> rootSymbol_name;
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;
std::mutex update_mutex;
std::mutex book_mutex;
//std::vector<std::string> symbol_name;

// pick the callback function to process data received from server
static int eventCallback(struct lws* conn, enum lws_callback_reasons reason, void* user, void* data, size_t len)
{
    switch(reason)
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            lws_callback_on_writable(conn);
            break;
        }
        case LWS_CALLBACK_PROTOCOL_INIT:
        {
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {
            if(md_instance)
            {
                md_instance->processData(conn, (const char*)data, len);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {
            if(md_instance)
            {
                md_instance->subscribeChannel(conn);
            }
            break;
        }
        case LWS_CALLBACK_CLOSED:
        {
            std::cout << "received signal LWS_CALLBACK_CLOSED" << std::endl;
            break;
        }
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
            std::cout << "received signal LWS_CALLBACK_CLIENT_CONNECTION_ERROR" << std::endl;
            if(md_instance)
            {
                md_instance->handleConnectionError(conn);
            }
            break;
        }
        default:
        {
            break;
        }
    }

    return 0;
}

static struct lws_protocols protocols[] =
{
    {
        "example_protocol",
        eventCallback,
        0,
        65536,
    },
    {NULL, NULL, 0, 0} /* terminator */
};

struct session_data {
    int fd;
};

MDEngineBitmex::MDEngineBitmex(): IMDEngine(SOURCE_BITMEX)
{
    logger = yijinjing::KfLog::getLogger("MDEngine.BitMEX");

    KF_LOG_INFO(logger, "initiating MDEngineBitmex");
}

void MDEngineBitmex::load(const json& config)//读json kungf
{
    KF_LOG_INFO(logger, "loading config from json");

    rest_get_interval_ms = config["rest_get_interval_ms"].get<int>();
    
    // add data_type argument
    // this code added by sinkinben
    std::vector<std::string> data_type = config["data_type"].get<std::vector<std::string>>();
    for (auto &x:data_type)
    {
        std::cout << x << std::endl;
        dataTypeMap[x] = true;
    }
    if (config.find("refresh_normal_check_book_s") != config.end())
    {
    	refresh_normal_check_book_s = config["refresh_normal_check_book_s"].get<int>();
    }
    if (config.find("level_threshold") != config.end())
    {
    	level_threshold = config["level_threshold"].get<int>();
    }
    if(config.find("snapshot_check_s") != config.end()) {
        snapshot_check_s = config["snapshot_check_s"].get<int>();
    }

    std::cout << refresh_normal_check_book_s << std::endl;
    std::cout << level_threshold << std::endl;
    priceBook.SetLeastLevel(level_threshold);
    //在白名单之前找到所有有效合约
    //首先获取全部可交易的期货rootSymbol
    whiteList.ReadWhiteLists(config, "whiteLists");//从配置信息读白名单whiteLists，通过whiteList使用返回的信息
    whiteList.Debug_print();
    //readWhiteLists(config);

    createSubscribeJsonStrings();//订阅
    debugPrint(subscribeJsonStrings);

    if(whiteList.Size() == 0)
    {
        KF_LOG_ERROR(logger, "subscribeCoinBaseQuote is empty please add whiteLists in kungfu.json like this");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }
}
/*void MDEngineBitmex::readWhiteLists(const json& j_config)
{
    KF_LOG_INFO(logger, "[readWhiteLists]");

    if(j_config.find("whiteLists") != j_config.end()) {
        KF_LOG_INFO(logger, "[readWhiteLists] found whiteLists");
        //has whiteLists
        json j_whiteLists = j_config["whiteLists"].get<json>();
        if(j_whiteLists.is_object())
        {
            for (json::iterator it = j_whiteLists.begin(); it != j_whiteLists.end(); ++it) {
                    std::string strategy_coinpair = it.key();
                    std::string exchange_coinpair = it.value();
                    KF_LOG_INFO(logger, "[readWhiteLists] (strategy_coinpair) " << strategy_coinpair << " (exchange_coinpair) " << exchange_coinpair);
                    keyIsStrategyCoinpairWhiteList.insert(std::pair<std::string, std::string>(strategy_coinpair, exchange_coinpair));
            }
        }
    }
}*/

void MDEngineBitmex::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "connecting to BitMEX");
    connected = true;
}

void MDEngineBitmex::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "initiating BitMEX websocket");

    md_instance = this;

    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
    lws_set_log_level(logs, NULL);

    KF_LOG_INFO(logger, "creating lws context");

    struct lws_context_creation_info creation_info;
    memset(&creation_info, 0, sizeof(creation_info));

    creation_info.port                     = CONTEXT_PORT_NO_LISTEN;
    creation_info.protocols                = protocols;
    creation_info.iface                    = NULL;
    creation_info.ssl_cert_filepath        = NULL;
    creation_info.ssl_private_key_filepath = NULL;
    creation_info.extensions               = NULL;
    creation_info.gid                      = -1;
    creation_info.uid                      = -1;
    creation_info.options                 |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    creation_info.fd_limit_per_thread      = 1024;
    creation_info.max_http_header_pool     = 1024;
    creation_info.ws_ping_pong_interval    = 10;
    creation_info.ka_time                  = 10;
    creation_info.ka_probes                = 10;
    creation_info.ka_interval              = 10;

    context = lws_create_context(&creation_info);

    KF_LOG_INFO(logger, "lws context created");

    KF_LOG_INFO(logger, "creating initial lws connection");

    struct lws_client_connect_info connect_info = {0};
    std::string host = "www.bitmex.com";
    std::string path = "/realtime";
    int port = 443;

    connect_info.context        = context;
    connect_info.address        = host.c_str();
    connect_info.port           = port;
    connect_info.path           = path.c_str();
    connect_info.host           = host.c_str();
    connect_info.origin         = host.c_str();
    connect_info.protocol       = protocols[0].name;
    connect_info.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

    struct lws* conn = lws_client_connect_via_info(&connect_info);
    
    KF_LOG_INFO(logger, "connecting to " << connect_info.host << ":" << connect_info.port << ":" << connect_info.path);

    if(!conn)
    {
        KF_LOG_INFO(logger, "error creating initial lws connection");
        return;
    }

    KF_LOG_INFO(logger, "done initiating and creating initial lws connection");

    logged_in = true;
    timer = getTimestamp();
}

int64_t MDEngineBitmex::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void MDEngineBitmex::logout()
{
    KF_LOG_INFO(logger, "logging out");
}

void MDEngineBitmex::release_api()
{
    KF_LOG_INFO(logger, "releasing API");
}

void MDEngineBitmex::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEngineBitmex::subscribeMarketData:");
}

void MDEngineBitmex::set_reader_thread()
{
    KF_LOG_INFO(logger, "setting reader thread");

    IMDEngine::set_reader_thread();
    read_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBitmex::enterEventLoop, this)));
    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBitmex::rest_loop, this)));
    check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBitmex::check_loop, this)));
}

std::string MDEngineBitmex::createOrderbookJsonString(std::string symbol)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("op");
    writer.String("subscribe");
    writer.Key("args");
    std::string str = "orderBookL2_25:" + symbol;
    writer.String(str.c_str());
    writer.EndObject();
    return buffer.GetString();
}

std::string MDEngineBitmex::createQuoteBinsJsonString(std::string symbol)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("op");
    writer.String("subscribe");
    writer.Key("args");
    std::string str = "quoteBin1m:" + symbol;
    writer.String(str.c_str());
    writer.EndObject();
    return buffer.GetString();
}

std::string MDEngineBitmex::createTradeJsonString(std::string symbol)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("op");
    writer.String("subscribe");
    writer.Key("args");
    std::string str = "trade:" + symbol;
    writer.String(str.c_str());
    writer.EndObject();
    return buffer.GetString();
}

std::string MDEngineBitmex::createTradeBinsJsonString(std::string symbol)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("op");
    writer.String("subscribe");
    writer.Key("args");
    std::string str = "tradeBin1m:" + symbol;
    writer.String(str.c_str());
    writer.EndObject();
    return buffer.GetString();
}

std::string MDEngineBitmex::createFundingJsonString()
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("op");
    writer.String("subscribe");
    writer.Key("args");
    std::string str = "funding";
    writer.String(str.c_str());
    writer.EndObject();
    return buffer.GetString();
}


void MDEngineBitmex::createSubscribeJsonStrings()
{
    string url1="https://www.bitmex.com/api/v1/instrument/active";
    const auto response = Get(Url{url1});
    rootSymbol_name.clear();
    Document d;
    d.Parse(response.text.c_str());
    if(!d.HasParseError()){
        for(int count1=0;count1<d.Size();count1++)
        {
            auto& temp=d.GetArray()[count1];
            rootSymbol_name.insert(std::make_pair(temp["symbol"].GetString(),temp["rootSymbol"].GetString()));
            //symbol_name.push_back(temp["symbol"].GetString());
            KF_LOG_DEBUG(logger, "rootsymbol: " << temp["rootSymbol"].GetString() << ", symbol: " << temp["symbol"].GetString());
            // std::cout<<instrument_name[i];
        }
    }
    if (dataTypeMap["funding"])
        subscribeJsonStrings.push_back(createFundingJsonString());
    std::unordered_map<std::string, std::string>::iterator iter = whiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    for( ; iter != whiteList.GetKeyIsStrategyCoinpairWhiteList().end(); iter++)
    {
        KF_LOG_DEBUG(logger, "creating subscribe json string for strategy symbol " << iter->first << ", market symbol " << iter->second);
        //iter->second means value && iter->first means key
        std::string pairkey=iter->first;
        std::string pairvalue=iter->second;
        if(pairkey.find("_futures")!=-1){
            KF_LOG_INFO(logger,"find the _futures");
            //int jsonsize = d["rootSymbol"].Size();
            for(auto rootSymbol_str:rootSymbol_name)
            {
                if(pairvalue == rootSymbol_str.second)
                {
                    KF_LOG_INFO(logger,"value == rootSymbol");
                    KF_LOG_DEBUG(logger,"key----->:"<<pairkey<<",value----->"<<pairvalue);
                    std::string submessage=rootSymbol_str.first;
                    if (dataTypeMap["tick"])
                        subscribeJsonStrings.push_back(createOrderbookJsonString(submessage));
                    if (dataTypeMap["trade"])
                        subscribeJsonStrings.push_back(createTradeJsonString(submessage));
                    if (dataTypeMap["bar"])
                        subscribeJsonStrings.push_back(createTradeBinsJsonString(submessage));
                }
            }
        }
        else{
            if (dataTypeMap["tick"])
                subscribeJsonStrings.push_back(createOrderbookJsonString(pairvalue));
            if (dataTypeMap["trade"])
                subscribeJsonStrings.push_back(createTradeJsonString(pairvalue));
            if (dataTypeMap["bar"])
                subscribeJsonStrings.push_back(createTradeBinsJsonString(pairvalue));
        }
    }
    
}


void MDEngineBitmex::debugPrint(std::vector<std::string> &jsons)
{
    KF_LOG_INFO(logger, "printing out all subscribe json strings");

    for(size_t count = 0; count < jsons.size(); count++)
    {
        KF_LOG_INFO(logger, "json string: " << jsons[count]);
    }
}

void MDEngineBitmex::subscribeChannel(struct lws* conn)
{
    if(num_subscribed >= subscribeJsonStrings.size())
    {
        return;
    }

    unsigned char message[512];
    memset(&message[LWS_PRE], 0, 512 - LWS_PRE);

    std::string json = subscribeJsonStrings[num_subscribed++];
    int length = json.length();
    strncpy((char *)message + LWS_PRE, json.c_str(), length);

    lws_write(conn, &message[LWS_PRE], length, LWS_WRITE_TEXT);

    KF_LOG_INFO(logger, "subscribed to " << json);

    if(num_subscribed < subscribeJsonStrings.size())
    {
        lws_callback_on_writable(conn);
        KF_LOG_INFO(logger, "there are more channels to subscribe to");
    }
    else
    {
        KF_LOG_INFO(logger, "there are no more channels to subscribe to");
    }
}
void MDEngineBitmex::get_snapshot_via_rest()
{
    //while (isRunning)
    //{
        std::unordered_map<std::string, std::string>::iterator iter = whiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
        for( ; iter != whiteList.GetKeyIsStrategyCoinpairWhiteList().end(); iter++)
        {
            std::string url = "https://www.bitmex.com/api/v1/orderBook/L2?depth=20&symbol=";
            url+=iter->second;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 

            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            std::string sell = "Sell";
            std::string buy = "Buy";
            uint64_t sequence;
            if(d.IsArray())
            {
                int len = d.Size();
                LFPriceBook20Field priceBook {0};
                strcpy(priceBook.ExchangeID, "bitmex");
                strncpy(priceBook.InstrumentID, iter->first.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, iter->first.size()));

                for(int i = 0; i < len; ++i)
                {
                    if(d.GetArray()[i]["side"].GetString() == buy){
                        priceBook.BidLevels[i-20].price = std::round(d.GetArray()[i]["price"].GetDouble() * scale_offset);
                        priceBook.BidLevels[i-20].volume = std::round(d.GetArray()[i]["size"].GetInt64() * scale_offset);
                        sequence = d.GetArray()[i]["id"].GetInt64();
                        priceBook.BidLevelCount += 1;
                    }else{
                        priceBook.AskLevels[19-i].price = std::round(d.GetArray()[i]["price"].GetDouble() * scale_offset);
                        priceBook.AskLevels[19-i].volume = std::round(d.GetArray()[i]["size"].GetInt64() * scale_offset);
                        sequence = d.GetArray()[i]["id"].GetInt64();
                        priceBook.AskLevelCount += 1;                   
                    }
                }

                BookMsg bookmsg;
                bookmsg.time = getTimestamp();
                bookmsg.book = priceBook;
                bookmsg.sequence = sequence;
                std::unique_lock<std::mutex> lck3(rest_book_mutex);
                rest_book_vec.push_back(bookmsg);    
                lck3.unlock();           

                on_price_book_update_from_rest(&priceBook);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(rest_get_interval_ms));
        }
    //}
}

void MDEngineBitmex::check_snapshot()
{
    std::vector<BookMsg>::iterator rest_it;
    std::unique_lock<std::mutex> lck3(rest_book_mutex);
    for(rest_it = rest_book_vec.begin();rest_it != rest_book_vec.end();){
        bool has_same_book = false;
        std::unique_lock<std::mutex> lck2(update_mutex);
        //ws 还没合成，跳过
        auto itr = has_bookupdate_map.find(string(rest_it->book.InstrumentID));
        if(itr == has_bookupdate_map.end()){
            KF_LOG_INFO(logger,"not start"<<string(rest_it->book.InstrumentID)<<" "<<itr->second);
            //has_same_book = true;
            rest_it = rest_book_vec.erase(rest_it);
            continue;
        }else{
            if(itr->second > rest_it->sequence){
                KF_LOG_INFO(logger,"not start2"<<string(rest_it->book.InstrumentID)<<" "<<itr->second);
                //has_same_book = true;
                rest_it = rest_book_vec.erase(rest_it);
                continue;
            }
        }
        lck2.unlock();

        int64_t now = getTimestamp();
        bool has_error = false;
        std::vector<BookMsg>::iterator ws_it;
        std::unique_lock<std::mutex> lck1(ws_book_mutex);
        for(ws_it = ws_book_vec.begin(); ws_it != ws_book_vec.end(); ){
            if(now - ws_it->time > 10000){
                KF_LOG_INFO(logger,"erase old ws");
                ws_it = ws_book_vec.erase(ws_it);
                continue;
            }

            //if(string(ws_it->book.InstrumentID) == string(rest_it->book.InstrumentID)){
            if(ws_it->sequence == rest_it->sequence && string(ws_it->book.InstrumentID) == string(rest_it->book.InstrumentID)){
                bool same_book = true;
                for(int i = 0; i < 20; i++ ){
                    if(ws_it->book.BidLevels[i].price != rest_it->book.BidLevels[i].price || ws_it->book.BidLevels[i].volume != rest_it->book.BidLevels[i].volume || 
                       ws_it->book.AskLevels[i].price != rest_it->book.AskLevels[i].price || ws_it->book.AskLevels[i].volume != rest_it->book.AskLevels[i].volume)
                    {
                        same_book = false;
                        has_error = true;
                        KF_LOG_INFO(logger, "2ws snapshot is not same as rest snapshot.sequence = "<< rest_it->sequence);
                        KF_LOG_INFO(logger,"ws_it:"<<ws_it->book.BidLevels[i].price<<" "<<ws_it->book.BidLevels[i].volume<<
                            " "<<ws_it->book.AskLevels[i].price<<" "<<ws_it->book.AskLevels[i].volume);
                        KF_LOG_INFO(logger,"rest_it:"<<rest_it->book.BidLevels[i].price<<" "<<rest_it->book.BidLevels[i].volume<<
                            " "<<rest_it->book.AskLevels[i].price<<" "<<rest_it->book.AskLevels[i].volume);
                        string errorMsg = "ws snapshot is not same as rest snapshot";
                        write_errormsg(116,errorMsg);                           
                        break;
                    }
                }
                if(same_book)
                {
                    has_same_book = true;
                    if(ws_it->time - rest_it->time > snapshot_check_s * 1000){
                        KF_LOG_INFO(logger, "ws snapshot is later than rest snapshot");
                        //rest_it = rest_book_vec.erase(rest_it);
                        string errorMsg = "ws snapshot is later than rest snapshot";
                        write_errormsg(115,errorMsg);
                    }
                    KF_LOG_INFO(logger, "same_book");
                    KF_LOG_INFO(logger,"ws_time="<<ws_it->time<<" rest_time="<<rest_it->time);
                    //break;
                }
                break;
            }
            ws_it++;
        }
        lck1.unlock();
        if(!has_same_book){
            if(now - rest_it->time > snapshot_check_s * 1000)
            {
                KF_LOG_INFO(logger, "ws snapshot is not same as rest snapshot.sequence = "<< rest_it->sequence);
                rest_it = rest_book_vec.erase(rest_it);
                string errorMsg = "ws snapshot is not same as rest snapshot";
                write_errormsg(116,errorMsg);   
            }else if(has_error){
                rest_it = rest_book_vec.erase(rest_it);
            }
            else{
                rest_it++;
            }                
        }else{
            KF_LOG_INFO(logger, "check good");
            rest_it = rest_book_vec.erase(rest_it);
        }        
    }
    lck3.unlock();
}

int64_t last_rest_time = 0;
void MDEngineBitmex::rest_loop()
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
void MDEngineBitmex::check_loop()
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
void MDEngineBitmex::enterEventLoop()
{
    KF_LOG_INFO(logger, "enter event loop");
    
    while(isRunning)
    {
        int64_t lag = getTimestamp() - timer;
        if ((lag / 1000) > refresh_normal_check_book_s)
        {
            int errorId = 114;
            string errorMsg = "orderbook max refresh wait time exceeded";           
           // LFPriceBook20Field md;
            //memset(&md, 0, sizeof(md));
           // md.Status = 3;
            //on_price_book_update(&md);
            KF_LOG_INFO(logger, "[quest2 fxw's edits v3]MDEngineBitfinex::update error status 3,lag is " << lag);
            write_errormsg(errorId,errorMsg);
            timer = getTimestamp();
        }
        lws_service(context, 500);
    }
    KF_LOG_INFO(logger, "exit event loop");
}

void MDEngineBitmex::handleConnectionError(struct lws* conn)
{
    KF_LOG_ERROR(logger, "having connection error");
    KF_LOG_ERROR(logger, "trying to login again");

    logged_in = false;
    num_subscribed = 0;

    priceBook.clearPriceBook();
    id_to_price.clear();
    received_partial.clear();

    long timeout_nsec = 0;
    login(timeout_nsec);
}

void MDEngineBitmex::processData(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "received data from BitMEX: " << data);

    Document json;
    json.Parse(data);

    if(!json.HasParseError() && json.IsObject() && json.HasMember("table") && json["table"].IsString())
    {
        if(strcmp(json["table"].GetString(), "orderBookL2_25") == 0)
        {
            KF_LOG_INFO(logger, "received data is orderBook");
            processOrderbookData(json);
        }
        else if(strcmp(json["table"].GetString(), "quoteBin1m") == 0)
        {
            KF_LOG_INFO(logger, "received data is 1-minute quote bins");
        }
        else if(strcmp(json["table"].GetString(), "trade") == 0)
        {
            KF_LOG_INFO(logger, "received data is live trade");
            processTradingData(json);
        }
        else if(strcmp(json["table"].GetString(), "tradeBin1m") == 0)
        {
            KF_LOG_INFO(logger, "received data is 1-minute trade bins");
            processTradeBinsData(json);
        }
        else if(strcmp(json["table"].GetString(), "funding") == 0)
        {
            KF_LOG_INFO(logger, "received data is funding data:" << data);
            processFundingData(json);
        }
        else
        {
            KF_LOG_INFO(logger, "received data is unknown");
        }
    }
}

void MDEngineBitmex::processOrderbookData(Document& json)
{
    KF_LOG_INFO(logger, "processing orderbook data");

    if(!json.HasMember("data") || !json["data"].IsArray() || json["data"].Size() == 0)
    {
        KF_LOG_INFO(logger, "received orderbook does not have valid data");
        return;
    }

    std::string symbol = json["data"].GetArray()[0]["symbol"].GetString();
    //std::string rootSymbol = json["data"].GetArray()[0]["rootSymbol"].GetString();
    std::string ticker = whiteList.GetKeyByValue(symbol);
    if(ticker.empty())
    {
        ticker = symbol;
    }
    KF_LOG_INFO(logger, "received orderbook symbol is " << symbol << " and ticker is " << ticker);

    std::string action = json["action"].GetString();
    KF_LOG_INFO(logger, "received orderbook action is a(n) " << action);

    uint64_t sequence;

    // upon subscription, an image of the existing data will be received through
    // a partial action, so you can get started and apply deltas after that
    if(action == "partial")
    {
        KF_LOG_INFO(logger,"partial");
        received_partial.insert(symbol);
        priceBook.clearPriceBook(ticker);
        auto& data = json["data"];
        for(int count = 0; count < data.Size(); count++)
        {
            auto& update = data.GetArray()[count];
            // each partial table data row contains symbol, id, side, size, and price field
            uint64_t id = update["id"].GetUint64();
            sequence = id;
            std::string side = update["side"].GetString();
            uint64_t size = std::round(update["size"].GetUint64() * scale_offset);
            int64_t price = std::round(update["price"].GetDouble() * scale_offset);
            // save id/price pair for future update/delete lookup
            id_to_price[id] = price;

            if(side == "Buy")
            {
                //KF_LOG_INFO(logger, "new bid: price " << price << " and amount " << size);
                priceBook.UpdateBidPrice(ticker, price, size);
            }
            else
            {
                //KF_LOG_INFO(logger, "new ask: price " << price << " and amount " << size);
                priceBook.UpdateAskPrice(ticker, price, size);
            }
        }
    }
    // other messages may be received before the partial action comes through
    // in that case drop any messages received until partial action has been received
    else if(received_partial.find(symbol) == received_partial.end())
    {
        KF_LOG_INFO(logger, "have not received first partial action for symbol " << symbol);
        KF_LOG_INFO(logger, "drop any messages received until partial action has been received");
    }
    else if(action == "update")
    {
        auto& data = json["data"];
        for(int count = 0; count < data.Size(); count++)
        {
            auto& update = data.GetArray()[count];
            // each update table data row contains symbol, id, side, and size field
            // price is looked up using id
            uint64_t id = update["id"].GetUint64();
            sequence = id;
            std::string side = update["side"].GetString();
            uint64_t size = std::round(update["size"].GetUint64() * scale_offset);
            int64_t price = id_to_price[id];

            if(side == "Buy")
            {
                //KF_LOG_INFO(logger, "updated bid: price " << price << " and amount " << size);
                priceBook.UpdateBidPrice(ticker, price, size);
            }
            else
            {
                //KF_LOG_INFO(logger, "updated ask: price " << price << " and amount " << size);
                priceBook.UpdateAskPrice(ticker, price, size);
            }
        }
    }
    else if(action == "insert")
    {
        auto& data = json["data"];
        for(int count = 0; count < data.Size(); count++)
        {
            auto& update = data.GetArray()[count];
            // each insert table data row contains symbol, id, side, size, and price field
            uint64_t id = update["id"].GetUint64();
            sequence = id;
            std::string side = update["side"].GetString();
            uint64_t size = std::round(update["size"].GetUint64() * scale_offset);
            int64_t price = std::round(update["price"].GetDouble() * scale_offset);
            // save id/price pair for future update/delete lookup
            id_to_price[id] = price;

            if(side == "Buy")
            {
                //KF_LOG_INFO(logger, "new bid: price " << price << " and amount " << size);
                priceBook.UpdateBidPrice(ticker, price, size);
            }
            else
            {
                //KF_LOG_INFO(logger, "new ask: price " << price << " and amount " << size);
                priceBook.UpdateAskPrice(ticker, price, size);
            }
        }
    }
    else if(action == "delete")
    {
        auto& data = json["data"];
        for(int count = 0; count < data.Size(); count++)
        {
            auto& update = data.GetArray()[count];
            // each delete table data row contains symbol, id, and side field
            // price is looked up using id
            uint64_t id = update["id"].GetUint64();
            sequence = id;
            std::string side = update["side"].GetString();
            int64_t price = id_to_price[id];
            id_to_price.erase(id);

            if(side == "Buy")
            {
                //KF_LOG_INFO(logger, "deleted bid: price " << price);
                priceBook.EraseBidPrice(ticker, price);
            }
            else
            {
                //KF_LOG_INFO(logger, "deleted ask: price " << price);
                priceBook.EraseAskPrice(ticker, price);
            }
        }
    }

    int errorId = 0;
    string errorMsg = "";
    LFPriceBook20Field update;
    memset(&update, 0, sizeof(update));
    if(priceBook.Assembler(ticker, update))
    {
        strcpy(update.ExchangeID, "bitmex");
        KF_LOG_INFO(logger, "sending out orderbook");
        if (priceBook.GetLeastLevel() > priceBook.GetNumberOfLevels_asks(ticker) ||
            priceBook.GetLeastLevel() > priceBook.GetNumberOfLevels_bids(ticker)
            )
        {
            update.Status = 1;
            errorId = 112;
            errorMsg = "orderbook level below threshold";
            /*need re-login*/
            KF_LOG_INFO(logger, "[FXW]MDEngineBitfinex on_price_book_update failed ,lose level,re-login....");
            on_price_book_update(&update);
            //GetSnapShotAndRtn(ticker);
            write_errormsg(errorId,errorMsg);

        }
        else if (priceBook.GetNumberOfLevels_asks(ticker) > 0 && priceBook.GetNumberOfLevels_bids(ticker) > 0 &&
            (-1 == priceBook.GetBestBidPrice(ticker) || -1 == priceBook.GetBestAskPrice(ticker) ||
            priceBook.GetBestBidPrice(ticker) >= priceBook.GetBestAskPrice(ticker)))
        {
            update.Status = 2;
            errorId = 113;
            errorMsg = "orderbook crossed";          
            /*need re-login*/
            KF_LOG_INFO(logger, "[FXW]MDEngineBitfinex on_price_book_update failed ,orderbook crossed,re-login....");
            on_price_book_update(&update);
            //GetSnapShotAndRtn(ticker);
            write_errormsg(errorId,errorMsg);
        }
        else
        {
            BookMsg bookmsg;
            bookmsg.book = update;
            bookmsg.time = getTimestamp();
            bookmsg.sequence = sequence;
            std::unique_lock<std::mutex> lck1(ws_book_mutex);
            ws_book_vec.push_back(bookmsg);
            lck1.unlock();
            std::unique_lock<std::mutex> lck2(update_mutex);
            auto it = has_bookupdate_map.find(ticker);
            if(it == has_bookupdate_map.end()){
                KF_LOG_INFO(logger,"insert"<<ticker);
                has_bookupdate_map.insert(make_pair(ticker, sequence));
            }
            lck2.unlock();
            std::unique_lock<std::mutex> lck4(book_mutex);
            book_map[ticker] = bookmsg;
            lck4.unlock();

            KF_LOG_INFO(logger,"ws sequence="<<sequence);
            update.Status = 0;
            on_price_book_update(&update);
            timer = getTimestamp();/*quest2 fxw's edits v3*/
            KF_LOG_INFO(logger, "[FXW successed]MDEngineBitfinex on_price_book_update successed");
        }
        //on_price_book_update(&update);
        //
        //LFFundingField fundingdata;
        //strcpy(fundingdata.InstrumentID, "test");
        //on_funding_update(&fundingdata);
    }
    else//*quest2 FXW's edits v5 
    {
            std::unique_lock<std::mutex> lck4(book_mutex);
            auto it = book_map.find(ticker);
            if(it != book_map.end()){
                BookMsg bookmsg;
                bookmsg.book = it->second.book;
                bookmsg.time = getTimestamp();
                bookmsg.sequence = sequence;
                std::unique_lock<std::mutex> lck1(ws_book_mutex);
                ws_book_vec.push_back(bookmsg);
                lck1.unlock(); 
            }
            lck4.unlock();

        timer = getTimestamp();
        //GetSnapShotAndRtn(ticker);
        KF_LOG_INFO(logger, "[FXW]MDEngineBitfinex on_price_book_update,priceBook20Assembler.Assembler(ticker, md) failed\n(ticker)" << ticker);
    }
}

void MDEngineBitmex::processTradingData(Document& json)
{
    KF_LOG_INFO(logger, "processing trade data");

    if(!json.HasMember("data") || !json["data"].IsArray() || json["data"].Size() == 0)
    {
        KF_LOG_INFO(logger, "received trade does not have valid data");
        return;
    }

    auto& data = json["data"];
    for(int count = 0; count < data.Size(); count++)
    {
        auto& update = data.GetArray()[count];
        std::string symbol = update["symbol"].GetString();
        std::string ticker = whiteList.GetKeyByValue(symbol);
        if(ticker.empty())
        {
            ticker = symbol;
        }
        KF_LOG_INFO(logger, "received trade symbol is " << symbol << " and ticker is " << ticker);
        LFL2TradeField trade;
        memset(&trade, 0, sizeof(trade));
        strcpy(trade.InstrumentID, ticker.c_str());
        strcpy(trade.ExchangeID, "bitmex");
        strcpy(trade.TradeTime,update["timestamp"].GetString());
        trade.TimeStamp = formatISO8601_to_timestamp(trade.TradeTime)*1000000; 
        int64_t price = std::round(update["price"].GetDouble() * scale_offset);
        uint64_t amount = std::round(update["size"].GetUint64() * scale_offset);
        std::string side = update["side"].GetString();

        trade.Price = price;
        trade.Volume = amount;
        trade.OrderBSFlag[0] = side == "Buy" ? 'B' : 'S';

        KF_LOG_INFO(logger, "ticker " << ticker << " traded at price " << trade.Price << " with volume " << trade.Volume << " as a " << side);

        on_trade(&trade);
    }
}

void MDEngineBitmex::processTradeBinsData(Document& json)
{
    KF_LOG_INFO(logger, "processing 1-min trade bins data");

    if(!json.HasMember("data") || !json["data"].IsArray() || json["data"].Size() == 0)
    {
        KF_LOG_INFO(logger, "received 1-min trade bin does not have valid data");
        return;
    }

    auto& data = json["data"];
    std::string symbol = data.GetArray()[0]["symbol"].GetString();
    std::string ticker = whiteList.GetKeyByValue(symbol);
    if(ticker.empty())
    {
        ticker = symbol;
    }
    KF_LOG_INFO(logger, "received 1-min trade bin symbol is " << symbol << " and ticker is " << ticker);

    for(int count = 0; count < data.Size(); count++)
    {
        auto& update = data.GetArray()[count];
        

        LFBarMarketDataField market;
        memset(&market, 0, sizeof(market));
        strcpy(market.InstrumentID, ticker.c_str());
        strcpy(market.ExchangeID, "bitmex");

        std::string timestamp = update["timestamp"].GetString();//'2019-01-06T03:32:00.000Z'
        if(timestamp.size() == strlen("2019-01-06T03:32:00.000Z"))
        {
            //sprintf(market.TradingDay, "%s%s%s", timestamp.substr(0,4).c_str(),timestamp.substr(5,7).c_str(),timestamp.substr(8,10).c_str());
            struct tm time;
            time.tm_year = std::stoi(timestamp.substr(0,4))-1900;
            time.tm_mon = std::stoi(timestamp.substr(5,7))-1;
            time.tm_mday = std::stoi(timestamp.substr(8,10));
            time.tm_hour = std::stoi(timestamp.substr(11,13));
            time.tm_min = std::stoi(timestamp.substr(14,16));
            time.tm_sec = std::stoi(timestamp.substr(17,19));
            
            time_t gm_time = timegm(&time);
            gm_time-=1;
            time = *gmtime(&gm_time);
            sprintf(market.EndUpdateTime,"%02d:%02d:%02d.999", time.tm_hour,time.tm_min,time.tm_sec);
            market.EndUpdateMillisec = gm_time *1000 + 999;
            gm_time-=59;
            time = *gmtime(&gm_time);
            sprintf(market.StartUpdateTime,"%02d:%02d:%02d.000", time.tm_hour,time.tm_min,time.tm_sec);
            market.StartUpdateMillisec =gm_time *1000;

            strftime(market.TradingDay, 9, "%Y%m%d", &time);
        }
        /*
        struct tm cur_tm, start_tm, end_tm;
        time_t now = time(0);
        cur_tm = *localtime(&now);
        strftime(market.TradingDay, 9, "%Y%m%d", &cur_tm);
	
        start_tm = cur_tm;
        start_tm.tm_min -= 1;
        market.StartUpdateMillisec = kungfu::yijinjing::parseTm(start_tm) / 1000000;
        strftime(market.StartUpdateTime, 13, "%H:%M:%S", &start_tm);

        end_tm = cur_tm;
        market.EndUpdateMillisec = kungfu::yijinjing::parseTm(end_tm) / 1000000;
        strftime(market.EndUpdateTime, 13, "%H:%M:%S", &end_tm);
        */
        market.PeriodMillisec = 60000;
        market.Open = std::round(update["open"].GetFloat() * scale_offset);;
        market.Close = std::round(update["close"].GetFloat() * scale_offset);;
        market.Low = std::round(update["low"].GetFloat() * scale_offset);;
        market.High = std::round(update["high"].GetFloat() * scale_offset);;
        market.BestBidPrice = priceBook.GetBestBidPrice(ticker);
        market.BestAskPrice = priceBook.GetBestAskPrice(ticker);
        market.Volume = std::round(update["volume"].GetUint64() * scale_offset);;

        on_market_bar_data(&market);
    }
}

int64_t getTimestampFromStr(std::string timestamp)
{
    std::string year = timestamp.substr(0,4);
    std::string month = timestamp.substr(5,2);
    std::string day = timestamp.substr(8,2);
    std::string hour = timestamp.substr(11,2);
    std::string min = timestamp.substr(14,2);
    std::string sec = timestamp.substr(17,2);
    std::string ms = timestamp.substr(20,3);
    struct tm localTM;
    localTM.tm_year = std::stoi(year)-1900;
    localTM.tm_mon = std::stoi(month)-1;
    localTM.tm_mday = std::stoi(day);
    localTM.tm_hour = std::stoi(hour);
    localTM.tm_min = std::stoi(min);
    localTM.tm_sec = std::stoi(sec);
    time_t time = mktime(&localTM);
    return time*1000+std::stoi(ms);
}

void MDEngineBitmex::processFundingData(Document& json)
{
    KF_LOG_INFO(logger, "processing funding data");

    if(!json.HasMember("data") || !json["data"].IsArray() || json["data"].Size() == 0)
    {
        KF_LOG_INFO(logger, "received funding does not have valid data");
        return;
    }

    auto& data = json["data"];
    

    for(int count = 0; count < data.Size(); count++)
    {
        auto& update = data.GetArray()[count];
        std::string symbol = update["symbol"].GetString();
        std::string ticker = whiteList.GetKeyByValue(symbol);
        if(ticker.empty())
        {
            ticker = symbol;
        }
        KF_LOG_INFO(logger, "received funding symbol is " << symbol << " and ticker is " << ticker);
        std::string timestamp = update["timestamp"].GetString();//"2019-03-19T07:52:44.318Z"
        

        LFFundingField fundingdata;
        memset(&fundingdata, 0, sizeof(fundingdata));
        strcpy(fundingdata.InstrumentID, ticker.c_str());
        strcpy(fundingdata.ExchangeID, "bitmex");

        //struct tm cur_tm;
        //time_t now = time(0);
        //cur_tm = *localtime(&now);
        fundingdata.TimeStamp = getTimestampFromStr(timestamp);//kungfu::yijinjing::parseTm(cur_tm) / 1000000;
        fundingdata.Rate = update["fundingRate"].GetDouble();
        fundingdata.RateDaily = update["fundingRateDaily"].GetDouble();
        on_funding_update(&fundingdata);
    }
}
BOOST_PYTHON_MODULE(libbitmexmd)
{
    using namespace boost::python;
    class_<MDEngineBitmex, boost::shared_ptr<MDEngineBitmex> >("Engine")
    .def(init<>())
    .def("init", &MDEngineBitmex::initialize)
    .def("start", &MDEngineBitmex::start)
    .def("stop", &MDEngineBitmex::stop)
    .def("logout", &MDEngineBitmex::logout)
    .def("wait_for_stop", &MDEngineBitmex::wait_for_stop);
}

