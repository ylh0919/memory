#include "MDEngineErisX.h"
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

static MDEngineErisX* global_md = nullptr;

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

MDEngineErisX::MDEngineErisX(): IMDEngine(SOURCE_ERISX)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.ErisX");
    timer = getTimestamp();/*edited by zyy*/
}

void MDEngineErisX::load(const json& j_config) 
{
    KF_LOG_ERROR(logger, "MDEngineErisX::load:");
    book_depth_count = j_config["book_depth_count"].get<int>();
    trade_count = j_config["trade_count"].get<int>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    /*edited by zyy,starts here*/
    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    /*edited by zyy ends here*/

    access_key = j_config["access_key"].get<string>();
    secret_key = j_config["secret_key"].get<string>();
    baseUrl = j_config["baseUrl"].get<string>();


    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    makeWebsocketSubscribeJsonString();
    debug_print(websocketSubscribeJsonString);

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineErisX::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_28sep18\": \"BTC-28SEP18\",");
        //KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
        KF_LOG_ERROR(logger, "},");
    }

    KF_LOG_INFO(logger, "MDEngineErisX::load:  book_depth_count: "
            << book_depth_count << " trade_count: " << trade_count << " rest_get_interval_ms: " << rest_get_interval_ms);
}

/*
std::string MDEngineErisX::getSignature(std::string event,std::string exchange_coinpair){
    struct timeval tv;  
    gettimeofday(&tv,NULL);  
    std::string nonce =  std::to_string(tv.tv_sec * 1000 + tv.tv_usec / 1000);
    std::string signatureString;
    if(event == "order_book"){
        KF_LOG_INFO(logger,"Get signature about order_book");
         signatureString = 
        "-=" + nonce
        + "&_ackey=" + access_key
        + "&_acsec=" + secret_key
        + "&_action=" + action
        + "&depth=" + std::to_string(book_depth_count)
        + "&event=" + event
        + "&instrument=" + exchange_coinpair;
    }
    else if(event == "trade"){
        KF_LOG_INFO(logger,"Get signature about trade");
        signatureString = 
        "-=" + nonce
        + "&_ackey=" + access_key
        + "&_acsec=" + secret_key
        + "&_action=" + action
        + "&event=" + event
        + "&instrument=" + exchange_coinpair;
    }
    else{
        KF_LOG_INFO(logger,"Type of signature is incorrett: "<< event);
        return "";
    }
    std::string binaryHash = hmac_sha256( secret_key.c_str(), signatureString.c_str() );
    return (
        access_key + "." + 
        nonce + "." +  
        binaryHash
    );
}*/

std::string MDEngineErisX::createJsonString(std::string signature,std::string exchange_coinpair,int type)
{
    std::string typestr = std::to_string(type);
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();

    writer.Key("correlation");
    writer.String(typestr.c_str());

    writer.Key("type");
    if(type==1){
        writer.String("MarketDataSubscribe");
    }
    else{
        writer.String("TopOfBookMarketDataSubscribe");
    }

    writer.Key("symbol");
    writer.String(exchange_coinpair.c_str());

    if(type==1){
        writer.Key("tradeOnly");
        //writer.String("True");
        writer.Bool(true);
    }
    else{
        writer.Key("topOfBookDepth");
        writer.String("20");
    }

    writer.EndObject(); 

    KF_LOG_DEBUG(logger,"MDEngineErisX::subscribe msg:"<<s.GetString());
    return s.GetString();  
     
}
void MDEngineErisX::makeWebsocketSubscribeJsonString()//创建请求
{
    int size=0;
    string pair[10];
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    while (map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);

        std::string jsonBookString = createJsonString("book",map_itr->second,0);
        websocketSubscribeJsonString.push_back(jsonBookString);

        std::string jsonTradeString = createJsonString("trade",map_itr->second,1);
        websocketSubscribeJsonString.push_back(jsonTradeString);

        map_itr++;
    }
}

void MDEngineErisX::debug_print(std::vector<std::string> &subJsonString)
{
    size_t count = subJsonString.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
    }
}

void MDEngineErisX::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineErisX::connect:");
    connected = true;
}

void MDEngineErisX::login(long timeout_nsec) {//连接到服务器
    KF_LOG_INFO(logger, "MDEngineErisX::login:");
    global_md = this;

    issub = false;
    issubok = false;

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
        KF_LOG_INFO(logger, "MDEngineErisX::login: context created.");
    }

    if (context == NULL) {
        KF_LOG_ERROR(logger, "MDEngineErisX::login: context is NULL. return");
        return;
    }

    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
    lws_set_log_level(logs, NULL);

    struct lws_client_connect_info ccinfo = {0};

    static std::string host  = baseUrl;
    static std::string path = "/";
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
    KF_LOG_INFO(logger, "MDEngineErisX::login: Connecting to " <<  ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "MDEngineErisX::login: wsi create error.");
        return;
    }
    KF_LOG_INFO(logger, "MDEngineErisX::login: wsi create success.");

    logged_in = true;
}

void MDEngineErisX::set_reader_thread()
{
    IMDEngine::set_reader_thread();

    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineErisX::loop, this)));
}

void MDEngineErisX::logout()
{
    KF_LOG_INFO(logger, "MDEngineErisX::logout:");
}

void MDEngineErisX::release_api()
{
    KF_LOG_INFO(logger, "MDEngineErisX::release_api:");
}

void MDEngineErisX::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEngineErisX::subscribeMarketData:");
}

std::string MDEngineErisX::makeSubscribetradeString()
{
    int64_t intime = getTimestampSeconds();
    std::string timestr = std::to_string(intime);
    KF_LOG_INFO(logger,"timestr"<<timestr);
    std::string strPayLoad;
        /*strPayLoad = R"({"sub":")" + access_key + R"(","iat":")" + timestr
            //+ R"(","query":")" + strQuery
            + R"("})";*/
        strPayLoad = R"({"sub":")" + access_key + R"(","iat":)" + timestr
            //+ R"(","query":")" + strQuery
            + R"(})";
        //strPayLoad = "{'sub': '2706b2c02c45db83e6f0bf5ea36c2f85g5e42bf7196996be93891361e14d1dc62', 'iat': 1570688147}";
    KF_LOG_INFO(logger,"strPayLoad:"<<strPayLoad);
    KF_LOG_INFO(logger,"secret_key:"<<secret_key);
    std::string strJWT = utils::crypto::jwt_hs256_create_erisx(strPayLoad,secret_key);
    std::string strAuthorization = "Bearer ";
    strAuthorization += strJWT;

    StringBuffer sbUpdate;
    Writer<StringBuffer> writer(sbUpdate);
    writer.StartObject();

    writer.Key("correlation");
    writer.String("2");

    writer.Key("type");
    writer.String("AuthenticationRequest");

    writer.Key("token");
    writer.String(strJWT.c_str());

    writer.EndObject(); 
    std::string strUpdate = sbUpdate.GetString();

    return strUpdate;
}

int MDEngineErisX::lws_write_subscribe(struct lws* conn)
{
    KF_LOG_INFO(logger, "MDEngineErisX::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    //std::string jsonString;
    if(!issub){
        KF_LOG_INFO(logger,"issub");
        std::string jsonString = makeSubscribetradeString();
        issub = true;
        unsigned char msg[1024];
        memset(&msg[LWS_PRE], 0, 1024-LWS_PRE);
        int length = jsonString.length();
        KF_LOG_INFO(logger, "TDEngineDeribit::lws_write_msg: " << jsonString.c_str() << " ,len = " << length);
        strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
        int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
        lws_callback_on_writable(conn);  
        return ret;        
    }
    else if(issubok){
        KF_LOG_INFO(logger,"issubok");
        if(websocketPendingSendMsg.size() > 0) {
            unsigned char msg[512];
            memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

            std::string jsonString = websocketPendingSendMsg[websocketPendingSendMsg.size() - 1];
            websocketPendingSendMsg.pop_back();
            KF_LOG_INFO(logger, "MDEngineErisX::lws_write_subscribe: websocketPendingSendMsg" << jsonString.c_str());
            int length = jsonString.length();

            strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
            int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

            if(websocketPendingSendMsg.size() > 0)
            {    //still has pending send data, emit a lws_callback_on_writable()
                lws_callback_on_writable( conn );
                KF_LOG_INFO(logger, "MDEngineErisX::lws_write_subscribe: (websocketPendingSendMsg,size)" << websocketPendingSendMsg.size());
            }
            return ret;
        }

        if(websocketSubscribeJsonString.size() == 0) return 0;//
        //sub depth
        if(subscribe_index >= websocketSubscribeJsonString.size())
        {
            //subscribe_index = 0;
            KF_LOG_INFO(logger, "MDEngineErisX::lws_write_subscribe: (none reset subscribe_index = 0, just return 0)");
    	    return 0;
        }
    

        unsigned char msg[512];
        memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

        std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

        KF_LOG_INFO(logger, "MDEngineErisX::lws_write_subscribe: " << jsonString.c_str() << " ,after ++, (subscribe_index)" << subscribe_index);
        int length = jsonString.length();

        strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
        int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

        if(subscribe_index < websocketSubscribeJsonString.size())
        {
            lws_callback_on_writable( conn );
            KF_LOG_INFO(logger, "MDEngineErisX::lws_write_subscribe: (subscribe_index < websocketSubscribeJsonString) call lws_callback_on_writable");
        }

        return ret;
    }
}

void MDEngineErisX::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "MDEngineErisX::on_lws_data: " << data<<",len:"<<len);
    Document json;
    json.Parse(data);

    if(json.HasParseError()) {
        KF_LOG_ERROR(logger, "MDEngineErisX::on_lws_data. parse json error: " << data);
        return;
    }
    
    if(json.HasMember("type")){
        std::string type = json["type"].GetString();
        if(type=="MarketDataIncrementalRefreshTrade"){
            onTrade(json);
        }
        else if(type=="TopOfBookMarketData"){
            onBook(json);
        }
        else if(type=="AuthenticationResult"){
            issubok = true;
        }
    }
   
    else KF_LOG_INFO(logger, "MDEngineErisX::on_lws_data: unknown data: " << parseJsonToString(json));
}


void MDEngineErisX::on_lws_connection_error(struct lws* conn) //liu
{
    KF_LOG_ERROR(logger, "MDEngineErisX::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineErisX::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;

    login(timeout_nsec);
}

int64_t MDEngineErisX::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

int64_t MDEngineErisX::getTimestampSeconds()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void MDEngineErisX::debug_print(std::vector<SubscribeChannel> &websocketSubscribeChannel)
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

void MDEngineErisX::onTrade(Document& json)
{
    KF_LOG_INFO(logger,"MDEngineErisX::onTrade");
    if(json.HasMember("symbol")){
    //std::string ticker = coinPairWhiteList.GetKeyByValue(split(json["channel"].GetString()));
        KF_LOG_INFO(logger, "inonTrade");

        Value &node=json["trades"];
        std::string ticker = json["symbol"].GetString();
        ticker = coinPairWhiteList.GetKeyByValue(ticker);
        KF_LOG_INFO(logger,"ticker"<<ticker);
        int len = node.GetArray().Size();
        
        double amount;
        uint64_t volume;

        for(int i=0;i<len;i++){

            KF_LOG_INFO(logger, "MDEngineErisX::onTrade: ticker:" << ticker);
	        LFL2TradeField trade;
            memset(&trade, 0, sizeof(trade));
            strcpy(trade.InstrumentID, ticker.c_str());
            strcpy(trade.ExchangeID, "erisx");
            std::string tradetime = node.GetArray()[i]["transactTime"].GetString();
            strcpy(trade.TradeTime,tradetime.c_str());  
            trade.Price = std::round(node.GetArray()[i]["price"].GetDouble() * scale_offset);
            amount = node.GetArray()[i]["size"].GetDouble();
            volume = std::round( amount * scale_offset);
            trade.Volume = volume;
	       	KF_LOG_INFO(logger, "MDEngineErisX::pricev");

            KF_LOG_INFO(logger, "MDEngineErisX::[onTrade]"  <<
                                                                " (Price)" << trade.Price <<
                                                                " (trade.Volume)" << trade.Volume);
            on_trade(&trade);
        }
    }
}

void MDEngineErisX::onBook(Document& json)
{
    std::string newstr = "NEW";
    std::string updatestr = "UPDATE";
    std::string deletestr = "DELETED";
    KF_LOG_INFO(logger,"MDEngineErisX::onBook");
    //if(json.HasMember("symbol")){
        //KF_LOG_INFO(logger,"inonbook");

        std::string ticker = json["symbol"].GetString();
        ticker = coinPairWhiteList.GetKeyByValue(ticker);
        KF_LOG_INFO(logger, "MDEngineErisX::onBook: (symbol) " << ticker);   

    	if(json.HasMember("bids")){
            int len1 = json["bids"].Size();
            Value &bidsnode = json["bids"];
            for (int i = 0; i < len1; i++) {
                int64_t price = std::round(bidsnode.GetArray()[i]["price"].GetDouble() * SCALE_OFFSET);
                uint64_t volume = std::round(bidsnode.GetArray()[i]["totalVolume"].GetDouble() * SCALE_OFFSET);
                if(volume == 0) {
                    priceBook20Assembler.EraseBidPrice(ticker, price);
                }
                else if(bidsnode.GetArray()[i]["action"].GetString()==newstr||bidsnode.GetArray()[i]["action"].GetString()==updatestr) {
                    priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
                }
            }
    	}
    	if(json.HasMember("offers")){
            int len2 = json["offers"].Size();
            Value &asksnode = json["offers"];
            for (int i = 0; i < len2; i++) {
                int64_t price = std::round(asksnode.GetArray()[i]["price"].GetDouble() * SCALE_OFFSET);
                uint64_t volume = std::round(asksnode.GetArray()[i]["totalVolume"].GetDouble() * SCALE_OFFSET);
                if(volume == 0) {
                    priceBook20Assembler.EraseAskPrice(ticker, price);
                }
                else if(asksnode.GetArray()[i]["action"].GetString()==newstr||asksnode.GetArray()[i]["action"].GetString()==updatestr){
                    priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
                }
    	}
            }                                
        LFPriceBook20Field md;
        memset(&md, 0, sizeof(md));
        if(priceBook20Assembler.Assembler(ticker, md)) {
            strcpy(md.ExchangeID, "erisx");
            strcpy(md.InstrumentID, ticker.c_str());
            KF_LOG_INFO(logger, "MDEngineDerbit::onbook: on_price_book_update");
            /*edited by zyy,starts here*/
            timer = getTimestamp();
            KF_LOG_INFO(logger, "MDEngineKumex::onBook: BidLevelCount="<<md.BidLevelCount<<",AskLevelCount="<<md.AskLevelCount<<",level_threshold="<<level_threshold);
            if(md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold)
            {
                string errorMsg = "orderbook level below threshold";
                write_errormsg(112,errorMsg);
                on_price_book_update(&md);
            }
            else if(md.BidLevels[0].price <=0 || md.AskLevels[0].price <=0 || md.BidLevels[0].price > md.AskLevels[0].price)
            {
                string errorMsg = "orderbook crossed";
                write_errormsg(113,errorMsg);
            }
            /*edited by zyy ends here*/
            else
            {
                on_price_book_update(&md);
            }
        }
    //}
}

std::string MDEngineErisX::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}

// std::string CoinPairWhiteList::GetKeyByValue(std::string exchange_coinpair)
//         {
//             std::unordered_map<std::string, std::string>::iterator map_itr;
//             map_itr = keyIsStrategyCoinpairWhiteList.begin();
//             while(map_itr != keyIsStrategyCoinpairWhiteList.end())
//             {
//                 if(exchange_coinpair == map_itr->second)
//                 {
// //                    std::cout << "[GetKeyByValue] found (strategy_coinpair) " <<
// //                              map_itr->first << " (exchange_coinpair) " << map_itr->second << std::endl;

//                     return map_itr->first;
//                 }
//                 map_itr++;
//             }
// //            std::cout << "[getWhiteListCoinpairFrom] not found (exchange_coinpair) " << exchange_coinpair << std::endl;
//             return "";
//         }



void MDEngineErisX::loop()
{
    while(isRunning)
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
        int n = lws_service( context, rest_get_interval_ms );
        std::cout << " 3.1415 loop() lws_service (n)" << n << std::endl;
    }
}

BOOST_PYTHON_MODULE(liberisxmd)
{
    using namespace boost::python;
    class_<MDEngineErisX, boost::shared_ptr<MDEngineErisX> >("Engine")
            .def(init<>())
            .def("init", &MDEngineErisX::initialize)
            .def("start", &MDEngineErisX::start)
            .def("stop", &MDEngineErisX::stop)
            .def("logout", &MDEngineErisX::logout)
            .def("wait_for_stop", &MDEngineErisX::wait_for_stop);
}
