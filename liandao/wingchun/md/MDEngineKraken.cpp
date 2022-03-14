#include "MDEngineKraken.h"
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
std::mutex update_mutex;
static MDEngineKraken* global_md = nullptr;

std::mutex book_mutex;
std::mutex trade_mutex;
std::mutex kline_mutex;

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

MDEngineKraken::MDEngineKraken(): IMDEngine(SOURCE_KRAKEN)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Kraken");
    timer = getTimestamp();/*edited by zyy*/
}

void MDEngineKraken::load(const json& j_config)
{
    book_depth_count = j_config["book_depth_count"].get<int>();
    trade_count = j_config["trade_count"].get<int>();
    if(j_config.find("snapshot_check_s") != j_config.end()) {
        snapshot_check_s = j_config["snapshot_check_s"].get<int>();
    }
//    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
//    KF_LOG_INFO(logger, "MDEngineKraken:: rest_get_interval_ms: " << rest_get_interval_ms);
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
    if(j_config.find("refresh_normal_check_kline_s") != j_config.end()) {
        refresh_normal_check_kline_s = j_config["refresh_normal_check_kline_s"].get<int>();
    }
    /*edited by zyy ends here*/

    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    makeWebsocketSubscribeJsonString();
    debug_print(websocketSubscribeJsonString);

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineKraken::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"tBTCUSDT\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
        KF_LOG_ERROR(logger, "},");
    }

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

        std::unique_lock<std::mutex> lck2(kline_mutex);
        control_kline_map.insert(make_pair(it->first, nowTime));
        lck2.unlock();
    }

    priceBook20Assembler.SetLevel(book_depth_count);

    KF_LOG_INFO(logger, "MDEngineKraken::load:  book_depth_count: "
            << book_depth_count << " trade_count: " << trade_count <<
            " baseUrl: " << baseUrl);
}

void MDEngineKraken::makeWebsocketSubscribeJsonString()
{
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    if(map_itr == coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()){
        return;
    }
    //map_itr++;
    while(map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) "<< map_itr->second);

        std::string jsonBookString = createBookJsonString(map_itr->second);
        websocketSubscribeJsonString.push_back(jsonBookString);

        std::string jsonTradeString = createTradeJsonString(map_itr->second);
        websocketSubscribeJsonString.push_back(jsonTradeString);

        std::string jsonOhlcString = createOhlcJsonString(map_itr->second);
        websocketSubscribeJsonString.push_back(jsonOhlcString);

        map_itr++;
    }
}

void MDEngineKraken::debug_print(std::vector<std::string> &subJsonString)
{
    size_t count = subJsonString.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
    }
}

void MDEngineKraken::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineKraken::connect:");
    connected = true;
}

void MDEngineKraken::login(long timeout_nsec) {
    KF_LOG_INFO(logger, "MDEngineKraken::login:");
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
    KF_LOG_INFO(logger, "MDEngineKraken::login: context created.");

    if (context == NULL) {
        KF_LOG_ERROR(logger, "MDEngineKraken::login: context is NULL. return");
        return;
    }

    struct lws_client_connect_info ccinfo;
    memset(&ccinfo, 0, sizeof(ccinfo));
    struct lws *wsi = NULL;
    std::string host = "ws.kraken.com";
    //std::string path = "/ws/2";
    std::string path = "/";

    ccinfo.context 	= context;
    ccinfo.address 	= host.c_str();
    ccinfo.port 	= 443;
    ccinfo.path 	= path.c_str();
    ccinfo.host 	= host.c_str();
    ccinfo.origin 	= host.c_str();
    ccinfo.protocol = protocols[0].name;
    //ccinfo.pwsi     = &wsi;
    ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    ccinfo.ietf_version_or_minus_one = -1;

    wsi = lws_client_connect_via_info(&ccinfo);
    KF_LOG_INFO(logger, "MDEngineKraken::login: Connecting to " <<  ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "MDEngineKraken::login: wsi create error.");
        return;
    }
    KF_LOG_INFO(logger, "MDEngineKraken::login: test login #7 " );
    KF_LOG_INFO(logger, "MDEngineKraken::login: wsi create success.");

    logged_in = true;
}

void MDEngineKraken::set_reader_thread()
{
    IMDEngine::set_reader_thread();

    ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKraken::loop, this)));
    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKraken::rest_loop, this)));
    check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKraken::check_loop, this)));
}

void MDEngineKraken::logout()
{
    KF_LOG_INFO(logger, "MDEngineKraken::logout:");
}

void MDEngineKraken::release_api()
{
    KF_LOG_INFO(logger, "MDEngineKraken::release_api:");
}

void MDEngineKraken::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEngineKraken::subscribeMarketData:");
}

int MDEngineKraken::lws_write_subscribe(struct lws* conn)
{
    //KF_LOG_INFO(logger, "MDEngineKraken::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    //有待发送的数据，先把待发送的发完，在继续订阅逻辑。  ping?
    if(websocketPendingSendMsg.size() > 0) {
        unsigned char msg[512];
        memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

        std::string jsonString = websocketPendingSendMsg[websocketPendingSendMsg.size() - 1];
        websocketPendingSendMsg.pop_back();
        KF_LOG_INFO(logger, "MDEngineKraken::lws_write_subscribe: websocketPendingSendMsg" << jsonString.c_str());
        int length = jsonString.length();

        strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
        int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

        if(websocketPendingSendMsg.size() > 0)
        {    //still has pending send data, emit a lws_callback_on_writable()
            lws_callback_on_writable( conn );
            KF_LOG_INFO(logger, "MDEngineKraken::lws_write_subscribe: (websocketPendingSendMsg,size)" << websocketPendingSendMsg.size());
        }
        return ret;
    }

    if(websocketSubscribeJsonString.size() == 0) return 0;
    //sub depth
    if(subscribe_index >= websocketSubscribeJsonString.size())
    {
        //subscribe_index = 0;
        //KF_LOG_INFO(logger, "MDEngineKraken::lws_write_subscribe: (none reset subscribe_index = 0, just return 0)");
	    return 0;
    }

    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

    std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

    KF_LOG_INFO(logger, "MDEngineKraken::lws_write_subscribe: " << jsonString.c_str() << " ,after ++, (subscribe_index)" << subscribe_index);
    int length = jsonString.length();

    strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

    if(subscribe_index < websocketSubscribeJsonString.size())
    {
        lws_callback_on_writable( conn );
        KF_LOG_INFO(logger, "MDEngineKraken::lws_write_subscribe: (subscribe_index < websocketSubscribeJsonString) call lws_callback_on_writable");
    }

    return ret;
}

void MDEngineKraken::onPing(struct lws* conn, Document& json)
{
    KF_LOG_INFO(logger, "MDEngineBitfinex::onPing: " << parseJsonToString(json));
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("event");
    writer.String("pong");

    writer.Key("reqid");
    writer.Int(json["reqid"].GetInt());

    writer.EndObject();

    std::string result = s.GetString();
    KF_LOG_INFO(logger, "MDEngineBitfinex::onPing: (Pong)" << result);
    //emit a callback
    lws_callback_on_writable( conn );
}

void MDEngineKraken::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: " << data);
    Document json;
    json.Parse(data);

    if(json.HasParseError()) {
        KF_LOG_ERROR(logger, "MDEngineKraken::on_lws_data. parse json error: " << data);
        return;
    }

    if(json.IsObject() && json.HasMember("event")) {
        if (strcmp(json["event"].GetString(), "info") == 0) {
            KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: is info");
            return;
        } else if (strcmp(json["event"].GetString(), "ping") == 0) {
            KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: is ping");
            onPing(conn, json);
            return;
        } else if (strcmp(json["event"].GetString(), "subscriptionStatus") == 0) {
            KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: is subscriptionStatus");
            if(!json.HasMember("errorMessage"))onSubscribed(json);
            return;
        }else if(strcmp(json["event"].GetString(), "systemStatus") == 0){
            KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: is systemStatus");
            return;
        }else if(strcmp(json["event"].GetString(), "heartbeat") == 0){
            KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: is heartbeat");
            return;
        }
    }

    //data
    if(json.IsArray()) {
        int chanId = json.GetArray()[0].GetInt();
        KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: (chanId) " << chanId);

        SubscribeChannel channel = findByChannelID( chanId );
        if (channel.channelId == 0) {
            KF_LOG_ERROR(logger, "MDEngineKraken::on_lws_data: EMPTY_CHANNEL (chanId) " << chanId);
        } else {
            if (channel.subType == book_channel) {
                KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: is book");
                onBook(channel, json);
            } else if (channel.subType == trade_channel) {
                KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: is trade");
                onTrade(channel, json);
            } else if (channel.subType == ohlc_channel) {
                KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: is ohlc");
                onOhlc(channel, json);
            } else {
                KF_LOG_INFO(logger, "MDEngineKraken::on_lws_data: unknown array data: " << data);
            }
        }
    }
}

void MDEngineKraken::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "MDEngineKraken::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineKraken::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;

    login(timeout_nsec);
}

int64_t MDEngineKraken::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

//kraken
/*{
  "channelID": 10001,
  "event": "subscriptionStatus",
  "status": "subscribed",
  "pair": "XBT/EUR",
  "subscription": {
    "name": "ticker"
  }

}*/
void MDEngineKraken::onSubscribed(Document& json)
{
    KF_LOG_INFO(logger, "MDEngineKraken::onSubscribed (json) "<<parseJsonToString(json));

    if(json.HasMember("channelID") && json.HasMember("pair") && json.HasMember("subscription")) {
        int chanId = json["channelID"].GetInt();
        std::string coinpair = json["pair"].GetString();
        rapidjson::Value data = json["subscription"].GetObject();
        string name = data["name"].GetString();
        KF_LOG_INFO(logger, "MDEngineKraken::onSubscribed (name) " << name );
        if(name == "trade") {
            KF_LOG_INFO(logger, "MDEngineKraken::onSubscribed (trade) ");
            SubscribeChannel newChannel;
            newChannel.channelId = chanId;
            newChannel.subType = trade_channel;
            newChannel.exchange_coinpair = coinpair;
            websocketSubscribeChannel.push_back(newChannel);
        }

        if(name == "book") {
            KF_LOG_INFO(logger, "MDEngineKraken::onSubscribed (book) ");
            SubscribeChannel newChannel;
            newChannel.channelId = chanId;
            newChannel.subType = book_channel;
            newChannel.exchange_coinpair = coinpair;
            websocketSubscribeChannel.push_back(newChannel);
        }

        if(name == "ohlc") {
            KF_LOG_INFO(logger, "MDEngineKraken::onSubscribed (ohlc) ");
            SubscribeChannel newChannel;
            newChannel.channelId = chanId;
            newChannel.subType = ohlc_channel;
            newChannel.exchange_coinpair = coinpair;
            websocketSubscribeChannel.push_back(newChannel);
        }
    }

    debug_print(websocketSubscribeChannel);
}

void MDEngineKraken::debug_print(std::vector<SubscribeChannel> &websocketSubscribeChannel)
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

SubscribeChannel MDEngineKraken::findByChannelID(int channelId)
{
    size_t count = websocketSubscribeChannel.size();

    for (size_t i = 0; i < count; i++)
    {
        if(channelId == websocketSubscribeChannel[i].channelId) {
            return websocketSubscribeChannel[i];
        }
    }
    return EMPTY_CHANNEL;
}

void MDEngineKraken::onTrade(SubscribeChannel &channel, Document& json)
{
    KF_LOG_INFO(logger, "MDEngineKraken::onTrade: (coinpair) " << channel.exchange_coinpair);

    std::string ticker = coinPairWhiteList.GetKeyByValue(channel.exchange_coinpair);
    if(ticker.length() == 0) {
        return;
    }

    int size = json.GetArray().Size();
    if(size < 2) return;

	for (int i = 0; i < size; i++) {
		if (json.GetArray()[i].IsArray()) {
			int len = json.GetArray()[i].Size();
			if (len == 0) return;

			if (json.GetArray()[i].GetArray()[0].IsArray())
			{
				/* snapshot
			 *
			 * */
				for (int j = 0; j < len; j++) {
					KF_LOG_INFO(logger, " (0)price" << json.GetArray()[i].GetArray()[j].GetArray()[0].GetString());
					KF_LOG_INFO(logger, " (1)volume" << json.GetArray()[i].GetArray()[j].GetArray()[1].GetString());
					KF_LOG_INFO(logger, " (2)time" << json.GetArray()[i].GetArray()[j].GetArray()[2].GetString());
					KF_LOG_INFO(logger, " (3)side" << json.GetArray()[i].GetArray()[j].GetArray()[3].GetString());
					KF_LOG_INFO(logger, " (4)orderType" << json.GetArray()[i].GetArray()[j].GetArray()[4].GetString());
					KF_LOG_INFO(logger, " (5)misc" << json.GetArray()[i].GetArray()[j].GetArray()[5].GetString());

					LFL2TradeField trade;
					memset(&trade, 0, sizeof(trade));
					strcpy(trade.InstrumentID, ticker.c_str());
					strcpy(trade.ExchangeID, "kraken");
                    trade.TimeStamp = std::round(std::stod(json.GetArray()[i].GetArray()[j].GetArray()[2].GetString()) * 1000000000);
                    string strTime  = timestamp_to_formatISO8601(trade.TimeStamp/1000000);
                    strncpy(trade.TradeTime, strTime.c_str(),sizeof(trade.TradeTime));
					trade.Price = std::round(std::stod(json.GetArray()[i].GetArray()[j].GetArray()[0].GetString()) * scale_offset);
					trade.Volume = std::round(std::stod(json.GetArray()[i].GetArray()[j].GetArray()[1].GetString()) * scale_offset);
					//trade.OrderBSFlag[0] = amount < 0 ? 'B' : 'S';
					if (strcmp(json.GetArray()[i].GetArray()[j].GetArray()[3].GetString(), "b") == 0) {
						trade.OrderBSFlag[0] = 'B';
					}
					else
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

					KF_LOG_INFO(logger, "MDEngineKraken::[onTrade] (ticker)" << ticker <<
						" (Price)" << trade.Price <<
						" (trade.Volume)" << trade.Volume);
					on_trade(&trade);

				}
			}

		}
	}	
}

void MDEngineKraken::onOhlc(SubscribeChannel &channel, Document& json)
{
    KF_LOG_INFO(logger, "MDEngineKraken::onOhlc ");
    std::string ticker = coinPairWhiteList.GetKeyByValue(channel.exchange_coinpair);
    if(ticker.length() == 0) {
        KF_LOG_INFO(logger, "received ohlc does not have valid data");
        return;
    }
    KF_LOG_INFO(logger, "MDEngineKraken::onOhlc: (ticker) " << ticker);
    int size = json.GetArray().Size();
    int last_element = size - 1;

    if(json.GetArray()[last_element].IsArray() )
    {
        LFBarMarketDataField market;
        memset(&market, 0, sizeof(market));
        strcpy(market.InstrumentID, ticker.c_str());
        strcpy(market.ExchangeID, "kraken");

        struct tm cur_tm, start_tm, end_tm;
		time_t now = time(0);
		cur_tm = *localtime(&now);
		strftime(market.TradingDay, 9, "%Y%m%d", &cur_tm);
		
        int64_t ttime = std::round(std::stod(json.GetArray()[last_element].GetArray()[0].GetString()) * 1000); 
        int64_t endtime = std::round(std::stod(json.GetArray()[last_element].GetArray()[1].GetString()) * 1000);
		int64_t nStartTime = endtime - ttime;
		int64_t nEndTime = endtime;
		market.StartUpdateMillisec = nStartTime;
		int ms = nStartTime % 1000;
		nStartTime/= 1000;
		start_tm = *localtime((time_t*)(&nStartTime));
		sprintf(market.StartUpdateTime,"%02d:%02d:%02d.%03d", start_tm.tm_hour,start_tm.tm_min,start_tm.tm_sec,ms);
		market.EndUpdateMillisec = nEndTime;
		ms = nEndTime%1000;
		nEndTime/= 1000;
		end_tm =  *localtime((time_t*)(&nEndTime));
		//strftime(market.EndUpdateTime,13, "%H:%M:%S", &end_tm);
		sprintf(market.EndUpdateTime,"%02d:%02d:%02d.%03d", end_tm.tm_hour,end_tm.tm_min,end_tm.tm_sec,ms);

		market.PeriodMillisec = 60000;
		market.Open = std::round(std::stod(json.GetArray()[last_element].GetArray()[2].GetString()) * scale_offset);
		market.Close = std::round(std::stod(json.GetArray()[last_element].GetArray()[5].GetString()) * scale_offset);
		market.Low = std::round(std::stod(json.GetArray()[last_element].GetArray()[4].GetString()) * scale_offset);
		market.High = std::round(std::stod(json.GetArray()[last_element].GetArray()[3].GetString()) * scale_offset);		
		market.Volume = std::round(std::stod(json.GetArray()[last_element].GetArray()[7].GetString()) * scale_offset);
		auto itPrice = priceBook.find(channel.exchange_coinpair);
		if(itPrice != priceBook.end())
		{
			market.BestBidPrice = itPrice->second.BidLevels[0].price;
			market.BestAskPrice = itPrice->second.AskLevels[0].price;
		}

        std::unique_lock<std::mutex> lck2(kline_mutex);
        auto it = control_kline_map.find(ticker);
        if(it != control_kline_map.end())
        {
            it->second = getTimestamp();
        }
        lck2.unlock();

		on_market_bar_data(&market);
    }
}
uint64_t MDEngineKraken::round_val(uint64_t val){
    uint64_t round=ceil(double(val)/1e5)*1e5;
    return round;
}
void MDEngineKraken::onBook(SubscribeChannel &channel, Document& json)
{
    KF_LOG_INFO(logger, "MDEngineKraken::onBook: (coinpair) " << channel.exchange_coinpair);

    std::string ticker = coinPairWhiteList.GetKeyByValue(channel.exchange_coinpair);
    if(ticker.length() == 0) {
        return;
    }

    KF_LOG_INFO(logger, "MDEngineKraken::onBook: (ticker) " << ticker);
    uint64_t sequence = 0;
    std::string str_sequece;
    int size = json.GetArray().Size();
    //kraken
	for (int i = 0; i < size; i++) {
		if (json.GetArray()[i].IsObject())
		{
			if (json.GetArray()[i].HasMember("as") &&
				json.GetArray()[i]["as"].IsArray() &&
				json.GetArray()[i].HasMember("bs") &&
				json.GetArray()[i]["bs"].IsArray())
			{
				/* snapshot
					[
						0,
						{
							"as": [
							[
								"5541.30000",
								"2.50700000",
								"1534614248.123678"
							],
							[
								"5541.80000",
								"0.33000000",
								"1534614098.345543"
							],
							[
								"5542.70000",
								"0.64700000",
								"1534614244.654432"
							]
							],
							"bs": [
							[
								"5541.20000",
								"1.52900000",
								"1534614248.765567"
							],
							[
								"5539.90000",
								"0.30000000",
								"1534614241.769870"
							],
							[
								"5539.50000",
								"5.00000000",
								"1534613831.243486"
							]
							]
						}
					]

				*/
                priceBook20Assembler.clearPriceBook(ticker);
				int len_as = json.GetArray()[i]["as"].GetArray().Size();
				int len_bs = json.GetArray()[i]["bs"].GetArray().Size();
				if (len_as == 0 || len_bs == 0) return;
				KF_LOG_INFO(logger, "MDEngineKraken::onBook: (len_as) " << len_as << " (len_bs) " << len_bs);
				for (int j = 0; j < len_as; j++)
				{
					int64_t price = std::round(std::stod(json.GetArray()[i]["as"].GetArray()[j].GetArray()[0].GetString()) * scale_offset);
					//KF_LOG_INFO(logger, "MDEngineKraken::onBook(as): price: " << price);
					uint64_t volume = std::round(std::stod(json.GetArray()[i]["as"].GetArray()[j].GetArray()[1].GetString()) * scale_offset);
                    volume = round_val(volume);
					//KF_LOG_INFO(logger, "MDEngineKraken::onBook(as): volume: " << volume);
                    str_sequece = json.GetArray()[i]["as"].GetArray()[j].GetArray()[2].GetString();
                    str_sequece = str_sequece.substr(0, 10);
                    uint64_t this_sequece = stoll(str_sequece);
                    if(this_sequece > sequence){
                        sequence = this_sequece;
                    }
					priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
				}
				for (int j = 0; j < len_bs; j++)
				{
					int64_t price = std::round(std::stod(json.GetArray()[i]["bs"].GetArray()[j].GetArray()[0].GetString()) * scale_offset);
					//KF_LOG_INFO(logger, "MDEngineKraken::onBook(bs): price: " << price);
					uint64_t volume = std::round(std::stod(json.GetArray()[i]["bs"].GetArray()[j].GetArray()[1].GetString()) * scale_offset);
                    volume = round_val(volume);
					//KF_LOG_INFO(logger, "MDEngineKraken::onBook(bs): volume: " << volume);
                    str_sequece = json.GetArray()[i]["bs"].GetArray()[j].GetArray()[2].GetString();
                    str_sequece = str_sequece.substr(0, 10);
                    uint64_t this_sequece = stoll(str_sequece);
                    if(this_sequece > sequence){
                        sequence = this_sequece;
                    }                    
					priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
				}
			}
			else if ((json.GetArray()[i].HasMember("a") && json.GetArray()[i]["a"].IsArray()) ||
				(json.GetArray()[i].HasMember("b") && json.GetArray()[i]["b"].IsArray()))
			{
				/*  update
					[
						1234,
						{"a": [
							[
							"5541.30000",
							"2.50700000",
							"1534614248.456738"
							],
							[
							"5542.50000",
							"0.40100000",
							"1534614248.456738"
							]
						]
						},
						{"b": [
							[
							"5541.30000",
							"0.00000000",
							"1534614335.345903"
							]
						]
						}
					]
				*/
				KF_LOG_INFO(logger, "MDEngineKraken::onBook: (a and b) ");
				if (json.GetArray()[i].HasMember("a"))
				{
					int len_a = json.GetArray()[i]["a"].GetArray().Size();
					KF_LOG_INFO(logger, "MDEngineKraken::onBook: (len_a) " << len_a);
					for (int j = 0; j < len_a; j++)
					{
						int64_t price = std::round(std::stod(json.GetArray()[i]["a"].GetArray()[j].GetArray()[0].GetString()) * scale_offset);
						//KF_LOG_INFO(logger, "MDEngineKraken::onBook(a): price: " << price);
						uint64_t volume = std::round(std::stod(json.GetArray()[i]["a"].GetArray()[j].GetArray()[1].GetString()) * scale_offset);
						volume = round_val(volume);
                        //KF_LOG_INFO(logger, "MDEngineKraken::onBook(a): volume: " << volume);
                        str_sequece = json.GetArray()[i]["a"].GetArray()[j].GetArray()[2].GetString();
                        str_sequece = str_sequece.substr(0, 10);
                        uint64_t this_sequece = stoll(str_sequece);
                        if(this_sequece > sequence){
                            sequence = this_sequece;
                        }                                          
						if (volume == 0)
						{
							priceBook20Assembler.EraseAskPrice(ticker, price);
						}
						else
						{
							priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
						}
					}
				}
				if (json.GetArray()[i].HasMember("b"))
				{
					int len_b = json.GetArray()[i]["b"].GetArray().Size();
					KF_LOG_INFO(logger, "MDEngineKraken::onBook: (len_b) " << len_b);
					for (int j = 0; j < len_b; j++)
					{
						int64_t price = std::round(std::stod(json.GetArray()[i]["b"].GetArray()[j].GetArray()[0].GetString()) * scale_offset);
						//KF_LOG_INFO(logger, "MDEngineKraken::onBook(b): price: " << price);
						uint64_t volume = std::round(std::stod(json.GetArray()[i]["b"].GetArray()[j].GetArray()[1].GetString()) * scale_offset);
						volume = round_val(volume);
                        //KF_LOG_INFO(logger, "MDEngineKraken::onBook(b): volume: " << volume);
                        str_sequece = json.GetArray()[i]["b"].GetArray()[j].GetArray()[2].GetString();
                        str_sequece = str_sequece.substr(0, 10);
                        uint64_t this_sequece = stoll(str_sequece);
                        if(this_sequece > sequence){
                            sequence = this_sequece;
                        }                                          
						if (volume == 0)
						{
							priceBook20Assembler.EraseBidPrice(ticker, price);
						}
						else
						{
							priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
						}
					}
				}
			}

		}
	}	
    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));
    md.UpdateMicroSecond = sequence;
    if(priceBook20Assembler.Assembler(ticker, md)) {     
        KF_LOG_INFO(logger, "MDEngineKraken::onDepth: on_price_book_update");
        strcpy(md.ExchangeID, "kraken");
        priceBook[ticker] = md;
        /*edited by zyy,starts here*/
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
            KF_LOG_INFO(logger, "MDEngineKraken::onBook: failed,level count < level threshold :"<<md.BidLevelCount<<" "<<md.AskLevelCount<<" "<<level_threshold);
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
            BookMsg bookmsg;
            bookmsg.book = md;
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
            on_price_book_update(&md);
        }
    }else{
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
    }

}

std::string MDEngineKraken::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}

/*
{
	"event":"subscribe",
	"pair":[
	"XBT/EUR",
	"XBT/USD"
	],
	"subscription":{
	"name":"book",
	"depth":25
	}
}
*/
std::string MDEngineKraken::createBookJsonString(std::string exchange_coinpair)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();

    writer.Key("event");
    writer.String("subscribe");

    writer.Key("pair");
    writer.StartArray();
    writer.String(exchange_coinpair.c_str());
    writer.EndArray();

    writer.Key("subscription");
    writer.StartObject();
    writer.Key("name");
    writer.String("book");
    writer.Key("depth");
    writer.Int(book_depth_count);
    writer.EndObject();

    writer.EndObject();
    return s.GetString();
}

/*
{
	"event":"subscribe",
	"pair":[
	"XBT/EUR",
	"XBT/USD"
	],
	"subscription":{
	"name":"trade"
	}
}
*/
std::string MDEngineKraken::createTradeJsonString(std::string exchange_coinpair)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    
    writer.Key("event");
    writer.String("subscribe");

    writer.Key("pair");
    writer.StartArray();
    writer.String(exchange_coinpair.c_str());
    writer.EndArray();

    writer.Key("subscription");
    writer.StartObject();
    writer.Key("name");
    writer.String("trade");
    writer.EndObject();

    writer.EndObject();
    return s.GetString();
}

std::string MDEngineKraken::createOhlcJsonString(std::string exchange_coinpair)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    
    writer.Key("event");
    writer.String("subscribe");

    writer.Key("pair");
    writer.StartArray();
    writer.String(exchange_coinpair.c_str());
    writer.EndArray();

    writer.Key("subscription");
    writer.StartObject();
    writer.Key("name");
    writer.String("ohlc");
    writer.EndObject();

    writer.EndObject();
    return s.GetString();
}
void MDEngineKraken::get_snapshot_via_rest()
{
    {
        std::unordered_map<std::string, std::string>::iterator map_itr;
        for(map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); map_itr++)
        {
            std::string url = "https://api.kraken.com/0/public/Depth?count=20&pair=";
            std::string pair = map_itr->second;
            int flag = pair.find("/");
            pair = pair.substr(0,flag) + pair.substr(flag+1,pair.length());
            url+=pair;
            const auto response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            
            Value node = d["result"].GetObject();
            for (Value::ConstMemberIterator itr = node.MemberBegin();itr != node.MemberEnd(); ++itr){
                //KF_LOG_INFO(logger,"ConstMemberIterator");
                std::string exchange = itr->name.GetString();
                char_64 pairname;
                strcpy(pairname, exchange.c_str());
                auto& tick = node[pairname];
                //KF_LOG_INFO(logger,"tick");
                LFPriceBook20Field priceBook {0};
                strcpy(priceBook.ExchangeID, "kucoin");
                strncpy(priceBook.InstrumentID, map_itr->first.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, map_itr->first.size()));
                int64_t this_sequece;
                uint64_t sequence = 0;
                if(tick.HasMember("bids") && tick["bids"].IsArray())
                {
                    auto& bids = tick["bids"];
                    int len = std::min((int)bids.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.BidLevels[i].price = std::round(stod(bids[i][0].GetString()) * scale_offset);
                        priceBook.BidLevels[i].volume = std::round(stod(bids[i][1].GetString()) * scale_offset);
                        this_sequece = bids[i][2].GetInt64();
                        if(this_sequece > sequence){
                            sequence = this_sequece;
                        }
                    }
                    priceBook.BidLevelCount = len;
                }
                if (tick.HasMember("asks") && tick["asks"].IsArray())
                {
                    auto& asks = tick["asks"];
                    int len = std::min((int)asks.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.AskLevels[i].price = std::round(stod(asks[i][0].GetString()) * scale_offset);
                        priceBook.AskLevels[i].volume = std::round(stod(asks[i][1].GetString()) * scale_offset);
                        this_sequece = asks[i][2].GetInt64();
                        if(this_sequece > sequence){
                            sequence = this_sequece;
                        }                        
                    }
                    priceBook.AskLevelCount = len;
                }
                priceBook.UpdateMicroSecond = sequence;
                BookMsg bookmsg;
                bookmsg.time = getTimestamp();
                bookmsg.book = priceBook;
                bookmsg.sequence = sequence;
                std::unique_lock<std::mutex> lck3(rest_book_mutex);
                rest_book_vec.push_back(bookmsg);    
                lck3.unlock();           

                on_price_book_update_from_rest(&priceBook);
            }
        }
    }
    

}

void MDEngineKraken::check_snapshot()
{
    std::vector<BookMsg>::iterator rest_it;
    std::unique_lock<std::mutex> lck3(rest_book_mutex);
    for(rest_it = rest_book_vec.begin();rest_it != rest_book_vec.end();){
        bool has_same_book = false;
        std::unique_lock<std::mutex> lck2(update_mutex);
        //ws 还没合成，跳过
        auto itr = has_bookupdate_map.find(string(rest_it->book.InstrumentID));
        if(itr == has_bookupdate_map.end()){
            KF_LOG_INFO(logger,"not start"<<string(rest_it->book.InstrumentID));
            //has_same_book = true;
            rest_it = rest_book_vec.erase(rest_it);
            continue;
        }else{
            if(itr->second > rest_it->sequence){
                KF_LOG_INFO(logger,"not start2"<<string(rest_it->book.InstrumentID));
                //has_same_book = true;
                rest_it = rest_book_vec.erase(rest_it);
                continue;
            }
        }
        lck2.unlock();

        int64_t now = getTimestamp();
        //bool has_error = false;
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
                        //has_error = true;
                        /*KF_LOG_INFO(logger, "2ws snapshot is not same as rest snapshot.sequence = "<< rest_it->sequence);
                        KF_LOG_INFO(logger,"ws_it:"<<ws_it->book.BidLevels[i].price<<" "<<ws_it->book.BidLevels[i].volume<<
                            " "<<ws_it->book.AskLevels[i].price<<" "<<ws_it->book.AskLevels[i].volume);
                        KF_LOG_INFO(logger,"rest_it:"<<rest_it->book.BidLevels[i].price<<" "<<rest_it->book.BidLevels[i].volume<<
                            " "<<rest_it->book.AskLevels[i].price<<" "<<rest_it->book.AskLevels[i].volume);
                        string errorMsg = "ws snapshot is not same as rest snapshot";
                        write_errormsg(116,errorMsg);*/ //ws存在时间戳同book不同                         
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
                    break;
                }
                //break;
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
            }/*else if(has_error){
                rest_it = rest_book_vec.erase(rest_it);
            }*/
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
void MDEngineKraken::rest_loop()
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
void MDEngineKraken::check_loop()
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
void MDEngineKraken::loop()
{
    while(isRunning)
    {
        /*edited by zyy,starts here*/
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

        std::unique_lock<std::mutex> lck2(kline_mutex);
        std::map<std::string,int64_t>::iterator it2;
        for(it2 = control_kline_map.begin(); it2 != control_kline_map.end(); it2++){
            if((now - it2->second) > refresh_normal_check_kline_s * 1000){
	            errorId = 116;
	            errorMsg = it2->first + " kline max refresh wait time exceeded";
	            KF_LOG_INFO(logger,"114"<<errorMsg); 
	            write_errormsg(errorId,errorMsg);
	            it2->second = now;           		
            }
        } 
        lck2.unlock();
        /*edited by zyy ends here*/
        int n = lws_service( context, rest_get_interval_ms );
        //std::cout << " 3.1415 loop() lws_service (n)" << n << std::endl;
        //KF_LOG_INFO(logger, "MDEngineKraken::loop:n=lws_service: "<<n);
    }
}

BOOST_PYTHON_MODULE(libkrakenmd)
{
    using namespace boost::python;
    class_<MDEngineKraken, boost::shared_ptr<MDEngineKraken> >("Engine")
            .def(init<>())
            .def("init", &MDEngineKraken::initialize)
            .def("start", &MDEngineKraken::start)
            .def("stop", &MDEngineKraken::stop)
            .def("logout", &MDEngineKraken::logout)
            .def("wait_for_stop", &MDEngineKraken::wait_for_stop);
}
