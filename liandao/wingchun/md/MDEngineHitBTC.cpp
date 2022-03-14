#include "MDEngineHitBTC.h"
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
#include <unistd.h>
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
std::atomic<int64_t> timestamp (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;
std::mutex update_mutex;
std::mutex book_mutex;
static MDEngineHitBTC* global_md = nullptr;

static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{

	switch( reason )
	{
		case LWS_CALLBACK_CLIENT_ESTABLISHED:
		{
			std::cout << "callback client established, reason = " << reason << std::endl;
			lws_callback_on_writable( wsi );
			break;
		}
		case LWS_CALLBACK_PROTOCOL_INIT:{
			std::cout << "init, reason = " << reason << std::endl;
			break;
		}
		case LWS_CALLBACK_CLIENT_RECEIVE:
		{
			std::cout << "on data, reason = " << reason << std::endl;
			if(global_md)
			{
				global_md->on_lws_data(wsi, (const char*)in, len);
			}
			break;
		}
		case LWS_CALLBACK_CLIENT_WRITEABLE:
		{
			std::cout << "writeable, reason = " << reason << std::endl;
			int ret = 0;
			if(global_md)
			{
				ret = global_md->lws_write_subscribe(wsi);
			}
			std::cout << "send depth result: " << ret << std::endl;
			break;
		}
		case LWS_CALLBACK_CLOSED:
		case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
		{
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

MDEngineHitBTC::MDEngineHitBTC(): IMDEngine(SOURCE_HITBTC)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.HitBTC");
}

void MDEngineHitBTC::load(const json& j_config)
{
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    KF_LOG_INFO(logger, "MDEngineHitBTC:: rest_get_interval_ms: " << rest_get_interval_ms);
	if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("snapshot_check_s") != j_config.end()) {
        snapshot_check_s = j_config["snapshot_check_s"].get<int>();
    }
    readWhiteLists(j_config);
	coinPairWhiteList_websocket.ReadWhiteLists(j_config, "whiteLists");
	coinPairWhiteList_websocket.Debug_print();

    debug_print(subscribeCoinBaseQuote);
    debug_print(keyIsStrategyCoinpairWhiteList);
    debug_print(websocketSubscribeJsonString);
    //display usage:
    if(keyIsStrategyCoinpairWhiteList.size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineHitBTC::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }
}

void MDEngineHitBTC::readWhiteLists(const json& j_config)
{
	KF_LOG_INFO(logger, "[readWhiteLists]");

	if(j_config.find("whiteLists") != j_config.end()) {
		KF_LOG_INFO(logger, "[readWhiteLists] found whiteLists");
		//has whiteLists
		json whiteLists = j_config["whiteLists"].get<json>();
		if(whiteLists.is_object())
		{
			for (json::iterator it = whiteLists.begin(); it != whiteLists.end(); ++it) {
				std::string strategy_coinpair = it.key();
				std::string exchange_coinpair = it.value();
				KF_LOG_INFO(logger, "[readWhiteLists] (strategy_coinpair) " << strategy_coinpair << " (exchange_coinpair) " << exchange_coinpair);
				keyIsStrategyCoinpairWhiteList.insert(std::pair<std::string, std::string>(strategy_coinpair, exchange_coinpair));
				//make subscribeCoinBaseQuote

				//coinmex MD must has base and quote, please see this->createDepthJsonString.
                SubscribeCoinBaseQuote baseQuote;
				split(it.key(), "_", baseQuote);

				if(baseQuote.base.length() > 0)
				{
					//get correct base_quote config
                    subscribeCoinBaseQuote.push_back(baseQuote);
                    //get ready websocket subscrube json strings
					std::string jsonOrderbookString = createOrderBookString(exchange_coinpair);
					websocketSubscribeJsonString.push_back(jsonOrderbookString);
					std::string jsonTradesString = createTradesString(exchange_coinpair);
					websocketSubscribeJsonString.push_back(jsonTradesString);
				}
			}
		}
	}
}

//example: btc_usdt
void MDEngineHitBTC::split(std::string str, std::string token, SubscribeCoinBaseQuote& sub)
{
	if (str.size() > 0) {
		size_t index = str.find(token);
		if (index != std::string::npos) {
			sub.base = str.substr(0, index);
			sub.quote = str.substr(index + token.size());
		}
		else {
			//not found, do nothing
		}
	}
}

int64_t MDEngineHitBTC::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}
std::string MDEngineHitBTC::getWhiteListCoinpairFrom(std::string md_coinpair)
{
    std::string ticker = md_coinpair;
    std::transform(ticker.begin(), ticker.end(), ticker.begin(), ::toupper);

    KF_LOG_INFO(logger, "[getWhiteListCoinpairFrom] find md_coinpair (md_coinpair) " << md_coinpair << " (toupper(ticker)) " << ticker);
    std::map<std::string, std::string>::iterator map_itr;
    map_itr = keyIsStrategyCoinpairWhiteList.begin();
    while(map_itr != keyIsStrategyCoinpairWhiteList.end()) {
		if(ticker == map_itr->second)
		{
            KF_LOG_INFO(logger, "[getWhiteListCoinpairFrom] found md_coinpair (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);
            return map_itr->first;
		}
        map_itr++;
    }
    KF_LOG_INFO(logger, "[getWhiteListCoinpairFrom] not found md_coinpair (md_coinpair) " << md_coinpair);
    return "";
}

void MDEngineHitBTC::debug_print(std::vector<SubscribeCoinBaseQuote> &sub)
{
    int count = sub.size();
    KF_LOG_INFO(logger, "[debug_print] SubscribeCoinBaseQuote (count) " << count);

	for (int i = 0; i < count;i++)
	{
		KF_LOG_INFO(logger, "[debug_print] SubscribeCoinBaseQuote (base) " << sub[i].base <<  " (quote) " << sub[i].quote);
	}
}

void MDEngineHitBTC::debug_print(std::map<std::string, std::string> &keyIsStrategyCoinpairWhiteList)
{
	std::map<std::string, std::string>::iterator map_itr;
	map_itr = keyIsStrategyCoinpairWhiteList.begin();
	while(map_itr != keyIsStrategyCoinpairWhiteList.end()) {
		KF_LOG_INFO(logger, "[debug_print] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (md_coinpair) "<< map_itr->second);
		map_itr++;
	}
}


void MDEngineHitBTC::debug_print(std::vector<std::string> &subJsonString)
{
    int count = subJsonString.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

    for (int i = 0; i < count;i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
    }
}

void MDEngineHitBTC::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineHitBTC::connect:");
    connected = true;
}

void MDEngineHitBTC::login(long timeout_nsec)
{
	KF_LOG_INFO(logger, "MDEngineHitBTC::login:");
	global_md = this;

	char inputURL[300] = "wss://api.hitbtc.com/api/2/ws";
	int inputPort = 443;
	const char *urlProtocol, *urlTempPath;
	char urlPath[300];
	int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;


	struct lws_context_creation_info ctxCreationInfo;
	struct lws_client_connect_info clientConnectInfo;
	struct lws *wsi = NULL;
	struct lws_protocols protocol;

	memset(&ctxCreationInfo, 0, sizeof(ctxCreationInfo));
	memset(&clientConnectInfo, 0, sizeof(clientConnectInfo));

	clientConnectInfo.port = 443;

	if (lws_parse_uri(inputURL, &urlProtocol, &clientConnectInfo.address, &clientConnectInfo.port, &urlTempPath))
	{
		KF_LOG_ERROR(logger, "MDEngineHitBTC::connect: Couldn't parse URL. Please check the URL and retry: " << inputURL);
		return;
	}

	// Fix up the urlPath by adding a / at the beginning, copy the temp path, and add a \0     at the end
	urlPath[0] = '/';
	strncpy(urlPath + 1, urlTempPath, sizeof(urlPath) - 2);
	urlPath[sizeof(urlPath) - 1] = '\0';
	clientConnectInfo.path = urlPath; // Set the info's path to the fixed up url path

	KF_LOG_INFO(logger, "MDEngineHitBTC::login:" << "urlProtocol=" << urlProtocol <<
												  "address=" << clientConnectInfo.address <<
												  "urlTempPath=" << urlTempPath <<
												  "urlPath=" << urlPath);

	ctxCreationInfo.port = CONTEXT_PORT_NO_LISTEN;
	ctxCreationInfo.iface = NULL;
	ctxCreationInfo.protocols = protocols;
	ctxCreationInfo.ssl_cert_filepath = NULL;
	ctxCreationInfo.ssl_private_key_filepath = NULL;
	ctxCreationInfo.extensions = NULL;
	ctxCreationInfo.gid = -1;
	ctxCreationInfo.uid = -1;
	ctxCreationInfo.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
	ctxCreationInfo.fd_limit_per_thread = 1024;
	ctxCreationInfo.max_http_header_pool = 1024;
	ctxCreationInfo.ws_ping_pong_interval=1;
	ctxCreationInfo.ka_time = 10;
	ctxCreationInfo.ka_probes = 10;
	ctxCreationInfo.ka_interval = 10;

	/*protocol.name  = protocols[PROTOCOL_TEST].name;
	protocol.callback = &ws_service_cb;
	protocol.per_session_data_size = sizeof(struct session_data);
	protocol.rx_buffer_size = 1024*1024;
	protocol.id = 0;
	protocol.user = NULL;
	*/
	context = lws_create_context(&ctxCreationInfo);
	KF_LOG_INFO(logger, "MDEngineHitBTC::login: context created.");


	if (context == NULL) {
		KF_LOG_ERROR(logger, "MDEngineHitBTC::login: context is NULL. return");
		return;
	}

	// Set up the client creation info
	clientConnectInfo.context = context;
	clientConnectInfo.port = 443;
	clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
	clientConnectInfo.host = clientConnectInfo.address;
	clientConnectInfo.origin = clientConnectInfo.address;
	clientConnectInfo.ietf_version_or_minus_one = -1;
	clientConnectInfo.protocol = protocols[PROTOCOL_TEST].name;
	clientConnectInfo.pwsi = &wsi;

	KF_LOG_INFO(logger, "MDEngineHitBTC::login:" << "Connecting to " << urlProtocol << ":" <<
												  clientConnectInfo.host << ":" <<
												  clientConnectInfo.port << ":" << urlPath);

	wsi = lws_client_connect_via_info(&clientConnectInfo);
	if (wsi == NULL) {
		KF_LOG_ERROR(logger, "MDEngineHitBTC::login: wsi create error.");
		return;
	}
	KF_LOG_INFO(logger, "MDEngineHitBTC::login: wsi create success.");
	logged_in = true;
}

void MDEngineHitBTC::set_reader_thread()
{
	IMDEngine::set_reader_thread();

	ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineHitBTC::loop, this)));
    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineHitBTC::rest_loop, this)));
    check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineHitBTC::check_loop, this)));
}

void MDEngineHitBTC::logout()
{
   KF_LOG_INFO(logger, "MDEngineHitBTC::logout:");
}

void MDEngineHitBTC::release_api()
{
   KF_LOG_INFO(logger, "MDEngineHitBTC::release_api:");
}

void MDEngineHitBTC::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
   KF_LOG_INFO(logger, "MDEngineHitBTC::subscribeMarketData:");
}

int MDEngineHitBTC::lws_write_subscribe(struct lws* conn)
{
	KF_LOG_INFO(logger, "MDEngineHitBTC::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    if(websocketSubscribeJsonString.size() == 0) return 0;
    //sub depth
    if(subscribe_index >= websocketSubscribeJsonString.size())
    {
        subscribe_index = 0;
		return 0;
    }

    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

    std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

    KF_LOG_INFO(logger, "MDEngineHitBTC::lws_write_subscribe: " << jsonString.c_str());
    int length = jsonString.length();

    strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

    if(subscribe_index < websocketSubscribeJsonString.size())
    {
        lws_callback_on_writable( conn );
    }

    return ret;
}

void MDEngineHitBTC::on_lws_data(struct lws* conn, const char* data, size_t len)
{
	KF_LOG_INFO(logger, "MDEngineHitBTC::on_lws_data: " << data);
    Document json;
	json.Parse(data);

	if (json.HasParseError()) {
		KF_LOG_ERROR(logger, "MDEngineHitBTC::on_lws_data. parse json error: " << data);
		return;
	}
	if (json.HasMember("method")) {
		if (strcmp(json["method"].GetString(), "snapshotOrderbook") == 0) {
			KF_LOG_INFO(logger, "MDEngineHitBTC::on_lws_data:snapshotOrderbook");
			onOrderBook(json);
		}
		else if (strcmp(json["method"].GetString(), "updateOrderbook") == 0) {
			KF_LOG_INFO(logger, "MDEngineHitBTC::on_lws_data:updateOrderbook");
			onOrderBook(json);
		}
		else if (strcmp(json["method"].GetString(), "updateTrades") == 0) {
			KF_LOG_INFO(logger, "MDEngineHitBTC::on_lws_data:updateTrades");
			onTrades(json);
		}
		else if (strcmp(json["method"].GetString(), "updateCandles") == 0) {
			KF_LOG_INFO(logger, "MDEngineHitBTC::on_lws_data:updateCandles");
			onCandles(json);
		}
		else {
			KF_LOG_INFO(logger, "MDEngineHitBTC::on_lws_data: unknown event: " << data);
		};
	}
}

void MDEngineHitBTC::onOrderBook(Document& json)
{
	KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook: " << parseJsonToString(json));
	std::string coinpair; int64_t price; uint64_t volume;
	if (json["params"].HasMember("symbol")) {
		coinpair = coinPairWhiteList_websocket.GetKeyByValue(json["params"]["symbol"].GetString());
		KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook: (symbol) " << json["params"]["symbol"].GetString());
		KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook: (coinpair) " << coinpair);
	}
	if (coinpair.length() == 0)
	{
		KF_LOG_ERROR(logger, "MDEngineHitBTC::onOrderBook:get coinpair error");
		return;
	}
    uint64_t sequence;
    std::string str_sequence;
    if(json["params"].HasMember("timestamp")){
        str_sequence = json["params"]["timestamp"].GetString();
        sequence = formatISO8601_to_timestamp(json["params"]["timestamp"].GetString());
    }
	if (json.HasMember("params") && json["params"].IsObject()) {
		//ask update
		if (json["params"].HasMember("ask") && json["params"]["ask"].IsArray()) {
			int len = json["params"]["ask"].Size();
			auto &asks = json["params"]["ask"];
			for (int i = 0; i < len; i++) {
				price = std::round(stod(asks.GetArray()[i]["price"].GetString()) * scale_offset);
				volume = std::round(stod(asks.GetArray()[i]["size"].GetString()) * scale_offset);
			
				if (volume == 0) {
					priceBook20Assembler.EraseAskPrice(coinpair, price);
					//KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook:EraseAskPrice():coinpair" << coinpair);
					//KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook:EraseAskPrice():price" << price);
				}
				else {
					priceBook20Assembler.UpdateAskPrice(coinpair, price, volume);
					//KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook:UpdateAskPrice():coinpair" << coinpair);
					//KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook:UpdateAskPrice():price" << price);
					//KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook:UpdateAskPrice():volume" << volume);
				}
			}
		}
		
		//bid update
		if (json["params"].HasMember("bid") && json["params"]["bid"].IsArray()) {
			int len = json["params"]["bid"].Size();
			auto &bid = json["params"]["bid"];
			for (int i = 0; i < len; i++) {
				int64_t price = std::round(stod(bid.GetArray()[i]["price"].GetString()) * scale_offset);
				uint64_t volume = std::round(stod(bid.GetArray()[i]["size"].GetString()) * scale_offset);
				if (volume == 0) {
					priceBook20Assembler.EraseBidPrice(coinpair, price);
					//KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook:EraseBidPrice():coinpair" << coinpair);
					//KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook:EraseBidPrice():price" << price);
				}
				else {
					priceBook20Assembler.UpdateBidPrice(coinpair, price, volume);
					//KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook:UpdateBidPrice():coinpair" << coinpair );
					//KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook:UpdateBidPrice():price" << price );
					//KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook:UpdateBidPrice():volume" << volume);
				}
			}
		}
	}

		//has any update
		LFPriceBook20Field md;
		memset(&md, 0, sizeof(md));
		if (priceBook20Assembler.Assembler(coinpair, md)) {
			md.UpdateMicroSecond = sequence;
			strcpy(md.ExchangeID, "HitBTC");
			KF_LOG_INFO(logger, "MDEngineHitBTC::onOrderBook: onOrderBook_update");
			if (priceBook20Assembler.GetLeastLevel() > priceBook20Assembler.GetNumberOfLevels_asks(coinpair) ||
				priceBook20Assembler.GetLeastLevel() > priceBook20Assembler.GetNumberOfLevels_bids(coinpair))
			{
				md.Status = 1;
				/*need re-login*/
				KF_LOG_DEBUG(logger, "MDEngineHiTBTC onOrderBook_update failed ,lose level,re-login....");
				 timestamp = getTimestamp();
				//if (md.AskLevelCount < level_threshold || md.BidLevelCount < level_threshold){
					string errorMsg = "orderbook level below threshold";
					write_errormsg(112,errorMsg);
					KF_LOG_INFO(logger, "MDEngineDerbit::onbook: " << errorMsg);
					on_price_book_update(&md);
				//}
				//else if (md.BidLevels[0].price > md.AskLevels[0].price || md.BidLevels[0].price <= 0 || md.AskLevels[0].price <= 0){
				//	string errorMsg = "orderbook crossed";
				//	write_errormsg(113,errorMsg);
				//	KF_LOG_INFO(logger, "MDEngineDerbit::onbook: " << errorMsg);
				//}
				//else
				//{
				//	on_price_book_update(&md);
				//}

			}
			else if ((-1 == priceBook20Assembler.GetBestBidPrice(coinpair)) || (-1 == priceBook20Assembler.GetBestAskPrice(coinpair)) ||
				priceBook20Assembler.GetBestBidPrice(coinpair) >= priceBook20Assembler.GetBestAskPrice(coinpair))
				{
				md.Status = 2;
				/*need re-login*/
				KF_LOG_DEBUG(logger, "MDEngineHitBTC onOrderBook_update failed ,orderbook crossed,re-login....");
				 timestamp = getTimestamp();
        //if (md.AskLevelCount < level_threshold || md.BidLevelCount < level_threshold){
       	//	string errorMsg = "orderbook level below threshold";
        //    write_errormsg(112,errorMsg);
        //    KF_LOG_INFO(logger, "MDEngineDerbit::onbook: " << errorMsg);
        //    on_price_book_update(&md);
       	//}
        //else if (md.BidLevels[0].price > md.AskLevels[0].price || md.BidLevels[0].price <= 0 || md.AskLevels[0].price <= 0){
        	string errorMsg = "orderbook crossed";
            write_errormsg(113,errorMsg);
        //     KF_LOG_INFO(logger, "MDEngineDerbit::onbook: " << errorMsg);
        //}
        //else
        //{
            on_price_book_update(&md);
        //}
				}
				else
				{
					md.Status = 0;
					 timestamp = getTimestamp();
        /*if (md.AskLevelCount < level_threshold || md.BidLevelCount < level_threshold){
       		string errorMsg = "orderbook level below threshold";
            write_errormsg(112,errorMsg);
            KF_LOG_INFO(logger, "MDEngineDerbit::onbook: " << errorMsg);
            on_price_book_update(&md);
       	}
        else if (md.BidLevels[0].price > md.AskLevels[0].price || md.BidLevels[0].price <= 0 || md.AskLevels[0].price <= 0){
        	string errorMsg = "orderbook crossed";
            write_errormsg(113,errorMsg);
             KF_LOG_INFO(logger, "MDEngineDerbit::onbook: " << errorMsg);
        }
        else
        {*/
	                BookMsg bookmsg;
	                bookmsg.book = md;
	                bookmsg.time = getTimestamp();
	                bookmsg.sequence = sequence;
	                bookmsg.str_sequence = str_sequence;
	                std::unique_lock<std::mutex> lck1(ws_book_mutex);
	                ws_book_vec.push_back(bookmsg);
	                lck1.unlock();
	                std::unique_lock<std::mutex> lck2(update_mutex);
	                auto it = has_bookupdate_map.find(coinpair);
	                if(it == has_bookupdate_map.end()){
	                    KF_LOG_INFO(logger,"insert"<<coinpair);
	                    has_bookupdate_map.insert(make_pair(coinpair, sequence));
	                }
	                lck2.unlock();
	                std::unique_lock<std::mutex> lck4(book_mutex);
	                book_map[coinpair] = bookmsg;
	                lck4.unlock();

	                KF_LOG_INFO(logger,"ws sequence="<<sequence<<"ws str_sequence"<<str_sequence);
	                on_price_book_update(&md);

            //on_price_book_update(&md);
        //}
					//timer = getTimestamp();
					KF_LOG_DEBUG(logger, "MDEngineHitBTC onOrderBook_update successed");
				}
		}
		else 
		{
            std::unique_lock<std::mutex> lck4(book_mutex);
            auto it = book_map.find(coinpair);
            if(it != book_map.end()){
                BookMsg bookmsg;
                bookmsg.book = it->second.book;
                bookmsg.time = getTimestamp();
                bookmsg.sequence = sequence;
                bookmsg.str_sequence = str_sequence;
                std::unique_lock<std::mutex> lck1(ws_book_mutex);
                ws_book_vec.push_back(bookmsg);
                lck1.unlock(); 
            }
            lck4.unlock();
			//timer = getTimestamp();
			//GetSnapShotAndRtn(ticker);
			KF_LOG_DEBUG(logger, "MDEngineHitBTC onOrderBook_update,priceBook20Assembler.Assembler(coinpair, md) failed\n(coinpair)" << coinpair);
		}
}

void MDEngineHitBTC::onCandles(Document& json) 
{

}

void MDEngineHitBTC::onTrades(Document& json)
{
	KF_LOG_INFO(logger, "MDEngineHitBTC::onTrades: " << parseJsonToString(json));
	int64_t price; uint64_t volume; std::string side; int64_t tradeID; std::string coinpair;
	if (json["params"].HasMember("symbol")) {
		coinpair = coinPairWhiteList_websocket.GetKeyByValue(json["params"]["symbol"].GetString());
		//KF_LOG_INFO(logger, "MDEngineBitfinex::onTrades: (symbol) " << json["params"]["symbol"].GetString());
		//KF_LOG_INFO(logger, "MDEngineBitfinex::onTrades: (coinpair) " << coinpair);
	}
	if (coinpair.length() == 0) 
	{
		KF_LOG_ERROR(logger, "MDEngineHitBTC::onTrades:get coinpair error");
		return;
	}
	if (json.HasMember("params") && json["params"].IsObject()) {
		//get trade data
		if (json["params"].HasMember("data") && json["params"]["data"].IsArray()) {
			int len = json["params"]["data"].Size();
			auto &data = json["params"]["data"];
			for (int i = 0; i < len; i++) {
				LFL2TradeField trade;
				memset(&trade, 0, sizeof(trade));
				strcpy(trade.InstrumentID,coinpair.c_str());
				//KF_LOG_INFO(logger, "MDEngineHitBTC::onTrades: InstrumentID " << coinpair);

				strcpy(trade.ExchangeID, "HitBTC");
				//KF_LOG_INFO(logger, "MDEngineHitBTC::onTrades: ExchangeID:HitBTC ");

				price = std::round(stod(data.GetArray()[i]["price"].GetString()) * scale_offset);
				//KF_LOG_INFO(logger, "MDEngineHitBTC::onTrades: price " << price);

				volume = std::round(stod(data.GetArray()[i]["quantity"].GetString()) * scale_offset);
				//KF_LOG_INFO(logger, "MDEngineHitBTC::onTrades: volume " << volume);
				
				tradeID = data.GetArray()[i]["id"].GetInt64();
				sprintf(trade.TradeID, "%lld", tradeID);
				//KF_LOG_INFO(logger, "MDEngineHitBTC::onTrades: TradeID " << tradeID);
				
				side = data.GetArray()[i]["side"].GetString();
				//KF_LOG_INFO(logger, "MDEngineHitBTC::onTrades: side " << side);
				trade.Price = price;
				trade.Volume = volume;
				if (side == "buy")
					strcpy(trade.OrderBSFlag, "B");
				else if (side == "sell")
					strcpy(trade.OrderBSFlag, "S");
				else KF_LOG_ERROR(logger, "MDEngineHItBTC::onTrade:get trade.OrderBSFlag error");
				strcpy(trade.TradeTime,data.GetArray()[i]["timestamp"].GetString());
				trade.TimeStamp = formatISO8601_to_timestamp(trade.TradeTime)*1000000; 
				KF_LOG_INFO(logger, "MDEngineHitBTC::[onTrade] (coinpair)" << coinpair <<
					" (Price)" << trade.Price <<
					" (trade.Volume)" << trade.Volume<<"(trade.OrderBSFlag):"<<trade.OrderBSFlag<<
					" (trade.TradeID): "<<trade.TradeID);
				on_trade(&trade);
			}
		}
		else KF_LOG_ERROR(logger, "MDEngineHitBTC::onTrade:get trade data error");
	}
	else KF_LOG_ERROR(logger, "MDEngineHitBTC::onTrade:json[parames].IsObject() error");
}
	
void MDEngineHitBTC::on_lws_connection_error(struct lws* conn)
{
	KF_LOG_ERROR(logger, "MDEngineHitBTC::on_lws_connection_error.");
	//market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineHitBTC::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
	clearPriceBook();
	//no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;

    login(timeout_nsec);
}

void MDEngineHitBTC::clearPriceBook()
{
    //clear price and volumes of tickers
    std::map<std::string, std::map<int64_t, uint64_t>*> ::iterator map_itr;

    map_itr = tickerAskPriceMap.begin();
    while(map_itr != tickerAskPriceMap.end()){
        map_itr->second->clear();
        map_itr++;
    }

    map_itr = tickerBidPriceMap.begin();
    while(map_itr != tickerBidPriceMap.end()){
        map_itr->second->clear();
        map_itr++;
    }
}

//{"jsonrpc":"2.0","method":"updateTrades","params":{"data":
// [{"id":400312606,"price":"0.031321","quantity":"0.004","side":"buy","timestamp":"2018-11-18T01:36:32.174Z"},
// {"id":400312607,"price":"0.031324","quantity":"0.014","side":"buy","timestamp":"2018-11-18T01:36:32.174Z"}],"symbol":"ETHBTC"}}



//sample HitBTC mesage
/*{"jsonrpc":"2.0","method":"updateOrderbook","params":{"ask":[{"price":"0.033772","size":"2.344"},
 * {"price":"0.033774","size":"0.000"},{"price":"0.033813","size":"10.145"},{"price":"0.033820","size":"0.000"},
 * {"price":"0.034017","size":"10.089"},{"price":"0.034031","size":"0.000"},{"price":"0.034051","size":"15.933"},
 * {"price":"0.034062","size":"16.765"},{"price":"0.034077","size":"23.386"},{"price":"0.034105","size":"0.000"},
 * {"price":"0.034149","size":"0.000"},{"price":"0.034160","size":"0.000"},{"price":"0.034165","size":"26.365"},
 * {"price":"0.034176","size":"20.446"},{"price":"0.034207","size":"0.000"},{"price":"0.034218","size":"28.204"},
 * {"price":"0.034257","size":"0.000"},{"price":"0.034278","size":"29.869"},{"price":"0.034285","size":"0.000"},
 * {"price":"0.034288","size":"22.409"},{"price":"0.034374","size":"0.000"},{"price":"0.034410","size":"29.976"},
 * {"price":"0.034438","size":"0.030"},{"price":"0.034451","size":"0.000"},{"price":"0.034512","size":"29.508"},
 *{"price":"0.034516","size":"30.699"},{"price":"0.034549","size":"0.200"},{"price":"0.034648","size":"34.985"},
 * {"price":"0.034740","size":"0.000"},{"price":"0.034845","size":"32.562"},{"price":"0.034847","size":"0.000"},
 * {"price":"0.034951","size":"41.828"},{"price":"0.034972","size":"0.000"},{"price":"0.035067","size":"0.000"}],
 * "bid":[{"price":"0.033705","size":"0.000"},{"price":"0.033703","size":"0.500"},
 * {"price":"0.033682","size":"0.000"},{"price":"0.033657","size":"8.953"},{"price":"0.033640","size":"12.389"},
 * {"price":"0.033635","size":"0.000"},{"price":"0.033623","size":"16.819"},{"price":"0.033615","size":"0.000"},
 * {"price":"0.030149","size":"0.000"},{"price":"0.029836","size":"121.917"}],"symbol":"ETHBTC","sequence":12701142}}*/

// {"base":"btc","biz":"spot","data":{"asks":[["6628.6245","0"],["6624.3958","0"]],
// "bids":[["6600.7846","0"],["6580.8484","0"]]},"quote":"usdt","type":"depth","zip":false}

std::string MDEngineHitBTC::parseJsonToString(const char* in)
{
	Document d;
	d.Parse(reinterpret_cast<const char*>(in));

	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	d.Accept(writer);

	return buffer.GetString();
}

std::string MDEngineHitBTC::parseJsonToString(Document& json) 
{
	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	json.Accept(writer);

	return buffer.GetString();
}

std::string MDEngineHitBTC::createOrderBookString(std::string coinpair)
{
   /*
   {
	"method":"subscribeOrderbook",
	"params":{
	"symbol":"ETHBTC"
	},
	"id":123
   }
   */
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("method");
	writer.String("subscribeOrderbook");
	writer.Key("params");
	writer.StartObject();
	writer.Key("symbol");
	writer.String(coinpair.c_str());
	writer.Key("limit");
	writer.String("20");
	writer.EndObject();
	writer.Key("id");
	writer.String("123");
	writer.EndObject();
	return s.GetString();
}

std::string MDEngineHitBTC::createTradesString(std::string coinpair)
{
    /*
	{
	"method":"subscribeTrades",
	"params":{
		"symbol":"ETHBTC",
		"limit":100
	},
	"id":123
	}
	*/
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("method");
    writer.String("subscribeTrades");
    writer.Key("params");
    writer.StartObject();
    writer.Key("symbol");
    writer.String(coinpair.c_str());
	writer.Key("limit");
	writer.String("100");
    writer.EndObject();
	writer.Key("id");
	writer.String("123");
	writer.EndObject();
    return s.GetString();
}

std::string MDEngineHitBTC::createTickersJsonString()
{
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("event");
	writer.String("subscribe");
	writer.Key("params");
	writer.StartObject();
	writer.Key("biz");
	writer.String("spot");
	writer.Key("type");
	writer.String("tickers");
	writer.Key("zip");
	writer.Bool(false);
	writer.EndObject();
	writer.EndObject();
	return s.GetString();
}

void MDEngineHitBTC::get_snapshot_via_rest()
{
    {
        for(auto& item : keyIsStrategyCoinpairWhiteList)
        {
            std::string url = "https://api.hitbtc.com/api/2/public/orderbook/";
            url+=item.second;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            Document d;
            d.Parse(response.text.c_str());
            if(d.IsObject())
            {
                LFPriceBook20Field md;
                strcpy(md.ExchangeID, "Bequant");
                strncpy(md.InstrumentID, item.first.c_str(),std::min(sizeof(md.InstrumentID)-1, item.first.size()));
                if(d.HasMember("ask") && d["ask"].IsArray())
                {
                    int len = d["ask"].Size();
                    len = std::min(len,20);
                    auto &asks = d["ask"];
                    for (int i = 0; i < len; i++) {
                        int64_t price = std::round(stod(asks.GetArray()[i]["price"].GetString()) * scale_offset);
                        uint64_t volume = std::round(stod(asks.GetArray()[i]["size"].GetString()) * scale_offset);
                        md.AskLevelCount = len;
                        md.AskLevels[i].price = price;
                        md.AskLevels[i].volume = volume;
                    }
                }
                if(d.HasMember("bid") && d["bid"].IsArray())
                {
                    int len = d["bid"].Size();
                    len = std::min(len,20);
                    auto &bid = d["bid"];
                    for (int i = 0; i < len; i++) {
                        int64_t price = std::round(stod(bid.GetArray()[i]["price"].GetString()) * scale_offset);
                        uint64_t volume = std::round(stod(bid.GetArray()[i]["size"].GetString()) * scale_offset);
                        md.BidLevelCount = len;
                        md.BidLevels[i].price = price;
                        md.BidLevels[i].volume = volume;
                    }
                }
                uint64_t sequence;
                std::string str_sequence;
                if(d.HasMember("timestamp"))
                {
                    str_sequence = d["timestamp"].GetString();
                    md.UpdateMicroSecond = formatISO8601_to_timestamp(d["timestamp"].GetString());
                    sequence = md.UpdateMicroSecond;
                }

                BookMsg bookmsg;
                bookmsg.time = getTimestamp();
                bookmsg.book = md;
                bookmsg.sequence = sequence;
                bookmsg.str_sequence = str_sequence;
                std::unique_lock<std::mutex> lck3(rest_book_mutex);
                rest_book_vec.push_back(bookmsg);    
                lck3.unlock();     

                on_price_book_update_from_rest(&md);
            }
        }
    }
    

}

void MDEngineHitBTC::check_snapshot()
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
                KF_LOG_INFO(logger,"sequence in");
                bool same_book = true;
                for(int i = 0; i < 20; i++ ){
                    if(ws_it->book.BidLevels[i].price != rest_it->book.BidLevels[i].price || ws_it->book.BidLevels[i].volume != rest_it->book.BidLevels[i].volume || 
                       ws_it->book.AskLevels[i].price != rest_it->book.AskLevels[i].price || ws_it->book.AskLevels[i].volume != rest_it->book.AskLevels[i].volume)
                    {
                        same_book = false;
                        has_error = true;
                        KF_LOG_INFO(logger, "2ws snapshot is not same as rest snapshot.sequence = "<< rest_it->sequence<<" str_sequence="<<rest_it->str_sequence);
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
                KF_LOG_INFO(logger, "ws snapshot is not same as rest snapshot.sequence = "<< rest_it->sequence<<" str_sequence"<<rest_it->str_sequence);
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
void MDEngineHitBTC::rest_loop()
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
void MDEngineHitBTC::check_loop()
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

void MDEngineHitBTC::loop()
{
		while(isRunning)
		{
			lws_service( context, rest_get_interval_ms );
			int errorId = 0;
		string errorMsg = "";
        /*quest3 edited by fxw starts here*/
        /*判断是否在设定时间内更新与否，*/
         //114错误
        int64_t now = getTimestamp();
        //KF_LOG_INFO(logger, "quest3: update check ");
        if ((now - timestamp) > refresh_normal_check_book_s * 1000)
        {
            	errorId = 114;
            	errorMsg = "orderbook max refresh wait time exceeded";
                KF_LOG_INFO(logger, "quest3:failed price book update");
                write_errormsg(errorId,errorMsg);
				timestamp = now;
        }
		}
}

BOOST_PYTHON_MODULE(libhitbtcmd)
{
    using namespace boost::python;
    class_<MDEngineHitBTC, boost::shared_ptr<MDEngineHitBTC> >("Engine")
    .def(init<>())
    .def("init", &MDEngineHitBTC::initialize)
    .def("start", &MDEngineHitBTC::start)
    .def("stop", &MDEngineHitBTC::stop)
    .def("logout", &MDEngineHitBTC::logout)
    .def("wait_for_stop", &MDEngineHitBTC::wait_for_stop);
}
