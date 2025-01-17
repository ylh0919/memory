#include "MDEngineBitfinex.h"
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

static MDEngineBitfinex* global_md = nullptr;

/*quest3 fxw v4 starts*/
int MDEngineBitfinex::GetSnapShotAndRtn(std::string ticker)//v1
{
	std::string symbol = coinPairWhiteList_rest.GetValueByKey(ticker);
	std::string requestPath = "/v1/book/";
	std::string body = "";
	string url = "https://api.bitfinex.com" + requestPath + symbol;//complete url
	KF_LOG_DEBUG(logger, "[quest2v4 fxw GetSnapShot]the url we ask :" << url);
	cpr::Response response = Get(
		Url{ url }, cpr::VerifySsl{ false },
		cpr::Body{ body },
		cpr::Timeout{ 10000 }
	);
	if (response.status_code >= 200 && response.status_code <= 299)
	{
		KF_LOG_DEBUG(logger, "[quest2v4 fxw GetSnapShot]request succeeded,the text is :" << response.text.c_str());
		Document d;
		d.Parse(response.text.c_str());
		LFPriceBook20Field md;
		strcpy(md.ExchangeID, "bitfinex");
		strcpy(md.InstrumentID, ticker.c_str());
		md.UpdateMicroSecond = 0;
		md.Status = 0;
		if (d.HasMember("bids"))
		{
			auto& bids = d["bids"];
			if (bids.IsArray() && bids.Size() > 0)
			{
				auto size = std::min((int)bids.Size(), 20);
				for (int i = 0; i < size; ++i)
				{
					md.BidLevels[i].price = stod(bids.GetArray()[i]["price"].GetString()) * scale_offset;
					md.BidLevels[i].volume = stod(bids.GetArray()[i]["amount"].GetString()) * scale_offset;
					KF_LOG_DEBUG(logger, "[quest2v4]bids price:" << md.BidLevels[i].price << "volume:" << md.BidLevels[i].volume);
				}
				md.BidLevelCount = size;
			}
		}
		if (d.HasMember("asks"))
		{
			auto& asks = d["asks"];

			if (asks.IsArray() && asks.Size() > 0)
			{
				auto size = std::min((int)asks.Size(), 20);

				for (int i = 0; i < size; ++i)
				{
					md.AskLevels[i].price = stod(asks.GetArray()[i]["price"].GetString()) * scale_offset;
					md.AskLevels[i].volume = stod(asks.GetArray()[i]["amount"].GetString()) * scale_offset;
					KF_LOG_DEBUG(logger, "[quest2v4]asks price:" << md.AskLevels[i].price << "volume:" << md.AskLevels[i].volume);
				}
				md.AskLevelCount = size;
			}
		}
		if (md.BidLevels[0].price > md.AskLevels[0].price)
			md.Status = 2;
		else md.Status = 0;
		timer = getTimestamp();
		on_price_book_update(&md);
		KF_LOG_DEBUG(logger, "[quest2v4 fxw GetSnapShot]snapshot price book update succeeded");
	}
	else
	{
		KF_LOG_DEBUG(logger, "[quest2 fxw GetSnapShot]request failed");
		KF_LOG_DEBUG(logger, "[quest2 fxw GetSnapShot](response.status_code)" << response.status_code << "(response.text)" << response.text.c_str());
	}
	return  0;
}
/*quest3 fxw v4 ends*/

static int ws_service_cb(struct lws* wsi, enum lws_callback_reasons reason, void* user, void* in, size_t len)
{

	switch (reason)
	{
	case LWS_CALLBACK_CLIENT_ESTABLISHED:
	{
		lws_callback_on_writable(wsi);
		break;
	}
	case LWS_CALLBACK_PROTOCOL_INIT:
	{
		break;
	}
	case LWS_CALLBACK_CLIENT_RECEIVE:
	{
		if (global_md)
		{
			global_md->on_lws_data(wsi, (const char*)in, len);
		}
		break;
	}
	case LWS_CALLBACK_CLIENT_CLOSED:
	{
		if (global_md) {
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
		if (global_md)
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
		if (global_md)
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

MDEngineBitfinex::MDEngineBitfinex() : IMDEngine(SOURCE_BITFINEX)
{
	logger = yijinjing::KfLog::getLogger("MdEngine.Bitfinex");
}

void MDEngineBitfinex::load(const json & j_config)
{
	book_depth_count = j_config["book_depth_count"].get<int>();
	priceBook20Assembler.SetLevel(book_depth_count);/*FXW's edits*/
	level_threshold = j_config["level_threshold"].get<int>();/*FXW's edits*/
	priceBook20Assembler.SetLeastLevel(level_threshold);/*FXW's edits*/
	trade_count = j_config["trade_count"].get<int>();
	rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
	KF_LOG_INFO(logger, "MDEngineBitfinex:: rest_get_interval_ms: " << rest_get_interval_ms);

	refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
	refresh_normal_check_trade_s = j_config["refresh_normal_check_trade_s"].get<int>();
	KF_LOG_INFO(logger, "MDEngineBitfinex:: refresh_normal_check_book_s: " << refresh_normal_check_book_s <<"\n"
						"MDEngineBitfinex:: refresh_normal_check_book_s: " << refresh_normal_check_trade_s);

	KF_LOG_INFO(logger, "MDEngineBitfinex:: there shall be \"whiteLists_websocket\":{} and \"whiteLists_rest\":{}");

	coinPairWhiteList_websocket.ReadWhiteLists(j_config, "whiteLists_websockets");
	coinPairWhiteList_websocket.Debug_print();

	coinPairWhiteList_rest.ReadWhiteLists(j_config, "whiteLists_rest");
	coinPairWhiteList_rest.Debug_print();

	makeWebsocketSubscribeJsonString();
	debug_print(websocketSubscribeJsonString);

	//display usage:
	if (coinPairWhiteList_websocket.Size() == 0) {
		KF_LOG_ERROR(logger, "MDEngineBitfinex::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists_websocket in kungfu.json like this :");
		KF_LOG_ERROR(logger, "\"whiteLists_websocket\":{");
		KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
		KF_LOG_ERROR(logger, "    \"btc_usdt\": \"tBTCUSDT\",");
		KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
		KF_LOG_ERROR(logger, "},");
	}
	if (coinPairWhiteList_rest.Size() == 0) {
		KF_LOG_ERROR(logger, "MDEngineBitfinex::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists_rest in kungfu.json like this :");
		KF_LOG_ERROR(logger, "\"whiteLists_rest\":{");
		KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
		KF_LOG_ERROR(logger, "    \"btc_usdt\": \"tBTCUSDT\",");
		KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
		KF_LOG_ERROR(logger, "},");
	}
	KF_LOG_INFO(logger, "MDEngineBitfinex::load:  book_depth_count: "
		<< book_depth_count << " trade_count: " << trade_count << " rest_get_interval_ms: " << rest_get_interval_ms);

	int64_t nowTime = getTimestamp();
	std::unordered_map<std::string, std::string>::iterator it;
	for(it = coinPairWhiteList_websocket.GetKeyIsStrategyCoinpairWhiteList().begin();it != coinPairWhiteList_websocket.GetKeyIsStrategyCoinpairWhiteList().end();it++)
	{
		std::unique_lock<std::mutex> lck(trade_mutex);
		control_trade_map.insert(std::make_pair(it->first, nowTime));
		lck.unlock();

		std::unique_lock<std::mutex> lck1(book_mutex);
		control_book_map.insert(std::make_pair(it->first, nowTime));
		lck1.unlock();
	}
}

void MDEngineBitfinex::makeWebsocketSubscribeJsonString()
{
	std::unordered_map<std::string, std::string>::iterator map_itr;
	map_itr = coinPairWhiteList_websocket.GetKeyIsStrategyCoinpairWhiteList().begin();
	while (map_itr != coinPairWhiteList_websocket.GetKeyIsStrategyCoinpairWhiteList().end()) {
		KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);

		std::string jsonBookString = createBookJsonString(map_itr->second);
		websocketSubscribeJsonString.push_back(jsonBookString);
		websocket.SendMessage(jsonBookString);

		std::string jsonTradeString = createTradeJsonString(map_itr->second);
		websocketSubscribeJsonString.push_back(jsonTradeString);
		websocket.SendMessage(jsonTradeString);
		map_itr++;
	}
}

void MDEngineBitfinex::debug_print(std::vector<std::string> & subJsonString)
{
	size_t count = subJsonString.size();
	KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

	for (size_t i = 0; i < count; i++)
	{
		KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
	}
}

void MDEngineBitfinex::connect(long timeout_nsec)
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::connect:");
	connected = true;
}

void MDEngineBitfinex::login(long timeout_nsec) 
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::login:");
	websocket.RegisterCallBack(this);
	logged_in = websocket.Connect("api.bitfinex.com/ws/2");
	timer = getTimestamp();/*quest2 fxw's edits v3*/
	KF_LOG_INFO(logger, "MDEngineBitfinex::login " << (logged_in ? "Success" : "Failed"));
}

void MDEngineBitfinex::set_reader_thread()
{
	IMDEngine::set_reader_thread();

	rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBitfinex::loop, this)));
}

void MDEngineBitfinex::logout()
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::logout:");
}

void MDEngineBitfinex::release_api()
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::release_api:");
}

void MDEngineBitfinex::subscribeMarketData(const vector<string> & instruments, const vector<string> & markets)
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::subscribeMarketData:");
}

int MDEngineBitfinex::lws_write_subscribe(struct lws* conn)
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::lws_write_subscribe: (subscribe_index)" << subscribe_index);

	//有待发送的数据，先把待发送的发完，在继续订阅逻辑。  ping?
	if (websocketPendingSendMsg.size() > 0) {
		unsigned char msg[512];
		memset(&msg[LWS_PRE], 0, 512 - LWS_PRE);

		std::string jsonString = websocketPendingSendMsg[websocketPendingSendMsg.size() - 1];
		websocketPendingSendMsg.pop_back();
		KF_LOG_INFO(logger, "MDEngineBitfinex::lws_write_subscribe: websocketPendingSendMsg" << jsonString.c_str());
		int length = jsonString.length();

		strncpy((char*)msg + LWS_PRE, jsonString.c_str(), length);
		int ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);

		if (websocketPendingSendMsg.size() > 0)
		{    //still has pending send data, emit a lws_callback_on_writable()
			lws_callback_on_writable(conn);
			KF_LOG_INFO(logger, "MDEngineBitfinex::lws_write_subscribe: (websocketPendingSendMsg,size)" << websocketPendingSendMsg.size());
		}
		return ret;
	}

	if (websocketSubscribeJsonString.size() == 0) return 0;
	//sub depth
	if (subscribe_index >= websocketSubscribeJsonString.size())
	{
		//subscribe_index = 0;
		KF_LOG_INFO(logger, "MDEngineBitfinex::lws_write_subscribe: (none reset subscribe_index = 0, just return 0)");
		return 0;
	}

	unsigned char msg[512];
	memset(&msg[LWS_PRE], 0, 512 - LWS_PRE);

	std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

	KF_LOG_INFO(logger, "MDEngineBitfinex::lws_write_subscribe: " << jsonString.c_str() << " ,after ++, (subscribe_index)" << subscribe_index);
	int length = jsonString.length();

	strncpy((char*)msg + LWS_PRE, jsonString.c_str(), length);
	int ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);

	if (subscribe_index < websocketSubscribeJsonString.size())
	{
		lws_callback_on_writable(conn);
		KF_LOG_INFO(logger, "MDEngineBitfinex::lws_write_subscribe: (subscribe_index < websocketSubscribeJsonString) call lws_callback_on_writable");
	}

	return ret;
}

void MDEngineBitfinex::OnConnected(const common::CWebsocket* instance)
{
	is_ws_disconnectd = false;
	makeWebsocketSubscribeJsonString();
}

void MDEngineBitfinex::OnReceivedMessage(const common::CWebsocket* instance,const std::string& msg)
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: " << msg);
	Document json;
	json.Parse(msg.c_str());

	if (json.HasParseError()) {
		KF_LOG_ERROR(logger, "MDEngineBitfinex::on_lws_data. parse json error.");
		return;
	}

	if (json.IsObject() && json.HasMember("event")) {
		if (strcmp(json["event"].GetString(), "info") == 0) {
			KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: is info");
			onInfo(json);
		}
		else if (strcmp(json["event"].GetString(), "ping") == 0) {
			KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: is ping");
			onPing(json);
		}
		else if (strcmp(json["event"].GetString(), "subscribed") == 0) {
			KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: is subscribed");
			onSubscribed(json);
		}
		else {
			KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: unknown event");
		};
	}

	//data
	if (json.IsArray()) {
		int chanId = json.GetArray()[0].GetInt();
		KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: (chanId)" << chanId);

		SubscribeChannel channel = findByChannelID(chanId);
		if (channel.channelId == 0) {
			KF_LOG_ERROR(logger, "MDEngineBitfinex::on_lws_data: EMPTY_CHANNEL (chanId)" << chanId);
		}
		else {
			if (channel.subType == book_channel) {
				KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: is book");
				onBook(channel, json);
			}
			else if (channel.subType == trade_channel) {
				KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: is trade");
				onTrade(channel, json);
			}
			else {
				KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: unknown array data.");
			}
		}
	}
}

void MDEngineBitfinex::OnDisconnected(const common::CWebsocket* instance)
{
	KF_LOG_ERROR(logger, "MDEngineBitfinex::on_lws_connection_error.");
	//market logged_in false;
	logged_in = false;
	KF_LOG_ERROR(logger, "MDEngineBitfinex::on_lws_connection_error. login again.");
	//clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
	priceBook20Assembler.clearPriceBook();
	//no use it
	long timeout_nsec = 0;
	//reset sub
	subscribe_index = 0;
	is_ws_disconnectd = true;
	//login(timeout_nsec);
}

void MDEngineBitfinex::on_lws_data(struct lws* conn, const char* data, size_t len)
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: " << data);
	Document json;
	json.Parse(data);

	if (json.HasParseError()) {
		KF_LOG_ERROR(logger, "MDEngineBitfinex::on_lws_data. parse json error: " << data);
		return;
	}

	if (json.IsObject() && json.HasMember("event")) {
		if (strcmp(json["event"].GetString(), "info") == 0) {
			KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: is info");
			onInfo(json);
		}
		else if (strcmp(json["event"].GetString(), "ping") == 0) {
			KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: is ping");
			//onPing(conn, json);
		}
		else if (strcmp(json["event"].GetString(), "subscribed") == 0) {
			KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: is subscribed");
			onSubscribed(json);
		}
		else {
			KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: unknown event: " << data);
		};
	}

	//data
	if (json.IsArray()) {
		int chanId = json.GetArray()[0].GetInt();
		KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: (chanId)" << chanId);

		SubscribeChannel channel = findByChannelID(chanId);
		if (channel.channelId == 0) {
			KF_LOG_ERROR(logger, "MDEngineBitfinex::on_lws_data: EMPTY_CHANNEL (chanId)" << chanId);
		}
		else {
			if (channel.subType == book_channel) {
				KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: is book");
				onBook(channel, json);
			}
			else if (channel.subType == trade_channel) {
				KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: is trade");
				onTrade(channel, json);
			}
			else {
				KF_LOG_INFO(logger, "MDEngineBitfinex::on_lws_data: unknown array data: " << data);
			}
		}
	}
}


void MDEngineBitfinex::on_lws_connection_error(struct lws* conn)
{
	KF_LOG_ERROR(logger, "MDEngineBitfinex::on_lws_connection_error.");
	//market logged_in false;
	logged_in = false;
	KF_LOG_ERROR(logger, "MDEngineBitfinex::on_lws_connection_error. login again.");
	//clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
	priceBook20Assembler.clearPriceBook();
	//no use it
	long timeout_nsec = 0;
	//reset sub
	subscribe_index = 0;

	login(timeout_nsec);
}

int64_t MDEngineBitfinex::getTimestamp()
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
void MDEngineBitfinex::onPing( Document & json)
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::onPing: " << parseJsonToString(json));
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("event");
	writer.String("pong");

	writer.Key("ts");
	writer.Int64(getTimestamp());

	writer.Key("cid");
	writer.Int(json["cid"].GetInt());

	writer.EndObject();

	std::string result = s.GetString();
	KF_LOG_INFO(logger, "MDEngineBitfinex::onPing: (Pong)" << result);
	//websocketPendingSendMsg.push_back(result);

	//emit a callback
	//lws_callback_on_writable(conn);
	websocket.SendMessage(result);
}

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
void MDEngineBitfinex::onInfo(Document & json)
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::onInfo: " << parseJsonToString(json));
}


//{\event\:\subscribed\,\channel\:\book\,\chanId\:56,\symbol\:\tETCBTC\,\prec\:\P0\,\freq\:\F0\,\len\:\25\,\pair\:\ETCBTC\}
//{\event\:\subscribed\,\channel\:\trades\,\chanId\:2337,\symbol\:\tETHBTC\,\pair\:\ETHBTC\}
//{\event\:\subscribed\,\channel\:\trades\,\chanId\:1,\symbol\:\tBTCUSD\,\pair\:\BTCUSD\}
void MDEngineBitfinex::onSubscribed(Document & json)
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::onSubscribed: " << parseJsonToString(json));

	if (json.HasMember("chanId") && json.HasMember("symbol") && json.HasMember("channel")) {
		int chanId = json["chanId"].GetInt();
		std::string symbol = json["symbol"].GetString();

		if (strcmp(json["channel"].GetString(), "trades") == 0) {
			SubscribeChannel newChannel;
			newChannel.channelId = chanId;
			newChannel.subType = trade_channel;
			newChannel.exchange_coinpair = symbol;
			websocketSubscribeChannel.push_back(newChannel);
		}

		if (strcmp(json["channel"].GetString(), "book") == 0) {
			SubscribeChannel newChannel;
			newChannel.channelId = chanId;
			newChannel.subType = book_channel;
			newChannel.exchange_coinpair = symbol;
			websocketSubscribeChannel.push_back(newChannel);
		}
	}

	debug_print(websocketSubscribeChannel);
}

void MDEngineBitfinex::debug_print(std::vector<SubscribeChannel> & websocketSubscribeChannel)
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

SubscribeChannel MDEngineBitfinex::findByChannelID(int channelId)
{
	size_t count = websocketSubscribeChannel.size();

	for (size_t i = 0; i < count; i++)
	{
		if (channelId == websocketSubscribeChannel[i].channelId) {
			return websocketSubscribeChannel[i];
		}
	}
	return EMPTY_CHANNEL;
}

//[1,[[279619183,1534151022575,0.05404775,6485.1],[279619171,1534151022010,-1.04,6485],[279619170,1534151021847,-0.02211732,6485],......]
//[1,"te",[279619192,1534151024181,-0.05678467,6485]]
void MDEngineBitfinex::onTrade(SubscribeChannel & channel, Document & json)
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::onTrade: (symbol) " << channel.exchange_coinpair);

	std::string ticker = coinPairWhiteList_websocket.GetKeyByValue(channel.exchange_coinpair);
	if (ticker.length() == 0) {
		return;
	}

	int size = json.GetArray().Size();
	if (size < 2) return;
	if (!json.GetArray()[1].IsString() || strcmp(json.GetArray()[1].GetString(),"tu") != 0)
	{
		return;
	}
	int last_element = size - 1;
	if (json.GetArray()[last_element].IsArray()) {
		int len = json.GetArray()[last_element].Size();
		if (len == 0) return;

		if (json.GetArray()[last_element].GetArray()[0].IsArray())
		{
			/* snapshot
			 * [
				  CHANNEL_ID,
				  [
					[
					  ID,
					  MTS,
					  AMOUNT,
					  PRICE
					],
					...
				  ]
				]
				//±AMOUNT	float	How much was bought (positive) or sold (negative).
				//正的是: S     (maker is buyer )
				//负的是: B
				//[1,[[279619183,1534151022575,0.05404775,6485.1],[279619171,1534151022010,-1.04,6485],[279619170,1534151021847,-0.02211732,6485],[279619167,1534151021199,-0.61188115,6485.1],[279619166,1534151019315,-0.22695,6485.1],[279619156,1534151014908,-0.05675262,6485.1],[279619153,1534151013760,0.04885593,6485.2],[279619149,1534151013009,-0.03700977,6485.1],[279619140,1534151009718,-0.3416722,6485.1],[279619135,1534151009007,-0.0099409,6485.1],[279619134,1534151008682,-0.28963734,6485.1],[279619129,1534151007656,-0.00695966,6485.1],[279619128,1534151007443,0.003855,6485.2],[279619123,1534151005539,-0.05533626,6485.1],[279619121,1534151005326,0.05081637,6485.2],[279619102,1534151004043,-0.00737768,6485.1],[279619100,1534151003819,-0.05475973,6485.1],[279619096,1534151002811,-0.05475973,6485.1],[279619080,1534151001414,-0.01828309,6485.1],[279619077,1534151000660,-0.0147,6485.1],[279619066,1534150998307,-0.09175605,6485.1],[279619065,1534150998206,-0.0522174,6485.1],[279619020,1534150994733,0.05436071,6485.2],[279618991,1534150990781,0.0101821,6485.2],[279618946,1534150986977,0.24390946,6485.2],[279618918,1534150986112,0.0203,6485.2],[279618917,1534150986109,0.05562306,6485.2],[279618838,1534150977454,0.03,6485.2],[279618828,1534150976649,0.05351248,6485.2],[279618827,1534150975684,0.02853241,6485.2]]]
			 * */
			for (int i = 0; i < len; i++) {
				//                KF_LOG_INFO(logger, " (0)" << json.GetArray()[last_element].GetArray()[i].GetArray()[0].GetInt64() );
				//                KF_LOG_INFO(logger, " (1)" << json.GetArray()[last_element].GetArray()[i].GetArray()[1].GetInt64() );
				//                KF_LOG_INFO(logger, " (2)" << json.GetArray()[last_element].GetArray()[i].GetArray()[2].GetDouble() );
				//                KF_LOG_INFO(logger, " (3)" << json.GetArray()[last_element].GetArray()[i].GetArray()[3].GetDouble() );

				LFL2TradeField trade;
				memset(&trade, 0, sizeof(trade));
				strcpy(trade.InstrumentID, ticker.c_str());
				strcpy(trade.ExchangeID, "bitfinex");
				trade.TimeStamp = json.GetArray()[last_element].GetArray()[i].GetArray()[1].GetInt64();
				string strTime  = timestamp_to_formatISO8601(trade.TimeStamp);
				strncpy(trade.TradeTime, strTime.c_str(),sizeof(trade.TradeTime));
				trade.TimeStamp*=1000000;
				trade.Price = std::round(json.GetArray()[last_element].GetArray()[i].GetArray()[3].GetDouble() * scale_offset);
				double amount = json.GetArray()[last_element].GetArray()[i].GetArray()[2].GetDouble();
				uint64_t volume = 0;
				if (amount < 0) {
					volume = std::round(-1 * amount * scale_offset);
				}
				else {
					volume = std::round(amount * scale_offset);
				}
				trade.Volume = volume;
				trade.OrderBSFlag[0] = amount < 0 ? 'B' : 'S';
				std::string tradeid = std::to_string(json.GetArray()[last_element].GetArray()[i].GetArray()[0].GetInt64());
				strncpy(trade.TradeID, tradeid.c_str(),sizeof(trade.TradeID));
				KF_LOG_INFO(logger, "MDEngineBitfinex::[onTrade] (ticker)" << ticker <<
					" (Price)" << trade.Price <<
					" (trade.Volume)" << trade.Volume);
				on_trade(&trade);

			}
		}
		else {
			/*update
			 * [
			  CHANNEL_ID,
			  <"te", "tu">,
			  [
				ID,
				MTS,
				AMOUNT,
				PRICE
			  ]
			]
			//±AMOUNT	float	How much was bought (positive) or sold (negative).
			//正的是: S     (maker is buyer )
			//负的是: B
			//[1,"te",[279619192,1534151024181,-0.05678467,6485.1]]
			 * */
			 //            KF_LOG_INFO(logger, " update(0)" << json.GetArray()[last_element].GetArray()[0].GetInt64());
			 //            KF_LOG_INFO(logger, " update(1)" << json.GetArray()[last_element].GetArray()[1].GetInt64());
			 //            KF_LOG_INFO(logger, " update(2)"<< json.GetArray()[last_element].GetArray()[2].GetDouble());
			 //            KF_LOG_INFO(logger, " update(3)"<< json.GetArray()[last_element].GetArray()[3].GetDouble());

			LFL2TradeField trade;
			memset(&trade, 0, sizeof(trade));
			strcpy(trade.InstrumentID, ticker.c_str());
			strcpy(trade.ExchangeID, "bitfinex");
			trade.TimeStamp = json.GetArray()[last_element].GetArray()[1].GetInt64();
			string strTime  = timestamp_to_formatISO8601(trade.TimeStamp);
			strncpy(trade.TradeTime, strTime.c_str(),sizeof(trade.TradeTime));
			trade.TimeStamp*=1000000;
			trade.Price = std::round(json.GetArray()[last_element].GetArray()[3].GetDouble() * scale_offset);
			double amount = json.GetArray()[last_element].GetArray()[2].GetDouble();
			uint64_t volume = 0;
			if (amount < 0) {
				volume = std::round(-1 * amount * scale_offset);
			}
			else {
				volume = std::round(amount * scale_offset);
			}
			trade.Volume = volume;
			trade.OrderBSFlag[0] = amount < 0 ? 'B' : 'S';
			std::string tradeid = std::to_string(json.GetArray()[last_element].GetArray()[0].GetInt64());
			strncpy(trade.TradeID, tradeid.c_str(),sizeof(trade.TradeID));
			KF_LOG_INFO(logger, "MDEngineBitfinex::[onTrade] (ticker)" << ticker <<
				" (Price)" << trade.Price <<
				" (trade.Volume)" << trade.Volume);

			std::unique_lock<std::mutex> lck(trade_mutex);
			auto it = control_trade_map.find(ticker);
			if(it != control_trade_map.end())
			{
				it->second = getTimestamp();
			}
			lck.unlock();

			on_trade(&trade);
		}
	}
}

void MDEngineBitfinex::onBook(SubscribeChannel & channel, Document & json)
{
	KF_LOG_INFO(logger, "MDEngineBitfinex::onBook: (symbol) " << channel.exchange_coinpair);

	std::string ticker = coinPairWhiteList_websocket.GetKeyByValue(channel.exchange_coinpair);
	if (ticker.length() == 0) {
		KF_LOG_DEBUG(logger, "MDEngineBitfinex::onBook: (ticker.length==0) " << ticker);
		return;
	}

	KF_LOG_INFO(logger, "MDEngineBitfinex::onBook: (ticker) " << ticker);

	int size = json.GetArray().Size();
	int last_element = size - 1;
	if (json.GetArray()[last_element].IsArray()) {
		int len = json.GetArray()[last_element].Size();
		if (len == 0) return;

		if (json.GetArray()[last_element].GetArray()[0].IsArray())
		{
			/* snapshot
			 * [
			  CHANNEL_ID,
			  [
				[
				  PRICE,
				  COUNT,
				  AMOUNT
				],
				...
			  ]
			]
			//±AMOUNT	float	Total amount available at that price level. Trading: if AMOUNT > 0 then bid else ask; Funding: if AMOUNT < 0 then bid else ask;
			//[1436,[[6462.1,1,0.44900651],[6462,1,0.47744492],[6461.8,1,0.37530027],[6460.9,1,0.02333184],[6460.2,1,0.22609269],[6460,3,56.42399791],[6458,1,0.03622],[6457,1,1.54],[6456.6,1,0.4],[6456.1,2,0.8],[6455.5,1,2.313],[6454.9,1,0.4],[6453.6,1,0.2],[6452,2,1.2],[6451.8,2,0.6],[6451.7,1,1.5],[6450.1,1,3],[6450,1,0.5],[6449,1,0.15],[6448.7,2,0.00708406],[6448.6,3,1.53750584],[6448.5,2,64.00891964],[6448.3,1,0.028],[6448.2,1,1.55],[6447.8,1,7.98930678],[6462.2,28,-18.24119121],[6462.3,2,-0.26],[6462.4,1,-2.01620248],[6462.7,1,-0.19283279],[6462.8,1,-1.4],[6462.9,1,-1.5],[6463.1,1,-5.835],[6463.2,2,-1.373],[6463.4,1,-0.35391244],[6463.7,1,-0.30874569],[6463.8,1,-2.5],[6463.9,2,-2.54],[6464,1,-20],[6464.1,1,-0.5],[6464.2,1,-0.02217419],[6464.4,1,-0.31008094],[6464.6,1,-1],[6464.7,2,-0.22977],[6464.8,1,-1.85],[6465,1,-1],[6465.1,1,-2],[6465.7,2,-1.079],[6466.1,2,-1.544342],[6466.4,1,-1.3],[6467.2,1,-1.7]]]
			 *
			 *
			Algorithm to create and keep a book instance updated

			subscribe to channel
			receive the book snapshot and create your in-memory book structure
			when count > 0 then you have to add or update the price level
			3.1 if amount > 0 then add/update bids
			3.2 if amount < 0 then add/update asks
			when count = 0 then you have to delete the price level.
			4.1 if amount = 1 then remove from bids
			4.2 if amount = -1 then remove from asks
			 * */
			priceBook20Assembler.clearPriceBook(ticker);
			for (int i = 0; i < len; i++) {
				int64_t price = std::round(json.GetArray()[last_element].GetArray()[i].GetArray()[0].GetDouble() * scale_offset);
				int count = json.GetArray()[last_element].GetArray()[i].GetArray()[1].GetInt();
				double dAmount = json.GetArray()[last_element].GetArray()[i].GetArray()[2].GetDouble();
				uint64_t amount = 0;
				if (dAmount < 0) {
					amount = std::round(-1 * dAmount * scale_offset);
				}
				else {
					amount = std::round(dAmount * scale_offset);
				}

				KF_LOG_INFO(logger, "MDEngineBitfinex::onBook: (ticker) " << ticker << " (price)" << price << " (amount)" << amount);

				if (count == 0) {
					if (dAmount == 1) {
						priceBook20Assembler.EraseBidPrice(ticker, price);
					}
					if (dAmount == -1) {
						priceBook20Assembler.EraseAskPrice(ticker, price);
					}
				}
				else if (count > 0) {
					if (dAmount > 0) {
						priceBook20Assembler.UpdateBidPrice(ticker, price, amount);
					}
					else if (dAmount <= 0) {
						priceBook20Assembler.UpdateAskPrice(ticker, price, amount);
					}
				}
				//                KF_LOG_INFO(logger, " (0)" << json.GetArray()[last_element].GetArray()[i].GetArray()[0].GetDouble() );
				//                KF_LOG_INFO(logger, " (1)" << json.GetArray()[last_element].GetArray()[i].GetArray()[1].GetInt() );
				//                KF_LOG_INFO(logger, " (2)" << json.GetArray()[last_element].GetArray()[i].GetArray()[2].GetDouble() );
			}
		}
		else {
			/*update
			 * [
				  CHANNEL_ID,
				  [
					PRICE,
					COUNT,
					AMOUNT
				  ]
				]
			//±AMOUNT	float	Total amount available at that price level. Trading: if AMOUNT > 0 then bid else ask; Funding: if AMOUNT < 0 then bid else ask;
			 //[1436,[6462.7,0,-1]]
			 //[5,[6464.8,2,-1.90818689]]
			 * */
			int64_t price = std::round(json.GetArray()[last_element].GetArray()[0].GetDouble() * scale_offset);
			int count = json.GetArray()[last_element].GetArray()[1].GetInt();
			double dAmount = json.GetArray()[last_element].GetArray()[2].GetDouble();
			uint64_t amount = 0;
			if (dAmount < 0) {
				amount = std::round(-1 * dAmount * scale_offset);
			}
			else {
				amount = std::round(dAmount * scale_offset);
			}

			KF_LOG_INFO(logger, "MDEngineBitfinex::onBook: (ticker) " << ticker << " (price)" << price << " (amount)" << amount);

			if (count == 0) {
				if (dAmount == 1) {
					priceBook20Assembler.EraseBidPrice(ticker, price);
				}
				if (dAmount == -1) {
					priceBook20Assembler.EraseAskPrice(ticker, price);
				}
			}
			else if (count > 0) {
				if (dAmount > 0) {
					priceBook20Assembler.UpdateBidPrice(ticker, price, amount);
				}
				else if (dAmount <= 0) {
					priceBook20Assembler.UpdateAskPrice(ticker, price, amount);
				}
			}
			//            KF_LOG_INFO(logger, " update(0)" << json.GetArray()[last_element].GetArray()[0].GetDouble() );
			//            KF_LOG_INFO(logger, " update(1)" << json.GetArray()[last_element].GetArray()[1].GetInt() );
			//            KF_LOG_INFO(logger, " update(2)"<< json.GetArray()[last_element].GetArray()[2].GetDouble() );
		}
	}
	//has any update
	int errorId = 0;
	string errorMsg = "";
	LFPriceBook20Field md;
	memset(&md, 0, sizeof(md));
	if (priceBook20Assembler.Assembler(ticker, md)) {
		strcpy(md.ExchangeID, "bitfinex");

		KF_LOG_INFO(logger, "MDEngineBitfinex::onDepth: on_price_book_update");
		/*on_price_book_update(&md);*/
		/*quest2 FXW's edits start here*/
		if (priceBook20Assembler.GetLeastLevel() > priceBook20Assembler.GetNumberOfLevels_asks(ticker) ||
			priceBook20Assembler.GetLeastLevel() > priceBook20Assembler.GetNumberOfLevels_bids(ticker)
			/*|| priceBook20Assembler.GetNumberOfLevels_asks(ticker)!= priceBook20Assembler.GetNumberOfLevels_bids(ticker) */
			)
		{
			md.Status = 1;
			errorId = 112;
			errorMsg = "orderbook level below threshold";
			/*need re-login*/
			KF_LOG_DEBUG(logger, "[FXW]MDEngineBitfinex on_price_book_update failed ,lose level,re-login....");
			on_price_book_update(&md);
			//GetSnapShotAndRtn(ticker);
			write_errormsg(errorId,errorMsg);

		}
		/*else if((priceBook20Assembler.GetNumberOfLevels_asks(ticker)!= priceBook20Assembler.GetNumberOfLevels_bids(ticker))&&once)
		{//这个if分支仅是为测试用
			once=0;
			md.Status = 4;
			//need re-login
			KF_LOG_DEBUG(logger, "[quest2test]MDEngineBitfinex on_price_book_update test request orderbook snapshot....");
			on_price_book_update(&md);
			KF_LOG_DEBUG(logger, "[quest2test]ticker this time:"<<ticker);
			GetSnapShotAndRtn(ticker);
			sleep(6000);
		}*/
		else if ((-1 == priceBook20Assembler.GetBestBidPrice(ticker)) || (-1 == priceBook20Assembler.GetBestAskPrice(ticker)) ||
			priceBook20Assembler.GetBestBidPrice(ticker) >= priceBook20Assembler.GetBestAskPrice(ticker))
		{
			md.Status = 2;
			errorId = 113;
			errorMsg = "orderbook crossed";			
			/*need re-login*/
			KF_LOG_DEBUG(logger, "[FXW]MDEngineBitfinex on_price_book_update failed ,orderbook crossed,re-login....");
			on_price_book_update(&md);
			//GetSnapShotAndRtn(ticker);
			write_errormsg(errorId,errorMsg);
		}
		else
		{
			md.Status = 0;
			on_price_book_update(&md);
			timer = getTimestamp();/*quest2 fxw's edits v3*/
			//本文件中出现的timer的赋值暂且都不进行修改，作为一个变量赋值过程
			//不会影响程序的运行结果

			KF_LOG_DEBUG(logger, "[FXW successed]MDEngineBitfinex on_price_book_update successed");
		}
		std::unique_lock<std::mutex> lck1(book_mutex);
		auto it = control_book_map.find(ticker);
		if(it != control_book_map.end())
		{
			it->second = getTimestamp();
		}
		lck1.unlock();
		/*quest2 FXW's edits end here*/
	}
	else//*quest2 FXW's edits v5 
	{
		std::unique_lock<std::mutex> lck1(book_mutex);
		auto it = control_book_map.find(ticker);
		if(it != control_book_map.end())
		{
			it->second = getTimestamp();
		}
		lck1.unlock();
		timer = getTimestamp();
		//GetSnapShotAndRtn(ticker);
		KF_LOG_DEBUG(logger, "[FXW]MDEngineBitfinex on_price_book_update,priceBook20Assembler.Assembler(ticker, md) failed\n(ticker)" << ticker);
	}
	
}

std::string MDEngineBitfinex::parseJsonToString(Document & d)
{
	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	d.Accept(writer);

	return buffer.GetString();
}

//{ "event": "subscribe", "channel": "book",  "symbol": "tBTCUSD" }
std::string MDEngineBitfinex::createBookJsonString(std::string exchange_coinpair)
{
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("event");
	writer.String("subscribe");

	writer.Key("channel");
	writer.String("book");

	writer.Key("symbol");
	writer.String(exchange_coinpair.c_str());

	writer.Key("len");
	//writer.Int(book_depth_count);
	writer.Int(25);

	writer.EndObject();
	return s.GetString();
}

//{ "event": "subscribe", "channel": "trades",  "symbol": "tETHBTC" }
std::string MDEngineBitfinex::createTradeJsonString(std::string exchange_coinpair)
{
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("event");
	writer.String("subscribe");

	writer.Key("channel");
	writer.String("trades");

	writer.Key("symbol");
	writer.String(exchange_coinpair.c_str());

	writer.EndObject();
	return s.GetString();
}

void MDEngineBitfinex::loop()
{
	while (isRunning)
	{
		/*quest2 fxw's edits v3 starts here*/
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
			std::cout << (now - it->second)/1000;
		}
		lck.unlock();

		std::unique_lock<std::mutex> lck1(trade_mutex);
		std::map<std::string, int64_t>::iterator it1;
		for(it1 = control_trade_map.begin();it1 != control_trade_map.end();it1++)
		{
			if((now - it1->second) > refresh_normal_check_book_s * 1000)
			{
				errorId = 114;
				errorMsg = it->first + " book max refresh wait time exceeded";
	            KF_LOG_INFO(logger,"114"<<errorMsg); 
	            write_errormsg(errorId,errorMsg);
	            it1->second = now; 
			}
			std::cout << (now - it1->second)/1000;
		}
		lck1.unlock();
		/*quest2 fxw's edits v3 ends here*/
		//int n = lws_service(context, rest_get_interval_ms);
		//std::cout << " 3.1415 loop() lws_service (n)" << n << std::endl;
		lws_service( context, rest_get_interval_ms );
	}
}

BOOST_PYTHON_MODULE(libbitfinexmd)
{
	using namespace boost::python;
	class_<MDEngineBitfinex, boost::shared_ptr<MDEngineBitfinex> >("Engine")
		.def(init<>())
		.def("init", &MDEngineBitfinex::initialize)
		.def("start", &MDEngineBitfinex::start)
		.def("stop", &MDEngineBitfinex::stop)
		.def("logout", &MDEngineBitfinex::logout)
		.def("wait_for_stop", &MDEngineBitfinex::wait_for_stop);
}

