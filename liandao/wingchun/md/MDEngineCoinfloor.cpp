#include "MDEngineCoinfloor.h"
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

static MDEngineCoinfloor* global_md = nullptr;
static std::string snapshot_data;
static int ws_service_cb(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len)
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

MDEngineCoinfloor::MDEngineCoinfloor() : IMDEngine(SOURCE_COINFLOOR)
{
	logger = yijinjing::KfLog::getLogger("MdEngine.Coinfloor");
}

void MDEngineCoinfloor::load(const json& j_config)
{
	book_depth_count = j_config["book_depth_count"].get<int>();
	trade_count = j_config["trade_count"].get<int>();
	rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
	KF_LOG_INFO(logger, "MDEngineCoinfloor:: rest_get_interval_ms: " << rest_get_interval_ms);


	coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
	coinPairWhiteList.Debug_print();

	makeWebsocketSubscribeJsonString();
	debug_print(websocketSubscribeJsonString);

	//display usage:
	if (coinPairWhiteList.Size() == 0) {
		KF_LOG_ERROR(logger, "MDEngineCoinfloor::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
		KF_LOG_ERROR(logger, "\"whiteLists\":{");
		KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
		KF_LOG_ERROR(logger, "    \"btc_usdt\": \"tBTCUSDT\",");
		KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
		KF_LOG_ERROR(logger, "},");
	}

	KF_LOG_INFO(logger, "MDEngineCoinfloor::load:  book_depth_count: "
		<< book_depth_count << " trade_count: " << trade_count << " rest_get_interval_ms: " << rest_get_interval_ms);
}

void MDEngineCoinfloor::makeWebsocketSubscribeJsonString()
{
	std::unordered_map<std::string, std::string>::iterator map_itr;
	map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
	int num = 1;
	while (map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
		KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);

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


void MDEngineCoinfloor::split(std::string str, std::string token, CoinBaseQuote& sub)
{
	if (str.size() > 0)
	{
		size_t index = str.find(token);
		if (index != std::string::npos)
		{
			std::string str1 = str.substr(0, index);
			std::string str2 = str.substr(index + token.size());
			sub.base = atoi(str1.c_str());
			sub.quote = atoi(str2.c_str());
		}
		else {
			//not found, do nothing
		}
	}
}


void MDEngineCoinfloor::debug_print(std::vector<std::string> &subJsonString)
{
	size_t count = subJsonString.size();
	KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

	for (size_t i = 0; i < count; i++)
	{
		KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
	}
}

void MDEngineCoinfloor::connect(long timeout_nsec)
{
	KF_LOG_INFO(logger, "MDEngineCoinfloor::connect:");
	connected = true;
}



void MDEngineCoinfloor::login(long timeout_nsec) {
	KF_LOG_INFO(logger, "MDEngineCoinfloor::login:");
	global_md = this;

	if (context == NULL) {
		struct lws_context_creation_info info;
		memset(&info, 0, sizeof(info));

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
		info.pt_serv_buf_size = 64 * 1024;
		context = lws_create_context(&info);
		KF_LOG_INFO(logger, "MDEngineCoinfloor::login: context created.");
	}

	if (context == NULL) {
		KF_LOG_ERROR(logger, "MDEngineCoinfloor::login: context is NULL. return");
		return;
	}

	int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
	lws_set_log_level(logs, NULL);

	struct lws_client_connect_info ccinfo = { 0 };

	static std::string host = "api.coinfloor.co.uk";
	static std::string path = "/";
	static int port = 443;

	ccinfo.context = context;
	ccinfo.address = host.c_str();    //
	ccinfo.port = port;
	ccinfo.path = path.c_str();    //
	ccinfo.host = host.c_str();
	ccinfo.origin = host.c_str();
	ccinfo.ietf_version_or_minus_one = -1;
	ccinfo.protocol = protocols[0].name;
	ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

	struct lws* wsi = lws_client_connect_via_info(&ccinfo);
	KF_LOG_INFO(logger, "MDEngineCoinfloor::login: Connecting to " << ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

	if (wsi == NULL) {
		KF_LOG_ERROR(logger, "MDEngineCoinfloor::login: wsi create error.");
		return;
	}
	KF_LOG_INFO(logger, "MDEngineCoinfloor::login: wsi create success.");

	logged_in = true;
}

void MDEngineCoinfloor::set_reader_thread()
{
	IMDEngine::set_reader_thread();

	rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineCoinfloor::loop, this)));
}

void MDEngineCoinfloor::logout()
{
	KF_LOG_INFO(logger, "MDEngineCoinfloor::logout:");
}

void MDEngineCoinfloor::release_api()
{
	KF_LOG_INFO(logger, "MDEngineCoinfloor::release_api:");
}

void MDEngineCoinfloor::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
	KF_LOG_INFO(logger, "MDEngineCoinfloor::subscribeMarketData:");
}

int MDEngineCoinfloor::lws_write_subscribe(struct lws* conn)
{
	KF_LOG_INFO(logger, "MDEngineCoinfloor::lws_write_subscribe: (subscribe_index)" << subscribe_index);

	//�д����͵����ݣ��ȰѴ����͵ķ��꣬�ڼ��������߼���  ping?
	if (websocketPendingSendMsg.size() > 0) {
		unsigned char msg[512];
		memset(&msg[LWS_PRE], 0, 512 - LWS_PRE);

		std::string jsonString = websocketPendingSendMsg[websocketPendingSendMsg.size() - 1];
		websocketPendingSendMsg.pop_back();
		KF_LOG_INFO(logger, "MDEngineCoinfloor::lws_write_subscribe: websocketPendingSendMsg" << jsonString.c_str());
		int length = jsonString.length();

		strncpy((char *)msg + LWS_PRE, jsonString.c_str(), length);
		int ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);

		if (websocketPendingSendMsg.size() > 0)
		{    //still has pending send data, emit a lws_callback_on_writable()
			lws_callback_on_writable(conn);
			KF_LOG_INFO(logger, "MDEngineCoinfloor::lws_write_subscribe: (websocketPendingSendMsg,size)" << websocketPendingSendMsg.size());
		}
		return ret;
	}

	if (websocketSubscribeJsonString.size() == 0) return 0;

	if (subscribe_index >= websocketSubscribeJsonString.size())
	{
		//subscribe_index = 0;
		KF_LOG_INFO(logger, "MDEngineCoinfloor::lws_write_subscribe: (none reset subscribe_index = 0, just return 0)");
		return 0;
	}

	unsigned char msg[512];
	memset(&msg[LWS_PRE], 0, 512 - LWS_PRE);

	std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

	KF_LOG_INFO(logger, "MDEngineCoinfloor::lws_write_subscribe: " << jsonString.c_str() << " ,after ++, (subscribe_index)" << subscribe_index);
	int length = jsonString.length();

	strncpy((char *)msg + LWS_PRE, jsonString.c_str(), length);
	int ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);

	if (subscribe_index < websocketSubscribeJsonString.size())
	{
		lws_callback_on_writable(conn);
		KF_LOG_INFO(logger, "MDEngineCoinfloor::lws_write_subscribe: (subscribe_index < websocketSubscribeJsonString) call lws_callback_on_writable");
	}

	return ret;
}

void MDEngineCoinfloor::on_lws_data(struct lws* conn, const char* data, size_t len)
{
	KF_LOG_INFO(logger, "MDEngineCoinfloor::on_lws_data: " << data<<"data len: "<<len);
	
	Document json;
	json.Parse(data);
	if (json.HasParseError()&&len>=4096) {
		snapshot_data += data;
	}
	else if (json.HasParseError()) {
		snapshot_data += data;
		json.Parse(snapshot_data.c_str());
		KF_LOG_INFO(logger, "MDEngineCoinfloor::on_lws_data: snapshot_data: " << snapshot_data);
		if (json.HasParseError()) {
			KF_LOG_INFO(logger, "MDEngineCoinfloor::on_lws_data:error snapshot_data: " << snapshot_data);
			snapshot_data = "";
		}		
	}

	if (!json.HasParseError() && json.IsObject() && ((json.HasMember("tag") && json["tag"].IsInt())
		|| (json.HasMember("notice") && json["notice"].IsString())))
	{

		if (json.HasMember("tag") && json["tag"].IsInt())
		{
			int num = json["tag"].GetInt();
			Document::AllocatorType& allocator = json.GetAllocator();
			Value value(3);
			value.SetInt(currency[num].base);
			json.AddMember("base", value, allocator);
			value.SetInt(currency[num].quote);
			json.AddMember("counter", value, allocator);
		}

		if (json.HasMember("notice") && json["notice"].IsString() && strcmp(json["notice"].GetString(), "OrdersMatched") == 0)
		{
			KF_LOG_INFO(logger, "MDEngineCoinmex::on_lws_data: is trade");
			onTrade(json);
		}

		KF_LOG_INFO(logger, "MDEngineCoinfloor::on_lws_data: is onbook");
		onBook(json);
	}
	
		


}


void MDEngineCoinfloor::on_lws_connection_error(struct lws* conn)
{
	KF_LOG_ERROR(logger, "MDEngineCoinfloor::on_lws_connection_error.");
	//market logged_in false;
	logged_in = false;
	KF_LOG_ERROR(logger, "MDEngineCoinfloor::on_lws_connection_error. login again.");
	//clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
	priceBook20Assembler.clearPriceBook();
	//no use it
	long timeout_nsec = 0;
	//reset sub
	subscribe_index = 0;

	login(timeout_nsec);
}

void MDEngineCoinfloor::onTrade(Document& json)
{
	std::string base = "";
	if (json.HasMember("base") && json["base"].IsInt()) {
		base = std::to_string(json["base"].GetInt());
	}
	std::string counter = "";
	if (json.HasMember("counter") && json["counter"].IsInt()) {
		counter = std::to_string(json["counter"].GetInt());
	}

	KF_LOG_INFO(logger, "MDEngineCoinfloor::onTrade:" << "base : " << base << "  counter: " << counter);
	std::string base_counter = base + "_" + counter;
	std::transform(base_counter.begin(), base_counter.end(), base_counter.begin(), ::toupper);
	std::string ticker = coinPairWhiteList.GetKeyByValue(base_counter);
	if (ticker.length() == 0) {
		KF_LOG_INFO(logger, "MDEngineCoinfloor::onTrade: not in WhiteList , ignore it:" << "base : " << base << "  counter: " << counter);
		return;
	}
	int64_t price = json["price"].GetInt();
	price *= scale_offset;
	int64_t volume = json["quantity"].GetInt();
	volume *= scale_offset;
	LFL2TradeField trade_ask, trade_bid;
	memset(&trade_ask, 0, sizeof(trade_ask));
	strcpy(trade_ask.InstrumentID, ticker.c_str());
	strcpy(trade_ask.ExchangeID, "COINFLOR");//ExchangeID char_9 ,can't assign for "coinfloor",this will make Array overflow error
	memset(&trade_bid, 0, sizeof(trade_bid));
	strcpy(trade_bid.InstrumentID, ticker.c_str());
	strcpy(trade_bid.ExchangeID, "COINFLOR");
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
	trade_ask.TimeStamp = json["time"].GetInt64()*1000;
	trade_bid.TimeStamp = trade_ask.TimeStamp;
	std::string strTime = timestamp_to_formatISO8601(trade_ask.TimeStamp/1000000);
	strncpy(trade_bid.TradeTime, strTime.c_str(),sizeof(trade_bid.TradeTime));
	strncpy(trade_ask.TradeTime, strTime.c_str(),sizeof(trade_ask.TradeTime));
	on_trade(&trade_ask);
	on_trade(&trade_bid);
}


void MDEngineCoinfloor::onBook(Document& json)
{

	std::string base = "";
	if (json.HasMember("base") && json["base"].IsInt()) {
		base = std::to_string(json["base"].GetInt());
	}
	std::string counter = "";
	if (json.HasMember("counter") && json["counter"].IsInt()) {
		counter = std::to_string(json["counter"].GetInt());
	}
	std::string base_counter = base + "_" + counter;
	std::transform(base_counter.begin(), base_counter.end(), base_counter.begin(), ::toupper);
	std::string ticker = coinPairWhiteList.GetKeyByValue(base_counter);
	if (ticker.length() == 0) {
		KF_LOG_INFO(logger, "MDEngineCoinfloor::onBook: not in WhiteList , ignore it:" << "base : " << base << "  counter: " << counter);
		return;
	}

	std::map<int64_t, PriceAndVolume>::iterator it;
	KF_LOG_INFO(logger, "MDEngineCoinfloor::onBook:" << "(ticker) " << ticker);
	//make depth map
	if (json.HasMember("orders") && json["orders"].IsArray())
	{
		size_t len = json["orders"].Size();
		auto& vum = json["orders"];
		priceBook20Assembler.clearPriceBook(ticker);
		for (size_t i = 0; i < len; i++)
		{
			auto& object = vum[i];
			if (object.IsObject())
			{
				PriceAndVolume pv;
				pv.price = object["price"].GetInt();
				if (object["quantity"].GetInt() < 0)
				{
					int64_t price = object["price"].GetInt();
					price *= scale_offset;
					uint64_t volume = (0 - object["quantity"].GetInt());
					pv.volume = volume;
					volume *= scale_offset;
					PriceId.insert(std::make_pair(object["id"].GetInt(), pv));
					if (volume == 0)
					{
						priceBook20Assembler.EraseAskPrice(ticker, price);
						KF_LOG_INFO(logger, "MDEngineCoinfloor::onBook: ##asksPriceAndVolume volume == 0## price:" << price << "  volume:" << volume);
					}
					else
						priceBook20Assembler.UpdateAskPrice(ticker, price, volume, 1);
				}

				if (object["quantity"].GetInt() > 0)
				{
					int64_t price = object["price"].GetInt();
					price *= scale_offset;
					uint64_t volume = object["quantity"].GetInt();
					pv.volume = volume;
					volume *= scale_offset;
					PriceId.insert(std::make_pair(object["id"].GetInt(), pv));
					if (volume == 0)
					{
						priceBook20Assembler.EraseBidPrice(ticker, price);
						KF_LOG_INFO(logger, "MDEngineCoinfloor::onBook: ##bidsPriceAndVolume volume == 0## price:" << price << "  volume:" << volume);

					}
					else
						priceBook20Assembler.UpdateBidPrice(ticker, price, volume, 1);
				}
			}
		}
	}

	if (json.HasMember("notice") && json["notice"].IsString() && json.HasMember("price") && json["price"].IsInt())
	{
		int64_t price = json["price"].GetInt();
		int64_t volume, volume1;
		if (json["quantity"].GetInt() < 0)
		{
			volume = (0 - json["quantity"].GetInt());
			volume1 = volume;
			volume *= scale_offset;
		}
		if (json["quantity"].GetInt() > 0)
		{
			volume = json["quantity"].GetInt();
			volume1 = volume;
			volume *= scale_offset;
		}

		if (strcmp(json["notice"].GetString(), "OrdersMatched") == 0 && json.HasMember("ask") && json["ask"].IsInt()
			&& json.HasMember("bid") && json["bid"].IsInt())
		{
			int64_t asknum = json["ask"].GetInt();
			it = PriceId.find(asknum);
			if (it != PriceId.end())
			{
				price = it->second.price;
				price *= scale_offset;
				it->second.volume = json["ask_rem"].GetInt();
				priceBook20Assembler.UpdateAskPrice(ticker, price, volume, 2);
			}
			int64_t bidnum = json["bid"].GetInt();
			it = PriceId.find(bidnum);
			if (it != PriceId.end())
			{
				price = it->second.price;
				price *= scale_offset;
				it->second.volume = json["bid_rem"].GetInt();
				priceBook20Assembler.UpdateBidPrice(ticker, price, volume, 2);
			}
		}

		//open ��������
		if (strcmp(json["notice"].GetString(), "OrderOpened") == 0 && json.HasMember("id") && json["id"].IsInt())
		{
			PriceAndVolume pv;
			pv.price = price;
			pv.volume = volume1;
			PriceId.insert(std::make_pair(json["id"].GetInt(), pv));    //��¼�¼Ӷ���
			price *= scale_offset;
			if (json["quantity"].GetInt() < 0)
				priceBook20Assembler.UpdateAskPrice(ticker, price, volume, 1);
			if (json["quantity"].GetInt() > 0)
				priceBook20Assembler.UpdateBidPrice(ticker, price, volume, 1);
		}

		//closed �������� 
		if (strcmp(json["notice"].GetString(), "OrderClosed") == 0)
		{
			it = PriceId.find(json["id"].GetInt());
			price *= scale_offset;
			if (it != PriceId.end())
				PriceId.erase(it);
			if (json["quantity"].GetInt() == 0)
				volume = 0;
			if (json["quantity"].GetInt() <= 0)
			{
				priceBook20Assembler.EraseAskPrice(ticker, price, volume, 1);
				KF_LOG_INFO(logger, "MDEngineCoinfloor::onBook: ##asksPriceAndVolume volume == 0## price:" << price << "  volume:" << volume);
			}
			if (json["quantity"].GetInt() >= 0)
			{
				priceBook20Assembler.EraseBidPrice(ticker, price, volume, 1);
				KF_LOG_INFO(logger, "MDEngineCoinfloor::onBook: ##asksPriceAndVolume volume == 0## price:" << price << "  volume:" << volume);
			}
		}

		//modified ����
		if (strcmp(json["notice"].GetString(), "OrderModified") == 0 && json.HasMember("id") && json["id"].IsInt())
		{
			int64_t idnum = json["id"].GetInt();
			it = PriceId.find(idnum);
			if (it != PriceId.end())
			{
				int tem = 0;
				if (it->second.volume < volume1)
				{
					volume = (volume1 - it->second.volume) * scale_offset;
					tem = 1;
				}
				if (it->second.volume > volume1)
				{
					volume = (it->second.volume - volume1) * scale_offset;
					tem = 2;
				}
				if (price != it->second.price)
				{
					tem = 1;
					int64_t volume_1 = it->second.volume * scale_offset;
					int64_t price_1 = it->second.price * scale_offset;
					if (json["quantity"].GetInt() <= 0)
					{
						priceBook20Assembler.EraseAskPrice(ticker, price_1, volume_1, 1);
						KF_LOG_INFO(logger, "MDEngineCoinfloor::onBook: ##asksPriceAndVolume volume == 0## price:" << price_1 << "  volume:" << volume_1);
					}
					if (json["quantity"].GetInt() >= 0)
					{
						priceBook20Assembler.EraseBidPrice(ticker, price_1, volume_1, 1);
						KF_LOG_INFO(logger, "MDEngineCoinfloor::onBook: ##asksPriceAndVolume volume == 0## price:" << price_1 << "  volume:" << volume_1);
					}
					it->second.price = price;

				}
				it->second.volume = volume1;
				price *= scale_offset;
				if (json["quantity"].GetInt() < 0)
					priceBook20Assembler.UpdateAskPrice(ticker, price, volume, tem);
				if (json["quantity"].GetInt() > 0)
					priceBook20Assembler.UpdateBidPrice(ticker, price, volume, tem);
			}
		}
	}

	LFPriceBook20Field md;
	memset(&md, 0, sizeof(md));
	if (priceBook20Assembler.Assembler(ticker, md)) {
		strcpy(md.ExchangeID, "COINFLOR");//ExchangeID char_9 ,can't assign for "coinfloor",this will make Array overflow error
		KF_LOG_INFO(logger, "MDEngineCoinfloor::onBook: on_price_book_update");
		on_price_book_update(&md);
	}

}

std::string MDEngineCoinfloor::parseJsonToString(Document &d)
{
	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	d.Accept(writer);

	return buffer.GetString();
}


//{ "event": "subscribe", "channel": "book",  "symbol": "tBTCUSD" }
std::string MDEngineCoinfloor::createBookJsonString(int num, int base, int quote)
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


void MDEngineCoinfloor::loop()
{
	while (isRunning)
	{
		int n = lws_service(context, rest_get_interval_ms);
		std::cout << " 3.1415 loop() lws_service (n)" << n << std::endl;
	}
}

BOOST_PYTHON_MODULE(libcoinfloormd)
{
	using namespace boost::python;
	class_<MDEngineCoinfloor, boost::shared_ptr<MDEngineCoinfloor> >("Engine")
		.def(init<>())
		.def("init", &MDEngineCoinfloor::initialize)
		.def("start", &MDEngineCoinfloor::start)
		.def("stop", &MDEngineCoinfloor::stop)
		.def("logout", &MDEngineCoinfloor::logout)
		.def("wait_for_stop", &MDEngineCoinfloor::wait_for_stop);
}


