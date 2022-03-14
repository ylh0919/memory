// https://ascendex.github.io/ascendex-pro-api/
#include "MDEngineAscendEX.h"
#include "../../utils/common/ld_utils.h"
#include <stringbuffer.h>
#include <writer.h>
#include <document.h>
#include <libwebsockets.h>
#include <algorithm>
#include <stdio.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <cpr/cpr.h>

using cpr::Get;
using cpr::Url;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;

using rapidjson::Document;
using namespace rapidjson;
using namespace kungfu;
using namespace std;

WC_NAMESPACE_START

//lws event function
static int lwsEventCallback(struct lws* conn, enum lws_callback_reasons reason, void* user, void* data, size_t len);
static  struct lws_protocols  lwsProtocols[]{ {"md-protocol", lwsEventCallback, 0, 65536,}, { NULL, NULL, 0, 0 } };

MDEngineAscendEX* MDEngineAscendEX::m_instance = nullptr;

MDEngineAscendEX::MDEngineAscendEX() : IMDEngine(SOURCE_ASCENDEX)
{
	logger = yijinjing::KfLog::getLogger("MdEngine.AscendEX");
	KF_LOG_DEBUG(logger, "MDEngineAscendEX construct");
	timer = getTimestamp();/*edited by zyy*/
}

MDEngineAscendEX::~MDEngineAscendEX()
{
	if (m_thread)
	{
		if (m_thread->joinable())
		{
			m_thread->join();
		}
	}
	KF_LOG_DEBUG(logger, "MDEngineAscendEX deconstruct");
}

void MDEngineAscendEX::set_reader_thread()
{
	IMDEngine::set_reader_thread();
	m_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineAscendEX::lwsEventLoop, this)));

	if (need_get_snapshot_via_rest)
		rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineAscendEX::rest_loop, this)));
	else
		KF_LOG_INFO(logger, "MDEngineAscendEX::set_reader_thread, won't start rest_loop");
}

void MDEngineAscendEX::load(const json& config)
{
	KF_LOG_INFO(logger, "load config start");
	try
	{
		//get_snapshot_via_rest
		if (config.find("need_get_snapshot_via_rest") != config.end())
			need_get_snapshot_via_rest = config["need_get_snapshot_via_rest"].get<bool>();
		if (config.find("rest_get_interval_ms") != config.end())
			rest_get_interval_ms = config["rest_get_interval_ms"].get<int>();

		//subscrobe two klines
		if (config.find("kline_a_interval_s") != config.end())
			kline_a_interval = config["kline_a_interval_s"].get<int64_t>() / 60;
		if (config.find("kline_b_interval_s") != config.end())
			kline_b_interval = config["kline_b_interval_s"].get<int64_t>() / 60;
		KF_LOG_INFO(logger, "kline_a_interval " << kline_a_interval << " kline_b_interval " << kline_b_interval);
		//diff
		m_priceBookNum = config["book_depth_count"].get<int>();
		//if (!parseAddress(config["exchange_url"].get<std::string>())) return;
		m_whiteList.ReadWhiteLists(config, "whiteLists");

		m_whiteList.Debug_print();
		genSubscribeString();
	}
	catch (const std::exception& e)
	{
		KF_LOG_INFO(logger, "load config exception," << e.what());
	}
	/*edited by zyy,starts here*/
	if (config.find("level_threshold") != config.end()) {
		level_threshold = config["level_threshold"].get<int>();
	}
	if (config.find("refresh_normal_check_book_s") != config.end()) {
		refresh_normal_check_book_s = config["refresh_normal_check_book_s"].get<int>();
	}
	/*edited by zyy ends here*/
	KF_LOG_INFO(logger, "load config end");
}

void MDEngineAscendEX::genSubscribeString()
{
	auto& symbol_map = m_whiteList.GetKeyIsStrategyCoinpairWhiteList();
	for (const auto& var : symbol_map)
	{
		m_subcribeJsons.push_back(genDepthString(var.second));
		m_subcribeJsons.push_back(genTradeString(var.second));
		//subscribe two kline
		if (kline_a_interval != kline_b_interval) {
			m_subcribeJsons.push_back(genKlineString(var.second, kline_a_interval));
			m_subcribeJsons.push_back(genKlineString(var.second, kline_b_interval));
		}
		else
			m_subcribeJsons.push_back(genKlineString(var.second, kline_a_interval));
	}
	if (m_subcribeJsons.empty())
	{
		KF_LOG_INFO(logger, "genSubscribeString failed, {error: has no white list}");
		exit(0);
	}
}

std::string MDEngineAscendEX::genDepthString(const std::string& symbol)
{
	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	writer.StartObject();
	writer.Key("op");
	writer.String("sub");
	writer.Key("id");
	//writer.String("abc123");
	writer.String(std::to_string(m_id++).c_str());
	writer.Key("ch");
	std::string sub_value("depth:");
	sub_value = sub_value + symbol;
	writer.String(sub_value.c_str());
	writer.EndObject();
	return buffer.GetString();
}

std::string MDEngineAscendEX::genTradeString(const std::string& symbol)
{
	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	writer.StartObject();
	writer.Key("op");
	writer.String("sub");
	writer.Key("id");
	//writer.String("abc123");
	writer.String(std::to_string(m_id++).c_str());
	writer.Key("ch");
	std::string sub_value("trades:");
	sub_value = sub_value + symbol;
	writer.String(sub_value.c_str());
	writer.EndObject();
	return buffer.GetString();
}

//default 1 min
std::string MDEngineAscendEX::genKlineString(const std::string& symbol)
{
	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	writer.StartObject();
	writer.Key("op");
	writer.String("sub");
	writer.Key("id");
	//writer.String("abc123");
	writer.String(std::to_string(m_id++).c_str());
	writer.Key("ch");
	std::string sub_value("bar:1:");
	sub_value = sub_value + symbol;
	writer.String(sub_value.c_str());
	writer.EndObject();
	return buffer.GetString();
}

std::string MDEngineAscendEX::genKlineString(const std::string& symbol, int interval)
{
	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	writer.StartObject();
	writer.Key("op");
	writer.String("sub");
	writer.Key("id");
	//writer.String("abc123");
	writer.String(std::to_string(m_id++).c_str());
	writer.Key("ch");
	std::string sub_value("bar:");
	sub_value = sub_value + to_string(interval) + ":" + symbol;
	writer.String(sub_value.c_str());
	writer.EndObject();
	return buffer.GetString();
}

std::string MDEngineAscendEX::genPongString()
{
	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	writer.StartObject();
	writer.Key("op");
	writer.String("pong");
	writer.EndObject();
	return buffer.GetString();
}

void MDEngineAscendEX::connect(long)
{
	m_connected = true;
}

void MDEngineAscendEX::login(long)
{
	KF_LOG_DEBUG(logger, "create context start");
	m_instance = this;
	struct lws_context_creation_info creation_info;
	memset(&creation_info, 0x00, sizeof(creation_info));
	creation_info.port = CONTEXT_PORT_NO_LISTEN;
	creation_info.protocols = lwsProtocols;
	creation_info.gid = -1;
	creation_info.uid = -1;
	creation_info.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
	creation_info.max_http_header_pool = 1024;
	creation_info.fd_limit_per_thread = 1024;
	m_lwsContext = lws_create_context(&creation_info);
	if (!m_lwsContext)
	{
		KF_LOG_ERROR(logger, "create context error");
		return;
	}
	KF_LOG_INFO(logger, "create context success");
	createConnection();
}

void MDEngineAscendEX::createConnection()
{
	KF_LOG_DEBUG(logger, "create connect start");
	struct lws_client_connect_info conn_info = { 0 };
	//parse uri
	conn_info.context = m_lwsContext;
	conn_info.protocol = "wss";
	conn_info.address = "ascendex.com";
	conn_info.port = 443;

	string path = ("/");
	path += to_string(accountGroup) + "/api/pro/v1/stream";
	conn_info.path = path.c_str();
	conn_info.host = conn_info.address;
	conn_info.origin = conn_info.address;
	conn_info.ietf_version_or_minus_one = -1;
	conn_info.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
	m_lwsConnection = lws_client_connect_via_info(&conn_info);
	if (!m_lwsConnection)
	{
		KF_LOG_INFO(logger, "create connect error");
		return;
	}
	KF_LOG_INFO(logger, "connect to " << conn_info.protocol << "://" << conn_info.address << conn_info.path << ":" << conn_info.port << " success");
	m_logged_in = true;
}

void MDEngineAscendEX::logout()
{
	lws_context_destroy(m_lwsContext);
	m_logged_in = false;
	KF_LOG_INFO(logger, "logout");
}

void MDEngineAscendEX::lwsEventLoop()
{
	while (isRunning)
	{
		/*edited by zyy,starts here*/
		int64_t now = getTimestamp();
		KF_LOG_INFO(logger, "now = " << now << ",timer = " << timer << ", refresh_normal_check_book_s=" << refresh_normal_check_book_s);
		if ((now - timer) > refresh_normal_check_book_s * 1000)
		{
			KF_LOG_INFO(logger, "failed price book update");
			write_errormsg(114, "orderbook max refresh wait time exceeded");
			timer = now;
		}
		/*edited by zyy ends here*/
		lws_service(m_lwsContext, 500);
	}
}

void MDEngineAscendEX::sendMessage(std::string&& msg)
{
	msg.insert(msg.begin(), LWS_PRE, 0x00);
	lws_write(m_lwsConnection, (uint8_t*)(msg.data() + LWS_PRE), msg.size() - LWS_PRE, LWS_WRITE_TEXT);
}

void MDEngineAscendEX::onMessage(struct lws* conn, char* data, size_t len)
{
	KF_LOG_DEBUG(logger, "received data from ascendex start");
	try
	{
		if (!isRunning)
			return;

		KF_LOG_DEBUG(logger, "received data from ascendex,{msg:" << data << "}");
		Document json;
		json.Parse(data);

		if (json.HasParseError())
		{
			KF_LOG_ERROR(logger, "received data from ascendex failed,json parse error");
			return;
		}

		if (json.HasMember("m")) {
			string m = json["m"].GetString();
			if (m == "ping")
				parsePingMsg(json);
			else if (m == "sub")
				//rsp sub ok;
				parseRspSubscribe(json);
			else if (m == "error")
				//rsp sub error;
				parseErrSubscribe(json);
			else
				//rtn sub
				parseSubscribeData(json, m);
		}
	}
	catch (const std::exception& e)
	{
		KF_LOG_ERROR(logger, "received data from ascendex exception,{error:" << e.what() << "}");
	}
	catch (...)
	{
		KF_LOG_ERROR(logger, "received data from ascendex system exception");
	}
	KF_LOG_DEBUG(logger, "received data from ascendex end");
}

void MDEngineAscendEX::onClose(struct lws* conn)
{
	if (isRunning)
	{
		reset();
		login(0);
	}
	if (!m_logged_in)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(2000));
	}
}

void MDEngineAscendEX::reset()
{
	m_subcribeIndex = 0;
	m_logged_in = false;
}

void MDEngineAscendEX::onWrite(struct lws* conn)
{
	if (!isRunning)
	{
		return;
	}
	KF_LOG_DEBUG(logger, "subcribe start");
	if (m_subcribeJsons.empty() || m_subcribeIndex == -1)
	{
		KF_LOG_DEBUG(logger, "subcribe ignore");
		return;
	}
	auto symbol = m_subcribeJsons[m_subcribeIndex++];
	KF_LOG_DEBUG(logger, "req subcribe " << symbol);
	sendMessage(std::move(symbol));
	if (m_subcribeIndex >= m_subcribeJsons.size())
	{
		m_subcribeIndex = -1;
		KF_LOG_DEBUG(logger, "subcribe end");
		return;
	}
	if (isRunning)
	{
		lws_callback_on_writable(conn);
	}
	KF_LOG_DEBUG(logger, "subcribe continue");
}

void MDEngineAscendEX::parsePingMsg(const rapidjson::Document& json)
{
	//{ "m": "ping", "hp": 3 }  # Server pushed ping message
	//{ "op": "pong" }   # Client responds with pong
	try
	{
		int hp = json["hp"].GetInt();
		auto pong = genPongString();
		KF_LOG_DEBUG(logger, "send pong msg to server, hp: " << hp << ",{ pong:" << pong << " }");
		sendMessage(std::move(pong));
	}
	catch (const std::exception& e)
	{
		KF_LOG_INFO(logger, "parsePingMsg,{error:" << e.what() << "}");
	}
}

//right subcribe rsp
//{"m":"sub", "id" : "abc23g", "ch" : "summary:BTC/USDT,ASD/USDT", "code" : 0}
void MDEngineAscendEX::parseRspSubscribe(const rapidjson::Document& json)
{
	KF_LOG_DEBUG(logger, "subscribe successfully {ch :" << json["ch"].GetString() << "}");
}

/*
* error subcribe rsp
{
	"m":      "error",
	"id":     "ab123",
	"code":   100005,
	"reason": "INVALID_WS_REQUEST_DATA",
	"info":   "Invalid request action: trade-snapshot"
}
 */
void MDEngineAscendEX::parseErrSubscribe(const rapidjson::Document& json)
{
	KF_LOG_INFO(logger, "subscribe sysmbol error, (code)" << json["code"].GetInt()
		<< " (reason)" << json["reason"].GetString()
		<< " (info)" << json["info"].GetString()
		<< " (id)" << json["id"].GetString()
	);
}

void MDEngineAscendEX::parseSubscribeData(const rapidjson::Document& json, const std::string& m)
{
	string symbol;
	if (json.HasMember("symbol"))
		symbol = json["symbol"].GetString();
	else if (json.HasMember("s"))
		symbol = json["s"].GetString();
	string instrument = m_whiteList.GetKeyByValue(symbol);
	if (instrument.empty())
		KF_LOG_DEBUG(logger, "[parseSubscribeData] coundn't find the symbol in whitelist {symbol :" << symbol << "}");

	if (m == "depth") {
		doDepthData(json, instrument);
		return;
	}
	else if (m == "trades") {
		doTradeData(json, instrument);
		return;
	}
	else if (m == "bar") {
		doKlineData(json, instrument);
		return;
	}
	else if (m == "connected") {
		KF_LOG_DEBUG(logger, "[parseSubscribeData] connected");
		return;
	}
	else
		//"bbo" or other
		KF_LOG_ERROR(logger, "[parseSubscribeData] hasn't handle with the json, m:" << m);
}

void MDEngineAscendEX::doDepthData(const rapidjson::Document& json, const std::string& instrument)
{
	try
	{
		KF_LOG_DEBUG(logger, "doDepthData start");
		if (!json.HasMember("data") || !json["data"].HasMember("asks") || !json["data"].HasMember("bids")) {
			KF_LOG_ERROR(logger, "[doDepthData] the json is abnormal, return");
			return;
		}

		auto& bids = json["data"]["bids"];
		auto& asks = json["data"]["asks"];

		LFPriceBook20Field priceBook{ 0 };
		strncpy(priceBook.ExchangeID, "ascendex", std::min<size_t>(sizeof(priceBook.ExchangeID) - 1, 5));
		strncpy(priceBook.InstrumentID, instrument.c_str(), std::min(sizeof(priceBook.InstrumentID) - 1, instrument.size()));

		if (bids.IsArray())
		{
			int i = 0, j = 0;//i for json, j for pricebook
			for (i = 0; i < std::min((int)bids.Size(), m_priceBookNum); ++i)
			{
				uint64_t volume = std::round(std::stod(bids[i][1].GetString()) * scale_offset);
				//if size is zero, you should delete the level at price
				if (volume != 0) {
					priceBook.BidLevels[j].price = std::round(std::stod(bids[i][0].GetString()) * scale_offset);
					priceBook.BidLevels[j].volume = volume;
					j++;
				}
			}
			priceBook.BidLevelCount = j;
		}
		if (asks.IsArray())
		{
			int i = 0, j = 0;//i for json, j for pricebook
			for (i = 0; i < std::min((int)asks.Size(), m_priceBookNum); ++i)
			{
				uint64_t volume = std::round(std::stod(asks[i][1].GetString()) * scale_offset);
				//if size is zero, you should delete the level at price
				if (volume != 0) {
					priceBook.AskLevels[j].price = std::round(std::stod(asks[i][0].GetString()) * scale_offset);
					priceBook.AskLevels[j].volume = volume;
					j++;
				}
			}
			priceBook.AskLevelCount = j;
		}

		/*edited by zyy,starts here*/
		timer = getTimestamp();
		/*
		if (priceBook.BidLevelCount < level_threshold || priceBook.AskLevelCount < level_threshold)
		{
			KF_LOG_INFO(logger, "onBook: failed,level count < level threshold :" << priceBook.BidLevelCount << " " << priceBook.AskLevelCount << " " << level_threshold);
			string errorMsg = "orderbook level below threshold";
			write_errormsg(112, errorMsg);
			on_price_book_update(&priceBook);
		}
		else*/ if (priceBook.BidLevels[0].price <= 0 || priceBook.AskLevels[0].price <= 0 || priceBook.BidLevels[0].price > priceBook.AskLevels[0].price)
		{
			string errorMsg = "orderbook crossed";
			write_errormsg(113, errorMsg);
		}
		/*edited by zyy ends here*/
		else
		{
			if (json["data"].HasMember("ts"))
				priceBook.UpdateMicroSecond = json["data"]["ts"].GetInt64();
			on_price_book_update(&priceBook);
		}
	}
	catch (const std::exception& e)
	{
		KF_LOG_INFO(logger, "doDepthData,{error:" << e.what() << "}");
	}
	KF_LOG_DEBUG(logger, "doDepthData end");
}

void MDEngineAscendEX::doTradeData(const rapidjson::Document& json, const std::string& instrument)
{
	try
	{
		KF_LOG_DEBUG(logger, "doTradeData start");
		if (!json.HasMember("data"))
			return;
		auto& data = json["data"];
		if (data.Empty())
			return;

		LFL2TradeField trade{ 0 };
		strncpy(trade.ExchangeID, "AscendEX", std::min((int)sizeof(trade.ExchangeID) - 1, 5));
		strncpy(trade.InstrumentID, instrument.c_str(), std::min(sizeof(trade.InstrumentID) - 1, instrument.size()));

		for (int i = 0; i < data.Size(); i++) {
			auto& part_data = data[i];
			trade.TimeStamp = part_data["ts"].GetInt64();
			string strTime = timestamp_to_formatISO8601(trade.TimeStamp);
			strncpy(trade.TradeTime, strTime.c_str(), sizeof(trade.TradeTime));
			trade.TimeStamp *= 1000000;

			//trade.Volume = std::round(part_data["q"].GetDouble() * scale_offset);
			//trade.Price = std::round(part_data["p"].GetDouble() * scale_offset);
			trade.Volume = std::round(std::stod(part_data["q"].GetString()) * scale_offset);
			trade.Price = std::round(std::stod(part_data["p"].GetString()) * scale_offset);
			trade.OrderBSFlag[0] = (part_data["bm"].GetBool()) ? 'B' : 'S';
			on_trade(&trade);
		}
	}
	catch (const std::exception& e)
	{
		KF_LOG_INFO(logger, "doTradeData,{error:" << e.what() << "}");
	}
	KF_LOG_DEBUG(logger, "doTradeData end");
}

void MDEngineAscendEX::doKlineData(const rapidjson::Document& json, const std::string& instrument)
{
	try
	{
		KF_LOG_DEBUG(logger, "doKilneData start");
		if (!json.HasMember("data"))
			return;
		auto& data = json["data"];

		auto& tickKLine = mapKLines[instrument];
		LFBarMarketDataField market;
		memset(&market, 0, sizeof(market));
		strcpy(market.InstrumentID, instrument.c_str());
		strcpy(market.ExchangeID, "AscendEX");

		///注意time检查
		time_t now = time(0);
		struct tm tradeday = *localtime(&now);
		strftime(market.TradingDay, 9, "%Y%m%d", &tradeday);

		int64_t nStartTime = data["ts"].GetInt64();
		market.StartUpdateMillisec = nStartTime;
		struct tm start_tm = *localtime((time_t*)(&nStartTime));
		sprintf(market.StartUpdateTime, "%02d:%02d:%02d.000", start_tm.tm_hour, start_tm.tm_min, start_tm.tm_sec);

		market.PeriodMillisec = std::stoi(data["i"].GetString()) * 60000;
		market.EndUpdateMillisec = market.StartUpdateMillisec + market.PeriodMillisec - 1;
		int64_t nEndTime = nStartTime + market.PeriodMillisec / 1000 - 1;
		struct tm end_tm = *localtime((time_t*)(&nEndTime));
		sprintf(market.EndUpdateTime, "%02d:%02d:%02d.999", end_tm.tm_hour, end_tm.tm_min, end_tm.tm_sec);

		market.Open = std::round(std::stod(data["o"].GetString()) * scale_offset);
		market.Close = std::round(std::stod(data["c"].GetString()) * scale_offset);
		market.Low = std::round(std::stod(data["l"].GetString()) * scale_offset);
		market.High = std::round(std::stod(data["h"].GetString()) * scale_offset);
		market.Volume = std::round(std::stod(data["v"].GetString()) * scale_offset);

		auto it = tickKLine.find(nStartTime);
		if (tickKLine.size() == 0 || it != tickKLine.end())
		{
			tickKLine[nStartTime] = market;
			KF_LOG_INFO(logger, "doKlineData(cached): StartUpdateMillisec " << market.StartUpdateMillisec << " StartUpdateTime " << market.StartUpdateTime << " EndUpdateMillisec " << market.EndUpdateMillisec << " EndUpdateTime " << market.EndUpdateTime
				<< "Open" << market.Open << " Close " << market.Close << " Low " << market.Low << " Volume " << market.Volume);
		}
		else
		{
			on_market_bar_data(&(tickKLine.begin()->second));
			tickKLine.clear();
			tickKLine[nStartTime] = market;
			KF_LOG_INFO(logger, "doKlineData: StartUpdateMillisec " << market.StartUpdateMillisec << " StartUpdateTime " << market.StartUpdateTime << " EndUpdateMillisec " << market.EndUpdateMillisec << " EndUpdateTime " << market.EndUpdateTime
				<< "Open" << market.Open << " Close " << market.Close << " Low " << market.Low << " Volume " << market.Volume);
		}
	}
	catch (const std::exception& e)
	{
		KF_LOG_INFO(logger, "doKlineData,{error:" << e.what() << "}");
	}
	KF_LOG_DEBUG(logger, "doKlineData end");
}

void MDEngineAscendEX::get_snapshot_via_rest()
{
	std::unordered_map<std::string, std::string>::iterator map_itr;
	for (map_itr = m_whiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); map_itr != m_whiteList.GetKeyIsStrategyCoinpairWhiteList().end(); map_itr++)
	{
		std::string url = "https://ascendex.com/api/pro/v1/depth?symbol=";
		url += map_itr->second;
		cpr::Response response = Get(Url{ url.c_str() }, Parameters{});
		Document full_json;
		full_json.Parse(response.text.c_str());
		KF_LOG_INFO(logger, "get_snapshot_via_rest get(" << url << "):" << response.text);

		if (!full_json.HasMember("data")) {
			KF_LOG_ERROR(logger, "[get_snapshot_via_rest] the json is abnormal, return");
			return;
		}
		auto& data = full_json["data"];

		try
		{
			if (!data.HasMember("data") || !data["data"].HasMember("asks") || !data["data"].HasMember("bids")) {
				KF_LOG_ERROR(logger, "[get_snapshot_via_rest] the json is abnormal, return");
				return;
			}

			auto& bids = data["data"]["bids"];
			auto& asks = data["data"]["asks"];

			LFPriceBook20Field priceBook{ 0 };
			strncpy(priceBook.ExchangeID, "ascendex", std::min<size_t>(sizeof(priceBook.ExchangeID) - 1, 5));
			strncpy(priceBook.InstrumentID, map_itr->first.c_str(), std::min(sizeof(priceBook.InstrumentID) - 1, map_itr->first.size()));
			if (bids.IsArray())
			{
				int i = 0, j = 0;//i for json, j for pricebook
				for (i = 0; i < std::min((int)bids.Size(), m_priceBookNum); ++i)
				{
					uint64_t volume = std::round(std::stod(bids[i][1].GetString()) * scale_offset);
					//if size is zero, you should delete the level at price
					if (volume != 0) {
						priceBook.BidLevels[j].price = std::round(std::stod(bids[i][0].GetString()) * scale_offset);
						priceBook.BidLevels[j].volume = volume;
						j++;
					}
				}
				priceBook.BidLevelCount = j;
			}
			if (asks.IsArray())
			{
				int i = 0, j = 0;//i for json, j for pricebook
				for (i = 0; i < std::min((int)asks.Size(), m_priceBookNum); ++i)
				{
					uint64_t volume = std::round(std::stod(asks[i][1].GetString()) * scale_offset);
					//if size is zero, you should delete the level at price
					if (volume != 0) {
						priceBook.AskLevels[j].price = std::round(std::stod(asks[i][0].GetString()) * scale_offset);
						priceBook.AskLevels[j].volume = volume;
						j++;
					}
				}
				priceBook.AskLevelCount = j;
			}


			/*edited by zyy,starts here*/
			/*
			if (priceBook.BidLevelCount < level_threshold || priceBook.AskLevelCount < level_threshold)
			{
				KF_LOG_INFO(logger, "get_snapshot_via_rest: failed,level count < level threshold :" << priceBook.BidLevelCount << " " << priceBook.AskLevelCount << " " << level_threshold);
				string errorMsg = "orderbook level below threshold";
				write_errormsg(112, errorMsg);
				on_price_book_update(&priceBook);
			}

			else */if (priceBook.BidLevels[0].price <= 0 || priceBook.AskLevels[0].price <= 0 || priceBook.BidLevels[0].price > priceBook.AskLevels[0].price)
			{
				string errorMsg = "orderbook crossed";
				write_errormsg(113, errorMsg);
			}
			/*edited by zyy ends here*/
			else
			{
				if (data["data"].HasMember("ts"))
					priceBook.UpdateMicroSecond = data["data"]["ts"].GetInt64();
				on_price_book_update_from_rest(&priceBook);
			}
		}
		catch (const std::exception& e)
		{
			KF_LOG_INFO(logger, "get_snapshot_via_rest,{error:" << e.what() << "}");
		}
		KF_LOG_DEBUG(logger, "get_snapshot_via_rest end");
	}
}


int64_t last_rest_time = 0;
void MDEngineAscendEX::rest_loop()
{
	while (isRunning)
	{
		int64_t now = getTimestamp();
		if ((now - last_rest_time) >= rest_get_interval_ms)
		{
			last_rest_time = now;
			get_snapshot_via_rest();
		}
	}
}

int lwsEventCallback(struct lws* conn, enum lws_callback_reasons reason, void*, void* data, size_t len)
{
	switch (reason)
	{
	case LWS_CALLBACK_CLIENT_ESTABLISHED:
	{
		lws_callback_on_writable(conn);
		break;
	}
	case LWS_CALLBACK_CLIENT_RECEIVE:
	{
		if (MDEngineAscendEX::m_instance)
		{
			MDEngineAscendEX::m_instance->onMessage(conn, (char*)data, len);
		}
		break;
	}
	case LWS_CALLBACK_CLIENT_WRITEABLE:
	{
		if (MDEngineAscendEX::m_instance)
		{
			MDEngineAscendEX::m_instance->onWrite(conn);
		}
		break;
	}
	case LWS_CALLBACK_CLIENT_CLOSED:
	case LWS_CALLBACK_CLOSED:
	case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
	{
		if (MDEngineAscendEX::m_instance)
		{
			MDEngineAscendEX::m_instance->onClose(conn);
		}
		break;
	}
	default:
		break;
	}
	return 0;
}

/*edited by zyy,starts here*/
inline int64_t MDEngineAscendEX::getTimestamp()
{   /*返回的是毫秒*/
	long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return timestamp;
}
/*edited by zyy ends here*/

BOOST_PYTHON_MODULE(libascendexmd)
{
	using namespace boost::python;
	class_<MDEngineAscendEX, boost::shared_ptr<MDEngineAscendEX> >("Engine")
		.def(init<>())
		.def("init", &MDEngineAscendEX::initialize)
		.def("start", &MDEngineAscendEX::start)
		.def("stop", &MDEngineAscendEX::stop)
		.def("logout", &MDEngineAscendEX::logout)
		.def("wait_for_stop", &MDEngineAscendEX::wait_for_stop);
}

WC_NAMESPACE_END