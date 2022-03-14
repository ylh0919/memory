#include "TDEngineAscendEX.h"
#include "longfist/ctp.h"
#include "longfist/LFUtils.h"
#include "TypeConvert.hpp"
#include <boost/algorithm/string.hpp>

#include <writer.h>
#include <stringbuffer.h>
#include <document.h>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <cpr/cpr.h>
#include <chrono>
#include "../../utils/crypto/openssl_util.h"

using cpr::Delete;
using cpr::Get;
using cpr::Url;
using cpr::Body;
using cpr::Header;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;
using cpr::Timeout;

using rapidjson::StringRef;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using std::string;
using std::to_string;
using std::stod;
using std::stoi;
using utils::crypto::hmac_sha256;
using utils::crypto::hmac_sha256_byte;
using utils::crypto::base64_encode;
using utils::crypto::base64_decode;

USING_WC_NAMESPACE

static TDEngineAscendEX* global_td = nullptr;

static int ws_service_cb(struct lws* wsi, enum lws_callback_reasons reason, void* user, void* in, size_t len)
{
	switch (reason)
	{
	case LWS_CALLBACK_CLIENT_ESTABLISHED:
	{
		global_td->on_lws_open(wsi);
		break;
	}
	case LWS_CALLBACK_PROTOCOL_INIT:
	{
		break;
	}
	case LWS_CALLBACK_CLIENT_RECEIVE:
	{
		if (global_td)
		{
			global_td->on_lws_data(wsi, (char*)in, len);
		}
		break;
	}
	case LWS_CALLBACK_CLIENT_WRITEABLE:
	{
		int ret = 0;
		if (global_td)
		{
			ret = global_td->on_lws_write_subscribe(wsi);
		}
		break;
	}
	case LWS_CALLBACK_CLOSED:
	{
		global_td->on_lws_close(wsi);
		break;
	}
	case LWS_CALLBACK_WSI_DESTROY:
	case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
	{
		if (global_td)
		{
			global_td->on_lws_connection_error(wsi);
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
		"td-protocol",
		ws_service_cb,
		0,
		65536,
	},
	{ NULL, NULL, 0, 0 } /* terminator */
};

std::mutex http_mutex;

AccountUnitAscendEX::AccountUnitAscendEX() {
	mutex_new_order = new std::mutex();
	mutex_rtn_order = new std::mutex();
	mutex_orderref_map = new std::mutex();
}

AccountUnitAscendEX::AccountUnitAscendEX(const AccountUnitAscendEX& source)
{
	api_key = source.api_key;
	secret_key = source.secret_key;
	baseUrl = source.baseUrl;
	// internal flags
	logged_in = source.logged_in;
	is_connecting = source.is_connecting;
	// ws flag
	ws_auth = source.ws_auth;
	ws_sub_order = source.ws_sub_order;
	// map
	sendOrderFilters = source.sendOrderFilters;
	orderRefMap = source.orderRefMap;
	newOrderMap = source.newOrderMap;
	rtnOrderMap = source.rtnOrderMap;
	// whitelist
	coinPairWhiteList = source.coinPairWhiteList;
	positionWhiteList = source.positionWhiteList;
	// mutex
	mutex_new_order = new std::mutex();
	mutex_rtn_order = new std::mutex();
	mutex_orderref_map = new std::mutex();
	// websocket
	context = source.context;
	websocketConn = source.websocketConn;
	sendmessage = source.sendmessage;
}

AccountUnitAscendEX::~AccountUnitAscendEX() {
	if (mutex_new_order != nullptr) delete mutex_new_order;
	if (mutex_rtn_order != nullptr) delete mutex_rtn_order;
	if (mutex_orderref_map != nullptr) delete mutex_orderref_map;
}

TDEngineAscendEX::TDEngineAscendEX() : ITDEngine(SOURCE_ASCENDEX)
{
	logger = yijinjing::KfLog::getLogger("TradeEngine.AscendEX");
	KF_LOG_INFO(logger, "[TDEngineAscendEX]");
}

TDEngineAscendEX::~TDEngineAscendEX()
{
	if (m_ThreadPoolPtr != nullptr) delete m_ThreadPoolPtr;
}

void TDEngineAscendEX::init()
{
	ITDEngine::init();
	JournalPair tdRawPair = getTdRawJournalPair(source_id);
	raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
	KF_LOG_INFO(logger, "[init]");
}

void TDEngineAscendEX::pre_load(const json& j_config)
{
	KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineAscendEX::resize_accounts(int account_num)
{
	account_units.resize(account_num);
	KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineAscendEX::load_account(int idx, const json& j_config)
{
	KF_LOG_INFO(logger, "[load_account]");
	// internal load
	string api_key = j_config["APIKey"].get<string>();
	string secret_key = j_config["SecretKey"].get<string>();

	//url
	string baseUrl = "";
	if (j_config.find("baseUrl") != j_config.end())
		baseUrl = j_config["baseUrl"].get<string>();
	else
		baseUrl = restBaseUrl;
	if (j_config.find("wsBaseUrl") != j_config.end())
		wsBaseUrl = j_config["wsBaseUrl"].get<string>();

	rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

	if (j_config.find("is_margin") != j_config.end())
		isMargin = j_config["is_margin"].get<bool>();

	if (j_config.find("rest_wait_time_ms") != j_config.end())
		rest_wait_time_ms = j_config["rest_wait_time_ms"].get<int>();

	AccountUnitAscendEX& unit = account_units[idx];
	unit.api_key = api_key;
	unit.secret_key = secret_key;
	unit.baseUrl = baseUrl;

	KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl);

	//
	int thread_pool_size = 0;
	if (j_config.find("thread_pool_size") != j_config.end()) {
		thread_pool_size = j_config["thread_pool_size"].get<int>();
	}
	if (thread_pool_size > 0)
	{
		m_ThreadPoolPtr = new ThreadPool(thread_pool_size);
	}

	unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
	unit.coinPairWhiteList.Debug_print();

	unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
	unit.positionWhiteList.Debug_print();

	//display usage:
	if (unit.coinPairWhiteList.Size() == 0) {
		KF_LOG_ERROR(logger, "TDEngineAscendEX::load_account: subscribeAscendEXBaseQuote is empty. please add whiteLists in kungfu.json like this :");
		KF_LOG_ERROR(logger, "\"whiteLists\":{");
		KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
		KF_LOG_ERROR(logger, "    \"btc_usdt\": \"BTC/USDT\",");
		KF_LOG_ERROR(logger, "     \"etc_eth\": \"ETC/ETH\"");
		KF_LOG_ERROR(logger, "},");
	}

	//set flag
	unit.ws_auth = false;
	unit.ws_sub_order = false;

	//set accountGroup before using other restful api, only for ascendex
	if (set_account_info(unit))
		KF_LOG_INFO(logger, "set accountGroup successfully");
	else
		KF_LOG_INFO(logger, "set_account_info failed");

	//cancel all openning orders on TD startup, list open order
	Document d;
	query_orders(unit, d);
	KF_LOG_INFO(logger, "[load_account] print get_open_orders");
	printResponse(d);

	//cancel all openning orders on TD startup, cancel_all_orders
	Document cancelResponse;
	cancel_all_orders(unit, cancelResponse);

	int errorId = 0;
	std::string errorMsg = "";
	if (cancelResponse.HasParseError() || !cancelResponse.IsObject() || !cancelResponse.HasMember("data"))
	{
		errorId = 100;
		errorMsg = "cancel_all_orders http response has parse error. please check the log";
		KF_LOG_ERROR(logger, "[load_account] cancel_all_orders error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
	}
	else if (cancelResponse["data"].HasMember("status") && strcmp("Ack", cancelResponse["data"]["status"].GetString()) == 0) {
		KF_LOG_INFO(logger, "[load_account] cancel_all_orders success!");
	}
	printResponse(cancelResponse);
	// cancel_all_orders end 

	//test
	Document accountJson;
	get_account(unit, accountJson);
	KF_LOG_INFO(logger, "[load_account] get_account:");
	printResponse(accountJson);

	// set up
	TradeAccount account = {};
	//partly copy this fields
	strncpy(account.UserID, api_key.c_str(), 16);
	strncpy(account.Password, secret_key.c_str(), 21);
	return account;
}

void TDEngineAscendEX::connect(long timeout_nsec)
{
	KF_LOG_INFO(logger, "[connect]");
	for (int idx = 0; idx < account_units.size(); idx++)
	{
		AccountUnitAscendEX& unit = account_units[idx];
		KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
		if (!unit.logged_in)
		{
			//exchange infos
			Document doc;
			get_products(unit, doc);
			KF_LOG_INFO(logger, "[connect] get_products");
			printResponse(doc);

			if (loadExchangeOrderFilters(unit, doc))
			{
				unit.logged_in = true;
				lws_login(unit, timeout_nsec);
			}
			else {
				KF_LOG_ERROR(logger, "[connect] logged_in = false for loadExchangeOrderFilters return false");
			}
			debug_print(unit.sendOrderFilters);
		}
	}
}

bool TDEngineAscendEX::loadExchangeOrderFilters(AccountUnitAscendEX& unit, Document& doc)
{
	KF_LOG_INFO(logger, "[loadExchangeOrderFilters]");
	if (doc.HasParseError() || !doc.HasMember("data") || doc["data"].IsObject()) {
		return false;
	}
	auto& data = doc["data"];
	if (data.IsArray())
	{
		size_t productCount = data.Size();
		for (size_t i = 0; i < productCount; i++) {
			const rapidjson::Value& prod = data.GetArray()[i];
			if (prod.IsObject() && prod.HasMember("tickSize")) {
				std::string symbol = prod["symbol"].GetString();
				string tickSizeStr = prod["tickSize"].GetString();

				KF_LOG_INFO(logger, "[loadExchangeOrderFilters] sendOrderFilters (symbol)" << symbol <<
					" (tickSizeStr)" << tickSizeStr);

				unsigned int locStart = tickSizeStr.find(".", 0);
				unsigned int locEnd = tickSizeStr.find("1", 0);
				if (locStart != string::npos && locEnd != string::npos) {
					int num = locEnd - locStart;
					SendOrderFilter afilter;
					memset(&afilter, 0, sizeof(SendOrderFilter));
					strncpy(afilter.InstrumentID, symbol.c_str(), 31);
					afilter.ticksize = num;
					unit.sendOrderFilters.insert(std::make_pair(symbol, afilter));
				}
			}
		}
	}
	return true;
}

void TDEngineAscendEX::debug_print(std::map<std::string, SendOrderFilter>& sendOrderFilters)
{
	std::map<std::string, SendOrderFilter>::iterator map_itr = sendOrderFilters.begin();
	while (map_itr != sendOrderFilters.end())
	{
		KF_LOG_INFO(logger, "[debug_print] sendOrderFilters (symbol)" << map_itr->first <<
			" (tickSize)" << map_itr->second.ticksize);
		map_itr++;
	}
}

SendOrderFilter TDEngineAscendEX::getSendOrderFilter(AccountUnitAscendEX& unit, const char* symbol)
{
	std::map<std::string, SendOrderFilter>::iterator map_itr = unit.sendOrderFilters.begin();
	while (map_itr != unit.sendOrderFilters.end())
	{
		if (strcmp(map_itr->first.c_str(), symbol) == 0)
		{
			return map_itr->second;
		}
		map_itr++;
	}
	//if not found
	SendOrderFilter defaultFilter;
	memset(&defaultFilter, 0, sizeof(SendOrderFilter));
	defaultFilter.ticksize = 8;
	strcpy(defaultFilter.InstrumentID, "notfound");
	return defaultFilter;
}

void TDEngineAscendEX::login(long timeout_nsec)
{
	KF_LOG_INFO(logger, "[login]");
	connect(timeout_nsec);
}

void TDEngineAscendEX::logout()
{
	KF_LOG_INFO(logger, "[logout]");
}

void TDEngineAscendEX::release_api()
{
	KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineAscendEX::is_logged_in() const
{
	KF_LOG_INFO(logger, "[is_logged_in]");
	for (auto& unit : account_units)
	{
		if (!unit.logged_in)
			return false;
	}
	return true;
}

bool TDEngineAscendEX::is_connected() const
{
	KF_LOG_INFO(logger, "[is_connected]");
	return is_logged_in();
}

std::string TDEngineAscendEX::GetSide(const LfDirectionType& input) {
	if (LF_CHAR_Buy == input) {
		return "buy";
	}
	else if (LF_CHAR_Sell == input) {
		return "sell";
	}
	else {
		return "";
	}
}

LfDirectionType TDEngineAscendEX::GetDirection(std::string input) {
	if ("buy" == input || "Buy" == input) {
		return LF_CHAR_Buy;
	}
	else if ("sell" == input || "Sell" == input) {
		return LF_CHAR_Sell;
	}
	else {
		return LF_CHAR_Buy;
	}
}

std::string TDEngineAscendEX::GetType(const LfOrderPriceTypeType& input) {
	if (LF_CHAR_LimitPrice == input) {
		return "limit";
	}
	else if (LF_CHAR_AnyPrice == input) {
		return "market";
	}
	else {
		return "";
	}
}

LfOrderPriceTypeType TDEngineAscendEX::GetPriceType(std::string input)
{
	//Limit/Market/StopMarket/StopLimit
	if ("Limit" == input || "limit" == input) {
		return LF_CHAR_LimitPrice;
	}
	else if ("Market" == input || "market" == input) {
		return LF_CHAR_AnyPrice;
	}
	else {
		return '0';
	}
}

LfOrderStatusType TDEngineAscendEX::GetOrderStatus(std::string input)
{
	//ACCEPT:	New, or PendingNew  
	//DONE:		Filled, PartiallyFilled, Cancelled, or Rejected
	//ERR:		Err
	if (input == "New" || input == "PendingNew")
		return LF_CHAR_NotTouched;

	else if (input == "Filled")
		return LF_CHAR_AllTraded;

	else if (input == "PartiallyFilled")
		return LF_CHAR_PartTradedQueueing;

	else if (input == "Cancelled" || input == "Canceled")
		return LF_CHAR_Canceled;

	else if (input == "Rejected")
		return LF_CHAR_NoTradeNotQueueing;

	else if (input == "Err")
		return LF_CHAR_Error;

	KF_LOG_INFO(logger, "[GetOrderStatus] unknow state, please check it (state)" << input);
	return LF_CHAR_Unknown;
}

int TDEngineAscendEX::GetTransferStatus(std::string input)
{
	//pending / reviewing / confirmed / rejected / canceled / failed
	if (input == "pending")
		return 0;
	else if (input == "reviewing")
		return 2;
	else if (input == "confirmed")
		return 6;
	else if (input == "rejected")
		return 3;
	else if (input == "canceled")
		return 1;
	else if (input == "failed")
		return 5;

	return -1;
}

/**
 * req functions
 */
void TDEngineAscendEX::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
	KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

	AccountUnitAscendEX& unit = account_units[account_index];
	KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key << " (InstrumentID) " << data->InstrumentID);

	//init
	LFRspPositionField pos;
	memset(&pos, 0, sizeof(LFRspPositionField));
	strncpy(pos.BrokerID, data->BrokerID, 11);
	strncpy(pos.InvestorID, data->InvestorID, 19);
	strncpy(pos.InstrumentID, data->InstrumentID, 31);
	pos.PosiDirection = LF_CHAR_Long;
	pos.HedgeFlag = LF_CHAR_Speculation;
	pos.Position = 0;
	pos.YdPosition = 0;
	pos.PositionCost = 0;

	//get_account
	int errorId = 0;
	std::string errorMsg = "";
	Document d;
	get_account(unit, d);

	//not expected response
	if (d.HasParseError() || !d.IsObject()) {
		errorId = 100;
		errorMsg = "get_account http response has parse error or is not json. please check the log";
		KF_LOG_ERROR(logger, "[req_investor_position] get_account error!  (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
	}
	else if (d.HasMember("status") && strcmp("Err", d["status"].GetString()) == 0)
	{
		errorId = 100;
		if (d.HasMember("message"))
			errorMsg = d["message"].GetString();
		KF_LOG_ERROR(logger, "[req_investor_position] get_account error!  (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
	}
	if (errorId > 0) {
		on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
		return;
	}

	send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_ASCENDEX, 1, requestId);

	std::vector<LFRspPositionField> tmp_vector;
	if (!d.HasParseError() && d.HasMember("data") && d["data"].IsArray())
	{
		size_t len = d["data"].Size();
		KF_LOG_INFO(logger, "[req_investor_position] (asset.length)" << len);
		for (int i = 0; i < len; i++)
		{
			std::string symbol = d["data"].GetArray()[i]["asset"].GetString();
			std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
			if (ticker.length() > 0) {
				strncpy(pos.InstrumentID, ticker.c_str(), 31);
				pos.Position = std::round(std::stod(d["data"].GetArray()[i]["availableBalance"].GetString()) * scale_offset);
				tmp_vector.push_back(pos);
				KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (symbol) " << symbol
					<< " availableBalance:" << d["data"].GetArray()[i]["availableBalance"].GetString()
					<< " totalBalance: " << d["data"].GetArray()[i]["totalBalance"].GetString());
			}
		}
	}

	bool findSymbolInResult = false;
	//send the filtered position
	int position_count = tmp_vector.size();
	for (int i = 0; i < position_count; i++)
	{
		on_rsp_position(&tmp_vector[i], i == (position_count - 1), requestId, errorId, errorMsg.c_str());
		findSymbolInResult = true;
	}

	if (!findSymbolInResult)
	{
		KF_LOG_INFO(logger, "[req_investor_position] (!findSymbolInResult) (requestId)" << requestId);
		on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	if (errorId != 0)
	{
		raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
	}
}

void TDEngineAscendEX::req_qry_account(const LFQryAccountField* data, int account_index, int requestId)
{
	KF_LOG_INFO(logger, "[req_qry_account]");
}

int64_t TDEngineAscendEX::fixPriceTickSize(int keepPrecision, int64_t price, bool isBuy) {
	if (keepPrecision == 8) return price;

	int removePrecisions = (8 - keepPrecision);
	double cutter = pow(10, removePrecisions);

	KF_LOG_INFO(logger, "[fixPriceTickSize input]" << " 1(price)" << std::fixed << std::setprecision(9) << price);
	double new_price = price / cutter;
	KF_LOG_INFO(logger, "[fixPriceTickSize input]" << " 2(price/cutter)" << std::fixed << std::setprecision(9) << new_price);
	if (isBuy) {
		new_price += 0.9;
		new_price = std::floor(new_price);
		KF_LOG_INFO(logger, "[fixPriceTickSize input]" << " 3(price is buy)" << std::fixed << std::setprecision(9) << new_price);
	}
	else {
		new_price = std::floor(new_price);
		KF_LOG_INFO(logger, "[fixPriceTickSize input]" << " 3(price is sell)" << std::fixed << std::setprecision(9) << new_price);
	}
	int64_t  ret_price = new_price * cutter;
	KF_LOG_INFO(logger, "[fixPriceTickSize input]" << " 4(new_price * cutter)" << std::fixed << std::setprecision(9) << new_price);
	return ret_price;
}

void TDEngineAscendEX::dealnum(string pre_num, string& fix_num)
{
	int size = pre_num.size();
	if (size > 8) {
		string s1 = pre_num.substr(0, size - 8);
		s1.append(".");
		string s2 = pre_num.substr(size - 8, size);
		fix_num = s1 + s2;
	}
	else {
		string s1 = "0.";
		for (int i = 0; i < 8 - size; i++) {
			s1.append("0");
		}
		fix_num = s1 + pre_num;
	}
	KF_LOG_INFO(logger, "[dealnum] pre_num:" << pre_num << "fix_num:" << fix_num);
}

//master-margin -> margin
string TDEngineAscendEX::splitAccountName(string dataName)
{
	string output = "";

	int pos = dataName.find("-");
	if (pos == -1)
		return output;

	output = dataName.substr(pos + 1, dataName.length() - (pos + 1));
	if (output == "main" || output == "spot")
		output = "cash";
	else if (output == "future")
		output = "futures";
	else if (output != "cash" && output != "margin" && output != "futures")
		output = "";

	return output;
}

void TDEngineAscendEX::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
	KF_LOG_INFO(logger, "[req_order_insert]" << " (rid)" << requestId
		<< " (APIKey)" << account_units[account_index].api_key
		<< " (Tid)" << data->InstrumentID
		<< " (Volume)" << data->Volume
		<< " (LimitPrice)" << data->LimitPrice
		<< " (OrderRef)" << data->OrderRef);
	send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_ASCENDEX, 1/*ISLAST*/, requestId);

	int errorId = 0;
	string errorMsg = "";
	//check ws status
	AccountUnitAscendEX& unit = account_units[account_index];
	if (!unit.is_connecting || !unit.ws_auth || !unit.ws_sub_order) {
		errorId = 203;
		errorMsg = "websocket is not connecting,please try again later";
		on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
		return;
	}

	on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
	;
	if (nullptr == m_ThreadPoolPtr)
	{
		send_order_thread(data, account_index, requestId, rcv_time);
	}
	else
	{
		KF_LOG_DEBUG(logger, "[req_order_insert] m_ThreadPoolPtr commit thread, idlThrNum" << m_ThreadPoolPtr->idlCount());
		m_ThreadPoolPtr->commit(std::bind(&TDEngineAscendEX::send_order_thread, this, data, account_index, requestId, rcv_time));
	}
}

void TDEngineAscendEX::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
	KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId << " (APIKey)" << account_units[account_index].api_key
		<< " (OrderRef)" << data->OrderRef << " (KfOrderID)" << data->KfOrderID);
	send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_ASCENDEX, 1, requestId);

	int errorId = 0;
	string errorMsg = "";
	//check ws status
	AccountUnitAscendEX& unit = account_units[account_index];
	if (!unit.is_connecting || !unit.ws_auth || !unit.ws_sub_order) {
		errorId = 203;
		errorMsg = "websocket is not connecting,please try again later";
		on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
		return;
	}

	on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());

	if (nullptr == m_ThreadPoolPtr)
	{
		action_order_thread(data, account_index, requestId, rcv_time);
	}
	else
	{
		KF_LOG_DEBUG(logger, "[req_order_action] m_ThreadPoolPtr commit thread, idlThrNum" << m_ThreadPoolPtr->idlCount());
		m_ThreadPoolPtr->commit(std::bind(&TDEngineAscendEX::action_order_thread, this, data, account_index, requestId, rcv_time));
	}
}

void TDEngineAscendEX::req_inner_transfer(const LFTransferField* data, int account_index, int requestId)
{
	AccountUnitAscendEX& unit = account_units[account_index];
	KF_LOG_DEBUG(logger, "[req_inner_transfer]" << " (rid) " << requestId
		<< " (APIKey) " << unit.api_key
		<< " (from) " << data->From
		<< " (Currency) " << data->Currency
		<< " (Volume) " << data->Volume);
	send_writer->write_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_ASCENDEX, 1, requestId);

	int errorId = 0;
	std::string errorMsg = "";
	Document json;
	std::string type = "";
	cpr::Response response;

	std::string Currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
	if (Currency.length() == 0) {
		errorId = 201;
		errorMsg = "Currency not in WhiteList, ignore it";
		KF_LOG_ERROR(logger, "[req_inner_transfer]: Currency not in WhiteList , ignore it. (rid)" << requestId
			<< " (errorId)" << errorId << " (errorMsg) " << errorMsg);
		on_rsp_transfer(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}

	string strvolume = to_string(data->Volume);
	string fixvolume;
	dealnum(strvolume, fixvolume);

	//cash/margin/futures
	string dataFrom = data->From;
	transform(dataFrom.begin(), dataFrom.end(), dataFrom.begin(), ::tolower);
	string dataTo = data->To;
	transform(dataTo.begin(), dataTo.end(), dataTo.begin(), ::tolower);

	if ((dataFrom.find("master-") == -1 && dataFrom.find("sub-") == -1) ||
		(dataTo.find("master-") == -1 && dataTo.find("sub-") == -1)) {
		errorId = 201;
		errorMsg = "[req_inner_transfer] Input Error, From / To must be like this: master-cash/master-margin/master-futures/sub-cash/sub-margin/sub-futures,";
		errorMsg += " input (dataFrom)" + dataFrom + " (dataTo)" + dataTo;
	}
	else if (dataFrom.find("master-") != -1 && dataTo.find("master-") != -1) {
		string fromAccount = splitAccountName(dataFrom);
		string toAccount = splitAccountName(dataTo);

		if (fromAccount.length() == 0 || toAccount.length() == 0) {
			errorId = 201;
			errorMsg = "[req_inner_transfer] Input Error, From / To must be like this: master-cash/master-margin/master-futures/sub-cash/sub-margin/sub-futures,";
			errorMsg += " input (dataFrom)" + dataFrom + " (dataTo)" + dataTo;
		}
		else
			response = balance_transfer(unit, fixvolume, Currency, fromAccount, toAccount, json);
	}
	else {
		string fromAccount = splitAccountName(dataFrom);
		string toAccount = splitAccountName(dataTo);

		string userFrom = data->FromName;
		string userTo = data->ToName;

		if (fromAccount.length() == 0 || toAccount.length() == 0) {
			errorId = 201;
			errorMsg = "[req_inner_transfer] Input Error, From / To must be like this: master-cash/master-margin/master-futures/sub-cash/sub-margin/sub-futures,";
			errorMsg += " input (dataFrom)" + dataFrom + " (dataTo)" + dataTo;
		}
		else {
			if (dataFrom.find("master-") != -1)
				userFrom = "parentUser";
			if (dataTo.find("master-") != -1)
				userTo = "parentUser";
			response = balance_transfer_for_subaccount(unit, fixvolume, Currency, fromAccount, toAccount, userFrom, userTo, json);
		}
	}

	// check before sending request
	if (errorId != 0) {
		KF_LOG_ERROR(logger, errorMsg);
		on_rsp_transfer(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	//not excepted response
	else if (response.status_code >= 400) {
		errorId = response.status_code;
		errorMsg = response.text;
		on_rsp_transfer(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	else if (json.HasParseError() || !json.IsObject() || !json.HasMember("code") || json["code"].GetInt() != 0) {
		errorId = 201;
		errorMsg = "json has parse error. (response.text)";
		errorMsg += response.text;
		KF_LOG_ERROR(logger, "[inner_transfer] json has parse error.");
		on_rsp_transfer(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	//excepted response { "code": 0, "info":{...} }
	//if (json.HasMember("code") && json["code"].GetInt() == 0) ->
	else {
		errorId = 0;
		KF_LOG_INFO(logger, "[inner_transfer] inner_transfer success. no error message");
		on_rsp_transfer(data, requestId, errorId, errorMsg.c_str());
		return;
	}
}

void TDEngineAscendEX::req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw)
{
	AccountUnitAscendEX& unit = account_units[account_index];
	KF_LOG_INFO(logger, "[req_transfer_history] (api_key)" << unit.api_key << "(data->UserID)" << data->UserID);
	send_writer->write_frame(data, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_ASCENDEX, 1, requestId);

	LFTransferHistoryField his;
	std::vector<LFTransferHistoryField> tmp_vector;
	memset(&his, 0, sizeof(LFTransferHistoryField));
	strncpy(his.UserID, data->UserID, 64);
	strncpy(his.ExchangeID, data->ExchangeID, 11);

	int errorId = 0;
	std::string errorMsg = "";

	std::string Currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
	Document json;
	cpr::Response response;
	if (data->IsWithdraw)
		response = query_wallet_transaction_history(unit, Currency, "withdrawal", -1, -1, json);
	else
		response = query_wallet_transaction_history(unit, Currency, "deposit", -1, -1, json);
	printResponse(json);

	if (response.status_code >= 400) {
		errorId = response.status_code;
		errorMsg = response.text;
		on_rsp_transfer_history(&his, data->IsWithdraw, 0, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	else if (json.HasParseError() || !json.IsObject() || !json.HasMember("code") || json["code"].GetInt() != 0) {
		errorId = 201;
		errorMsg = "json has parse error. (response.text)";
		errorMsg += response.text;
		KF_LOG_ERROR(logger, "[req_transfer_history] json has parse error.");
		on_rsp_transfer_history(&his, data->IsWithdraw, 0, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	else if (json.HasMember("data") && json["data"].HasMember("data") && json["data"]["data"].IsArray()) {
		auto his_data = json["data"]["data"].GetArray();
		for (int i = 0; i < his_data.Size(); i++) {
			//check positionWhiteList
			char asset[32];
			strncpy(asset, his_data[i]["asset"].GetString(), 32);
			std::string ticker = unit.positionWhiteList.GetKeyByValue(asset);
			if (ticker.length() == 0)
				continue;

			strncpy(his.Currency, ticker.c_str(), 32);
			strncpy(his.TimeStamp, std::to_string(his_data[i]["time"].GetInt64()).c_str(), 32);
			his.Volume == std::round(atof(his_data[i]["amount"].GetString()) * scale_offset);
			his.Status = GetTransferStatus(his_data[i]["status"].GetString());

			strncpy(his.Address, his_data[i]["destAddress"]["address"].GetString(), 130);
			if (his_data[i]["destAddress"].HasMember("destTag") && his_data[i]["destAddress"]["destTag"].IsString())
				strncpy(his.Tag, his_data[i]["destAddress"]["destTag"].GetString(), 64);

			tmp_vector.push_back(his);
			KF_LOG_INFO(logger, "[req_deposit_history] (insertTime)" << his.TimeStamp << " (amount)" << his.Volume
				<< " (asset)" << his.Currency << " (address)" << his.Address << " (addressTag)" << his.Tag
				<< " (status)" << his.Status << " (requestId)" << requestId);
			memset(his.Tag, 0, 64);
		}
	}

	bool findSymbolInResult = false;

	int history_count = tmp_vector.size();
	if (history_count == 0)
		his.Status = -1;

	for (int i = 0; i < history_count; i++)
	{
		on_rsp_transfer_history(&tmp_vector[i], isWithdraw, i == (history_count - 1), requestId, errorId, errorMsg.c_str());
		findSymbolInResult = true;
	}

	if (!findSymbolInResult)
		on_rsp_transfer_history(&his, isWithdraw, 1, requestId, errorId, errorMsg.c_str());

	if (errorId != 0)
		raw_writer->write_error_frame(&his, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
}

void TDEngineAscendEX::send_order_thread(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
	AccountUnitAscendEX& unit = account_units[account_index];

	int errorId = 0;
	std::string errorMsg = "";

	//get ticker
	std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
	if (ticker.length() == 0) {
		errorId = 200;
		errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
		KF_LOG_ERROR(logger, "[send_order_thread]: not in WhiteList, ignore it  (rid)" << requestId <<
			" (errorId)" << errorId << " (errorMsg) " << errorMsg);
		on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	KF_LOG_DEBUG(logger, "[send_order_thread] (exchange_ticker)" << ticker);

	//generate id
	string id = generateId(data->OrderRef);
	std::unique_lock<std::mutex> orderref_lock(*unit.mutex_orderref_map);
	unit.orderRefMap.insert(make_pair(data->OrderRef, id));
	orderref_lock.unlock();

	//send order 
	Document d;
	SendOrderFilter filter = getSendOrderFilter(unit, ticker.c_str());
	int64_t fixedPrice = fixPriceTickSize(filter.ticksize, data->LimitPrice, LF_CHAR_Buy == data->Direction);

	KF_LOG_DEBUG(logger, "[send_order_thread] SendOrderFilter  (Tid)" << ticker <<
		" (LimitPrice)" << data->LimitPrice <<
		" (ticksize)" << filter.ticksize <<
		" (fixedPrice)" << fixedPrice);
	send_order(unit, ticker.c_str(), id.c_str(), GetSide(data->Direction).c_str(),
		GetType(data->OrderPriceType).c_str(), data->Volume * 1.0 / scale_offset, fixedPrice * 1.0 / scale_offset, d);
	int64_t timeBefore = getTimestamp();

	//not expected response
	if (d.HasParseError() || !d.IsObject()) {
		errorId = 100;
		errorMsg = "send_order http response has parse error or is not json. please check the log";
		KF_LOG_ERROR(logger, "[send_order_thread] send_order error!  (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
	}
	else if ((!d.HasMember("data") || !d["data"].HasMember("info") || !d["data"]["info"].HasMember("orderId"))
		|| (d.HasMember("status") && strcmp("Err", d["status"].GetString()) == 0))
	{
		errorId = 100;
		if (d.HasMember("message"))
			errorMsg = d["message"].GetString();
		else
			errorMsg = "send_order http response data is abnormal. please check the log";
		KF_LOG_ERROR(logger, "[send_order_thread] send_order error!  (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
	}
	if (errorId != 0)
	{
		on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(&data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());

	string remoteOrderId = d["data"]["info"]["orderId"].GetString();
	std::unique_lock<std::mutex> new_order_lock(*unit.mutex_new_order);
	std::unique_lock<std::mutex> rtn_order_lock(*unit.mutex_rtn_order);
	//std::unique_lock<std::mutex> orderref_lock(*unit.mutex_orderref_map);
	orderref_lock.lock();
	KF_LOG_DEBUG(logger, "[send_order_thread] is locking mutex_rtn_order and mutex_new_order and mutex_orderref_map (requestId)" << requestId);
	//rtnOrderMap没找到，orderRefMap能找到，才应该插入newOrderMap
	auto check_itr = unit.rtnOrderMap.find(remoteOrderId);
	auto ref_itr = unit.orderRefMap.find(data->OrderRef);
	if (check_itr == unit.rtnOrderMap.end() && ref_itr != unit.orderRefMap.end()) {
		KF_LOG_INFO(logger, "TDEngineAscendEX::send_order_thread, insert into newOrderMap");
		AsNewOrderStatus orderStatus;
		orderStatus.requestId = requestId;
		orderStatus.OrderRef = data->OrderRef;
		orderStatus.data = *data;
		unit.newOrderMap.insert(make_pair(remoteOrderId, orderStatus));
	}
	KF_LOG_DEBUG(logger, "[send_order_thread] unlock mutex_rtn_order, send_order_thread finished");
}

void TDEngineAscendEX::action_order_thread(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
	AccountUnitAscendEX& unit = account_units[account_index];

	int errorId = 0;
	std::string errorMsg = "";

	//get ticker 
	std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
	if (ticker.length() == 0) {
		errorId = 200;
		errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
		KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
			errorId << " (errorMsg) " << errorMsg);
		on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	KF_LOG_DEBUG(logger, "[req_order_action] (requestId)" << requestId << " (exchange_ticker)" << ticker << " (OrderRef)" << data->OrderRef);

	//get remote orderId by OrderRef
	std::unique_lock<std::mutex> rtn_order_lock(*unit.mutex_rtn_order);
	KF_LOG_DEBUG(logger, "[req_order_action] is locking mutex_rtn_order (requestId)" << requestId);
	auto itr = unit.rtnOrderMap.begin();
	for (; itr != unit.rtnOrderMap.end(); itr++) {
		if (itr->second.OrderRef == data->OrderRef)
			break;
	}

	if (itr == unit.rtnOrderMap.end()) {
		errorId = 200;
		errorMsg = std::string(data->OrderRef) + " cannot be found";
		KF_LOG_ERROR(logger, "[req_order_action]: OrderRef cannot be found , ignore it: (rid)" << requestId << " (errorId)" <<
			errorId << " (errorMsg) " << errorMsg);
		on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	string remoteOrderId = itr->first;
	rtn_order_lock.unlock();
	KF_LOG_DEBUG(logger, "[req_order_action] unlock mutex_rtn_order (requestId)" << requestId);
	KF_LOG_DEBUG(logger, "[req_order_action] (requestId)" << requestId << " (exchange_ticker)" << ticker << " (OrderRef)" << data->OrderRef << " (remoteOrderId)" << remoteOrderId);

	//cancel
	Document d;
	cancel_order(unit, ticker.c_str(), remoteOrderId.c_str(), d);
	int64_t timeBefore = getTimestamp();

	//not expected response
	if (d.HasParseError() || !d.IsObject()) {
		errorId = 100;
		errorMsg = "cancel_order http response has parse error or is not json. please check the log";
		KF_LOG_ERROR(logger, "[req_order_action] cancel_order error!  (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
	}
	else if (!d.HasMember("data") || d.HasMember("status") && strcmp("Err", d["status"].GetString()) == 0)
	{
		errorId = 100;
		if (d.HasMember("message"))
			errorMsg = d["message"].GetString();
		KF_LOG_ERROR(logger, "[req_order_action] cancel_order error!  (rid)" << requestId << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
	}
	if (errorId != 0)
	{
		on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_ASCENDEX, 1, requestId, errorId, errorMsg.c_str());

	//std::unique_lock<std::mutex> rtn_order_lock(*unit.mutex_rtn_order);
	rtn_order_lock.lock();
	KF_LOG_DEBUG(logger, "[req_order_action] is locking mutex_rtn_order (requestId)" << requestId);
	//找到应该将order_canceled标记为true
	auto check_itr = unit.rtnOrderMap.find(remoteOrderId);
	if (check_itr != unit.rtnOrderMap.end()) {
		KF_LOG_INFO(logger, "TDEngineAscendEX::req_order_action, set order_canceled = true");
		check_itr->second.order_canceled = true;
		check_itr->second.last_action_time = getTimestamp();
		check_itr->second.data = *data;
		check_itr->second.requestId = requestId;
	}
	KF_LOG_DEBUG(logger, "[req_order_action] unlock mutex_rtn_order, req_order_action finished");
}

//a17abc775163U0498873147bethuhuuu -> a + Hex(1626659574115) + U0498873147 + bethuhuuu -> method + timestamp + USER_UID + id.substr(0,9)
string TDEngineAscendEX::GetOrderRefViaOrderId(AccountUnitAscendEX& unit, string orderId)
{
	string orderRef = "";
	string method = orderId.substr(0, 1);
	string timestampHex = orderId.substr(1, orderId.length() - 21);
	string userUID = orderId.substr(orderId.length() - 20, 11);
	string id = orderId.substr(orderId.length() - 9, 9);

	bool viaRestApi = (method == "a");
	int64_t ts = 0;
	sscanf(timestampHex.c_str(), "%llx", &ts);

	//found orderRef in orderRefMap, then erase it
	std::unique_lock<std::mutex> orderref_lock(*unit.mutex_orderref_map);
	std::map<std::string, std::string>::iterator itr;
	for (itr = unit.orderRefMap.begin(); itr != unit.orderRefMap.end(); itr++) {
		if (itr->second == id) {
			orderRef = itr->first;
			break;
		}
	}
	orderref_lock.unlock();

	KF_LOG_DEBUG(logger, "[GetOrderRefViaOrderId] (orderId)" << orderId
		<< " (viaRestApi)" << viaRestApi
		<< " (timestamp)" << ts
		<< " (userUID)" << userUID
		<< " (id)" << id
		<< " (orderRef)" << orderRef);
	return orderRef;
}

string TDEngineAscendEX::generateId(string orderRef)
{
	if (orderRef.length() <= 7) {
		string res(8 - orderRef.length(), 'o');
		res += orderRef + "o";
		return res;
	}
	else {
		string res(1, 'o');
		res += orderRef.substr(0, 7) + "o";
		return res;
	}
}

AccountUnitAscendEX& TDEngineAscendEX::findAccountUnitByWebsocketConn(struct lws* websocketConn)
{
	for (size_t idx = 0; idx < account_units.size(); idx++) {
		AccountUnitAscendEX& unit = account_units[idx];
		if (unit.websocketConn == websocketConn) {
			return unit;
		}
	}
	return account_units[0];
}

int TDEngineAscendEX::findAccountUnitIndexByWebsocketConn(struct lws* websocketConn)
{
	for (size_t idx = 0; idx < account_units.size(); idx++) {
		AccountUnitAscendEX& unit = account_units[idx];
		if (unit.websocketConn == websocketConn) {
			return idx;
		}
	}
	return -1;
}

//lws_write
int TDEngineAscendEX::subscribeTopic(lws* conn, string strSubscribe)
{
	unsigned char msg[1024];
	memset(&msg[LWS_PRE], 0, 1024 - LWS_PRE);
	int length = strSubscribe.length();
	KF_LOG_INFO(logger, "[subscribeTopic] " << strSubscribe.c_str() << " ,len = " << length);
	strncpy((char*)msg + LWS_PRE, strSubscribe.c_str(), length);
	//请求
	int ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);
	lws_callback_on_writable(conn);
	return ret;
}

//account: specific account id
//if(account == "") decidied by margin
//default: cash
std::string TDEngineAscendEX::makeSubscribeOrdersUpdate(string account, bool margin)
{
	KF_LOG_INFO(logger, "[makeSubscribeOrdersUpdate] margin " << margin);
	StringBuffer sbUpdate;
	Writer<StringBuffer> writer(sbUpdate);
	writer.StartObject();
	writer.Key("op");
	writer.String("sub");
	writer.Key("id");
	writer.String("abc123");
	writer.Key("ch");
	string ch = "order:";
	if (account != "")
		ch += account;
	else if (margin)
		ch += "margin";
	else
		ch += "cash";
	writer.String(ch.c_str());
	writer.EndObject();
	std::string strUpdate = sbUpdate.GetString();
	return strUpdate;
}

void TDEngineAscendEX::lws_auth(AccountUnitAscendEX& unit)
{
	KF_LOG_INFO(logger, "[lws_auth] auth");
	//message
	std::string apiPath = "stream";
	int64_t ts = getTimestamp();
	//std::string timestamp = getTimestampString();
	std::string timestamp = to_string(ts);
	string message = timestamp + "+" + apiPath;

	//sign
	std::string secret_key_64decode = base64_decode(unit.secret_key);
	unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
	string sign = base64_encode(signature, 32);

	//json
	StringBuffer authBuffer;
	Writer<StringBuffer> writer(authBuffer);
	writer.StartObject();
	writer.Key("op");
	writer.String("auth");
	// may safely skip it
	// writer.Key("id");
	// writer.String("abc123");
	writer.Key("t");
	writer.Int64(ts);
	writer.Key("key");
	writer.String(unit.api_key.c_str());
	writer.Key("sig");
	writer.String(sign.c_str());
	writer.EndObject();
	std::string strSubscribe = authBuffer.GetString();

	unsigned char msg[1024];
	memset(&msg[LWS_PRE], 0, 1024 - LWS_PRE);
	int length = strSubscribe.length();
	KF_LOG_INFO(logger, "[ascendexAuth] auth data " << strSubscribe.c_str() << " ,len = " << length);
	unit.sendmessage.push(strSubscribe);
	lws_callback_on_writable(unit.websocketConn);
	KF_LOG_INFO(logger, "[lws_auth] auth success...");
}

void TDEngineAscendEX::lws_pong(struct lws* conn)
{
	KF_LOG_INFO(logger, "[Pong] pong the ping of websocket," << conn);
	auto& unit = findAccountUnitByWebsocketConn(conn);
	StringBuffer sbPing;
	Writer<StringBuffer> writer(sbPing);
	writer.StartObject();
	writer.Key("op");
	writer.String("pong");
	writer.EndObject();
	std::string strPong = sbPing.GetString();
	unit.sendmessage.push(strPong);
	lws_callback_on_writable(conn);
}

//call makeSubscribeOrdersUpdate
int TDEngineAscendEX::on_lws_write_subscribe(lws* conn)
{
	KF_LOG_INFO(logger, "[on_lws_write_subscribe]");
	int ret = 0;
	AccountUnitAscendEX& unit = findAccountUnitByWebsocketConn(conn);
	if (unit.sendmessage.size() > 0)
	{
		ret = subscribeTopic(conn, unit.sendmessage.front());
		unit.sendmessage.pop();
	}
	if (unit.ws_auth == true && unit.ws_sub_order == false) {
		unit.ws_sub_order = true;
		string strSubscribe = makeSubscribeOrdersUpdate();
		unit.sendmessage.push(strSubscribe);
		lws_callback_on_writable(conn);
	}
	return ret;
}

bool TDEngineAscendEX::lws_login(AccountUnitAscendEX& unit, long timeout_nsec)
{
	KF_LOG_INFO(logger, "TDEngineAscendEX::lws_login");
	global_td = this;

	unit.context = NULL;
	int errorId = 0;
	string errorMsg = "";
	Document json;

	if (unit.context == NULL) {
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

		unit.context = lws_create_context(&info);
		KF_LOG_INFO(logger, "TDEngineAscendEX::lws_login: context created:" << unit.api_key);
	}

	if (unit.context == NULL) {
		KF_LOG_ERROR(logger, "TDEngineAscendEX::lws_login: context of" << unit.api_key << " is NULL. return");
		return false;
	}

	int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
	lws_set_log_level(logs, NULL);

	struct lws_client_connect_info ccinfo = { 0 };

	std::string host = wsBaseUrl;
	//std::string path = "/ws/" + unit.listenKey;
	string path = "/";
	path += to_string(unit.accountGroup) + "/api/pro/v1/stream";
	int port = 443;

	ccinfo.context = unit.context;
	ccinfo.address = host.c_str();
	ccinfo.port = port;
	ccinfo.path = path.c_str();
	ccinfo.host = host.c_str();
	ccinfo.origin = host.c_str();
	ccinfo.ietf_version_or_minus_one = -1;
	ccinfo.protocol = protocols[0].name;
	ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

	unit.websocketConn = lws_client_connect_via_info(&ccinfo);
	KF_LOG_INFO(logger, "TDEngineAscendEX::lws_login: Connecting to " << ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

	if (unit.websocketConn == NULL) {
		KF_LOG_ERROR(logger, "TDEngineAscendEX::lws_login: wsi create error.");
		return false;
	}
	KF_LOG_INFO(logger, "TDEngineAscendEX::lws_login: wsi create success.");
	unit.is_connecting = true;
	return true;
}

void TDEngineAscendEX::on_lws_open(lws* wsi)
{
	KF_LOG_INFO(logger, "[on_lws_open] wsi" << wsi);
	lws_auth(findAccountUnitByWebsocketConn(wsi));
	KF_LOG_INFO(logger, "[on_lws_open] finished ");
}

void TDEngineAscendEX::on_lws_data(lws* conn, const char* data, size_t len)
{
	KF_LOG_INFO(logger, "[on_lws_data]" << data);
	try
	{
		Document json;
		json.Parse(data);

		if (json.HasParseError())
		{
			KF_LOG_ERROR(logger, "[on_lws_data] json parse error");
			return;
		}

		if (json.HasMember("m")) {
			string m = json["m"].GetString();
			if (m == "auth")
				onAuthMsg(conn, json);
			else if (m == "ping")
				lws_pong(conn);
			else if (m == "sub")
				onSubMsg(conn, json);
			else if (m == "error")
				onErrorMsg(conn, json);
			else if (m == "order")
				onOrder(conn, json);
			else if (m == "balance")
				onBalance(conn, json);
			else
				KF_LOG_DEBUG(logger, "[on_lws_data] hasn't handle with the json, m:" << m);
		}
	}
	catch (const std::exception& e)
	{
		KF_LOG_ERROR(logger, "[on_lws_data] received data from ascendex exception,{error:" << e.what() << "}");
	}
	catch (...)
	{
		KF_LOG_ERROR(logger, "[on_lws_data] received data from ascendex system exception");
	}
	KF_LOG_DEBUG(logger, "[on_lws_data] received data from ascendex end");
}

void TDEngineAscendEX::on_lws_connection_error(lws* conn)
{
	int idx = findAccountUnitIndexByWebsocketConn(conn);
	if (idx == -1) {
		KF_LOG_INFO(logger, "[on_lws_close] cannot get account unit by websocket connection");
		return;
	}
	else {
		KF_LOG_INFO(logger, "[on_lws_connection_error] cancel all orders");
		AccountUnitAscendEX& unit = account_units[idx];
		Document cancelResponse;
		cancel_all_orders(unit, cancelResponse);

		if (isRunning) {
			KF_LOG_INFO(logger, "[on_lws_connection_error] reconnect, lws_login");
			lws_login(unit, 0);
		}
	}
}

void TDEngineAscendEX::on_lws_close(lws* conn)
{
	int idx = findAccountUnitIndexByWebsocketConn(conn);
	if (idx == -1) {
		KF_LOG_INFO(logger, "[on_lws_close] cannot get account unit by websocket connection");
		return;
	}

	else {
		AccountUnitAscendEX& unit = account_units[idx];
		unit.ws_auth = false;
		unit.ws_sub_order = false;
		unit.is_connecting = false;
		unit.logged_in = false;
		KF_LOG_INFO(logger, "[websocket close]");
	}
}

void TDEngineAscendEX::onAuthMsg(lws* conn, const rapidjson::Document& json)
{
	int code = 0;
	if (json.HasMember("code"))
		code = json["code"].GetInt();
	else {
		KF_LOG_ERROR(logger, "[onAuthMsg] json does not hasmember code");
		return;
	}

	if (code == 0) {
		AccountUnitAscendEX& unit = findAccountUnitByWebsocketConn(conn);
		unit.ws_auth = true;
		KF_LOG_INFO(logger, "[onAuthMsg] Auth success");
	}
	else {
		string errorMsg = "";
		if (json.HasMember("err"))
			errorMsg = json["err"].GetString();
		KF_LOG_INFO(logger, "[onAuthMsg] Auth error, (code)" << code << " (err)" << errorMsg);
	}
}

void TDEngineAscendEX::onSubMsg(lws* conn, const rapidjson::Document& json)
{
	if (json.HasMember("ch"))
		KF_LOG_DEBUG(logger, "[onSubMsg] subscribe successfully {ch :" << json["ch"].GetString() << "}");
}

void TDEngineAscendEX::onErrorMsg(lws* conn, const rapidjson::Document& json)
{
	if (json.HasMember("code") && json.HasMember("reason") && json.HasMember("info"))
		KF_LOG_INFO(logger, "subscribe sysmbol error, (code)" << json["code"].GetInt()
			<< " (reason)" << json["reason"].GetString()
			<< " (info)" << json["info"].GetString());
}

void TDEngineAscendEX::onOrder(lws* conn, const rapidjson::Document& json)
{
	KF_LOG_INFO(logger, "TDEngineAscendEX::onOrder");
	AccountUnitAscendEX& unit = findAccountUnitByWebsocketConn(conn);

	if (json.HasMember("data"))
	{
		auto& data = json["data"];
		if (data.HasMember("s") && data.HasMember("st"))
		{
			std::string remoteOrderId = data["orderId"].GetString();
			LfOrderStatusType status = GetOrderStatus(data["st"].GetString());

			// 在newOrderMap中搜索remoteOrderId
			// 如果找到且订单状态错误，则报错且将其从所有map中删去
			// 否则只从newOrderMap中删去即可
			std::unique_lock<std::mutex> new_order_lock(*unit.mutex_new_order);
			KF_LOG_DEBUG(logger, "[onOrder] is locking mutex_new_order ");
			auto new_itr = unit.newOrderMap.find(remoteOrderId);
			if (new_itr != unit.newOrderMap.end()) {
				// 报错
				if (status == LF_CHAR_NoTradeNotQueueing || status == LF_CHAR_Error)
				{
					int errorId = 204;
					string errorMsg = "";
					if (data.HasMember("err"))
						errorMsg = data["err"].GetString();
					if(errorMsg == "")
						errorMsg = "insert order error";

					KF_LOG_INFO(logger, "TDEngineAscendEX::onOrder, insert order error (errorMsg)" << errorMsg.c_str());
					on_rsp_order_insert(&new_itr->second.data, new_itr->second.requestId, errorId, errorMsg.c_str());

					KF_LOG_INFO(logger, "TDEngineAscendEX::onOrder, erase this order (remoteOrderId)" << remoteOrderId.c_str() << " from newOrderMap and orderRefMap");

					// 从orderRefMap中删除
					std::unique_lock<std::mutex> orderref_lock(*unit.mutex_orderref_map);
					std::map<std::string, std::string>::iterator orderref_itr = unit.orderRefMap.find(new_itr->second.OrderRef);
					if (orderref_itr != unit.orderRefMap.end())
						unit.orderRefMap.erase(orderref_itr);
					orderref_lock.unlock();
					// 从newOrderMap中删除
					unit.newOrderMap.erase(new_itr);

					KF_LOG_DEBUG(logger, "[onOrder] return, unlock new_order_lock and orderref_lock");
					return;
				}
				// 正常
				// 从newOrderMap中删除
				KF_LOG_INFO(logger, "TDEngineAscendEX::onOrder,rtn_order erase new_itr from newOrderMap");
				unit.newOrderMap.erase(new_itr);
			}
			new_order_lock.unlock();
			KF_LOG_DEBUG(logger, "[onOrder] unlock new_order_lock ");

			// 在rtnOrderMap中搜索remoteOrderId
			std::unique_lock<std::mutex> rtn_order_lock(*unit.mutex_rtn_order);
			KF_LOG_DEBUG(logger, "[onOrder] is locking mutex_rtn_order ");
			auto itr = unit.rtnOrderMap.find(remoteOrderId);

			// 在rtnOrderMap中搜索不到remoteOrderId，则先插入rtnOrderMap中，废单状态不用插入map
			if (itr == unit.rtnOrderMap.end()) {
				//get orderRef in orderRefMap
				string orderRef = GetOrderRefViaOrderId(unit, remoteOrderId);
				if (orderRef.length() == 0)
				{
					KF_LOG_ERROR(logger, "[onOrder] cannot get orderRef via remoteOrderId " << remoteOrderId);
					KF_LOG_DEBUG(logger, "[onOrder] unlock mutex_rtn_order ");
					return;
				}

				// rtn_order notTouch
				AsOrderStatus orderStatus;
				orderStatus.order_canceled = false;
				orderStatus.OrderRef = orderRef;
				LFRtnOrderField& rtn_order = orderStatus.last_rtn_order;

				memset(&rtn_order, 0, sizeof(LFRtnOrderField));
				// 不是废单状态就先返回LF_CHAR_NotTouched慢慢分析，不然就快点报错return
				if (status == LF_CHAR_NoTradeNotQueueing || status == LF_CHAR_Error)
					rtn_order.OrderStatus = LF_CHAR_NotTouched;
				else
					rtn_order.OrderStatus = status;

				rtn_order.VolumeTraded = 0;
				strcpy(rtn_order.ExchangeID, "ascendex");
				strncpy(rtn_order.UserID, unit.api_key.c_str(), 16);
				string instrument = unit.coinPairWhiteList.GetKeyByValue(data["s"].GetString());
				if (instrument.length() == 0) {
					KF_LOG_DEBUG(logger, "[onOrder] cannot found the coinpair from whitelist ");
					KF_LOG_DEBUG(logger, "[onOrder] unlock mutex_rtn_order ");
					return;
				}
				strncpy(rtn_order.InstrumentID, instrument.c_str(), 31);
				rtn_order.Direction = GetDirection(data["sd"].GetString());
				rtn_order.OrderPriceType = GetPriceType(data["ot"].GetString());
				if (rtn_order.OrderPriceType == LF_CHAR_AnyPrice)
					rtn_order.TimeCondition = LF_CHAR_IOC;
				else
					rtn_order.TimeCondition = LF_CHAR_GTC;
				strncpy(rtn_order.OrderRef, orderRef.c_str(), 21);
				rtn_order.VolumeTotalOriginal = std::round(std::stod(data["q"].GetString()) * scale_offset);
				rtn_order.LimitPrice = std::round(std::stod(data["p"].GetString()) * scale_offset);
				rtn_order.VolumeTotal = std::round(std::stod(data["q"].GetString()) * scale_offset);

				on_rtn_order(&rtn_order);

				// 废单状态不用插入map，从orderRefMap删除后直接返回
				if ( rtn_order.OrderStatus == LF_CHAR_NoTradeNotQueueing || rtn_order.OrderStatus == LF_CHAR_Error)
				{
					KF_LOG_DEBUG(logger, "[onOrder] final status. erase it from every map");

					//如果有撤单错误就报错
					int errorId = 207;
					string errorMsg = "";
					if (data.HasMember("err"))
						errorMsg = data["err"].GetString();
					if (errorMsg != "")
						errorMsg = "cancel order error";
					if (itr->second.order_canceled) {
						on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
					}

					//从orderRefMap中删除
					std::unique_lock<std::mutex> orderref_lock(*unit.mutex_orderref_map);
					std::map<std::string, std::string>::iterator orderref_itr = unit.orderRefMap.find(rtn_order.OrderRef);
					if (orderref_itr != unit.orderRefMap.end())
						unit.orderRefMap.erase(orderref_itr);
					orderref_lock.unlock();

					KF_LOG_DEBUG(logger, "[onOrder] final status, unlock and return ");
					return;
				}

				//insert into map
				unit.rtnOrderMap.insert(std::make_pair(remoteOrderId, orderStatus));
				itr = unit.rtnOrderMap.find(remoteOrderId);
				if (itr == unit.rtnOrderMap.end()) {
					KF_LOG_DEBUG(logger, "[onOrder] cannot found remoteOrderId after insert, unlock mutex_rtn_order ");
					return;
				}
			}

			LFRtnOrderField& rtn_order = itr->second.last_rtn_order;

			//if status is not changed/updated, return
			uint64_t VolumeTraded = std::round(std::stod(data["cfq"].GetString()) * scale_offset);
			if (VolumeTraded < rtn_order.VolumeTraded || (status == rtn_order.OrderStatus && VolumeTraded == rtn_order.VolumeTraded))
			{
				KF_LOG_INFO(logger, "[onOrder] status is not changed");
				KF_LOG_DEBUG(logger, "[onOrder] unlock mutex_rtn_order ");
				return;
			}

			//otherwise update
			uint64_t oldVolumeTraded = rtn_order.VolumeTraded;
			rtn_order.VolumeTraded = VolumeTraded;
			rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTraded;
			int64_t avaragePrice = std::round(std::stod(data["ap"].GetString()) * scale_offset);
			rtn_order.OrderStatus = status;

			//rtn trade
			if (oldVolumeTraded < rtn_order.VolumeTraded) {
				LFRtnTradeField rtn_trade;
				memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
				strncpy(rtn_trade.OrderRef, rtn_order.OrderRef, 13);
				strcpy(rtn_trade.ExchangeID, rtn_order.ExchangeID);
				strncpy(rtn_trade.UserID, rtn_order.UserID, 16);
				strncpy(rtn_trade.InstrumentID, rtn_order.InstrumentID, 31);
				rtn_trade.Direction = rtn_order.Direction;
				rtn_trade.Volume = rtn_order.VolumeTraded - oldVolumeTraded;
				strcpy(rtn_trade.TradeID, rtn_order.BusinessUnit);
				rtn_trade.Price = avaragePrice;
				strncpy(rtn_trade.TradeTime, to_string(data["t"].GetInt64()).c_str(), 32);

				KF_LOG_INFO(logger, "TDEngineAscendex::onOrder, rtn_trade");
				on_rtn_trade(&rtn_trade);
				raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField), source_id, MSG_TYPE_LF_RTN_TRADE_ASCENDEX, 1, -1);
			}

			//rtn order
			KF_LOG_INFO(logger, "TDEngineAscendEX::onOrder,rtn_order");
			on_rtn_order(&rtn_order);
			raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField), source_id, MSG_TYPE_LF_RTN_ORDER_ASCENDEX, 1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);

			// 如果有撤单错误就报错
			int errorId = 207;
			string errorMsg = "";
			if (data.HasMember("err"))
				errorMsg = data["err"].GetString();
			if (itr->second.order_canceled && errorMsg != "" && rtn_order.OrderStatus != LF_CHAR_Canceled)
			{
				KF_LOG_DEBUG(logger, "[onOrder] cancel order error (errorMsg)" << errorMsg.c_str());
				on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
			}

			//final status
			if (rtn_order.OrderStatus == LF_CHAR_AllTraded || rtn_order.OrderStatus == LF_CHAR_PartTradedNotQueueing ||
				rtn_order.OrderStatus == LF_CHAR_Canceled || rtn_order.OrderStatus == LF_CHAR_NoTradeNotQueueing || rtn_order.OrderStatus == LF_CHAR_Error)
			{
				KF_LOG_DEBUG(logger, "[onOrder] final status. erase it from every map");

				//从orderRefMap中删除
				std::unique_lock<std::mutex> orderref_lock(*unit.mutex_orderref_map);
				std::map<std::string, std::string>::iterator orderref_itr = unit.orderRefMap.find(rtn_order.OrderRef);
				if (orderref_itr != unit.orderRefMap.end())
					unit.orderRefMap.erase(orderref_itr);
				orderref_lock.unlock();

				//从rtnOrderMap中删除
				unit.rtnOrderMap.erase(itr);
			}
			KF_LOG_DEBUG(logger, "[onOrder] unlock mutex_rtn_order ");
		}
	}
}

void TDEngineAscendEX::onBalance(struct lws* conn, const rapidjson::Document& json)
{
	KF_LOG_INFO(logger, "TDEngineAscendEX::onBalance");
	if (json.HasMember("data"))
	{
		auto& data = json["data"];
		if (data.HasMember("a"))
		{
			string accountId = json["accountId"].GetString();
			string type = json["ac"].GetString(); //CASH or MARGIN
			string asset = data["a"].GetString();
			string totalBalance = data["tb"].GetString();
			string availableBalance = data["ab"].GetString();

			KF_LOG_INFO(logger, "TDEngineAscendEX::onBalance accountId:" << accountId << " type:" << type
				<< " asset:" << asset << " totalBalance:" << totalBalance << " availableBalance:" << availableBalance);
		}
	}
}

void TDEngineAscendEX::set_reader_thread()
{
	ITDEngine::set_reader_thread();

	KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on AccountUnitAscendex::loop");
	rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineAscendEX::loop, this)));

	KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on AccountUnitAscendex::query_order_loop");
	orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineAscendEX::check_order_loop, this)));
}

void TDEngineAscendEX::loop()
{
	while (isRunning)
	{
		KF_LOG_INFO(logger, "TDEngineAscendEX::loop:lws_service");
		for (size_t idx = 0; idx < account_units.size(); idx++)
		{
			AccountUnitAscendEX& unit = account_units[idx];
			lws_service(unit.context, rest_get_interval_ms);
		}
	}
}

void TDEngineAscendEX::check_order_loop()
{
	while (isRunning)
	{
		for (int i = 0; i < account_units.size(); i++)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(rest_wait_time_ms));
			KF_LOG_INFO(logger, "TDEngineAscendEX::check_order_loop ");
			AccountUnitAscendEX& unit = account_units[i];
			check_insert_orders(unit);
			std::this_thread::sleep_for(std::chrono::milliseconds(rest_wait_time_ms));
			check_canceled_order(unit);
		}
	}
}

void TDEngineAscendEX::check_insert_orders(AccountUnitAscendEX& unit)
{
	std::unique_lock<std::mutex> new_order_lock(*unit.mutex_new_order);
	KF_LOG_DEBUG(logger, "[check_insert_orders] is locking mutex_new_order");
	string orderref = "";
	for (auto check_itr = unit.newOrderMap.begin(); check_itr != unit.newOrderMap.end();)
	{
		orderref = check_itr->second.OrderRef;
		if (getTimestamp() - check_itr->second.last_action_time > rest_wait_time_ms)
		{
			KF_LOG_INFO(logger, "TDEngineAscendEX::check_insert_orders (orderref)" << check_itr->second.OrderRef.c_str()
				<< " (remoteOrderId)" << check_itr->first.c_str()
				<< " (last_action_time)" << check_itr->second.last_action_time
				<< " (timeNow)" << getTimestamp());

			Document doc;
			query_order(unit, check_itr->first, doc);

			/*
			{"code":0,"accountId":"2EPQKEZ6KC42MAVUBRR6CUAUVWGNLSP2","ac":"CASH",
			"data":{"seqNum":22250501691,"orderId":"a17ad2620270U0498873147ooooooo1o","symbol":"INFT/USDT","orderType":"Limit",
			"lastExecTime":1627027277610,"price":"0.0011","orderQty":"10000","side":"Sell","status":"New",
			"avgPx":"0","cumFilledQty":"0","stopPrice":"","errorCode":"","cumFee":"0","feeAsset":"USDT","execInst":"NULL_VAL"}}
			*/
			//not expected response
			string errorMsg;
			if (doc.HasParseError() || !doc.IsObject()) {
				errorMsg = "query_order http response has parse error or is not json. please check the log";
				KF_LOG_ERROR(logger, "[check_insert_order] query_order error! (errorMsg) " << errorMsg);
				check_itr++;
				continue;
			}
			else if ((!doc.HasMember("data") || !doc["data"].HasMember("orderId")) || (doc.HasMember("status") && strcmp("Err", doc["status"].GetString()) == 0))
			{
				if (doc.HasMember("message"))
					errorMsg = doc["message"].GetString();
				else
					errorMsg = "query_order http response data is abnormal. please check the log";
				KF_LOG_ERROR(logger, "[check_insert_order] query_order error! (errorMsg) " << errorMsg);
				check_itr++;
				continue;
			}
			else {
				KF_LOG_DEBUG(logger, "[check_insert_order] return order via rest");

				//rtn order
				auto& order_data = doc["data"];
				AsOrderStatus asStatus;
				LFRtnOrderField &rtn_order = asStatus.last_rtn_order;
				memset(&rtn_order, 0, sizeof(LFRtnOrderField));
				string instrument = unit.coinPairWhiteList.GetKeyByValue(order_data["symbol"].GetString());
				if (instrument.length() == 0) {
					KF_LOG_ERROR(logger, "[check_insert_order] error, cannot get order's strategy_coinpair by order's exchange_coinpair");
					continue;
				}
				strncpy(rtn_order.InstrumentID, instrument.c_str(), 31);
				rtn_order.OrderStatus = GetOrderStatus(order_data["status"].GetString());
				rtn_order.VolumeTraded = std::round(std::stod(order_data["cumFilledQty"].GetString()) * scale_offset);
				strcpy(rtn_order.ExchangeID, "ascendex");
				strncpy(rtn_order.UserID, unit.api_key.c_str(), 16);
				rtn_order.Direction = GetDirection(order_data["side"].GetString());
				rtn_order.OrderPriceType = GetPriceType(order_data["orderType"].GetString());
				if (rtn_order.OrderPriceType == LF_CHAR_AnyPrice)
					rtn_order.TimeCondition = LF_CHAR_IOC;
				else
					rtn_order.TimeCondition = LF_CHAR_GTC;
				strncpy(rtn_order.OrderRef, check_itr->second.OrderRef.c_str(), 21);
				rtn_order.VolumeTotalOriginal = std::round(std::stod(order_data["orderQty"].GetString()) * scale_offset);
				rtn_order.LimitPrice = std::round(std::stod(order_data["price"].GetString()) * scale_offset);
				rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTraded;
				on_rtn_order(&rtn_order);

				//若为结单状态，则从各map中检查删除
				if (rtn_order.OrderStatus == LF_CHAR_AllTraded || rtn_order.OrderStatus == LF_CHAR_PartTradedNotQueueing ||
					rtn_order.OrderStatus == LF_CHAR_Canceled || rtn_order.OrderStatus == LF_CHAR_NoTradeNotQueueing || rtn_order.OrderStatus == LF_CHAR_Error)
				{
					KF_LOG_DEBUG(logger, "[check_insert_order] final status. erase it from every map");
					// LF_CHAR_NoTradeNotQueueing和LF_CHAR_Error状态不但要删除，还要报错
					if (rtn_order.OrderStatus == LF_CHAR_NoTradeNotQueueing || rtn_order.OrderStatus == LF_CHAR_Error) {
						int errorId = 204;
						if (order_data.HasMember("errorCode"))
							errorMsg = order_data["errorCode"].GetString();
						if (errorMsg == "")
							errorMsg = "insert order error";

						on_rsp_order_insert(&check_itr->second.data, check_itr->second.requestId, errorId, errorMsg.c_str());
					}

					//从orderRefMap中删除
					std::unique_lock<std::mutex> orderref_lock(*unit.mutex_orderref_map);
					std::map<std::string, std::string>::iterator orderref_itr = unit.orderRefMap.find(check_itr->second.OrderRef);
					if (orderref_itr != unit.orderRefMap.end())
						unit.orderRefMap.erase(orderref_itr);
					orderref_lock.unlock();

					//从newOrderMap中删除
					check_itr = unit.newOrderMap.erase(check_itr);
				}
				//若不为结单状态，则插入到rtnOrderMap中，从newOrderMap中检查删除
				else {
					KF_LOG_DEBUG(logger, "[check_insert_order] not final status. insert into rtnOrderMap,erase it from newOrderMap");
					//插入到rtnOrderMap中
					std::unique_lock<std::mutex> rtn_order_lock(*unit.mutex_rtn_order);
					KF_LOG_DEBUG(logger, "[check_insert_order] is locking mutex_rtn_order");

					asStatus.OrderRef = check_itr->second.OrderRef;
					asStatus.order_canceled = false;

					unit.rtnOrderMap.insert(std::make_pair(check_itr->first, asStatus));
					rtn_order_lock.unlock();

					//从newOrderMap中删除
					check_itr = unit.newOrderMap.erase(check_itr);
					KF_LOG_DEBUG(logger, "[check_insert_order] unlock mutex_rtn_order");
				}
			}
		}
		if (check_itr != unit.newOrderMap.end() && orderref == check_itr->second.OrderRef )
			check_itr++;
	}
	KF_LOG_DEBUG(logger, "[check_insert_orders] unlock mutex_new_order");
}

void TDEngineAscendEX::check_canceled_order(AccountUnitAscendEX& unit)
{
	std::unique_lock<std::mutex> rtn_order_lock(*unit.mutex_rtn_order);
	KF_LOG_DEBUG(logger, "[check_canceled_order] is locking mutex_rtn_order");
	string orderref = "";
	for (auto check_itr = unit.rtnOrderMap.begin(); check_itr != unit.rtnOrderMap.end();)
	{
		orderref = check_itr->second.OrderRef;
		if (getTimestamp() - check_itr->second.last_action_time > rest_wait_time_ms && check_itr->second.order_canceled)
		{
			KF_LOG_INFO(logger, "TDEngineAscendEX::check_canceled_order (orderref)" << check_itr->second.OrderRef.c_str()
				<< " (remoteOrderId)" << check_itr->first.c_str()
				<< " (last_action_time)" << check_itr->second.last_action_time
				<< " (timeNow)" << getTimestamp());

			Document doc;
			query_order(unit, check_itr->first, doc);

			/*
			{"code":0,"accountId":"2EPQKEZ6KC42MAVUBRR6CUAUVWGNLSP2","ac":"CASH",
			"data":{"seqNum":22250501691,"orderId":"a17ad2620270U0498873147ooooooo1o","symbol":"INFT/USDT","orderType":"Limit",
			"lastExecTime":1627027277610,"price":"0.0011","orderQty":"10000","side":"Sell","status":"New",
			"avgPx":"0","cumFilledQty":"0","stopPrice":"","errorCode":"","cumFee":"0","feeAsset":"USDT","execInst":"NULL_VAL"}}
			*/
			//not expected response
			string errorMsg;
			if (doc.HasParseError() || !doc.IsObject()) {
				errorMsg = "query_order http response has parse error or is not json. please check the log";
				KF_LOG_ERROR(logger, "[check_canceled_order] query_order error! (errorMsg) " << errorMsg);
				check_itr++;
				continue;
			}
			else if ((!doc.HasMember("data") || !doc["data"].HasMember("orderId")) || (doc.HasMember("status") && strcmp("Err", doc["status"].GetString()) == 0))
			{
				if (doc.HasMember("message"))
					errorMsg = doc["message"].GetString();
				else
					errorMsg = "query_order http response data is abnormal. please check the log";
				KF_LOG_ERROR(logger, "[check_canceled_order] query_order error! (errorMsg) " << errorMsg);
				check_itr++;
				continue;
			}
			else {
				KF_LOG_DEBUG(logger, "[check_canceled_order] return order via rest");

				//rtn order
				auto& order_data = doc["data"];
				LFRtnOrderField& rtn_order = check_itr->second.last_rtn_order;

				rtn_order.OrderStatus = GetOrderStatus(order_data["status"].GetString());
				int64_t oldVolumeTraded = rtn_order.VolumeTraded;
				rtn_order.VolumeTraded = std::round(std::stod(order_data["cumFilledQty"].GetString()) * scale_offset);
				rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTraded;
				on_rtn_order(&rtn_order);

				//rtn trade
				if (oldVolumeTraded < rtn_order.VolumeTraded) {
					LFRtnTradeField rtn_trade;
					memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
					strncpy(rtn_trade.OrderRef, rtn_order.OrderRef, 13);
					strcpy(rtn_trade.ExchangeID, rtn_order.ExchangeID);
					strncpy(rtn_trade.UserID, rtn_order.UserID, 16);
					strncpy(rtn_trade.InstrumentID, rtn_order.InstrumentID, 31);
					rtn_trade.Direction = rtn_order.Direction;
					rtn_trade.Volume = rtn_order.VolumeTraded - oldVolumeTraded;
					strcpy(rtn_trade.TradeID, rtn_order.BusinessUnit);
					rtn_trade.Price = std::round(std::stod(order_data["avgPx"].GetString()) * scale_offset);
					strncpy(rtn_trade.TradeTime, to_string(order_data["lastExecTime"].GetInt64()).c_str(), 32);

					KF_LOG_INFO(logger, "TDEngineAscendex::check_canceled_order, rtn_trade");
					on_rtn_trade(&rtn_trade);
					raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField), source_id, MSG_TYPE_LF_RTN_TRADE_ASCENDEX, 1, -1);
				}

				// 如果有撤单错误就报错
				int errorId = 207;
				string errorMsg = "";
				if (order_data.HasMember("errorCode"))
					errorMsg = order_data["errorCode"].GetString();
				if (check_itr->second.order_canceled && errorMsg != "" && rtn_order.OrderStatus != LF_CHAR_Canceled)
				{
					KF_LOG_DEBUG(logger, "[check_canceled_order] cancel order error (errorMsg)" << errorMsg.c_str());
					on_rsp_order_action(&check_itr->second.data, check_itr->second.requestId, errorId, errorMsg.c_str());
				}

				//erase from map
				if (rtn_order.OrderStatus == LF_CHAR_AllTraded || rtn_order.OrderStatus == LF_CHAR_PartTradedNotQueueing ||
					rtn_order.OrderStatus == LF_CHAR_Canceled || rtn_order.OrderStatus == LF_CHAR_NoTradeNotQueueing || rtn_order.OrderStatus == LF_CHAR_Error)
				{
					KF_LOG_DEBUG(logger, "[check_canceled_order] final status. erase it from every map");

					//从orderRefMap中删除
					std::unique_lock<std::mutex> orderref_lock(*unit.mutex_orderref_map);
					std::map<std::string, std::string>::iterator orderref_itr = unit.orderRefMap.find(check_itr->second.OrderRef);
					if (orderref_itr != unit.orderRefMap.end())
						unit.orderRefMap.erase(orderref_itr);
					orderref_lock.unlock();

					//从rtnOrderMap中删除
					check_itr = unit.rtnOrderMap.erase(check_itr);
				}
				else
				{
					KF_LOG_INFO(logger, "TDEngineAscendex::check_canceled_order error, the order is not canceled");
				}
			}
		}
		if (check_itr != unit.rtnOrderMap.end() && orderref == check_itr->second.OrderRef)
			check_itr++;
	}
	KF_LOG_DEBUG(logger, "[check_canceled_order] unlock mutex_rtn_order");
}

void TDEngineAscendEX::printResponse(const Document& d)
{
	if (!d.IsObject() || !d.HasMember("code"))
	{
		KF_LOG_INFO(logger, "[printResponse] error ");
		return;
	}
	else if (d.HasMember("message")) {
		KF_LOG_INFO(logger, "[printResponse] error (code) " << d["code"].GetInt() << " (message) " << d["message"].GetString());
	}
	else {
		StringBuffer buffer;
		Writer<StringBuffer> writer(buffer);
		d.Accept(writer);
		KF_LOG_INFO(logger, "[printResponse] ok " << "(code)" << d["code"].GetInt() << "(text)" << buffer.GetString());
	}
}

void TDEngineAscendEX::getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json)
{
	json.Parse(responseText.c_str());
}

bool TDEngineAscendEX::shouldRetry(cpr::Response response)
{
	bool retry = false;
	if (response.status_code >= 400)
		return true;
	else {
		Document json;
		json.Parse(response.text.c_str());

		if (json.HasParseError())
			return true;
		else if (json.HasMember("status") && (strcmp(json["status"].GetString(), "Err") == 0 || strcmp(json["status"].GetString(), "ERR") == 0))
			return true;
	}

	return false;
}

//read response from get_account_info, only for ascendex
bool TDEngineAscendEX::set_account_info(AccountUnitAscendEX& unit)
{
	KF_LOG_INFO(logger, "[set_account_info]");

	int errorId = 0;
	std::string errorMsg = "";
	Document d;
	get_account_info(unit, d);

	//not expected response
	if (d.HasParseError() || !d.IsObject()) {
		errorId = 100;
		errorMsg = "get_account_info http response has parse error or is not json. please check the log";
		KF_LOG_ERROR(logger, "[set_account_info] get_account_info error!  (errorId)" << errorId << " (errorMsg) " << errorMsg);
		return false;
	}
	else if (d.HasMember("status") && strcmp("Err", d["status"].GetString()) == 0)
	{
		errorId = 100;
		if (d.HasMember("message"))
			errorMsg = d["message"].GetString();
		KF_LOG_ERROR(logger, "[set_account_info] get_account_info error!  (errorId)" << errorId << " (errorMsg) " << errorMsg);
		return false;
	}
	//accountGroup
	if (d.HasMember("data") && d["data"].HasMember("accountGroup")) {
		unit.accountGroup = d["data"]["accountGroup"].GetInt();
		return true;
	}
	else
		return false;
}

// GET /api/pro/v1/info
void TDEngineAscendEX::get_account_info(AccountUnitAscendEX& unit, Document& json)
{
	KF_LOG_INFO(logger, "[get_account_info]");

	//message
	std::string apiPath = "info";
	std::string timestamp = getTimestampString();
	string message = timestamp + "+" + apiPath;

	//sign
	std::string secret_key_64decode = base64_decode(unit.secret_key);
	unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
	string sign = base64_encode(signature, 32);

	//url
	string requestPath = "/api/pro/v1/info";
	string url = unit.baseUrl + requestPath;

	//GET
	cpr::Response response;
	std::unique_lock<std::mutex> lock(http_mutex);
	response = Get(
		Url{ url },
		Header{ {"Accept",			 "application/json"},
				{"Content-Type",	 "application/json; charset=UTF-8"},
				{"x-auth-key",	     unit.api_key},
				{"x-auth-signature", sign},
				{"x-auth-timestamp", timestamp} },
		Timeout{ 30000 });
	lock.unlock();

	KF_LOG_INFO(logger, "[get_account_info] (url) " << url << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());
	getResponse(response.status_code, response.text, response.error.message, json);
}

//infact get account balance
//GET <account-group>/api/pro/v1/cash/balance
//GET <account-group>/api/pro/v1/margin/balance
void TDEngineAscendEX::get_account(AccountUnitAscendEX& unit, Document& json)
{
	KF_LOG_INFO(logger, "[get_account]");

	//message
	std::string apiPath = "balance";
	std::string timestamp = getTimestampString();
	string message = timestamp + "+" + apiPath;

	//sign
	std::string secret_key_64decode = base64_decode(unit.secret_key);
	unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
	string sign = base64_encode(signature, 32);

	//url
	string requestPath = "/api/pro/v1/cash/";
	if (isMargin)
		string requestPath = "/api/pro/v1/margin/";
	requestPath += apiPath;
	string url = unit.baseUrl + "/" + to_string(unit.accountGroup) + requestPath;

	//GET
	cpr::Response response;
	std::unique_lock<std::mutex> lock(http_mutex);
	response = Get(
		Url{ url },
		Header{ {"Accept",			 "application/json"},
				{"Content-Type",	 "application/json; charset=UTF-8"},
				{"x-auth-key",	     unit.api_key},
				{"x-auth-signature", sign},
				{"x-auth-timestamp", timestamp} },
		Timeout{ 30000 });
	lock.unlock();

	KF_LOG_INFO(logger, "[get_account] (url) " << url << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());
	getResponse(response.status_code, response.text, response.error.message, json);
}

//may not need to be signed
//GET /api/pro/v1/products
void TDEngineAscendEX::get_products(AccountUnitAscendEX& unit, Document& json)
{
	KF_LOG_INFO(logger, "[get_products]");

	//message
	std::string apiPath = "products";

	//url
	string requestPath = "/api/pro/v1/";
	requestPath += apiPath;
	string url = unit.baseUrl + requestPath;

	//GET
	cpr::Response response;
	std::unique_lock<std::mutex> lock(http_mutex);
	response = Get(
		Url{ url },
		Header{ {"Accept",			 "application/json"},
				{"Content-Type",	 "application/json; charset=UTF-8"} },
		Timeout{ 30000 });
	lock.unlock();

	KF_LOG_INFO(logger, "[get_products] (url) " << url << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());
	getResponse(response.status_code, response.text, response.error.message, json);
}

//POST <account-group>/api/pro/v1/{account-category}/order
void TDEngineAscendEX::send_order(AccountUnitAscendEX& unit, const char* code, const char* id,
	const char* side, const char* type, double size, double price, Document& json)
{
	KF_LOG_INFO(logger, "[send_order]");

	//message
	std::string apiPath = "order";
	std::string timestamp = getTimestampString();
	string message = timestamp + "+" + apiPath;

	//sign
	std::string secret_key_64decode = base64_decode(unit.secret_key);
	unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
	string sign = base64_encode(signature, 32);

	//url
	string requestPath = "/api/pro/v1/cash/";
	if (isMargin)
		string requestPath = "/api/pro/v1/margin/";
	requestPath += apiPath;
	string url = unit.baseUrl + "/" + to_string(unit.accountGroup) + requestPath;

	//body
	std::string priceStr;
	std::stringstream convertPriceStream;
	convertPriceStream << std::fixed << std::setprecision(8) << price;
	convertPriceStream >> priceStr;

	std::string sizeStr;
	std::stringstream convertSizeStream;
	convertSizeStream << std::fixed << std::setprecision(8) << size;
	convertSizeStream >> sizeStr;

	KF_LOG_INFO(logger, "[send_order] (code) " << code << " (side) " << side << " (id) " << id
		<< " (type) " << type << " (size) " << sizeStr << " (price) " << priceStr << " (time) " << time);

	Document document;
	document.SetObject();
	Document::AllocatorType& allocator = document.GetAllocator();

	rapidjson::Value timeVal;
	timeVal.SetString(timestamp.c_str(), timestamp.length(), allocator);
	document.AddMember("id", StringRef(id), allocator);
	document.AddMember("time", timeVal, allocator);
	document.AddMember("symbol", StringRef(code), allocator);
	document.AddMember("orderQty", StringRef(sizeStr.c_str()), allocator);
	document.AddMember("orderType", StringRef(type), allocator);
	document.AddMember("side", StringRef(side), allocator);
	if (strcmp("limit", type) == 0 || strcmp("stop_limit", type) == 0)
		document.AddMember("orderPrice", StringRef(priceStr.c_str()), allocator);
	StringBuffer jsonStr;
	Writer<StringBuffer> writer(jsonStr);
	document.Accept(writer);
	std::string body = jsonStr.GetString();
	//finish body here

	//POST
	KF_LOG_INFO(logger, "[send_order] now post");
	std::unique_lock<std::mutex> lock(http_mutex);
	const auto response = Post(
		Url{ url },
		Header{ {"Accept",			 "application/json"},
				{"Content-Type",	 "application/json; charset=UTF-8"},
				{"x-auth-key",	     unit.api_key},
				{"x-auth-signature", sign},
				{"x-auth-timestamp", timestamp} },
		Body{ body },
		Timeout{ 30000 }
	);
	lock.unlock();

	KF_LOG_INFO(logger, "[send_order] (url) " << url << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());
	getResponse(response.status_code, response.text, response.error.message, json);
}

//DELETE <account-group>/api/pro/v1/{account-category}/order
void TDEngineAscendEX::cancel_order(AccountUnitAscendEX& unit, const char* code, const char* remoteOrderId, Document& json)
{
	KF_LOG_INFO(logger, "[cancel_order]");
	bool should_retry = false;
	int retry_times = 0;
	cpr::Response response;

	do {
		//message
		std::string apiPath = "order";
		std::string timestamp = getTimestampString();
		string message = timestamp + "+" + apiPath;

		//sign
		std::string secret_key_64decode = base64_decode(unit.secret_key);
		unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
		string sign = base64_encode(signature, 32);

		//url
		string requestPath = "/api/pro/v1/cash/";
		if (isMargin)
			string requestPath = "/api/pro/v1/margin/";
		requestPath += apiPath;
		string url = unit.baseUrl + "/" + to_string(unit.accountGroup) + requestPath;

		//body
		Document document;
		document.SetObject();
		Document::AllocatorType& allocator = document.GetAllocator();
		document.AddMember("orderId", StringRef(remoteOrderId), allocator);
		document.AddMember("symbol", StringRef(code), allocator);
		document.AddMember("time", StringRef(timestamp.c_str()), allocator);
		StringBuffer jsonStr;
		Writer<StringBuffer> writer(jsonStr);
		document.Accept(writer);
		std::string body = jsonStr.GetString();
		//finish body here

		//delete
		KF_LOG_DEBUG(logger, "[cancel_order] now delete (exchange_ticker)" << code << " (timestamp)" << timestamp << " (remoteOrderId)" << remoteOrderId);
		std::unique_lock<std::mutex> lock(http_mutex);
		response = Delete(
			Url{ url },
			Header{ {"Accept",			 "application/json"},
					{"Content-Type",	 "application/json; charset=UTF-8"},
					{"x-auth-key",	     unit.api_key},
					{"x-auth-signature", sign},
					{"x-auth-timestamp", timestamp} },
			//Parameters{ {"orderId",		 remoteOrderId}, {"symbol",		 code}, "time",		 timestamp} },
			Body{ body },
			Timeout{ 30000 }
		);
		lock.unlock();
		KF_LOG_INFO(logger, "[cancel_order] after delete");

		//check should retry
		if (shouldRetry(response))
		{
			should_retry = true;
			retry_times++;
			std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
		}
		//need not retry
		if (should_retry)
		{
			std::unique_lock<std::mutex> rtn_order_lock(*unit.mutex_rtn_order);
			KF_LOG_DEBUG(logger, "[cancel_order] is locking mutex_rtn_order ");
			auto itr = unit.rtnOrderMap.find(remoteOrderId);
			if (itr == unit.rtnOrderMap.end())
			{
				KF_LOG_INFO(logger, "[cancel_order] order was removed from rtnOrderMap, no need to retry");
				break;
			}
			rtn_order_lock.unlock();
			KF_LOG_DEBUG(logger, "[cancel_order] unlock mutex_rtn_order");
		}

		KF_LOG_INFO(logger, "[cancel_order] (url) " << url << " (response.status_code) " << response.status_code <<
			" (response.error.message) " << response.error.message <<
			" (response.text) " << response.text.c_str());
	} while (should_retry && retry_times < max_rest_retry_times);

	getResponse(response.status_code, response.text, response.error.message, json);
}

//if(symbol == "") cancel all orders of every symbols
//if(symbol != "") cancel all orders of the provided symbol
//DELETE <account-group>/api/pro/v1/{account-category}/order/all
void TDEngineAscendEX::cancel_all_orders(AccountUnitAscendEX& unit, Document& json, std::string symbol)
{
	KF_LOG_INFO(logger, "[cancel_all_orders]");
	bool should_retry = false;
	int retry_times = 0;
	cpr::Response response;

	do {
		//message
		std::string apiPath = "order/all";
		std::string timestamp = getTimestampString();
		string message = timestamp + "+" + apiPath;

		//sign
		std::string secret_key_64decode = base64_decode(unit.secret_key);
		unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
		string sign = base64_encode(signature, 32);

		//url
		string requestPath = "/api/pro/v1/cash/";
		if (isMargin)
			string requestPath = "/api/pro/v1/margin/";
		requestPath += apiPath;
		string url = unit.baseUrl + "/" + to_string(unit.accountGroup) + requestPath;

		//delete
		if (symbol.length() == 0) {
			std::unique_lock<std::mutex> lock(http_mutex);
			response = Delete(
				Url{ url },
				Header{ {"Accept",			 "application/json"},
						{"Content-Type",	 "application/json; charset=UTF-8"},
						{"x-auth-key",	     unit.api_key},
						{"x-auth-signature", sign},
						{"x-auth-timestamp", timestamp} },
				Timeout{ 30000 });
			lock.unlock();
		}
		else {
			//body
			Document document;
			document.SetObject();
			Document::AllocatorType& allocator = document.GetAllocator();
			document.AddMember("symbol", StringRef(symbol.c_str()), allocator);
			StringBuffer jsonStr;
			Writer<StringBuffer> writer(jsonStr);
			document.Accept(writer);
			std::string body = jsonStr.GetString();
			//finish body here

			std::unique_lock<std::mutex> lock(http_mutex);
			response = Delete(
				Url{ url },
				Header{ {"Accept",			 "application/json"},
						{"Content-Type",	 "application/json; charset=UTF-8"},
						{"x-auth-key",	     unit.api_key},
						{"x-auth-signature", sign},
						{"x-auth-timestamp", timestamp} },
				//Parameters{ {"symbol",		 symbol} },
				Body{ body },
				Timeout{ 30000 });
			lock.unlock();
		}

		//check should retry
		if (shouldRetry(response))
		{
			should_retry = true;
			retry_times++;
			std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_milliseconds));
		}

		KF_LOG_INFO(logger, "[cancel_all_orders] (url) " << url << " (response.status_code) " << response.status_code <<
			" (response.error.message) " << response.error.message <<
			" (response.text) " << response.text.c_str());
	} while (should_retry && retry_times < max_rest_retry_times);

	getResponse(response.status_code, response.text, response.error.message, json);
}

//GET <account-group>/api/pro/v1/{account-category}/order/open
void TDEngineAscendEX::query_orders(AccountUnitAscendEX& unit, Document& json)
{
	KF_LOG_INFO(logger, "[query_orders]");

	//message
	std::string apiPath = "order/open";
	std::string timestamp = getTimestampString();
	string message = timestamp + "+" + apiPath;

	//sign
	std::string secret_key_64decode = base64_decode(unit.secret_key);
	unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
	string sign = base64_encode(signature, 32);

	//url
	string requestPath = "/api/pro/v1/cash/";
	if (isMargin)
		string requestPath = "/api/pro/v1/margin/";
	requestPath += apiPath;
	string url = unit.baseUrl + "/" + to_string(unit.accountGroup) + requestPath;

	//GET
	cpr::Response response;
	std::unique_lock<std::mutex> lock(http_mutex);
	response = Get(
		Url{ url },
		Header{ {"Accept",			 "application/json"},
				{"Content-Type",	 "application/json; charset=UTF-8"},
				{"x-auth-key",	     unit.api_key},
				{"x-auth-signature", sign},
				{"x-auth-timestamp", timestamp} },
		Timeout{ 30000 });
	lock.unlock();

	KF_LOG_INFO(logger, "[query_orders] (url) " << url << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());
	getResponse(response.status_code, response.text, response.error.message, json);
}

//GET <account-group>/api/pro/v1/{account-category}/order/status?orderId={orderId}
void TDEngineAscendEX::query_order(AccountUnitAscendEX& unit, std::string remoteOrderId, Document& json)
{
	KF_LOG_INFO(logger, "[query_order]");

	//message
	std::string apiPath = "order/status";
	std::string timestamp = getTimestampString();
	string message = timestamp + "+" + apiPath;

	//sign
	std::string secret_key_64decode = base64_decode(unit.secret_key);
	unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
	string sign = base64_encode(signature, 32);

	//url
	string requestPath = "/api/pro/v1/cash/";
	if (isMargin)
		string requestPath = "/api/pro/v1/margin/";
	requestPath += apiPath + "?orderId=" + remoteOrderId;
	string url = unit.baseUrl + "/" + to_string(unit.accountGroup) + requestPath;

	//GET
	cpr::Response response;
	std::unique_lock<std::mutex> lock(http_mutex);
	response = Get(
		Url{ url },
		Header{ {"Accept",			 "application/json"},
				{"Content-Type",	 "application/json; charset=UTF-8"},
				{"x-auth-key",	     unit.api_key},
				{"x-auth-signature", sign},
				{"x-auth-timestamp", timestamp} },
		Timeout{ 30000 });
	lock.unlock();

	KF_LOG_INFO(logger, "[query_order] (url) " << url << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());
	getResponse(response.status_code, response.text, response.error.message, json);
}

cpr::Response TDEngineAscendEX::balance_transfer(AccountUnitAscendEX& unit, std::string amount, std::string asset, std::string fromAccount, std::string toAccount, Document& json)
{
	KF_LOG_INFO(logger, "[balance_transfer]");

	//message
	std::string apiPath = "transfer";
	std::string timestamp = getTimestampString();
	string message = timestamp + "+" + apiPath;

	//sign
	std::string secret_key_64decode = base64_decode(unit.secret_key);
	unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
	string sign = base64_encode(signature, 32);

	//url
	string requestPath = "/api/pro/v1/transfer";
	string url = unit.baseUrl + "/" + to_string(unit.accountGroup) + requestPath;

	//POST
	Document document;
	document.SetObject();
	Document::AllocatorType& allocator = document.GetAllocator();
	document.AddMember("amount", StringRef(amount.c_str()), allocator);
	document.AddMember("asset", StringRef(asset.c_str()), allocator);
	document.AddMember("fromAccount", StringRef(fromAccount.c_str()), allocator);
	document.AddMember("toAccount", StringRef(toAccount.c_str()), allocator);
	StringBuffer jsonStr;
	Writer<StringBuffer> writer(jsonStr);
	document.Accept(writer);
	std::string body = jsonStr.GetString();

	cpr::Response response;
	std::unique_lock<std::mutex> lock(http_mutex);
	response = Post(
		Url{ url },
		Header{ {"Accept",			 "application/json"},
				{"Content-Type",	 "application/json; charset=UTF-8"},
				{"x-auth-key",	     unit.api_key},
				{"x-auth-signature", sign},
				{"x-auth-timestamp", timestamp} },
		Body{ body },
		Timeout{ 30000 });
	lock.unlock();

	KF_LOG_INFO(logger, "[balance_transfer] (url) " << url << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());
	getResponse(response.status_code, response.text, response.error.message, json);
	return response;
}

cpr::Response TDEngineAscendEX::balance_transfer_for_subaccount(AccountUnitAscendEX& unit, std::string amount, std::string asset, std::string acFrom, std::string acTo, std::string userFrom, std::string userTo, Document& json)
{
	KF_LOG_INFO(logger, "[balance_transfer_for_subaccount]");

	//message
	std::string apiPath = "subuser-transfer";
	std::string timestamp = getTimestampString();
	string message = timestamp + "+" + apiPath;

	//sign
	std::string secret_key_64decode = base64_decode(unit.secret_key);
	unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
	string sign = base64_encode(signature, 32);

	//url
	string requestPath = "/api/pro/v2/subuser/subuser-transfer";
	string url = unit.baseUrl + "/" + to_string(unit.accountGroup) + requestPath;

	//POST
	Document document;
	document.SetObject();
	Document::AllocatorType& allocator = document.GetAllocator();
	document.AddMember("userFrom", StringRef(userFrom.c_str()), allocator);
	document.AddMember("userTo", StringRef(userTo.c_str()), allocator);
	document.AddMember("acFrom", StringRef(acFrom.c_str()), allocator);
	document.AddMember("acTo", StringRef(acTo.c_str()), allocator);
	document.AddMember("amount", StringRef(amount.c_str()), allocator);
	document.AddMember("asset", StringRef(asset.c_str()), allocator);
	StringBuffer jsonStr;
	Writer<StringBuffer> writer(jsonStr);
	document.Accept(writer);
	std::string body = jsonStr.GetString();

	cpr::Response response;
	std::unique_lock<std::mutex> lock(http_mutex);
	response = Post(
		Url{ url },
		Header{ {"Accept",			 "application/json"},
				{"Content-Type",	 "application/json; charset=UTF-8"},
				{"x-auth-key",	     unit.api_key},
				{"x-auth-signature", sign},
				{"x-auth-timestamp", timestamp} },
		Body{ body },
		Timeout{ 30000 });
	lock.unlock();

	KF_LOG_INFO(logger, "[balance_transfer_for_subaccount] (url) " << url << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());
	getResponse(response.status_code, response.text, response.error.message, json);
	return response;
}

cpr::Response TDEngineAscendEX::query_wallet_transaction_history(AccountUnitAscendEX& unit, std::string asset, std::string txType, int page, int pageSize, Document& json)
{
	KF_LOG_INFO(logger, "[query_wallet_transaction_history]");

	//message
	std::string apiPath = "wallet/transactions";
	std::string timestamp = getTimestampString();
	string message = timestamp + "+" + apiPath;

	//sign
	std::string secret_key_64decode = base64_decode(unit.secret_key);
	unsigned char* signature = hmac_sha256_byte(secret_key_64decode.c_str(), message.c_str());
	string sign = base64_encode(signature, 32);

	//url
	string requestPath = "/api/pro/v1/wallet/transactions";
	string url = unit.baseUrl + "/" + to_string(unit.accountGroup) + requestPath;

	//POST
	Document document;
	document.SetObject();
	Document::AllocatorType& allocator = document.GetAllocator();
	document.AddMember("txType", StringRef(txType.c_str()), allocator);
	if (asset.length() > 0)
		document.AddMember("asset", StringRef(asset.c_str()), allocator);
	if (page > 0)
		document.AddMember("page", page, allocator);
	if (pageSize > 0)
		document.AddMember("pageSize", pageSize, allocator);
	StringBuffer jsonStr;
	Writer<StringBuffer> writer(jsonStr);
	document.Accept(writer);
	std::string body = jsonStr.GetString();

	cpr::Response response;
	std::unique_lock<std::mutex> lock(http_mutex);
	response = Post(
		Url{ url },
		Header{ {"Accept",			 "application/json"},
				{"Content-Type",	 "application/json; charset=UTF-8"},
				{"x-auth-key",	     unit.api_key},
				{"x-auth-signature", sign},
				{"x-auth-timestamp", timestamp} },
		Body{ body },
		Timeout{ 30000 });
	lock.unlock();

	KF_LOG_INFO(logger, "[query_wallet_transaction_history] (url) " << url << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());
	getResponse(response.status_code, response.text, response.error.message, json);
	return response;
}

inline int64_t TDEngineAscendEX::getTimestamp()
{
	long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return timestamp;
}

std::string TDEngineAscendEX::getTimestampString()
{
	long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return std::to_string(timestamp);
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

BOOST_PYTHON_MODULE(libascendextd)
{
	using namespace boost::python;
	class_<TDEngineAscendEX, boost::shared_ptr<TDEngineAscendEX> >("Engine")
		.def(init<>())
		.def("init", &TDEngineAscendEX::initialize)
		.def("start", &TDEngineAscendEX::start)
		.def("stop", &TDEngineAscendEX::stop)
		.def("logout", &TDEngineAscendEX::logout)
		.def("wait_for_stop", &TDEngineAscendEX::wait_for_stop);
}