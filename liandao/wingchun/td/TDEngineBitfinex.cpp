#include "TDEngineBitfinex.h"
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

#include<unistd.h>

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
using utils::crypto::hmac_sha384;
using utils::crypto::base64_encode;

USING_WC_NAMESPACE

/*
 an example of insert order and order action , all successful.

[0,"n",[1536659719199,"on-req",null,null,[16662905539,null,4,"tLTCBTC",null,null,0.2001,0.2001,"EXCHANGE LIMIT",null,null,null,null,null,null,null,0.0085489,null,null,null,null,null,null,0,null,null,null,null,null,null,null,null],null,"SUCCESS","Submitting exchange limit buy order for 0.2001 LTC."]]

[0,"on",[16662905539,0,4,"tLTCBTC",1536659719189,1536659719212,0.2001,0.2001,"EXCHANGE LIMIT",null,null,null,0,"ACTIVE",null,null,0.0085489,0,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]

[0,"n",[1536659721156,"oc-req",null,null,[16662905539,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,0,null,null,null,null,null,null,null,null],null,"SUCCESS","Submitted for cancellation; waiting for confirmation (ID: 16662905539)."]]

[0,"oc",[16662905539,0,4,"tLTCBTC",1536659719189,1536659721181,0.2001,0.2001,"EXCHANGE LIMIT",null,null,null,0,"CANCELED",null,null,0.0085489,0,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]


 check journal:


order insert
yjj journal -n TD_SEND_BITFINEX -s 20180911-18:02:00 -e 20181001-19:00:00 -d -t -m 22204
yjj journal -n TD_RAW_BITFINEX -s 20180911-18:02:00 -e 20181001-19:00:00 -d -t -m 22204

order action
yjj journal -n TD_SEND_BITFINEX -s 20180911-18:02:00 -e 20181001-19:00:00 -m 22207
yjj journal -n TD_RAW_BITFINEX -s 20180911-18:02:00 -e 20181001-19:00:00 -d -t -m 22207

order
yjj journal -n TD_BITFINEX -s 20180911-18:02:00 -e 20181001-19:00:00 -d -t -m 205
yjj journal -n TD_RAW_BITFINEX -s 20180911-18:02:00 -e 20181001-19:00:00 -d -t -m 22205

trade

yjj journal -n TD_BITFINEX -s 20180911-18:02:00 -e 20181001-19:00:00 -d -t -m 206
yjj journal -n TD_RAW_BITFINEX -s 20180911-18:02:00 -e 20181001-19:00:00 -d -t -m 22206
 * */
	static TDEngineBitfinex* global_md = nullptr;
/*���Ĳ���nice*/
/*FXW's fix starts here*/
cpr::Response TDEngineBitfinex::cancelOrder(AccountUnitBitfinex& unit, int64_t& remoteOrderId)//v1
{
	std::string Timestamp = std::to_string(getTimestamp());
	std::string orderId = std::to_string(remoteOrderId);
	//std::string orderId = std::to_string(insertData.remoteOrderId);
	std::string requestPath = "/v1/order/cancel";
	std::string queryString = "";
	std::string body = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\"" + "}";
	string Message = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\",\"order_id\":" + orderId + "}";
	string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
	std::string sig = hmac_sha384(unit.secret_key.c_str(), payload.c_str());
	string url = unit.baseUrl + requestPath + queryString;//complete url
	KF_LOG_DEBUG(logger, "[cancelOrder-Rest]the url we ask" << url);
	const auto response = Post(
		Url{ url }, cpr::VerifySsl{ false },
		Header{ {"X-BFX-APIKEY", unit.api_key},
		{"Content-Type", "application/json"},
		{"X-BFX-PAYLOAD", payload},
		{"X-BFX-SIGNATURE",  sig} },
		Body{ body },
		Timeout{ 10000 }
	);

	/*KF_LOG_DEBUG(logger, "[cancelOrder-Rest]rest interface ordercancel test,start from here" << " (rid)" << insertData.requestId
			<< " (Iid)" << insertData.data.InvestorID
			<< " (OrderRef)" << insertData.data.OrderRef
			<<"(body)"<<body);*/
	return response;
}

cpr::Response TDEngineBitfinex::orderStatus(AccountUnitBitfinex& unit, int64_t& remoteOrderId)//v1
{
	/* {
		 "id":23794542055,
			 "cid":1,
			 "cid_date":"2019-04-05",
			 "gid":0,
			 "symbol":"ltcbtc",
			 "exchange":"bitfinex",
			 "price":"0.014",
			 "avg_execution_price":"0.0",
			 "side":"buy",
			 "type":"exchange limit",
			 "timestamp":"1554430044.0",
			 "is_live":false,
			 "is_cancelled":true,
			 "is_hidden":false,
			 "oco_order":null,
			 "was_forced":false,
			 "original_amount":"1.0",
			 "remaining_amount":"1.0",
			 "executed_amount":"0.0",
			 "src":"api"
	 }*/
	std::string Timestamp = std::to_string(getTimestamp());
	std::string orderId = std::to_string(remoteOrderId);
	//std::string orderId = std::to_string(insertData.remoteOrderId);
	std::string requestPath = "/v1/order/status";
	std::string queryString = "";
	std::string body = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\"" + "}";
	string Message = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\",\"order_id\":" + orderId + "}";
	string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
	std::string sig = hmac_sha384(unit.secret_key.c_str(), payload.c_str());
	string url = unit.baseUrl + requestPath + queryString;//complete url
	KF_LOG_DEBUG(logger, "[orderStatus-Rest]the url we ask" << url);
	const auto response = Post(
		Url{ url }, cpr::VerifySsl{ false },
		Header{ {"X-BFX-APIKEY", unit.api_key},
		{"Content-Type", "application/json"},
		{"X-BFX-PAYLOAD", payload},
		{"X-BFX-SIGNATURE",  sig} },
		Body{ body },
		Timeout{ 10000 }
	);
	/*KF_LOG_DEBUG(logger, "[orderStatus-Rest]rest interface order status test,start from here" << " (rid)" << insertData.requestId
			<< " (Iid)" << insertData.data.InvestorID
			<< " (OrderRef)" << insertData.data.OrderRef
			<<"(body)"<<body);*/
	return response;
}

cpr::Response TDEngineBitfinex::retriveTradeStatus(AccountUnitBitfinex& unit, OrderInsertData& insertData)//v2
{
	std::string Timestamp = std::to_string(getTimestamp());
	std::string nonce = Timestamp;
	std::string orderId = std::to_string(insertData.remoteOrderId);
	std::string symbol = "/" + unit.coinPairWhiteList.GetValueByKey(std::string(insertData.data.InstrumentID)) + ":";
	std::string apiPath = "v2/auth/r/order";
	std::string queryParams = "";

	std::string body = "{}";
	std::string signature = "/api/" + apiPath + symbol + orderId + "/trades" + nonce + body;
	std::string sig = hmac_sha384(unit.secret_key.c_str(), signature.c_str());//already is hex!!!
	std::string url = unit.baseUrl + "/" + apiPath + symbol + orderId + "/trades";

	const auto response = Post(
		Url{ url }, cpr::VerifySsl{ false },
		Header{ {"bfx-apikey", unit.api_key},
		{"Content-Type", "application/json"},
		{"bfx-nonce", nonce},
		{"bfx-signature",sig} },
		Body{ body },
		Timeout{ 10000 }
	);
	/*KF_LOG_DEBUG(logger, "[retriveTradeStatus-Rest]" << " (rid)" << insertData.requestId
			<< " (Iid)" << insertData.data.InvestorID
			<< " (OrderRef)" << insertData.data.OrderRef
			<<"(url)"<<url
            <<"(signature)"<<signature
            <<"(sig.hex)"<<sig);*/
    return response;
}

bool TDEngineBitfinex::deal_connect_error(struct lws* conn)//true for success false for failed
{
	
    AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);
    Document json;
    cpr::Response r;
    std::unordered_map<int, OrderInsertData>::iterator itr;
    for (itr = CIDorderInsertData.begin(); itr != CIDorderInsertData.end(); ++itr)
    {
        OrderInsertData &insertData = itr->second;
        std::lock_guard<std::mutex> guard_mutex(*mutex_order_and_trade);
        r = retriveTradeStatus(unit, insertData);
        if (r.status_code >= 200 && r.status_code <= 299)
        {
            KF_LOG_DEBUG(logger, "[retriveTradeStatus] rest interface retriveTradeStatus test successed");
            KF_LOG_INFO(logger, "[retriveTradeStatus] " << " (response.status_code) " << r.status_code <<
                    " (response.text) " << r.text.c_str());
            json.Parse(r.text.c_str());
            if (json.IsArray())
            {
                /*json={[[...],[...],[...]]}*/
                //auto A = json.GetArray();//A=[[...],[...],[...]];
                if (true)//just for the name of my vars not to be the same in this function field
                {
                    //KF_LOG_INFO(logger,"yes , A[0] is an array");
                    int len = json.Size();
                    KF_LOG_DEBUG(logger, "[json.Size]" << len);
                    for (int i = 0; i < len; i++)
                    {
                        KF_LOG_INFO(logger, "bug allocate0");
                        int trade_id = json[i].GetArray()[0].GetInt();
                        KF_LOG_INFO(logger, "bug allocate1"
                                << "(trae_id)" << trade_id);
                        std::string symbol = json[i].GetArray()[1].GetString();
                        KF_LOG_INFO(logger, "(symbol)" << symbol);
                        /*/std::string ticker = unit.coinPairWhiteList.GetKeyByValue(symbol);*/
                        int64_t remoteOrderId = json[i].GetArray()[3].GetInt64();
                        KF_LOG_INFO(logger, "(remoteOrderId)" << remoteOrderId);

                        double exec_amount = json[i].GetArray()[4].GetDouble();
                        KF_LOG_INFO(logger, "(exec_amount)" << exec_amount);
                        double exec_price = json[i].GetArray()[5].GetDouble();
                        KF_LOG_INFO(logger, "(exec_price)" << exec_price);
                        int maker = json[i].GetArray()[8].GetInt();
                        KF_LOG_DEBUG(logger, "[retriveTradeStatus](trade_id)" << trade_id
                                << "(symbol)" << symbol
                                << "(remoteOrderId)" << remoteOrderId
                                << "(exec_amount)" << exec_amount
                                << "(exec_price)" << exec_price
                                << "(maker)" << maker);
                        LFRtnTradeField rtn_trade;//send OnRtnTrade
                        memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
                        strcpy(rtn_trade.ExchangeID, "bitfinex");
                        strncpy(rtn_trade.UserID, unit.api_key.c_str(), 16);

                        strncpy(rtn_trade.TradeID, std::to_string(trade_id).c_str(), 21);
                        /*/strncpy(rtn_trade.InstrumentID, ticker.c_str(), 31);*/
                        strncpy(rtn_trade.InstrumentID, insertData.data.InstrumentID, 31);
                        strncpy(rtn_trade.OrderRef, insertData.data.OrderRef, 13);
                        rtn_trade.OffsetFlag = insertData.data.OffsetFlag;
                        rtn_trade.HedgeFlag = insertData.data.HedgeFlag;

                        if (exec_amount >= 0) {
                            rtn_trade.Volume = std::round(exec_amount * scale_offset);
                            rtn_trade.Direction = LF_CHAR_Buy;
                        }
                        else {
                            rtn_trade.Volume = std::round(exec_amount * scale_offset * -1);
                            rtn_trade.Direction = LF_CHAR_Sell;
                        }

                        rtn_trade.Price = std::round(exec_price * scale_offset);

                        on_rtn_trade(&rtn_trade);
                        raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
                                source_id, MSG_TYPE_LF_RTN_TRADE_BITFINEX, 1, -1);
                    }
                }
            }
            KF_LOG_INFO(logger, "[retriveTradeStatus] " << " on_rtn_trade done ");
        }

        else
        {
            KF_LOG_INFO(logger, "[retriveTradeStatus] failed ");
            return false;
        }
        r = cancelOrder(unit, insertData.remoteOrderId);
        if (r.status_code >= 200 && r.status_code <= 299)
        {
			KF_LOG_DEBUG(logger, "[cancelOrder-Rest] rest interface order cancel successed");
			KF_LOG_INFO(logger, "[cancelOrder-Rest] " << " (response.status_code) " << r.status_code <<
				"\n (response.text) " << r.text.c_str());
		}
		else
		{
			KF_LOG_DEBUG(logger, "[cancelOrder-Rest] rest interface order cancel failed");
			KF_LOG_INFO(logger, "[cancelOrder-Rest] " << " (response.status_code) " << r.status_code <<
				" (response.text) " << r.text.c_str());
			return false;
		}
		KF_LOG_DEBUG(logger, "[orderStatus]start");
		r = orderStatus(unit, insertData.remoteOrderId);
		if (r.status_code >= 200 && r.status_code <= 299)
		{
			KF_LOG_INFO(logger, "[orderStatus] " << " (response.status_code) " << r.status_code <<
				"\n (response.text) " << r.text.c_str());
			KF_LOG_DEBUG(logger, "[orderStatus] rest interface orderStatus successed");
			json.Parse(r.text.c_str());
			if (json.IsObject())
			{
				std::string symbol = json["symbol"].GetString();
				double price = stod(json["price"].GetString());
				bool order_status = json["is_cancelled"].GetBool();
				double original_amount = stod(json["original_amount"].GetString());
				double remaining_amount = stod(json["remaining_amount"].GetString());
				double executed_amount = stod(json["executed_amount"].GetString());
				LFRtnOrderField rtn_order;
				memset(&rtn_order, 0, sizeof(LFRtnOrderField));
				strcpy(rtn_order.ExchangeID, "bitfinex");
				strncpy(rtn_order.UserID, unit.api_key.c_str(), 16);

				KF_LOG_DEBUG(logger, "[orderStatus] (exchange_ticker)" << symbol);
				strncpy(rtn_order.InstrumentID, insertData.data.InstrumentID, 31);
				if (remaining_amount == 0)
				{
					rtn_order.OrderStatus = GetOrderStatus("EXECUTED");
				}
				else if (order_status == true)
				{
					rtn_order.OrderStatus = GetOrderStatus("CANCELED");
				}
				if (insertData.data.Direction == LF_CHAR_Buy) {

					rtn_order.VolumeTotal = std::round(remaining_amount * scale_offset);
					rtn_order.Direction = LF_CHAR_Buy;
				}
				else {
					rtn_order.VolumeTotal = std::round(remaining_amount * scale_offset * -1);
					rtn_order.Direction = LF_CHAR_Sell;
				}
				if (original_amount > 0) {

					rtn_order.VolumeTotalOriginal = std::round(original_amount * scale_offset);
				}
				else {
					rtn_order.VolumeTotalOriginal = std::round(original_amount * scale_offset * -1);
				}
				rtn_order.VolumeTraded = executed_amount;
				rtn_order.TimeCondition = insertData.data.TimeCondition;

				rtn_order.OrderPriceType = insertData.data.OrderPriceType;
				strncpy(rtn_order.OrderRef, insertData.data.OrderRef, 13);

				rtn_order.LimitPrice = std::round(price * scale_offset);
				KF_LOG_DEBUG(logger, "[OrderStatus] (test point3"
					<< "(symbol)" << symbol
					<< "(price)" << price
					<< "(order_status)" << order_status
					<< "(original_amount)" << original_amount
					<< "(remaining_amount)" << remaining_amount
					<< "(executed_amount)" << executed_amount);
				on_rtn_order(&rtn_order);
				raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
					source_id, MSG_TYPE_LF_RTN_ORDER_BITFINEX,
					1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);
			}

			KF_LOG_INFO(logger, "[orderStatus] " << " on_rtn_order done ");

		}
		else
		{
			KF_LOG_DEBUG(logger, "[cancelStatus-Rest] rest interface order status failed");
			KF_LOG_INFO(logger, "[cancelStatus-Rest] " << " (response.status_code) " << r.status_code <<
				" (response.text) " << r.text.c_str());
			return false;
		}


	}

	return true;
}
/*FXW's fix ends here*/



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
		std::cout << "3.1415926 LWS_CALLBACK_CLIENT_CLOSED, reason = " << reason << std::endl;
		if (global_md) {
			std::cout << "3.1415926 LWS_CALLBACK_CLIENT_CLOSED 2,  (call on_lws_connection_error)  reason = " << reason << std::endl;
			global_md->on_lws_connection_error(wsi);
			if (global_md->deal_connect_error(wsi))
			{
				std::cout << "data loss caused byconnect error fixed " << std::endl;
			}
			else
			{
				std::cout << "data loss caused byconnect error fixing failed " << std::endl;
			}
		}
		break;
	}
	case LWS_CALLBACK_CLIENT_RECEIVE_PONG:
	{
		std::cout << "3.1415926 LWS_CALLBACK_CLIENT_RECEIVE_PONG, reason = " << reason << std::endl;
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


TDEngineBitfinex::TDEngineBitfinex() : ITDEngine(SOURCE_BITFINEX)
{
	logger = yijinjing::KfLog::getLogger("TradeEngine.Bitfinex");
	KF_LOG_INFO(logger, "[TDEngineBitfinex]");

	mutex_order_and_trade = new std::mutex();
    mutex_orderaction_waiting_response = new std::mutex();
    mutex_orderinsert_waiting_response = new std::mutex();
}

TDEngineBitfinex::~TDEngineBitfinex()
{
	if (mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
    if(mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
    if(mutex_orderinsert_waiting_response != nullptr) delete mutex_orderinsert_waiting_response;
}

void TDEngineBitfinex::init()
{
	ITDEngine::init();
	JournalPair tdRawPair = getTdRawJournalPair(source_id);
	raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
	KF_LOG_INFO(logger, "[init]");
}

void TDEngineBitfinex::pre_load(const json& j_config)
{
	KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineBitfinex::resize_accounts(int account_num)
{
	account_units.resize(account_num);
	KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineBitfinex::load_account(int idx, const json& j_config)
{
	KF_LOG_INFO(logger, "[load_account]");
	// internal load
	string api_key = j_config["APIKey"].get<string>();
	string secret_key = j_config["SecretKey"].get<string>();
	string baseUrl = j_config["baseUrl"].get<string>();
	rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

	AccountUnitBitfinex& unit = account_units[idx];
	unit.api_key = api_key;
	unit.secret_key = secret_key;
	unit.baseUrl = baseUrl;

	KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl);

	unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
	unit.coinPairWhiteList.Debug_print();

	unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
	unit.positionWhiteList.Debug_print();

	//display usage:
	if (unit.coinPairWhiteList.Size() == 0) {
		KF_LOG_ERROR(logger, "TDEngineBitfinex::load_account: subscribeBitfinexBaseQuote is empty. please add whiteLists in kungfu.json like this :");
		KF_LOG_ERROR(logger, "\"whiteLists\":{");
		KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
		KF_LOG_ERROR(logger, "    \"btc_usdt\": \"tBTCUSDT\",");
		KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
		KF_LOG_ERROR(logger, "},");
	}

	//cancel all openning orders on TD startup---dont need do anything: we use :Dead-Man-Switch flag (optional). Values: 4

	// set up
	TradeAccount account = {};
	//partly copy this fields
	strncpy(account.UserID, api_key.c_str(), 16);
	strncpy(account.Password, secret_key.c_str(), 21);
	return account;
}

void TDEngineBitfinex::connect(long timeout_nsec)
{
	KF_LOG_INFO(logger, "[connect]");
	for (size_t idx = 0; idx < account_units.size(); idx++)
	{
		AccountUnitBitfinex& unit = account_units[idx];
		KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
		Document doc;
		cancel_all_orders(unit, doc);

		if (!unit.logged_in)
		{
			unit.newPendingSendMsg.push_back(createAuthJsonString(unit));
			lws_login(unit, 0);
			//set true to for let the kungfuctl think td is running.
			unit.logged_in = true;
		}
	}

}

void TDEngineBitfinex::login(long timeout_nsec)
{
	KF_LOG_INFO(logger, "[login]");
	connect(timeout_nsec);
}

void TDEngineBitfinex::logout()
{
	KF_LOG_INFO(logger, "[logout]");
}

void TDEngineBitfinex::release_api()
{
	KF_LOG_INFO(logger, "[release_api]");
	//
}

bool TDEngineBitfinex::is_logged_in() const
{
	KF_LOG_INFO(logger, "[is_logged_in]");
	for (auto& unit : account_units)
	{
		if (!unit.logged_in)
			return false;
	}
	return true;
}

bool TDEngineBitfinex::is_connected() const
{
	KF_LOG_INFO(logger, "[is_connected]");
	return is_logged_in();
}



//type	string	MARKET, EXCHANGE MARKET, LIMIT, EXCHANGE LIMIT, STOP, EXCHANGE STOP, TRAILING STOP, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK, STOP LIMIT, EXCHANGE STOP LIMIT
//std::string TDEngineBitfinex::GetType(const LfOrderPriceTypeType& input) {
//    if (LF_CHAR_LimitPrice == input) {
//        return "LIMIT";
//    } else if (LF_CHAR_AnyPrice == input) {
//        return "MARKET";
//    } else {
//        return "";
//    }
//}

std::string TDEngineBitfinex::GetType(const LfOrderPriceTypeType& input) {
	if (LF_CHAR_LimitPrice == input) {
		return "EXCHANGE LIMIT";
	}
	else if (LF_CHAR_AnyPrice == input) {
		return "EXCHANGE MARKET";
	}
	else {
		return "";
	}
}

LfOrderPriceTypeType TDEngineBitfinex::GetPriceType(std::string input) {
	if ("LIMIT" == input || "EXCHANGE LIMIT" == input) {
		return LF_CHAR_LimitPrice;
	}
	else if ("MARKET" == input || "EXCHANGE MARKET" == input) {
		return LF_CHAR_AnyPrice;
	}
	else {
		return '0';
	}
}

//Order Status: ACTIVE, EXECUTED @ PRICE(AMOUNT) e.g. "EXECUTED @ 107.6(-0.2)",
// PARTIALLY FILLED @ PRICE(AMOUNT), INSUFFICIENT MARGIN was: PARTIALLY FILLED @ PRICE(AMOUNT), CANCELED, CANCELED was: PARTIALLY FILLED @ PRICE(AMOUNT)

//true example:
//[0,"oc",[16653210798,0,2,"tLTCBTC",1536641097028,1536641097058,0,-1.0001,"LIMIT",null,null,null,0,"EXECUTED @ 0.0085406(-0.31225457): was PARTIALLY FILLED @ 0.0085406(-0.68784543)",null,null,0.0085397,0.0085406,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
//[0,"oc",[16649428489,0,2,"tLTCBTC",1536633838845,1536633840840,-0.2001,-0.2001,"LIMIT",null,null,null,0,"CANCELED",null,null,9.9999,0,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
//[0,"oc",[16649214640,0,1,"tLTCBTC",1536633469325,1536633469352,0,0.2001,"MARKET",null,null,null,0,"EXECUTED @ 0.008535(0.2001)",null,null,0.00854,0.008535,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
//[0,"oc",[16650541381,0,2,"tLTCBTC",1536635990678,1536635990698,0,-1.0104,"LIMIT",null,null,null,0,"EXECUTED @ 0.0085376(-1.0104)",null,null,0.0085376,0.0085376,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
//[0,"oc",[17748166239,0,2,"tLTCBTC",1538872907976,1538872907992,0.2,0.2,"EXCHANGE FOK",null,null,null,0,"FILLORKILL CANCELED",null,null,0.00876,0,0,0,null,null,null,0,0,null,null,null,"API>BFX",null,null,null]]

LfOrderStatusType TDEngineBitfinex::GetOrderStatus(std::string input) {
	if (startWith(input, "ACTIVE")) {
		return LF_CHAR_NotTouched;
	}
	else if (startWith(input, "PARTIALLY FILLED")) {
		return LF_CHAR_PartTradedQueueing;
	}
	else if (startWith(input, "CANCELED")) {
		return LF_CHAR_Canceled;
	}
	else if (startWith(input, "EXECUTED")) {
		return LF_CHAR_AllTraded;
	}
	else if (startWith(input, "INSUFFICIENT")) {
		return LF_CHAR_Error;
	}
	else if (startWith(input, "ERROR")) {
		return LF_CHAR_Error;
	}
	else if (input.find("FILLORKILL CANCELED") != std::string::npos) {
		return LF_CHAR_Canceled;
	}
	else {
		return LF_CHAR_NotTouched;
	}
}


void TDEngineBitfinex::lws_login(AccountUnitBitfinex& unit, long timeout_nsec) {
	KF_LOG_INFO(logger, "TDEngineBitfinex::login:");
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

		context = lws_create_context(&info);
		KF_LOG_INFO(logger, "TDEngineBitfinex::login: context created.");
	}

	if (context == NULL) {
		KF_LOG_ERROR(logger, "TDEngineBitfinex::login: context is NULL. return");
		return;
	}

	int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
	lws_set_log_level(logs, NULL);

	struct lws_client_connect_info ccinfo = { 0 };

	static std::string host = "api.bitfinex.com";
	static std::string path = "/ws/2";
	static int port = 443;

	ccinfo.context = context;
	ccinfo.address = host.c_str();
	ccinfo.port = port;
	ccinfo.path = path.c_str();
	ccinfo.host = host.c_str();
	ccinfo.origin = host.c_str();
	ccinfo.ietf_version_or_minus_one = -1;
	ccinfo.protocol = protocols[0].name;
	ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

	unit.websocketConn = lws_client_connect_via_info(&ccinfo);
	KF_LOG_INFO(logger, "TDEngineBitfinex::login: Connecting to " << ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

	if (unit.websocketConn == NULL) {
		KF_LOG_ERROR(logger, "TDEngineBitfinex::login: wsi create error.");
		return;
	}
	KF_LOG_INFO(logger, "TDEngineBitfinex::login: wsi create success.");
	unit.is_connecting = true;
}


int TDEngineBitfinex::lws_write_subscribe(struct lws* conn)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::lws_write_subscribe");
	AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);
	moveNewtoPending(unit);

	//�д����͵����ݣ��ȰѴ����͵ķ��꣬�ڼ��������߼���  ping?
	if (unit.pendingSendMsg.size() > 0) {
		unsigned char msg[512];
		memset(&msg[LWS_PRE], 0, 512 - LWS_PRE);

		std::string jsonString = unit.pendingSendMsg[unit.pendingSendMsg.size() - 1];
		unit.pendingSendMsg.pop_back();
		KF_LOG_INFO(logger, "TDEngineBitfinex::lws_write_subscribe: websocketPendingSendMsg: " << jsonString.c_str());
		int length = jsonString.length();

		strncpy((char *)msg + LWS_PRE, jsonString.c_str(), length);
		int ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);

		if (unit.pendingSendMsg.size() > 0)
		{    //still has pending send data, emit a lws_callback_on_writable()
			lws_callback_on_writable(conn);
			KF_LOG_INFO(logger, "TDEngineBitfinex::lws_write_subscribe: (websocketPendingSendMsg,size)" << unit.pendingSendMsg.size());
		}
		return ret;
	}
	return 0;
}


/*
 * 1)   {"event":"info","version":2,"serverId":"f14477e6-3b99-4b48-a65d-f252bd265aa1","platform":{"status":1}}
 * 2)   {"event":"auth","status":"OK","chanId":0,"userId":2103396,"dms":4,"auth_id":"6386d4dc-978f-4c03-a239-4683f7654d9e","caps":{"orders":{"read":1,"write":1},"account":{"read":1,"write":0},"funding":{"read":1,"write":1},"history":{"read":1,"write":0},"wallets":{"read":1,"write":1},"withdraw":{"read":0,"write":0},"positions":{"read":1,"write":1}}}
 *
 * */
void TDEngineBitfinex::on_lws_data(struct lws* conn, const char* data, size_t len)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::on_lws_data: " << data);
	AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);

	Document json;
	json.Parse(data);

	if (json.HasParseError()) {
		KF_LOG_ERROR(logger, "TDEngineBitfinex::on_lws_data. parse json error: " << data);
		return;
	}

	if (json.IsObject() && json.HasMember("event")) {
		if (strcmp(json["event"].GetString(), "info") == 0) {
			KF_LOG_INFO(logger, "TDEngineBitfinex::on_lws_data: is info");
			onInfo(json);
		}
		else if (strcmp(json["event"].GetString(), "auth") == 0) {
			KF_LOG_INFO(logger, "TDEngineBitfinex::on_lws_data: is auth");
			onAuth(conn, json);
		}
		else {
			KF_LOG_INFO(logger, "TDEngineBitfinex::on_lws_data: unknown event: " << data);
		};
	}
	/*
	 *
	 * on_lws_data: [0,"ps",[]]
	3.1415926 LWS_CALLBACK_CLIENT_RECEIVE on data, reason = 8
	on_lws_data: [0,"ws",[]]
	3.1415926 LWS_CALLBACK_CLIENT_RECEIVE on data, reason = 8
	on_lws_data: [0,"os",[]]
	3.1415926 LWS_CALLBACK_CLIENT_RECEIVE on data, reason = 8
	on_lws_data: [0,"fos",[]]
	3.1415926 LWS_CALLBACK_CLIENT_RECEIVE on data, reason = 8
	on_lws_data: [0,"fcs",[]]
	3.1415926 LWS_CALLBACK_CLIENT_RECEIVE on data, reason = 8
	on_lws_data: [0,"fls",[]]


	 //[ CHANNEL_ID, "hb" ]
	 * */
	 //data
	if (json.IsArray()) {
		int len = json.Size();
		if (len != 3) {
			KF_LOG_DEBUG(logger, "TDEngineBitfinex::on_lws_data: (len<3, is hb?)" << data);
			return;
		};

		int chanId = json.GetArray()[0].GetInt();
		KF_LOG_INFO(logger, "TDEngineBitfinex::on_lws_data: (chanId)" << chanId);

		if (json.GetArray()[1].IsString()) {
			std::string dataType = json.GetArray()[1].GetString();
			KF_LOG_INFO(logger, "TDEngineBitfinex::on_lws_data: dataType: " << dataType);
			if (dataType == "ps") {
				//onPosition(conn, json);
			}
			if (dataType == "te") {
				onTradeExecuted(conn, json);
			}
			if (dataType == "tu") {
				onTradeExecutionUpdate(conn, json);
			}
			if (dataType == "os") {
				//fix duplicate send 'no touch' on_rtn_order
//                onOrderSnapshot(conn, json);
			}
			if (dataType == "on" || dataType == "ou" || dataType == "oc") {
				onOrderNewUpdateCancel(conn, json);
			}
			if (dataType == "n") {
				onNotification(conn, json);
			}
		}
	}
}

void TDEngineBitfinex::onInfo(Document& json)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::onInfo: " << parseJsonToString(json));
}

void TDEngineBitfinex::onAuth(struct lws* conn, Document& json)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::onAuth: " << parseJsonToString(json));

	if (json.IsObject() && json.HasMember("status")) {
		std::string status = json["status"].GetString();
		std::transform(status.begin(), status.end(), status.begin(), ::toupper);

		if (status == "OK") {
			//login ok
			AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);
			unit.logged_in = true;
			KF_LOG_INFO(logger, "TDEngineBitfinex::onAuth success: " << parseJsonToString(json));
		}
		else {
			//login fail.
			AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);
			unit.logged_in = false;
			KF_LOG_INFO(logger, "TDEngineBitfinex::onAuth fail:" << parseJsonToString(json));
		}
	}
}

/*
	[
	  CHAN_ID,
	  'ps',
	  [
		[
		  SYMBOL,
		  STATUS,
		  AMOUNT,
		  BASE_PRICE,
		  MARGIN_FUNDING,
		  MARGIN_FUNDING_TYPE,
		  PL,
		  PL_PERC,
		  PRICE_LIQ,
		  LEVERAGE,
		  FLAG
		  ...
		],
		...
	  ]
	]

SYMBOL	string	Pair (tBTCUSD, ��).
STATUS	string	Status (ACTIVE, CLOSED).
��AMOUNT	float	Size of the position. Positive values means a long position, negative values means a short position.
BASE_PRICE	float	The price at which you entered your position.
MARGIN_FUNDING	float	The amount of funding being used for this position.
MARGIN_FUNDING_TYPE	int	0 for daily, 1 for term.
PL	float	Profit & Loss
PL_PERC	float	Profit & Loss Percentage
PRICE_LIQ	float	Liquidation price
LEVERAGE	float	Beta value
 * */
void TDEngineBitfinex::onPosition(struct lws* conn, Document& json)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::onPosition: " << parseJsonToString(json));
	AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);

	if (json.GetArray()[2].IsArray()) {
		int len = json.GetArray()[2].Size();
		for (int i = 0; i < len; i++) {
			auto& position_i = json.GetArray()[2].GetArray()[i];
			if (position_i.IsArray() && position_i.Size() > 0) {
				std::string symbol = position_i.GetArray()[0].GetString();
				std::string status = position_i.GetArray()[1].GetString();
				std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
				if (ticker.length() > 0 && status == "ACTIVE") {
					PositionSetting position;
					position.ticker = ticker;
					double amount = position_i.GetArray()[2].GetDouble();
					if (amount < 0) {
						position.isLong = false;
						position.amount = std::round(amount * scale_offset * -1);
					}
					else {
						position.isLong = true;
						position.amount = std::round(amount * scale_offset);
					}
					positionHolder.push_back(position);
					KF_LOG_INFO(logger, "TDEngineBitfinex::on_lws_data: position: (ticker)"
						<< ticker << " (isLong)" << position.isLong << " (amount)" << position.amount);
				}
			}
		}

	}
}
/*
 [
  CHAN_ID,
  'te',
  [
	ID,
	SYMBOL,
	MTS_CREATE,
	ORDER_ID,
	EXEC_AMOUNT,
	EXEC_PRICE,
	ORDER_TYPE,
	ORDER_PRICE,
	MAKER,
	...
  ]
]

 ID	integer	Trade database id
PAIR	string	Pair (BTCUSD, ��)
MTS_CREATE	integer	Execution timestamp
ORDER_ID	integer	Order id
EXEC_AMOUNT	float	Positive means buy, negative means sell
EXEC_PRICE	float	Execution price
ORDER_TYPE	string	Order type
ORDER_PRICE	float	Order price
MAKER	int	1 if true, 0 if false

 * */
 //[0,"te",[291788386,"tLTCBTC",1536562548954,16609409732,0.21,0.008763,null,null,-1,null,null,null]]
 //[0,"te",[292081982,"tLTCBTC",1536641097049,16653210798,-0.68784543,0.0085406,null,null,-1,null,null,null]]
 //[0,"te",[292081983,"tLTCBTC",1536641097051,16653210798,-0.31225457,0.0085406,null,null,-1,null,null,null]]
 //[0,"tu",[292081982,"tLTCBTC",1536641097049,16653210798,-0.68784543,0.0085406,"LIMIT",0.0085397,-1,-0.07469864,"USD"]]
 //[0,"tu",[292081983,"tLTCBTC",1536641097051,16653210798,-0.31225457,0.0085406,"LIMIT",0.0085397,-1,-0.03391022,"USD"]]
 //te

 /*
  ��Ϊoc���ȵ����ģ� oc�������EXECUTED��ͷ��״̬����on_rtn_order����ȫ�ɽ���te��tu�Ǻ��������ģ�����te������ֻ��EXEC_AMOUNT	û��origin_amount����֪���ǲ���ȫ�ɽ����������ֳɽ�on_rtn_order�������������������������������on_rtn_order��ֱ����֮���tuֱ�ӱ�on_rtn_trade
  * */
void TDEngineBitfinex::onTradeExecuted(struct lws* conn, Document& json)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::onTradeExecuted.");

	if (1 == 1) return;

	AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);

	if (json.GetArray()[2].IsArray()) {
		auto& orderStatus = json.GetArray()[2];
		int len = orderStatus.Size();
		int64_t trade_id = orderStatus.GetArray()[0].GetInt64();
		std::string symbol = orderStatus.GetArray()[1].GetString();
		int64_t remoteOrderId = orderStatus.GetArray()[3].GetInt64();
		double exec_amount = orderStatus.GetArray()[4].GetDouble();
		double exec_price = orderStatus.GetArray()[5].GetDouble();
		int maker = orderStatus.GetArray()[8].GetInt();
		KF_LOG_INFO(logger, "TDEngineBitfinex::onTradeExecuted: (trade_id)" << trade_id << " (symbol)" << symbol
			<< " (orderId)" << remoteOrderId
			<< " (exec_amount)" << exec_amount
			<< " (exec_price)" << exec_price
			<< " (maker)" << maker);

		std::string ticker = unit.coinPairWhiteList.GetKeyByValue(symbol);
		if (ticker.length() == 0) {
			KF_LOG_INFO(logger, "[onTradeExecuted]: not in WhiteList , ignore it:" << symbol);
			return;
		}

		OrderInsertData InsertData = findOrderInsertDataByOrderId(remoteOrderId);
		if (InsertData.requestId == 0) {
			//not found
			KF_LOG_INFO(logger, "TDEngineBitfinex::onTradeExecuted: cannot find orderId, ignore (orderId)" << remoteOrderId);
			return;
		}
		KF_LOG_DEBUG(logger, "[onTradeExecuted] (exchange_ticker)" << ticker);

		LFRtnOrderField rtn_order;
		memset(&rtn_order, 0, sizeof(LFRtnOrderField));
		strcpy(rtn_order.ExchangeID, "bitfinex");
		strncpy(rtn_order.UserID, unit.api_key.c_str(), 16);

		//���ַ����޷��ж�ȫ���ɽ��� ֻ��һֱ�ǲ��ֳɽ���״̬
		rtn_order.OrderStatus = LF_CHAR_PartTradedQueueing;

		strncpy(rtn_order.InstrumentID, ticker.c_str(), 31);

		if (exec_amount >= 0) {
			rtn_order.VolumeTraded = std::round(exec_amount * scale_offset);
			rtn_order.Direction = LF_CHAR_Buy;
		}
		else {
			rtn_order.VolumeTraded = std::round(exec_amount * scale_offset * -1);
			rtn_order.Direction = LF_CHAR_Sell;
		}

		rtn_order.TimeCondition = InsertData.data.TimeCondition;

		rtn_order.OrderPriceType = InsertData.data.OrderPriceType;
		strncpy(rtn_order.OrderRef, InsertData.data.OrderRef, 13);
		rtn_order.VolumeTotalOriginal = InsertData.data.Volume;
		rtn_order.LimitPrice = InsertData.data.LimitPrice;
		rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTraded;

		on_rtn_order(&rtn_order);
		raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
			source_id, MSG_TYPE_LF_RTN_ORDER_BITFINEX,
			1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);
	}
}


/*
 * [
  CHAN_ID,
  'tu',
  [
	ID,
	PAIR,
	MTS_CREATE,
	ORDER_ID,
	EXEC_AMOUNT,
	EXEC_PRICE,
	ORDER_TYPE,
	ORDER_PRICE,
	MAKER,
	FEE,
	FEE_CURRENCY,
	...
  ]
]

ID	integer	Trade database id
PAIR	string	Pair (BTCUSD, ��)
MTS_CREATE	integer	Execution timestamp
ORDER_ID	integer	Order id
EXEC_AMOUNT	float	Positive means buy, negative means sell
EXEC_PRICE	float	Execution price
ORDER_TYPE	string	Order type
ORDER_PRICE	float	Order price
MAKER	int	1 if true, 0 if false
FEE	float	Fee
FEE_CURRENCY	string	Fee currency

 tu
 * */


 //PARTIALLY FILLED:
 //[0,"te",[292081982,"tLTCBTC",1536641097049,16653210798,-0.68784543,0.0085406,null,null,-1,null,null,null]]
 //[0,"te",[292081983,"tLTCBTC",1536641097051,16653210798,-0.31225457,0.0085406,null,null,-1,null,null,null]]
 //[0,"tu",[292081982,"tLTCBTC",1536641097049,16653210798,-0.68784543,0.0085406,"LIMIT",0.0085397,-1,-0.07469864,"USD"]]
 //[0,"tu",[292081983,"tLTCBTC",1536641097051,16653210798,-0.31225457,0.0085406,"LIMIT",0.0085397,-1,-0.03391022,"USD"]]

 //tu
void TDEngineBitfinex::onTradeExecutionUpdate(struct lws* conn, Document& json)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::onTradeExecutionUpdate.");
	AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);

	if (json.GetArray()[2].IsArray()) {
		auto& orderStatus = json.GetArray()[2];
		int len = orderStatus.Size();
		int64_t trade_id = orderStatus.GetArray()[0].GetInt64();
		std::string symbol = orderStatus.GetArray()[1].GetString();
		std::string ticker = unit.coinPairWhiteList.GetKeyByValue(symbol);
		if (ticker.length() == 0) {
			KF_LOG_INFO(logger, "[onTradeExecutionUpdate]: not in WhiteList , ignore it:" << symbol);
			return;
		}
		KF_LOG_DEBUG(logger, "[onTradeExecutionUpdate] (exchange_ticker)" << ticker);

		int64_t remoteOrderId = orderStatus.GetArray()[3].GetInt64();
		double exec_amount = orderStatus.GetArray()[4].GetDouble();
		double exec_price = orderStatus.GetArray()[5].GetDouble();
		//std::string orderType = orderStatus.GetArray()[6].GetString();// null
		//double order_price = orderStatus.GetArray()[7].GetDouble();// null
		int maker = orderStatus.GetArray()[8].GetInt();
		KF_LOG_INFO(logger, "TDEngineBitfinex::onTradeExecutionUpdate: (trade_id)" << trade_id << " (symbol)" << symbol
			<< " (orderId)" << remoteOrderId
			<< " (exec_amount)" << exec_amount
			<< " (exec_price)" << exec_price
			<< " (maker)" << maker);



		OrderInsertData InsertData = findOrderInsertDataByOrderId(remoteOrderId);
		if (InsertData.requestId == 0) {
			//not found
			KF_LOG_INFO(logger, "TDEngineBitfinex::onTradeExecutionUpdate: cannot find orderId, ignore (orderId)" << remoteOrderId);
			return;
		}
		auto it = remoteOrderIdOrderInsertSentTime.find(std::string(InsertData.data.OrderRef));
        if(it != remoteOrderIdOrderInsertSentTime.end()){
        	remoteOrderIdOrderInsertSentTime.erase(it);
        }
		auto itr = remoteOrderIdOrderActionSentTime.find(std::string(InsertData.data.OrderRef));
        if(itr != remoteOrderIdOrderActionSentTime.end()){
        	remoteOrderIdOrderActionSentTime.erase(itr);
        }        
		/*std::map<std::string, OrderInsertSentTime>::iterator itr;
		for (itr = remoteOrderIdOrderInsertSentTime.begin(); itr != remoteOrderIdOrderInsertSentTime.end(); ++itr){
			if(itr->second==InsertData){
				remoteOrderIdOrderInsertSentTime.erase(itr);
			}
		}
		std::map<std::string, OrderActionSentTime>::iterator itr;
		for (itr = remoteOrderIdOrderActionSentTime.begin(); itr != remoteOrderIdOrderActionSentTime.end(); ++itr){
			if(itr->second==InsertData){
				remoteOrderIdOrderActionSentTime.erase(itr);
			}
		}*/
		//send OnRtnTrade
		LFRtnTradeField rtn_trade;
		memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
		strcpy(rtn_trade.ExchangeID, "bitfinex");
		strncpy(rtn_trade.UserID, unit.api_key.c_str(), 16);

		strncpy(rtn_trade.TradeID, std::to_string(trade_id).c_str(), 21);
		strncpy(rtn_trade.InstrumentID, ticker.c_str(), 31);
		strncpy(rtn_trade.OrderRef, InsertData.data.OrderRef, 13);
		rtn_trade.OffsetFlag = InsertData.data.OffsetFlag;
		rtn_trade.HedgeFlag = InsertData.data.HedgeFlag;

		if (exec_amount >= 0) {
			rtn_trade.Volume = std::round(exec_amount * scale_offset);
			rtn_trade.Direction = LF_CHAR_Buy;
		}
		else {
			rtn_trade.Volume = std::round(exec_amount * scale_offset * -1);
			rtn_trade.Direction = LF_CHAR_Sell;
		}

		rtn_trade.Price = std::round(exec_price * scale_offset);

		on_rtn_trade(&rtn_trade);
		raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
			source_id, MSG_TYPE_LF_RTN_TRADE_BITFINEX, 1, -1);
	}
}



/*
 * [
  CHAN_ID,
  'os',
  [
	[
	  ID,
	  GID,
	  CID,
	  SYMBOL,
	  MTS_CREATE,
	  MTS_UPDATE,
	  AMOUNT,
	  AMOUNT_ORIG,
	  TYPE,
	  TYPE_PREV,
	  _PLACEHOLDER,
	  _PLACEHOLDER,
	  FLAGS,
	  STATUS,
	  _PLACEHOLDER,
	  _PLACEHOLDER,
	  PRICE,
	  PRICE_AVG,
	  PRICE_TRAILING,
	  PRICE_AUX_LIMIT,
	  _PLACEHOLDER,
	  _PLACEHOLDER,
	  _PLACEHOLDER,
	  NOTIFY,
	  _PLACEHOLDER,
	  PLACED_ID,
	  ...
	],
	...
  ]
]

 ID	int	Order ID
GID	int	Group ID
CID	int	Client Order ID
SYMBOL	string	Pair (tBTCUSD, ��)
MTS_CREATE	int	Millisecond timestamp of creation
MTS_UPDATE	int	Millisecond timestamp of update
AMOUNT	float	Positive means buy, negative means sell.
AMOUNT_ORIG	float	Original amount
TYPE	string	The type of the order: LIMIT, MARKET, STOP, TRAILING STOP, EXCHANGE MARKET, EXCHANGE LIMIT, EXCHANGE STOP, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK.
TYPE_PREV	string	Previous order type
ORDER_STATUS	string	Order Status: ACTIVE, EXECUTED @ PRICE(AMOUNT) e.g. "EXECUTED @ 107.6(-0.2)", PARTIALLY FILLED @ PRICE(AMOUNT), INSUFFICIENT MARGIN was: PARTIALLY FILLED @ PRICE(AMOUNT), CANCELED, CANCELED was: PARTIALLY FILLED @ PRICE(AMOUNT)
PRICE	float	Price
PRICE_AVG	float	Average price
PRICE_TRAILING	float	The trailing price
PRICE_AUX_LIMIT	float	Auxiliary Limit price (for STOP LIMIT)
PLACED_ID	int	If another order caused this order to be placed (OCO) this will be that other order's ID
FLAGS	int	See flags below.

os	order snapshot
 * */
void TDEngineBitfinex::onOrderSnapshot(struct lws* conn, Document& json)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::onOrderSnapshot: " << parseJsonToString(json));
	AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);

	if (json.GetArray()[2].IsArray()) {
		auto& orderStatus = json.GetArray()[2];
		int len = orderStatus.Size();
		for (int i = 0; i < len; i++) {
			auto &order_i = orderStatus.GetArray()[i];
			onOrder(conn, order_i);
		}
	}
}

/*
on	new order
ou	order update
oc	order cancel



 [
  CHAN_ID,
  <'on', 'ou', 'oc'>,
  [
	ID,
	GID,
	CID,
	SYMBOL,
	MTS_CREATE,
	MTS_UPDATE,
	AMOUNT,
	AMOUNT_ORIG,
	TYPE,
	TYPE_PREV,
	_PLACEHOLDER,
	_PLACEHOLDER,
	FLAGS,
	STATUS,
	_PLACEHOLDER,
	_PLACEHOLDER,
	PRICE,
	PRICE_AVG,
	PRICE_TRAILING,
	PRICE_AUX_LIMIT,
	_PLACEHOLDER,
	_PLACEHOLDER,
	_PLACEHOLDER,
	NOTIFY,
	HIDDEN,
	PLACED_ID,
	...
  ]
]


 ID	int	Order ID
GID	int	Group ID
CID	int	Client Order ID
SYMBOL	string	Pair (tBTCUSD, ��)
MTS_CREATE	int	Millisecond timestamp of creation
MTS_UPDATE	int	Millisecond timestamp of update
AMOUNT	float	Positive means buy, negative means sell.
AMOUNT_ORIG	float	Original amount
TYPE	string	The type of the order: LIMIT, MARKET, STOP, TRAILING STOP, EXCHANGE MARKET, EXCHANGE LIMIT, EXCHANGE STOP, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK.
TYPE_PREV	string	Previous order type
ORDER_STATUS	string	Order Status: ACTIVE, EXECUTED @ PRICE(AMOUNT) e.g. "EXECUTED @ 107.6(-0.2)", PARTIALLY FILLED @ PRICE(AMOUNT), INSUFFICIENT MARGIN was: PARTIALLY FILLED @ PRICE(AMOUNT), CANCELED, CANCELED was: PARTIALLY FILLED @ PRICE(AMOUNT)

-------�������� EXECUTED��ͷ��״̬����ʾȫ�ɽ�����ʵ���ϣ����״̬���ܰ������ֳɽ�֮���ȫ�ɽ���
-------��ʱ��oc�������Ǹ����ֳɽ���ֱ�ӱ������µ�ȫ�ɽ���������ô������ʵ�������ɽ���Ҫ�����������tu�� ���γ�����on_rtn_trade
"EXECUTED @ 0.0085406(-0.31225457): was PARTIALLY FILLED @ 0.0085406(-0.68784543)",


PRICE	float	Price
PRICE_AVG	float	Average price
PRICE_TRAILING	float	The trailing price
PRICE_AUX_LIMIT	float	Auxiliary Limit price (for STOP LIMIT)
PLACED_ID	int	If another order caused this order to be placed (OCO) this will be that other order's ID
FLAGS	int	See flags below.

 * */


 //[0,"oc",[16649214640,0,1,"tLTCBTC",1536633469325,1536633469352,0,0.2001,"MARKET",null,null,null,0,"EXECUTED @ 0.008535(0.2001)",null,null,0.00854,0.008535,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
 //[0,"te",[292064805,"tLTCBTC",1536633469341,16649214640,0.2001,0.008535,null,null,-1,null,null,null]]
 //[0,"tu",[292064805,"tLTCBTC",1536633469341,16649214640,0.2001,0.008535,"MARKET",0.00854,-1,-0.02171085,"USD"]]

 //[0,"oc",[16649428489,0,2,"tLTCBTC",1536633838845,1536633840840,-0.2001,-0.2001,"LIMIT",null,null,null,0,"CANCELED",null,null,9.9999,0,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
 //
 //[0,"oc",[16649592438,0,1,"tLTCBTC",1536634154883,1536634156838,0.2001,0.2001,"LIMIT",null,null,null,0,"CANCELED",null,null,0.008542,0,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
 //
 //[0,"oc",[16650325630,0,1,"tLTCBTC",1536635555458,1536635555474,0,0.2001,"MARKET",null,null,null,0,"EXECUTED @ 0.008545(0.2001)",null,null,0.008545,0.008545,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
 //[0,"oc",[16650519644,0,1,"tLTCBTC",1536635943168,1536635945166,-1.0104,-1.0104,"LIMIT",null,null,null,0,"CANCELED",null,null,0.008541,0,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
 //[0,"oc",[16651134080,0,4,"tLTCBTC",1536637134189,1536637136167,-1.0001,-1.0001,"LIMIT",null,null,null,0,"CANCELED",null,null,0.0085204,0,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
 //[0,"oc",[16653164135,0,1,"tLTCBTC",1536640990454,1536640995441,-1.0001,-1.0001,"LIMIT",null,null,null,0,"CANCELED",null,null,0.0085397,0,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]
 //[0,"oc",[16653210798,0,2,"tLTCBTC",1536641097028,1536641097058,0,-1.0001,"LIMIT",null,null,null,0,"EXECUTED @ 0.0085406(-0.31225457): was PARTIALLY FILLED @ 0.0085406(-0.68784543)",null,null,0.0085397,0.0085406,null,null,null,null,null,0,0,0,null,null,"API>BFX",null,null,null]]

 //[0,"oc",[17748166239,0,2,"tLTCBTC",1538872907976,1538872907992,0.2,0.2,"EXCHANGE FOK",null,null,null,0,"FILLORKILL CANCELED",null,null,0.00876,0,0,0,null,null,null,0,0,null,null,null,"API>BFX",null,null,null]]

void TDEngineBitfinex::onOrderNewUpdateCancel(struct lws* conn, Document& json)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::onOrderNewUpdateCancel: " << parseJsonToString(json));
	AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);

	if (json.GetArray()[2].IsArray()) {
		auto &order_i = json.GetArray()[2];
		onOrder(conn, order_i);
	}
}


void TDEngineBitfinex::onOrder(struct lws* conn, rapidjson::Value& order_i)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::onOrder.");
	AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);

	int64_t remoteOrderId = order_i.GetArray()[0].IsInt64();
	if(order_i.GetArray()[1].IsNumber()){
		int gid = order_i.GetArray()[1].GetInt();
	}
	int cid = order_i.GetArray()[2].GetInt();
	std::string symbol = order_i.GetArray()[3].GetString();
	double remaining_amount = order_i.GetArray()[6].GetDouble();

	double amount_orig = order_i.GetArray()[7].GetDouble();
	std::string type = order_i.GetArray()[8].GetString();
	//TYPE_PREV
	std::string order_status = order_i.GetArray()[13].GetString();

	double price = order_i.GetArray()[16].GetDouble();

	LFRtnOrderField rtn_order;
	memset(&rtn_order, 0, sizeof(LFRtnOrderField));
	strcpy(rtn_order.ExchangeID, "bitfinex");
	strncpy(rtn_order.UserID, unit.api_key.c_str(), 16);

	rtn_order.OrderStatus = GetOrderStatus(order_status);
	if(rtn_order.OrderStatus==LF_CHAR_NotTouched){
		auto it = remoteOrderIdOrderInsertSentTime.find(std::to_string(cid));
		if(it != remoteOrderIdOrderInsertSentTime.end()){
			remoteOrderIdOrderInsertSentTime.erase(it);
		}		
	}
	if(rtn_order.OrderStatus==LF_CHAR_Canceled){
		auto itr = remoteOrderIdOrderActionSentTime.find(std::to_string(cid));
		if(itr != remoteOrderIdOrderActionSentTime.end()){
			remoteOrderIdOrderActionSentTime.erase(itr);
		}		
	}

	std::string ticker = unit.coinPairWhiteList.GetKeyByValue(symbol);
	if (ticker.length() == 0) {
		KF_LOG_INFO(logger, "[onOrder]: not in WhiteList , ignore it:" << symbol);
		return;
	}
	KF_LOG_DEBUG(logger, "[onOrder] (exchange_ticker)" << ticker);
	strncpy(rtn_order.InstrumentID, ticker.c_str(), 31);


	if (remaining_amount >= 0) {
		//ʣ������
		rtn_order.VolumeTotal = std::round(remaining_amount * scale_offset);
		rtn_order.Direction = LF_CHAR_Buy;
	}
	else {
		rtn_order.VolumeTotal = std::round(remaining_amount * scale_offset * -1);
		rtn_order.Direction = LF_CHAR_Sell;
	}

	if (amount_orig > 0) {
		//����
		rtn_order.VolumeTotalOriginal = std::round(amount_orig * scale_offset);
	}
	else {
		rtn_order.VolumeTotalOriginal = std::round(amount_orig * scale_offset * -1);
	}

	//��ɽ�����
	rtn_order.VolumeTraded = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTotal;


	if ("FOK" == type || "EXCHANGE FOK" == type) {
		rtn_order.TimeCondition = LF_CHAR_FOK;
	}
	else {
		rtn_order.TimeCondition = LF_CHAR_GTC;
	}

	rtn_order.OrderPriceType = GetPriceType(type);
	strncpy(rtn_order.OrderRef, std::to_string(cid).c_str(), 13);

	rtn_order.LimitPrice = std::round(price * scale_offset);

	on_rtn_order(&rtn_order);
	raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
		source_id, MSG_TYPE_LF_RTN_ORDER_BITFINEX,
		1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);
}

//n : notification

//[0,"n",[1536136851766,"oc-req",null,null,[null,null,1,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,0,null,null,null,null,null,null,null,null],null,"ERROR","Order not found."]]
//[0,"n",[1536136851810,"on-req",null,null,[0,null,1,"tTRXBTC",null,null,0.00000123,null,"LIMIT",null,null,null,null,null,null,null,1.2e-7,null,null,null,null,null,null,0,null,null,null,null,null,null,null,null],null,"ERROR","This pair cannot be traded on margin."]]

//[0,"n",[1536562548955,"on-req",null,null,[16609409732,null,3,"tLTCBTC",null,null,0.21,0.21,"LIMIT",null,null,null,null,null,null,null,0.008763,null,null,null,null,null,null,0,null,null,null,null,null,null,null,null],null,"SUCCESS","Submitting limit buy order for 0.21 LTC."]]
//[0,"n",[1536562255341,"oc-req",null,null,[null,null,2,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,0,null,null,null,null,null,null,null,null],null,"SUCCESS","Submitted for cancellation; waiting for confirmation (ID: 16609272883)."]]
//no this order
// [0,"n",[1536571802054,"oc-req",null,null,[22948376339485,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,0,null,null,null,null,null,null,null,null],null,"ERROR","Order not found."]]
//has this order
//[0,"n",[1536575651298,"oc-req",null,null,[16616840342,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,0,null,null,null,null,null,null,null,null],null,"SUCCESS","Submitted for cancellation; waiting for confirmation (ID: 16616840342)."]]

//after reformat:
//[0,"n",[1536136851766,"oc-req",null,null,[],null,"ERROR","Order not found."]]
//[0,"n",[1536136851810,"on-req",null,null,[],null,"ERROR","This pair cannot be traded on margin."]]
//[0,"n",[1536562548955,"on-req",null,null,[],null,"SUCCESS","Submitting limit buy order for 0.21 LTC."]]
//[0,"n",[1536562255341,"oc-req",null,null,[],null,"SUCCESS","Submitted for cancellation; waiting for confirmation (ID: 16609272883)."]]
void TDEngineBitfinex::onNotification(struct lws* conn, Document& json)
{
	KF_LOG_INFO(logger, "TDEngineBitfinex::onNotification: " << parseJsonToString(json));
	AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);
	if (json.GetArray()[2].IsArray()) {
		auto &notify = json.GetArray()[2];
		if (notify.IsArray() && notify.Size() == 8) {
			std::string orderType = notify.GetArray()[1].GetString();
			std::string state = notify.GetArray()[6].GetString();
			std::string stateValue = notify.GetArray()[7].GetString();
			auto &notify_data = notify.GetArray()[4];
			KF_LOG_INFO(logger, "TDEngineBitfinex::onNotification: (orderType)" << orderType << " (state)" << state << " (stateValue)" << stateValue);
			if ("SUCCESS" == state) {
				if ("on-req" == orderType) {
					int cid = notify_data.GetArray()[2].GetInt();

					//                    std::string symbol = "";
					//                    if(notify_data.GetArray()[3].IsString()) {
					//                        symbol = notify_data.GetArray()[3].GetString();
					//                    }
					//
					//                    std::string ticker = unit.coinPairWhiteList.GetKeyByValue(std::string(symbol));
					//                    if(ticker.length() == 0) {
					//                        KF_LOG_ERROR(logger, "[onNotification]: not in WhiteList , ignore it: (symbol)" << symbol << " (cid)" << cid);
					//                        return;
					//                    }
					int64_t remoteOrderId = notify_data.GetArray()[0].GetInt64();

					std::unordered_map<int, OrderInsertData>::iterator itr;
					itr = CIDorderInsertData.find(cid);
					if (itr != CIDorderInsertData.end()) {

						OrderInsertData& cache = itr->second;
						cache.remoteOrderId = remoteOrderId;

						raw_writer->write_error_frame(&cache.data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITFINEX, 1, cache.requestId, 0, stateValue.c_str());
						KF_LOG_INFO(logger, "TDEngineBitfinex::onNotification: (cid) " << cid
							<< " (orderId)" << cache.remoteOrderId <<
							" (orderType)" << orderType <<
							" (state)" << state <<
							" (stateValue)" << stateValue);
					}
					//the pendingOrderActionData wait and got remoteOrderId, then send OrderAction
					std::unordered_map<int, OrderActionData>::iterator orderActionItr;
					orderActionItr = pendingOrderActionData.find(cid);
					if (orderActionItr != pendingOrderActionData.end()) {
						OrderActionData& cache = orderActionItr->second;

						std::string cancelOrderJsonString = createCancelOrderIdJsonString(remoteOrderId);
						addPendingSendMsg(unit, cancelOrderJsonString);
						KF_LOG_DEBUG(logger, "TDEngineBitfinex::onNotification: pending_and_send  [req_order_action] createCancelOrderIdJsonString (remoteOrderId) " << remoteOrderId);
						RemoteOrderIDorderActionData.insert(std::pair<int64_t, OrderActionData>(remoteOrderId, cache));
						//emit e event for websocket callback
						lws_callback_on_writable(unit.websocketConn);
					}
					pendingOrderActionData.erase(cid);
				}

				if ("oc-req" == orderType) {
					//send order action with remoteOrderId, will get this
					if (notify_data.GetArray()[0].IsInt64()) {
						int64_t remoteOrderId = notify_data.GetArray()[0].GetInt64();

						std::unordered_map<int64_t, OrderActionData>::iterator itr;
						itr = RemoteOrderIDorderActionData.find(remoteOrderId);
						if (itr != RemoteOrderIDorderActionData.end()) {
							OrderActionData cache = itr->second;
							raw_writer->write_error_frame(&cache.data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFINEX, 1, cache.requestId, 0, stateValue.c_str());
						}
					}
					else if (notify_data.GetArray()[2].IsInt()) {
						//send order action with cid+dateStr, will get this
						int cid = notify_data.GetArray()[2].GetInt();

						std::unordered_map<int, OrderActionData>::iterator itr;
						itr = CIDorderActionData.find(cid);
						if (itr != CIDorderActionData.end()) {
							OrderActionData cache = itr->second;
							raw_writer->write_error_frame(&cache.data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFINEX, 1, cache.requestId, 0, stateValue.c_str());
						}
					}
				}
			}
			if ("ERROR" == state) {
				if ("on-req" == orderType) {
					int cid = notify_data.GetArray()[2].GetInt();

					//                    std::string symbol = "";
					//                    if(notify_data.GetArray()[3].IsString()) {
					//                        symbol = notify_data.GetArray()[3].GetString();
					//                    }
					//
					//                    std::string ticker = unit.coinPairWhiteList.GetKeyByValue(std::string(symbol));
					//                    if(ticker.length() == 0) {
					//                        KF_LOG_ERROR(logger, "[onNotification]: not in WhiteList , ignore it: (symbol)" << symbol << " (cid)" << cid);
					//                        return;
					//                    }

					std::unordered_map<int, OrderInsertData>::iterator itr;
					itr = CIDorderInsertData.find(cid);
					if (itr != CIDorderInsertData.end()) {
						OrderInsertData cache = itr->second;
						KF_LOG_INFO(logger,
							"TDEngineBitfinex::onNotification: on_rsp_order_insert  (cache.requestId)" << cache.requestId
							<< " (OrderRef)"
							<< cache.data.OrderRef
							<< " (LimitPrice)"
							<< cache.data.LimitPrice
							<< " (Volume)"
							<< cache.data.Volume);
						on_rsp_order_insert(&cache.data, cache.requestId, 100, stateValue.c_str());
						raw_writer->write_error_frame(&cache.data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITFINEX, 1, cache.requestId, 100, stateValue.c_str());
					}
				}
				if ("oc-req" == orderType) {
					//send order action with remoteOrderId, will get this
					if (notify_data.GetArray()[0].IsInt64()) {
						int64_t remoteOrderId = notify_data.GetArray()[0].GetInt64();

						std::unordered_map<int64_t, OrderActionData>::iterator itr;
						itr = RemoteOrderIDorderActionData.find(remoteOrderId);
						if (itr != RemoteOrderIDorderActionData.end()) {
							OrderActionData cache = itr->second;
							KF_LOG_INFO(logger, "TDEngineBitfinex::onNotification: on_rsp_order_action  (cache.requestId)" << cache.requestId <<
								" (OrderRef)" << cache.data.OrderRef <<
								" (LimitPrice)" << cache.data.LimitPrice <<
								" (KfOrderID)" << cache.data.KfOrderID);
							on_rsp_order_action(&cache.data, cache.requestId, 100, stateValue.c_str());
							raw_writer->write_error_frame(&cache.data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFINEX, 1, cache.requestId, 100, stateValue.c_str());
							auto it = remoteOrderIdOrderInsertSentTime.find(std::string(cache.data.OrderRef));
					        if(it != remoteOrderIdOrderInsertSentTime.end()){
					        	remoteOrderIdOrderInsertSentTime.erase(it);
					        }
							auto iter = remoteOrderIdOrderActionSentTime.find(std::string(cache.data.OrderRef));
					        if(iter != remoteOrderIdOrderActionSentTime.end()){
					        	remoteOrderIdOrderActionSentTime.erase(iter);
					        }        
						}
					}
					else if (notify_data.GetArray()[2].IsInt()) {
						//send order action with cid+dateStr, will get this
						int cid = notify_data.GetArray()[2].GetInt();
						std::unordered_map<int, OrderActionData>::iterator itr;
						itr = CIDorderActionData.find(cid);
						if (itr != CIDorderActionData.end()) {
							OrderActionData cache = itr->second;
							KF_LOG_INFO(logger, "TDEngineBitfinex::onNotification: on_rsp_order_action  (cache.requestId)" << cache.requestId <<
								" (OrderRef)" << cache.data.OrderRef <<
								" (LimitPrice)" << cache.data.LimitPrice <<
								" (KfOrderID)" << cache.data.KfOrderID);
							on_rsp_order_action(&cache.data, cache.requestId, 100, stateValue.c_str());
							raw_writer->write_error_frame(&cache.data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFINEX, 1, cache.requestId, 100, stateValue.c_str());
							auto it = remoteOrderIdOrderInsertSentTime.find(std::string(cache.data.OrderRef));
					        if(it != remoteOrderIdOrderInsertSentTime.end()){
					        	remoteOrderIdOrderInsertSentTime.erase(it);
					        }
							auto iter = remoteOrderIdOrderActionSentTime.find(std::string(cache.data.OrderRef));
					        if(iter != remoteOrderIdOrderActionSentTime.end()){
					        	remoteOrderIdOrderActionSentTime.erase(iter);
					        }
						}
					}
				}
			}
		}
	}
}


std::string TDEngineBitfinex::parseJsonToString(Document &d)
{
	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	d.Accept(writer);

	return buffer.GetString();
}

void TDEngineBitfinex::on_lws_connection_error(struct lws* conn)
{
	KF_LOG_ERROR(logger, "TDEngineBitfinex::on_lws_connection_error.");
	//market logged_in false;
	AccountUnitBitfinex& unit = findAccountUnitBitfinexByWebsocketConn(conn);
	unit.logged_in = false;
	unit.is_connecting = false;
	KF_LOG_ERROR(logger, "TDEngineBitfinex::on_lws_connection_error. login again.");

	long timeout_nsec = 0;
	unit.newPendingSendMsg.push_back(createAuthJsonString(unit));
	lws_login(unit, timeout_nsec);
}

AccountUnitBitfinex& TDEngineBitfinex::findAccountUnitBitfinexByWebsocketConn(struct lws * websocketConn)
{
	for (size_t idx = 0; idx < account_units.size(); idx++) {
		AccountUnitBitfinex &unit = account_units[idx];
		if (unit.websocketConn == websocketConn) {
			return unit;
		}
	}
	return account_units[0];
}

/**
 * req functions
 */
void TDEngineBitfinex::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
	KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

	AccountUnitBitfinex& unit = account_units[account_index];
	KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key << " (InstrumentID) " << data->InstrumentID);

	send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_BITFINEX, 1, requestId);
	int errorId = 0;
	std::string errorMsg = "";

	LFRspPositionField pos;
	memset(&pos, 0, sizeof(LFRspPositionField));
	strncpy(pos.BrokerID, data->BrokerID, 32);
	strncpy(pos.InvestorID, data->InvestorID, 32);
	strncpy(pos.InstrumentID, data->InstrumentID, 31);
	pos.PosiDirection = LF_CHAR_Long;
	pos.HedgeFlag = LF_CHAR_Speculation;
	pos.Position = 0;
	pos.YdPosition = 0;
	pos.PositionCost = 0;
	//lyw add
    KF_LOG_INFO(logger, "[req_investor_position] (data->BrokerID)" << data->BrokerID);    
    Document d;
    cpr::Response r;
    if( !strcmp(data->BrokerID, "margin"))
    {
    	r = get_margin_position(unit);
    	KF_LOG_INFO(logger, "[req_investor_position] get_margin_position");
    }
    else
    {
    	r = get_position(unit);
    	KF_LOG_INFO(logger, "[req_investor_position] get_position");
    }
    d.Parse(r.text.c_str());
    if(d.HasParseError() )
    {
        errorId=100;
        if( !strcmp(data->BrokerID, "margin"))
        {
        	errorMsg= "get_position http response has parse error. please check the log";
            KF_LOG_ERROR(logger, "[req_investor_position] get_position error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
        else
        {
        	errorMsg= "get_margin_position http response has parse error. please check the log";
            KF_LOG_ERROR(logger, "[req_investor_position] get_margin_position error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
        }
    }
    if (r.status_code >= 200 && r.status_code <= 299)
    {
		KF_LOG_DEBUG(logger, "[req_investor_position] rest interface get_position successed");
		KF_LOG_INFO(logger, "[req_investor_position] " << " (response.status_code) " << r.status_code <<
			"\n (response.text) " << r.text.c_str());
		
	}
	else
	{
		KF_LOG_DEBUG(logger, "[req_investor_position] rest interface get_position failed");
		KF_LOG_INFO(logger, "[req_investor_position] " << " (response.status_code) " << r.status_code <<
			" (response.text) " << r.text.c_str());
		return ;
	}

	send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_BITFINEX, 1, requestId);

    std::vector<LFRspPositionField> tmp_vector;

    if(!d.HasParseError() && d.IsObject() )
    {
        int len = d.Size();
        for ( int i  = 0 ; i < len ; i++ ) {
        	if(!strcmp(data->BrokerID, "margin"))
        	{
        		std::string symbol = d.GetArray()[i]["currency"].GetString();
           	 	std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
            	if(ticker.length() > 0) {
                	strncpy(pos.InstrumentID, ticker.c_str(), 31);
                	pos.Position = std::round(stod(d.GetArray()[i]["amount"].GetString()) * scale_offset);
                	tmp_vector.push_back(pos);
                	KF_LOG_INFO(logger, "[req_investor_position] (type) "<< d.GetArray()[i]["type"].GetString() <<" (currency)" << symbol << " (amount)" 
                	                	<<  d.GetArray()[i]["amount"].GetString() << " (available)" << d.GetArray()[i]["available"].GetString());
                	KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << "(type) "<< d.GetArray()[i]["type"].GetString() 
                	                	<<" (currency) " << symbol << " (position) " << pos.Position);
            	}
        	}
        	else
        	{
        	    if(d.GetArray()[i].HasMember("margin_limits"))
        	    {
        	    	if(d.GetArray()[i].HasMember("on_pair"))
        	    	{
        	    		std::string symbol = d.GetArray()[i]["on_pair"].GetString();
        	    		std::string ticker = unit.positionWhiteList.GetKeyByValue(symbol);
            	        if(ticker.length() > 0) {
            	        	strncpy(pos.InstrumentID, ticker.c_str(), 31);
                	    	pos.Position = std::round(stod(d.GetArray()[i]["margin_balance"].GetString()) * scale_offset);
                	    	tmp_vector.push_back(pos);
                	    	KF_LOG_INFO(logger, "[req_investor_position] (type) "<< d.GetArray()[i]["type"].GetString() <<" (currency)" << symbol << " (amount)" 
                	                	    	<<  d.GetArray()[i]["amount"].GetString() << " (available)" << d.GetArray()[i]["available"].GetString());
                	    	KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId << " (currency)" << symbol << " (position) " << pos.Position);
        	    	    }
        	    	}
        	    }
        	}
        }
    }
	//lyw add ends
	bool findSymbolInResult = false;
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
	}

	if (errorId != 0)
	{
		raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_BITFINEX, 1, requestId, errorId, errorMsg.c_str());
	}
}

void TDEngineBitfinex::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
	KF_LOG_INFO(logger, "[req_qry_account]");
}


void TDEngineBitfinex::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
	AccountUnitBitfinex& unit = account_units[account_index];
	KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
		<< " (APIKey)" << unit.api_key
		<< " (Tid)" << data->InstrumentID
		<< " (Volume)" << data->Volume
		<< " (LimitPrice)" << data->LimitPrice
		<< " (OrderRef)" << data->OrderRef);
	send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITFINEX, 1/*ISLAST*/, requestId);

	int errorId = 0;
	std::string errorMsg = "";

	std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
	if (ticker.length() == 0) {
		errorId = 200;
		errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
		KF_LOG_ERROR(logger, "[req_order_insert]: not in WhiteList, ignore it  (rid)" << requestId <<
			" (errorId)" << errorId << " (errorMsg) " << errorMsg);
		on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_BITFINEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);
	on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
	//Price (Not required for market orders)
	double price = data->LimitPrice*1.0 / scale_offset;

	double size = data->Volume*1.0 / scale_offset;
	//amount	decimal string	Positive for buy, Negative for sell
	if (LF_CHAR_Sell == data->Direction) {
		size = size * -1;
	}

	std::string priceStr;
	std::stringstream convertPriceStream;
	convertPriceStream << std::fixed << std::setprecision(8) << price;
	convertPriceStream >> priceStr;

	std::string sizeStr;
	std::stringstream convertSizeStream;
	convertSizeStream << std::fixed << std::setprecision(8) << size;
	convertSizeStream >> sizeStr;

	//https://docs.bitfinex.com/v2/reference#ws-input-order-new
	//type	string	MARKET, EXCHANGE MARKET, LIMIT, EXCHANGE LIMIT, STOP, EXCHANGE STOP, TRAILING STOP, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK, STOP LIMIT, EXCHANGE STOP LIMIT
	std::string type = "";
	if (data->VolumeCondition == LF_CHAR_CV) {
		//WCStrategyUtil.cpp
		//insert_fok_order: order.VolumeCondition = LF_CHAR_CV;
		//������ϻ�û�������л���hardcodeֻ֧��EXCHANGE ���͵Ľ���
		type = "EXCHANGE FOK";
	}
	else {
		//WCStrategyUtil.cpp
		//insert_fak_order/insert_limit_order/insert_market_order:  order.VolumeCondition = LF_CHAR_AV;
		type = GetType(data->OrderPriceType);
	}


	int cid = atoi(data->OrderRef);
	std::string dateStr = getDateStr();

	KF_LOG_INFO(logger, "[send_order] (ticker) " << ticker << " (type) " <<
		type << " (size) " << sizeStr << " (price) " << priceStr
		<< " (cid) " << cid << " (dateStr) " << dateStr);
    int flags=0;
    if(is_post_only(data))
        flags=4096;
    //flags=4096;
	std::string insertOrderJsonString = createInsertOrderJsonString(0, cid, type, ticker, sizeStr, priceStr,flags);
	
	if(!unit.is_connecting){
		errorId = 203;
		errorMsg = "websocket is not connecting,please try again later";
		on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
		return;
	}else{
		addRemoteOrderIdOrderInsertSentTime(data, requestId, std::string(data->OrderRef));
		addPendingSendMsg(unit, insertOrderJsonString);
	}
	//emit e event for websocket callback
	lws_callback_on_writable(unit.websocketConn);

	OrderInsertData cache;
	cache.unit = unit;
	cache.requestId = requestId;
	cache.remoteOrderId = 0;
	cache.dateStr = dateStr;
	memcpy(&cache.data, data, sizeof(LFInputOrderField));
	CIDorderInsertData.insert(std::pair<int, OrderInsertData>(cid, cache));
}

void TDEngineBitfinex::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
	AccountUnitBitfinex& unit = account_units[account_index];
	KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
		<< " (APIKey)" << unit.api_key
		<< " (Iid)" << data->InvestorID
		<< " (OrderRef)" << data->OrderRef
		<< " (KfOrderID)" << data->KfOrderID);

	send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFINEX, 1, requestId);

	int errorId = 0;
	std::string errorMsg = "";

	std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
	if (ticker.length() == 0) {
		errorId = 200;
		errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
		KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
			errorId << " (errorMsg) " << errorMsg);
		on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFINEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);

	OrderInsertData insertData = findOrderInsertDataByOrderRef(data->OrderRef);
	if (insertData.requestId == 0) {
		//not find
		errorId = 1;
		std::stringstream ss;
		ss << "[req_order_action] not found in OrderRefAndDateStr map (orderRef) " << data->OrderRef;
		errorMsg = ss.str();
		KF_LOG_ERROR(logger, "[req_order_action] not found in OrderRefAndDateStr map. "
			<< " (rid)" << requestId << " (orderRef)" << data->OrderRef << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
		on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFINEX, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	else {
		KF_LOG_DEBUG(logger, "[req_order_action] found in OrderRefAndDateStr map (requestId) "
			<< insertData.requestId << " (remoteOrderId) " << insertData.remoteOrderId
			<< " (dateStr) " << insertData.dateStr << " (Volume) " << insertData.data.Volume);
	}

	int cid = atoi(data->OrderRef);
	if (insertData.remoteOrderId > 0) {
		//use remote order id first
		std::string cancelOrderJsonString = createCancelOrderIdJsonString(insertData.remoteOrderId);

		//addRemoteOrderIdOrderActionSentTime(data, requestId, std::string(data->OrderRef));
		if(!unit.is_connecting){
			errorId = 203;
			errorMsg = "websocket is not connecting,please try again later";
			on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
			return;
		}else{
			addRemoteOrderIdOrderActionSentTime(data, requestId, std::string(data->OrderRef));
			addPendingSendMsg(unit, cancelOrderJsonString);
		}
		KF_LOG_DEBUG(logger, "[req_order_action] createCancelOrderIdJsonString (remoteOrderId) " << insertData.remoteOrderId);
		OrderActionData cache;
		cache.requestId = requestId;
		memcpy(&cache.data, data, sizeof(LFOrderActionField));
		RemoteOrderIDorderActionData.insert(std::pair<int64_t, OrderActionData>(insertData.remoteOrderId, cache));
	}
	else {
		/*
		//remote order id ����on-req��Ϣ�����ȡ�ģ� ��������󣬻�û�յ�o-req�ͳ�������ֻ��ʹ��cid+dateStr��
		std::string cancelOrderJsonString = createCancelOrderCIdJsonString(cid, insertData.dateStr);
		addPendingSendMsg(unit, cancelOrderJsonString);
		KF_LOG_DEBUG(logger, "[req_order_action] createCancelOrderIdJsonString (cid) " << cid << " (dateStr)" << insertData.dateStr);
		*/
		OrderActionData cache;
		cache.requestId = requestId;
		memcpy(&cache.data, data, sizeof(LFOrderActionField));
		CIDorderActionData.insert(std::pair<int, OrderActionData>(cid, cache));
		//if not remoteOrderId, should wait for remoteid then send orderaction
		pendingOrderActionData.insert(std::pair<int, OrderActionData>(cid, cache));

	}

	//emit e event for websocket callback
	lws_callback_on_writable(unit.websocketConn);
}
void TDEngineBitfinex::req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId)
{
	AccountUnitBitfinex& unit = account_units[account_index];
	KF_LOG_DEBUG(logger, "[req_withdraw_currency]" << " (rid) " << requestId
                                              << " (APIKey) " << unit.api_key
                                              << " (withdraw_type) " << data->Currency
                                              << " (walletselected) " << " exchange"
                                              << " (Volume) " << data->Volume
                                              << " (Address) " << data->Address);

	send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_WITHDRAW_BITFINEX, 1, requestId);

	int errorId = 0;
	std::string errorMsg = "";
	string address = data->Address;
	if(address == ""){
        errorId = 100;
        errorMsg = "address is null";
        KF_LOG_ERROR(logger,"[req_withdraw_currency] address is null");
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_BITFINEX, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }
    Document json;
    cpr::Response r;
    r = withdrawl_currency(data->Currency,"exchange",std::to_string(((double)(data->Volume)/scale_offset)),address,unit);
    json.Parse(r.text.c_str());
    if (json.HasParseError() || !json.IsObject()){
        errorId = 101;
        errorMsg = "json has parse error.";
        KF_LOG_ERROR(logger,"[withdrawl_currency] json has parse error.");
        on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_BITFINEX, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }
	if (r.status_code >= 200 && r.status_code <= 299)
    {
		if(json.HasMember("status") && json["status"].GetString() == "success"){
            errorId = 0;
            double id = json["withdrawal_id"].GetDouble();
            string message = json["message"].GetString();
            KF_LOG_INFO(logger, "[withdrawl_currency] (message) " << message);
            KF_LOG_INFO(logger, "[withdrawl_currency] (withdrawal_id) " << id);
            KF_LOG_INFO(logger, "[withdrawl_currency] withdrawl success. no error message");
            on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
        }else if(json.HasMember("status") && json["status"].GetString() != "success"){
        
            string message = json["message"].GetString();
            KF_LOG_INFO(logger, "[withdrawl_currency] (msg) " << message);
            KF_LOG_INFO(logger, "[withdrawl_currency] withdrawl faild!");
            errorId = r.status_code;
            errorMsg = message;
            on_rsp_withdraw(data,requestId,errorId,errorMsg.c_str());
            raw_writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW_BITFINEX, 1, 
                requestId, errorId, errorMsg.c_str());
        }
	}
	else
	{
		KF_LOG_DEBUG(logger, "[withdrawl_currency] withdrawl_currency failed");
		KF_LOG_INFO(logger, "[withdrawl_currency] " << " (response.status_code) " << r.status_code <<
			" (response.text) " << r.text.c_str());
		return ;
	}
}

cpr::Response TDEngineBitfinex::withdrawl_currency(string withdraw_type, string walletselected, string amount, string address,  AccountUnitBitfinex& unit)
{
	std::string Timestamp = std::to_string(getTimestamp());
	std::string requestPath = "/v1/withdraw";
	std::string queryString = "";
	std::string body = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\"" + "}";
	string Message = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\",\"withdraw_type\":\"" + withdraw_type 
	                 + "\",\"walletselected\":\"" + walletselected + "\",\"amount\":\"" + amount + "\",\"address\":\"" + address + "\"" + "}";

	string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
	std::string sig = hmac_sha384(unit.secret_key.c_str(), payload.c_str());
	string url = unit.baseUrl + requestPath + queryString;//complete url
	KF_LOG_DEBUG(logger, "[withdrawl_currency]the url we ask" << url);
	const auto response = Post(
		Url{ url }, cpr::VerifySsl{ false },
		Header{ {"X-BFX-APIKEY", unit.api_key},
		{"Content-Type", "application/json"},
		{"X-BFX-PAYLOAD", payload},
		{"X-BFX-SIGNATURE",  sig} },
		Body{ body },
		Timeout{ 10000 }
	);

	return response;
}

void TDEngineBitfinex::req_inner_transfer(const LFTransferField* data, int account_index, int requestId)
{
	AccountUnitBitfinex& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_inner_transfer]" << " (rid) " << requestId
                                              << " (APIKey) " << unit.api_key
                                              << " (From) " << data->From
                                              << " (Currency) " << data->Currency
                                              << " (Volume) " << data->Volume 
                                              << " (To) " << data->To);
    send_writer->write_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_BITFINEX, 1, requestId);
    int errorId = 0;
    std::string errorMsg = "";
    Document json;
    std::string type = "";
    cpr::Response response;
    std::string Currency = unit.positionWhiteList.GetValueByKey(std::string(data->Currency));
    if(Currency.length() == 0) {
        errorId = 200;
        errorMsg = "Currency not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_inner_transfer]: Currency not in WhiteList , ignore it. (rid)" << requestId << " (errorId)" <<
                                                                                      errorId << " (errorMsg) " << errorMsg);
        on_rsp_transfer(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_BITFINEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }

    response = inner_transfer(std::to_string(((double)(data->Volume)/scale_offset)), Currency,data->From,data->To,unit);
    json.Parse(response.text.c_str());
    if (json.HasParseError() || !json.IsObject()){
        errorId = response.status_code;
        errorMsg = "json has parse error.";
        KF_LOG_ERROR(logger,"[inner_transfer] json has parse error.");
        on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_BITFINEX, 1, 
            requestId, errorId, errorMsg.c_str());
        return;
    }

    if (response.status_code >= 200 && response.status_code <= 299)
    {
		if(json.HasMember("status") && json["status"].GetString() == "success"){
            errorId = 0;
            string message = json["message"].GetString();
            KF_LOG_INFO(logger, "[inner_transfer] (message) " << message);
            KF_LOG_INFO(logger, "[inner_transfer] inner_transfer success. no error message");
            on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
        }else if(json.HasMember("status") && json["status"].GetString() != "success"){
        
            string message = json["message"].GetString();
            KF_LOG_INFO(logger, "[inner_transfer] (msg) " << message);
            KF_LOG_INFO(logger, "[inner_transfer] inner_transfer faild!");
            errorId = response.status_code;
            errorMsg = message;
            on_rsp_transfer(data,requestId,errorId,errorMsg.c_str());
            raw_writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER_BITFINEX, 1, 
                requestId, errorId, errorMsg.c_str());
        }
	}
	else
	{
		KF_LOG_DEBUG(logger, "[inner_transfer] inner_transfer failed");
		KF_LOG_INFO(logger, "[inner_transfer] " << " (response.status_code) " << response.status_code <<
			" (response.text) " << response.text.c_str());
		return ;
	}

}

cpr::Response TDEngineBitfinex::inner_transfer(string amount, string currency, string walletfrom, string walletto, AccountUnitBitfinex& unit)
{
	std::string Timestamp = std::to_string(getTimestamp());
	std::string requestPath = "/v1/transfer";
	std::string queryString = "";
	std::string body = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\"" + "}";
	string Message = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\",\"amount\":\"" + amount 
	                 + "\",\"currency\":\"" + currency + "\",\"walletfrom\":\"" + walletfrom + "\",\"walletto\":\"" + walletto + "\"" + "}";

	string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
	std::string sig = hmac_sha384(unit.secret_key.c_str(), payload.c_str());
	string url = unit.baseUrl + requestPath + queryString;//complete url
	KF_LOG_DEBUG(logger, "[inner_transfer]the url we ask" << url);
	const auto response = Post(
		Url{ url }, cpr::VerifySsl{ false },
		Header{ {"X-BFX-APIKEY", unit.api_key},
		{"Content-Type", "application/json"},
		{"X-BFX-PAYLOAD", payload},
		{"X-BFX-SIGNATURE",  sig} },
		Body{ body },
		Timeout{ 10000 }
	);

	return response;
}

void TDEngineBitfinex::req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool flag)
{
	KF_LOG_INFO(logger, "[req_transfer_history]");
    AccountUnitBitfinex& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_transfer_history] (api_key)" << unit.api_key);

    LFTransferHistoryField his;
    memset(&his, 0, sizeof(LFTransferHistoryField));
    strncpy(his.UserID, data->UserID, 64);
    strncpy(his.ExchangeID, data->ExchangeID, 11);

    int errorId = 0;
    std::string errorMsg = "";
    cpr::Response r;
    KF_LOG_INFO(logger, "[req_transfer_history] (data->UserID)" << data->UserID);
    
    string asset = data->Currency;
    char startTime[32] ;
    strncpy(startTime, data->StartTime, strlen(data->StartTime)-3);
    char endTime[32];
    strncpy(endTime, data->EndTime, strlen(data->EndTime)-3);
    Document d;
    r = get_transfer_history(unit, d, asset, startTime, endTime);
    KF_LOG_INFO(logger, "[req_transfer_history] req_transfer_history");
    //printResponse(d);

    if(d.HasParseError() )
    {
        errorId=100;
        errorMsg= "req_transfer_history http response has parse error. please check the log";
        KF_LOG_ERROR(logger, "[req_transfer_history] req_transfer_history error! (rid)  -1 (errorId)" << errorId << " (errorMsg) " << errorMsg);
    }
    
    send_writer->write_frame(data, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_BITFINEX, 1, requestId);
    

    std::vector<LFTransferHistoryField> tmp_vector;
    if (r.status_code >= 200 && r.status_code <= 299)
    {  
        int len = d.Size();
        for ( int i  = 0 ; i < len ; i++ ) {
        	if(d.HasMember("type"))
        	{
        		std::string type = d.GetArray()[i]["type"].GetString();
        		if(type == "WITHDRAWAL" && flag)
        		{
        			char Currency[32];
          	   	 	strncpy(Currency, d.GetArray()[i]["currency"].GetString(), 32);
          	    	std::string ticker = unit.positionWhiteList.GetKeyByValue(Currency);
         	    	if(ticker.length() > 0) {
          	        	strncpy(his.Currency, Currency, 32 );
          	        	strncpy(his.TimeStamp, (std::to_string(d.GetArray()[i]["timestamp"].GetDouble())).c_str(), 32);
         	       	 	//his.Status = d.GetArray()[i]["status"].GetInt();
         	      	  	his.Volume = std::round(stod(d.GetArray()[i]["amount"].GetString()) * scale_offset);
         	        	strncpy(his.Address, d.GetArray()[i]["address"].GetString(), 130);

         	        	if(d.GetArray()[i].HasMember("addressTag"))
         	        	strncpy(his.Tag, d.GetArray()[i]["addressTag"].GetString(), 64);

         	        	strncpy(his.TxId, std::to_string(d.GetArray()[i]["txid"].GetDouble()).c_str(), 130);
         	        	tmp_vector.push_back(his);
         	        	KF_LOG_INFO(logger,  "[req_withdraw_history] (timestamp (s))" << his.TimeStamp << " (amount)" <<  his.Volume
            	                            	<< " (currency)" << his.Currency<< " (address)" << his.Address <<" ()" 
           	                                	<< " (txid)" << his.TxId <<" (status)" << d.GetArray()[i]["status"].GetString()  
           	                                	<< " (type)" << type <<" (requestId)" << requestId );
                	}
        		}
        		else if(type == "DEPOSIT" && !flag)
        		{
        			char Currency[32];
          	   	 	strncpy(Currency, d.GetArray()[i]["currency"].GetString(), 32);
          	    	std::string ticker = unit.positionWhiteList.GetKeyByValue(Currency);
         	    	if(ticker.length() > 0) {
          	        	strncpy(his.Currency, Currency, 32 );
          	        	strncpy(his.TimeStamp, (std::to_string(d.GetArray()[i]["timestamp"].GetDouble())).c_str(), 32);
         	       	 	//his.Status = d.GetArray()[i]["status"].GetInt();
         	      	  	his.Volume = std::round(stod(d.GetArray()[i]["amount"].GetString()) * scale_offset);
         	        	strncpy(his.Address, d.GetArray()[i]["address"].GetString(), 130);

         	        	if(d.GetArray()[i].HasMember("addressTag"))
         	        	strncpy(his.Tag, d.GetArray()[i]["addressTag"].GetString(), 64);

         	        	strncpy(his.TxId, (std::to_string(d.GetArray()[i]["txid"].GetDouble())).c_str(), 130);
         	        	tmp_vector.push_back(his);
         	        	KF_LOG_INFO(logger,  "[req_deposit_history] (timestamp (s))" << his.TimeStamp << " (amount)" <<  his.Volume
            	                            	<< " (currency)" << his.Currency<< " (address)" << his.Address <<" ()" 
           	                                	<< " (txid)" << his.TxId <<" (status)" << d.GetArray()[i]["status"].GetString()  
           	                                	<< " (type)" << type <<" (requestId)" << requestId );
                	}
        		}
        	}
        }
	}
	else
	{
		errorId = r.status_code;
		errorMsg = r.error.message;
		KF_LOG_DEBUG(logger, "[inner_transfer] inner_transfer failed");
		KF_LOG_INFO(logger, "[inner_transfer] " << " (response.status_code) " << r.status_code <<
			"(response.error.message) "<< r.error.message<< " (response.text) " << r.text.c_str());
	}
    bool findSymbolInResult = false;
    
    int history_count = tmp_vector.size();
    for (int i = 0; i < history_count; i++)
    {
    	on_rsp_transfer_history(&tmp_vector[i], flag, i == (history_count - 1), requestId,errorId, errorMsg.c_str());
        findSymbolInResult = true;
    }

    if(!findSymbolInResult)
    {
    	on_rsp_transfer_history(&his, flag, 1, requestId,errorId, errorMsg.c_str());
    }
    if(errorId != 0)
    {
      raw_writer->write_error_frame(&his, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY_BITFINEX, 1, requestId, errorId, errorMsg.c_str());
    }
}

cpr::Response TDEngineBitfinex::get_transfer_history(AccountUnitBitfinex& unit, Document &json, string currency , string startTime , string endTime)
{
	std::string Timestamp = std::to_string(getTimestamp());
	std::string requestPath = "/v1/history/movements";
	std::string queryString = "";
	std::string body = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\"" + "}";
	string Message = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\",\"currency\":\"" + currency + "\"";
	if(startTime != "")
	{
		Message += "\",\"since\":\"" + startTime + "\"";
	}
	if(endTime != "")
	{
		Message += "\",\"until\":\"" + endTime + "\"";
	}
	Message += "}";

	string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
	std::string sig = hmac_sha384(unit.secret_key.c_str(), payload.c_str());
	string url = unit.baseUrl + requestPath + queryString;//complete url
	KF_LOG_DEBUG(logger, "[get_transfer_history]the url we ask" << url);
	const auto response = Post(
		Url{ url }, cpr::VerifySsl{ false },
		Header{ {"X-BFX-APIKEY", unit.api_key},
		{"Content-Type", "application/json"},
		{"X-BFX-PAYLOAD", payload},
		{"X-BFX-SIGNATURE",  sig} },
		Body{ body },
		Timeout{ 10000 }
	);

	return response;
}

cpr::Response TDEngineBitfinex::get_position(AccountUnitBitfinex& unit)
{
	std::string Timestamp = std::to_string(getTimestamp());
	std::string requestPath = "/v1/balances";
	std::string queryString = "";
	std::string body = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\"" + "}";
	string Message = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\"" + "}";

	string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
	std::string sig = hmac_sha384(unit.secret_key.c_str(), payload.c_str());
	string url = unit.baseUrl + requestPath + queryString;//complete url
	KF_LOG_DEBUG(logger, "[get_position]the url we ask" << url);
	const auto response = Post(
		Url{ url }, cpr::VerifySsl{ false },
		Header{ {"X-BFX-APIKEY", unit.api_key},
		{"Content-Type", "application/json"},
		{"X-BFX-PAYLOAD", payload},
		{"X-BFX-SIGNATURE",  sig} },
		Body{ body },
		Timeout{ 10000 }
	);
	return response;
}

cpr::Response TDEngineBitfinex::get_margin_position(AccountUnitBitfinex& unit)
{
	std::string Timestamp = std::to_string(getTimestamp());
	std::string requestPath = "/v1/margin_infos";
	std::string queryString = "";
	std::string body = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\"" + "}";
	string Message = "{\"request\":\"" + requestPath + "\",\"nonce\":\"" + Timestamp + "\"" + "}";

	string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());
	std::string sig = hmac_sha384(unit.secret_key.c_str(), payload.c_str());
	string url = unit.baseUrl + requestPath + queryString;//complete url
	KF_LOG_DEBUG(logger, "[get_margin_position]the url we ask" << url);
	const auto response = Post(
		Url{ url }, cpr::VerifySsl{ false },
		Header{ {"X-BFX-APIKEY", unit.api_key},
		{"Content-Type", "application/json"},
		{"X-BFX-PAYLOAD", payload},
		{"X-BFX-SIGNATURE",  sig} },
		Body{ body },
		Timeout{ 10000 }
	);
	return response;
}
OrderInsertData TDEngineBitfinex::findOrderInsertDataByOrderId(int64_t orderId)
{
	std::unordered_map<int, OrderInsertData>::iterator itr;
	for (itr = CIDorderInsertData.begin(); itr != CIDorderInsertData.end(); ++itr)
	{
		KF_LOG_DEBUG(logger, "[findOrderInsertDataByOrderId] (requestId)" << itr->second.requestId <<
			" (remoteOrderId)" << itr->second.remoteOrderId <<
			" (dateStr)" << itr->second.dateStr << " (OrderRef) " <<
			itr->second.data.OrderRef << " (LimitPrice)" <<
			itr->second.data.LimitPrice << " (Volume)" << itr->second.data.Volume);
		if (itr->second.remoteOrderId == orderId) {
			return itr->second;
		}
	}
	OrderInsertData empty;
	empty.requestId = 0;
	return empty;
}

OrderInsertData TDEngineBitfinex::findOrderInsertDataByOrderRef(const char_21 orderRef)
{
	std::unordered_map<int, OrderInsertData>::iterator itr;
	for (itr = CIDorderInsertData.begin(); itr != CIDorderInsertData.end(); ++itr)
	{
		KF_LOG_DEBUG(logger, "[findOrderInsertDataByOrderRef] (requestId)" << itr->second.requestId <<
			" (remoteOrderId)" << itr->second.remoteOrderId <<
			" (dateStr)" << itr->second.dateStr << " (OrderRef) " <<
			itr->second.data.OrderRef << " (LimitPrice)" <<
			itr->second.data.LimitPrice << " (Volume)" << itr->second.data.Volume);
		if (strcmp(itr->second.data.OrderRef, orderRef) == 0) {
			return itr->second;
		}
	}
	OrderInsertData empty;
	empty.requestId = 0;
	return empty;
}


void TDEngineBitfinex::addPendingSendMsg(AccountUnitBitfinex& unit, std::string msg)
{
	std::lock_guard<std::mutex> guard_mutex(*mutex_order_and_trade);
	unit.newPendingSendMsg.push_back(msg);
}


void TDEngineBitfinex::moveNewtoPending(AccountUnitBitfinex& unit)
{
	std::lock_guard<std::mutex> guard_mutex(*mutex_order_and_trade);

	std::vector<std::string>::iterator newMsgIterator;
	for (newMsgIterator = unit.newPendingSendMsg.begin(); newMsgIterator != unit.newPendingSendMsg.end();)
	{
		unit.pendingSendMsg.push_back(*newMsgIterator);
		newMsgIterator = unit.newPendingSendMsg.erase(newMsgIterator);
	}
}


void TDEngineBitfinex::set_reader_thread()
{
	ITDEngine::set_reader_thread();

	KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on AccountUnitBitfinex::loop");
	rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBitfinex::loop, this)));

    KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineBitfinex::loopOrderActionNoResponseTimeOut");
    orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBitfinex::loopOrderActionNoResponseTimeOut, this)));

    KF_LOG_INFO(logger, "[set_reader_thread] orderinsert_timeout_thread start on TDEngineBitfinex::loopOrderInsertNoResponseTimeOut");
    orderinsert_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineBitfinex::loopOrderInsertNoResponseTimeOut, this)));
}

void TDEngineBitfinex::loop()
{
	KF_LOG_INFO(logger, "[loop] (isRunning) " << isRunning);
	while (isRunning)
	{
		int n = lws_service(context, rest_get_interval_ms);
		std::cout << " 3.1415 loop() lws_service (n)" << n << std::endl;
	}
}

void TDEngineBitfinex::loopOrderActionNoResponseTimeOut()
{
    KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        orderActionNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
}

void TDEngineBitfinex::loopOrderInsertNoResponseTimeOut()
{
    KF_LOG_INFO(logger, "[loopOrderInsertNoResponseTimeOut] (isRunning) " << isRunning);
    while(isRunning)
    {
        orderInsertNoResponseTimeOut();
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
}

void TDEngineBitfinex::orderActionNoResponseTimeOut(){
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut]");
    int errorId = 100;
    std::string errorMsg = "OrderAction has none response for a long time, please send OrderAction again";

    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    int64_t currentNano = getTimestamp();
    int64_t timeBeforeNano = currentNano - orderaction_max_waiting_seconds * 1000;
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (currentNano)" << currentNano << " (timeBeforeNano)" << timeBeforeNano);
    std::map<std::string, OrderActionSentTime>::iterator itr;
    for(itr = remoteOrderIdOrderActionSentTime.begin(); itr != remoteOrderIdOrderActionSentTime.end();)
    {
        if(itr->second.sentNameTime < timeBeforeNano)
        {
            KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.erase remoteOrderId)" << itr->first );
            OrderInsertData insertData = findOrderInsertDataByOrderRef((itr->first).c_str());
            cpr::Response r;
            r = retriveTradeStatus(insertData.unit, insertData);
            KF_LOG_INFO(logger, "[retriveTradeStatus1] " << " (response.status_code) " << r.status_code <<
                    " (response.text) " << r.text.c_str());
            Document json;
            json.Parse(r.text.c_str());
            if (json.IsArray()&&json.Size()>3){
            	int64_t remoteOrderId = json.GetArray()[0].GetInt64();
            	std::string order_status = json.GetArray()[13].GetString();
            	if(order_status=="CANCELED"){
            		std::unordered_map<int64_t, OrderActionData>::iterator it;
					it = RemoteOrderIDorderActionData.find(remoteOrderId);
					if (it != RemoteOrderIDorderActionData.end()) {
						std::string stateValue = "canceled";
						OrderActionData cache = it->second;
						KF_LOG_INFO(logger, "TDEngineBitfinex::onNotification: on_rsp_order_action  (cache.requestId)" << cache.requestId <<
							" (OrderRef)" << cache.data.OrderRef <<
							" (LimitPrice)" << cache.data.LimitPrice <<
							" (KfOrderID)" << cache.data.KfOrderID);
						on_rsp_order_action(&cache.data, cache.requestId, 100, stateValue.c_str());
						raw_writer->write_error_frame(&cache.data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_BITFINEX, 1, cache.requestId, 100, stateValue.c_str());
					}
					itr = remoteOrderIdOrderActionSentTime.erase(itr);
            	}
            	/*else{
			            on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
			            itr = remoteOrderIdOrderActionSentTime.erase(itr);
            	}*/
            }
            else{
            		on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
			        itr = remoteOrderIdOrderActionSentTime.erase(itr);
            }

            //on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
            //itr = remoteOrderIdOrderActionSentTime.erase(itr);
        } else {
            ++itr;
        }
    }
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}

void TDEngineBitfinex::orderInsertNoResponseTimeOut(){
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut]");
    int errorId = 100;
    std::string errorMsg = "OrderInsert has none response for a long time, please send OrderInsert again";

    std::lock_guard<std::mutex> guard_mutex_order_insert(*mutex_orderinsert_waiting_response);

    int64_t currentNano = getTimestamp();
    int64_t timeBeforeNano = currentNano - orderinsert_max_waiting_seconds * 1000;
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (currentNano)" << currentNano << " (timeBeforeNano)" << timeBeforeNano);
    std::map<std::string, OrderInsertSentTime>::iterator itr;
    for(itr = remoteOrderIdOrderInsertSentTime.begin(); itr != remoteOrderIdOrderInsertSentTime.end();)
    {
        if(itr->second.sentNameTime < timeBeforeNano)
        {
            KF_LOG_DEBUG(logger, "[orderInsertNoResponseTimeOut] (remoteOrderIdOrderInsertSentTime.erase remoteOrderId)" << itr->first );
            OrderInsertData insertData = findOrderInsertDataByOrderRef((itr->first).c_str());
            cpr::Response r;
            r = retriveTradeStatus(insertData.unit, insertData);
            KF_LOG_INFO(logger, "[retriveTradeStatus2] " << " (response.status_code) " << r.status_code <<
                    " (response.text) " << r.text.c_str());
            Document json;
            json.Parse(r.text.c_str());
            if (json.IsArray()&&json.Size()>3){
            	int len = json.Size();
            	for (int i = 0; i < len; i++){
            		int64_t remoteOrderId = json.GetArray()[0].IsInt64();
					int gid = json.GetArray()[1].GetInt();
					int cid = json.GetArray()[2].GetInt();
					std::string symbol = json.GetArray()[3].GetString();
					double remaining_amount = json.GetArray()[6].GetDouble();

					double amount_orig = json.GetArray()[7].GetDouble();
					std::string type = json.GetArray()[8].GetString();
					//TYPE_PREV
					std::string order_status = json.GetArray()[13].GetString();

					double price = json.GetArray()[16].GetDouble();

					LFRtnOrderField rtn_order;
					memset(&rtn_order, 0, sizeof(LFRtnOrderField));
					strcpy(rtn_order.ExchangeID, "bitfinex");
					strncpy(rtn_order.UserID, insertData.unit.api_key.c_str(), 16);

					rtn_order.OrderStatus = GetOrderStatus(order_status);

					std::string ticker = insertData.unit.coinPairWhiteList.GetKeyByValue(symbol);
					if (ticker.length() == 0) {
						KF_LOG_INFO(logger, "[onOrder]: not in WhiteList , ignore it:" << symbol);
						return;
					}
					KF_LOG_DEBUG(logger, "[onOrder] (exchange_ticker)" << ticker);
					strncpy(rtn_order.InstrumentID, ticker.c_str(), 31);


					if (remaining_amount >= 0) {
						//ʣ������
						rtn_order.VolumeTotal = std::round(remaining_amount * scale_offset);
						rtn_order.Direction = LF_CHAR_Buy;
					}
					else {
						rtn_order.VolumeTotal = std::round(remaining_amount * scale_offset * -1);
						rtn_order.Direction = LF_CHAR_Sell;
					}

					if (amount_orig > 0) {
						//����
						rtn_order.VolumeTotalOriginal = std::round(amount_orig * scale_offset);
					}
					else {
						rtn_order.VolumeTotalOriginal = std::round(amount_orig * scale_offset * -1);
					}

					rtn_order.VolumeTraded = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTotal;

					if ("FOK" == type || "EXCHANGE FOK" == type) {
						rtn_order.TimeCondition = LF_CHAR_FOK;
					}
					else {
						rtn_order.TimeCondition = LF_CHAR_GTC;
					}

					rtn_order.OrderPriceType = GetPriceType(type);
					strncpy(rtn_order.OrderRef, std::to_string(cid).c_str(), 13);

					rtn_order.LimitPrice = std::round(price * scale_offset);

					on_rtn_order(&rtn_order);
					raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),
						source_id, MSG_TYPE_LF_RTN_ORDER_BITFINEX,
						1, (rtn_order.RequestID > 0) ? rtn_order.RequestID : -1);
					itr = remoteOrderIdOrderInsertSentTime.erase(itr);
            	}
            }
            else{//[]
	            on_rsp_order_insert(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
	            itr = remoteOrderIdOrderInsertSentTime.erase(itr);
	        }
        } else {
            ++itr;
        }
    }
    //    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}

void TDEngineBitfinex::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId){
    std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

    OrderActionSentTime newOrderActionSent;
    newOrderActionSent.requestId = requestId;
    newOrderActionSent.sentNameTime = getTimestamp();
    memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
    remoteOrderIdOrderActionSentTime[remoteOrderId] = newOrderActionSent;
}

void TDEngineBitfinex::addRemoteOrderIdOrderInsertSentTime(const LFInputOrderField* data, int requestId, const std::string& remoteOrderId){
    std::lock_guard<std::mutex> guard_mutex_order_insert(*mutex_orderinsert_waiting_response);

    OrderInsertSentTime newOrderInsertSent;
    newOrderInsertSent.requestId = requestId;
    newOrderInsertSent.sentNameTime = getTimestamp();
    memcpy(&newOrderInsertSent.data, data, sizeof(LFInputOrderField));
    remoteOrderIdOrderInsertSentTime[remoteOrderId] = newOrderInsertSent;
}

void TDEngineBitfinex::cancel_all_orders(AccountUnitBitfinex& unit, Document& json)
{
	/*
	 // view how to authenticate here:
// https://docs.bitfinex.com/v1/docs/rest-auth

var payload = {
   request: '/v1/order/cancel/all',
   nonce: Date.now().toString(),
}

// or use bitfinex-api-node

const BFX = require('bitfinex-api-node')
const bfxRest = new BFX(apiKey, apiSecretKey, {version: 1}).rest

bfxRest.cancel_all_orders((err, res) => {
  if (err) console.log(err)
  console.log(result)
})


	 * */

	std::string Timestamp = std::to_string(getTimestamp());
	std::string Method = "GET";
	std::string requestPath = "/v1/order/cancel/all";
	std::string queryString = "";
	std::string body = "{\"request\": \"/v1/order/cancel/all\",\"nonce\":\"" + Timestamp + "\"}";
	string Message = body;

	string payload = base64_encode((const unsigned char*)Message.c_str(), Message.length());

	std::string signature = hmac_sha384(unit.secret_key.c_str(), payload.c_str());
	string url = unit.baseUrl + requestPath + queryString;

	const auto response = Get(Url{ url }, cpr::VerifySsl{ false },
		Header{ {"X-BFX-APIKEY", unit.api_key},
			   {"Content-Type", "application/json"},
			   {"X-BFX-PAYLOAD", payload},
			   {"X-BFX-SIGNATURE",  signature} },
		Body{ body }, Timeout{ 10000 });

	KF_LOG_INFO(logger, "[query_order] (url) " << url << " (Message)" << Message << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());


}

inline int64_t TDEngineBitfinex::getTimestamp()
{
	long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return timestamp;
}


std::string TDEngineBitfinex::createAuthJsonString(AccountUnitBitfinex& unit)
{
	std::string authNonce = std::to_string(getTimestamp());
	std::string secret_key = unit.secret_key;
	std::string payload = "AUTH" + authNonce;
	std::string signature = hmac_sha384(secret_key.c_str(), payload.c_str());

	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("event");
	writer.String("auth");

	writer.Key("apiKey");
	writer.String(unit.api_key.c_str());

	writer.Key("authSig");
	writer.String(signature.c_str());

	writer.Key("authPayload");
	writer.String(payload.c_str());

	writer.Key("authNonce");
	writer.String(authNonce.c_str());

	writer.Key("dms");
	writer.Int(4);
	//dms: 4 -> when socket is closed, cancel all account orders
	writer.EndObject();
	return s.GetString();
}

/*
 // Model
[
  0,
  "on",
  null,
  {
	"gid": GID,
	"cid": CID,
	"type": TYPE,
	"symbol": SYMBOL,
	"amount": AMOUNT,
	"price": PRICE,
	...
  }
]

// Example
[
  0,
  "on",
  null,
  {
	"gid": 1,
	"cid": 12345,
	"type": "LIMIT",
	"symbol": "tBTCUSD",
	"amount": "1.0",
	"price": "500"
  }
]
 * */
std::string TDEngineBitfinex::createInsertOrderJsonString(int gid, int cid, std::string type, std::string symbol, std::string amountStr, std::string priceStr,int flags)
{

	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartArray();

	writer.Int(0);
	writer.String("on");
	writer.Null();

	writer.StartObject();
	writer.Key("gid");
	writer.Int(gid);

	writer.Key("cid");
	writer.Int(cid);

	writer.Key("type");
	writer.String(type.c_str());

	writer.Key("symbol");
	writer.String(symbol.c_str());

	writer.Key("amount");
	writer.String(amountStr.c_str());

	writer.Key("price");
	writer.String(priceStr.c_str());

    if(flags!=0)
    {
        writer.Key("flags");
        writer.Int(flags);
    }

	writer.EndObject();
	writer.EndArray();

	return s.GetString();
}

std::string TDEngineBitfinex::getDateStr()
{
	time_t rawtime;
	struct tm * timeinfo;
	char buffer[80];

	time(&rawtime);
	timeinfo = localtime(&rawtime);
	strftime(buffer, 80, "%Y-%m-%d", timeinfo);

	return std::string(buffer);
}

/*
// Cancel order by internal order Id
[
  0,
  "oc",
  null,
  {
	"id": ID
  }
]

// Cancel order using client order Id and order creation date
[
  0,
  "oc",
  null,
  {
	"cid": CID,
	"cid_date": CID_DATE
  }
]

//You can cancel the order by the Internal Order ID or using a Client Order ID (supplied by you).
// The Client Order ID is unique per day, so you also have to provide the date of the order as a date string in this format YYYY-MM-DD.
 * */
std::string TDEngineBitfinex::createCancelOrderIdJsonString(int64_t orderId)
{
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartArray();

	writer.Int(0);
	writer.String("oc");
	writer.Null();

	writer.StartObject();
	writer.Key("id");
	writer.Int64(orderId);

	writer.EndObject();
	writer.EndArray();

	return s.GetString();
}

std::string TDEngineBitfinex::createCancelOrderCIdJsonString(int cid, std::string dateStr)
{
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartArray();

	writer.Int(0);
	writer.String("oc");
	writer.Null();

	writer.StartObject();
	writer.Key("cid");
	writer.Int(cid);

	writer.Key("cid_date");
	writer.String(dateStr.c_str());

	writer.EndObject();
	writer.EndArray();

	return s.GetString();
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

BOOST_PYTHON_MODULE(libbitfinextd)
{
	using namespace boost::python;
	class_<TDEngineBitfinex, boost::shared_ptr<TDEngineBitfinex> >("Engine")
		.def(init<>())
		.def("init", &TDEngineBitfinex::initialize)
		.def("start", &TDEngineBitfinex::start)
		.def("stop", &TDEngineBitfinex::stop)
		.def("logout", &TDEngineBitfinex::logout)
		.def("wait_for_stop", &TDEngineBitfinex::wait_for_stop);
}

