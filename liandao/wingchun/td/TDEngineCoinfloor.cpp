#include "TDEngineCoinfloor.h"
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
#include <mutex>
#include <chrono>
#include "../../utils/crypto/openssl_util.h"

using cpr::Get;
using cpr::Url;
using cpr::Body;
using cpr::Header;
using cpr::Parameters;
using cpr::Payload;
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
using utils::crypto::hmac_sha224;
using utils::crypto::hmac_sha224_byte;
using utils::crypto::base64_encode;
using utils::crypto::base64_decode;
using utils::crypto::b2a_hex;
using utils::crypto::sha224digest;
USING_WC_NAMESPACE

std::mutex mutex_msg_queue;
std::mutex g_httpMutex;
TDEngineCoinfloor::TDEngineCoinfloor() : ITDEngine(SOURCE_COINFLOOR)
{
	logger = yijinjing::KfLog::getLogger("TradeEngine.COINFLOOR");
	KF_LOG_INFO(logger, "[TDEngineCoinfloor]");

	m_mutexOrder = new std::mutex();
	mutex_order_and_trade = new std::mutex();
	mutex_response_order_status = new std::mutex();
	mutex_orderaction_waiting_response = new std::mutex();
}

TDEngineCoinfloor::~TDEngineCoinfloor()
{
	if (m_mutexOrder != nullptr) delete m_mutexOrder;
	if (mutex_order_and_trade != nullptr) delete mutex_order_and_trade;
	if (mutex_response_order_status != nullptr) delete mutex_response_order_status;
	if (mutex_orderaction_waiting_response != nullptr) delete mutex_orderaction_waiting_response;
}

static TDEngineCoinfloor* global_md = nullptr;

static int ws_service_cb(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len)
{
	std::stringstream ss;
	//ss << "lws_callback,reason=" << reason << ",";
	switch (reason)
	{
	case LWS_CALLBACK_CLIENT_ESTABLISHED:
	{
		ss << "LWS_CALLBACK_CLIENT_ESTABLISHED.";
		if (global_md) global_md->writeErrorLog(ss.str());
		lws_callback_on_writable(wsi);
		break;
	}
	case LWS_CALLBACK_PROTOCOL_INIT:
	{
		ss << "LWS_CALLBACK_PROTOCOL_INIT.";
		if (global_md) global_md->writeErrorLog(ss.str());
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
	case LWS_CALLBACK_CLIENT_WRITEABLE:
	{
		ss << "LWS_CALLBACK_CLIENT_WRITEABLE.";

		int ret = 0;
		if (global_md)
		{
			global_md->writeErrorLog(ss.str());
			ret = global_md->lws_write_msg(wsi);
		}
		break;
	}
	case LWS_CALLBACK_CLOSED:
	case LWS_CALLBACK_WSI_DESTROY:
	case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
	{
		ss << "lws_callback,reason=" << reason;

		if (global_md)
		{
			global_md->writeErrorLog(ss.str());
			global_md->on_lws_connection_error(wsi);
		}
		break;
	}
	default:
		//if(global_md) global_md->writeErrorLog(ss.str());
		break;
	}

	return 0;
}

std::string TDEngineCoinfloor::getTimestampStr()
{
	//long long timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return  std::to_string(getMSTime());
}


void TDEngineCoinfloor::onOrderChange(Document& msg)
{
	int zero = 0;
	int two = 2;
	int four = 4;
	string closestr = "OrderClosed";
	if (msg.HasMember("error_code") && msg["error_code"].GetInt() == zero && msg["tag"].GetInt() == two) {
		orderclosed = 0;
		int64_t id = msg["id"].GetInt64();
		strOrderId = std::to_string(id);
		int64_t timestamp = msg["time"].GetInt64();
		std::string timestampstr = std::to_string(timestamp);
		timestampstr = timestampstr.erase(8, 16);
		KF_LOG_DEBUG(logger, "timestampstr1:" << timestampstr);

		auto it = m_mapOrder.find(strOrderId);
		if (it != m_mapOrder.end())
		{
			KF_LOG_INFO(logger, "will canceled");
			it->second.OrderStatus = LF_CHAR_Canceled;
			//撤单回报延时返回
			m_mapCanceledOrder.insert(std::make_pair(strOrderId, getMSTime()));
			//on_rtn_order(&(it->second));

			auto it_id = localOrderRefRemoteOrderId.find(it->second.OrderRef);
			if (it_id != localOrderRefRemoteOrderId.end())
			{
				KF_LOG_INFO(logger, "earse local");
				localOrderRefRemoteOrderId.erase(it_id);
			}
			//m_mapOrder.erase(it);
		}
		//
		auto it2 = m_mapInputOrder.find(strOrderId);
		if (it2 != m_mapInputOrder.end())
		{
			m_mapInputOrder.erase(it2);
		}
		auto it3 = m_mapOrderAction.find(strOrderId);
		if (it3 != m_mapOrderAction.end())
		{
			m_mapOrderAction.erase(it3);
		}
	}
	else if (msg.HasMember("notice") && msg["notice"].GetString() == closestr) {
		orderclosed = 1;
	}
	else if (msg.HasMember("error_code") && msg["error_code"].GetInt() == zero && msg["tag"].GetInt() == four && msg.HasMember("id")) {
		orderclosed = 0;
		int64_t id = msg["id"].GetInt64();
		strOrderId = std::to_string(id);
		int64_t timestamp = msg["time"].GetInt64();
		std::string timestampstr = std::to_string(timestamp);
		timestampstr = timestampstr.erase(8, 16);
		KF_LOG_DEBUG(logger, "timestampstr2:" << timestampstr);
		//std::string strClientId = node["client_id"].GetString();
		//std::lock_guard<std::mutex> lck(*m_mutexOrder); 
		auto it = m_mapNewOrder.find(timestampstr);
		if (it != m_mapNewOrder.end())
		{
			it->second.OrderStatus = LF_CHAR_NotTouched;
			//on_rtn_order(&(it->second));
			strncpy(it->second.BusinessUnit, strOrderId.c_str(), 64);
			m_mapOrder.insert(std::make_pair(strOrderId, it->second));
			m_mapNewOrder.erase(it);
		}

		auto it2 = m_mapInputOrder.find(timestampstr);
		if (it2 != m_mapInputOrder.end())
		{
			auto data = it2->second;
			m_mapInputOrder.erase(it2);
			m_mapInputOrder.insert(std::make_pair(strOrderId, data));

		}
		KF_LOG_DEBUG(logger, "straction=open");
		auto it3 = m_mapOrder.find(strOrderId);
		if (it3 != m_mapOrder.end())
		{
			KF_LOG_DEBUG(logger, "straction=open in");
			localOrderRefRemoteOrderId.insert(std::make_pair(it3->second.OrderRef, strOrderId));
			on_rtn_order(&(it3->second));
		}
	}
	else if (msg.HasMember("error_code") && msg["error_code"].GetInt() == zero && msg["tag"].GetInt() == four && !msg.HasMember("id")) {
		orderclosed = 0;
		KF_LOG_DEBUG(logger, "timestampstr4:" << timestr);
		//std::string strClientId = node["client_id"].GetString();
		//std::lock_guard<std::mutex> lck(*m_mutexOrder); 
		auto it = m_mapNewOrder.find(timestr);
		if (it != m_mapNewOrder.end())
		{
			KF_LOG_INFO(logger, "time4find success");
			it->second.OrderStatus = LF_CHAR_NotTouched;
			//on_rtn_order(&(it->second));
			//strncpy(it->second.BusinessUnit,strOrderId.c_str(),64);
			m_mapOrder.insert(std::make_pair(timestr, it->second));
			m_mapNewOrder.erase(it);
		}
		auto it2 = m_mapInputOrder.find(timestr);
		if (it2 != m_mapInputOrder.end())
		{
			auto data = it2->second;
			m_mapInputOrder.erase(it2);
			m_mapInputOrder.insert(std::make_pair(timestr, data));

		}

		KF_LOG_DEBUG(logger, "straction=open");
		auto it3 = m_mapOrder.find(timestr);
		if (it3 != m_mapOrder.end())
		{
			KF_LOG_DEBUG(logger, "straction=open in");
			localOrderRefRemoteOrderId.insert(std::make_pair(it3->second.OrderRef, timestr));
			on_rtn_order(&(it3->second));
		}
	}
	else if (msg.HasMember("ask")) {
		//std::string strOrderId = node.GetArray()[0]["order_id"].GetString();
		orderclosed = 0;
		/*int64_t id = msg["bid"].GetInt64();
		strOrderId = std::to_string(id); */
		int64_t timestamp = msg["time"].GetInt64();
		std::string timestampstr = std::to_string(timestamp);
		timestampstr = timestampstr.erase(8, 16);
		KF_LOG_DEBUG(logger, "timestampstr3:" << timestampstr);
		//std::string strClientId = node["client_id"].GetString();
		//std::lock_guard<std::mutex> lck(*m_mutexOrder); 
		auto it = m_mapNewOrder.find(timestampstr);
		if (it != m_mapNewOrder.end())
		{
			it->second.OrderStatus = LF_CHAR_NotTouched;
			//on_rtn_order(&(it->second));
			strncpy(it->second.BusinessUnit, strOrderId.c_str(), 64);
			m_mapOrder.insert(std::make_pair(strOrderId, it->second));
			m_mapNewOrder.erase(it);
		}

		auto it2 = m_mapInputOrder.find(timestampstr);
		if (it2 != m_mapInputOrder.end())
		{
			auto data = it2->second;
			m_mapInputOrder.erase(it2);
			m_mapInputOrder.insert(std::make_pair(strOrderId, data));

		}
		auto it3 = m_mapOrder.find(strOrderId);
		if (it3 != m_mapOrder.end())
		{
			KF_LOG_DEBUG(logger, "straction=open in");
			localOrderRefRemoteOrderId.insert(std::make_pair(it3->second.OrderRef, strOrderId));
			on_rtn_order(&(it3->second));
		}
		KF_LOG_DEBUG(logger, "straction=filled");
		//std::string strStatus = data["status"].GetString();
		int64_t price = msg["price"].GetInt64();
		uint64_t volume = msg["quantity"].GetInt();
		volume = volume * scale_offset;
		auto it4 = m_mapOrder.find(strOrderId);
		if (it4 != m_mapOrder.end())
		{
			KF_LOG_DEBUG(logger, "straction=filled in");
			//it4->second.OrderStatus = LF_CHAR_AllTraded;

			it4->second.VolumeTraded = volume;
			it4->second.VolumeTotal = it4->second.VolumeTotalOriginal - it4->second.VolumeTraded;
			if (it4->second.VolumeTraded == it4->second.VolumeTotalOriginal) {
				it4->second.OrderStatus = LF_CHAR_AllTraded;
			}
			else {
				it4->second.OrderStatus = LF_CHAR_PartTradedQueueing;
			}
			on_rtn_order(&(it4->second));
			raw_writer->write_frame(&(it4->second), sizeof(LFRtnOrderField),
				source_id, MSG_TYPE_LF_RTN_ORDER_COINFLOOR, 1, -1);

			LFRtnTradeField rtn_trade;
			memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
			strcpy(rtn_trade.ExchangeID, it4->second.ExchangeID);
			strncpy(rtn_trade.UserID, it4->second.UserID, sizeof(rtn_trade.UserID));
			strncpy(rtn_trade.InstrumentID, it4->second.InstrumentID, sizeof(rtn_trade.InstrumentID));
			strncpy(rtn_trade.OrderRef, it4->second.OrderRef, sizeof(rtn_trade.OrderRef));
			rtn_trade.Direction = it4->second.Direction;
			strncpy(rtn_trade.OrderSysID, strOrderId.c_str(), sizeof(rtn_trade.OrderSysID));
			rtn_trade.Volume = std::round(it4->second.VolumeTraded);
			rtn_trade.Price = (price*scale_offset);
			on_rtn_trade(&rtn_trade);
			raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
				source_id, MSG_TYPE_LF_RTN_TRADE_COINFLOOR, 1, -1);

			if (it4->second.OrderStatus == LF_CHAR_AllTraded)
			{
				auto it_id = localOrderRefRemoteOrderId.find(it4->second.OrderRef);
				if (it_id != localOrderRefRemoteOrderId.end())
				{
					KF_LOG_INFO(logger, "earse local");
					localOrderRefRemoteOrderId.erase(it_id);
				}
				m_mapOrder.erase(it4);
				//
				auto it2 = m_mapInputOrder.find(strOrderId);
				if (it2 != m_mapInputOrder.end())
				{
					m_mapInputOrder.erase(it2);
				}
				auto it3 = m_mapOrderAction.find(strOrderId);
				if (it3 != m_mapOrderAction.end())
				{
					m_mapOrderAction.erase(it3);
				}
			}
		}
	}
	else {
		orderclosed = 0;
		int64_t timestamp = msg["time"].GetInt64();
		std::string timestampstr = std::to_string(timestamp);
		timestampstr = timestampstr.erase(8, 16);
		KF_LOG_DEBUG(logger, "timestampstr5:" << timestampstr);
		//std::string strClientId = node["client_id"].GetString();
		//std::lock_guard<std::mutex> lck(*m_mutexOrder); 
		auto it = m_mapNewOrder.find(timestampstr);
		if (it != m_mapNewOrder.end())
		{
			it->second.OrderStatus = LF_CHAR_NotTouched;
			//on_rtn_order(&(it->second));
			//strncpy(it->second.BusinessUnit,strOrderId.c_str(),64);
			m_mapOrder.insert(std::make_pair(timestampstr, it->second));
			m_mapNewOrder.erase(it);
		}

		auto it2 = m_mapInputOrder.find(timestampstr);
		if (it2 != m_mapInputOrder.end())
		{
			auto data = it2->second;
			m_mapInputOrder.erase(it2);
			m_mapInputOrder.insert(std::make_pair(timestampstr, data));

		}
		KF_LOG_DEBUG(logger, "straction=filled");
		//std::string strStatus = data["status"].GetString();
		int64_t price = msg["price"].GetInt64();
		uint64_t volume = msg["quantity"].GetInt();
		volume = volume * scale_offset;
		auto it4 = m_mapOrder.find(timestampstr);
		if (it4 != m_mapOrder.end())
		{
			KF_LOG_DEBUG(logger, "straction=filled in");
			//it4->second.OrderStatus = LF_CHAR_AllTraded;

			it4->second.VolumeTraded = volume;
			it4->second.VolumeTotal = it4->second.VolumeTotalOriginal - it4->second.VolumeTraded;
			if (it4->second.VolumeTraded == it4->second.VolumeTotalOriginal) {
				it4->second.OrderStatus = LF_CHAR_AllTraded;
			}
			else {
				it4->second.OrderStatus = LF_CHAR_PartTradedQueueing;
			}
			on_rtn_order(&(it4->second));
			raw_writer->write_frame(&(it4->second), sizeof(LFRtnOrderField),
				source_id, MSG_TYPE_LF_RTN_ORDER_COINFLOOR, 1, -1);

			LFRtnTradeField rtn_trade;
			memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
			strcpy(rtn_trade.ExchangeID, it4->second.ExchangeID);
			strncpy(rtn_trade.UserID, it4->second.UserID, sizeof(rtn_trade.UserID));
			strncpy(rtn_trade.InstrumentID, it4->second.InstrumentID, sizeof(rtn_trade.InstrumentID));
			strncpy(rtn_trade.OrderRef, it4->second.OrderRef, sizeof(rtn_trade.OrderRef));
			rtn_trade.Direction = it4->second.Direction;
			//strncpy(rtn_trade.OrderSysID,strOrderId.c_str(),sizeof(rtn_trade.OrderSysID));
			rtn_trade.Volume = std::round(it4->second.VolumeTraded);
			rtn_trade.Price = (price*scale_offset);
			on_rtn_trade(&rtn_trade);
			raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),
				source_id, MSG_TYPE_LF_RTN_TRADE_COINFLOOR, 1, -1);

			if (it4->second.OrderStatus == LF_CHAR_AllTraded)
			{
				auto it_id = localOrderRefRemoteOrderId.find(it4->second.OrderRef);
				if (it_id != localOrderRefRemoteOrderId.end())
				{
					KF_LOG_INFO(logger, "earse local");
					localOrderRefRemoteOrderId.erase(it_id);
				}
				m_mapOrder.erase(it4);
				//
				auto it2 = m_mapInputOrder.find(timestampstr);
				if (it2 != m_mapInputOrder.end())
				{
					m_mapInputOrder.erase(it2);
				}
				auto it3 = m_mapOrderAction.find(timestampstr);
				if (it3 != m_mapOrderAction.end())
				{
					m_mapOrderAction.erase(it3);
				}
			}
		}
	}
}

void TDEngineCoinfloor::on_lws_data(struct lws* conn, const char* data, size_t len)
{
	//std::string strData = dealDataSprit(data);
	KF_LOG_INFO(logger, "TDEngineCoinfloor::on_lws_data: " << data);
	Document json;
	json.Parse(data);
	int zero = 0;
	int one = 1;
	int two = 2;
	int four = 4;
	string closestr = "OrderClosed";
	if (!json.HasParseError() && json.IsObject())
	{
		if (json.HasMember("nonce") && json.HasMember("notice")) {
			recnonce = json["nonce"].GetString();
			//get_auth(nonce);
			welcome = true;
		}
		if (json.HasMember("error_code") && json["error_code"].GetInt() == zero && json["tag"].GetInt() == one)
		{
			m_isSubOK = true;
			KF_LOG_INFO(logger, "m_isSubOK");
		}
		if (json.HasMember("error_code") && json["error_code"].GetInt() == zero && (json["tag"].GetInt() == four || json["tag"].GetInt() == two))
		{
			KF_LOG_INFO(logger, "into onOrderChange");
			onOrderChange(json);
		}
		if (json.HasMember("ask") || json.HasMember("ask_base_fee")) {
			onOrderChange(json);
		}
		if (json.HasMember("notice") && json["notice"].GetString() == closestr) {
			onOrderChange(json);
		}
		if (json.HasMember("base") && json.HasMember("id") && json.HasMember("notice")) {
			KF_LOG_INFO(logger, "dealid");
			int64_t id = json["id"].GetInt64();
			strOrderId = std::to_string(id);
		}
	}
	else
	{
		KF_LOG_ERROR(logger, "MDEngineCOINFLOOR::on_lws_data . parse json error");
	}

}

std::string TDEngineCoinfloor::makeSubscribetradeString(AccountUnitCoinfloor& unit)
{
	string api_key = unit.api_key;
	string secret_key = unit.secret_key;
	std::string strTime = getTimestampStr();
	StringBuffer sbUpdate;
	Writer<StringBuffer> writer(sbUpdate);
	writer.StartObject();
	writer.Key("tag");
	writer.Int(6);
	writer.Key("method");
	writer.String("WatchOrders");
	writer.Key("base");
	writer.Int(51207);
	writer.Key("counter");
	writer.Int(51887);
	writer.Key("watch");
	writer.Bool(true);
	writer.EndObject();
	std::string strUpdate = sbUpdate.GetString();

	return strUpdate;
}

std::string TDEngineCoinfloor::makeSubscribeChannelString(AccountUnitCoinfloor& unit)
{
	std::string strTime = getTimestampStr();
	StringBuffer sbUpdate;
	Writer<StringBuffer> writer(sbUpdate);
	writer.StartObject();
	writer.Key("tag");
	writer.Int(1);
	writer.Key("method");
	writer.String("Authenticate");
	writer.Key("user_id");
	writer.Int(348);
	writer.Key("cookie");
	writer.String("dKPxKc/5NEJ8BPwZzae+SoB0Lng=");
	writer.Key("nonce");
	writer.String("8IyYyvH9gujOqYJdv/BP0A==");
	writer.Key("signature");
	writer.StartArray();
	unsigned char * keySignatrue = key_sign();
	unsigned char * msgSignatrue = tomsg_sign(recnonce);
	std::string strSignatrue1;
	std::string strSignatrue2;

	mp_limb_t r[MP_NLIMBS(29)], s[MP_NLIMBS(29)], d[MP_NLIMBS(29)], z[MP_NLIMBS(29)];
	uint8_t rb[28], sb[28], db[28], zb[28];
	unsigned char d1[28], z1[28], r1[28], s1[28];
	//char r1[28], s1[28];
	for (int i = 0; i < 28; i++) {
		d1[i] = *keySignatrue;
		z1[i] = *msgSignatrue;
		//KF_LOG_INFO(logger, "d[" << i<<"]"<<d1[i]);
		keySignatrue++;
		msgSignatrue++;
	}

	for (int i = 0; i < 28; i++) {
		db[i] = (uint8_t)d1[i];
		zb[i] = (uint8_t)z1[i];
	}

#if GMP_LIMB_BITS == 32
	z[sizeof *z / sizeof z - 1] = d[sizeof *d / sizeof d - 1] = 0;
#endif

	bytes_to_mpn(d, db, sizeof db);
	bytes_to_mpn(z, zb, sizeof zb);
	ecp_sign(r, s, secp224k1_p, secp224k1_a, *secp224k1_G, secp224k1_n, d, z, MP_NLIMBS(29));
	mpn_to_bytes(rb, r, sizeof rb);
	mpn_to_bytes(sb, s, sizeof sb);

	for (int i = 0; i < 28; i++) {
		r1[i] = (unsigned char)rb[i];
		s1[i] = (unsigned char)sb[i];
	}
	unsigned char* sign1 = r1;
	unsigned char* sign2 = s1;

	strSignatrue1 = base64_encode(sign1, 28);
	strSignatrue2 = base64_encode(sign2, 28);

	writer.String(strSignatrue1.c_str());
	writer.String(strSignatrue2.c_str());

	writer.EndArray();
	writer.EndObject();
	std::string strUpdate = sbUpdate.GetString();

	return strUpdate;
}

unsigned char * TDEngineCoinfloor::tomsg_sign(const std::string& re_nonce)
{
	string decode_nonce = base64_decode(re_nonce);
	std::string random = "8IyYyvH9gujOqYJdv/BP0A==";
	random = base64_decode(random);
	char* ran = (char*)random.data();
	string hex_random = b2a_hex((char*)ran, 16);
	KF_LOG_ERROR(logger, "random:" << hex_random);
	char client[16];
	memcpy(client, ran, sizeof(client));

	char* recnonce = (char*)decode_nonce.data();
	int decode_nonce_size = decode_nonce.size();
	std::string hex_decode_nonce = b2a_hex((char*)recnonce, 16);
	char server[16];
	memcpy(server, recnonce, sizeof(server));
	KF_LOG_INFO(logger, "decode_nonce_size" << decode_nonce_size);

	uint64_t user_id = 348;
	if (is_big_endian() == false) {
		user_id = swap_uint64(user_id);
	}
	KF_LOG_INFO(logger, "user_id" << user_id);
	char buf[8];
	memset(buf, 0, sizeof(buf));
	memcpy(buf, &user_id, sizeof(user_id));

	char msgbuf[40];
	for (int i = 0; i < 8; i++) {
		msgbuf[i] = buf[i];
	}
	for (int i = 8; i < 24; i++) {
		msgbuf[i] = server[i - 8];
	}
	for (int i = 24; i < 40; i++) {
		msgbuf[i] = client[i - 24];
	}
	char* msgchar = msgbuf;
	KF_LOG_ERROR(logger, "msgbuf[8]:" << msgbuf[8]);

	int leng = 100;
	int keychar_len = 40;
	unsigned char * masage = new unsigned char[leng];
	sha224digest(msgchar, keychar_len, masage, leng);
	std::string msgdigest = b2a_hex((char*)masage, 28);
	KF_LOG_ERROR(logger, "msgdigest:" << msgdigest);

	return masage;
}

unsigned char * TDEngineCoinfloor::key_sign()
{
	std::string hex_passphrase = "wMH%7y$@r";
	char * passphrase = (char*)hex_passphrase.data();
	char pass[9];
	memcpy(pass, passphrase, sizeof(pass));
	uint64_t user_id = 348;
	if (is_big_endian() == false) {
		user_id = swap_uint64(user_id);
	}
	KF_LOG_INFO(logger, "user_id" << user_id);
	char buf[8];
	memset(buf, 0, sizeof(buf));
	memcpy(buf, &user_id, sizeof(user_id));

	char keybuf[17];
	for (int i = 0; i < 8; i++) {
		keybuf[i] = buf[i];
	}
	for (int i = 8; i < 17; i++) {
		keybuf[i] = pass[i - 8];
	}
	char* keychar = keybuf;
	KF_LOG_ERROR(logger, "keybuf[8]:" << keybuf[8]);

	int leng = 100;
	int keychar_len = 17;
	unsigned char * key = new unsigned char[leng];
	sha224digest(keychar, keychar_len, key, leng);
	std::string keydigest = b2a_hex((char*)key, 28);
	KF_LOG_ERROR(logger, "keydigest:" << keydigest);

	return key;
}

uint64_t TDEngineCoinfloor::swap_uint64(uint64_t val) {
	val = ((val << 8) & 0xFF00FF00FF00FF00ULL) | ((val >> 8) & 0x00FF00FF00FF00FFULL);
	val = ((val << 16) & 0xFFFF0000FFFF0000ULL) | ((val >> 16) & 0x0000FFFF0000FFFFULL);
	return (val << 32) | (val >> 32);
}

bool TDEngineCoinfloor::is_big_endian()
{
	unsigned short test = 0x1234;
	if (*((unsigned char*)&test) == 0x12)
		return true;
	else
		return false;
}

int TDEngineCoinfloor::lws_write_msg(struct lws* conn)
{
	//KF_LOG_INFO(logger, "TDEngineCoinfloor::lws_write_msg:" );

	int ret = 0;
	std::string strMsg = "";

	if (welcome && !m_isSub)
	{
		strMsg = makeSubscribeChannelString(account_units[0]);
		m_isSub = true;
	}
	/*else if(m_isSubOK&&!istrade){
		strMsg = makeSubscribetradeString(account_units[0]);
		istrade = true;
	}*/
	else if (m_isSubOK)
	{
		std::lock_guard<std::mutex> lck(mutex_msg_queue);
		if (m_vstMsg.size() == 0) {
			KF_LOG_INFO(logger, "TDEngineCoinfloor::m_vstMsg.size()=0 ");
			return 0;
		}
		else
		{
			KF_LOG_INFO(logger, "TDEngineCoinfloor::m_vstMsg");
			strMsg = m_vstMsg.front();
			m_vstMsg.pop();
		}
	}
	else
	{
		KF_LOG_INFO(logger, "return 0");
		return 0;
	}

	unsigned char msg[1024];
	memset(&msg[LWS_PRE], 0, 1024 - LWS_PRE);
	int length = strMsg.length();
	KF_LOG_INFO(logger, "TDEngineCoinfloor::lws_write_msg: " << strMsg.c_str() << " ,len = " << length);
	strncpy((char *)msg + LWS_PRE, strMsg.c_str(), length);
	ret = lws_write(conn, &msg[LWS_PRE], length, LWS_WRITE_TEXT);
	lws_callback_on_writable(conn);
	return ret;
}

void TDEngineCoinfloor::on_lws_connection_error(struct lws* conn)
{
	KF_LOG_ERROR(logger, "TDEngineCoinfloor::on_lws_connection_error. login again.");
	//no use it
	long timeout_nsec = 0;

	login(timeout_nsec);
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

void TDEngineCoinfloor::genUniqueKey()
{
	struct tm cur_time = getCurLocalTime();
	//SSMMHHDDN
	char key[11]{ 0 };
	snprintf((char*)key, 11, "%02d%02d%02d%02d%02d", cur_time.tm_sec, cur_time.tm_min, cur_time.tm_hour, cur_time.tm_mday, m_CurrentTDIndex);
	m_uniqueKey = key;
}

//clientid =  m_uniqueKey+orderRef
std::string TDEngineCoinfloor::genClinetid(const std::string &orderRef)
{
	static int nIndex = 0;
	return m_uniqueKey + orderRef + std::to_string(nIndex++);
}

void TDEngineCoinfloor::writeErrorLog(std::string strError)
{
	KF_LOG_ERROR(logger, strError);
}



int64_t TDEngineCoinfloor::getMSTime()
{
	long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return  timestamp;
}




void TDEngineCoinfloor::init()
{
	genUniqueKey();
	ITDEngine::init();
	JournalPair tdRawPair = getTdRawJournalPair(source_id);
	raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
	KF_LOG_INFO(logger, "[init]");
}

void TDEngineCoinfloor::pre_load(const json& j_config)
{
	KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineCoinfloor::resize_accounts(int account_num)
{
	account_units.resize(account_num);
	KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineCoinfloor::load_account(int idx, const json& j_config)
{
	KF_LOG_INFO(logger, "[load_account]");
	// internal load
	string api_key = j_config["APIKey"].get<string>();
	secret_key = j_config["SecretKey"].get<string>();
	string baseUrl = j_config["baseUrl"].get<string>();
	string wsUrl = j_config["wsUrl"].get<string>();
	rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

	if (j_config.find("orderaction_max_waiting_seconds") != j_config.end()) {
		orderaction_max_waiting_seconds = j_config["orderaction_max_waiting_seconds"].get<int>();
	}
	KF_LOG_INFO(logger, "[load_account] (orderaction_max_waiting_seconds)" << orderaction_max_waiting_seconds);

	if (j_config.find("max_rest_retry_times") != j_config.end()) {
		max_rest_retry_times = j_config["max_rest_retry_times"].get<int>();
	}
	KF_LOG_INFO(logger, "[load_account] (max_rest_retry_times)" << max_rest_retry_times);


	if (j_config.find("retry_interval_milliseconds") != j_config.end()) {
		retry_interval_milliseconds = j_config["retry_interval_milliseconds"].get<int>();
	}
	if (j_config.find("current_td_index") != j_config.end()) {
		m_CurrentTDIndex = j_config["current_td_index"].get<int>();
	}
	KF_LOG_INFO(logger, "[load_account] (retry_interval_milliseconds)" << retry_interval_milliseconds);

	AccountUnitCoinfloor& unit = account_units[idx];
	unit.api_key = api_key;
	unit.secret_key = secret_key;
	unit.baseUrl = baseUrl;
	unit.wsUrl = wsUrl;
	KF_LOG_INFO(logger, "[load_account] (api_key)" << api_key << " (baseUrl)" << unit.baseUrl);


	unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
	unit.coinPairWhiteList.Debug_print();

	unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
	unit.positionWhiteList.Debug_print();

	//display usage:
	if (unit.coinPairWhiteList.Size() == 0) {
		KF_LOG_ERROR(logger, "TDEngineCoinfloor::load_account: please add whiteLists in kungfu.json like this :");
		KF_LOG_ERROR(logger, "\"whiteLists\":{");
		KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
		KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
		KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
		KF_LOG_ERROR(logger, "},");
	}

	//test
	//Document json;
	//get_account(unit, json);

	getPriceIncrement(unit);
	// set up
	TradeAccount account = {};
	//partly copy this fields
	strncpy(account.UserID, api_key.c_str(), 16);
	strncpy(account.Password, secret_key.c_str(), 21);
	return account;
}

void TDEngineCoinfloor::connect(long timeout_nsec)
{
	KF_LOG_INFO(logger, "[connect]");
	for (size_t idx = 0; idx < account_units.size(); idx++)
	{
		AccountUnitCoinfloor& unit = account_units[idx];
		unit.logged_in = true;
		KF_LOG_INFO(logger, "[connect] (api_key)" << unit.api_key);
		login(timeout_nsec);
	}

	cancel_all_orders();
}

void TDEngineCoinfloor::getPriceIncrement(AccountUnitCoinfloor& unit)
{
	KF_LOG_INFO(logger, "[getPriceIncrement]");
	std::string requestPath = "webapi.coinfloor.com/markets/";
	string url = "webapi.coinfloor.com/markets/";
	std::string strTimestamp = getTimestampStr();

	std::string strSignatrue = "a";
	cpr::Header mapHeader = cpr::Header{ {"COINFLOOR-ACCESS-SIG",strSignatrue},
										{"COINFLOOR-ACCESS-TIMESTAMP",strTimestamp},
										{"COINFLOOR-ACCESS-KEY",unit.api_key} };
	KF_LOG_INFO(logger, "COINFLOOR-ACCESS-SIG = " << strSignatrue
		<< ", COINFLOOR-ACCESS-TIMESTAMP = " << strTimestamp
		<< ", COINFLOOR-API-KEY = " << unit.api_key);


	std::unique_lock<std::mutex> lock(g_httpMutex);
	const auto response = cpr::Get(Url{ url }, Timeout{ 10000 });
	lock.unlock();
	KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
		" (response.error.message) " << response.error.message <<
		" (response.text) " << response.text.c_str());
	Document json;
	json.Parse(response.text.c_str());

	if (!json.HasParseError())
	{
		size_t len = json.Size();
		KF_LOG_INFO(logger, "[getPriceIncrement] (accounts.length)" << len);
		for (size_t i = 0; i < len; i++)
		{
			int basenum = json.GetArray()[i]["base"].GetInt();
			int counternum = json.GetArray()[i]["counter"].GetInt();
			std::string base = std::to_string(basenum);
			std::string counter = std::to_string(counternum);
			std::string symbol = base + "_" + counter;
			std::string ticker = unit.coinPairWhiteList.GetKeyByValue(symbol);
			KF_LOG_INFO(logger, "[getPriceIncrement] (symbol) " << symbol << " (ticker) " << ticker);
			if (ticker.length() > 0) {
				int tick = json.GetArray()[i]["tick"].GetInt();
				std::string size = std::to_string(tick);
				PriceIncrement increment;
				increment.nPriceIncrement = tick;
				unit.mapPriceIncrement.insert(std::make_pair(ticker, increment));
				KF_LOG_INFO(logger, "[getPriceIncrement] (symbol) " << symbol << " (position) " << increment.nPriceIncrement);
			}
		}
	}

}

void TDEngineCoinfloor::login(long timeout_nsec)
{
	KF_LOG_INFO(logger, "TDEngineCoinfloor::login:");

	global_md = this;

	welcome = false;
	m_isSub = false;
	m_isSubOK = false;
	//istrade = false;
	global_md = this;
	int inputPort = 8443;
	int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;

	struct lws_context_creation_info ctxCreationInfo;
	struct lws_client_connect_info clientConnectInfo;
	struct lws *wsi = NULL;
	struct lws_protocols protocol;

	memset(&ctxCreationInfo, 0, sizeof(ctxCreationInfo));
	memset(&clientConnectInfo, 0, sizeof(clientConnectInfo));

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
	ctxCreationInfo.ws_ping_pong_interval = 1;
	ctxCreationInfo.ka_time = 10;
	ctxCreationInfo.ka_probes = 10;
	ctxCreationInfo.ka_interval = 10;

	protocol.name = protocols[PROTOCOL_TEST].name;
	protocol.callback = &ws_service_cb;
	protocol.per_session_data_size = sizeof(struct session_data);
	protocol.rx_buffer_size = 0;
	protocol.id = 0;
	protocol.user = NULL;

	context = lws_create_context(&ctxCreationInfo);
	KF_LOG_INFO(logger, "TDEngineCoinfloor::login: context created.");


	if (context == NULL) {
		KF_LOG_ERROR(logger, "TDEngineCoinfloor::login: context is NULL. return");
		return;
	}

	// Set up the client creation info
	std::string strAddress = "api.coinfloor.co.uk";
	clientConnectInfo.address = strAddress.c_str();
	clientConnectInfo.path = "/"; // Set the info's path to the fixed up url path
	clientConnectInfo.context = context;
	clientConnectInfo.port = 443;
	clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
	clientConnectInfo.host = strAddress.c_str();
	clientConnectInfo.origin = strAddress.c_str();
	clientConnectInfo.ietf_version_or_minus_one = -1;
	clientConnectInfo.protocol = protocols[PROTOCOL_TEST].name;
	clientConnectInfo.pwsi = &wsi;

	KF_LOG_INFO(logger, "TDEngineCoinfloor::login: address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);

	wsi = lws_client_connect_via_info(&clientConnectInfo);
	if (wsi == NULL) {
		KF_LOG_ERROR(logger, "TDEngineCoinfloor::login: wsi create error.");
		return;
	}
	KF_LOG_INFO(logger, "TDEngineCoinfloor::login: wsi create success.");
	m_conn = wsi;
	//connect(timeout_nsec);
}

void TDEngineCoinfloor::logout()
{
	KF_LOG_INFO(logger, "[logout]");
}

void TDEngineCoinfloor::release_api()
{
	KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineCoinfloor::is_logged_in() const
{
	KF_LOG_INFO(logger, "[is_logged_in]");
	for (auto& unit : account_units)
	{
		if (!unit.logged_in)
			return false;
	}
	return true;
}

bool TDEngineCoinfloor::is_connected() const
{
	KF_LOG_INFO(logger, "[is_connected]");
	return is_logged_in();
}


std::string TDEngineCoinfloor::GetSide(const LfDirectionType& input) {
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

LfDirectionType TDEngineCoinfloor::GetDirection(std::string input) {
	if ("buy" == input) {
		return LF_CHAR_Buy;
	}
	else if ("sell" == input) {
		return LF_CHAR_Sell;
	}
	else {
		return LF_CHAR_Buy;
	}
}

std::string TDEngineCoinfloor::GetType(const LfOrderPriceTypeType& input) {
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

LfOrderPriceTypeType TDEngineCoinfloor::GetPriceType(std::string input)
{
	if ("limit" == input) {
		return LF_CHAR_LimitPrice;
	}
	else if ("market" == input) {
		return LF_CHAR_AnyPrice;
	}
	else {
		return '0';
	}
}

/**
 * req functions
 */
void TDEngineCoinfloor::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
	KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

	AccountUnitCoinfloor& unit = account_units[account_index];
	KF_LOG_INFO(logger, "[req_investor_position] (api_key)" << unit.api_key << " (InstrumentID) " << data->InstrumentID);

	send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_COINFLOOR, 1, requestId);
	int errorId = 0;
	std::string errorMsg = "";
	get_account_();

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

	bool findSymbolInResult = false;
	//send the filtered position
	int position_count = positionHolder.size();
	for (int i = 0; i < position_count; i++)
	{
		pos.PosiDirection = LF_CHAR_Long;
		strncpy(pos.InstrumentID, positionHolder[i].ticker.c_str(), 31);
		if (positionHolder[i].isLong) {
			pos.PosiDirection = LF_CHAR_Long;
		}
		else {
			pos.PosiDirection = LF_CHAR_Short;
		}
		pos.Position = positionHolder[i].amount;
		on_rsp_position(&pos, i == (position_count - 1), requestId, errorId, errorMsg.c_str());
		findSymbolInResult = true;
	}

	if (!findSymbolInResult)
	{
		KF_LOG_INFO(logger, "[req_investor_position] (!findSymbolInResult) (requestId)" << requestId);
		on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());
	}

	if (errorId != 0)
	{
		raw_writer->write_error_frame(&pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS_COINFLOOR, 1, requestId, errorId, errorMsg.c_str());
	}
}

void TDEngineCoinfloor::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
	KF_LOG_INFO(logger, "[req_qry_account]");
}

void TDEngineCoinfloor::dealPriceVolume(AccountUnitCoinfloor& unit, const std::string& symbol, int64_t nPrice, int64_t nVolume, double& dDealPrice, double& dDealVolume, bool Isbuy)
{
	KF_LOG_DEBUG(logger, "[dealPriceVolume] (symbol)" << symbol);
	auto it = unit.mapPriceIncrement.find(symbol);
	if (it == unit.mapPriceIncrement.end())
	{
		KF_LOG_INFO(logger, "[dealPriceVolume] symbol not find :" << symbol);
		int64_t intvolume = nVolume / scale_offset;
		dDealVolume = intvolume;
		int64_t intprice = nPrice / scale_offset;
		dDealPrice = intprice;
		if (Isbuy == true) {
			dDealPrice = (floor(dDealPrice / it->second.nPriceIncrement))*it->second.nPriceIncrement;
		}
		else {
			dDealPrice = (ceil(dDealPrice / it->second.nPriceIncrement))*it->second.nPriceIncrement;
		}
	}
	else
	{
		KF_LOG_INFO(logger, "[dealPriceVolume]");
		/*int64_t nDealVolume =  it->second.nQuoteIncrement  > 0 ? nVolume / it->second.nQuoteIncrement * it->second.nQuoteIncrement : nVolume;
		int64_t nDealPrice = it->second.nPriceIncrement > 0 ? nPrice / it->second.nPriceIncrement * it->second.nPriceIncrement : nPrice;*/
		int64_t intvolume = nVolume / scale_offset;
		dDealVolume = intvolume;
		int64_t intprice = nPrice / scale_offset;
		dDealPrice = intprice;
		if (Isbuy == true) {
			dDealPrice = (floor(dDealPrice / it->second.nPriceIncrement))*it->second.nPriceIncrement;
		}
		else {
			dDealPrice = (ceil(dDealPrice / it->second.nPriceIncrement))*it->second.nPriceIncrement;
		}
	}

	KF_LOG_INFO(logger, "[dealPriceVolume]  (symbol)" << symbol << " (Volume)" << nVolume << " (Price)" << nPrice << " (FixedVolume)" << dDealVolume << " (FixedPrice)" << dDealPrice);
}

void TDEngineCoinfloor::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
	AccountUnitCoinfloor& unit = account_units[account_index];
	KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
		<< " (APIKey)" << unit.api_key
		<< " (Tid)" << data->InstrumentID
		<< " (Volume)" << data->Volume
		<< " (LimitPrice)" << data->LimitPrice
		<< " (OrderRef)" << data->OrderRef);
	send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_COINFLOOR, 1/*ISLAST*/, requestId);
	int errorId = 0;
	std::string errorMsg = "";
	on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
	std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
	if (ticker.length() == 0) {
		errorId = 200;
		errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
		KF_LOG_ERROR(logger, "[req_order_insert]: not in WhiteList, ignore it  (rid)" << requestId <<
			" (errorId)" << errorId << " (errorMsg) " << errorMsg);
		on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_COINFLOOR, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);

	bool isbuy;
	if (GetSide(data->Direction) == "buy") {
		isbuy = true;
	}
	else if (GetSide(data->Direction) == "sell") {
		isbuy = false;
	}
	double fixedPrice = 0;
	double fixedVolume = 0;
	dealPriceVolume(unit, data->InstrumentID, data->LimitPrice, data->Volume, fixedPrice, fixedVolume, isbuy);

	if (fixedVolume == 0)
	{
		KF_LOG_DEBUG(logger, "[req_order_insert] fixed Volume error" << ticker);
		errorId = 200;
		errorMsg = data->InstrumentID;
		errorMsg += " : quote less than baseMinSize";
		on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_COINFLOOR, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	std::string strClientId = genClinetid(data->OrderRef);
	KF_LOG_INFO(logger, "(strClientId)" << strClientId);
	timestr = getTimestampStr();
	timestr = timestr.erase(8, 13);
	KF_LOG_DEBUG(logger, "timestr" << timestr);
	{
		std::lock_guard<std::mutex> lck(*m_mutexOrder);
		m_mapInputOrder.insert(std::make_pair(timestr, *data));
		LFRtnOrderField order;
		memset(&order, 0, sizeof(LFRtnOrderField));
		order.OrderStatus = LF_CHAR_Unknown;
		order.VolumeTotalOriginal = std::round(fixedVolume*scale_offset);
		order.VolumeTotal = order.VolumeTotalOriginal;
		strncpy(order.OrderRef, data->OrderRef, 21);
		strncpy(order.InstrumentID, data->InstrumentID, 31);
		order.RequestID = requestId;
		strcpy(order.ExchangeID, "COINFLOOR");
		strncpy(order.UserID, unit.api_key.c_str(), 16);
		order.LimitPrice = std::round(fixedPrice*scale_offset);
		order.TimeCondition = data->TimeCondition;
		order.Direction = data->Direction;
		order.OrderPriceType = data->OrderPriceType;
		m_mapNewOrder.insert(std::make_pair(timestr, order));
	}

	send_order(ticker.c_str(), strClientId.c_str(), GetSide(data->Direction).c_str(), GetType(data->OrderPriceType).c_str(), fixedVolume, fixedPrice);


}

void TDEngineCoinfloor::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
	AccountUnitCoinfloor& unit = account_units[account_index];
	KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
		<< " (APIKey)" << unit.api_key
		<< " (Iid)" << data->InvestorID
		<< " (OrderRef)" << data->OrderRef
		<< " (KfOrderID)" << data->KfOrderID);

	send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_COINFLOOR, 1, requestId);

	int errorId = 0;
	std::string errorMsg = "";
	on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
	std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
	if (ticker.length() == 0) {
		errorId = 200;
		errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
		KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
			errorId << " (errorMsg) " << errorMsg);
		on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_COINFLOOR, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);
	std::lock_guard<std::mutex> lck(*m_mutexOrder);
	std::map<std::string, std::string>::iterator itr = localOrderRefRemoteOrderId.find(data->OrderRef);
	std::string remoteOrderId;
	if (itr == localOrderRefRemoteOrderId.end()) {
		errorId = 1;
		std::stringstream ss;
		ss << "[req_order_action] not found in localOrderRefRemoteOrderId map (orderRef) " << data->OrderRef;
		errorMsg = ss.str();
		KF_LOG_ERROR(logger, "[req_order_action] not found in localOrderRefRemoteOrderId map. "
			<< " (rid)" << requestId << " (orderRef)" << data->OrderRef << " (errorId)" << errorId << " (errorMsg) " << errorMsg);
		on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
		raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_COINFLOOR, 1, requestId, errorId, errorMsg.c_str());
		return;
	}
	else if (itr != localOrderRefRemoteOrderId.end() && orderclosed != 1) {
		remoteOrderId = itr->second;
		KF_LOG_DEBUG(logger, "[req_order_action] found in localOrderRefRemoteOrderId map (orderRef) "
			<< data->OrderRef << " (remoteOrderId) " << remoteOrderId);
		{
			//std::lock_guard<std::mutex> lck(*m_mutexOrder);
			m_mapOrderAction.insert(std::make_pair(remoteOrderId, *data));
		}
		cancel_order(remoteOrderId);
	}

}

//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
void TDEngineCoinfloor::addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId)
{
	std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

	OrderActionSentTime newOrderActionSent;
	newOrderActionSent.requestId = requestId;
	newOrderActionSent.sentNameTime = getTimestamp();
	memcpy(&newOrderActionSent.data, data, sizeof(LFOrderActionField));
	remoteOrderIdOrderActionSentTime[remoteOrderId] = newOrderActionSent;
}


void TDEngineCoinfloor::set_reader_thread()
{
	ITDEngine::set_reader_thread();

	KF_LOG_INFO(logger, "[set_reader_thread] rest_thread start on TDEngineCoinfloor::loop");
	rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineCoinfloor::loopwebsocket, this)));

	//KF_LOG_INFO(logger, "[set_reader_thread] orderaction_timeout_thread start on TDEngineCoinfloor::loopOrderActionNoResponseTimeOut");
	//orderaction_timeout_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineCoinfloor::loopOrderActionNoResponseTimeOut, this)));
}

void TDEngineCoinfloor::loopwebsocket()
{
	while (isRunning)
	{
		//KF_LOG_INFO(logger, "TDEngineCoinfloor::loop:lws_service");
		lws_service(context, rest_get_interval_ms);
		//延时返回撤单回报
		std::lock_guard<std::mutex> lck(*m_mutexOrder);
		for (auto canceled_order = m_mapCanceledOrder.begin(); canceled_order != m_mapCanceledOrder.end(); ++canceled_order)
		{
			if (getMSTime() - canceled_order->second >= 1000)
			{// 撤单成功超过1秒时，回报205
				auto it = m_mapOrder.find(canceled_order->first);
				if (it != m_mapOrder.end())
				{
					on_rtn_order(&(it->second));
					m_mapOrder.erase(it);
				}
				canceled_order = m_mapCanceledOrder.erase(canceled_order);
				if (canceled_order == m_mapCanceledOrder.end())
				{
					break;
				}
			}
		}
	}
}



void TDEngineCoinfloor::loopOrderActionNoResponseTimeOut()
{
	KF_LOG_INFO(logger, "[loopOrderActionNoResponseTimeOut] (isRunning) " << isRunning);
	while (isRunning)
	{
		orderActionNoResponseTimeOut();
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	}
}

void TDEngineCoinfloor::orderActionNoResponseTimeOut()
{
	//    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut]");
	int errorId = 100;
	std::string errorMsg = "OrderAction has none response for a long time(" + std::to_string(orderaction_max_waiting_seconds) + " s), please send OrderAction again";

	std::lock_guard<std::mutex> guard_mutex_order_action(*mutex_orderaction_waiting_response);

	int64_t currentNano = getTimestamp();
	int64_t timeBeforeNano = currentNano - orderaction_max_waiting_seconds * 1000;
	//    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (currentNano)" << currentNano << " (timeBeforeNano)" << timeBeforeNano);
	std::map<std::string, OrderActionSentTime>::iterator itr;
	for (itr = remoteOrderIdOrderActionSentTime.begin(); itr != remoteOrderIdOrderActionSentTime.end();)
	{
		if (itr->second.sentNameTime < timeBeforeNano)
		{
			KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.erase remoteOrderId)" << itr->first);
			on_rsp_order_action(&itr->second.data, itr->second.requestId, errorId, errorMsg.c_str());
			itr = remoteOrderIdOrderActionSentTime.erase(itr);
		}
		else {
			++itr;
		}
	}
	//    KF_LOG_DEBUG(logger, "[orderActionNoResponseTimeOut] (remoteOrderIdOrderActionSentTime.size)" << remoteOrderIdOrderActionSentTime.size());
}

void TDEngineCoinfloor::printResponse(const Document& d)
{
	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	d.Accept(writer);
	KF_LOG_INFO(logger, "[printResponse] ok (text) " << buffer.GetString());
}

/*
void TDEngineCoinfloor::get_account(AccountUnitCOINFLOOR& unit, Document& json)
{
	KF_LOG_INFO(logger, "[get_account]");

	std::string requestPath = "/v1/positions";

	string url = unit.baseUrl + requestPath ;

	std::string strTimestamp = getTimestampStr();

	std::string strSignatrue = sign(unit,"GET",strTimestamp,requestPath);
	cpr::Header mapHeader = cpr::Header{{"COINFLOOR-ACCESS-SIG",strSignatrue},
										{"COINFLOOR-ACCESS-TIMESTAMP",strTimestamp},
										{"COINFLOOR-ACCESS-KEY",unit.api_key}};
	 KF_LOG_INFO(logger, "COINFLOOR-ACCESS-SIG = " << strSignatrue
						<< ", COINFLOOR-ACCESS-TIMESTAMP = " << strTimestamp
						<< ", COINFLOOR-API-KEY = " << unit.api_key);


	std::unique_lock<std::mutex> lock(g_httpMutex);
	const auto response = cpr::Get(Url{url},
							 Header{mapHeader}, Timeout{10000} );
	lock.unlock();
	KF_LOG_INFO(logger, "[get] (url) " << url << " (response.status_code) " << response.status_code <<
											   " (response.error.message) " << response.error.message <<
											   " (response.text) " << response.text.c_str());

	json.Parse(response.text.c_str());
	return ;
}*/

/*
 {
  channel: "trading",
  type: "request",
  action: "create-order",
  data: {
	contract_code: "BTCU18",
	client_id: "My Order #10",
	type: "limit",
	side: "buy",
	size: "10.0000",
	price: "6500.00"
  }
}
 */
std::string TDEngineCoinfloor::createInsertOrderString(const char *code, const char* strClientId, const char *side, const char *type, double& size, double& price)
{
	KF_LOG_INFO(logger, "[TDEngineCoinfloor::createInsertOrdertring]:(price)" << price << "(volume)" << size << "(type)" << type);
	int volume = size;
	string limitstr = "limit";
	string buystr = "buy";
	string codestr = code;
	string basestr = codestr.substr(0, 5);
	string counterstr = codestr.substr(6, 11);
	int base = atoi(basestr.c_str());
	int counter = atoi(counterstr.c_str());
	int intprice = price;
	if (side == buystr) {
		volume = size;
	}
	else {
		volume = -size;
	}
	//string strvolume=std::to_string(volume);
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("tag");
	writer.Int(4);
	writer.Key("method");
	writer.String("PlaceOrder");
	writer.Key("base");
	writer.Int(base);
	writer.Key("counter");
	writer.Int(counter);
	writer.Key("quantity");
	writer.Int(volume);
	if (type == limitstr) {
		writer.Key("price");
		writer.Int(intprice);
	}

	writer.EndObject();
	std::string strOrder = s.GetString();
	KF_LOG_INFO(logger, "[TDEngineCoinfloor::createInsertOrdertring]:" << strOrder);
	return strOrder;
}

void TDEngineCoinfloor::send_order(const char *code, const char* strClientId, const char *side, const char *type, double& size, double& price)
{
	KF_LOG_INFO(logger, "[send_order]");
	{
		std::string new_order = createInsertOrderString(code, strClientId, side, type, size, price);
		//std::lock_guard<std::mutex> lck(mutex_msg_queue);
		m_vstMsg.push(new_order);
		lws_callback_on_writable(m_conn);
	}
}



void TDEngineCoinfloor::cancel_all_orders()
{
	KF_LOG_INFO(logger, "[cancel_all_orders]");

	std::string cancel_allorder = createCancelallOrderString();
	//std::lock_guard<std::mutex> lck(mutex_msg_queue);
	m_vstMsg.push(cancel_allorder);
	//lws_callback_on_writable(m_conn);
}

void TDEngineCoinfloor::get_account_()
{
	KF_LOG_INFO(logger, "[get_account_]");

	std::string get_account_ = createaccountString();
	//std::lock_guard<std::mutex> lck(mutex_msg_queue);
	m_vstMsg.push(get_account_);
	//lws_callback_on_writable(m_conn);
}
/*
{
  channel: "trading",
  type: "request",
  action: "cancel-order",
  data: {
	order_id: "58f5435e-02b8-4875-81d4-e3976c5ed68b"
  }
}
*/
std::string TDEngineCoinfloor::createCancelOrderString(const char* strOrderId_)
{
	KF_LOG_INFO(logger, "[TDEngineCoinfloor::createCancelOrderString]");
	string Id = strOrderId_;
	int64_t id = atoi(Id.c_str());
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("tag");
	writer.Int(2);
	writer.Key("method");
	writer.String("CancelOrder");
	writer.Key("id");
	writer.Int64(id);

	writer.EndObject();
	std::string strOrder = s.GetString();
	KF_LOG_INFO(logger, "[TDEngineCoinfloor::createCancelOrderString]:" << strOrder);
	return strOrder;
}

std::string TDEngineCoinfloor::createCancelallOrderString()
{
	KF_LOG_INFO(logger, "[TDEngineCoinfloor::createCancelallOrderString]");
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("tag");
	writer.Int(3);
	writer.Key("method");
	writer.String("CancelAllOrders");

	writer.EndObject();
	std::string strOrder = s.GetString();
	KF_LOG_INFO(logger, "[TDEngineCoinfloor::createCancelallOrderString]:" << strOrder);
	return strOrder;
}

std::string TDEngineCoinfloor::createaccountString()
{
	KF_LOG_INFO(logger, "[TDEngineCoinfloor::createaccountString]");
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("tag");
	writer.Int(5);
	writer.Key("method");
	writer.String("GetBalances");

	writer.EndObject();
	std::string strOrder = s.GetString();
	KF_LOG_INFO(logger, "[TDEngineCoinfloor::createaccountString]:" << strOrder);
	return strOrder;
}

void TDEngineCoinfloor::cancel_order(std::string orderId)
{
	KF_LOG_INFO(logger, "[cancel_order]");
	std::string cancel_order = createCancelOrderString(orderId.c_str());
	//std::lock_guard<std::mutex> lck(mutex_msg_queue);
	m_vstMsg.push(cancel_order);
	lws_callback_on_writable(m_conn);
}

std::string TDEngineCoinfloor::parseJsonToString(Document &d)
{
	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	d.Accept(writer);

	return buffer.GetString();
}


inline int64_t TDEngineCoinfloor::getTimestamp()
{
	long long timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return timestamp;
}



#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

BOOST_PYTHON_MODULE(libcoinfloortd)
{
	using namespace boost::python;
	class_<TDEngineCoinfloor, boost::shared_ptr<TDEngineCoinfloor> >("Engine")
		.def(init<>())
		.def("init", &TDEngineCoinfloor::initialize)
		.def("start", &TDEngineCoinfloor::start)
		.def("stop", &TDEngineCoinfloor::stop)
		.def("logout", &TDEngineCoinfloor::logout)
		.def("wait_for_stop", &TDEngineCoinfloor::wait_for_stop);
}

