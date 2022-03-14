#include "MDEngineCoincheck.h"
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

static MDEngineCoincheck* global_md = nullptr;


MDEngineCoincheck::MDEngineCoincheck() : IMDEngine(SOURCE_COINCHECK)
{
	logger = yijinjing::KfLog::getLogger("MdEngine.Coincheck");
}

void MDEngineCoincheck::load(const json& j_config)
{
	book_depth_count = j_config["book_depth_count"].get<int>();
	priceBook20Assembler.SetLevel(book_depth_count);
	level_threshold = j_config["level_threshold"].get<int>();
	priceBook20Assembler.SetLeastLevel(level_threshold);

	refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
	refresh_normal_check_trade_s = j_config["refresh_normal_check_trade_s"].get<int>();

	coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
	coinPairWhiteList.Debug_print();

	if (coinPairWhiteList.Size() == 0) {
		KF_LOG_ERROR(logger, "MDEngineCoincheck::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
		KF_LOG_ERROR(logger, "\"whiteLists\":{");
		KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
		KF_LOG_ERROR(logger, "    \"btc_jpy\": \"btc_jpy\"");
		KF_LOG_ERROR(logger, "},");
	}
	KF_LOG_INFO(logger, "MDEngineCoincheck::load:  book_depth_count: "<< book_depth_count << " refresh_normal_check_book_s:"<<refresh_normal_check_book_s <<" level_threshold:"<<level_threshold);

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
	}
}

void MDEngineCoincheck::makeWebsocketSubscribeJsonString()
{
	std::unordered_map<std::string, std::string>::iterator map_itr;
	map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
	while (map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
		KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);

		std::string jsonBookString = createBookJsonString(map_itr->second);
		websocket.SendMessage(jsonBookString);

		std::string jsonTradeString = createTradeJsonString(map_itr->second);
		websocket.SendMessage(jsonTradeString);
		//KF_LOG_INFO(logger, "sendmessage "<< jsonBookString <<" "<<jsonTradeString);
		map_itr++;
	}
}

void MDEngineCoincheck::connect(long timeout_nsec)
{
	KF_LOG_INFO(logger, "MDEngineCoincheck::connect:");
	connected = true;
}

void MDEngineCoincheck::login(long timeout_nsec) {
	KF_LOG_INFO(logger, "MDEngineCoincheck::login:");
	websocket.RegisterCallBack(this);
	logged_in = websocket.Connect("ws-api.coincheck.com/");
	timer = getTimestamp();
	KF_LOG_INFO(logger, "MDEngineCoincheck::login " << (logged_in ? "Success" : "Failed"));
}

void MDEngineCoincheck::set_reader_thread()
{
	IMDEngine::set_reader_thread();

	rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineCoincheck::loop, this)));
}

void MDEngineCoincheck::logout()
{
	KF_LOG_INFO(logger, "MDEngineCoincheck::logout:");
}

void MDEngineCoincheck::release_api()
{
	KF_LOG_INFO(logger, "MDEngineCoincheck::release_api:");
}

void MDEngineCoincheck::subscribeMarketData(const vector<string> & instruments, const vector<string> & markets)
{
	KF_LOG_INFO(logger, "MDEngineCoincheck::subscribeMarketData:");
}

void MDEngineCoincheck::initBook(std::string strategy_coinpair, std::string exchange_coinpair)
{
	KF_LOG_INFO(logger, "MDEngineCoincheck::initBook,ticker="<<strategy_coinpair);
	priceBook20Assembler.clearPriceBook(strategy_coinpair);
	auto r = cpr::Get(cpr::Url{"https://coincheck.com/api/order_books"});
	KF_LOG_INFO(logger, " (response.status_code) "<<r.status_code<< "\n (response.error.message) "<< r.error.message);
	Document json;
	json.Parse(r.text.c_str());

	auto &asks = json["asks"];
	auto &bids = json["bids"];
	double price;
	double volume;
	int size;

	if(asks.IsArray() && asks.Size() >0)
	{
	    size = (int)asks.Size();
		for(int i = 0; i < size; ++i)
		{
			price = std::round(stod(asks.GetArray()[i][0].GetString()) * scale_offset);
			volume = std::round(stod(asks.GetArray()[i][1].GetString()) * scale_offset);
			priceBook20Assembler.UpdateAskPrice(strategy_coinpair, price, volume);
		}
	}

	if(bids.IsArray() && bids.Size() >0)
	{
		size = (int)bids.Size();
		for(int i = 0; i < size; ++i)
		{
			price = std::round(stod(bids.GetArray()[i][0].GetString()) * scale_offset);
			volume = std::round(stod(bids.GetArray()[i][1].GetString()) * scale_offset);
			priceBook20Assembler.UpdateBidPrice(strategy_coinpair, price, volume);
		}
	}
	KF_LOG_INFO(logger, "MDEngineCoincheck::initBook Success");

}

void MDEngineCoincheck::initSnapshot()
{
	std::unordered_map<std::string, std::string>::iterator map_itr;
	map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
	while (map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
		KF_LOG_DEBUG(logger, "[initBook] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);
		initBook(map_itr->first, map_itr->second);

		map_itr++;
	}
}

void MDEngineCoincheck::OnConnected(const common::CWebsocket* instance)
{
	is_ws_disconnectd = false;
	
	initSnapshot();
	makeWebsocketSubscribeJsonString();
}

void MDEngineCoincheck::OnReceivedMessage(const common::CWebsocket* instance,const std::string& msg)
{
	KF_LOG_INFO(logger, "MDEngineCoincheck::on_lws_data: " << msg);
	Document json;
	json.Parse(msg.c_str());

	if (json.HasParseError()) {
		KF_LOG_ERROR(logger, "MDEngineCoincheck::on_lws_data. parse json error.");
		return;
	}

	//order book
	if(json.GetArray().Size() == 2)
	{
		KF_LOG_INFO(logger, "MDEngineCoincheck::on_lws_data: is book");
		onBook(json,json.GetArray()[0].GetString());
	}
	//trade
	else if(json.GetArray().Size() > 2)
	{
		KF_LOG_INFO(logger, "MDEngineCoincheck::on_lws_data: is trade");
		onTrade(json,json.GetArray()[1].GetString());
	}
	else {
		KF_LOG_INFO(logger, "MDEngineCoincheck::on_lws_data: unknown array data.");
	}
}

void MDEngineCoincheck::OnDisconnected(const common::CWebsocket* instance)
{
	KF_LOG_ERROR(logger, "MDEngineCoincheck::on_lws_connection_error.");
	//market logged_in false;
	logged_in = false;
	KF_LOG_ERROR(logger, "MDEngineCoincheck::on_lws_connection_error. login again.");
	//clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
	priceBook20Assembler.clearPriceBook();
	//no use it
	long timeout_nsec = 0;
	is_ws_disconnectd = true;
	//login(timeout_nsec);
}

int64_t MDEngineCoincheck::getTimestamp()
{
	long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	return timestamp;
}

void MDEngineCoincheck::onTrade(Document& json,std::string exchange_coinpair)
{
	KF_LOG_INFO(logger, "MDEngineCoincheck::onTrade: (symbol) " << exchange_coinpair);

	std::string ticker = coinPairWhiteList.GetKeyByValue(exchange_coinpair);
	if (ticker.length() == 0) {
		return;
	}

	int len = json.GetArray().Size();

	if (len == 0) return;

	LFL2TradeField trade;
	memset(&trade, 0, sizeof(trade));
	strcpy(trade.InstrumentID, ticker.c_str());
	strcpy(trade.ExchangeID, "coincheck");
	strcpy(trade.TradeID,std::to_string(json.GetArray()[0].GetInt()).c_str());

	trade.Price = std::round(stod(json.GetArray()[2].GetString()) * scale_offset);
	trade.Volume = std::round(stod(json.GetArray()[3].GetString()) * scale_offset);

	trade.OrderBSFlag[0] = json.GetArray()[4] == "buy" ? 'B' : 'S';
	trade.TimeStamp = getTimestamp()*1000000;
	std::string strTime = timestamp_to_formatISO8601(trade.TimeStamp/1000000);
    strncpy(trade.TradeTime, strTime.c_str(),sizeof(trade.TradeTime));
	KF_LOG_INFO(logger, "MDEngineCoincheck::[onTrade] (ticker)" << ticker <<
		" (Price)" << trade.Price <<
		" (trade.Volume)" << trade.Volume 
		<< "trade.OrderBSFlag[0]" <<trade.OrderBSFlag[0]);
	on_trade(&trade);
	KF_LOG_INFO(logger, "MDEngineCoincheck::onTrade: Success");

	std::unique_lock<std::mutex> lck(trade_mutex);
	auto it = control_trade_map.find(ticker);
	if(it != control_trade_map.end())
	{
		it->second = getTimestamp();
	}
	lck.unlock();
}

void MDEngineCoincheck::onBook(Document& json,std::string exchange_coinpair)
{
	KF_LOG_INFO(logger, "MDEngineCoincheck::onBook: (symbol) " << exchange_coinpair);

	std::string ticker = coinPairWhiteList.GetKeyByValue(exchange_coinpair);
	if (ticker.length() == 0) {
		KF_LOG_DEBUG(logger, "MDEngineCoincheck::onBook: (ticker.length==0) " << ticker);
		return;
	}

	KF_LOG_INFO(logger, "MDEngineCoincheck::onBook: (ticker) " << ticker);

	int size = json.GetArray().Size();
	int last_element = size - 1;
	double price;
	double volume;
	if (json.GetArray()[last_element].IsObject()) {
		auto data = json.GetArray()[last_element].GetObject();
		if(data.HasMember("asks"))
		{
			auto& asks = data["asks"];
			if(asks.IsArray() && asks.Size() >0)
			{
				size = (int)asks.Size();
				for(int i=0;i<size;i++)
				{
					price = std::round(stod(asks.GetArray()[i][0].GetString()) * scale_offset);
					volume = std::round(stod(asks.GetArray()[i][1].GetString()) * scale_offset);
					if(volume == 0)
						priceBook20Assembler.EraseAskPrice(ticker, price);
					else
						priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
				}
			}
		}
		if(data.HasMember("bids"))
		{
			auto& bids = data["bids"];
			if(bids.IsArray() && bids.Size() >0)
			{
				size = (int)bids.Size();
				for(int i=0;i<size;i++)
				{
					price = std::round(stod(bids.GetArray()[i][0].GetString()) * scale_offset);
					volume = std::round(stod(bids.GetArray()[i][1].GetString()) * scale_offset);
					if(volume == 0)
						priceBook20Assembler.EraseBidPrice(ticker, price);
					else
						priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
				}
			}
		}		
	}
	//has any update
	int errorId = 0;
	string errorMsg = "";
	LFPriceBook20Field md;
	memset(&md, 0, sizeof(md));
	if (priceBook20Assembler.Assembler(ticker, md)) 
	{
		strcpy(md.ExchangeID, "coincheck");

		KF_LOG_INFO(logger, "MDEngineCoincheck::onBook: on_price_book_update");

		if (priceBook20Assembler.GetLeastLevel() > priceBook20Assembler.GetNumberOfLevels_asks(ticker) ||
			priceBook20Assembler.GetLeastLevel() > priceBook20Assembler.GetNumberOfLevels_bids(ticker) )
		{
			md.Status = 1;
			errorId = 112;
			errorMsg = "orderbook level below threshold";
			/*need re-login*/
			KF_LOG_DEBUG(logger, "MDEngineCoincheck on_price_book_update failed ,lose level,re-login....");
			on_price_book_update(&md);
			write_errormsg(errorId,errorMsg);

		}
		else if ((priceBook20Assembler.GetBestBidPrice(ticker)) <= 0 || (priceBook20Assembler.GetBestAskPrice(ticker) <= 0) ||
			priceBook20Assembler.GetBestBidPrice(ticker) >= priceBook20Assembler.GetBestAskPrice(ticker))
		{
			md.Status = 2;
			errorId = 113;
			errorMsg = "orderbook crossed";			
			/*need re-login*/
			KF_LOG_DEBUG(logger, "MDEngineCoincheck on_price_book_update failed ,orderbook crossed,re-login....");
			on_price_book_update(&md);
			write_errormsg(errorId,errorMsg);
		}
		else
		{
			md.Status = 0;
			on_price_book_update(&md);
			timer = getTimestamp();
			KF_LOG_DEBUG(logger, "MDEngineCoincheck on_price_book_update successed");
		}

	}
	else
	{
		timer = getTimestamp();
		KF_LOG_DEBUG(logger, "MDEngineCoincheck on_price_book_update,priceBook20Assembler.Assembler(ticker, md) failed\n(ticker)" << ticker);
	}
	std::unique_lock<std::mutex> lck1(book_mutex);
	auto it = control_book_map.find(ticker);
	if(it != control_book_map.end())
	{
		it->second = getTimestamp();
	}
	lck1.unlock();
}

//{type: "subscribe", channel: "btc_jpy-orderbook"}
std::string MDEngineCoincheck::createBookJsonString(std::string exchange_coinpair)
{
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("type");
	writer.String("subscribe");

	writer.Key("channel");
	writer.String("btc_jpy-orderbook");

	writer.EndObject();
	return s.GetString();
}

//{type: "subscribe", channel: "btc_jpy-trades"}
std::string MDEngineCoincheck::createTradeJsonString(std::string exchange_coinpair)
{
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("type");
	writer.String("subscribe");

	writer.Key("channel");
	writer.String("btc_jpy-trades");

	writer.EndObject();
	return s.GetString();
}

void MDEngineCoincheck::loop()
{
	while (isRunning)
	{
		int errorId = 0;
		std::string errorMsg = "";
		int64_t now = getTimestamp();

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
		if(is_ws_disconnectd)
		{
			login(0);
			is_ws_disconnectd = false;
		}
	}
}

BOOST_PYTHON_MODULE(libcoincheckmd)
{
	using namespace boost::python;
	class_<MDEngineCoincheck, boost::shared_ptr<MDEngineCoincheck> >("Engine")
		.def(init<>())
		.def("init", &MDEngineCoincheck::initialize)
		.def("start", &MDEngineCoincheck::start)
		.def("stop", &MDEngineCoincheck::stop)
		.def("logout", &MDEngineCoincheck::logout)
		.def("wait_for_stop", &MDEngineCoincheck::wait_for_stop);
}

