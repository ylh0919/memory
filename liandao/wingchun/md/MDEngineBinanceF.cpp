/*
* connect to "COIN-M Futures" or "USDT-M Futures"
* binanced -> "COIN-M Futures"
* "COIN-M Futures":{ "restapi":"https://dapi.binance.com", "websocket":"dstream.binance.com"}
* is_coin_m: true
* binancef -> "USDT-M Futures"
* "USDT-M Futures": { "restapi":"https://fapi.binance.com", "websocket":"fstream.binance.com"}
* is_coin_m: false
*/
#include "MDEngineBinanceF.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/LFDataStruct.h"

#include <document.h>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <string>
#include <cpr/cpr.h>
#include <chrono>
#include <algorithm>

using cpr::Get;
using cpr::Url;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;

using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using std::string;
using std::to_string;
using std::stod;
using std::stoi;

USING_WC_NAMESPACE

std::mutex ws_book_mutex;
std::mutex ws_reconnect_mutex;
std::mutex rest_book_mutex;
std::mutex trade_mutex;
std::mutex book_mutex;
std::mutex kline_mutex;
//add line here
std::mutex kline_mutex2;
static MDEngineBinanceF* g_md_binancef = nullptr;

static int lws_event_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{
	std::cout << "MdEngine.BinanceF: lws_event_cb (lws_callback_reasons)" << reason << std::endl;
    switch( reason )
    {   
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            lws_callback_on_writable( wsi );
            break;      
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {
			if(g_md_binancef)
			{
				g_md_binancef->on_lws_data((const char*)in, len);
			}
            break;      
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {       
            break;      
        }       
        case LWS_CALLBACK_CLOSED:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        case LWS_CALLBACK_CLIENT_CLOSED:
		/*
        {
			std::cout << "MdEngine.BinanceF LWS_CALLBACK_CLOSED or LWS_CALLBACK_CLIENT_CONNECTION_ERROR" << std::endl;
            if(g_md_binancef)
			{
				g_md_binancef->on_lws_connection_error(wsi);
			}      
            break;      
        }
		*/
		case LWS_CALLBACK_WSI_DESTROY: //先CLOSED再DESTROY
		{
			if (g_md_binancef)
			{
				g_md_binancef->on_lws_connection_destroy(wsi, reason);
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
		"example-protocol",
		lws_event_cb,
		0,
		65536,
	},
	{ NULL, NULL, 0, 0 } /* terminator */
};

MDEngineBinanceF::MDEngineBinanceF(): IMDEngine(SOURCE_BINANCEF)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.BinanceF");
    timer[0] = getTimestamp();
    timer[1] = timer[2] = timer[0];
}

void MDEngineBinanceF::load(const json& j_config)
{
    book_depth_count = j_config["book_depth_count"].get<int>();   
    level_threshold =  j_config["level_threshold"].get<int>(); 

	if (j_config.find("need_get_snapshot_via_rest") != j_config.end())
		need_get_snapshot_via_rest = j_config["need_get_snapshot_via_rest"].get<bool>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    if(j_config.find("snapshot_check_s") != j_config.end()) {
        snapshot_check_s = j_config["snapshot_check_s"].get<int>();
    }
	refresh_normal_check_trade_s = j_config["refresh_normal_check_trade_s"].get<int>();
	refresh_normal_check_kline_s = j_config["refresh_normal_check_kline_s"].get<int>();
	//check orderbook
	if (j_config.find("orderbook_latency_threshold_exceeded_ms") != j_config.end())
		orderbook_latency_threshold_exceeded_ms = j_config["orderbook_latency_threshold_exceeded_ms"].get<int>();
	if (j_config.find("need_restart") != j_config.end())
		need_restart = j_config["need_restart"].get<bool>();

    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

	//produce kline locally
	if (j_config.find("need_another_kline") != j_config.end())
		need_another_kline = j_config["need_another_kline"].get<bool>();
	if (j_config.find("bar_duration_s") != j_config.end())
		bar_duration_s = j_config["bar_duration_s"].get<int>();
	if (j_config.find("bar_wait_time_ms") != j_config.end())
		bar_wait_time_ms = j_config["bar_wait_time_ms"].get<int>();

	//subscribe two kline
	/*
	if (j_config.find("kline_a_interval") != j_config.end())
		kline_a_interval = j_config["kline_a_interval"].get<string>();
	if (j_config.find("kline_b_interval") != j_config.end())
		kline_b_interval = j_config["kline_b_interval"].get<string>();
	*/
	if (j_config.find("kline_a_interval_s") != j_config.end())
		kline_a_interval = getIntervalStr(j_config["kline_a_interval_s"].get<int64_t>());
	if (j_config.find("kline_b_interval_s") != j_config.end())
		kline_b_interval = getIntervalStr(j_config["kline_b_interval_s"].get<int64_t>());

	//receive kline limit
	if (j_config.find("only_receive_complete_kline") != j_config.end())
		only_receive_complete_kline = j_config["only_receive_complete_kline"].get<bool>();

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineBinanceF::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"BTCUSDT\"");
        KF_LOG_ERROR(logger, "},");
    }
	
    KF_LOG_INFO(logger, "MDEngineBinanceF::load:  book_depth_count: "
		<< book_depth_count << " level_threshold: " << level_threshold 
		<< " rest_get_interval_ms: " << rest_get_interval_ms
    	<< " refresh_normal_check_book_s: " << refresh_normal_check_book_s);
	
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
}

void MDEngineBinanceF::connect(long timeout_nsec)
{
   KF_LOG_INFO(logger, "MDEngineBinanceF::connect:"); 	
	
   connected = true;
}

void MDEngineBinanceF::login(long timeout_nsec)
{
	g_md_binancef = this;

	struct lws_context_creation_info info;
	memset( &info, 0, sizeof(info) );
	info.port = CONTEXT_PORT_NO_LISTEN;
	info.protocols = protocols;
	info.gid = -1;
	info.uid = -1;
	info.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
	info.max_http_header_pool = 1024;
	info.fd_limit_per_thread = 1024;
	context = lws_create_context( &info );
	
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    std::string path = "";
	//modified, not used in binancef
	std::string path_d = "";
    while(true) {
        KF_LOG_INFO(logger, "[debug_print] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) "<< map_itr->second);
        std::string symbol = map_itr->second;
        std::transform(symbol.begin(), symbol.end(), symbol.begin(), ::tolower);

		//modified
		if (!is_coin_m_futures(symbol)) {
			path = path + symbol + "@" + "markPrice" + "/";
			path = path + symbol + "@" + "aggTrade" + "/";
			path = path + symbol + "@" + "depth20" + "/";
			//path = path + symbol + "@" + "kline_1m";
			
			//not just kline_1m, interval can be fixed
			path = path + symbol + "@kline_" + kline_a_interval;
			if (kline_a_interval != kline_b_interval)
				path = path + "/" + symbol + "@kline_" + kline_b_interval;
		}
		else {
			path_d = path_d + symbol + "@" + "markPrice" + "/";
			path_d = path_d + symbol + "@" + "aggTrade" + "/";
			path_d = path_d + symbol + "@" + "depth20" + "/";
			//path_d = path_d + symbol + "@" + "kline_1m";

			//not just kline_1m, interval can be fixed
			path_d = path_d + symbol + "@kline_" + kline_a_interval;
			if (kline_a_interval != kline_b_interval)
				path_d = path_d + "/" + symbol + "@kline_" + kline_b_interval;
		}


        map_itr++;
		if (map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
			if(!is_coin_m_futures(symbol))
				path += "/";
			else
				path_d += "/";
		}
		else {
			if (path_d.back() == '/')
				path_d = path_d.substr(0, path_d.size() - 1);
			if (path.back() == '/')
				path = path.substr(0, path.size() - 1);
			break;
		}
    }
    //connect_lws(path);
	KF_LOG_INFO(logger, "path " << path);
    connect_lws(path, false);
	KF_LOG_INFO(logger, "path_d,not used in binancef " << path_d);

   	KF_LOG_INFO(logger, "MDEngineBinanceF::login:"); 	

   	logged_in = true;
	is_logout = false;
}

bool MDEngineBinanceF::is_coin_m_futures(std::string symbol)
{//like "btcusd_200326" or "btcusd_perp"
	if ((symbol.find("usd_") != -1) || (symbol.find("USD_") != -1))
		return true;
	else
		return false;
}

void MDEngineBinanceF::get_symbol_and_event(std::string temp,std::vector<pair>& vec)
{
	KF_LOG_INFO(logger, "MDEngineBinanceF::get_symbol_and_event");
	int pos=1,pos1,pos2;
	std::string str1,str2;
	pos2 = temp.find("/");
	if(pos2 == -1)
	{
		struct pair p1;
		pos1 = temp.find("@",pos);
		str1 = temp.substr(pos-1, pos1-pos+1);
		str2 = temp.substr(pos1+1,temp.size()-1-pos1);
		p1.symbol = str1;
		p1.event_type = str2;
		vec.push_back(p1);
	}
	else
	{
		bool flag = true;
		while(flag)
		{
			struct pair p2;
		   	pos1 = temp.find("@",pos);
	   		pos2 = temp.find("/",pos1+1);
	   		if(pos2 == -1)
   			{
			   pos2 = temp.size();
			   flag = false;
  		 	}	
    		str1 = temp.substr(pos-1,pos1-pos+1);
   			str2 = temp.substr(pos1+1,pos2-pos1-1);
   			p2.symbol = str1;
			p2.event_type = str2;
			vec.push_back(p2);
			pos = pos2 + 2;
		}
	}
	KF_LOG_INFO(logger, "MDEngineBinanceF::get_symbol_and_event end");
}

//mofified, is_coin_m decided which api connectting
void MDEngineBinanceF::connect_lws(std::string temp, bool is_coin_m)
{
	KF_LOG_INFO(logger, "MDEngineBinanceF::connect_lws");
    std::string path("/stream?streams=");
    std::string old_path = path;

    std::vector<pair> vec;
	get_symbol_and_event(temp,vec);
	KF_LOG_INFO(logger, "connect_lws vec.size " << vec.size());

	vector<pair>::iterator i;
	i = vec.begin();
	while(i != vec.end())
	{
		if(i->event_type == "aggTrade")
		{
			path = path + i->symbol + "@" + i->event_type;
			old_path = old_path + i->symbol + "@" + i->event_type;

		}
		else if(i->event_type == "depth20")
		{
			path = path + i->symbol + "@" + i->event_type + "@0ms";
			old_path = old_path + i->symbol + "@" + i->event_type;
		}
		else if(i->event_type.find("kline_") != -1)
		{
			path = path + i->symbol + "@" + i->event_type;
			old_path = old_path + i->symbol + "@" + i->event_type;
		}
		else if(i->event_type == "markPrice")
		{
			path = path + i->symbol + "@" + i->event_type;
			old_path = old_path + i->symbol + "@" + i->event_type;
		}
		else
		{
			KF_LOG_ERROR(logger, "invalid event type");
			return;				
		}
		i++;
		if(i != vec.end())
			path += "/";
	}
	KF_LOG_INFO_FMT(logger, "path is %s", path.c_str());

	
	struct lws_client_connect_info ccinfo = {0};
	ccinfo.context = context;
	//modified
	if (is_coin_m)
		ccinfo.address = "dstream.binance.com";
	else
		ccinfo.address = "fstream.binance.com";
	ccinfo.host = lws_canonical_hostname(context);
	ccinfo.port 	= 443;
	ccinfo.path 	= path.c_str();
	ccinfo.origin 	= "origin";
	ccinfo.protocol = protocols[0].name;
	ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

	struct lws* conn = lws_client_connect_via_info(&ccinfo);
	KF_LOG_INFO_FMT(logger, "create a lws connection at %lu", reinterpret_cast<uint64_t>(conn));
	lws_handle_map[conn] = old_path.substr(16,old_path.size()-16);
	lws_reconnect_map[conn] = temp;
	lws_connect_to_dstream_map[conn] = is_coin_m;
}

void MDEngineBinanceF::on_lws_data(const char* data, size_t len)
{
    std::string str_data = data;
	KF_LOG_DEBUG(logger, "[on_lws_data] data is:" << str_data);

	if(str_data.find("aggTrade") != -1)
		on_lws_market_trade(data, len);
	else if(str_data.find("depth20") != -1)
		on_lws_book_update(data, len);
	else if(str_data.find("kline_") != -1)
		on_lws_kline(data, len);
	else if(str_data.find("markPriceUpdate") != -1)
		on_lws_funding(data, len);
}
//fixed
void MDEngineBinanceF::on_lws_connection_error(struct lws* conn)
{
	KF_LOG_INFO(logger, "on_lws_connection_error");
	auto iter = lws_handle_map.find(conn);
	//modified, is_coin_m
	auto iter_d = lws_connect_to_dstream_map.find(conn);
	if(iter != lws_handle_map.end() && iter_d != lws_connect_to_dstream_map.end())
	{
		KF_LOG_ERROR_FMT(logger, "lws connection %lu has broken ," ,reinterpret_cast<uint64_t>(conn));
		
		connect_lws(iter->second, iter_d->second);
		lws_handle_map.erase(iter);
		lws_connect_to_dstream_map.erase(iter_d);
	}
	else {
		if (iter == lws_handle_map.end()) {
			KF_LOG_INFO(logger, "on_lws_connection_error, not found in lws_handle_map");
		}
			
		if (iter_d == lws_connect_to_dstream_map.end()) {
			KF_LOG_INFO(logger, "on_lws_connection_error, not found in lws_connect_to_dstream_map");
			string path = iter->second;
			if(path.find("perp") != -1 || path.find("usd_") != -1)
				connect_lws(iter->second, true);
			else
				connect_lws(iter->second, false);
		}
	}
}

void MDEngineBinanceF::on_lws_connection_destroy(lws* conn, lws_callback_reasons reason)
{
	KF_LOG_INFO(logger, "on_lws_connection_destroy");
	if (isRunning && !is_logout) {
		KF_LOG_INFO(logger, "on_lws_connection_destroy, reason:" << reason);

		//ws_reconnect_mutex
		std::unique_lock<std::mutex> lck(ws_reconnect_mutex);

		auto iter = lws_reconnect_map.find(conn);
		//modified, is_coin_m
		auto iter_d = lws_connect_to_dstream_map.find(conn);
		if (iter != lws_reconnect_map.end() && iter_d != lws_connect_to_dstream_map.end())
		{
			KF_LOG_ERROR_FMT(logger, "lws connection %lu has broken ,", reinterpret_cast<uint64_t>(conn));

			connect_lws(iter->second, iter_d->second);
			lws_reconnect_map.erase(iter);
			lws_connect_to_dstream_map.erase(iter_d);
		}

		else if (iter == lws_reconnect_map.end())
			KF_LOG_INFO(logger, "on_lws_connection_destroy, not found in lws_reconnect_map");

		else if (iter != lws_reconnect_map.end() && iter_d == lws_connect_to_dstream_map.end()) {
			KF_LOG_INFO(logger, "on_lws_connection_destroy, not found in lws_connect_to_dstream_map");
			string path = iter->second;
			if (path.find("perp") != -1 || path.find("usd_") != -1)
				connect_lws(iter->second, true);
			else
				connect_lws(iter->second, false);
			lws_reconnect_map.erase(iter);
		}
		lck.unlock();
	}
}

void MDEngineBinanceF::on_lws_funding(const char* data, size_t len)
{
    KF_LOG_INFO(logger, "processing funding data");
    Document json;
    json.Parse(data);

    if(!json.HasMember("data"))
    {
        KF_LOG_INFO(logger, "received funding does not have valid data");
        return;
    }

    auto& node = json["data"];
    
    std::string symbol = node["s"].GetString();
    std::string ticker = coinPairWhiteList.GetKeyByValue(symbol);
    if(ticker.empty())
    {
        KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_funding: not in WhiteList , ignore it:" << symbol);
        return;
    }
    KF_LOG_INFO(logger, "received funding symbol is " << symbol << " and ticker is " << ticker);
    
    
    LFFundingField fundingdata;
    memset(&fundingdata, 0, sizeof(fundingdata));
    strcpy(fundingdata.InstrumentID, ticker.c_str());
    strcpy(fundingdata.ExchangeID, "Binance Futures");
	//fix, 资金费率，对非永续合约显示""
	std::string rate = node["r"].GetString();
	if (rate.length() == 0)
		fundingdata.Rate = 0;
	else
		fundingdata.Rate = stod(rate);
    //fundingdata.Rate = stod(node["r"].GetString());
    fundingdata.TimeStamp = node["E"].GetInt64();
    KF_LOG_INFO(logger,"Rate:"<<fundingdata.Rate);
    
    on_funding_update(&fundingdata);

    LFMarkPrice markprice;
    memset(&markprice, 0, sizeof(markprice));
    strcpy(markprice.InstrumentID, ticker.c_str());

    markprice.MarkPrice = std::round(stod(node["p"].GetString()) * scale_offset);

    on_markprice(&markprice);
}

void MDEngineBinanceF::on_lws_market_trade(const char* data, size_t len)
{
	//{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1572252826003,"a":3615797,"s":"BTCUSDT","p":"9371.01","q":"0.200","f":4700165,"l":4700165,"T":1572252826003,"m":true}}
	
	KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_market_trade:" << data);
    Document d;
    d.Parse(data);

	LFL2TradeField trade;
	memset(&trade, 0, sizeof(trade));

	auto& real_data = d["data"];
	
	if(!real_data.HasMember("s") || !real_data.HasMember("p") || !real_data.HasMember("q") || !real_data.HasMember("m"))
	{
		KF_LOG_ERROR(logger, "invalid market trade message");
		return;	
	}

	std::string symbol = real_data["s"].GetString();
	std::string ticker = coinPairWhiteList.GetKeyByValue(symbol);
    if(ticker.length() == 0) {
        KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_market_trade: not in WhiteList , ignore it:" << symbol);
        return;
    }

	strcpy(trade.InstrumentID, ticker.c_str());
	strcpy(trade.ExchangeID, "Binance Futures");
	trade.TimeStamp = real_data["T"].GetInt64()*1000000;
	auto strTime = timestamp_to_formatISO8601(real_data["T"].GetInt64());
	strcpy(trade.TradeTime,strTime.c_str()); 
	strcpy(trade.TradeTime,std::to_string(real_data["T"].GetInt64()).c_str());

	strcpy(trade.TradeID, std::to_string(real_data["l"].GetInt()).c_str());

	trade.Price = std::round(std::stod(real_data["p"].GetString()) * scale_offset);
	trade.Volume = std::round(std::stod(real_data["q"].GetString()) * scale_offset);
	//"m": true,        // Is the buyer the market maker?
	trade.OrderBSFlag[0] = real_data["m"].GetBool() ? 'B' : 'S';
    //timer[0] = getTimestamp();
	std::unique_lock<std::mutex> lck(trade_mutex);
	auto it = control_trade_map.find(ticker);
	if(it != control_trade_map.end())
		it->second = getTimestamp();

	lck.unlock();

	//edit begin
	if (need_another_kline) {
		KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_market_trade: writing klinedata" << symbol);
		KF_LOG_INFO(logger, "ts=" << trade.TimeStamp);
		KlineData klinedata;
		klinedata.ts = trade.TimeStamp;
		klinedata.price = trade.Price;
		klinedata.volume = trade.Volume;
		//klinedata.TradingDay = getdate();
		klinedata.InstrumentID = trade.InstrumentID;
		klinedata.ExchangeID = "binance future";

		std::unique_lock<std::mutex> lock(kline_mutex2);
		//kline_receive_vec.push_back(klinedata);
		kline_receive_list.push_back(klinedata);
		lock.unlock();
		KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_market_trade: writing klinedata end" << symbol);
	}
	//edit end

	on_trade(&trade);

	KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_market_trade: processed successfully");
}

void MDEngineBinanceF::on_lws_book_update(const char* data, size_t len)
{	
	KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_book_update:" << data);
    Document d;
    d.Parse(data);

	LFPriceBook20Field md;
	memset(&md, 0, sizeof(md));

	auto& real_data = d["data"];
	uint64_t sequence;
	if(real_data.HasMember("u")){
		sequence = real_data["u"].GetUint64();
	}
    bool has_update = false;	    	
	if(real_data.HasMember("b"))
	{
		auto& bids = real_data["b"];

		if(bids.IsArray() && bids.Size() >0)
		{
			auto size = std::min((int)bids.Size(), 20);
		
			for(int i = 0; i < size; ++i)
			{
				//CYS add std::round
                md.BidLevels[i].price = std::round(stod(bids.GetArray()[i][0].GetString()) * scale_offset);
				md.BidLevels[i].volume = std::round(stod(bids.GetArray()[i][1].GetString()) * scale_offset);
			}
			md.BidLevelCount = size;

			has_update = true;
		}
	}

	if(real_data.HasMember("a"))
	{
		auto& asks = real_data["a"];

		if(asks.IsArray() && asks.Size() >0)
		{
			auto size = std::min((int)asks.Size(), 20);
		
			for(int i = 0; i < size; ++i)
			{
				//CYS edit std::round
				md.AskLevels[i].price = std::round(stod(asks.GetArray()[i][0].GetString()) * scale_offset);
				md.AskLevels[i].volume = std::round(stod(asks.GetArray()[i][1].GetString()) * scale_offset);
			}
			md.AskLevelCount = size;

			has_update = true;
		}
	}	
    
    if(has_update)
    {
    	std::string symbol = real_data["s"].GetString();
        std::string strategy_ticker = coinPairWhiteList.GetKeyByValue(symbol);
        if(strategy_ticker.length() == 0) {
            KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_book_update: not in WhiteList , ignore it:" << symbol);
            return;
        }

        strcpy(md.InstrumentID, strategy_ticker.c_str());
	    strcpy(md.ExchangeID, "Binance Futures");

	    md.UpdateMicroSecond = real_data["T"].GetUint64();
		//modified
		int64_t timeNow = getTimestamp();
		if (timeNow - (int64_t)md.UpdateMicroSecond > orderbook_latency_threshold_exceeded_ms) {
			KF_LOG_INFO(logger, "timeNow: " << timeNow << " md.UpdateMicroSecond: " << md.UpdateMicroSecond);
			string errMsg = "md orderbook latency threshold exceeded";
			int errId = 117;
			if (need_restart)
				KF_LOG_INFO(logger, "need restart ");
			else {
				KF_LOG_INFO(logger, "just send mail");
				errId = 17;
			}
			KF_LOG_INFO(logger, "errId: " << errId << " errMsg: " << errMsg);
			write_errormsg(errId, errMsg);
		}
     
        std::unique_lock<std::mutex> lck1(book_mutex);
		auto it = control_book_map.find(strategy_ticker);
		if(it != control_book_map.end())
		{
			it->second = getTimestamp();
		}
		lck1.unlock();
        //timer[1] = getTimestamp();

        bool flag = false;
        int i;
        for(i = 0;i< md.BidLevelCount;i++)
        {
        	if(md.BidLevels[i].price <= 0)
        		flag = true;
        }
        for(i = 0;i< md.AskLevelCount;i++)
        {
        	if(md.AskLevels[i].price <= 0)
        		flag = true;
        }

        string errorMsg;
        if (md.BidLevels[0].price > md.AskLevels[0].price)
        {
        	errorMsg = "failed ,orderbook crossed";
        	write_errormsg(113,errorMsg);
        }
        else if(flag == true)
        {
         	errorMsg = "failed ,price less than or equal to zero";
        	write_errormsg(113,errorMsg);       	
        }
        else if(md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold)
        {
        	errorMsg = "failed ,levelcount less than threshold";
        	write_errormsg(112,errorMsg);
        }
        else{ 
        	int64_t now = getTimestamp();
        	std::unique_lock<std::mutex> lck_ws_book(ws_book_mutex);
        	auto itr = ws_book_map.find(strategy_ticker);
        	if(itr == ws_book_map.end()){
        		std::map<uint64_t, int64_t> bookmsg_map;
        		bookmsg_map.insert(std::make_pair(sequence, now));
        		ws_book_map.insert(std::make_pair(strategy_ticker, bookmsg_map));
        	}else{
        		itr->second.insert(std::make_pair(sequence, now));
        	}
        	lck_ws_book.unlock();

        	on_price_book_update(&md);
        }
	}
	KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_book_update: processed successfully");
}

void MDEngineBinanceF::on_lws_kline(const char* src, size_t len)
{
	KF_LOG_INFO(logger, "processing 1-min trade bins data" << src);


	Document temp_json;
	temp_json.Parse(src);
 	rapidjson::Value& json = temp_json["data"];



    if(!json.HasMember("s") || !json.HasMember("k"))
    {
        KF_LOG_INFO(logger, "received 1-min trade bin does not have valid data");
        return;
    }  
    std::string symbol = json["s"].GetString();
    std::string ticker = coinPairWhiteList.GetKeyByValue(symbol);
    if(ticker.empty())
    {
        KF_LOG_INFO(logger, "received 1-min trade bin symbol not in white list");
        return;
    }
    //KF_LOG_INFO(logger, "received 1-min trade bin symbol is " << symbol << " and ticker is " << ticker);
 	auto& data = json["k"];
	//if(data["x"].GetBool())
	if (data["x"].GetBool() || !only_receive_complete_kline)
	{
		LFBarMarketDataField market;
		memset(&market, 0, sizeof(market));
		strcpy(market.InstrumentID, ticker.c_str());
		strcpy(market.ExchangeID, "Binance Futures");

		struct tm cur_tm, start_tm, end_tm;
		time_t now = time(0);
		cur_tm = *localtime(&now);
		strftime(market.TradingDay, 9, "%Y%m%d", &cur_tm);
		
		int64_t nStartTime = data["t"].GetInt64();
		int64_t nEndTime = data["T"].GetInt64();
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

		//edit here
		//market.PeriodMillisec = 60000;
		market.PeriodMillisec = nEndTime - nStartTime;
		market.PeriodMillisec = (market.PeriodMillisec + 1) * 1000;

		//CYS edit std::round
		market.Open = std::round(std::stod(data["o"].GetString()) * scale_offset);
		market.Close = std::round(std::stod(data["c"].GetString()) * scale_offset);
		market.Low = std::round(std::stod(data["l"].GetString()) * scale_offset);
		market.High = std::round(std::stod(data["h"].GetString()) * scale_offset);		
		market.Volume = std::round(std::stod(data["v"].GetString()) * scale_offset);
		//market.IsComplete = data["x"].GetBool();
	
		KF_LOG_DEBUG(logger,"[on_lws_kline] open: "<<market.Open <<" close: "<<market.Close<<" low: "<<market.Low <<" high: "<< market.High<<" volume "<<market.Volume);
        //timer[2] = getTimestamp();/*quest3 edited by fxw*/
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

void MDEngineBinanceF::set_reader_thread()
{
	IMDEngine::set_reader_thread();

   	ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBinanceF::loop, this)));
	if (need_get_snapshot_via_rest)
		rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBinanceF::rest_loop, this)));
	else
		KF_LOG_INFO(logger, "MDEngineBinanceF::set_reader_thread, won't start rest_loop");
	//edit here
	if(need_another_kline)
		kline_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBinanceF::klineloop, this)));
}

//edit begin
//int bar_duration_s = 60;
//int bar_duration_s = 5;
void MDEngineBinanceF::klineloop()
{
	bool first_kline = true;
	int64_t last_time = time(0);
	KF_LOG_INFO(logger, "start=" << last_time);

	//int64_t rest = 5;
	while (isRunning) {
		last_time = time(0);
		//if ((getTimestamp() / 100) % (bar_duration_s * 10) == rest) {
		if ((getTimestamp() % (bar_duration_s * 1000)) == bar_wait_time_ms) {
			KF_LOG_INFO(logger, "last_time=" << last_time);
			KF_LOG_INFO(logger, "klineloop getTimestamp=" << getTimestamp());
			control_kline(first_kline, last_time);
			first_kline = false;
			break;
		}
	}
	if (!first_kline) {
		KF_LOG_INFO(logger, "!first_kline");
		while (isRunning) {
			int64_t now = time(0);
			//if ((getTimestamp() / 100 - last_time * 10 - rest) >= (bar_duration_s * 10)) {
			if ((getTimestamp() - last_time * 1000 - bar_wait_time_ms) >= (bar_duration_s * 1000)) {
				last_time = now;
				KF_LOG_INFO(logger, "now1=" << now);
				KF_LOG_INFO(logger, "klineloop getTimestamp=" << getTimestamp());
				control_kline(first_kline, now);
			}
		}
	}
}


void MDEngineBinanceF::control_kline(bool first_kline, int64_t time)
{
	KF_LOG_INFO(logger, "control_kline");
	std::unique_lock<std::mutex> lock(kline_mutex2);

	//int size = kline_receive_vec.size();
	int size = kline_receive_list.size();
	if (size == 0 && !first_kline) {
		KF_LOG_INFO(logger, "receive no kline");
		if (kline_hand_map.size() > 0) {
			handle_kline(time);
		}
	}
	else if (size > 0) {
		KF_LOG_INFO(logger, "update kline");
		bool update = false;

		for (auto it = kline_receive_list.begin(); it != kline_receive_list.end();) {
			if (it->ts < (time - bar_duration_s) * 1e9) {
				KF_LOG_INFO(logger, "data in kline_receive_list is overtime");
				kline_receive_list.erase(it++);
			}
			else {
				if (it->ts >= (time - bar_duration_s) * 1e9 && it->ts < time * 1e9) {
					update = true;
					auto it1 = kline_hand_map.find(it->InstrumentID);
					if (it1 != kline_hand_map.end()) {
						kline_hand_map.erase(it1);
					}
				}
				it++;
			}
		}

		for (auto it = kline_receive_list.begin(); it != kline_receive_list.end();) {
			//for (it = kline_receive_vec.begin(); it != kline_receive_vec.end();) {
			int64_t tradetime = it->ts;
			if (tradetime >= (time - bar_duration_s) * 1e9 && tradetime < time * 1e9) {
				/*auto it1 = kline_hand_map.find(it->InstrumentID);
				if(it1 != kline_hand_map.end()){
					kline_hand_map.erase(it1);
				}
				update = true;*/
				auto itr = kline_hand_map.find(it->InstrumentID);
				if (itr == kline_hand_map.end()) {
					KlineData klinedata;
					klinedata.TradingDay = getdate(time - bar_duration_s);
					klinedata.InstrumentID = it->InstrumentID;
					klinedata.ExchangeID = "binancef";
					klinedata.StartUpdateTime = gettime(time - bar_duration_s);
					klinedata.StartUpdateMillisec = (time - bar_duration_s) * 1000;
					klinedata.EndUpdateTime = gettime(time);
					klinedata.EndUpdateMillisec = time * 1000 - 1;
					klinedata.PeriodMillisec = bar_duration_s * 1000;
					klinedata.Open = it->price;
					klinedata.Close = it->price;
					klinedata.Low = it->price;
					klinedata.High = it->price;
					klinedata.Volume = it->volume;
					kline_hand_map.insert(make_pair(it->InstrumentID, klinedata));
				}
				else {
					//KF_LOG_INFO(logger,"else");
					//itr->second.EndUpdateTime = it->EndUpdateTime;
					//itr->second.EndUpdateMillisec = it->EndUpdateMillisec;
					itr->second.Close = it->price;
					if (it->price < itr->second.Low) {
						//KF_LOG_INFO(logger,"low");
						itr->second.Low = it->price;
					}
					else if (it->price > itr->second.High) {
						//KF_LOG_INFO(logger,"high");
						itr->second.High = it->price;
					}
					itr->second.Volume += it->volume;
				}
				kline_receive_list.erase(it++);
				//it = kline_receive_vec.erase(it);
			}
			else {
				it++;
			}
		}
		if (update) {
			handle_kline(time);
		}
		else {
			if (kline_hand_map.size() > 0) {
				handle_kline(time);
			}
			//write_errormsg(114,"kline max refresh wait time exceeded");            
		}
	}
	lock.unlock();
}


void MDEngineBinanceF::handle_kline(int64_t time)
{
	KF_LOG_INFO(logger, "handle_kline");
	std::map<std::string, KlineData>::iterator it;
	for (it = kline_hand_map.begin(); it != kline_hand_map.end(); it++) {
		KF_LOG_INFO(logger, "it in");
		LFBarMarketDataField market;
		memset(&market, 0, sizeof(market));
		strcpy(market.InstrumentID, it->second.InstrumentID.c_str());
		strcpy(market.ExchangeID, "binancef");

		std::string date = getdate(time - bar_duration_s);
		std::string starttime = gettime(time - bar_duration_s);
		int64_t startmsec = (time - bar_duration_s) * 1000;
		std::string endtime = gettime(time);
		int64_t endmsec = time * 1000 - 1;
		strcpy(market.TradingDay, date.c_str());
		strcpy(market.StartUpdateTime, starttime.c_str());
		strcpy(market.EndUpdateTime, endtime.c_str());

		market.StartUpdateMillisec = startmsec;
		market.EndUpdateMillisec = endmsec;
		market.PeriodMillisec = it->second.PeriodMillisec;
		market.Open = it->second.Open;
		market.Close = it->second.Close;
		market.Low = it->second.Low;
		market.High = it->second.High;
		market.Volume = it->second.Volume;
		KF_LOG_INFO(logger, "market=" << market.StartUpdateMillisec << " " << market.EndUpdateMillisec << " " << market.PeriodMillisec);
		KF_LOG_INFO(logger, market.Open << " " << market.Close << " " << market.Low << " " << market.High << " " << market.Volume);
		KF_LOG_INFO(logger, std::string(market.TradingDay) << " " << std::string(market.StartUpdateTime));
		//last_market = market;
		on_market_bar_data(&market);
		//it = kline_hand_map.erase(it);
		//KF_LOG_INFO(logger,"erase it");
	}
	KF_LOG_INFO(logger, "handle_kline end");
}

std::string MDEngineBinanceF::gettime(int64_t time)
{
	time_t now = time;
	tm* ltm = localtime(&now);
	std::string hour = std::to_string(ltm->tm_hour);
	hour = dealzero(hour);
	std::string min = std::to_string(ltm->tm_min);
	min = dealzero(min);
	std::string sec = std::to_string(ltm->tm_sec);
	sec = dealzero(sec);
	std::string timestr = hour + ":" + min + ":" + sec;
	return timestr;
}

std::string MDEngineBinanceF::dealzero(std::string time)
{
	if (time.length() == 1) {
		time = "0" + time;
	}
	return time;
}

void MDEngineBinanceF::get_kline_via_rest(string symbol, string interval, bool ignore_startTime, int64_t startTime, int64_t endTime, int limit)
{
	std::string ticker = coinPairWhiteList.GetKeyByValue(symbol);
	if (ticker.empty())
	{
		KF_LOG_INFO(logger, "symbol not in white list");
		return;
	}

	const auto static url = "https://api.binance.com/api/v3/klines";
	cpr::Response response;
	if (ignore_startTime)
		response = Get(Url{ url }, Parameters{ {"symbol", symbol},{"interval", interval},{"limit", to_string(limit)} });
	else
		response = Get(Url{ url }, Parameters{ {"symbol", symbol},{"interval", interval},
			{"startTime", to_string(startTime)}, {"endTime", to_string(endTime)}, {"limit", to_string(limit)} });
	Document d;
	d.Parse(response.text.c_str());
	if (d.IsArray()) {
		for (int i = 0; i < d.Size(); i++) {
			if (!d[i].IsArray()) {
				KF_LOG_INFO(logger, "MDEngineBinance::get_kline_via_rest: response is abnormal");
			}
			LFBarMarketDataField market;
			memset(&market, 0, sizeof(market));
			strcpy(market.InstrumentID, ticker.c_str());
			strcpy(market.ExchangeID, "binance");

			struct tm cur_tm, start_tm, end_tm;
			//time_t now = time(0);
			//cur_tm = *localtime(&now);
			//strftime(market.TradingDay, 9, "%Y%m%d", &cur_tm);

			int64_t nStartTime = d[i][0].GetInt64();
			int64_t nEndTime = d[i][6].GetInt64();
			market.StartUpdateMillisec = nStartTime;
			int ms = nStartTime % 1000;
			nStartTime /= 1000;
			start_tm = *localtime((time_t*)(&nStartTime));
			sprintf(market.StartUpdateTime, "%02d:%02d:%02d.%03d", start_tm.tm_hour, start_tm.tm_min, start_tm.tm_sec, ms);
			market.EndUpdateMillisec = nEndTime;
			ms = nEndTime % 1000;
			nEndTime /= 1000;
			end_tm = *localtime((time_t*)(&nEndTime));
			sprintf(market.EndUpdateTime, "%02d:%02d:%02d.%03d", end_tm.tm_hour, end_tm.tm_min, end_tm.tm_sec, ms);

			market.PeriodMillisec = nEndTime - nStartTime;
			market.PeriodMillisec = (market.PeriodMillisec + 1);

			market.Open = std::round(std::stod(d[i][1].GetString()) * scale_offset);
			market.Close = std::round(std::stod(d[i][4].GetString()) * scale_offset);
			market.Low = std::round(std::stod(d[i][3].GetString()) * scale_offset);
			market.High = std::round(std::stod(d[i][2].GetString()) * scale_offset);
			market.Volume = std::round(std::stod(d[i][5].GetString()) * scale_offset);

			on_market_bar_data(&market);
		}
	}
}

string MDEngineBinanceF::getIntervalStr(int64_t sec)
{
	switch (sec) {
	case 60: 
		return "1m";
	case 180:
		return "3m";
	case 300:
		return "5m";
	case 900:
		return "15m";
	case 1800:
		return "30m";
	case 3600:
		return "1h";
	case 7200:
		return "2h";
	case 14400:
		return "4h";
	case 21600:
		return "6h";
	case 28800:
		return "8h";
	case 43200:
		return "12h";
	case 86400:
		return "1d";
	case 259200:
		return "3d";
	case 604800:
		return "1w";
	case 2678400:
		return "1M";
	}
}

std::string MDEngineBinanceF::getdate(int64_t time)
{
	time_t now = time;
	tm* ltm = localtime(&now);
	std::string year = std::to_string(1900 + ltm->tm_year);
	std::string month = std::to_string(1 + ltm->tm_mon);
	month = dealzero(month);
	std::string date = std::to_string(ltm->tm_mday);
	date = dealzero(date);
	std::string datestr = year + month + date;
	return datestr;
}
//edit end

void MDEngineBinanceF::logout()
{
	is_logout = true;
	lws_context_destroy( context );
   	KF_LOG_INFO(logger, "MDEngineBinanceF::logout:"); 	
}

void MDEngineBinanceF::release_api()
{
   KF_LOG_INFO(logger, "MDEngineBinanceF::release_api:"); 	
}

void MDEngineBinanceF::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
	/* Connect if we are not connected to the server. */
   	KF_LOG_INFO(logger, "MDEngineBinanceF::subscribeMarketData:"); 	
}

void MDEngineBinanceF::get_snapshot_via_rest()
{
    //while (isRunning)
    //{
	std::unordered_map<std::string, std::string>::iterator map_itr;
	for(map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); map_itr++)
        {
            std::string url = "https://fapi.binance.com/fapi/v1/depth?limit=20&symbol=";
			//modified
			if(is_coin_m_futures(map_itr->second))
				url = "https://dapi.binance.com/dapi/v1/depth?limit=20&symbol=";
            url+=map_itr->second;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            
            if(d.IsObject() && d.HasMember("lastUpdateId"))
            {
            	uint64_t sequence = d["lastUpdateId"].GetUint64();

                LFPriceBook20Field priceBook {0};
                strcpy(priceBook.ExchangeID, "Binance Futures");
                strncpy(priceBook.InstrumentID, map_itr->first.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, map_itr->first.size()));
                if(d.HasMember("bids") && d["bids"].IsArray())
                {
                    auto& bids = d["bids"];
                    int len = std::min((int)bids.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.BidLevels[i].price = std::round(stod(bids[i][0].GetString()) * scale_offset);
                        priceBook.BidLevels[i].volume = std::round(stod(bids[i][1].GetString()) * scale_offset);
                    }
                    priceBook.BidLevelCount = len;
                }
                if (d.HasMember("asks") && d["asks"].IsArray())
                {
                    auto& asks = d["asks"];
                    int len = std::min((int)asks.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.AskLevels[i].price = std::round(stod(asks[i][0].GetString()) * scale_offset);
                        priceBook.AskLevels[i].volume = std::round(stod(asks[i][1].GetString()) * scale_offset);
                    }
                    priceBook.AskLevelCount = len;
                }

            	BookMsg bookmsg;
            	bookmsg.InstrumentID = map_itr->first;
            	bookmsg.sequence = sequence;
            	bookmsg.time = getTimestamp();
            	std::unique_lock<std::mutex> lck_rest_book(rest_book_mutex);
            	rest_book_vec.push_back(bookmsg);
            	lck_rest_book.unlock();

            	on_price_book_update_from_rest(&priceBook);
            }
        }
    //}
}

void MDEngineBinanceF::check_snapshot()
{
    std::vector<BookMsg>::iterator rest_it;
    std::unique_lock<std::mutex> lck_rest_book(rest_book_mutex);
    for(rest_it = rest_book_vec.begin();rest_it != rest_book_vec.end();){
        int64_t now = getTimestamp();
        std::unique_lock<std::mutex> lck_ws_book(ws_book_mutex);
        auto map_itr = ws_book_map.find(rest_it->InstrumentID);
        if(map_itr != ws_book_map.end()){
            std::map<uint64_t, int64_t>::iterator ws_it;
            for(ws_it = map_itr->second.begin(); ws_it != map_itr->second.end();){
            	//if(now - ws_it->second > 10000){
            	if(ws_it->first < rest_it->sequence){
            		KF_LOG_INFO(logger,"erase old");
            		ws_it = map_itr->second.erase(ws_it);
            		continue;
            	}else if(ws_it->first == rest_it->sequence){
	            	if(ws_it->second - rest_it->time > snapshot_check_s * 1000){
	                    KF_LOG_INFO(logger, "ws snapshot is later than rest snapshot");
	                    string errorMsg = "ws snapshot is later than rest snapshot";
	                    write_errormsg(115,errorMsg);
	            	}
	                KF_LOG_INFO(logger, "same_book:"<<rest_it->InstrumentID);
	                KF_LOG_INFO(logger,"ws_time="<<ws_it->second<<" rest_time="<<rest_it->time);            		
            	}
            	ws_it++;
            }
        }
        lck_ws_book.unlock();
        rest_it = rest_book_vec.erase(rest_it);
    }
    lck_rest_book.unlock();
}

int64_t last_rest_time = 0;
void MDEngineBinanceF::rest_loop()
{
        while(isRunning)
        {
            int64_t now = getTimestamp();
            if((now - last_rest_time) >= rest_get_interval_ms)
            {
                last_rest_time = now;
                get_snapshot_via_rest();
                check_snapshot();
            }
        }
}


void MDEngineBinanceF::loop()
{
	while (isRunning)
	{
		int errorId = 0;
		string errorMsg = "";
		/*判断是否在设定时间内更新与否，*/
		int64_t now = getTimestamp();

		std::unique_lock<std::mutex> lck(trade_mutex);
		std::map<std::string, int64_t>::iterator it;
		for (it = control_trade_map.begin(); it != control_trade_map.end(); it++)
		{
			if ((now - it->second) > refresh_normal_check_trade_s * 1000)
			{
				errorId = 115;
				errorMsg = it->first + " trade max refresh wait time exceeded";
				KF_LOG_INFO(logger, "115" << errorMsg << " (now)" << now << " (it->second)" << it->second);
				write_errormsg(errorId, errorMsg);
				it->second = now;
			}
		}
		lck.unlock();

		std::unique_lock<std::mutex> lck1(book_mutex);
		std::map<std::string, int64_t>::iterator it1;
		for (it1 = control_book_map.begin(); it1 != control_book_map.end(); it1++)
		{
			if ((now - it1->second) > refresh_normal_check_book_s * 1000)
			{
				errorId = 114;
				errorMsg = it1->first + " book max refresh wait time exceeded";
				KF_LOG_INFO(logger, "114" << errorMsg);
				write_errormsg(errorId, errorMsg);
				it1->second = now;
			}
		}
		lck1.unlock();

		std::unique_lock<std::mutex> lck2(kline_mutex);
		std::map<std::string, int64_t>::iterator it2;
		for (it2 = control_kline_map.begin(); it2 != control_kline_map.end(); it2++)
		{
			if ((now - it2->second) > refresh_normal_check_kline_s * 1000)
			{
				errorId = 116;
				errorMsg = it2->first + " kline max refresh wait time exceeded";
				KF_LOG_INFO(logger, "116" << errorMsg);
				write_errormsg(errorId, errorMsg);
				it2->second = now;
			}
		}
		lck2.unlock();

		lws_service(context, rest_get_interval_ms);
	}
}

inline int64_t MDEngineBinanceF::getTimestamp()
{   /*返回的是毫秒*/
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

BOOST_PYTHON_MODULE(libbinancefmd)
{
    using namespace boost::python;
    class_<MDEngineBinanceF, boost::shared_ptr<MDEngineBinanceF> >("Engine")
    .def(init<>())
    .def("init", &MDEngineBinanceF::initialize)
    .def("start", &MDEngineBinanceF::start)
    .def("stop", &MDEngineBinanceF::stop)
    .def("logout", &MDEngineBinanceF::logout)
    .def("wait_for_stop", &MDEngineBinanceF::wait_for_stop);
}