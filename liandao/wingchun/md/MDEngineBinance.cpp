#include "MDEngineBinance.h"
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

std::mutex trade_mutex;
std::mutex book_mutex;
std::mutex kline_mutex;
//add line here
std::mutex kline_mutex2;
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;

static MDEngineBinance* g_md_binance = nullptr;

static int lws_event_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{

    switch( reason )
    {   
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            lws_callback_on_writable( wsi );
            break;      
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {
            if(g_md_binance)
            {
                g_md_binance->on_lws_data(wsi, (const char*)in, len);
            }
            break;      
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {       
            break;      
        }       
        case LWS_CALLBACK_CLOSED:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
            if(g_md_binance)
            {
                g_md_binance->on_lws_connection_error(wsi);
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

MDEngineBinance::MDEngineBinance(): IMDEngine(SOURCE_BINANCE)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Binance");
    /*timer[0] = getTimestamp();quest3 edited by fxw
    timer[1] = timer[2] = timer[0];quest3 edited by fxw*/
}

void MDEngineBinance::load(const json& j_config)
{
    book_depth_count = j_config["book_depth_count"].get<int>();
    // level_threshold = j_config["level_threshold"].get<int>();//quest3 edited by fxw ,need edit the kungfu.json
    /*quest3v4 fxw starts*/
    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("refresh_normal_check_trade_s") != j_config.end()) {
        refresh_normal_check_trade_s = j_config["refresh_normal_check_trade_s"].get<int>();
    }
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    if(j_config.find("refresh_normal_check_kline_s") != j_config.end()) {
        refresh_normal_check_kline_s = j_config["refresh_normal_check_kline_s"].get<int>();
    }
    /*quest3v4 fxw ends*/
    trade_count = j_config["trade_count"].get<int>();

    //kline edit
    if (j_config.find("need_another_kline") != j_config.end())
        need_another_kline = j_config["need_another_kline"].get<bool>();
    if (j_config.find("need_get_snapshot_via_rest") != j_config.end())
        need_get_snapshot_via_rest = j_config["need_get_snapshot_via_rest"].get<bool>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();

    if(j_config.find("snapshot_check_s") != j_config.end()) {
        snapshot_check_s = j_config["snapshot_check_s"].get<int>();
    }
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

    if (j_config.find("kline_a_interval") != j_config.end())
        kline_a_interval = j_config["kline_a_interval"].get<string>();
    if (j_config.find("kline_b_interval") != j_config.end())
        kline_b_interval = j_config["kline_b_interval"].get<string>();

    if (j_config.find("kline_a_interval") != j_config.end())
        kline_a_interval = j_config["kline_a_interval"].get<string>();
    if (j_config.find("kline_b_interval") != j_config.end())
        kline_b_interval = j_config["kline_b_interval"].get<string>();

    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineBinance::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"BTCUSDT\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"ETCETH\"");
        KF_LOG_ERROR(logger, "},");
    }

    KF_LOG_INFO(logger, "MDEngineBinance::load:  book_depth_count: "
        << book_depth_count << " trade_count: " << trade_count << " rest_get_interval_ms: " << rest_get_interval_ms);     
    //write_errormsg(160,"test md binance");
    int64_t time = getTimestamp();
    std::unordered_map<std::string, std::string>::iterator it;
    for(it = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); it != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); it++){
        std::unique_lock<std::mutex> lck(trade_mutex);
        control_trade_map.insert(make_pair(it->first, time));
        lck.unlock();

        std::unique_lock<std::mutex> lck1(book_mutex);
        control_book_map.insert(make_pair(it->first, time));
        lck1.unlock();

        std::unique_lock<std::mutex> lck2(kline_mutex);
        control_kline_map.insert(make_pair(it->first, time));
        lck2.unlock();
    }
    //KF_LOG_INFO(logger,"control_trade_map="<<control_trade_map.size()<<" "<<control_book_map.size());
}

void MDEngineBinance::connect(long timeout_nsec)
{
   KF_LOG_INFO(logger, "MDEngineBinance::connect:");     
    
   connected = true;
}

void MDEngineBinance::login(long timeout_nsec)
{
    g_md_binance = this;

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
    while(map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_INFO(logger, "[debug_print] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) "<< map_itr->second);
        connect_lws(map_itr->second, lws_event::trade);
        //connect_lws(map_itr->second, lws_event::depth5);
        connect_lws(map_itr->second, lws_event::depth20);
        connect_lws(map_itr->second, lws_event::kline_a);
        if (kline_a_interval != kline_b_interval)
            connect_lws(map_itr->second, lws_event::kline_b);
        connect_lws(map_itr->second, lws_event::bookticker);
        map_itr++;
    }

       KF_LOG_INFO(logger, "MDEngineBinance::login:");     

       logged_in = true;
}

void MDEngineBinance::connect_lws(std::string symbol, lws_event e)
{
        std::string t = symbol;
        std::transform(t.begin(), t.end(), t.begin(), ::tolower);
        std::string path("/ws/");
        switch(e)
        {
                case trade:
                        path += t + "@trade";
                        break;
                case depth5:
                        path += t + "@depth5@100ms";
                        break;
                case depth20:
                        path += t + "@depth20@100ms";
                        break;
                case kline_a:
                    //interval can be fixed
                        path += t + "@kline_" + kline_a_interval;
                        break;
                case kline_b:
                    //interval can be fixed
                        path += t + "@kline_" + kline_b_interval;
                    break;
                case bookticker:
                        path += t + "@bookTicker";
                        break;
                default:
                        KF_LOG_ERROR(logger, "invalid lws event");
                        return;
        }

        struct lws_client_connect_info ccinfo = {0};
        ccinfo.context     = context;
        ccinfo.address     = "stream.binance.com";
        ccinfo.port     = 9443;
        ccinfo.path     = path.c_str();
        ccinfo.host     = lws_canonical_hostname( context );
        ccinfo.origin     = "origin";
        ccinfo.protocol = protocols[0].name;
        ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

        struct lws* conn = lws_client_connect_via_info(&ccinfo);
        KF_LOG_INFO_FMT(logger, "create a lws connection for %s %d at %lu", 
                        t.c_str(), static_cast<int>(e), reinterpret_cast<uint64_t>(conn));
        lws_handle_map[conn] = std::make_pair(symbol, e);
}

void MDEngineBinance::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    
    KF_LOG_INFO(logger, "MDEngineBinance::on_lws_data: (data)" << data);

    auto iter = lws_handle_map.find(conn);
    if(iter == lws_handle_map.end())
    {
        KF_LOG_ERROR_FMT(logger, "failed to find ticker and event from %lu", reinterpret_cast<uint64_t>(conn));
        return;
    }
    
    if(iter->second.second == lws_event::trade)
    {
        on_lws_market_trade(data, len);
    }
    else if(iter->second.second == lws_event::depth5 || iter->second.second == lws_event::depth20)
    {
        on_lws_book_update(data, len, iter->second.first);
    }
    else if(iter->second.second == lws_event::kline_a)
    {
        on_lws_kline(data,len);
    }
    else if (iter->second.second == lws_event::kline_b)
    {
        on_lws_kline(data, len);
    }
    else if(iter->second.second == lws_event::bookticker)
    {
        on_lws_ticker(data,len);
    }
}

void MDEngineBinance::on_lws_connection_error(struct lws* conn)
{
    auto iter = lws_handle_map.find(conn);
    if(iter != lws_handle_map.end())
    {
        KF_LOG_ERROR_FMT(logger, "lws connection broken for %s %d %lu, ", 
                    iter->second.first.c_str(), static_cast<int>(iter->second.second), reinterpret_cast<uint64_t>(conn));
        
        connect_lws(iter->second.first, iter->second.second);
        lws_handle_map.erase(iter);
    }
}

std::string TimeToFormatISO8601(int64_t timestamp)
{
    int ms = timestamp % 1000;
    tm utc_time{};
    time_t time = timestamp/1000;
    gmtime_r(&time, &utc_time);
    char timeStr[50]{};
    sprintf(timeStr, "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", utc_time.tm_year + 1900, utc_time.tm_mon + 1, utc_time.tm_mday, utc_time.tm_hour, utc_time.tm_min, utc_time.tm_sec,ms);
    return std::string(timeStr);
}

void MDEngineBinance::on_lws_market_trade(const char* data, size_t len)
{
    //{"e":"trade","E":1529713551873,"s":"BTCUSDT","t":52690105,"p":"6040.80000000","q":"0.10006000","b":122540296,"a":122539764,"T":1529713551870,"m":false,"M":true}
    
    Document d;
    d.Parse(data);

    LFL2TradeField trade;
    memset(&trade, 0, sizeof(trade));
    
    if(!d.HasMember("s") || !d.HasMember("p") || !d.HasMember("q") || !d.HasMember("m"))
    {
        KF_LOG_ERROR(logger, "invalid market trade message");
        return;    
    }

    std::string symbol = d["s"].GetString();
    std::string ticker = coinPairWhiteList.GetKeyByValue(symbol);
    if(ticker.length() == 0) {
        KF_LOG_INFO(logger, "MDEngineBinance::on_lws_market_trade: not in WhiteList , ignore it:" << symbol);
        return;
    }

    strcpy(trade.InstrumentID, ticker.c_str());
    strcpy(trade.ExchangeID, "binance");
    trade.TimeStamp = d["T"].GetInt64()*1000000;
    auto strTime = timestamp_to_formatISO8601(d["T"].GetInt64());
    strcpy(trade.TradeTime,strTime.c_str()); 
    trade.Price = std::round(std::stod(d["p"].GetString()) * scale_offset);
    trade.Volume = std::round(std::stod(d["q"].GetString()) * scale_offset);
    //"m": true,        // Is the buyer the market maker?
    trade.OrderBSFlag[0] = d["m"].GetBool() ? 'B' : 'S';
    std::string tradeid =std::to_string(d["t"].GetInt64());
    strncpy(trade.TradeID,tradeid.c_str(),sizeof(trade.TradeID)); 
    //timer[0] = getTimestamp();/*quest3 edited by fxw*/
    std::unique_lock<std::mutex> lck(trade_mutex);
    auto it = control_trade_map.find(ticker);
    if(it != control_trade_map.end()){
        it->second = getTimestamp();
    }
    lck.unlock();

    //edit begin
    if (need_another_kline) {
        KF_LOG_INFO(logger, "MDEngineBinance2::on_lws_market_trade: writing klinedata" << symbol);
        KF_LOG_INFO(logger, "ts=" << trade.TimeStamp);
        KlineData klinedata;
        klinedata.ts = trade.TimeStamp;
        klinedata.price = trade.Price;
        klinedata.volume = trade.Volume;
        //klinedata.TradingDay = getdate();
        klinedata.InstrumentID = trade.InstrumentID;
        klinedata.ExchangeID = "binance";

        std::unique_lock<std::mutex> lock(kline_mutex2);
        //kline_receive_vec.push_back(klinedata);
        kline_receive_list.push_back(klinedata);
        lock.unlock();
        KF_LOG_INFO(logger, "MDEngineBinance2::on_lws_market_trade: writing klinedata end" << symbol);
    }
    //edit end

    on_trade(&trade);
}

void MDEngineBinance::on_lws_book_update(const char* data, size_t len, const std::string& ticker)
{
    //{"lastUpdateId":86947555,"bids":[["0.00000702","17966.00000000",[]],["0.00000701","111276.00000000",[]],["0.00000700","11730816.00000000",[]],["0.00000699","304119.00000000",[]],["0.00000698","337397.00000000",[]]],"asks":[["0.00000703","65956.00000000",[]],["0.00000704","213919.00000000",[]],["0.00000705","463226.00000000",[]],["0.00000706", "709268.00000000",[]],["0.00000707","78529.00000000",[]]]}

    Document d;
    d.Parse(data);

	LFPriceBook20Field md;
	memset(&md, 0, sizeof(md));

	uint64_t sequence;
	if(d.HasMember("lastUpdateId")){
		sequence = d["lastUpdateId"].GetUint64();
	}

    bool has_update = false;	    	
	if(d.HasMember("bids"))
	{
		auto& bids = d["bids"];

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
    if(d.HasMember("asks"))
    {
        auto& asks = d["asks"];
        if(asks.IsArray() && asks.Size() >0)
        {
            auto size = std::min((int)asks.Size(), 20);
            for(int i = 0; i < size; ++i)
            {
                md.AskLevels[i].price = std::round(stod(asks.GetArray()[i][0].GetString()) * scale_offset);
                md.AskLevels[i].volume = std::round(stod(asks.GetArray()[i][1].GetString()) * scale_offset);
            }
            md.AskLevelCount = size;
            has_update = true;
        }
    }    
    if(has_update)
    {
        std::string strategy_ticker = coinPairWhiteList.GetKeyByValue(ticker);
        if(strategy_ticker.length() == 0) {
            KF_LOG_INFO(logger, "MDEngineBinance::on_lws_market_trade: not in WhiteList , ignore it:" << strategy_ticker);
            return;
        }
        strcpy(md.InstrumentID, strategy_ticker.c_str());
        strcpy(md.ExchangeID, "binance");
        priceBook[ticker]=md;
        /*quest3 edited by fxw,starts here*/
        //timer[1] = getTimestamp();
        std::unique_lock<std::mutex> lck1(book_mutex);
        auto it = control_book_map.find(strategy_ticker);
        if(it != control_book_map.end()){
            it->second = getTimestamp();
        }
        lck1.unlock();
        if(md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold){
            string errorMsg = "orderbook level below threshold";
            write_errormsg(112,errorMsg);
            on_price_book_update(&md);
        }
        else if (md.BidLevels[0].price <=0 || md.AskLevels[0].price <=0 || md.BidLevels[0].price > md.AskLevels[0].price){
            string errorMsg = "orderbook crossed";
            write_errormsg(113,errorMsg);
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
        /*quest3 edited by fxw,ends here*/
        
    } 
}

void MDEngineBinance::on_lws_kline(const char* src, size_t len)
{
    //KF_LOG_INFO(logger, "processing 1-min trade bins data" << src);
     Document json;
    json.Parse(src);
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
     //modified
    if(data["x"].GetBool() || !only_receive_complete_kline)
    {
        LFBarMarketDataField market;
        memset(&market, 0, sizeof(market));
        strcpy(market.InstrumentID, ticker.c_str());
        strcpy(market.ExchangeID, "binance");

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

        auto itPrice = priceBook.find(symbol);
        if(itPrice != priceBook.end())
        {
            market.BestBidPrice = itPrice->second.BidLevels[0].price;
            market.BestAskPrice = itPrice->second.AskLevels[0].price;
        }
        //timer[2] = getTimestamp();/*quest3 edited by fxw*/
        std::unique_lock<std::mutex> lck2(kline_mutex);
        auto it = control_kline_map.find(ticker);
        if(it != control_kline_map.end()){
            it->second = getTimestamp();
        }
        lck2.unlock();
        
		on_market_bar_data(&market);
	}
}

void MDEngineBinance::on_lws_ticker(const char* data, size_t len)
{
    Document d;
    d.Parse(data);

    LFMarketDataField bookticker;
    memset(&bookticker, 0, sizeof(bookticker));
    
    if(!d.HasMember("u") || !d.HasMember("s") || !d.HasMember("b") || !d.HasMember("B") || !d.HasMember("a") || !d.HasMember("A"))
    {
        KF_LOG_ERROR(logger, "invalid bookticker message");
        return;    
    }

    std::string symbol = d["s"].GetString();
    std::string ticker = coinPairWhiteList.GetKeyByValue(symbol);
    if(ticker.length() == 0) {
        KF_LOG_INFO(logger, "MDEngineBinance::on_lws_ticker: not in WhiteList , ignore it:" << symbol);
        return;
    }

    strcpy(bookticker.InstrumentID, ticker.c_str());
    strcpy(bookticker.ExchangeID, "binance");

    bookticker.BidPrice1 = std::round(std::stod(d["b"].GetString()) * scale_offset);
    bookticker.BidVolume1 = std::round(std::stod(d["B"].GetString()) * scale_offset);
    bookticker.AskPrice1 = std::round(std::stod(d["a"].GetString()) * scale_offset);
    bookticker.AskVolume1 = std::round(std::stod(d["A"].GetString()) * scale_offset);

    KF_LOG_INFO(logger, "MDEngineBinance::on_lws_ticker (InstrumentID)"<<bookticker.InstrumentID<<" (ExchangeID)"<<bookticker.ExchangeID
        <<" (BidPrice1)"<<bookticker.BidPrice1<<" (BidVolume1)"<<bookticker.BidVolume1<<" (AskPrice1)"<<bookticker.AskPrice1
        <<" (AskVolume1)"<<bookticker.AskVolume1);

    on_market_data(&bookticker);
}

void MDEngineBinance::set_reader_thread()
{
    IMDEngine::set_reader_thread();

   	ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBinance::loop, this)));
    if (need_get_snapshot_via_rest)
        rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBinance::rest_loop, this)));
    else
        KF_LOG_INFO(logger, "MDEngineBinance::set_reader_thread, won't start rest_loop");
    //edit here
    if (need_another_kline)
        kline_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBinance::klineloop, this)));
}

//edit begin
//int bar_duration_s = 60;
//int bar_duration_s = 5;
void MDEngineBinance::klineloop()
{
    bool first_kline = true;
    int64_t last_time = time(0);
    KF_LOG_INFO(logger, "start=" << last_time);

    int64_t rest = 5;
    while (isRunning) {
        last_time = time(0);

        //1 -> 0.5
        //if (last_time % bar_duration_s == 1) {
        if ((getTimestamp() / 100) % (bar_duration_s * 10) == rest) {
            KF_LOG_INFO(logger, "last_time=" << last_time);
            //control_kline(first_kline, last_time - 1);
            control_kline(first_kline, last_time);
            first_kline = false;
            break;
        }
    }
    if (!first_kline) {
        KF_LOG_INFO(logger, "!first_kline");
        while (isRunning) {
            int64_t now = time(0);
            if ((getTimestamp() / 100 - last_time * 10 - rest) >= (bar_duration_s * 10)) {
                //if (now - last_time >= bar_duration_s) {
                last_time = now;
                KF_LOG_INFO(logger, "now1=" << now);
                //control_kline(first_kline, now - 1);
                control_kline(first_kline, now);
            }
        }
    }
}

void MDEngineBinance::control_kline(bool first_kline, int64_t time)
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
        //kline_hand_map.clear();

        /*
        std::vector<KlineData>::iterator it;
        for (it = kline_receive_vec.begin(); it != kline_receive_vec.end(); it++) {
            if (it->ts >= (time - bar_duration_s) * 1e9 && it->ts < time * 1e9) {
                update = true;
                auto it1 = kline_hand_map.find(it->InstrumentID);
                if (it1 != kline_hand_map.end()) {
                    kline_hand_map.erase(it1);
                }
            }
        }*/
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
                    klinedata.ExchangeID = "binance";
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

void MDEngineBinance::handle_kline(int64_t time)
{
    KF_LOG_INFO(logger, "handle_kline");
    std::map<std::string, KlineData>::iterator it;
    for (it = kline_hand_map.begin(); it != kline_hand_map.end(); it++) {
        KF_LOG_INFO(logger, "it in");
        LFBarMarketDataField market;
        memset(&market, 0, sizeof(market));
        strcpy(market.InstrumentID, it->second.InstrumentID.c_str());
        strcpy(market.ExchangeID, "binance");

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

std::string MDEngineBinance::gettime(int64_t time)
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

std::string MDEngineBinance::dealzero(std::string time)
{
    if (time.length() == 1) {
        time = "0" + time;
    }
    return time;
}
/*
[
  [
    1499040000000,      // 开盘时间
    "0.01634790",       // 开盘价
    "0.80000000",       // 最高价
    "0.01575800",       // 最低价
    "0.01577100",       // 收盘价(当前K线未结束的即为最新价)
    "148976.11427815",  // 成交量
    1499644799999,      // 收盘时间
    "2434.19055334",    // 成交额
    308,                // 成交笔数
    "1756.87402397",    // 主动买入成交量
    "28.46694368",      // 主动买入成交额
    "17928899.62484339" // 请忽略该参数
  ]
]
*/
void MDEngineBinance::get_kline_via_rest(string symbol, string interval, bool ignore_startTime, int64_t startTime, int64_t endTime, int limit)
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

string MDEngineBinance::getIntervalStr(int64_t sec)
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

std::string MDEngineBinance::getdate(int64_t time)
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

void MDEngineBinance::logout()
{
    lws_context_destroy( context );
       KF_LOG_INFO(logger, "MDEngineBinance::logout:");     
}

void MDEngineBinance::release_api()
{
   KF_LOG_INFO(logger, "MDEngineBinance::release_api:");     
}

void MDEngineBinance::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    /* Connect if we are not connected to the server. */
       KF_LOG_INFO(logger, "MDEngineBinance::subscribeMarketData:");     
}

void MDEngineBinance::GetAndHandleDepthResponse(const std::string& symbol, int limit) 
{
    const auto static url = "https://api.binance.com/api/v1/depth";
    const auto response = Get(Url{url}, Parameters{{"symbol", symbol},
                                                        {"limit",  to_string(limit)}});
    Document d;
    d.Parse(response.text.c_str());

    LFMarketDataField md;
    memset(&md, 0, sizeof(md));

    bool has_update = false;            
    if(d.HasMember("bids") && d["bids"].IsArray() && d["bids"].Size() >= limit)
    {
        md.BidPrice1 = std::round(stod(d["bids"].GetArray()[0][0].GetString()) * scale_offset);
        md.BidVolume1 = std::round(stod(d["bids"].GetArray()[0][1].GetString()) * scale_offset);
        md.BidPrice2 = std::round(stod(d["bids"].GetArray()[1][0].GetString()) * scale_offset);
        md.BidVolume2 = std::round(stod(d["bids"].GetArray()[1][1].GetString()) * scale_offset);
        md.BidPrice3 = std::round(stod(d["bids"].GetArray()[2][0].GetString()) * scale_offset);
        md.BidVolume3 = std::round(stod(d["bids"].GetArray()[2][1].GetString()) * scale_offset);
        md.BidPrice4 = std::round(stod(d["bids"].GetArray()[3][0].GetString()) * scale_offset);
        md.BidVolume4 = std::round(stod(d["bids"].GetArray()[3][1].GetString()) * scale_offset);
        md.BidPrice5 = std::round(stod(d["bids"].GetArray()[4][0].GetString()) * scale_offset);
        md.BidVolume5 = std::round(stod(d["bids"].GetArray()[4][1].GetString()) * scale_offset);
        
        has_update = true;
    }

    if(d.HasMember("asks") && d["asks"].IsArray() && d["asks"].Size() >= limit)
    {
        md.AskPrice1 = std::round(stod(d["asks"].GetArray()[0][0].GetString()) * scale_offset);
        md.AskVolume1 = std::round(stod(d["asks"].GetArray()[0][1].GetString()) * scale_offset);
        md.AskPrice2 = std::round(stod(d["asks"].GetArray()[1][0].GetString()) * scale_offset);
        md.AskVolume2 = std::round(stod(d["asks"].GetArray()[1][1].GetString()) * scale_offset);
        md.AskPrice3 = std::round(stod(d["asks"].GetArray()[2][0].GetString()) * scale_offset);
        md.AskVolume3 = std::round(stod(d["asks"].GetArray()[2][1].GetString()) * scale_offset);
        md.AskPrice4 = std::round(stod(d["asks"].GetArray()[3][0].GetString()) * scale_offset);
        md.AskVolume4 = std::round(stod(d["asks"].GetArray()[3][1].GetString()) * scale_offset);
        md.AskPrice5 = std::round(stod(d["asks"].GetArray()[4][0].GetString()) * scale_offset);
        md.AskVolume5 = std::round(stod(d["asks"].GetArray()[4][1].GetString()) * scale_offset);
        
        has_update = true;
    }
    
    if(has_update)
    {
        strcpy(md.InstrumentID, symbol.c_str());
        strcpy(md.ExchangeID, "binance");
        md.UpdateMillisec = last_rest_get_ts;

        on_market_data(&md);
    } 
}

void MDEngineBinance::GetAndHandleTradeResponse(const std::string& symbol, int limit)
{
    const auto static url = "https://api.binance.com/api/v1/trades";
    const auto response = Get(Url{url}, Parameters{{"symbol", symbol},
            {"limit",  to_string(limit)}});
    Document d;
    d.Parse(response.text.c_str());
    if(d.IsArray())
    {
        LFL2TradeField trade;
        memset(&trade, 0, sizeof(trade));
        std::string symbols = "TRXBTC";
        strcpy(trade.InstrumentID, symbols.c_str());
        strcpy(trade.ExchangeID, "binance");

        for(int i = 0; i < d.Size(); ++i)
        {
            const auto& ele = d[i];
            if(!ele.HasMember("id"))
            {
            continue;
            }
            
              const auto trade_id = ele["id"].GetUint64();
            if(trade_id <= last_trade_id)
            {
                continue;
            }
            
            last_trade_id = trade_id;
            if(ele.HasMember("price") && ele.HasMember("qty") && ele.HasMember("isBuyerMaker") && ele.HasMember("isBestMatch"))
            {
                trade.Price = std::round(std::stod(ele["price"].GetString()) * scale_offset);
                trade.Volume = std::round(std::stod(ele["qty"].GetString()) * scale_offset);
                trade.OrderKind[0] = ele["isBestMatch"].GetBool() ? 'B' : 'N';
                trade.OrderBSFlag[0] = ele["isBuyerMaker"].GetBool() ? 'B' : 'S';
                on_trade(&trade);
            }
        }
    }   
}

void MDEngineBinance::get_snapshot_via_rest()
{
    //while (isRunning)
    //{
    std::unordered_map<std::string, std::string>::iterator map_itr;
    for (map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); map_itr++)
    {
        std::string url = "https://api.binance.com/api/v3/depth?limit=20&symbol=";
        url += map_itr->second;
        cpr::Response response = Get(Url{ url.c_str() }, Parameters{});
        Document d;
        d.Parse(response.text.c_str());
        KF_LOG_INFO(logger, "get_snapshot_via_rest get(" << url << "):" << response.text);
        //"code":"200000"
        if (d.IsObject() && d.HasMember("lastUpdateId"))
        {
            uint64_t sequence = d["lastUpdateId"].GetUint64();

            LFPriceBook20Field priceBook{ 0 };
            strcpy(priceBook.ExchangeID, "binance");
            strncpy(priceBook.InstrumentID, map_itr->first.c_str(), std::min(sizeof(priceBook.InstrumentID) - 1, map_itr->first.size()));
            if (d.HasMember("bids") && d["bids"].IsArray())
            {
                auto& bids = d["bids"];
                int len = std::min((int)bids.Size(), 20);
                for (int i = 0; i < len; ++i)
                {
                    priceBook.BidLevels[i].price = std::round(stod(bids[i][0].GetString()) * scale_offset);
                    priceBook.BidLevels[i].volume = std::round(stod(bids[i][1].GetString()) * scale_offset);
                }
                priceBook.BidLevelCount = len;
            }
            if (d.HasMember("asks") && d["asks"].IsArray())
            {
                auto& asks = d["asks"];
                int len = std::min((int)asks.Size(), 20);
                for (int i = 0; i < len; ++i)
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

void MDEngineBinance::check_snapshot()
{
    std::vector<BookMsg>::iterator rest_it;
    std::unique_lock<std::mutex> lck_rest_book(rest_book_mutex);
    for(rest_it = rest_book_vec.begin();rest_it != rest_book_vec.end();){
        int64_t now = getTimestamp();
        std::unique_lock<std::mutex> lck_ws_book(ws_book_mutex);
        auto map_itr = ws_book_map.find(rest_it->InstrumentID);
        if(map_itr != ws_book_map.end()){
            /*auto ws_it = map_itr->second.find(rest_it->sequence);
            if(ws_it == map_itr->second.end()){
            	KF_LOG_INFO(logger,"not start:"<<rest_it->InstrumentID<<" "<<rest_it->sequence);
            }else{
            	if(ws_it->second - rest_it->time > snapshot_check_s * 1000){
                    KF_LOG_INFO(logger, "ws snapshot is later than rest snapshot");
                    string errorMsg = "ws snapshot is later than rest snapshot";
                    write_errormsg(115,errorMsg);
            	}
                KF_LOG_INFO(logger, "same_book:"<<rest_it->InstrumentID);
                KF_LOG_INFO(logger,"ws_time="<<ws_it->second<<" rest_time="<<rest_it->time);
            }*/
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
void MDEngineBinance::rest_loop()
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

void MDEngineBinance::loop()
{
        while(isRunning)
        {
            int errorId = 0;
            string errorMsg = "";
            /*quest3 edited by fxw starts here*/
            /*判断是否在设定时间内更新与否，*/
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

            std::unique_lock<std::mutex> lck2(kline_mutex);
            std::map<std::string,int64_t>::iterator it2;
            for(it2 = control_kline_map.begin(); it2 != control_kline_map.end(); it2++){
                if((now - it2->second) > refresh_normal_check_kline_s * 1000){
                    errorId = 116;
                    errorMsg = it2->first + " kline max refresh wait time exceeded";
                    KF_LOG_INFO(logger,"116"<<errorMsg); 
                    write_errormsg(errorId,errorMsg);
                    it2->second = now;                   
                }
            }    
            lck2.unlock();            
            lws_service( context, rest_get_interval_ms );
        }
}
/*quest3 edited by fxw,starts here*/
int MDEngineBinance::Get_refresh_normal_check_book_s()
{
    return refresh_normal_check_book_s;
}

int MDEngineBinance::Get_refresh_normal_check_kline_s()
{
    return refresh_normal_check_kline_s;
}

inline int64_t MDEngineBinance::getTimestamp()
{   /*返回的是毫秒*/
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}
/*quest3 edited by fxw,ends here*/

BOOST_PYTHON_MODULE(libbinancemd)
{
    using namespace boost::python;
    class_<MDEngineBinance, boost::shared_ptr<MDEngineBinance> >("Engine")
    .def(init<>())
    .def("init", &MDEngineBinance::initialize)
    .def("start", &MDEngineBinance::start)
    .def("stop", &MDEngineBinance::stop)
    .def("logout", &MDEngineBinance::logout)
    .def("wait_for_stop", &MDEngineBinance::wait_for_stop);
}
