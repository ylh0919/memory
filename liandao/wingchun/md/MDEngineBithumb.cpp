#include "MDEngineBithumb.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/LFDataStruct.h"

#include <document.h>
#include <stringbuffer.h>
#include <writer.h>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <string>
#include <cpr/cpr.h>
#include <chrono>
#include <algorithm>
#include <limits>

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


static MDEngineBithumb* g_md_bithumb = nullptr;


MDEngineBithumb::MDEngineBithumb(): IMDEngine(SOURCE_BITHUMB)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Bithumb");
}

void MDEngineBithumb::load(const json& j_config)
{
    book_depth_count = j_config["book_depth_count"].get<int>();
    trade_count = j_config["trade_count"].get<int>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }

    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineBithumb::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"BTCUSDT\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"ETCETH\"");
        KF_LOG_ERROR(logger, "},");
    }
    KF_LOG_INFO(logger, "MDEngineBithumb::load:  book_depth_count: "
        << book_depth_count << " trade_count: " << trade_count << " rest_get_interval_ms: " << rest_get_interval_ms); 	
}
int64_t MDEngineBithumb::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void MDEngineBithumb::connect(long timeout_nsec)
{
   KF_LOG_INFO(logger, "MDEngineBithumb::connect:"); 	
    
   connected = true;
}

void MDEngineBithumb::login(long timeout_nsec)
{
    //g_md_bithumb = this;

    KF_LOG_INFO(logger, "MDEngineBithumb::login:");
    //logged_in = true;
    websocket.RegisterCallBack(this);
    logged_in = websocket.Connect("pubwss.bithumb.com/pub/ws");
    /*if(logged_in){
        websocket.StartPtrThread();
    }*/

    timer = getTimestamp();
    KF_LOG_INFO(logger, "MDEngineBithumb::login " << (logged_in ? "Success" : "Failed"));	

       logged_in = true;
}
void MDEngineBithumb::OnConnected(const common::CWebsocket* instance)
{
    KF_LOG_INFO(logger,"OnConnected");
    makeWebsocketSubscribeJsonString();
}

void MDEngineBithumb::OnReceivedMessage(const common::CWebsocket* instance,const std::string& msg)
{
    KF_LOG_INFO(logger, "MDEngineBithumb::on_lws_data: " << msg);
    Document json;
    json.Parse(msg.c_str());

    if (json.HasParseError()) {
        KF_LOG_ERROR(logger, "MDEngineBithumb::on_lws_data. parse json error.");
        return;
    }

    std::string type;
    if(json.HasMember("type")){
        type = json["type"].GetString();
    }
    if(type == "orderbookdepth")
    {
        KF_LOG_INFO(logger, "MDEngineBithumb::on_lws_data: is book");
        onBook(json);
    }
    else if(type == "transaction")
    {
        KF_LOG_INFO(logger, "MDEngineBithumb::on_lws_data: is trade");
        onTrade(json);
    }
    else {
        KF_LOG_INFO(logger, "MDEngineBithumb::on_lws_data: unknown array data.");
    }
}

void MDEngineBithumb::OnDisconnected(const common::CWebsocket* instance)
{
    KF_LOG_ERROR(logger, "MDEngineBithumb::on_lws_connection_error.");
    //market logged_in false;
    //logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineBithumb::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    is_ws_disconnectd = true;
    //login(timeout_nsec);
}
void MDEngineBithumb::makeWebsocketSubscribeJsonString()
{
    std::vector<std::string> vecPairs;
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    while (map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);
        vecPairs.push_back(map_itr->second);
        
        map_itr++;
    }
    //std::string jsonBookString = createBookJsonString(vecPairs);
    //websocket.SendMessage(jsonBookString);
    std::string jsonTradeString = createTradeJsonString(vecPairs);
    websocket.SendMessage(jsonTradeString);
    KF_LOG_INFO(logger, "sendmessage "<<jsonTradeString);
}
std::string MDEngineBithumb::createBookJsonString(std::vector<std::string>& exchange_coinpair)
{
    //{"type": "orderbookdepth", "symbols": ["BTC_KRW"]}
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("type");
    writer.String("orderbookdepth");

    writer.Key("symbols");
    writer.StartArray();
    for(auto& item:exchange_coinpair)
    {
        writer.String(item.c_str());
    }
    writer.EndArray();

    writer.EndObject();
    return s.GetString();
}

std::string MDEngineBithumb::createTradeJsonString(std::vector<std::string>& exchange_coinpair)
{
    //{"type": "transaction", "symbols": ["BTC_KRW"]}
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("type");
    writer.String("transaction");

    writer.Key("symbols");
    writer.StartArray();
    for(auto& item:exchange_coinpair)
    {
        writer.String(item.c_str());
    }
    writer.EndArray();

    writer.EndObject();
    return s.GetString();
}

int64_t tokyotime2timestamp(std::string time)
{
    int year,month,day,hour,min,sec,millsec;
    sscanf(time.c_str(),"%04d-%02d-%02d %02d:%02d:%02d.%06d",&year,&month,&day,&hour,&min,&sec,&millsec);
    tm utc_time{};
    utc_time.tm_year = year - 1900;
    utc_time.tm_mon = month -1;
    utc_time.tm_mday = day;
    utc_time.tm_hour = hour;
    utc_time.tm_min = min;
    utc_time.tm_sec = sec;
    time_t timet = mktime(&utc_time);
    return (timet-3600)*1000000+millsec;
}

void MDEngineBithumb::onTrade(Document& json)
{
    std::string exchange_coinpair = json["content"]["list"].GetArray()[0]["symbol"].GetString();
    KF_LOG_INFO(logger, "MDEngineBithumb::onTrade: (symbol) " << exchange_coinpair);

    std::string ticker = coinPairWhiteList.GetKeyByValue(exchange_coinpair);
    if (ticker.length() == 0) {
        return;
    }

    auto& node = json["content"]["list"];

    LFL2TradeField trade;
    memset(&trade, 0, sizeof(trade));
    strcpy(trade.InstrumentID, ticker.c_str());
    strcpy(trade.ExchangeID, "bithumb");

    std::string strTime = node.GetArray()[0]["contDtm"].GetString();//2018-04-10 17:47:46
    int64_t tkTime =tokyotime2timestamp(strTime);
    trade.TimeStamp = tkTime*1000;
    strTime = timestamp_to_formatISO8601(tkTime/1000);
	strcpy(trade.TradeTime,strTime.c_str());
    trade.Price = std::round(std::stod(node.GetArray()[0]["contPrice"].GetString()) * scale_offset);
    trade.Volume = std::round(std::stod(node.GetArray()[0]["contQty"].GetString()) * scale_offset);
    std::string strTemp = node.GetArray()[0]["buySellGb"].GetString();
    trade.OrderBSFlag[0] = strTemp == "1" ? 'B' : 'S';
    on_trade(&trade);
}

void MDEngineBithumb::onBook(Document& json)
{
    std::string exchange_coinpair = json["content"]["list"].GetArray()[0]["symbol"].GetString();
    KF_LOG_INFO(logger, "MDEngineCoincheck::onBook: (symbol) " << exchange_coinpair);

    std::string ticker = coinPairWhiteList.GetKeyByValue(exchange_coinpair);
    if (ticker.length() == 0) {
        KF_LOG_DEBUG(logger, "MDEngineCoincheck::onBook: (ticker.length==0) " << ticker);
        return;
    }
    KF_LOG_INFO(logger, "MDEngineCoincheck::onBook: (ticker) " << ticker);

    bool need_initbook = true;
    auto itPriceBook = m_mapPriceBookData.find(ticker);
    if(itPriceBook == m_mapPriceBookData.end())
    {
        if(!getInitPriceBook(exchange_coinpair,itPriceBook))
        {
            KF_LOG_INFO(logger,"1MDEngineKumex::onDepth:return");
            return;
        }
    }else{
        need_initbook = false;
    }

    //if(need_initbook){
        if(json["content"].HasMember("datetime"))
        {
            std::string datetime = json["content"]["datetime"].GetString();
            datetime = datetime.substr(0,13);
            int64_t sequence = stoll(datetime);
            KF_LOG_INFO(logger, "start to compare");
            while(1)
            {
                if(itPriceBook->second < sequence)
                {
                    if(need_initbook){
                        string errorMsg = "Orderbook update sequence missed, request for a new snapshot";
                        write_errormsg(5,errorMsg);
                        KF_LOG_ERROR(logger, "Orderbook update missing "<< itPriceBook->second<<"-" << sequence);

                        if(!getInitPriceBook(ticker,itPriceBook))
                        {
                            KF_LOG_INFO(logger,"2::onDepth:return");
                            return;
                        }
                    }else{
                        KF_LOG_INFO(logger,"new update");
                        break;
                    }
                }
                else //if(itPriceBook->second >= sequence)
                {
                    KF_LOG_INFO(logger, "onDepth:  old data,last sequence:" << itPriceBook->second<< ">= now sequence:" << sequence);
                    return;
                }
                /*else
                {
                    KF_LOG_INFO(logger, "lichengyi-kumex:No error" << itPriceBook->second.nSequence << "-" << sequence);
                    break;
                } */           
            }
        }
    //}

    auto& node = json["content"]["list"];
    int size = node.Size();
    for(int i = 0; i < size; i++){
        std::string orderType = node.GetArray()[i]["orderType"].GetString();
        int64_t price = std::round(stod(node.GetArray()[i]["price"].GetString()) * scale_offset);
        uint64_t volume = std::round(stod(node.GetArray()[i]["quantity"].GetString()) * scale_offset);
        if(orderType == "bid"){
            if(volume == 0){
                priceBook20Assembler.EraseBidPrice(ticker, price);
            }else{
                priceBook20Assembler.UpdateBidPrice(ticker,price,volume);
            }        	
        }else{
            if(volume == 0){
                priceBook20Assembler.EraseAskPrice(ticker, price);
            }else{
                priceBook20Assembler.UpdateAskPrice(ticker,price,volume);
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
        strcpy(md.ExchangeID, "bithumb");

        KF_LOG_INFO(logger, "MDEngineCoincheck::onBook: on_price_book_update");

        if (md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold)
        {
            md.Status = 1;
            errorId = 112;
            errorMsg = "orderbook level below threshold";
            /*need re-login*/
            KF_LOG_DEBUG(logger, "MDEngineCoincheck on_price_book_update failed ,lose level,re-login....");
            on_price_book_update(&md);
            write_errormsg(errorId,errorMsg);

        }
        else if (md.BidLevels[0].price <=0 || md.AskLevels[0].price <=0 || md.BidLevels[0].price >= md.AskLevels[0].price)
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

}
bool MDEngineBithumb::getInitPriceBook(const std::string& strSymbol,std::map<std::string,int64_t>::iterator& itPriceBookData)
{
    int nTryCount = 0;
    cpr::Response response;
    std::string url = "https://api.bithumb.com/public/orderbook/";
    url += strSymbol;
    //KF_LOG_INFO(logger,"getInitPriceBook url="<<url);

    do{  
       response = Get(Url{url.c_str()}, Parameters{}); 
       
    }while(++nTryCount < rest_try_count && response.status_code != 200);

    if(response.status_code != 200)
    {
        KF_LOG_ERROR(logger, "MDEngineKumex::login::getInitPriceBook Error, response = " <<response.text.c_str());
        return false;
    }
    KF_LOG_INFO(logger, "MDEngineKumex::getInitPriceBook: " << response.text.c_str());

    std::string ticker = coinPairWhiteList.GetKeyByValue(strSymbol);
    priceBook20Assembler.clearPriceBook(ticker);

    Document d;
    d.Parse(response.text.c_str());
    //itPriceBookData = m_mapPriceBookData.insert(std::make_pair(ticker,PriceBookData())).first;
    if(!d.HasMember("data"))
    {
        return  true;
    }
    auto& jsonData = d["data"];

    std::string strTime = jsonData["timestamp"].GetString();
    int64_t book_time = stoll(strTime);
    itPriceBookData = m_mapPriceBookData.insert(std::make_pair(ticker, book_time)).first;
    itPriceBookData->second = book_time;

    if(jsonData.HasMember("bids"))
    {
        auto& bids =jsonData["bids"];
         if(bids .IsArray()) 
         {
                int len = bids.Size();
                for(int i = 0 ; i < len; i++)
                {
                    int64_t price = std::round(stod(bids.GetArray()[i]["price"].GetString()) * scale_offset);
                    uint64_t volume = std::round(stod(bids.GetArray()[i]["quantity"].GetString()) * scale_offset);
                    priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
                }
         }
    }
    if(jsonData.HasMember("asks"))
    {
        auto& asks =jsonData["asks"];
         if(asks .IsArray()) 
         {
                int len = asks.Size();
                for(int i = 0 ; i < len; i++)
                {
                    int64_t price = std::round(stod(asks.GetArray()[i]["price"].GetString()) * scale_offset);
                    uint64_t volume = std::round(stod(asks.GetArray()[i]["quantity"].GetString()) * scale_offset);
                    priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
                }
         }
    }
    KF_LOG_INFO(logger,"ticker="<<ticker);

    return true;
}
void MDEngineBithumb::set_reader_thread()
{
    IMDEngine::set_reader_thread();
        
       rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineBithumb::loop, this)));
    KF_LOG_INFO(logger,"MDEngineBithumb::set_reader_thread:rest_thread begin");
}

void MDEngineBithumb::logout()
{
       KF_LOG_INFO(logger, "MDEngineBithumb::logout:"); 	
}

void MDEngineBithumb::release_api()
{
   KF_LOG_INFO(logger, "MDEngineBithumb::release_api:"); 	
}

void MDEngineBithumb::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    /* Connect if we are not connected to the server. */
       KF_LOG_INFO(logger, "MDEngineBithumb::subscribeMarketData:"); 	
}

void MDEngineBithumb::GetAndHandleDepthResponse(const std::string& symbol, int limit) 
{
    std::string url = "https://api.bithumb.com/public/orderbook/";
    url += symbol;
    const auto response = Get(Url{url.c_str()}, Parameters{{"group_orders", to_string(1)},
                                                        {"count",  to_string(limit)}});
    KF_LOG_INFO(logger,"GetAndHandleDepthResponse: "<<response.text);
    
    Document d;
    d.Parse(response.text.c_str());

    std::string strStatus = d["status"].GetString();
    if(strStatus != "0000")
    {
        KF_LOG_ERROR(logger,"MDEngineBithumb::GetAndHandleDepthResponse:Error Code[" << strStatus << "],url[" << url.c_str() << "],group_orders[" << to_string(1) << "],count[" << to_string(limit) << "]");	
        return ;
    }
    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));

   // bool has_update = false;	    	
    if(d.HasMember("data"))
    {
        auto& data = d["data"];
        if(data.HasMember("bids"))
        {
            auto& bids = data["bids"];
            if(bids.IsArray() && bids.Size() >0)
            {
                auto size = std::min((int)bids.Size(), limit);
        
                for(int i = 0; i < size; ++i)
                {
                    md.BidLevels[i].price = stod(bids.GetArray()[i]["price"].GetString()) * scale_offset;
                    md.BidLevels[i].volume = stod(bids.GetArray()[i]["quantity"].GetString()) * scale_offset;
                }
                md.BidLevelCount = size;

                //has_update = true;
            }
        }

        if(data.HasMember("asks"))
        {
            auto& asks = data["asks"];

            if(asks.IsArray() && asks.Size() >0)
            {
                auto size = std::min((int)asks.Size(), limit);
        
                for(int i = 0; i < size; ++i)
                {
                    md.AskLevels[i].price = stod(asks.GetArray()[i]["price"].GetString()) * scale_offset;
                    md.AskLevels[i].volume = stod(asks.GetArray()[i]["quantity"].GetString()) * scale_offset;
                }
                md.AskLevelCount = size;

                //has_update = true;
            }
        }
           if(data.HasMember("timestamp"))
        {
        md.UpdateMicroSecond = std::stoull(data["timestamp"].GetString()) ;
        }
    }
    
    bool has_update = false;
    static LFPriceBook20Field lastMD = {0};
        if(md.BidLevelCount != lastMD.BidLevelCount)
        {
            has_update = true;
        }
        else
        {
            for(int i = 0;i < md.BidLevelCount; ++i)
            {
                if(md.BidLevels[i].price != lastMD.BidLevels[i].price || md.BidLevels[i].volume != lastMD.BidLevels[i].volume)
                {
                    has_update = true;
                    break;
                }
            }
        }
        if(!has_update && md.AskLevelCount != lastMD.AskLevelCount)
        {
            has_update = true;
        }
        else if(!has_update)
        {
            for(int i = 0;i < md.AskLevelCount ;++i)
            {
                if(md.AskLevels[i].price != lastMD.AskLevels[i].price || md.AskLevels[i].volume != lastMD.AskLevels[i].volume)
                {
                    has_update = true;
                    break;
                }
            }
        }	
    if(has_update)
    {
        std::string strategy_ticker = coinPairWhiteList.GetKeyByValue(symbol);
        strcpy(md.InstrumentID, strategy_ticker.c_str());
        strcpy(md.ExchangeID, "bithumb");
        lastMD = md;
        //md.UpdateMillisec = last_rest_get_ts;
        //KF_LOG_INFO(logger,"preBid:" << lastMD.BidLevels[0].price << "," << lastMD.BidLevels[0].volume << "  lapreAsk:" << lastMD.AskLevels[0].price << "," << lastMD.AskLevels[0].volume << " preBidLevelCount:" << lastMD.BidLevelCount << " preAskLevelCount:" << lastMD.AskLevelCount << std::endl);
        KF_LOG_INFO(logger,"on_price_book_update2");
        on_price_book_update(&md);
    }
    else
    {
    //KF_LOG_INFO(logger,"Filter Data! preBid:" << lastMD.BidLevels[0].price << "," << lastMD.BidLevels[0].volume << "  lapreAsk:" << lastMD.AskLevels[0].price << "," << lastMD.AskLevels[0].volume << " preBidLevelCount:" << lastMD.BidLevelCount << " preAskLevelCount:" << lastMD.AskLevelCount << std::endl);
    } 
}

void MDEngineBithumb::GetAndHandleTradeResponse(const std::string& symbol, int limit)
{
    std::string url = "https://api.bithumb.com/public/transaction_history/";
    url += symbol;
    long long static cont_no = std::numeric_limits<long long>::max();
    const auto response = Get(Url{url.c_str()}, Parameters{{"cont_no", to_string(cont_no)},{"count", to_string(limit)}});
    Document d;
    d.Parse(response.text.c_str());
    
    std::string strStatus = d["status"].GetString();
    if(strStatus != "0000")
    {
        KF_LOG_ERROR(logger,"MDEnginebithumb::GetAndHandleTradeResponse:ErrorCode[" << strStatus << "],url[" << url.c_str() << "],cont_no[" << to_string(cont_no) << "],count[" << to_string(limit) << "]");
         return;
    }
    if(d.HasMember("data"))
    {	
        auto& data = d["data"];
        if(data.IsArray())
        {
        LFL2TradeField trade;
        memset(&trade, 0, sizeof(trade));
        strcpy(trade.InstrumentID, symbol.c_str());
        strcpy(trade.ExchangeID, "bithumb");

        for(int i = 0; i < data.Size(); ++i)
        {
            const auto& ele = data[i];
            if(ele.HasMember("price") && ele.HasMember("units_traded") && ele.HasMember("type"))
            {
                std::string strTime = ele["transaction_date"].GetString();//2018-04-10 17:47:46
                int year,month,day,hour,min,sec,millsec;
                sscanf(strTime.c_str(),"%04d-%02d-%02d %02d:%02d:%02d",&year,&month,&day,&hour,&min,&sec);
                sprintf(trade.TradeTime,"%04d-%02d-%02dT%02d:%02d:%02d.000Z",year,month,day,hour,min,sec);
                trade.TimeStamp = formatISO8601_to_timestamp(trade.TradeTime)*1000000;
                trade.Price = std::stod(ele["price"].GetString()) * scale_offset;
                trade.Volume = std::stod(ele["units_traded"].GetString()) * scale_offset;
                std::string strTemp = ele["type"].GetString();
                trade.OrderBSFlag[0] = strTemp == "bid" ? 'B' : 'S';
                on_trade(&trade);

                //KF_LOG_INFO(logger,"Trade:" << trade.Price << "," << trade.Volume << " type:" << strTemp << "," << trade.OrderBSFlag << " last_trade_id:" << last_trade_id<< std::endl);
            }
        }
        }
    }
    else
    {
        KF_LOG_INFO(logger,response.text);
    }   
}


void MDEngineBithumb::loop()
{
        while(isRunning)
        {
            int64_t lag = getTimestamp() - timer;
            if ((lag / 1000) > refresh_normal_check_book_s)
            {
                int errorId = 114;
                string errorMsg = "orderbook max refresh wait time exceeded";			
                write_errormsg(errorId,errorMsg);
                timer = getTimestamp();
            }
            if(is_ws_disconnectd)
            {
                login(0);
                is_ws_disconnectd = false;
            }
            using namespace std::chrono;
            auto current_ms = duration_cast< milliseconds>(system_clock::now().time_since_epoch()).count();
            if(last_rest_get_ts != 0 && (current_ms - last_rest_get_ts) < rest_get_interval_ms)
            {	
                continue;	
            }

            last_rest_get_ts = current_ms;
            auto map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
            while (map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) 
            {
                //KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);
                GetAndHandleDepthResponse(map_itr->second, book_depth_count);
                map_itr++;
            }
        }
}

BOOST_PYTHON_MODULE(libbithumbmd)
{
    using namespace boost::python;
    class_<MDEngineBithumb, boost::shared_ptr<MDEngineBithumb> >("Engine")
    .def(init<>())
    .def("init", &MDEngineBithumb::initialize)
    .def("start", &MDEngineBithumb::start)
    .def("stop", &MDEngineBithumb::stop)
    .def("logout", &MDEngineBithumb::logout)
    .def("wait_for_stop", &MDEngineBithumb::wait_for_stop);
}


