#include "MDEngineFtx.h"
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
#include <string>
#include <unistd.h>
#include<set>
using namespace std;
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


USING_WC_NAMESPACE

static MDEngineFtx* global_md = nullptr;

std::mutex book_mutex;
MDEngineFtx::MDEngineFtx() : IMDEngine(SOURCE_FTX)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Ftx");
}

void MDEngineFtx::load(const json& j_config)
{
    book_depth_count = j_config["book_depth_count"].get<int>();
    priceBook20Assembler.SetLevel(book_depth_count);
    level_threshold = j_config["level_threshold"].get<int>();
    priceBook20Assembler.SetLeastLevel(level_threshold);

    if(j_config.find("quote_get_interval_ms") != j_config.end()) {
        quote_get_interval_ms = j_config["quote_get_interval_ms"].get<int>();
    }

    refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();

    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    if (coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineFtx::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_jpy\": \"btc_jpy\"");
        KF_LOG_ERROR(logger, "},");
    }
    KF_LOG_INFO(logger, "MDEngineFtx::load:  book_depth_count: "<< book_depth_count << " refresh_normal_check_book_s:"<<refresh_normal_check_book_s <<" level_threshold:"<<level_threshold);
    int64_t nowTime = getTimestamp();
    std::unique_lock<std::mutex> lck1(book_mutex);
	std::unordered_map<std::string, std::string>::iterator it;
    for(it = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();it != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end();it++)
    {
        control_book_map.insert(make_pair(it->first,nowTime));
    }
}

/*
void MDEngineFtx::makeWebsocketSubscribeJsonString()
{
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    while (map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);

        std::string jsonBookString = createBookJsonString(map_itr->second);
        websocket.SendMessage(jsonBookString);
        std::string jsonTradeString = createTradeJsonString(map_itr->second);
        websocket.SendMessage(jsonTradeString);
        KF_LOG_INFO(logger, "sendmessage "<< jsonBookString <<" "<<jsonTradeString);
        map_itr++;
    }
}*/

/*https://ftx.com/api/markets  get֮���response
{"result":[{...����һ����Ϣ} {...����һ����Ϣ} ]
   "success":true
}
ÿһ����Ϣ��ʽΪ
 {
      "name": "BTC-0628",
      "baseCurrency": null,
      "quoteCurrency": null,
      "type": "future",
      "underlying": "BTC",
      "enabled": true,
      "ask": 3949.25,
      "bid": 3949,
      "last": 10579.52,
      "postOnly": false,
      "priceIncrement": 0.25,
      "sizeIncrement": 0.001,
      "restricted": false
}
*/

void MDEngineFtx::makeWebsocketSubscribeJsonString()
{
    vector<string> target;

    string url = "https://ftx.com/api/markets";
    const auto response = Get(Url{ url });
    KF_LOG_INFO(logger, " (response.text) " << response.text.c_str());
    Document d;
    d.Parse(response.text.c_str());

    if (d.IsObject())
    {
        bool success_info = d["success"].GetBool();
        if (success_info)
            KF_LOG_INFO(logger, "MDEngineFtx::makeWebsocketSubscribeJsonString get url success ");
        else
            KF_LOG_ERROR(logger, "MDEngineFtx::makeWebsocketSubscribeJsonString get url failed ");

        int jsonsize = d["result"].Size();
        for (int j = 0; j < jsonsize; j++) {
            std::string name = d["result"].GetArray()[j]["name"].GetString();
            KF_LOG_INFO(logger, "name " << name);
            std::string type = d["result"].GetArray()[j]["type"].GetString();
            KF_LOG_INFO(logger, "type " << type);
            
            std::string underlying;
            if(type == "future")
                underlying = d["result"].GetArray()[j]["underlying"].GetString();

            for (auto& map_itr : coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList()) {
                KF_LOG_INFO(logger, "begin of json circle");

                //map_itr.second from WhiteList
                //name from response
                if (map_itr.second.find("-PERP") != string::npos && name.find("-PERP") != string::npos)
                {
                    if (map_itr.second == name)
                    {
                        KF_LOG_INFO(logger, "MDEngineFtx::makeWebsocketSubscribeJsonString. PERP: " << name << " " << map_itr.second);
                        target.push_back(name);
                        break;
                    }
                }

                else if (map_itr.second.find("-MOVE") != string::npos && name.find("-MOVE") != string::npos)
                {
                    if (name.find(map_itr.second) != string::npos)
                    {
                        KF_LOG_INFO(logger, "MDEngineFtx::makeWebsocketSubscribeJsonString. MOVE: " << name << " " << map_itr.second);
                        target.push_back(name);
                        break;
                    }
                }

                else if (map_itr.second.find("FUTURE") != string::npos) 
                {
                    if(type == "future" && name.find("-PERP") == string::npos && name.find("-MOVE") == string::npos)
                    {
                        if(underlying.empty())
                        {
                            KF_LOG_ERROR(logger, "FUTURE without underlying: " << underlying);
                        }
                        else 
                        {
                            underlying += "-";
                            if(map_itr.second.find(underlying) != string::npos)
                            {
                                KF_LOG_INFO(logger, "MDEngineFtx::makeWebsocketSubscribeJsonString. FUTURE: " << name << " " << map_itr.second);
                                target.push_back(name);
                                break;
                            }
                        }
                    }
                }

                else if (map_itr.second == name ) {
                    target.push_back(name);
                    break;
                }

                KF_LOG_INFO(logger, "end of json circle");
            }
        }

        for (auto& itr : target) {
            KF_LOG_INFO(logger, "begin of target circle");
            std::string jsonBookString = createBookJsonString(itr);
            websocket.SendMessage(jsonBookString);
            std::string jsonTradeString = createTradeJsonString(itr);
            websocket.SendMessage(jsonTradeString);
            KF_LOG_INFO(logger, "sendmessage " << jsonBookString << " " << jsonTradeString);
        }
    }
}

void MDEngineFtx::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineFtx::connect:");
    connected = true;
}

void MDEngineFtx::login(long timeout_nsec) {
    KF_LOG_INFO(logger, "MDEngineFtx::login:");
    websocket.RegisterCallBack(this);
    logged_in = websocket.Connect("ftx.com/ws/");
    timer = getTimestamp();
    KF_LOG_INFO(logger, "MDEngineFtx::login " << (logged_in ? "Success" : "Failed"));
    logged_in = true;
}

void MDEngineFtx::set_reader_thread()
{
    IMDEngine::set_reader_thread();
    quote_requests_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineFtx::quote_requests_loop, this)));
}

void MDEngineFtx::logout()
{
    KF_LOG_INFO(logger, "MDEngineFtx::logout:");
}

void MDEngineFtx::release_api()
{
    KF_LOG_INFO(logger, "MDEngineFtx::release_api:");
}

void MDEngineFtx::subscribeMarketData(const vector<string> & instruments, const vector<string> & markets)
{
    KF_LOG_INFO(logger, "MDEngineFtx::subscribeMarketData:");
}

void MDEngineFtx::OnConnected(const common::CWebsocket* instance)
{
    is_ws_disconnectd = false;
    
    makeWebsocketSubscribeJsonString();
}

void MDEngineFtx::OnReceivedMessage(const common::CWebsocket* instance,const std::string& msg)
{
    KF_LOG_INFO(logger, "MDEngineFtx::on_lws_data: " << msg);
    Document json;
    json.Parse(msg.c_str());
    std::string channel = json["channel"].GetString();

    if (json.HasParseError()) {
        KF_LOG_ERROR(logger, "MDEngineFtx::on_lws_data. parse json error.");
        return;
    }

    if(json.HasMember("data")){    
        if(channel == "orderbook")
        {
            onBook(json);
        }
        else if(channel == "trades")
        {
            onTrade(json);
        }
        else {
            KF_LOG_INFO(logger, "MDEngineFtx::on_lws_data: unknown array data.");
        }
    }
}

void MDEngineFtx::OnDisconnected(const common::CWebsocket* instance)
{
    KF_LOG_ERROR(logger, "MDEngineFtx::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineFtx::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    is_ws_disconnectd = true;
    //login(timeout_nsec);
}
void MDEngineFtx::DoLoopItem()
{
    int64_t now = getTimestamp();
    std::unique_lock<std::mutex> lck1(book_mutex);
    std::map<std::string, int64_t>::iterator it1;
    for(it1 = control_book_map.begin();it1 != control_book_map.end();it1++)
    {
        if((now - it1->second) > refresh_normal_check_book_s * 1000)
        {
            int errorId = 114;
            std::string errorMsg = it1->first + " book max refresh wait time exceeded";
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
int64_t MDEngineFtx::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

void MDEngineFtx::onTrade(Document& json)
{
    KF_LOG_INFO(logger, "MDEngineFtx::onTrade");

    std::string exchange_coinpair = json["market"].GetString();
    std::string ticker = coinPairWhiteList.GetKeyByValue(exchange_coinpair);
    if (ticker.length() == 0) {
        KF_LOG_DEBUG(logger, "MDEngineFtx::onTrade: (ticker.length==0) " << ticker);
        ticker = exchange_coinpair;
        //return;
    }
    //else
    //    ticker = exchange_coinpair;
    KF_LOG_DEBUG(logger, "MDEngineFtx::onTrade: (ticker.length==0) " << ticker);

    auto& data = json["data"];
     int len = data.Size(); 
    for (int i = 0; i < len; i++) {
        LFL2TradeField trade;
        memset(&trade, 0, sizeof(trade));
        strcpy(trade.InstrumentID, ticker.c_str());
        strcpy(trade.ExchangeID, "ftx");
        strcpy(trade.TradeID,std::to_string(data.GetArray()[i]["id"].GetInt64()).c_str());
        trade.Price = std::round(data.GetArray()[i]["price"].GetDouble() * scale_offset);
        trade.Volume = std::round(data.GetArray()[i]["size"].GetDouble() * scale_offset);

        std::string side = data.GetArray()[i]["side"].GetString();
        trade.OrderBSFlag[0] = side == "buy" ? 'B' : 'S';
        trade.Liquidation = data.GetArray()[i]["liquidation"].GetBool();
        string strTime = data.GetArray()[i]["time"].GetString();
        strTime = strTime.substr(0,23);
        strTime+="Z";
        strncpy(trade.TradeTime, strTime.c_str(),sizeof(trade.TradeTime));
        trade.TimeStamp = formatISO8601_to_timestamp(trade.TradeTime)*1000000;
        KF_LOG_INFO(logger, "MDEngineFtx::[onTrade] (ticker)" << ticker <<" (Price)" << trade.Price <<
            " (trade.Volume)" << trade.Volume << "trade.OrderBSFlag[0]" <<trade.OrderBSFlag[0]);
        on_trade(&trade);
        KF_LOG_INFO(logger, "MDEngineFtx::onTrade: Success");
    }

}


void MDEngineFtx::onBook(Document& json)
{
    KF_LOG_INFO(logger, "MDEngineFtx::onBook");

    std::string exchange_coinpair = json["market"].GetString();
    std::string ticker = coinPairWhiteList.GetKeyByValue(exchange_coinpair);
    if (ticker.length() == 0) {
        KF_LOG_DEBUG(logger, "MDEngineFtx::onBook: (ticker.length==0) " << ticker);
        ticker = exchange_coinpair;
        //return;
    }
    else
    //    ticker = exchange_coinpair;
    KF_LOG_INFO(logger, "MDEngineFtx::onBook: (ticker) " << ticker);

    std::string action = json["type"].GetString();
    int64_t time = std::round(json["data"]["time"].GetDouble()*1000000);//micro sec
    //int  checksum = json["data"]["checksum"].GetInt();
    if(action == "partial")//snapshot 
    {
        priceBook20Assembler.clearPriceBook(ticker);
        auto& bids = json["data"]["bids"];  
        int len = bids.Size(); 
        for (int i = 0; i < len; i++) {
            int64_t price = std::round(bids[i][0].GetDouble() * scale_offset);
            uint64_t volume = std::round(bids[i][1].GetDouble() * scale_offset);
            priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
        }
        auto& asks = json["data"]["asks"]; 
        len = asks.Size();  
        for (int i = 0; i < len; i++) {
            int64_t price = std::round(asks[i][0].GetDouble() * scale_offset);
            uint64_t volume = std::round(asks[i][1].GetDouble() * scale_offset);
            priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
        }
    }
    else{
        //KF_LOG_INFO(logger,"update=");
        auto& bids = json["data"]["bids"];  
        int len = bids.Size(); 
        for (int i = 0; i < len; i++) {
            int64_t price = std::round(bids[i][0].GetDouble() * scale_offset);
            uint64_t volume = std::round(bids[i][1].GetDouble() * scale_offset);
            //KF_LOG_INFO(logger,"bids: price="<<price<<" volume="<<volume);
            if(volume == 0){
                //KF_LOG_INFO(logger,"erase");
                priceBook20Assembler.EraseBidPrice(ticker, price);
            }else{
                //KF_LOG_INFO(logger,"update");
                priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
            }
        }
        auto& asks = json["data"]["asks"]; 
        len = asks.Size();  
        for (int i = 0; i < len; i++) {
            int64_t price = std::round(asks[i][0].GetDouble() * scale_offset);
            uint64_t volume = std::round(asks[i][1].GetDouble() * scale_offset);
            //KF_LOG_INFO(logger,"asks: price="<<price<<" volume="<<volume);
            if(volume == 0){
                //KF_LOG_INFO(logger,"erase");
                priceBook20Assembler.EraseAskPrice(ticker, price);
            }else{
                //KF_LOG_INFO(logger,"update");
                priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
            }
        }
        
    }
    
    // has any update
    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));
    if(priceBook20Assembler.Assembler(ticker, md)) {
        md.UpdateMicroSecond = time;
        std::unique_lock<std::mutex> lck1(book_mutex);
        auto it = control_book_map.find(ticker);
        if(it != control_book_map.end())
        {
            it->second = getTimestamp();
        }
        lck1.unlock();
        if(md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold)
        {
            KF_LOG_INFO(logger,"failed ,level count < level threshold");
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"bids["<<i<<"]:"<<md.BidLevels[i].price);
            }
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"asks["<<i<<"]:"<<md.AskLevels[i].price);
            }
            string errorMsg = "orderbook level below threshold";
            md.Status = 1;
            write_errormsg(112,errorMsg);
        }
        else if(md.BidLevels[0].price > md.AskLevels[0].price)
        {
            KF_LOG_INFO(logger,"failed ,orderbook crossed");
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"bids["<<i<<"]:"<<md.BidLevels[i].price);
            }
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"asks["<<i<<"]:"<<md.AskLevels[i].price);
            }
            string errorMsg = "orderbook crossed";
            md.Status = 2;
            write_errormsg(113,errorMsg);
        }
        else
        {
            strcpy(md.ExchangeID, "ftx");
            on_price_book_update(&md);
        }
    }

}

//��֮���Ҷ���ԭ��д��
//{'op': 'subscribe', 'channel': 'orderbook', 'market': 'BTC-PERP'}
std::string MDEngineFtx::createBookJsonString(std::string exchange_coinpair)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("op");
    writer.String("subscribe");

    writer.Key("channel");
    writer.String("orderbook");

    writer.Key("market");
    writer.String(exchange_coinpair.c_str());    

    writer.EndObject();
    return s.GetString();
}

//{"op": "subscribe", "channel": "trades","market": "BTC/USDT"}
std::string MDEngineFtx::createTradeJsonString(std::string exchange_coinpair)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("op");
    writer.String("subscribe");

    writer.Key("channel");
    writer.String("trades");

    writer.Key("market");
    writer.String(exchange_coinpair.c_str());    

    writer.EndObject();
    return s.GetString();
}
int64_t last_quote_time = 0;
void MDEngineFtx::quote_requests_loop()
{
        while(isRunning)
        {
            int64_t now = getTimestamp();
            if((now - last_quote_time) >= quote_get_interval_ms)
            {
                last_quote_time = now;
                get_quote_requests();
            }
        }
}

/*{ "id":182128505,
    "option" : {
        "expiry":"2020-09-04T03:00:00+00:00",
        "strike" : 11500.0,
        "type" : "call",
        "underlying" : "BTC"
    },
    "requestExpiry" : "2020-08-25T02:06:53.375255+00:00",
    "side" : "sell",
    "size" : 10.0012,
    "status" : "open",
    "time" : "2020-08-25T02:01:53.347968+00:00"
}*/

std::set<int64_t> setQuotes;
void MDEngineFtx::get_quote_requests()
{
    std::string url = "https://ftx.com/api/options/requests";
    cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
    Document d;
    d.Parse(response.text.c_str());
    KF_LOG_INFO(logger, "get_quote_requests: " << response.text);
    std::set<int64_t> setCurrentQuotes;
    if(d.IsObject() && d.HasMember("result") && d["result"].IsArray()){
        int len = d["result"].Size();
        for(int i=0; i<len; i++)
        {
            //KF_LOG_INFO(logger,"get_quote_requests i:"<<i);
            //KF_LOG_INFO(logger,"in1");
            auto& node = d["result"].GetArray()[i];
            int64_t id = node["id"].GetInt64();
            setCurrentQuotes.insert(id);
            auto it = setQuotes.find(id);
            if(it != setQuotes.end())
            {
                continue;
            }
            setQuotes.insert(id);
            LFQuoteRequestsField quote;
            memset(&quote, 0, sizeof(quote));
            //KF_LOG_INFO(logger,"in2");
            std::string underlying = node["option"]["underlying"].GetString();
            double strike = node["option"]["strike"].GetDouble();
            char strStrike[20]={};
            sprintf(strStrike,"%.0lf",strike);
            std::string type = node["option"]["type"].GetString();
            type = type.substr(0,1);
            std::string ticker = underlying + "_" + std::string(strStrike) + "_" + type;
            transform(ticker.begin(),ticker.end(),ticker.begin(),::tolower);
            strcpy(quote.InstrumentID, ticker.c_str());
            strcpy(quote.ExchangeID, "ftx");
            //KF_LOG_INFO(logger,"in4");
            quote.ID = id;

            std::string side = node["side"].GetString();
            //KF_LOG_INFO(logger,"in5");
            quote.OrderBSFlag[0] = side == "buy" ? 'B' : 'S';
            //2020-07-23T15:28:42.296607+00:00
            std::string requestExpiry = node["requestExpiry"].GetString();
            requestExpiry = requestExpiry.substr(0,19);
            //KF_LOG_INFO(logger,"in6");
            strcpy(quote.RequestExpiry, requestExpiry.c_str());
            //edit next line
            std::string expiry = node["option"]["expiry"].GetString();
            expiry = expiry.substr(0,19);
            //KF_LOG_INFO(logger,"in6");
            strcpy(quote.Expiry, expiry.c_str());
            std::string strtime = node["time"].GetString();
            strcpy(quote.Time, strtime.c_str());
            std::string status = node["status"].GetString();
            //KF_LOG_INFO(logger,"in7");
            strcpy(quote.Status, status.c_str());
            quote.Volume = std::round(node["size"].GetDouble() * scale_offset);
            //KF_LOG_INFO(logger,"in8");
            if(node.HasMember("limitPrice")){
                quote.Price = std::round(node["limitPrice"].GetDouble() * scale_offset);
            }
            //KF_LOG_INFO(logger,"in9");

            on_quote_requests(&quote);
        }
    }
    setQuotes = setCurrentQuotes;
}

BOOST_PYTHON_MODULE(libftxmd)
{
    using namespace boost::python;
    class_<MDEngineFtx, boost::shared_ptr<MDEngineFtx> >("Engine")
        .def(init<>())
        .def("init", &MDEngineFtx::initialize)
        .def("start", &MDEngineFtx::start)
        .def("stop", &MDEngineFtx::stop)
        .def("logout", &MDEngineFtx::logout)
        .def("wait_for_stop", &MDEngineFtx::wait_for_stop);
}

