/*1.计算逻辑
2.测
3.WEB
4.监听*/
#include <iostream>
#include "IWCStrategy.h"
#include <deque>
#include <map>
#include <queue>
#include <cpr/cpr.h>
#include <writer.h>
#include <stringbuffer.h>
#include <document.h>
#include <ctime>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <libwebsockets.h>
#include "longfist/LFUtils.h"
#include "JournalReader.h"
#include "Timer.h"
#include "longfist/sys_messages.h"
#include "longfist/LFConstants.h"

using cpr::Get;
using cpr::Url;
using cpr::Timeout;
using rapidjson::StringRef;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using namespace std;
USING_WC_NAMESPACE

std::mutex queue_mutex;
std::mutex time_mutex;

#define SOURCE_INDEX12 SOURCE_ERISX

#define M_TICKER "TRXBTC"
#define M_EXCHANGE EXCHANGE_SHFE
#define TRADED_VOLUME_LIMIT 500

#define KGRN "\033[0;32;32m"
#define KCYN "\033[0;36m"
#define KRED "\033[0;32;31m"
#define KYEL "\033[1;33m"
#define KMAG "\033[0;35m"
#define KBLU "\033[0;32;34m"
#define KCYN_L "\033[1;36m"
#define RESET "\033[0m"

//double maker_fee_rate = 0.1;
//double taker_fee_rate = 0.1;
JournalReaderPtr reader;
vector<string> folders;
vector<string> names;
map<string,string> client_map;

struct RspOMsg
{
    string strategy;
    string tag;
};

struct Msg
{
    string instrument;
    string strategy;
    string start_time;
    string exchange; 
    double maker_buy_vol = 0;
    double maker_sell_vol = 0;
    double taker_buy_vol = 0;
    double taker_sell_vol = 0;
    double net_pos = 0;
    double maker_fees = 0;
    double taker_fees = 0;
    double ave_maker_buy_px = 0;
    double ave_maker_sell_px = 0;
    double ave_taker_buy_px = 0;
    double ave_taker_sell_px = 0;
    double maker_profit = 0;
    double taker_profit = 0;
    double total_profit = 0;
    double total_profit_usd = 0;
    //
    double maker_fee_rate;
    double taker_fee_rate;
    //string quote_currency;
    //下面是中间变量
    double total_maker_buy_px = 0;
    double total_maker_sell_px = 0;
    double total_taker_buy_px = 0;
    double total_taker_sell_px = 0;    
};

struct Info
{
    map<string, Msg> info_map;//(exchange_name,msg)
    Msg global_mag;
    string quote_currency;
};
map<string, Info> form_map;//(strtegy_symbol,exchange)
map<int, RspOMsg> tag_map;//(request_id,204msg);
queue<string> json_queue;
map<string, string> time_map;//(strtegyname,start_time)

/*struct Signal
{
    string name;
    int look_back;
    int param1;
    int param2;
    int trade_size;
    std::deque<double> TickPrice;
    bool has_open_position;
    bool has_open_long_position;
    bool has_open_short_position;
};*/

class Strategy: public IWCStrategy
{
protected:
    bool td_connected;
    bool trade_completed;
    int rid;
    int cancel_id;
    int md_num;
    int traded_volume;
    //Signal signal;
public:
    virtual void init();
    //virtual void on_market_data(const LFMarketDataField* data, int source, long rcv_time);
    //virtual void on_rsp_position(const PosHandlerPtr posMap, int request_id, int source, long rcv_time);
    virtual void on_rtn_order(const LFRtnOrderField* data, int request_id, int source, long rcv_time);
    virtual void on_rtn_trade(const LFRtnTradeField* data, int request_id, int source, long rcv_time);
    virtual void on_rsp_order(const LFInputOrderField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    //virtual void on_price_book_update(const LFPriceBook20Field* data, int source, long rcv_time);

public:
    Strategy(const string& name);
};



//init()

Strategy::Strategy(const string& name): IWCStrategy(name)
{
    std::cout << "[Strategy] (Strategy)" <<std::endl;
    rid = -1;
    cancel_id = -1;

}



static int destroy_flag = 0;

static void INT_HANDLER(int signo) {
    destroy_flag = 0;
}

static void
sigint_handler(int sig)
{
    destroy_flag = 1;
}

string dealstring(string b)
{
    for(int i=b.length(); i>0; i--){
        //cout<<i<<endl;
        string end = b.substr(b.length()-1, b.length());
        if(end == "0" || end == "."){
            b = b.substr(0,b.length()-1);
            if(end == "."){
                break;
            }
            //cout<<b<<endl;
        }else{
            break;
        }
    }
    return b;   
}

string get_time()
{
    time_t now = time(0);
    //tm *ltm = localtime(&now);
    //string now_str = to_string(1900 + ltm->tm_year) + "/" + to_string(1 + ltm->tm_mon) + "/" + to_string(ltm->tm_mday) + " " + to_string(ltm->tm_hour) + ":" + to_string(ltm->tm_min) + ":" + to_string(ltm->tm_sec);
    string now_str = ctime(&now);
    return now_str;
}
/* *
 * websocket_write_back: write the string data to the destination wsi.
 *//*
int websocket_write_back(struct lws *wsi_in, char *str, int str_size_in) 
{
    if (str == NULL || wsi_in == NULL)
        return -1;

    int n;
    int len;
    unsigned char *out = NULL;

    if (str_size_in < 1) 
        len = strlen(str);
    else
        len = str_size_in;

    out = (unsigned char *)malloc(sizeof(unsigned char)*(LWS_SEND_BUFFER_PRE_PADDING + len + LWS_SEND_BUFFER_POST_PADDING));
    //setup the buffer
    memcpy (out + LWS_SEND_BUFFER_PRE_PADDING, str, len );
    //write out
    n = lws_write(wsi_in, out + LWS_SEND_BUFFER_PRE_PADDING, len, LWS_WRITE_TEXT);

    //printf(KBLU"[websocket_write_back] %s\n"RESET, str);
    cout<<"[websocket_write_back]"<<endl;
    //free the buffer
    free(out);

    return n;
}*/

int lws_write_send(struct lws *conn)
{
    //cout<<"[lws_write_send]"<<endl;
    int ret;
    std::unique_lock<std::mutex> lck(queue_mutex);
    if(json_queue.size() > 0) {
        unsigned char msg[1024];
        memset(&msg[LWS_PRE], 0, 1024-LWS_PRE);

        std::string jsonString = json_queue.front();
        json_queue.pop();
        lck.unlock();
        cout << jsonString<<endl;
        int length = jsonString.length();
        //cout<<"length"<<length<<endl;
        strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
        //cout<<"will write"<<endl;
        ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

        //if(websocketPendingSendMsg.size() > 0)
        //{    //still has pending send data, emit a lws_callback_on_writable()
        //cout<<"writealbe1 "<<endl;
            lws_callback_on_writable( conn );
        //    cout<<"writealbe2 "<<endl;
            //KF_LOG_INFO(logger, "MDEnginebitFlyer::lws_write_subscribe: (websocketPendingSendMsg,size)" << websocketPendingSendMsg.size());
        //}
        return ret;
    }
    lck.unlock();
    lws_callback_on_writable( conn );
    return ret;
}

static int ws_service_callback(
                         struct lws *wsi,
                         enum lws_callback_reasons reason, void *user,
                         void *in, size_t len)
{
    //cout<<"reason"<<reason<<endl;
    switch (reason) {

        case LWS_CALLBACK_ESTABLISHED:
            //printf(KYEL"[Main Service] Connection established\n"RESET);
            cout<<"[Main Service] Connection established"<<endl;
            lws_callback_on_writable( wsi );
            break;

        //* If receive a data from client*/
        /*case LWS_CALLBACK_RECEIVE:
            //printf(KCYN_L"[Main Service] Server recvived:%s\n"RESET,(char *)in);
            cout<<"[Main Service] Server recvived"<<endl;
            //echo back to client
            websocket_write_back(wsi ,(char *)in, -1);

            break;*/
        case LWS_CALLBACK_SERVER_WRITEABLE:
        //{
            //if(global_md)
            //{
            //cout<<"[Main Service] Server write"<<endl;
            lws_write_send(wsi);
            //}
            break;
        //}
    case LWS_CALLBACK_CLOSED:
            //printf(KYEL"[Main Service] Client close.\n"RESET);
            cout<<"[Main Service] Client close."<<endl;
        break;

    default:
            break;
    }

    return 0;
}

struct per_session_data {
    int fd;
};

string readfile(const char *filename){
    FILE *fp = fopen(filename, "rb");
    if(!fp){
        //printf("open failed! file: %s", filename);
        cout<<"open failed!"<<endl;
        return "";
    }
    
    char *buf = new char[1024*16];
    int n = fread(buf, 1, 1024*16, fp);
    fclose(fp);
    
    string result;
    if(n>=0){
        result.append(buf, 0, n);
    }
    delete []buf;
    return result;
}

double Get_usd_rate(string ticker){
    int flag = ticker.find("_");
    string currencies = ticker.substr(flag + 1, ticker.length());
    transform(currencies.begin(), currencies.end(), currencies.begin(), ::toupper);

    string url = "https://api.kucoin.com/api/v1/prices?base=USD&currencies=" + currencies;
    cpr::Response response = Get(Url{url},Timeout{10000});
    /*cout<<"[Get] (url) " << url << " (response.status_code) " << response.status_code <<
      " (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str()<<endl;*/

    string rate;
    char_64 tickerchar;
    strcpy(tickerchar,currencies.c_str());  
    Document json;
    json.Parse(response.text.c_str());
    if(!json.HasParseError() && json.IsObject()){
        rate = json["data"][tickerchar].GetString();
    }
    double usd_rate = stod(rate);
    return usd_rate;   
}

void Init_map()
{
    string pnl_msg = readfile("../pnl.json");
    Document json;
    json.Parse(pnl_msg.c_str());

    Info info;
    Msg msg;

    /*msg.maker_fee_rate = json["rate"]["kucoin"]["maker_fee_rate"].GetDouble();
    msg.taker_fee_rate = json["rate"]["kucoin"]["taker_fee_rate"].GetDouble();
    info.info_map.insert(make_pair("kucoin", msg));
    msg.maker_fee_rate = json["rate"]["binance"]["maker_fee_rate"].GetDouble();
    msg.taker_fee_rate = json["rate"]["binance"]["taker_fee_rate"].GetDouble();    
    info.info_map.insert(make_pair("binance", msg));
    msg.maker_fee_rate = json["rate"]["huobi"]["maker_fee_rate"].GetDouble();
    msg.taker_fee_rate = json["rate"]["huobi"]["taker_fee_rate"].GetDouble();
    info.info_map.insert(make_pair("huobi", msg));
    msg.maker_fee_rate = json["rate"]["bequant"]["maker_fee_rate"].GetDouble();
    msg.taker_fee_rate = json["rate"]["bequant"]["taker_fee_rate"].GetDouble();
    info.info_map.insert(make_pair("bequant", msg));*/

    Value node = json["rate"].GetObject();
    for (rapidjson::Value::ConstMemberIterator itr = node.MemberBegin();itr != node.MemberEnd(); ++itr){
        string exchange = itr->name.GetString();
        cout<<exchange<<endl;
        char_64 exname;
        strcpy(exname, exchange.c_str());
        msg.maker_fee_rate = node[exname]["maker_fee_rate"].GetDouble();
        msg.taker_fee_rate = node[exname]["taker_fee_rate"].GetDouble();    
        info.info_map.insert(make_pair(exchange, msg));        
    }
    cout<<"info_map.size()"<<info.info_map.size()<<endl;

    int size = json["strategy_instrument"].Size();
    for(int i=0; i < size; i++){
        string strategy = json["strategy_instrument"].GetArray()[i]["strategy"].GetString();
        string instrument = json["strategy_instrument"].GetArray()[i]["instrument"].GetString();
        info.quote_currency = json["strategy_instrument"].GetArray()[i]["quote_currency"].GetString();
        string name = strategy + "/" + instrument;
        form_map.insert(make_pair(name, info));
        //form_map.insert(make_pair("aa/eth_btc", info));
    }
}

void Strategy::on_rsp_order(const LFInputOrderField* order, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{

}

void Strategy::on_rtn_order(const LFRtnOrderField* data, int request_id, int source, long rcv_time){

}

void Strategy::on_rtn_trade(const LFRtnTradeField* rtn_trade, int request_id, int source, long rcv_time)
{

}

void Deal_204(const LFInputOrderField* order, int request_id)
{
    //cout<<"strategy:"<<order->BusinessUnit<<endl;
    RspOMsg rspomsg;
    rspomsg.strategy = order->BusinessUnit;
    rspomsg.tag = order->MiscInfo;
    tag_map.insert(make_pair(request_id, rspomsg));
}

void Deal_205(const LFRtnOrderField* data, int request_id){
    //cout<<"on_rtn_order"<<endl;
    cout<<"data->OrderStatus="<<data->OrderStatus<<endl;
    if(data->OrderStatus == LF_CHAR_Canceled){
        auto it = tag_map.find(request_id);
        if(it != tag_map.end()){
            tag_map.erase(it);
        }
    }else if(data->OrderStatus == LF_CHAR_AllTraded){
        //
    }
}

void Deal_206(const LFRtnTradeField* rtn_trade, int request_id)
{
    double price = rtn_trade->Price/1e8;
    double volume = rtn_trade->Volume/1e8;
    string strategy,tag;
    auto it = tag_map.find(request_id);
    if(it != tag_map.end()){
        strategy = it->second.strategy;
        tag = it->second.tag;
    }
    string name = strategy + "/" + rtn_trade->InstrumentID;
    string mt_flag = tag.substr(0,1);

    auto it1 = form_map.find(name);
    if(it1 != form_map.end()){

        auto it2 = (*it1).second.info_map.find(rtn_trade->ExchangeID);
        if(it2 != (*it1).second.info_map.end()){

            cout<<endl<<"in change"<<endl;
            it2->second.instrument = it1->second.global_mag.instrument = rtn_trade->InstrumentID;
            it2->second.strategy = it1->second.global_mag.strategy = strategy;
            string starttime;
            std::unique_lock<std::mutex> lck1(time_mutex);
            auto it4 = time_map.find(strategy);
            if(it4 != time_map.end()){
                starttime = it4->second;
            }
            lck1.unlock();
            it2->second.start_time = it1->second.global_mag.start_time = starttime;
            it2->second.exchange = rtn_trade->ExchangeID;
            it1->second.global_mag.exchange = "global";

            if(rtn_trade->Direction == LF_CHAR_Buy && mt_flag == "0"){

                it2->second.maker_buy_vol += volume;
                it1->second.global_mag.maker_buy_vol += volume;

                it2->second.total_maker_buy_px += price * volume;
                it1->second.global_mag.total_maker_buy_px += price * volume;

                if(it2->second.maker_buy_vol != 0){
                    it2->second.ave_maker_buy_px = (it2->second.total_maker_buy_px)/(it2->second.maker_buy_vol);
                    it1->second.global_mag.ave_maker_buy_px = (it1->second.global_mag.total_maker_buy_px)/(it1->second.global_mag.maker_buy_vol);
                }

                it2->second.maker_fees = it2->second.maker_fee_rate * (it2->second.maker_buy_vol + it2->second.maker_sell_vol);
                it1->second.global_mag.maker_fees += it2->second.maker_fee_rate * volume;

                it2->second.maker_profit = (it2->second.ave_maker_sell_px - it2->second.ave_maker_buy_px) * min(it2->second.maker_buy_vol,it2->second.maker_sell_vol) - 2 * it2->second.maker_fee_rate * min(it2->second.maker_buy_vol,it2->second.maker_sell_vol);
                map<string, Msg>::iterator it3;
                for(it3 = (*it1).second.info_map.begin(); it3 != (*it1).second.info_map.end(); it3++){
                    it1->second.global_mag.maker_profit = 0;
                    it1->second.global_mag.maker_profit += it3->second.maker_profit;
                }

            }else if(rtn_trade->Direction == LF_CHAR_Sell && mt_flag == "0"){ 

                it2->second.maker_sell_vol += volume;
                it1->second.global_mag.maker_sell_vol += volume;

                it2->second.total_maker_sell_px += price * volume;
                it1->second.global_mag.total_maker_sell_px += price * volume;

                if(it2->second.maker_sell_vol != 0){
                    it2->second.ave_maker_sell_px = (it2->second.total_maker_sell_px)/(it2->second.maker_sell_vol);
                    it1->second.global_mag.ave_maker_sell_px = (it1->second.global_mag.total_maker_sell_px)/(it1->second.global_mag.maker_sell_vol);
                }

                it2->second.maker_fees = it2->second.maker_fee_rate * (it2->second.maker_buy_vol + it2->second.maker_sell_vol);
                it1->second.global_mag.maker_fees += it2->second.maker_fee_rate * volume;

                it2->second.maker_profit = (it2->second.ave_maker_sell_px - it2->second.ave_maker_buy_px) * min(it2->second.maker_buy_vol,it2->second.maker_sell_vol) - 2 * it2->second.maker_fee_rate * min(it2->second.maker_buy_vol,it2->second.maker_sell_vol);
                map<string, Msg>::iterator it3;
                for(it3 = (*it1).second.info_map.begin(); it3 != (*it1).second.info_map.end(); it3++){
                    it1->second.global_mag.maker_profit = 0;
                    it1->second.global_mag.maker_profit += it3->second.maker_profit;
                }

            }else if(rtn_trade->Direction == LF_CHAR_Buy && mt_flag == "1"){

                it2->second.taker_buy_vol += volume;
                it1->second.global_mag.taker_buy_vol += volume;

                it2->second.total_taker_buy_px += price * volume;
                it1->second.global_mag.total_taker_buy_px += price * volume;

                if(it2->second.taker_buy_vol != 0){
                    it2->second.ave_taker_buy_px = (it2->second.total_taker_buy_px)/(it2->second.taker_buy_vol);
                    it1->second.global_mag.ave_taker_buy_px = (it1->second.global_mag.total_taker_buy_px)/(it1->second.global_mag.taker_buy_vol);
                }

                it2->second.taker_fees = it2->second.taker_fee_rate * (it2->second.taker_buy_vol + it2->second.taker_sell_vol);
                it1->second.global_mag.taker_fees += it2->second.taker_fee_rate * volume;

                it2->second.taker_profit = (it2->second.ave_taker_sell_px - it2->second.ave_taker_buy_px) * min(it2->second.taker_buy_vol,it2->second.taker_sell_vol) - 2 * it2->second.taker_fee_rate * min(it2->second.taker_buy_vol,it2->second.taker_sell_vol);
                map<string, Msg>::iterator it3;
                for(it3 = (*it1).second.info_map.begin(); it3 != (*it1).second.info_map.end(); it3++){
                    it1->second.global_mag.taker_profit = 0;
                    it1->second.global_mag.taker_profit += it3->second.taker_profit;
                }

            }else{

                it2->second.taker_sell_vol += volume;
                it1->second.global_mag.taker_sell_vol += volume;

                it2->second.total_taker_sell_px += price * volume;
                it1->second.global_mag.total_taker_sell_px += price * volume;

                if(it2->second.taker_sell_vol != 0){
                    it2->second.ave_taker_sell_px = (it2->second.total_taker_sell_px)/(it2->second.taker_sell_vol);
                    it1->second.global_mag.ave_taker_sell_px = (it1->second.global_mag.total_taker_sell_px)/(it1->second.global_mag.taker_sell_vol);
                }

                it2->second.taker_fees = it2->second.taker_fee_rate * (it2->second.taker_buy_vol + it2->second.taker_sell_vol);
                it1->second.global_mag.taker_fees += it2->second.taker_fee_rate * volume;

                it2->second.taker_profit = (it2->second.ave_taker_sell_px - it2->second.ave_taker_buy_px) * min(it2->second.taker_buy_vol,it2->second.taker_sell_vol) - 2 * it2->second.taker_fee_rate * min(it2->second.taker_buy_vol,it2->second.taker_sell_vol);
                map<string, Msg>::iterator it3;
                for(it3 = (*it1).second.info_map.begin(); it3 != (*it1).second.info_map.end(); it3++){
                    it1->second.global_mag.taker_profit = 0;
                    it1->second.global_mag.taker_profit += it3->second.taker_profit;
                }

            }

            it2->second.net_pos = it2->second.maker_buy_vol + it2->second.taker_buy_vol - it2->second.maker_sell_vol - it2->second.taker_sell_vol;
            it1->second.global_mag.net_pos = it1->second.global_mag.maker_buy_vol + it1->second.global_mag.taker_buy_vol - it1->second.global_mag.maker_sell_vol - it1->second.global_mag.taker_sell_vol;
            
            //it2->second.maker_fees = it2->second.maker_fee_rate * (it2->second.maker_buy_vol + it2->second.maker_sell_vol);
            //it2->second.taker_fees = it2->second.taker_fee_rate * (it2->second.taker_buy_vol + it2->second.taker_sell_vol);

                /*if(it2->second.maker_buy_vol != 0){
                    it2->second.ave_maker_buy_px = (it2->second.total_maker_buy_px)/(it2->second.maker_buy_vol);
                    it1->second.global_mag.ave_maker_buy_px = (it1->second.global_mag.total_maker_buy_px)/(it1->second.global_mag.maker_buy_vol);
                }
                if(it2->second.maker_sell_vol != 0){
                    it2->second.ave_maker_sell_px = (it2->second.total_maker_sell_px)/(it2->second.maker_sell_vol);
                    it1->second.global_mag.ave_maker_sell_px = (it1->second.global_mag.total_maker_sell_px)/(it1->second.global_mag.maker_sell_vol);
                }
                if(it2->second.taker_buy_vol != 0){
                    it2->second.ave_taker_buy_px = (it2->second.total_taker_buy_px)/(it2->second.taker_buy_vol);
                    it1->second.global_mag.ave_taker_buy_px = (it1->second.global_mag.total_taker_buy_px)/(it1->second.global_mag.taker_buy_vol);
                }
                if(it2->second.taker_sell_vol != 0){
                    it2->second.ave_taker_sell_px = (it2->second.total_taker_sell_px)/(it2->second.taker_sell_vol);
                    it1->second.global_mag.ave_taker_sell_px = (it1->second.global_mag.total_taker_sell_px)/(it1->second.global_mag.taker_sell_vol);
                }*/

            //it2->second.maker_profit = (it2->second.ave_maker_sell_px - it2->second.ave_maker_buy_px) * min(it2->second.maker_buy_vol,it2->second.maker_sell_vol) - 2 * it2->second.maker_fee_rate * min(it2->second.maker_buy_vol,it2->second.maker_sell_vol);
            //it2->second.taker_profit = (it2->second.ave_taker_sell_px - it2->second.ave_taker_buy_px) * min(it2->second.taker_buy_vol,it2->second.taker_sell_vol) - 2 * it2->second.taker_fee_rate * min(it2->second.taker_buy_vol,it2->second.taker_sell_vol);
            
            it2->second.total_profit = it2->second.maker_profit + it2->second.taker_profit;
            it1->second.global_mag.total_profit = it1->second.global_mag.maker_profit + it1->second.global_mag.taker_profit;

            double usd_rate = Get_usd_rate(string(rtn_trade->InstrumentID));

            it2->second.total_profit_usd = it2->second.total_profit * usd_rate;
            it1->second.global_mag.total_profit_usd = it1->second.global_mag.total_profit * usd_rate;

            /*cout<<"instrument:"<<it2->second.instrument<<endl;
            cout<<"strategy:"<<it2->second.strategy<<endl;
            cout<<"start_time:"<<it2->second.start_time<<endl;
            cout<<"exchange:"<<it2->second.exchange<<endl; 
            cout<<"maker_buy_vol:"<<it2->second.maker_buy_vol<<endl;
            cout<<"maker_sell_vol:"<<it2->second.maker_sell_vol<<endl;
            cout<<"taker_buy_vol:"<<it2->second.taker_buy_vol<<endl;
            cout<<"taker_sell_vol:"<<it2->second.taker_sell_vol<<endl;
            cout<<"net_pos:"<<it2->second.net_pos<<endl;
            cout<<"maker_fees:"<<it2->second.maker_fees<<endl;
            cout<<"taker_fees:"<<it2->second.taker_fees<<endl;
            cout<<"ave_maker_buy_px:"<<it2->second.ave_maker_buy_px<<endl;
            cout<<"ave_maker_sell_px:"<<it2->second.ave_maker_sell_px<<endl;
            cout<<"ave_taker_buy_px:"<<it2->second.ave_taker_buy_px<<endl;
            cout<<"ave_taker_sell_px:"<<it2->second.ave_taker_sell_px<<endl;
            cout<<"maker_profit:"<<it2->second.maker_profit<<endl;
            cout<<"taker_profit:"<<it2->second.taker_profit<<endl;
            cout<<"total_profit:"<<it2->second.total_profit<<endl;
            cout<<"total_profit_usd:"<<it2->second.total_profit_usd<<endl;

            cout<<"(global)instrument:"<<it1->second.global_mag.instrument<<endl;
            cout<<"(global)strategy:"<<it1->second.global_mag.strategy<<endl;
            cout<<"(global)start_time:"<<it1->second.global_mag.start_time<<endl;
            cout<<"(global)exchange:"<<it1->second.global_mag.exchange<<endl; 
            cout<<"(global)maker_buy_vol:"<<it1->second.global_mag.maker_buy_vol<<endl;
            cout<<"(global)maker_sell_vol:"<<it1->second.global_mag.maker_sell_vol<<endl;
            cout<<"(global)taker_buy_vol:"<<it1->second.global_mag.taker_buy_vol<<endl;
            cout<<"(global)taker_sell_vol:"<<it1->second.global_mag.taker_sell_vol<<endl;
            cout<<"(global)net_pos:"<<it1->second.global_mag.net_pos<<endl;
            cout<<"(global)maker_fees:"<<it1->second.global_mag.maker_fees<<endl;
            cout<<"(global)taker_fees:"<<it1->second.global_mag.taker_fees<<endl;
            cout<<"(global)ave_maker_buy_px:"<<it1->second.global_mag.ave_maker_buy_px<<endl;
            cout<<"(global)ave_maker_sell_px:"<<it1->second.global_mag.ave_maker_sell_px<<endl;
            cout<<"(global)ave_taker_buy_px:"<<it1->second.global_mag.ave_taker_buy_px<<endl;
            cout<<"(global)ave_taker_sell_px:"<<it1->second.global_mag.ave_taker_sell_px<<endl;
            cout<<"(global)maker_profit:"<<it1->second.global_mag.maker_profit<<endl;
            cout<<"(global)taker_profit:"<<it1->second.global_mag.taker_profit<<endl;
            cout<<"(global)total_profit:"<<it1->second.global_mag.total_profit<<endl;
            cout<<"(global)total_profit_usd:"<<it1->second.global_mag.total_profit_usd<<endl;*/

            StringBuffer s;
            Writer<StringBuffer> writer(s);
            writer.StartObject();

            writer.Key("instrument");
            writer.String(it2->second.instrument.c_str());

            writer.Key("strategy");
            writer.String(it2->second.strategy.c_str());

            writer.Key("start_time");
            writer.String(it2->second.start_time.c_str());

            writer.Key("data");
            writer.StartArray();
            writer.StartObject();

            writer.Key("exchange");
            writer.String(it1->second.global_mag.exchange.c_str());

            writer.Key("maker_buy_vol");
            writer.String(dealstring(to_string(it1->second.global_mag.maker_buy_vol)).c_str());

            writer.Key("maker_sell_vol");
            writer.String(dealstring(to_string(it1->second.global_mag.maker_sell_vol)).c_str());

            writer.Key("taker_buy_vol");
            writer.String(dealstring(to_string(it1->second.global_mag.taker_buy_vol)).c_str());

            writer.Key("taker_sell_vol");
            writer.String(dealstring(to_string(it1->second.global_mag.taker_sell_vol)).c_str());

            writer.Key("net_pos");
            writer.String(dealstring(to_string(it1->second.global_mag.net_pos)).c_str());

            writer.Key("maker_fees");
            writer.String(dealstring(to_string(it1->second.global_mag.maker_fees)).c_str());

            writer.Key("taker_fees");
            writer.String(dealstring(to_string(it1->second.global_mag.taker_fees)).c_str());

            writer.Key("ave_maker_buy_px");
            writer.String(dealstring(to_string(it1->second.global_mag.ave_maker_buy_px)).c_str());

            writer.Key("ave_maker_sell_px");
            writer.String(dealstring(to_string(it1->second.global_mag.ave_maker_sell_px)).c_str());

            writer.Key("ave_taker_buy_px");
            writer.String(dealstring(to_string(it1->second.global_mag.ave_taker_buy_px)).c_str());

            writer.Key("ave_taker_sell_px");
            writer.String(dealstring(to_string(it1->second.global_mag.ave_taker_sell_px)).c_str());

            writer.Key("maker_profit");
            writer.String(dealstring(to_string(it1->second.global_mag.maker_profit)).c_str());

            writer.Key("taker_profit");
            writer.String(dealstring(to_string(it1->second.global_mag.taker_profit)).c_str());

            writer.Key("total_profit");
            writer.String(dealstring(to_string(it1->second.global_mag.total_profit)).c_str());

            writer.Key("total_profit_usd");
            writer.String(dealstring(to_string(it1->second.global_mag.total_profit_usd)).c_str());
            writer.EndObject();

            writer.StartObject();

            writer.Key("exchange");
            writer.String(it2->second.exchange.c_str());

            writer.Key("maker_buy_vol");
            writer.String(dealstring(to_string(it2->second.maker_buy_vol)).c_str());

            writer.Key("maker_sell_vol");
            writer.String(dealstring(to_string(it2->second.maker_sell_vol)).c_str());

            writer.Key("taker_buy_vol");
            writer.String(dealstring(to_string(it2->second.taker_buy_vol)).c_str());

            writer.Key("taker_sell_vol");
            writer.String(dealstring(to_string(it2->second.taker_sell_vol)).c_str());

            writer.Key("net_pos");
            writer.String(dealstring(to_string(it2->second.net_pos)).c_str());

            writer.Key("maker_fees");
            writer.String(dealstring(to_string(it2->second.maker_fees)).c_str());

            writer.Key("taker_fees");
            writer.String(dealstring(to_string(it2->second.taker_fees)).c_str());

            writer.Key("ave_maker_buy_px");
            writer.String(dealstring(to_string(it2->second.ave_maker_buy_px)).c_str());

            writer.Key("ave_maker_sell_px");
            writer.String(dealstring(to_string(it2->second.ave_maker_sell_px)).c_str());

            writer.Key("ave_taker_buy_px");
            writer.String(dealstring(to_string(it2->second.ave_taker_buy_px)).c_str());

            writer.Key("ave_taker_sell_px");
            writer.String(dealstring(to_string(it2->second.ave_taker_sell_px)).c_str());

            writer.Key("maker_profit");
            writer.String(dealstring(to_string(it2->second.maker_profit)).c_str());

            writer.Key("taker_profit");
            writer.String(dealstring(to_string(it2->second.taker_profit)).c_str());

            writer.Key("total_profit");
            writer.String(dealstring(to_string(it2->second.total_profit)).c_str());

            writer.Key("total_profit_usd");
            writer.String(dealstring(to_string(it2->second.total_profit_usd)).c_str());
            writer.EndObject();
            writer.EndArray();

            writer.EndObject();
            cout<<"s="<<s.GetString()<<endl;
            std::unique_lock<std::mutex> lck(queue_mutex);
            json_queue.push(s.GetString());
            lck.unlock();

        }
    }
}

void start_server()
{
    // server url will usd port 5000
    int port = 5000;
    const char *interface = NULL;
    struct lws_context_creation_info info;
    struct lws_protocols protocol;
    struct lws_context *context;
    // Not using ssl
    const char *cert_path = NULL;
    const char *key_path = NULL;
    // no special options
    int opts = 0;


    //* register the signal SIGINT handler */
    struct sigaction act;
    act.sa_handler = INT_HANDLER;
    act.sa_flags = 0;
    sigemptyset(&act.sa_mask);
    sigaction( SIGINT, &act, 0);

    //* setup websocket protocol */
    protocol.name = "my-echo-protocol";
    protocol.callback = ws_service_callback;
    protocol.per_session_data_size=sizeof(struct per_session_data);
    protocol.rx_buffer_size = 0;

    //* setup websocket context info*/
    memset(&info, 0, sizeof info);
    info.port = port;
    info.iface = interface;
    info.protocols = &protocol;
    info.extensions = NULL;
    info.ssl_cert_filepath = cert_path;
    info.ssl_private_key_filepath = key_path;
    info.gid = -1;
    info.uid = -1;
    info.options = opts;

    //* create libwebsocket context. */
    context = lws_create_context(&info);
    if (context == NULL) {
        //printf(KRED"[Main] Websocket context create error.\n"RESET);
        cout<<"[Main] Websocket context create error."<<endl;
        //return -1;
        return;
    }

    //printf(KGRN"[Main] Websocket context create success.\n"RESET);
    cout<<"[Main] Websocket context create success."<<endl;

    //* websocket service */
    while ( !destroy_flag ) {
        signal(SIGINT, sigint_handler);
        lws_service(context, 50);
    }
    usleep(10);
    cout<<"lws_context_destroy"<<endl;
    lws_context_destroy(context);  
    return;
}

int add_client(const string name,const string folder)
{
    //KF_LOG_INFO(logger, "Add Client Start!");
    auto iter = client_map.find(folder);
    if(iter != client_map.end())
    {
        //KF_LOG_INFO(logger, "Client already exist: " << name);
        return 0;
    }
    client_map[folder] = name;
    //KF_LOG_INFO(logger, "folder:" << folder << " name:" << name);
    reader->addJournal(folder, name);
    //KF_LOG_INFO(logger, "Add Client End!");
    return 1;
}

void listen_time()
{
    cout<<"[listen_time]"<<endl;
    auto mapSources = get_map_sources();
    for(auto iter = mapSources.begin();iter != mapSources.end();iter++)
    {
        JournalPair jp = getMdJournalPair(iter->first);
        folders.push_back(jp.first);
        names.push_back(jp.second);
        jp = getTdJournalPair(iter->first);
        folders.push_back(jp.first);
        names.push_back(jp.second);
    }
    reader = kungfu::yijinjing::JournalReader::createReaderWithSys(folders, names, kungfu::yijinjing::getNanoTime(), "reader_");

    FramePtr frame;
    while (1){
        frame = reader->getNextFrame();
        if (frame.get() != nullptr)
        {
            short msg_type = frame->getMsgType();
            short msg_source = frame->getSource();
            //long cur_time = frame->getNano();
            int request_id = frame->getRequestId();
            //cout<<"request_id:"<<request_id<<endl;
            if (msg_type == MSG_TYPE_STRATEGY_START)
            {
                try
                {
                    string content((char*)frame->getData());
                    json j_request = json::parse(content);
                    string folder = j_request["folder"].get<string>();
                    string name = j_request["name"].get<string>();
                    //if (add_client(name,folder))
                    //{
                        cout<<"[user] Accepted: " << name<<endl;
                        string time = get_time();
                        cout<<"time="<<time<<endl;
                        std::unique_lock<std::mutex> lck1(time_mutex);
                        time_map.insert(make_pair(name, time));
                        lck1.unlock();
                    //}
                    /*else
                    {
                        cout<<"[user] Rejected: " << name<<endl;
                    }*/
                }
                catch (...)
                {
                    //KF_LOG_ERROR(logger, "error in parsing STRATEGY_START: " << (char*)frame->getData());
                    cout<<"error in parsing STRATEGY_START: " << (char*)frame->getData()<<endl;
                }
            }
            else if(msg_type == 204){
                cout<<"204"<<endl;
                void* fdata = frame->getData();
                LFInputOrderField* order = (LFInputOrderField*)fdata;
                Deal_204(order, request_id);

            }
            else if(msg_type == 205){
                cout<<"205"<<endl;
                void* fdata = frame->getData();
                LFRtnOrderField* data205 = (LFRtnOrderField*)fdata;
                Deal_205(data205, request_id);

            }
            else if(msg_type == 206){
                cout<<"206"<<endl;
                void* fdata = frame->getData();
                LFRtnTradeField* rtn_trade = (LFRtnTradeField*)fdata;
                Deal_206(rtn_trade, request_id);
            }
        }

    }
}

void Strategy::init()
{
    std::cout << "[Strategy] (init)" <<std::endl;
    
    vector<string> tickers;
    tickers.push_back(M_TICKER);

    data->add_market_data(SOURCE_INDEX12);
    data->add_register_td(SOURCE_INDEX12);
    util->subscribeMarketData(tickers, SOURCE_INDEX12);

    td_connected = false;
    trade_completed = true;
    md_num = 0;
    traded_volume = 0;
    // ========= bind and initialize a signal ========
    /*signal.name = "sample_signal";
    signal.look_back = 1000;
    signal.param1 = 200;
    signal.param2 = 50;
    signal.TickPrice.clear();
    signal.has_open_position = false;
    signal.has_open_long_position = false;
    signal.has_open_short_position = false;
    signal.trade_size = 1;*/
    int my_order_id = 0;

    //std::thread t1(start_server);
    //t1.join();    

    std::cout << "[Strategy] (init) end" <<std::endl;
}

int main(int argc, const char* argv[])
{
    std::cout<<"a"<<endl;
    /*string url = "https://api.kucoin.com/api/v1/symbols";
    cpr::Response response = Get(Url{url},Timeout{10000});
    cout<<"[Get] (url) " << url << " (response.status_code) " << response.status_code <<
      " (response.error.message) " << response.error.message <<" (response.text) " << response.text.c_str()<<endl;*/
    /*string a = readfile("/liandao/wingchun/strategy/cpp_demo/a.json");
    cout<<a<<endl;
    Document json;
    json.Parse(a.c_str());
    int b = json["a"].GetInt();
    cout<<b<<endl;*/

    Init_map();
    cout<<form_map.size()<<endl;
    
    std::thread t1(start_server);
    //t1.join();
    std::thread t2(listen_time);
    
    /*json_queue.push("m1");
    json_queue.push("m2");
    json_queue.push("m3");*/

    /*Init_map();
    cout<<form_map.size()<<endl;*/

    sleep(2);

    Strategy str(string("aa"));

    LFInputOrderField test_204_1;
    strcpy(test_204_1.BusinessUnit,string("aa").c_str());
    strcpy(test_204_1.MiscInfo,string("14547").c_str());
    //str.on_rsp_order(&test_204_1, 1, 1, 1);
    Deal_204(&test_204_1, 1);
    strcpy(test_204_1.BusinessUnit,string("aa").c_str());
    strcpy(test_204_1.MiscInfo,string("14548").c_str());
    //str.on_rsp_order(&test_204_1, 2, 1, 1);
    Deal_204(&test_204_1, 2);
    strcpy(test_204_1.BusinessUnit,string("aa").c_str());
    strcpy(test_204_1.MiscInfo,string("14549").c_str());
    //str.on_rsp_order(&test_204_1, 3, 1, 1);
    Deal_204(&test_204_1, 3);

    LFRtnTradeField test_206_1;
    strcpy(test_206_1.InstrumentID,string("eth_btc").c_str());
    strcpy(test_206_1.ExchangeID,string("kucoin").c_str());
    test_206_1.Price = 100000000;
    test_206_1.Volume = 200000000;
    test_206_1.Direction = LF_CHAR_Buy;
    //str.on_rtn_trade(&test_206_1, 1, 1, 1);
    Deal_206(&test_206_1, 1);
    strcpy(test_206_1.InstrumentID,string("btc_usdt").c_str());
    strcpy(test_206_1.ExchangeID,string("kucoin").c_str());
    test_206_1.Price = 200000000;
    test_206_1.Volume = 300000000;
    test_206_1.Direction = LF_CHAR_Sell;
    //str.on_rtn_trade(&test_206_1, 2, 1, 1);
    Deal_206(&test_206_1, 2);
    strcpy(test_206_1.InstrumentID,string("eth_btc").c_str());
    strcpy(test_206_1.ExchangeID,string("binance").c_str());
    test_206_1.Price = 100000000;
    test_206_1.Volume = 300000000;
    test_206_1.Direction = LF_CHAR_Sell;
    //str.on_rtn_trade(&test_206_1, 3, 1, 1);
    Deal_206(&test_206_1, 3);

    std::cout<<"b"<<endl;

    str.init();

    //std::thread t1(start_server);
    //t1.join();

    str.start();
    str.block();

    cout<<"join1"<<endl;
    t1.join();
    cout<<"join2"<<endl;
    t2.join();

    std::cout<<"c";
    return 0;
}


