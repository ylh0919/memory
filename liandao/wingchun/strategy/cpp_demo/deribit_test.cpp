/**
 * Strategy demo, same logic as band_demo_strategy.py
 * @Author cjiang (changhao.jiang@taurus.ai)
 * @since   Nov, 2017
 */

#include "IWCStrategy.h"
#include <deque>

USING_WC_NAMESPACE

/*#define SOURCE_INDEX1 SOURCE_BINANCE
#define SOURCE_INDEX2 SOURCE_BITFINEX
#define SOURCE_INDEX3 SOURCE_BITFLYER
#define SOURCE_INDEX4 SOURCE_BITHUMB
#define SOURCE_INDEX5 SOURCE_BITMAX
#define SOURCE_INDEX6 SOURCE_BITMEX
#define SOURCE_INDEX7 SOURCE_BITSTAMP
#define SOURCE_INDEX8 SOURCE_BITTREX
#define SOURCE_INDEX9 SOURCE_COINMEX
#define SOURCE_INDEX10 SOURCE_CTP
#define SOURCE_INDEX11 SOURCE_DAYBIT*/
#define SOURCE_INDEX12 SOURCE_DERIBIT
/*#define SOURCE_INDEX13 SOURCE_EMX
#define SOURCE_INDEX14 SOURCE_HITBTC
#define SOURCE_INDEX15 SOURCE_HUOBI
#define SOURCE_INDEX16 SOURCE_KRAKEN
#define SOURCE_INDEX17 SOURCE_KUCOIN
#define SOURCE_INDEX18 SOURCE_MOCK
#define SOURCE_INDEX19 SOURCE_OCEANEX
#define SOURCE_INDEX20 SOURCE_OCEANEXB
#define SOURCE_INDEX21 SOURCE_POLONIEX
#define SOURCE_INDEX22 SOURCE_PROBIT
#define SOURCE_INDEX23 SOURCE_UPBIT
#define SOURCE_INDEX24 SOURCE_XTP*/
#define M_TICKER "TRXBTC"
#define M_EXCHANGE EXCHANGE_SHFE
#define TRADED_VOLUME_LIMIT 500
#define STARTTIME 1563288960*1e9
#define ENDTIME 1563289200*1e9
//To replay, set STARTTIME, ENDTIME, SOURCE_INDEX1

struct Signal
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
};

class Strategy: public IWCStrategy
{
protected:
    bool td_connected;
    bool trade_completed;
    int rid;
    int cancel_id;
    int md_num;
    int traded_volume;
    Signal signal;
public:
    virtual void init();
    virtual void on_market_data(const LFMarketDataField* data, int source, long rcv_time);
    virtual void on_rsp_position(const PosHandlerPtr posMap, int request_id, int source, long rcv_time);
    virtual void on_rtn_trade(const LFRtnTradeField* data, int request_id, int source, long rcv_time);
    virtual void on_rsp_order(const LFInputOrderField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_price_book_update(const LFPriceBook20Field* data, int source, long rcv_time);

public:
    Strategy(const string& name);
};

Strategy::Strategy(const string& name): IWCStrategy(name)
{
    std::cout << "[Strategy] (Strategy)" <<std::endl;
    rid = -1;
    cancel_id = -1;

}

void Strategy::init()
{
    std::cout << "[Strategy] (init)" <<std::endl;
    //KF_LOG_DEBUG(logger, "[init] SOURCE_INDEX:" << SOURCE_INDEX);
    vector<string> tickers;
    tickers.push_back(M_TICKER);

/*        data->add_market_data(SOURCE_INDEX1);
    data->add_register_td(SOURCE_INDEX1);
    util->subscribeMarketData(tickers, SOURCE_INDEX1);
        data->add_market_data(SOURCE_INDEX2);
    data->add_register_td(SOURCE_INDEX2);
    util->subscribeMarketData(tickers, SOURCE_INDEX2);
        data->add_market_data(SOURCE_INDEX3);
    data->add_register_td(SOURCE_INDEX3);
    util->subscribeMarketData(tickers, SOURCE_INDEX3);
        data->add_market_data(SOURCE_INDEX4);
    data->add_register_td(SOURCE_INDEX4);
    util->subscribeMarketData(tickers, SOURCE_INDEX4);
        data->add_market_data(SOURCE_INDEX5);
    data->add_register_td(SOURCE_INDEX5);
    util->subscribeMarketData(tickers, SOURCE_INDEX5);
        data->add_market_data(SOURCE_INDEX6);
    data->add_register_td(SOURCE_INDEX6);
    util->subscribeMarketData(tickers, SOURCE_INDEX6);
    data->add_market_data(SOURCE_INDEX7);
    data->add_register_td(SOURCE_INDEX7);
    util->subscribeMarketData(tickers, SOURCE_INDEX7);
        data->add_market_data(SOURCE_INDEX8);
    data->add_register_td(SOURCE_INDEX8);
    util->subscribeMarketData(tickers, SOURCE_INDEX8);
        data->add_market_data(SOURCE_INDEX9);
    data->add_register_td(SOURCE_INDEX9);
    util->subscribeMarketData(tickers, SOURCE_INDEX9);
        data->add_market_data(SOURCE_INDEX10);
    data->add_register_td(SOURCE_INDEX10);
    util->subscribeMarketData(tickers, SOURCE_INDEX10);
        data->add_market_data(SOURCE_INDEX11);
    data->add_register_td(SOURCE_INDEX11);
    util->subscribeMarketData(tickers, SOURCE_INDEX11);*/
        data->add_market_data(SOURCE_INDEX12);
    data->add_register_td(SOURCE_INDEX12);
    util->subscribeMarketData(tickers, SOURCE_INDEX12);
/*        data->add_market_data(SOURCE_INDEX13);
    data->add_register_td(SOURCE_INDEX13);
    util->subscribeMarketData(tickers, SOURCE_INDEX13);
        data->add_market_data(SOURCE_INDEX14);
    data->add_register_td(SOURCE_INDEX14);
    util->subscribeMarketData(tickers, SOURCE_INDEX14);
        data->add_market_data(SOURCE_INDEX15);
    data->add_register_td(SOURCE_INDEX15);
    util->subscribeMarketData(tickers, SOURCE_INDEX15);
        data->add_market_data(SOURCE_INDEX16);
    data->add_register_td(SOURCE_INDEX16);
    util->subscribeMarketData(tickers, SOURCE_INDEX16);
        data->add_market_data(SOURCE_INDEX17);
    data->add_register_td(SOURCE_INDEX17);
    util->subscribeMarketData(tickers, SOURCE_INDEX17);
        data->add_market_data(SOURCE_INDEX18);
    data->add_register_td(SOURCE_INDEX18);
    util->subscribeMarketData(tickers, SOURCE_INDEX18);
        data->add_market_data(SOURCE_INDEX19);
    data->add_register_td(SOURCE_INDEX19);
    util->subscribeMarketData(tickers, SOURCE_INDEX19);
        data->add_market_data(SOURCE_INDEX20);
    data->add_register_td(SOURCE_INDEX20);
    util->subscribeMarketData(tickers, SOURCE_INDEX20);
        data->add_market_data(SOURCE_INDEX21);
    data->add_register_td(SOURCE_INDEX21);
    util->subscribeMarketData(tickers, SOURCE_INDEX21);
        data->add_market_data(SOURCE_INDEX22);
    data->add_register_td(SOURCE_INDEX22);
    util->subscribeMarketData(tickers, SOURCE_INDEX22);
        data->add_market_data(SOURCE_INDEX23);
    data->add_register_td(SOURCE_INDEX23);
    util->subscribeMarketData(tickers, SOURCE_INDEX23);
        data->add_market_data(SOURCE_INDEX24);
    data->add_register_td(SOURCE_INDEX24);
    util->subscribeMarketData(tickers, SOURCE_INDEX24);*/

    
   //     data->set_time_range(STARTTIME,ENDTIME);
    
    // necessary initialization of internal fields.
    td_connected = false;
    trade_completed = true;
    md_num = 0;
    traded_volume = 0;
    // ========= bind and initialize a signal ========
    signal.name = "sample_signal";
    signal.look_back = 1000;
    signal.param1 = 200;
    signal.param2 = 50;
    signal.TickPrice.clear();
    signal.has_open_position = false;
    signal.has_open_long_position = false;
    signal.has_open_short_position = false;
    signal.trade_size = 1;
    int my_order_id = 0;
    std::cout << "[Strategy] (init) end" <<std::endl;
}

void Strategy::on_rsp_position(const PosHandlerPtr posMap, int request_id, int source, long rcv_time)
{
    KF_LOG_INFO(logger, " [on_rsp_position] (source)" << source << " (request_id)" << request_id);
    /*if (request_id == -1 && source == SOURCE_INDEX)
    {
        td_connected = true;
        KF_LOG_INFO(logger, "td connected");
        if (posMap.get() == nullptr)
        {
            data->set_pos(PosHandlerPtr(new PosHandler(source)), source);
        }
    }
    else
    {
        KF_LOG_DEBUG(logger, "[RSP_POS] " << posMap->to_string());
    }*/
}

void Strategy::on_market_data(const LFMarketDataField* md, int source, long rcv_time)
{
    KF_LOG_INFO(logger, "[on_market_data] (source)"<< source << " (InstrumentID)" << md->InstrumentID <<  "(AskPrice1)" << md->AskPrice1);
}

void Strategy::on_price_book_update(const LFPriceBook20Field* data, int source, long rcv_time)
{
    KF_LOG_INFO(logger, "[on_price_book_update] (source)" << source << " (ticker)" << data->InstrumentID
                                                                         << " (bidcount)" << data->BidLevelCount
                                                                         << " (askcount)" << data->AskLevelCount
                                                                         << " (status)" << data->Status);//FXW's edits


    /*if(rid == -1)
    {
        KF_LOG_INFO(logger, "[on_price_book_update] insert order ");
        //new insert
        rid = util->insert_limit_order(SOURCE_INDEX, M_TICKER, M_EXCHANGE,
                                       289, 51200000000,
                                       LF_CHAR_Buy, LF_CHAR_Open);

        KF_LOG_INFO(logger, "[insert_limit_order] (rid)" << rid);
    }
    else
    {
        //cancel it
        if(cancel_id == -1) {
            cancel_id = util->cancel_order(SOURCE_INDEX, rid);
            KF_LOG_INFO(logger, "[cancel_order] (cancel_id)" << cancel_id);
        }
    }*/
}


void Strategy::on_rtn_trade(const LFRtnTradeField* rtn_trade, int request_id, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[on_rtn_trade]" << " (InstrumentID)" << rtn_trade->InstrumentID <<
                                          " (Price)" << rtn_trade->Price <<
                                          " (Volume)" << rtn_trade->Volume);

}

void Strategy::on_rsp_order(const LFInputOrderField* order, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    KF_LOG_ERROR(logger, "[on_rsp_order] (err_id)" << errorId << " (err_msg)" << errorMsg <<
                                                   " (order_id)" << request_id <<
                                                   " (source)" << source <<
                                                   " (LFInputOrderField.InstrumentID)" << order->InstrumentID <<
                                                   " (LFInputOrderField.OrderRef)" << order->OrderRef <<
                                                   " (LFInputOrderField.LimitPrice)" << order->LimitPrice <<
                                                   "(LFInputOrderField.Volume)" << order->Volume);
}


int main(int argc, const char* argv[])
{
    Strategy str(string("YOUR_STRATEGY2"));
    str.init();
    str.start();
    str.block();
    return 0;
}

