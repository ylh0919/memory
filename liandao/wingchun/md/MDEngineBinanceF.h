#ifndef WINGCHUN_MDENGINEBINANCEF_H
#define WINGCHUN_MDENGINEBINANCEF_H

#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include <libwebsockets.h>
#include <map>
#include <unordered_map>
//edit here
#include <list>

WC_NAMESPACE_START

//edit here
struct KlineData
{
    int64_t     ts;
    int64_t     price;
    uint64_t    volume;
    std::string     TradingDay;            //交易日
    std::string     InstrumentID;          //合约代码
    std::string     ExchangeID;            //交易所代码
    std::string     StartUpdateTime;       //首tick修改时间
    int64_t     StartUpdateMillisec;   //首tick最后修改毫秒
    std::string     EndUpdateTime;         //尾tick最后修改时间
    int64_t     EndUpdateMillisec;     //尾tick最后修改毫秒
    int         PeriodMillisec;        //周期（毫秒）
    int64_t     Open;                  //开
    int64_t     Close;                 //收
    int64_t     Low;                   //低
    int64_t     High;                  //高
    uint64_t    Volume;                //区间交易量
};

struct BookMsg
{
    std::string InstrumentID;
    int64_t time;
    uint64_t sequence;
};

class MDEngineBinanceF: public IMDEngine
{
public:
    struct pair
    {
        std::string symbol;
        std::string event_type;
    };
public:
    /** load internal information from config json */
    virtual void load(const json& j_config);
    virtual void connect(long timeout_nsec);
    virtual void login(long timeout_nsec);
    virtual void logout();
    virtual void release_api();
    virtual void subscribeMarketData(const vector<string>& instruments, const vector<string>& markets);
    virtual bool is_connected() const { return connected; };
    virtual bool is_logged_in() const { return logged_in; };
    virtual string name() const { return "MDEngineBinanceF"; };

public:
    MDEngineBinanceF();
	
	void on_lws_data(const char* data, size_t len);
	
	void on_lws_connection_error(struct lws* conn);

    bool is_logout = true;
	void on_lws_connection_destroy(struct lws* conn, enum lws_callback_reasons reason);

private:
	//COIN-M: is_coin_m == true; USDT-M: is_coin_m == false;
    void connect_lws(std::string temp, bool is_coin_m);

    //COIN-M or USDT-M Futures
    bool is_coin_m_futures(std::string symbol);

    void get_symbol_and_event(std::string temp,std::vector<pair>& vec);
    
	void on_lws_market_trade(const char* data, size_t len);

	void on_lws_book_update(const char* data, size_t len);

    void on_lws_kline(const char* src, size_t len);

    void on_lws_funding(const char* src, size_t len);

    void loop();

    void check_snapshot();
    void get_snapshot_via_rest();
    void rest_loop();
    void check_loop();

    virtual void set_reader_thread() override;

    CoinPairWhiteList coinPairWhiteList;

    inline int64_t getTimestamp();

    //edit here
    void klineloop();
    void control_kline(bool first_kline, int64_t time);
    void handle_kline(int64_t time);
    std::string getdate(int64_t time);
    std::string gettime(int64_t time);
    std::string dealzero(std::string time);

    //get kline via rest
    void get_kline_via_rest(string symbol, string interval, bool ignore_startTime, int64_t startTime, int64_t endTime, int limit);
    //get interval, subscribe kline via websocket
    string getIntervalStr(int64_t sec);

private:
    ThreadPtr rest_thread;
    ThreadPtr ws_thread;
    ThreadPtr check_thread;
    bool connected = false;
    bool logged_in = false;

    //edit here
    bool need_another_kline = false;
    int bar_duration_s = 5;
    int bar_wait_time_ms = 500;
    ThreadPtr kline_thread;
    std::map<std::string, KlineData> kline_hand_map;
    //std::vector<KlineData> kline_receive_vec;
    std::list<KlineData> kline_receive_list;

    int book_depth_count = 20;
    int level_threshold = 10;
    int refresh_normal_check_book_s = 120;
    int refresh_normal_check_trade_s = 120;
    int refresh_normal_check_kline_s = 120;
    int orderbook_latency_threshold_exceeded_ms = 1000;
    bool need_restart = false;

    int64_t timer[3];
    bool need_get_snapshot_via_rest = true;
    int rest_get_interval_ms = 500;
    int snapshot_check_s = 20;
    //subscribe two kline
    string kline_a_interval = "1m";
    string kline_b_interval = "1m";
    //receive kline limit
    bool only_receive_complete_kline = true;

    uint64_t last_rest_get_ts = 0;
    uint64_t last_trade_id = 0;
    static constexpr int scale_offset = 1e8;

    struct lws_context *context = nullptr;
	//std::map<std::string,LFPriceBook20Field> priceBook;
	
	std::unordered_map<struct lws *,std::string> lws_handle_map;
	//lws connect to "dstream.binance.com"(true) or "fstream.binance.com"(false)
    std::unordered_map<struct lws*, std::string> lws_reconnect_map;
    std::unordered_map<struct lws *, bool> lws_connect_to_dstream_map;
    std::map<std::string, std::map<uint64_t, int64_t>> ws_book_map;
    std::vector<BookMsg> rest_book_vec;

    std::map<std::string, int64_t> control_book_map;
    std::map<std::string, int64_t> control_trade_map;
    std::map<std::string, int64_t> control_kline_map;
};

DECLARE_PTR(MDEngineBinanceF);


WC_NAMESPACE_END

#endif
