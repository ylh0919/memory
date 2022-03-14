#ifndef WINGCHUN_MDENGINEUPBIT_H
#define WINGCHUN_MDENGINEUPBIT_H

#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include "PriceBook20Assembler.h"
#include <libwebsockets.h>
#include <document.h>
#include <map>
#include <vector>
#include <queue>
#include <mutex>

WC_NAMESPACE_START

using rapidjson::Document;

struct BookMsg
{
    std::string InstrumentID;
    int64_t time;
    uint64_t sequence;
};

class MDEngineUpbit: public IMDEngine
{
public:
    struct CoinBaseQuote
    {
        std::string base;
        std::string quote;
    };

    /** load internal information from config json */
    virtual void load(const json& j_config);
    virtual void connect(long timeout_nsec);
    virtual void login(long timeout_nsec);
    virtual void logout();
    virtual void release_api();
    virtual void subscribeMarketData(const vector<string>& instruments, const vector<string>& markets);
    virtual bool is_connected() const { return connected; };
    virtual bool is_logged_in() const { return logged_in; };
    virtual string name() const { return "MDEngineUpbit"; };

public:
    MDEngineUpbit();

    void on_lws_data(struct lws* conn, const char* data, size_t len);
    void on_lws_connection_error(struct lws* conn);
    int lws_write_subscribe(struct lws* conn);

private:
    inline int64_t getTimestamp();
    void onBook(Document& json);
	void onTrade(Document& json);
	void onKline(string coinpair, int minutes, int count);

    std::string parseJsonToString(const char* in);
	std::string parseJsonToString(Document& json);
	std::string getUUID();
	std::string createOrderBookJsonString(std::vector<std::string>& base_quote);
	std::string createTradeJsonString(std::vector<std::string>& base_quote);
    void loop();
    void check_snapshot();
    void get_snapshot_via_rest();
    void rest_loop();
    void check_loop();
    void get_kline_loop();
    void onKline_loop();
    virtual void set_reader_thread() override;
    void debug_print(std::vector<std::string> &subJsonString);

    void split(std::string str, std::string token, CoinBaseQuote& sub);
    //从白名单的策略定义中提取出币种的名称
    void getBaseQuoteFromWhiteListStrategyCoinPair();

    void makeWebsocketSubscribeJsonString();

    //get kline/bar/candles via rest
    void get_kline_via_rest(string symbol, int minutes = 5, int count = 1);
private:
    ThreadPtr rest_thread;
    ThreadPtr ws_thread;
    ThreadPtr check_thread;
    ThreadPtr get_kline_thread;
    ThreadPtr onKline_thread;

    bool connected = false;
    bool logged_in = false;

    int book_depth_count = 5;
    int trade_count = 10;
    int rest_get_interval_ms = 500;
    int snapshot_check_s = 20;

    /*edited by zyy,starts here*/
    int level_threshold = 15;
    int refresh_normal_check_book_s = 120;
    int refresh_normal_check_trade_s = 120;
    int64_t timer;
    /*edited by zyy ends here*/
    int64_t ping_time;

    //get kline via rest
    bool need_get_kline_via_rest = false;
    int kline_a_interval_min = 5;
    int kline_b_interval_min = 5;
    int get_kline_wait_ms = 60000;
    int get_kline_via_rest_count = 1;

    static constexpr int scale_offset = 1e8;

    struct lws_context *context = nullptr;
    struct lws *ws_wsi = NULL;

    int64_t subscribe_index = 0;

    PriceBook20Assembler priceBook20Assembler;

    std::vector<std::string> websocketSubscribeJsonString;

    CoinPairWhiteList coinPairWhiteList;
    std::map<std::string, std::map<uint64_t, int64_t>> ws_book_map;
    std::vector<BookMsg> rest_book_vec;
    std::queue<int> ping_queue;

    std::map<std::string, int64_t> control_book_map;
    std::map<std::string, int64_t> control_trade_map;
    //订阅的币种的base和quote, 全是大写字母
    std::vector<CoinBaseQuote> coinBaseQuotes;
};



DECLARE_PTR(MDEngineUpbit);

WC_NAMESPACE_END

#endif
