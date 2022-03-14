#ifndef WINGCHUN_MDENGINEKRAKEN_H
#define WINGCHUN_MDENGINEKRAKEN_H

#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include "PriceBook20Assembler.h"
#include <libwebsockets.h>
#include <document.h>
#include <map>
#include <vector>

WC_NAMESPACE_START

using rapidjson::Document;


struct SubscribeChannel
{
    int channelId;
    string exchange_coinpair;
    //book or trade or ...
    string subType;
};

struct BookMsg
{
    LFPriceBook20Field book;
    int64_t time;
    uint64_t sequence;
};

class MDEngineKraken: public IMDEngine
{
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
    virtual string name() const { return "MDEngineKraken"; };

public:
    MDEngineKraken();

    void on_lws_data(struct lws* conn, const char* data, size_t len);
    void on_lws_connection_error(struct lws* conn);
    int lws_write_subscribe(struct lws* conn);

private:
    inline int64_t getTimestamp();

 //   void onPing(struct lws* conn, Document& json);
 //   void onInfo(Document& json);
    void onSubscribed(Document& json);

    void onBook(SubscribeChannel& channel, Document& json);
    void onTrade(SubscribeChannel& channel, Document& json);
    void onOhlc(SubscribeChannel& channel, Document& json);

    SubscribeChannel findByChannelID(int channelId);

    std::string parseJsonToString(Document &d);
    std::string createBookJsonString(std::string exchange_coinpair);
    std::string createTradeJsonString(std::string exchange_coinpair);
    std::string createOhlcJsonString(std::string exchange_coinpair);

    void loop();
    void check_snapshot();
    void get_snapshot_via_rest();
    void rest_loop();
    void check_loop();
    uint64_t round_val(uint64_t val);

    virtual void set_reader_thread() override;
    void debug_print(std::vector<std::string> &subJsonString);
    void debug_print(std::vector<SubscribeChannel> &websocketSubscribeChannel);
    void onPing(struct lws* conn, Document& json);
    void makeWebsocketSubscribeJsonString();
private:
    ThreadPtr rest_thread;
    ThreadPtr ws_thread;
    ThreadPtr check_thread;
    bool connected = false;
    bool logged_in = false;
    /*edited by zyy,starts here*/
    int level_threshold = 15;
    int refresh_normal_check_book_s = 120;
    int refresh_normal_check_trade_s = 120;
    int refresh_normal_check_kline_s = 120;
    int64_t timer;
    /*edited by zyy ends here*/
    int book_depth_count = 25;
    int trade_count = 10;
    int rest_get_interval_ms = 500;
    int snapshot_check_s = 20;
    std::string baseUrl;

    static constexpr int scale_offset = 1e8;

    struct lws_context *context = nullptr;

    size_t subscribe_index = 0;

    //subscribe_channel
    std::vector<SubscribeChannel> websocketSubscribeChannel;
    SubscribeChannel EMPTY_CHANNEL = {0};

    std::string trade_channel = "trade";
    std::string book_channel = "book";
    std::string ohlc_channel = "ohlc";

    PriceBook20Assembler priceBook20Assembler;
    std::map<std::string,LFPriceBook20Field> priceBook;

    std::vector<std::string> websocketSubscribeJsonString;

    std::vector<std::string> websocketPendingSendMsg;
    std::vector<BookMsg> ws_book_vec;
    std::vector<BookMsg> rest_book_vec;
    std::map<std::string, uint64_t> has_bookupdate_map;
    std::map<std::string, BookMsg> book_map;

    CoinPairWhiteList coinPairWhiteList;
    std::map<std::string, int64_t> control_trade_map;
    std::map<std::string, int64_t> control_book_map;
    std::map<std::string, int64_t> control_kline_map;
};

DECLARE_PTR(MDEngineKraken);

WC_NAMESPACE_END

#endif
