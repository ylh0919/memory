#ifndef WINGCHUN_MDENGINEDERIBIT_H
#define WINGCHUN_MDENGINEDERIBIT_H

#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include "PriceBook20Assembler.h"
#include <libwebsockets.h>
#include <document.h>
#include <map>
#include <vector>
#include <atomic>
#include <mutex>

WC_NAMESPACE_START

using rapidjson::Document;

struct BookMsg
{
    std::string InstrumentID;
    int64_t time;
    uint64_t sequence;
};

struct PriceBookData
{
    std::map<int64_t, uint64_t> mapAskPrice;
    std::map<int64_t, uint64_t> mapBidPrice;
    int64_t idChange = -1;
};

struct SubscribeChannel
{
    int channelId;
    string exchange_coinpair;
    //book or trade or ...
    string subType;
};


class MDEngineDeribit: public IMDEngine
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
    virtual string name() const { return "MDEngineDeribit"; };

public:
    MDEngineDeribit();
    ~MDEngineDeribit();

    void on_lws_data(struct lws* conn, const char* data, size_t len);
    void on_lws_connection_error(struct lws* conn);
    int lws_write_subscribe(struct lws* conn);

private:
    inline int64_t getTimestamp();

    //void onPing(struct lws* conn, Document& json);
    //void onInfo(Document& json);
   // void onSubscribed(Document& json);

    void onBook(Document& json);
    void onTrade(Document& json);
    void onPriceIndex(Document& json);
    void onMarkPrice(Document& json);
    void onPerpetual(Document& json);
    void onTicker(Document& json);

    void clearPriceBook();

    std::string split(string s);

    bool getInitPriceBook(const std::string& strSymbol,std::map<std::string,PriceBookData>::iterator& it);
    std::string getWhiteListCoinpairFrom(std::string md_coinpair);
    std::map<std::string, std::string> keyIsStrategyCoinpairWhiteList;
    std::map<std::string,PriceBookData> m_mapPriceBookData;
    std::mutex* m_mutexPriceBookData;
    std::map<std::string, uint64_t> saveChangeID;
    //重新建的一个map，为了在比较change_id和prev_change_id的时候能用同一币对进行比较

    std::map<std::string,LFPriceBook20Field> mapLastData;
    //SubscribeChannel findByChannelID(int channelId);

    std::string parseJsonToString(Document &d);
    //std::string createJsonString(std::string singnature,std::string exchange_coinpair,int type);
    std::string createJsonString(std::string singnature,std::vector<std::string> exchange_coinpairs);
    //std::string CoinPairWhiteList::GetKeyByValue(std::string exchange_coinpair);


    void loop();
    void get_funding_rest(int64_t end_timestamp);


    virtual void set_reader_thread() override;
    void debug_print(std::vector<std::string> &subJsonString);
    void debug_print(std::vector<SubscribeChannel> &websocketSubscribeChannel);
    void check_snapshot();
    void get_snapshot_via_rest();
    void rest_loop();


    void makeWebsocketSubscribeJsonString();
private:
    ThreadPtr rest_thread;
    ThreadPtr ws_thread;
    ThreadPtr check_thread;
    ThreadPtr funding_thread;
    bool connected = false;
    bool logged_in = false;
    bool snapshot_finish = false;

    int book_depth_count = 20;
    int trade_count = 10;
    int rest_get_interval_ms = 500;
    int funding_get_interval_s = 3600;
    int level_threshold = 1;
    int refresh_normal_check_book_s = 120;
    int snapshot_check_s = 20;
    int refresh_normal_check_trade_s = 120;
    int max_subscription_per_message = 400;
    int rest_try_count = 1;
    //std::string secret_key;
    //std::string access_key;
    std::string action = "/api/v1/private/subscribe";

    static constexpr int scale_offset = 1e8;

    struct lws_context *context = nullptr;

    size_t subscribe_index = 0;

    //subscribe_channel
    std::vector<SubscribeChannel> websocketSubscribeChannel;
    SubscribeChannel EMPTY_CHANNEL = {0};

    std::string trade_channel = "trades";
    std::string book_channel = "book";

    PriceBook20Assembler priceBook20Assembler;

    std::vector<std::string> websocketSubscribeJsonString;

    std::vector<std::string> websocketPendingSendMsg;
    std::map<std::string, std::map<uint64_t, int64_t>> ws_book_map;
    std::vector<BookMsg> rest_book_vec;
    std::vector<std::string> ticker_vec;


    CoinPairWhiteList coinPairWhiteList;
    CoinPairWhiteList coinPairWhiteList_websocket;
	CoinPairWhiteList coinPairWhiteList_rest;

    std::map<std::string, int64_t> control_trade_map;
    std::map<std::string, int64_t> control_book_map;
 };

DECLARE_PTR(MDEngineDeribit);

WC_NAMESPACE_END

#endif
