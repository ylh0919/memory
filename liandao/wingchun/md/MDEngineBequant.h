#ifndef WINGCHUN_MDENGINEBEQUANT_H
#define WINGCHUN_MDENGINEBEQUANT_H

#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include <libwebsockets.h>
#include <document.h>
#include <map>
#include <vector>
#include "CoinPairWhiteList.h"
#include "PriceBook20Assembler.h"

WC_NAMESPACE_START

using rapidjson::Document;


struct PriceAndVolume
{
    int64_t price;
    uint64_t volume;
    bool operator < (const PriceAndVolume &other) const
    {
        if (price<other.price)
        {
            return true;
        }
        return false;
    }
};

//coinmex use base and quote to sub depth data
struct SubscribeCoinBaseQuote
{
    std::string base;
    std::string quote;
};

struct BookMsg
{
    LFPriceBook20Field book;
    int64_t time;
    uint64_t sequence;
    std::string str_sequence;
};

//BEQUANT can subscribe Orderbook ��Ticker��Trades ��Candles
struct SubscribeChannel 
{
	int channelId;
	string exchange_coinpair;
	string subType;//orderbook ��Ticker��Trades
};

static int sort_price_asc(const PriceAndVolume &p1,const PriceAndVolume &p2)
{
    return p1.price < p2.price;
};

static int sort_price_desc(const PriceAndVolume &p1,const PriceAndVolume &p2)
{
    return p1.price > p2.price;
};

template<typename T>
static void sortMapByKey(std::map<int64_t, uint64_t> &t_map, std::vector<PriceAndVolume> &t_vec, T& sort_by)
{
    for(std::map<int64_t, uint64_t>::iterator iter = t_map.begin();iter != t_map.end(); iter ++)
    {
        PriceAndVolume pv;
        pv.price = iter->first;
        pv.volume = iter->second;
        t_vec.push_back(pv);
    }
    sort(t_vec.begin(), t_vec.end(), sort_by);
};

class MDEngineBequant: public IMDEngine
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
    virtual string name() const { return "MDEngineBequant"; };

public:
    MDEngineBequant();

    void on_lws_data(struct lws* conn, const char* data, size_t len);
    void on_lws_connection_error(struct lws* conn);
    int lws_write_subscribe(struct lws* conn);

private:
    inline int64_t getTimestamp();
    void onDepth(Document& json);
    void onDepthHit(Document& json);
    void onTickers(Document& json);
    void onFills(Document& json);
    void onFillsHit(Document& json);
	void onOrderBook(Document& json,bool is_snapshot = false);
	void onTrades(Document& json);
	void onCandles(Document& json);

    std::string createOrderBookString_unsubscribe(std::string coinpair);
    std::string parseJsonToString(const char* in);
	std::string parseJsonToString(Document& json);
	std::string createOrderBookString(std::string coinpair);
    std::string createTickersJsonString();
	std::string createTradesString(std::string coinpair);
    std::string createCandlesString(std::string coinpair);
    void clearPriceBook();
    //void loop();
    void loop();
    void check_snapshot();
    void rest_loop();
    void check_loop();

    virtual void set_reader_thread() override;
private:
    ThreadPtr rest_thread;
    ThreadPtr ws_thread;
    ThreadPtr check_thread;
    bool connected = false;
    bool logged_in = false;
    int level_threshold = 15;
    int refresh_normal_check_book_s = 120;
    int refresh_normal_check_trade_s = 120;
    int rest_get_interval_ms = 500;
    int snapshot_check_s = 20;
    static constexpr int64_t scale_offset = 1e8;

    struct lws_context *context = nullptr;

    int subscribe_index = 0;

        //<ticker, <price, volume>>
    std::map<std::string, std::map<int64_t, uint64_t>*> tickerAskPriceMap;
    std::map<std::string, std::map<int64_t, uint64_t>*> tickerBidPriceMap;
private:
    void readWhiteLists(const json& j_config);
    std::string getWhiteListCoinpairFrom(std::string md_coinpair);
    void get_snapshot_via_rest();
    void split(std::string str, std::string token, SubscribeCoinBaseQuote& sub);
    void debug_print(std::vector<SubscribeCoinBaseQuote> &sub);
    void debug_print(std::map<std::string, std::string> &keyIsStrategyCoinpairWhiteList);
    void debug_print(std::vector<std::string> &subJsonString);
    //coinmex use base and quote to sub depth data, so make this vector for it
    std::vector<SubscribeCoinBaseQuote> subscribeCoinBaseQuote;
    std::vector<BookMsg> ws_book_vec;
    std::vector<BookMsg> rest_book_vec;
    std::map<std::string, uint64_t> has_bookupdate_map;
    std::map<std::string, BookMsg> book_map;

    std::vector<std::string> websocketSubscribeJsonString;
	PriceBook20Assembler priceBook20Assembler;
    //in MD, lookup direction is:
    // incoming exchange coinpair ---> our strategy recognized coinpair
    //if coming data 's coinpair is not in this map ,ignore it
    //"strategy_coinpair(base_quote)":"exchange_coinpair",
    std::map<std::string, std::string> keyIsStrategyCoinpairWhiteList;
	CoinPairWhiteList coinPairWhiteList_websocket;
    std::map<std::string,std::map<int64_t,LFBarMarketDataField>> mapKLines;


    std::map<std::string, int64_t> saveSequence;
    //重新建的一个map，为了在比较change_id和prev_change_id的时候能用同一币对进行比较
    std::map<std::string,int64_t> control_trade_map;
    std::map<std::string,int64_t> control_book_map;
};

DECLARE_PTR(MDEngineBequant);

WC_NAMESPACE_END

#endif
