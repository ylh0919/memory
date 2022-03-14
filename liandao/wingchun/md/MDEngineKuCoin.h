#ifndef WINGCHUN_MDENGINEKUCOIN_H
#define WINGCHUN_MDENGINEKUCOIN_H

#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include "PriceBook20Assembler.h"
#include <libwebsockets.h>
#include <document.h>
#include <map>
#include <vector>
#include <mutex>
#include "../base/ThreadPool.h"
WC_NAMESPACE_START

using rapidjson::Document;
using rapidjson::Value;
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

struct ServerInfo
{
   int nPingInterval = 0;
   int nPingTimeOut = 0;
   std::string strEndpoint ;
   std::string strProtocol ;
   bool bEncrypt = true;
}; 

struct PriceBookData
{
    std::map<int64_t, uint64_t> mapAskPrice;
    std::map<int64_t, uint64_t> mapBidPrice;
    int64_t nSequence = -1;
};

struct BookMsg
{
    LFPriceBook20Field book;
    int64_t time;
    uint64_t sequence;
    bool isChecked;
};

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

struct PriceRange
{
    int64_t min_price;
    int64_t max_price;
};

class MDEngineKuCoin: public IMDEngine
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
    virtual string name() const { return "MDEngineKuCoin"; };

public:
    MDEngineKuCoin();
    ~MDEngineKuCoin();

    void on_lws_data(struct lws* conn, const char* data, size_t len);
    void on_lws_connection_error(struct lws* conn);
    int lws_write_subscribe(struct lws* conn);
    void writeErrorLog(std::string strError);
private:
    void onDepth(Value& json,std::string& symbol);
    void onTickers(Document& json);
    void onFills(Value& json,std::string& symbol);

    void clearPriceBook();
    int64_t getTimestamp();
    void check_snapshot();
    void get_snapshot_via_rest();
    void rest_loop();
    void check_loop();
    void loop();
    void klineloop();
    void control_kline(bool first_kline,int64_t time);
    void handle_kline(int64_t time);
    void handle_lws_data(struct lws* conn, std::string data);
    std::string get_utc_date(int64_t time);
    std::string get_utc_time(int64_t time);
    std::string dealzero(std::string time);
    std::string dealDataSprit(const char* src);

    virtual void set_reader_thread() override;
private:
    bool isPong = false;
    bool shouldPing = true;
    struct lws* m_conn;
    LFBarMarketDataField last_market;

    ThreadPtr rest_thread;
    ThreadPtr ws_thread;
    ThreadPtr kline_thread;
    ThreadPtr check_thread;
    bool connected = false;
    bool logged_in = false;

    int rest_get_interval_ms = 500;
    int book_depth_count = 5;
    int rest_try_count = 1;
    int snapshot_check_s = 20;
    /*edited by zyy,starts here*/
    int64_t timer;
    int refresh_normal_check_book_s = 120;
    int level_threshold = 15;
    /*edited by zyy ends here*/

    static constexpr int scale_offset = 1e8;

    std::vector<ServerInfo> m_vstServerInfos;
    std::string m_strToken;

    struct lws_context *context = nullptr;

    int subscribe_index = 0;
    int bar_duration_s = 60;

    //<ticker, <price, volume>>
    std::mutex* m_mutexPriceBookData;
    std::map<std::string,PriceBookData> m_mapPriceBookData;

private:
    std::string parseJsonToString(const char* in);
    void readWhiteLists(const json& j_config);
    std::string getWhiteListCoinpairFrom(std::string md_coinpair);
    bool shouldUpdateData(const LFPriceBook20Field& md);
    std::string getLiandaoCoin(const std::string& strExchangeCoin);
    void debug_print(std::map<std::string, std::string> &keyIsStrategyCoinpairWhiteList);
    bool getToken(Document& d);
    bool getServers(Document& d);
    void Ping(struct lws* conn);
    void onPong(struct lws* conn);
    std::string getId();
    int64_t getMSTime();
    std::string makeSubscribeL2Update(std::string& strSymbol);
    std::string makeSubscribeMatch(std::string& strSymbol);
    bool getInitPriceBook(const std::string& strSymbol,std::map<std::string,PriceBookData>::iterator& it) ;
    void printPriceBook(const PriceBookData& stPriceBookData);

    PriceBook20Assembler priceBook20Assembler;
    std::map<std::string,LFPriceBook20Field> mapLastData;
    //in MD, lookup direction is:
    // incoming exchange coinpair ---> our strategy recognized coinpair
    //if coming data 's coinpair is not in this map ,ignore it
    //"strategy_coinpair(base_quote)":"exchange_coinpair",
    size_t m_nSubscribePos = 0;
    std::vector<std::string> m_vstrSubscribeJsonString;
    std::map<std::string, std::string> keyIsStrategyCoinpairWhiteList;
    //std::map<std::string, KlineData> kline_receive_map;
    //std::vector<BookMsg> ws_book_vec;

    // fixed here
    // 在这里把检查所有记录改成只检查最后一条
    //std::map<std::string, std::vector<BookMsg>> ws_book_map;
    std::map<std::string, BookMsg> ws_book_map;

    std::vector<BookMsg> rest_book_vec;
    //std::map<std::string, uint64_t> has_bookupdate_map;
    std::map<std::string, BookMsg> book_map;
    std::map<std::string, KlineData> kline_hand_map;
    std::vector<KlineData> kline_receive_vec;
    std::map<std::string, PriceRange> price_range_map;
    std::map<int, int> message_type_map;
    ThreadPoolOfMultiTask* m_ThreadPoolPtr = nullptr;

    std::map<std::string, int64_t> control_book_map;
};

DECLARE_PTR(MDEngineKuCoin);

WC_NAMESPACE_END

#endif

