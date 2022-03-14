#ifndef WINGCHUN_MDENGINEMOCK_H
#define WINGCHUN_MDENGINEMOCK_H

#include "IEngine.h"
#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include <document.h>
#include <map>
#include <vector>

WC_NAMESPACE_START

struct MDdata{
    LFPriceBook20Field MD;
    int source_id;
};

struct TDdata{
    LFL2TradeField TD;
    int source_id;
};

struct KLdata {
    LFBarMarketDataField KL;
    int source_id;
};

class MDEngineMock: public IMDEngine
{
public:
    /** load internal information from config json */
    virtual void load(const json& j_config);
    virtual void connect(long timeout_nsec);
    virtual void login(long timeout_nsec);
    virtual void logout();
    virtual void release_api();
    virtual bool is_connected() const { return connected; };
    virtual bool is_logged_in() const { return logged_in; };
    virtual string name() const { return "MDEngineMock"; };

public:
    MDEngineMock();
    ~MDEngineMock();
private:
    bool onDepth();
    bool onFills();
    bool onKline();
    void book_loop();
    void trade_loop();
    void compare_loop();
    //book_empty  trade_empty  kline_empty最多有一个为真
    //return 0 for book(105), 1 for trade(106), 2 for kline(110)
    int  compare_data_time(int64_t book_time, int64_t trade_time, int64_t kline_time, bool book_empty, bool trade_empty, bool kline_empty); 
    //book_empty  trade_empty  kline_empty最多有一个为真
    //计算等待时间
    int64_t  calculate_wait_time(int64_t pre, int64_t book_time, int64_t trade_time, int64_t kline_time, bool book_empty, bool trade_empty, bool kline_empty);
    void compare_loop_with_kline();
    void get_book_map(string,int);
    void get_trade_map(string,int);
    void get_kline_map(string,int);

    virtual void set_reader_thread() override;
private:
    ThreadPtr rest_thread;
    ThreadPtr book_thread;
    ThreadPtr trade_thread;
    //modified here compare nano between book and trade
    ThreadPtr compare_thread;
    bool connected = false;
    bool logged_in = false;

    int64_t playback_rate = 360000000000; // 可配置
    int buffer_line_size = 5; // 可配置
    int64_t book_data_firtm = -1; // the time of first book data(106)
    int64_t trade_data_firtm = -1; // the time of first trade data(105)
    int64_t kline_data_firtm = -1; // the time of first kline data(110)
    
    int rest_get_interval_ms = 500;
    std::string book_data_path ;
    std::string trade_data_path ;
    int64_t book_interval_ms = 500;
    int64_t trade_interval_ms = 1000;

    std::map<int64_t, std::pair<MDdata,int>>  book_line_data;
    std::map<int64_t, std::pair<TDdata,int>>  trade_line_data;
    std::map<int64_t, std::pair<KLdata,int>>  kline_line_data;


    static constexpr int scale_offset = 1e8;

private:
    void readWhiteLists(const json& j_config);
    std::vector<std::string> split(std::string str, std::string token);
    //in MD, lookup direction is:
    // incoming exchange coinpair ---> our strategy recognized coinpair
    //if coming data 's coinpair is not in this map ,ignore it
    //"strategy_coinpair(base_quote)":"exchange_coinpair",
    std::map<std::string, std::string> keyIsStrategyCoinpairWhiteList;
};

DECLARE_PTR(MDEngineMock);

WC_NAMESPACE_END

#endif
