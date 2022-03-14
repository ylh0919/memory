
#ifndef PROJECT_TDENGINEMOCKBASE_H
#define PROJECT_TDENGINEMOCKBASE_H

#include "ITDEngine.h"
#include "longfist/LFConstants.h"
#include <vector>
#include <sstream>
#include <map>
#include <atomic>
#include <mutex>
#include "Timer.h"
#include "CoinPairWhiteList.h"
#include <thread>

WC_NAMESPACE_START

struct AccountUnitMock
{
    // internal flags
    bool    logged_in;
    CoinPairWhiteList coinPairWhiteList;
    CoinPairWhiteList positionWhiteList;
};

struct OrderBook
{
    std::map<int64_t,map<int64_t,LFRtnOrderField>,std::greater<int64_t> > orderbook_buy_line; // <price,<ordertime,order>> the price is high to low; the time is low to high
    std::map<int64_t,map<int64_t,LFRtnOrderField> > orderbook_sell_line; // the price is low to high; the time is low to high    
};

const int LIMIT = 1000; // Return the maximum value

/**
 * CTP trade engine
 */
class TDEngineMockBase: public ITDEngine
{
public:
    
    /** init internal journal writer (both raw and send) */
    virtual void init();
    /** for settleconfirm and authenticate setting */
    virtual void pre_load(const json& j_config);
    virtual TradeAccount load_account(int idx, const json& j_account);
    virtual void resize_accounts(int account_num);
    /** connect && login related */
    virtual void connect(long timeout_nsec);
    virtual void login(long timeout_nsec);
    virtual void logout();
    virtual void release_api();
    virtual bool is_connected() const;
    virtual bool is_logged_in() const;
    virtual string name() const { return module_name; };
    virtual void set_reader_thread();

    // req functions
    virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
    virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
    virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
    // editted
    virtual void req_batch_cancel_orders(const LFBatchCancelOrderField* data, int account_index, int requestId, long rcv_time);
    virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);
    virtual void req_get_kline_via_rest(const GetKlineViaRest *data, int account_index, int requestId, long rcv_time);

    /* load history data*/
    virtual void load_history_data();
public:
    TDEngineMockBase(std::string exchange_name,int exchange_id);
    ~TDEngineMockBase();
private:
    std::string exchange_name = "MOCK";
    std::string module_name = "TDMOCK";
    int QRY_POS_MOCK = 20201;
    int ORDER_MOCK = 20204;
    int ORDER_ACTION_MOCK = 20207;
    // journal writers
    yijinjing::JournalWriterPtr raw_writer;
    vector<AccountUnitMock> account_units;

    bool load_data_flag = false;

    std::vector<std::vector<std::string>> mockData; // read data to memory
    std::string history_data_path = "";             // history data path
    int history_date_type = -1;                     // history data type: 110, 210to110
    int period_millisec = 60000;

    int source_id_105 = 20; // SOURCE_MOCK
    int64_t trade_latency_ns = 1000;
    int64_t cancel_latency_ns = 1000;
    //std::map<int64_t,map<int64_t,LFRtnOrderField>,std::greater<int64_t> > orderbook_buy_line; // <price,<ordertime,order>> the price is high to low; the time is low to high
    //std::map<int64_t,map<int64_t,LFRtnOrderField> > orderbook_sell_line; // the price is low to high; the time is low to high    
    std::map<string,int64_t> orderref_price_map;  // local orderbook
    std::map<std::string, OrderBook> orderbook_map;  // ticker-orderbook
    JournalReaderPtr reader2;
    void listening2();

    void write_orderbook(const char*,const char*,uint64_t,std::map<std::string, OrderBook>::iterator orderbook_it);
    void init_orderbook(AccountUnitMock unit);
    //modified
    void parse(const LFBatchCancelOrderField* data, LFOrderActionField* res, int index);

    std::shared_ptr<std::thread> rest_thread;

private:
    inline std::string getTimestampString();
    void updateOrderBook106(LfDirectionType direction,int64_t price, uint64_t&  volume,std::map<std::string, OrderBook>::iterator orderbook_it);

};

WC_NAMESPACE_END

#endif //PROJECT_TDEngineMock_H



