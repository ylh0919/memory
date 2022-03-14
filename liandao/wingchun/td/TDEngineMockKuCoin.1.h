
#ifndef PROJECT_TDENGINEMOCK_H
#define PROJECT_TDENGINEMOCK_H

#include "ITDEngine.h"
#include "longfist/LFConstants.h"
#include <vector>
#include <sstream>
#include <map>
#include <atomic>
#include <mutex>
#include "Timer.h"
#include <document.h>
#include "CoinPairWhiteList.h"
using rapidjson::Document;

WC_NAMESPACE_START
/*
1. Mock MD从固定地点取数据写入MD journal；
2. Kucoin Mock TD，用户配置response latency，match latency，limit_exe_price_diff, market_order_105_diff, 
    partial_exe_count (分几次随机量成交，每次间隔match_latency），total_trade_count（总共有几个收到的订单最终完全成交）；
3. Mock Bitmex TD（类似Kucoin）
*/
struct AccountUnitMockKuCoin
{
    int64_t response_latency_ms = 500;//unknown ->NotTouched
    int64_t match_latency_ms = 1000;//NotTouched -> AllTraded/Partially Trade
    double limit_exe_price_diff = 0.05;//现价单成交偏移
    double market_exe_price_diff = 0.05;//市价单成交偏移
    int partial_exe_count = 0;//单个订单部分成交次数
    int total_trade_count =-1;//TD总成交次数
    int64_t match_interval_ms = 0;//每笔成交的间隔时间
    // internal flags
    bool    logged_in;

    CoinPairWhiteList coinPairWhiteList;
    CoinPairWhiteList positionWhiteList;
};
struct OrderStatus
{
    LFRtnOrderField order;
    int exe_count =0;
    int64_t lastChangeTime = 0;
    int64_t expect_price = 0;
};

/**
 * CTP trade engine
 */
class TDEngineMockKuCoin: public ITDEngine
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
    virtual string name() const { return "TDEngineMockKuCoin"; };
    virtual void set_reader_thread();
    // req functions
    virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
    virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
    virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
    virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);

public:
    TDEngineMockKuCoin();
    ~TDEngineMockKuCoin();
private:
    int64_t get_timestamp_ms();
    // journal writers
    yijinjing::JournalWriterPtr raw_writer;
    vector<AccountUnitMockKuCoin> account_units;

    void loop();

    static constexpr int scale_offset = 1e8;
    int total_traded_orders = 0;
    ThreadPtr rest_thread;
    std::map<std::string,OrderStatus> mapOrders;
};

WC_NAMESPACE_END

#endif //PROJECT_TDEngineMock_H



