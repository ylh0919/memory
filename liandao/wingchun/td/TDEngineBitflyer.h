#pragma once
#ifndef PROJECT_TDENGINEBITFLYER_H
#define PROJECT_TDENGINEBITFLYER_H

#include "ITDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include <iostream>
#include <vector>
#include <sstream>
#include <map>
#include <atomic>
#include <mutex>
#include <queue>
#include <thread>
#include <chrono>
#include "Timer.h"
#include <libwebsockets.h>
#include <cpr/cpr.h>
#include <document.h>
#include "../base/ThreadPool.h"
using rapidjson::Document;
using namespace std;
#define minute_num 5
WC_NAMESPACE_START

struct PositionSetting
{
    string ticker;
    bool isLong;
    uint64_t amount;
};
struct OrderInfo
{
    string child_order_acceptance_id;
    int64_t timestamp;
    int64_t requestId;
    string product_code;
};
struct AccountUnitBitflyer
{
    string api_key;
    string secret_key;
    string baseUrl;
    // internal flags
    bool    logged_in;

    CoinPairWhiteList coinPairWhiteList;
    CoinPairWhiteList positionWhiteList;

    map<std::string, OrderInfo> map_new_order;
    std::vector<PositionSetting> positionHolder;//记录每种持仓币种的情况，需要函数来更新
};

class TDEngineBitflyer : public ITDEngine
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
    virtual string name() const { return "TDEngineOceanEx"; };
    void action_order_thread(AccountUnitBitflyer* unit,string product_code,const LFOrderActionField data,int requestId,int errorId,std::string errorMsg);
    void send_order_thread(AccountUnitBitflyer* unit,string product_code,const LFInputOrderField data,int requestId,int errorId,std::string errorMsg);
    // req functions  //this echanger has no account_index ,just give function an any-value
    virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
    virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
    virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
    virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);


public:
    TDEngineBitflyer();
    ~TDEngineBitflyer();

private:
    // journal writers
    yijinjing::JournalWriterPtr raw_writer;
    vector<AccountUnitBitflyer> account_units;
    vector<std::string> request;
    std::mutex* mutex_map_new_order = nullptr;
    inline int64_t get_timestamp_ms();
    inline int64_t get_timestamp_s();
//   void limit(int64_t call_time);
    void limit_place(string data_volume);
    void reset_limit_arr();
    int get_response_parsed_position(cpr::Response r);
    string get_order_type(LfOrderPriceTypeType type);
    string get_order_side(LfDirectionType type);
    void loopOrderAction();
    void DealorderAction();

    cpr::Response rest_withoutAuth(string& method, string& path, string& body);
    cpr::Response rest_withAuth(AccountUnitBitflyer& unit, string& method, string& path, string& body);
    cpr::Response chat();//可用来测试接口实现是否有问题
    cpr::Response get_order(std::string requestId, int type);//type 0 means child order ,type 1 means parent type,check the api document,you will know why

    virtual void set_reader_thread() override;
    void loop();
    void GetAndHandleOrderTradeResponse();
    void retrieveOrderStatus(AccountUnitBitflyer& unit);
    void dealnum(string pre_num,string& fix_num);
    void dealprice(string pre_num,string& fix_num,string Side,string ticker);

private:

    static constexpr int scale_offset = 1e8;
    int rest_interval_milliseconds;
    ThreadPtr rest_thread;
    ThreadPtr orderaction_thread;

    uint64_t last_rest_get_ts = 0;
    uint64_t rest_get_interval_ms = 1000;
    uint64_t period_ms = 1300;
    uint64_t last_5_ts = 0;
    
 //   int remain = -1;
  //  int64_t reset = 0;
    int enable_call_num = 500;
    int HTTP_RESPONSE_OK = 200;

    std::queue<int64_t> call_queue;
    std::map<string,LFRtnOrderField> ordermap;
    std::map<std::string,std::string> cancel_map;
    ThreadPool* m_ThreadPoolPtr = nullptr ;
    std::mutex* mutex_order_and_trade = nullptr;
    std::mutex* mutex_response_order_status = nullptr;
    std::mutex* mutex_orderaction_waiting_response = nullptr;
    std::mutex* mutex_order_map = nullptr;
    std::mutex* mutex_cancel_map = nullptr;
    

};

WC_NAMESPACE_END

#endif //PROJECT_TDENGINEOCEANEX_H




