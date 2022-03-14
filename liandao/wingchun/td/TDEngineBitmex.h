#ifndef PROJECT_TDENGINEBITMEX_H
#define PROJECT_TDENGINEBITMEX_H

#include "ITDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include <vector>
#include <sstream>
#include <map>
#include <atomic>
#include <mutex>
#include "Timer.h"
#include <document.h>
#include <libwebsockets.h>
#include <cpr/cpr.h>
#include "InterfaceMgr.h"
#include "../base/ThreadPool.h"
using rapidjson::Document;

WC_NAMESPACE_START

/**
 * account information unit extra is here.
 */

struct PendingBitmexOrderStatus
{
    char_31 InstrumentID;   //合约代码
    char_21 OrderRef;       //报单引用
    LfOrderStatusType OrderStatus;  //报单状态
    uint64_t VolumeTraded;  //今成交数量
    int64_t averagePrice;
    //std::string remoteOrderId;
    int requestID;
};


struct PendingBitmexTradeStatus
{
    char_31 InstrumentID;   //合约代码
    uint64_t last_trade_id; //for myTrade
};

struct SendOrderFilter
{
    std::string InstrumentID;   //合约代码
    double ticksize; //for price round.
    double lotsize;
    //...other
};

struct SubscribeBitmexBaseQuote
{
    std::string base;
    std::string quote;
};

struct OrderActionSentTime
{
    LFOrderActionField data;
    int requestId;
    int64_t sentNameTime;
};

struct LocalOrderRef
{
    std::string remoteOrderId;
    std::string clordID;
};

//zaf new add
struct OrderTime
{
    LFRtnOrderField data;
    LFInputOrderField datal;
    int64_t startime;
    int64_t amount = 0 ;
};

struct InsertTime
{
    int64_t insert_time;
    int64_t flags = 0;
};

struct AccountUnitBitmex
{
    string api_key;
    string secret_key;
    string baseUrl;
    string wsUrl;
    // internal flags
     bool    logged_in=false;
     bool    is_connecting = false;
    std::map<std::string, SendOrderFilter> sendOrderFilters;
    std::map<std::string, OrderTime> ordersMap;         //zaf change
    std::map<std::string, InsertTime> ordersInsertTimeMap;
    std::map<std::string, OrderActionSentTime> remoteOrderIdOrderActionSentTime; //zaf new add
    CoinPairWhiteList coinPairWhiteList;
    CoinPairWhiteList positionWhiteList;
    std::mutex* mutex_timemap = nullptr;
    std::mutex* mutex_ordermap=nullptr;
    std::mutex* mutex_orderaction = nullptr;
    struct lws * websocketConn = nullptr;
    struct lws_context *context = nullptr;
    int wsStatus=0;
    int maxRetryCount=3;
    AccountUnitBitmex();
    AccountUnitBitmex(const AccountUnitBitmex& source);
    ~AccountUnitBitmex();
};

/**
 * CTP trade engine
 */
class TDEngineBitmex: public ITDEngine
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
    virtual string name() const { return "TDEngineBitmex"; };

    // req functions
    virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
    virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
    virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
    virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);

public:
    TDEngineBitmex();
    ~TDEngineBitmex();



    //websocket
    void on_lws_data(struct lws* conn, const char* data, size_t len);
    void on_lws_connection_error(struct lws* conn);
    int lws_write_subscribe(struct lws* conn);
    bool lws_login(AccountUnitBitmex& unit, long timeout_nsec);
    
    int Round(std::string tickSizeStr);
private:
    // journal writers
    yijinjing::JournalWriterPtr raw_writer;
    vector<AccountUnitBitmex> account_units;
    void send_order_thread(AccountUnitBitmex *, LFInputOrderField, std::string ,int);
    void action_order_thread(AccountUnitBitmex* unit,LFOrderActionField data,std::string ticker, int requestId,std::string remoteOrderId,std::string clordID);
    virtual void set_reader_thread() override;

    std::string GetSide(const LfDirectionType& input);
    LfDirectionType GetDirection(std::string input);
    std::string GetType(const LfOrderPriceTypeType& input);
    LfOrderPriceTypeType GetPriceType(std::string input);
    LfOrderStatusType GetOrderStatus(std::string input);

    std::vector<std::string> split(std::string str, std::string token);
    void addNewQueryOrdersAndTrades(AccountUnitBitmex& unit, const LFInputOrderField* data, const LfOrderStatusType OrderStatus,
                                    const int64_t LimitPrice,const int64_t VolumeTotal,int reqID, std::string newClientId);

    void addRemoteOrderIdOrderActionSentTime(AccountUnitBitmex& unit,const LFOrderActionField* data, int requestId, std::string remoteOrderId);
    void moveNewtoPending(AccountUnitBitmex& unit);
    static constexpr int scale_offset = 1e10;

    int64_t rest_get_interval_ms = 0;
    int64_t base_interval_ms=500;
    int64_t no_response_wait_ms = 2000;
    std::map<std::string, LocalOrderRef> localOrderRefRemoteOrderId;
    int m_limitRate_Remain = 0;
    int64_t m_TimeStamp_Reset;

    std::map<std::string,AccountUnitBitmex*> mapInsertOrders;
//websocket
    AccountUnitBitmex& findAccountUnitByWebsocketConn(struct lws * websocketConn);
    void onOrder(struct lws * websocketConn, Document& json);
    void onTrade(struct lws * websocketConn, Document& json);
    std::string handle_order(AccountUnitBitmex& unit,rapidjson::Value& order);
    void wsloop();
    void loop();
    //void addWebsocketPendingSendMsg(AccountUnitBitmex& unit, std::string msg);
    std::string createAuthJsonString(AccountUnitBitmex& unit );
    std::string createOrderJsonString();

    
    ThreadPtr ws_thread;
    ThreadPtr rest_thread;

private:
    std::string m_uniqueKey;
    int HTTP_RESPONSE_OK = 200;
    int m_CurrentTDIndex = 0;

    void get_account(AccountUnitBitmex& unit, Document& json);

    void get_products(AccountUnitBitmex& unit, Document& json);
    std::string send_order(AccountUnitBitmex& unit, const char *code,
                    const LFInputOrderField* data, int64_t size, int64_t price, int reqID , Document& json, bool isPostOnly);
    std::vector<std::string> get_order(AccountUnitBitmex& unit,int64_t startTime);
    void cancel_all_orders(AccountUnitBitmex& unit, Document& json);
    void cancel_order(AccountUnitBitmex& unit, std::string orderId, Document& json);
    //void query_order(AccountUnitBitmex& unit, std::string code, std::string orderId, Document& json);
    void getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json);
    void printResponse(const Document& d);
    void handleResponse(cpr::Response rsp, Document& json);
    std::string getLwsAuthReq(AccountUnitBitmex& unit);
    std::string getLwsSubscribe(AccountUnitBitmex& unit);
    void genUniqueKey();
    std::string genClinetid(const std::string& orderRef);
    std::map<std::string,double> avgPx_map;


    inline int64_t getTimestamp();
    inline int64_t getTimestampMS();
    int64_t fixPriceTickSize(double keepPrecision, int64_t price, bool isBuy);
    int64_t fixVolumeLotSize(double keepPrecision, int64_t volume);
    bool loadExchangeOrderFilters(AccountUnitBitmex& unit, Document &doc);
    void debug_print(std::map<std::string, SendOrderFilter> &sendOrderFilters);
    SendOrderFilter getSendOrderFilter(AccountUnitBitmex& unit, const std::string& symbol);
    bool ShouldRetry(const Document& json);
    AccountUnitBitmex& get_current_account();
    ThreadPool* m_ThreadPoolPtr=nullptr;
    InterfaceMgr m_interfaceMgr;
    int m_interface_switch = 0;
};

WC_NAMESPACE_END

#endif //PROJECT_TDENGINEBITMEX_H

