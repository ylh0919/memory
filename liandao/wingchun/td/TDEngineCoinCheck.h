#ifndef PROJECT_TDENGINECOINCHECK_H
#define PROJECT_TDENGINECOINCHECK_H

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
#include <stringbuffer.h>
#include "../base/ThreadPool.h"
using rapidjson::Document;
using rapidjson::StringBuffer;
WC_NAMESPACE_START

/**
 * account information unit extra is here.
 */
struct PendingOrderStatus
{
    LFRtnOrderField rtn_order;
    int64_t averagePrice;
};
struct OrderActionSentTime
{
    LFOrderActionField data;
    int requestId;
    int64_t sentNameTime;
};
struct TradeMsg
{
    std::string orderid;
    int64_t tid;
    int64_t price;
    uint64_t volume;
};
struct ResponsedOrderStatus
{
    std::string ticker;
    int64_t averagePrice = 0;
    //今成交数量
    uint64_t VolumeTraded;
    int id = 0;
    uint64_t openVolume = 0;
    int64_t PriceTraded = 0;
    //报单状态
    LfOrderStatusType OrderStatus;
    uint64_t volume = 0;
};
//价格和数量精度
/*
字段名称    数据类型    描述
base-currency   string  交易对中的基础币种
quote-currency  string  交易对中的报价币种
price-precision integer 交易对报价的精度（小数点后位数）
amount-precision    integer 交易对基础币种计数精度（小数点后位数）
symbol-partition    string  交易区，可能值: [main，innovation，bifurcation]
*/
struct PriceVolumePrecision
{
    std::string baseCurrency;
    std::string quoteCurrency;
    int pricePrecision=0;
    int amountPrecision=0;
    std::string symbol;
};
enum CoincheckWsStatus{
    nothing,
    Coincheck_auth,
    accounts_sub,
    orders_sub,
    accounts_list_req,
    order_list_req,
    order_detail_req
};
struct AccountUnitCoincheck
{
    string api_key;//uid
    string secret_key;
    string passphrase;
    std::string customer_id="brwv5325";

    string baseUrl;
    // internal flags
    bool    logged_in;
    std::vector<PendingOrderStatus> newOrderStatus;
    std::vector<PendingOrderStatus> pendingOrderStatus;
    std::map<std::string,PriceVolumePrecision> mapPriceVolumePrecision;
    CoinPairWhiteList coinPairWhiteList;
    CoinPairWhiteList positionWhiteList;
    std::string spotAccountId;
    std::string marginAccountId;
    std::string userref;
    struct lws* webSocketConn;
    map<string,LFRtnOrderField> restOrderStatusMap;
    vector<string> websocketOrderStatusMap;
};
/**
 * CTP trade engine
 */
class TDEngineCoincheck: public ITDEngine
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
    virtual string name() const { return "TDEngineCoincheck"; };

    // req functions
    virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
    virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
    virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
    virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);


public:
    TDEngineCoincheck();
    ~TDEngineCoincheck();

private:
    // journal writers
    yijinjing::JournalWriterPtr raw_writer;
    vector<AccountUnitCoincheck> account_units;

    std::string GetSide(const LfDirectionType& input);
    LfDirectionType GetDirection(std::string input);
    std::string GetType(const LfOrderPriceTypeType& input);
    LfOrderPriceTypeType GetPriceType(std::string input);
    LfOrderStatusType GetOrderStatus(std::string state);
    inline int64_t getTimestamp();

    virtual void set_reader_thread() override;
    void loop();
    void GetAndHandleOrderTradeResponse();
    void addNewQueryOrdersAndTrades(AccountUnitCoincheck& unit, PendingOrderStatus pOrderStatus, std::string& remoteOrderId);

    void retrieveOrderStatus(AccountUnitCoincheck& unit);
    void moveNewOrderStatusToPending(AccountUnitCoincheck& unit);
    void handlerResponseOrderStatus(AccountUnitCoincheck& unit, std::vector<PendingOrderStatus>::iterator itr, 
                                        ResponsedOrderStatus& responsedOrderStatus);

    void loopOrderActionNoResponseTimeOut();
    void orderActionNoResponseTimeOut();
    void orderIsCanceled(AccountUnitCoincheck& unit, LFRtnOrderField* rtn_order);

private:
    void get_account(AccountUnitCoincheck& unit, Document& json);
    void cancel_all_orders(AccountUnitCoincheck& unit, Document& json);
    void send_order(AccountUnitCoincheck& unit, string userref, string code,
                        string side, string type, string volume, string price, Document& json);

    void cancel_order(AccountUnitCoincheck& unit, std::string code, std::string orderId, Document& json);
    void get_open_orders(AccountUnitCoincheck& unit, Document& json);
    void query_order(AccountUnitCoincheck& unit, std::string code, std::string orderId, Document& json);
    void getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json);
    void printResponse(const Document& d);

    bool shouldRetry(Document& d);
    
    std::string createInsertOrdertring(string pair,string type,string ordertype,string price,string volume,
        string oflags,string userref);

    cpr::Response Get(const std::string& url,const std::string& body, std::string postData,AccountUnitCoincheck& unit);
    cpr::Response Post(const std::string& url,const std::string& body, std::string postData,AccountUnitCoincheck& unit);
    void genUniqueKey();
    std::string genClinetid(const std::string& orderRef);
    //精度处理
    void getPriceVolumePrecision(AccountUnitCoincheck& unit);
    void dealPriceVolume(AccountUnitCoincheck& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,std::string& nDealPrice,std::string& nDealVome);

    std::string parseJsonToString(Document &d);
    void addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId);
    void Ping(struct lws* conn);
    void Pong(struct lws* conn,long long ping);
    AccountUnitCoincheck& findAccountUnitCoincheckByWebsocketConn(struct lws * websocketConn);
    std::string makeSubscribeOrdersUpdate(AccountUnitCoincheck& unit);
    int64_t getMSTime();
    void dealnum(string pre_num,string& fix_num);
    void getPrecision();
    void handle_order_insert(AccountUnitCoincheck& unit,const LFInputOrderField data,int requestId,const std::string& ticker);
    void handle_order_action(AccountUnitCoincheck& unit,const LFOrderActionField data, int requestId,const std::string& ticker);
public:
    //cys add kraken websocket status
    CoincheckWsStatus wsStatus = nothing;
    CoincheckWsStatus isAuth = nothing,isOrders=nothing;
    //当webSocket建立时
    void on_lws_open(struct lws* wsi);
    std::string getCoincheckSignature(std::string& path,std::string& nonce, std::string postdata,AccountUnitCoincheck& unit);
    std::vector<unsigned char> sha256(string& data);
    vector<unsigned char> hmac_sha512_Coincheck(vector<unsigned char>& data,vector<unsigned char> key);
    std::string b64_encode(const std::vector<unsigned char>& data);
    std::vector<unsigned char> b64_decode(const std::string& data) ;
public:
    //websocket
    void lws_login(AccountUnitCoincheck& unit, long timeout_nsec);
    void writeInfoLog(std::string strInfo);
    void writeErrorLog(std::string strError);
    //ws_service_cb回调函数
    void on_lws_data(struct lws* conn, const char* data, size_t len);
    int subscribeTopic(struct lws* conn,string strSubscribe);
    int on_lws_write_subscribe(struct lws* conn);
    void on_lws_connection_error(struct lws* conn);
    void on_lws_close(struct lws* conn);
    //websocket deal order status

private:
    bool m_isPong = false;
    string version="0";
    struct lws_context *context = nullptr;
    struct lws* m_conn;
private:
    std::string m_uniqueKey;
    int HTTP_RESPONSE_OK = 200;
    static constexpr int scale_offset = 1e8;

    ThreadPtr rest_thread;
    ThreadPtr orderaction_timeout_thread;

    uint64_t last_rest_get_ts = 0;
    uint64_t rest_get_interval_ms = 500;

    std::mutex* mutex_order_and_trade = nullptr;
    std::mutex* mutex_response_order_status = nullptr;
    std::mutex* mutex_orderaction_waiting_response = nullptr;

    std::map<std::string, std::string> localOrderRefRemoteOrderId;
    std::map<std::string, LFRtnOrderField> order_map;
    std::map<std::string, std::string> iscancel_map;
    std::map<std::string, int> precision_map;

    //对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
    std::map<std::string, OrderActionSentTime> remoteOrderIdOrderActionSentTime;
    std::vector<TradeMsg> TradeMsg_vec;
    int max_rest_retry_times = 3;
    int retry_interval_milliseconds = 1000;
    int orderaction_max_waiting_seconds = 30;
    ThreadPool* m_ThreadPoolPtr = nullptr;

};

WC_NAMESPACE_END

#endif //PROJECT_TDENGINEHuoTDEngineKraken_H





