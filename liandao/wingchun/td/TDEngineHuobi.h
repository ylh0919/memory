
#ifndef PROJECT_TDENGINEHUOBI_H
#define PROJECT_TDENGINEHUOBI_H

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
#include <queue>
#include <stringbuffer.h>
#include "../base/ThreadPool.h"
using rapidjson::Document;
using rapidjson::StringBuffer;
using namespace std;
WC_NAMESPACE_START

/**
 * account information unit extra is here.
 */

struct PendingOrderStatus
{
    char_31 InstrumentID = {0};   //合约代码
    char_21 OrderRef = {0};       //报单引用
    LfOrderStatusType OrderStatus = LF_CHAR_NotTouched;  //报单状态
    uint64_t VolumeTraded = 0;  //今成交数量
    int64_t averagePrice = 0;// given averagePrice on response of query_order
    std::string remoteOrderId;// sender_order response order id://{"orderId":19319936159776,"result":true}
};


/*
struct OrderInsertSentTime
{
    LFInputOrderField data;
    int requestId;
    int64_t sentNameTime;
    AccountUnitHuobi unit;
};*/

struct ResponsedOrderStatus
{
    int64_t averagePrice = 0;
    std::string ticker;
    int64_t createdDate = 0;


    //今成交数量
    uint64_t VolumeTraded;
    int id = 0;
    uint64_t openVolume = 0;
    std::string orderId;
    std::string orderType;
    //报单价格条件
    LfOrderPriceTypeType OrderPriceType;
    int64_t price = 0;
    //买卖方向
    LfDirectionType Direction;

    //报单状态
    LfOrderStatusType OrderStatus;
    uint64_t trunoverVolume = 0;
    uint64_t volume = 0;
};
//价格和数量精度
/*
字段名称	数据类型	描述
base-currency	string	交易对中的基础币种
quote-currency	string	交易对中的报价币种
price-precision	integer	交易对报价的精度（小数点后位数）
amount-precision	integer	交易对基础币种计数精度（小数点后位数）
symbol-partition	string	交易区，可能值: [main，innovation，bifurcation]
*/
struct PriceVolumePrecision
{
    std::string baseCurrency;
    std::string quoteCurrency;
    int pricePrecision=0;
    int amountPrecision=0;
    std::string symbolPartition;
    std::string symbol;
};
struct UpdateMsg
{
    std::string orderId;
    double fillamount = 0;
    LfOrderStatusType status = LF_CHAR_NotTouched;
};
enum HuobiWsStatus{
    nothing,
    huobi_auth,
    accounts_sub,
    orders_sub,
    accounts_list_req,
    order_list_req,
    order_detail_req
};
struct AccountUnitHuobi
{
    string api_key;//uid
    string secret_key;

    string baseUrl;
    // internal flags
    bool    logged_in;
    bool    is_connecting = false;
    std::map<std::string,PriceVolumePrecision> mapPriceVolumePrecision;
    CoinPairWhiteList coinPairWhiteList;
    CoinPairWhiteList positionWhiteList;
    std::string spotAccountId;
    std::string marginAccountId;
    struct lws* webSocketConn;
    HuobiWsStatus isAuth = nothing,isOrders=nothing;
    queue<string> sendmessage;
    struct lws_context *context = nullptr;
    map<string,LFRtnOrderField> restOrderStatusMap;
    vector<string> websocketOrderStatusMap;
};
struct OrderActionSentTime
{
    LFOrderActionField data;
    int requestId;
    int64_t sentNameTime;
    AccountUnitHuobi unit;
    std::string orderid;
};
/**
 * CTP trade engine
 */
class TDEngineHuobi: public ITDEngine
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
    virtual string name() const { return "TDEngineHuobi"; };
    void send_order_thread(AccountUnitHuobi* unit, string ticker,const LFInputOrderField data,int requestId,int errorId,std::string errorMsg);
    // req functions
    void action_order_thread(AccountUnitHuobi* unit,string ticker,const LFOrderActionField data,int requestId,std::string remoteOrderId,int errorId,std::string errorMsg);

    virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
    virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
    virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
    virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);
    virtual void req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId);
    virtual void req_inner_transfer(const LFTransferField* data, int account_index, int requestId);
    virtual void req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw);
    virtual void req_get_kline_via_rest(const GetKlineViaRest* data, int account_index, int requestId, long rcv_time);
public:
    TDEngineHuobi();
    ~TDEngineHuobi();

private:
    // journal writers
    yijinjing::JournalWriterPtr raw_writer;
    vector<AccountUnitHuobi> account_units;

    std::string GetSide(const LfDirectionType& input);
    LfDirectionType GetDirection(std::string input);
    std::string GetType(const LfOrderPriceTypeType& input);
    LfOrderPriceTypeType GetPriceType(std::string input);
    LfOrderStatusType GetOrderStatus(std::string state);
    inline int64_t getTimestamp();
    int get_transfer_status(std::string status,bool is_withdraw = true);
    virtual void set_reader_thread() override;
    void addNewOrderToMap(AccountUnitHuobi& unit, LFRtnOrderField& rtn_order);
    void handleResponseOrderStatus(AccountUnitHuobi& unit, LFRtnOrderField& rtn_order, Document& json);
    void handleWSOrderStatus(AccountUnitHuobi& unit, LFRtnOrderField& rtn_order, Document& json);
    void loopOrderActionNoResponseTimeOut();
    void orderActionNoResponseTimeOut();
    void loopOrderInsertNoResponseTimeOut();
    void orderInsertNoResponseTimeOut();
    void loopwebsocket();
private:
    void get_account(AccountUnitHuobi& unit, Document& json,bool isMargin = false);
    void get_sub_account(string subuid, AccountUnitHuobi& unit, Document& json);
    //void send_order(AccountUnitHuobi& unit, const char *code,
    //                        const char *side, const char *type, std::string volume, std::string price, Document& json, std::string cid,bool isPostOnly);
    void send_order(AccountUnitHuobi& unit, const char *code, const char *side, const char *type, std::string volume,
                              std::string price, Document& json, std::string& output_cid, const LFInputOrderField data, bool isPostOnly);
    void withdrawl_currency(string address, string amount, string asset, string addresstag, Document& json, AccountUnitHuobi& unit);
    void inner_transfer(string symbol, string currency, string amount, string type, Document& json, AccountUnitHuobi& unit);
    void sub_inner_transfer(string subuid,string currency,string amount,string type, Document& json, AccountUnitHuobi& unit);
    void get_transfer_history(std::string currency,std::string type, AccountUnitHuobi& unit, Document& json);
    void cancel_all_orders(AccountUnitHuobi& unit, std::string code, Document& json);
    void cancel_order(AccountUnitHuobi& unit, std::string code, std::string orderId, Document& json);
    void query_order(AccountUnitHuobi& unit, std::string code, std::string orderId, Document& json);
    int orderIsTraded(AccountUnitHuobi& unit, std::string code, std::string orderId, Document& json);
    void getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json);
    void dealnum(string pre_num,string& fix_num);
    void printResponse(const Document& d);

    bool shouldRetry(Document& d);
    
    std::string createInsertOrdertring(const char *accountId,
                    const char *amount, const char *price, const char *source, const char *symbol,const char *type,std::string cid);

    cpr::Response Get(const std::string& url,const std::string& body, AccountUnitHuobi& unit, std::string parameters);
    cpr::Response Post(const std::string& url,const std::string& body, AccountUnitHuobi& unit);
    void genUniqueKey();
    std::string genClinetid(const std::string& orderRef);
    //火币精度处理
    void getPriceVolumePrecision(AccountUnitHuobi& unit);
    void dealPriceVolume(AccountUnitHuobi& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,std::string& nDealPrice,std::string& nDealVome);

    std::string parseJsonToString(Document &d);
    void addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId, AccountUnitHuobi& unit, std::string orderid);
    void addRemoteOrderIdOrderInsertSentTime(const LFInputOrderField* data, int requestId, const std::string& remoteOrderId, AccountUnitHuobi& unit);
    void Ping(struct lws* conn);
    void Pong(struct lws* conn,long long ping);
    AccountUnitHuobi& findAccountUnitHuobiByWebsocketConn(struct lws * websocketConn);
    std::string makeSubscribeOrdersUpdate(AccountUnitHuobi& unit, string ticker);
    int64_t  GetMillsecondByInterval(const std::string& interval);

public:
    //cys add huobi websocket status
    HuobiWsStatus wsStatus = nothing;
    
    //当webSocket建立时
    void on_lws_open(struct lws* wsi);
    //zip 压缩和解压
    int gzCompress(const char *src, int srcLen, char *dest, int destLen);
    int gzDecompress(const char *src, int srcLen, const char *dst, int dstLen);
    //cys add
    char dec2hexChar(short int n);
    std::string escapeURL(const string &URL);
    void getAccountId(AccountUnitHuobi& unit);
    std::string getHuobiTime();
    std::string getHuobiNormalTime();
    std::string getHuobiSignatrue(std::string parameters[],int psize,std::string timestamp,std::string method_url,std::string reqType,AccountUnitHuobi& unit,std::string parameter);
public:
    //websocket
    void huobiAuth(AccountUnitHuobi& unit);
    void lws_login(AccountUnitHuobi& unit, long timeout_nsec);
    void writeInfoLog(std::string strInfo);
    void writeErrorLog(std::string strError);
    //ws_service_cb回调函数
    void on_lws_data(struct lws* conn, const char* data, size_t len);
    int subscribeTopic(struct lws* conn,string strSubscribe);
    int on_lws_write_subscribe(struct lws* conn);
    void on_lws_connection_error(struct lws* conn);
    void on_lws_close(struct lws* conn);
    void on_lws_receive_orders(struct lws* conn,Document& json);
    //websocket deal order status

private:
    bool isMargin=false;
    bool m_shouldPing = true;
    bool m_isPong = false;
    bool m_isSubL3 = false;
    
    std::string m_strToken;
    struct lws* m_conn;
private:
    std::string m_uniqueKey;
    int HTTP_RESPONSE_OK = 200;
    static constexpr int scale_offset = 1e8;

    ThreadPtr rest_thread;
    ThreadPtr ac_thread;
    ThreadPtr orderaction_timeout_thread;
    ThreadPtr orderinsert_timeout_thread;

    uint64_t last_rest_get_ts = 0;
    uint64_t rest_get_interval_ms = 500;

    std::mutex* mutex_order_and_trade = nullptr;
    std::mutex* mutex_response_order_status = nullptr;
    std::mutex* mutex_orderaction_waiting_response = nullptr;
    std::mutex* mutex_orderaction_waiting_response1 = nullptr;
    std::mutex* mutex_orderinsert_waiting_response = nullptr;
    std::mutex* mutex_map_pricevolum = nullptr;
    std::mutex* mutex_web_orderstatus = nullptr;
     std::mutex* mutex_web_connect = nullptr;

    std::map<std::string,AccountUnitHuobi*> mapInsertOrders;
    std::map<std::string, LFInputOrderField> cid_map;
    //添加一个map
    std::map<std::string, int> requestId_map;
    std::vector<UpdateMsg> Updatemsg_vec;
    AccountUnitHuobi& get_current_account();

    ThreadPool* m_ThreadPoolPtr = nullptr ;
    std::map<std::string, std::string> localOrderRefRemoteOrderId;

    //对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
    std::map<std::string, OrderActionSentTime> remoteOrderIdOrderActionSentTime;
    std::map<std::string, LFOrderActionField> cancelmap;
    int max_rest_retry_times = 3;
    int retry_interval_milliseconds = 1000;
    int orderaction_max_waiting_seconds = 30;
    int orderinsert_max_waiting_seconds = 30;

};

WC_NAMESPACE_END

#endif //PROJECT_TDENGINEHuoTDEngineHuoBi_H




