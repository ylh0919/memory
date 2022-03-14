
#ifndef PROJECT_TDENGINEHBDM_H
#define PROJECT_TDENGINEHBDM_H

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
#include <websocket.h>
#include <libwebsockets.h>
#include <cpr/cpr.h>
#include <stringbuffer.h>
using rapidjson::Document;
using rapidjson::StringBuffer;
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
enum HbdmWsStatus{
    nothing,
    hbdm_auth,
    accounts_sub,
    orders_sub,
    accounts_list_req,
    order_list_req,
    order_detail_req
};
struct WsMsg
{
    bool is_swap;
    std::string ws_url;
    common::CWebsocket websocket;
    bool is_ws_disconnectd = false;
    bool logged_in;
};
struct AccountUnitHbdm
{
    string api_key;//uid
    string secret_key;
    string passphrase;

    string baseUrl;
    // internal flags
    bool    logged_in;
    bool    is_connecting = false;
    std::vector<PendingOrderStatus> newOrderStatus;
    std::vector<PendingOrderStatus> pendingOrderStatus;
    std::map<std::string,PriceVolumePrecision> mapPriceVolumePrecision;
    CoinPairWhiteList coinPairWhiteList;
    CoinPairWhiteList positionWhiteList;
    std::string spotAccountId;
    std::string marginAccountId;
    struct lws* webSocketConn;
    map<string,LFRtnOrderField> restOrderStatusMap;
    vector<string> websocketOrderStatusMap;
    std::vector<WsMsg> ws_msg_vec;
};

struct OrderActionSentTime
{
    LFOrderActionField data;
    int requestId;
    int64_t sentNameTime;
    AccountUnitHbdm unit;
};

/**
 * CTP trade engine
 */
class TDEngineHbdm: public ITDEngine,public common::CWebsocketCallBack
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
    virtual string name() const { return "TDEngineHbdm"; };

    // req functions
    virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
    virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
    virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
    virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);
    virtual void req_get_kline_via_rest(const GetKlineViaRest* data, int account_index, int requestId, long rcv_time);

    virtual void OnConnected(const common::CWebsocket* instance);
    virtual void OnReceivedMessage(const common::CWebsocket* instance,const std::string& msg);
    virtual void OnDisconnected(const common::CWebsocket* instance);    

public:
    TDEngineHbdm();
    ~TDEngineHbdm();

private:
    // journal writers
    yijinjing::JournalWriterPtr raw_writer;
    vector<AccountUnitHbdm> account_units;

    std::string GetSide(const LfDirectionType& input);
    LfDirectionType GetDirection(std::string input);
    std::string GetType(const LfOrderPriceTypeType& input);
    LfOrderPriceTypeType GetPriceType(std::string input);
    LfOrderStatusType GetOrderStatus(std::string state);
    inline int64_t getTimestamp();

    virtual void set_reader_thread() override;

    void handleResponseOrderStatus(AccountUnitHbdm& unit, LFRtnOrderField& rtn_order, 
                                        Document& json);
    void loopOrderActionNoResponseTimeOut();
    void orderActionNoResponseTimeOut();
    void loopwebsocket();
private:
    void get_account(AccountUnitHbdm& unit, Document& json, bool is_swap);
    void send_order(AccountUnitHbdm& unit, const char *code,
                            const char *side, const char *type, int volume, std::string price, Document& json,std::string offset,std::string cid,bool isPostOnly);
    void cancel_all_orders(AccountUnitHbdm& unit, std::string code, Document& json);
    void cancel_order(AccountUnitHbdm& unit, std::string code, std::string orderId, Document& json);
    void query_order(AccountUnitHbdm& unit, std::string code, std::string orderId, Document& json);
    int orderIsTraded(AccountUnitHbdm& unit, std::string code, std::string orderId, Document& json);
    void getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json);
    void printResponse(const Document& d);

    bool shouldRetry(Document& d);
    
    std::string createInsertOrdertring(const char *accountId,
                    int amount, const char *price, const char *source, const char *symbol,const char *side,std::string offset,std::string type,std::string cid,bool isPostOnly);

    cpr::Response Get(const std::string& url,const std::string& body, AccountUnitHbdm& unit);
    cpr::Response Post(const std::string& url,const std::string& body, AccountUnitHbdm& unit);
    void genUniqueKey();
    std::string genClinetid(const std::string& orderRef);
    //火币精度处理
    void getPriceVolumePrecision(AccountUnitHbdm& unit);
    void dealnum(string pre_num,string& fix_num);
    void dealPriceVolume(AccountUnitHbdm& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,std::string& nDealPrice,std::string& nDealVome);

    std::string parseJsonToString(Document &d);
    void addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId,std::string remoteOrderId,AccountUnitHbdm& unit);
    void Ping(struct lws* conn);
    void Pong(const common::CWebsocket* instance,long long ping);
    AccountUnitHbdm& findAccountUnitHbdmByWebsocketConn(struct lws * websocketConn);
    std::string makeSubscribeOrdersUpdate(AccountUnitHbdm& unit, string ticker);
    int64_t getMSTime();
    int64_t GetMillsecondByInterval(const std::string& interval);
    void DealCancel(std::string order_id_str);
    void DealTrade(std::string order_id_str,int64_t price,uint64_t volume);
    void onOrderChange(Document& json);
    void subscribeTopic(const common::CWebsocket* instance);

public:
    //cys add Hbdm websocket status
    HbdmWsStatus wsStatus = nothing;
    HbdmWsStatus isAuth = nothing,isOrders=nothing;
    //当webSocket建立时
    void on_lws_open(struct lws* wsi);
    //zip 压缩和解压
    int gzCompress(const char *src, int srcLen, char *dest, int destLen);
    int gzDecompress(const char *src, int srcLen, const char *dst, int dstLen);
    //cys add
    char dec2hexChar(short int n);
    std::string escapeURL(const string &URL);
    void getAccountId(AccountUnitHbdm& unit);
    std::string getHbdmTime();
    std::string getHbdmNormalTime();
    std::string getHbdmSignatrue(std::string parameters[],int psize,std::string timestamp,std::string method_url,std::string reqType,AccountUnitHbdm& unit);
public:
    //websocket
    void hbdmAuth(AccountUnitHbdm& unit,const common::CWebsocket* instance);
    void lws_login(AccountUnitHbdm& unit, long timeout_nsec);
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
    struct lws_context *context = nullptr;
    std::string m_strToken;
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
    std::map<std::string,LFRtnOrderField> m_mapOrder;
    std::map<std::string, double> precision_map;

    //对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
    std::map<std::string, OrderActionSentTime> remoteOrderIdOrderActionSentTime;
    int max_rest_retry_times = 3;
    int retry_interval_milliseconds = 1000;
    int orderaction_max_waiting_seconds = 30;
    int lever_rate = 10;

};

WC_NAMESPACE_END

#endif //PROJECT_TDENGINEHuoTDEngineHbdm_H





