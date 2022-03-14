
#ifndef PROJECT_TDENGINEFTX_H
#define PROJECT_TDENGINEFTX_H

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
#include <set>
#include <stringbuffer.h>
#include "../base/ThreadPool.h"
using rapidjson::Document;
using rapidjson::StringBuffer;
using namespace std;
WC_NAMESPACE_START

struct Precision
{
    int64_t min_size;
    int64_t price_filter;
    int64_t lot_size;
};

struct AccountUnitFtx
{
    string api_key;//uid
    string secret_key;

    string baseUrl;
    // internal flags
    bool    logged_in;
    CoinPairWhiteList coinPairWhiteList;
    CoinPairWhiteList positionWhiteList;
};

struct QuoteReuqest
{
    std::map<int64_t,LFRtnQuoteField> mapQuote;
    LFRtnOrderField data;
};
/**
 * CTP trade engine
 */
class TDEngineFtx: public ITDEngine
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
    virtual string name() const { return "TDEngineFtx"; };
    void send_order_thread(AccountUnitFtx* unit, string ticker,const LFInputOrderField data,int requestId,int errorId,std::string errorMsg);
    // req functions
    void action_order_thread(AccountUnitFtx* unit,string ticker,const LFOrderActionField data,int requestId,int64_t remoteOrderId,int errorId,std::string errorMsg);

    virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
    virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
    virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
    virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);
    virtual void req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId);
    virtual void req_inner_transfer(const LFTransferField* data, int account_index, int requestId);
    virtual void req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw);
    /** insert quote */
    virtual void req_quote_insert(const LFInputQuoteField* data, int account_index, int requestId, long rcv_time);
    /** request cancel quote*/
    virtual void req_quote_action(const LFQuoteActionField* data, int account_index, int requestId, long rcv_time);
    virtual void set_reader_thread() override;
public:
    TDEngineFtx();
    ~TDEngineFtx();

private:
    // journal writers
    yijinjing::JournalWriterPtr raw_writer;
    vector<AccountUnitFtx> account_units;

    std::string GetSide(const LfDirectionType& input);
    LfDirectionType GetDirection(std::string input);
    std::string GetType(const LfOrderPriceTypeType& input);
    LfOrderPriceTypeType GetPriceType(std::string input);
    LfOrderStatusType GetOrderStatus(std::string state);
    inline int64_t getTimestamp();
    void loop();
    void get_account(AccountUnitFtx& unit, Document& json);
    void send_order(AccountUnitFtx& unit, std::string underlying, std::string type,std::string strike,
                                 std::string side,int64_t expiry, std::string price, std::string volume,bool is_hide,int64_t requestExpiry, Document& json);
    void cancel_order(AccountUnitFtx& unit, std::string code, int64_t orderId, Document& json);

    void getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json);
    void getQuoteStatus(AccountUnitFtx& unit);       
    void getQuoteRequestStatus(AccountUnitFtx& unit);
    void getQuoteFills(AccountUnitFtx& unit);
    std::string createQuoteRequestString(std::string underlying,std::string type,std::string side,std::string strike,int64_t expiry,std::string price,std::string volume,bool is_hide,int64_t requestExpiry);

    cpr::Response Delete(const std::string& url,AccountUnitFtx& unit, std::string parameters);
    cpr::Response Get(const std::string& url,AccountUnitFtx& unit, std::string parameters);
    cpr::Response Post(const std::string& url,const std::string& body, AccountUnitFtx& unit);
    void dealPriceVolume(AccountUnitFtx& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,std::string& nDealPrice,std::string& nDealVome,bool isbuy);
    void getServerTime();
    bool isClosed(char status);
public:
    //websocket
    void ftxAuth(AccountUnitFtx& unit);

private:
    bool isMargin=false;
    int HTTP_RESPONSE_OK = 200;
    static constexpr int scale_offset = 1e8;

    ThreadPtr rest_thread;

    uint64_t last_rest_get_ts = 0;
    uint64_t rest_get_interval_ms = 500;
    AccountUnitFtx& get_current_account();

    ThreadPool* m_ThreadPoolPtr = nullptr ;
    std::map<std::string, int64_t> localOrderRefRemoteOrderId;
    std::map<std::string, Precision> precision_map;
    std::map<int64_t, QuoteReuqest> order_map;
    std::map<int64_t, LFRtnQuoteField> quote_map;
    int max_rest_retry_times = 3;
    int retry_interval_milliseconds = 1000;

};

WC_NAMESPACE_END

#endif //PROJECT_TDENGINEHuoTDEngineFtx_H




