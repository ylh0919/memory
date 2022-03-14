
#ifndef PROJECT_TDENGINEKUCOIN_H
#define PROJECT_TDENGINEKUCOIN_H

#include "ITDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include <vector>
#include <sstream>
#include <map>
#include <iostream>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <future>
#include <functional>
#include <mutex>
#include "Timer.h"
#include <document.h>
#include <libwebsockets.h>
#include <cpr/cpr.h>
#include "../base/ThreadPool.h"
using rapidjson::Document;

WC_NAMESPACE_START

/**
 * account information unit extra is here.
 */

        struct PendingOrderStatus
        {
            int64_t nVolume = 0;
            int64_t nPrice = 0;
            char_31 InstrumentID = {0};   //合约代码
            char_21 OrderRef = {0};   //报单引用
            int nRequestID = -1;   
            std::string strUserID;
            LfOrderStatusType OrderStatus = LF_CHAR_NotTouched;  //报单状态
            uint64_t VolumeTraded = 0;  //成交数量
            LfDirectionType Direction;  //买卖方向
            LfOrderPriceTypeType OrderPriceType; //报单价格条件
            std::string remoteOrderId;// sender_order response order id://{"orderId":19319936159776,"result":true}
            std::string strClientId;
            int64_t nSendTime = 0;
            LfOffsetFlagType OffsetFlag;
            LFInputOrderField data;
        };

        struct KEY
        {
            std::string api_key;
            std::string secret_key;
            std::string passphrase;
        };

        struct OrderActionSentTime
        {
            LFOrderActionField data;
            int requestId;
            int64_t sentNameTime;
            int64_t nSendTime;
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

          struct PriceIncrement
        {
            int64_t nBaseMinSize = 0;
            int64_t nPriceIncrement = 0;
            int64_t nQuoteIncrement = 0;
            int64_t nBaseIncrement = 0;
        };

        struct CurrencyList
        {
            std::string currency;
            std::string name;
            std::string fullName;
            int precision;
            bool isWithdrawEnabled;
            bool isDepositEnabled;
            int withdrawalMinFee;
            int withdrawalMinSize;
        };

        struct SubUser
        {
            std::string userId;
            std::string subName;
            std::string remarks;
        };

        struct Items_withdrawals
        {
            string id;
            string address;
            string memo;
            string currency;
            double amount;
            double fee;
            string walletTxId;
            bool isInner;
            string status;
            int64_t createdAt;
            int64_t updatedAt;
        };

        struct WithdrawalsList
        {
            string currentPage;
            string pageSize;
            string totalNum;
            string totalPage;
            Items_withdrawals items;
        };

        struct Items_deposits
        {
            string address;
            string memo;
            string currency;
            double amount;
            double fee;
            string walletTxId;
            bool isInner;
            string status;
            int64_t createdAt;
            int64_t updatedAt;
        };

        struct DepositsList
        {
            string currentPage;
            string pageSize;
            string totalNum;
            string totalPage;
            Items_withdrawals items;
        };

        
        struct AccountUnitKuCoin
        {
            string api_key;//uid
            string secret_key;
            string passphrase;
            string baseUrl;
            // internal flags
            bool    logged_in;
            bool    is_connecting = false;
            // fix here
            bool    is_sub_trade_orders = false;
            std::map<std::string,PriceIncrement> mapPriceIncrement;
            std::map<std::string,CurrencyList> mapCurrencyList;
            std::map<std::string,SubUser> mapSubUser;
            std::map<std::string,WithdrawalsList> mapWithdrawalsList;
            std::map<std::string,DepositsList> mapDepositsList;
            CoinPairWhiteList coinPairWhiteList;
            CoinPairWhiteList positionWhiteList;
            struct lws* webSocketConn;
        };

        struct ServerInfo
        {
            int nPingInterval = 0;
            int nPingTimeOut = 0;
            std::string strEndpoint ;
            std::string strProtocol ;
            bool bEncrypt = true;
        };

        

      
/**
 * kucoin trade engine
 */
        class TDEngineKuCoin: public ITDEngine
        {
        public:
            /** init internal journal writer (both raw and send) */
            virtual void init();
            /** for settleconfirm and authenticate setting */
            virtual void pre_load(const json& j_config);
            virtual TradeAccount load_account(int idx, const json& j_account);
            virtual void resize_accounts(int account_num);
            virtual void connect(long timeout_nsec);
            virtual void login(long timeout_nsec);
            virtual void logout();
            virtual void release_api();
            virtual bool is_connected() const;
            virtual bool is_logged_in() const;
            virtual string name() const { return "TDEngineKuCoin"; };

            // req functions
            virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
            virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
            virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
            virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);
            virtual void req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId);
            virtual void req_inner_transfer(const LFTransferField* data,int account_index,int requestId);
            virtual void req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw);

        public:
            TDEngineKuCoin();
            ~TDEngineKuCoin();

        private:
            
            // journal writers
            yijinjing::JournalWriterPtr raw_writer;
            vector<AccountUnitKuCoin> account_units;

            std::string GetSide(const LfDirectionType& input);
            LfDirectionType GetDirection(std::string input);
            std::string GetType(const LfOrderPriceTypeType& input);
            LfOrderPriceTypeType GetPriceType(std::string input);
            LfOrderStatusType GetOrderStatus(bool isCancel,int64_t nSize,int64_t nDealSize);
            inline int64_t getTimestamp();


            virtual void set_reader_thread() override;
            std::vector<std::string> split(std::string str, std::string token);
            void getPriceIncrement(AccountUnitKuCoin& unit);
            void dealPriceVolume(AccountUnitKuCoin& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,double& dDealPrice,double& dDealVome);
            void check_orders(AccountUnitKuCoin& unit, int64_t startTime, int64_t endTime, int64_t mflag, int64_t rflag, int64_t flag);
            bool check_single_order(AccountUnitKuCoin& unit,PendingOrderStatus& stPendingOrderStatus);
            int64_t get_order_price(string api_key, string orderId);
            std::string parseJsonToString(Document &d);

            void addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId, int64_t sentNameTime);

            void loopOrderActionNoResponseTimeOut();
        private:
            void get_account(AccountUnitKuCoin& unit, Document& json);
            void send_order(AccountUnitKuCoin& unit,PendingOrderStatus& stPendingOrderStatus,const char* code,double size,double price,bool isPostOnly,Document& json);
            void handle_order_insert(AccountUnitKuCoin& unit,const LFInputOrderField data,int requestId,const std::string& ticker);
            void handle_order_action(AccountUnitKuCoin& unit,const LFOrderActionField data, int requestId,const std::string& ticker);
            void cancel_all_orders(AccountUnitKuCoin& unit, Document& json);
            void cancel_order(AccountUnitKuCoin& unit, std::string code, std::string orderId, Document& json);
            void getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json);
            void printResponse(const Document& d);
            void withdrawl_currency(string currency, string volume, string address, string tag, Document& json, AccountUnitKuCoin& unit);
            void transfer_inner(string clientOid, string currency, string from, string to,string amount,
                                    Document& json, AccountUnitKuCoin& unit,std::string api_key,std::string secret_key,std::string passphrase);
            void req_transfer_inner(const LFTransferField* data,int account_index,int requestId,string from,string to,std::string api_key,std::string secret_key,std::string passphrase);
            void transfer_sub(string clientOid, string currency, string amount, string direction,string subAccountType,string subUserId,Document& json, AccountUnitKuCoin& unit,std::string api_key,std::string secret_key,std::string passphrase);
            void req_transfer_sub(const LFTransferField* data,int account_index,int requestId,string direction,string subAccountType,string subUserId,std::string api_key,std::string secret_key,std::string passphrase);
            void currency_list(Document& json, AccountUnitKuCoin& unit);
            void req_currencylist(AccountUnitKuCoin& unit);
            void sub_user(Document& json, AccountUnitKuCoin& unit);
            void get_sub_user(AccountUnitKuCoin& unit);
            void get_sub_accounts(string subUserId, AccountUnitKuCoin& unit, Document& json);
            void withdrawals_list(Document& json,string currency,string status,string startAt,string endAt,AccountUnitKuCoin& unit);
            void req_withdrawals_list(string currency,long startAt,long endAt,string status);
            void deposits_list(Document& json,string currency,string status,string startAt,string endAt,AccountUnitKuCoin& unit);
            void req_deposits_list(string currency,long startAt,long endAt,string status);

            bool shouldRetry(Document& d);

            int get_transfer_status(std::string status);
            std::string get_query_transfer_status(int status);

            std::string construct_request_body(const AccountUnitKuCoin& unit,const  std::string& data,bool isget = true);
            std::string createInsertOrdertring(const char *code,const char* strClientId,
                                               const char *side, const char *type, double& size, double& price,bool isPostOnly);
            std::string createInsertOrdertring_withdraw(string currency, string volume, string address, string tag);
            std::string createInsertOrdertring_inner_transfer(string clientOid,string currency, string from,string to, string amount);
            std::string createInsertOrdertring_sub_transfer(string clientOid,string currency, string amount, string direction,
                                                               string subAccountType,string subUserId);
            std::string createInsertOrdertring_sub_accounts(string subUserId);
            std::string createInsertWithdrawals_list(string currency,string status,string startAt,string endAt);
            std::string createInsertDeposits_list(string currency,string status,string startAt,string endAt);
            cpr::Response Get(const std::string& url,const std::string& body, AccountUnitKuCoin& unit);
            cpr::Response Post(const std::string& url,const std::string& body, AccountUnitKuCoin& unit,std::string api_key,std::string secret_key,std::string passphrase);
            cpr::Response Delete(const std::string& url,const std::string& body, AccountUnitKuCoin& unit);


            void genUniqueKey();
            std::string genClinetid(const std::string& orderRef);

        public:
            void writeErrorLog(std::string strError);
            void handle_lws_data(struct lws* conn,std::string data);
            void on_lws_data(struct lws* conn, const char* data, size_t len);
            void onOrderChange( Document& d);
            void onOrder(const PendingOrderStatus& stPendingOrderStatus);
            void onTrade(const PendingOrderStatus& stPendingOrderStatus,int64_t nSize,int64_t nPrice,std::string& strTradeId,std::string& strTime);
            int lws_write_subscribe(struct lws* conn);
            void on_lws_connection_error(struct lws* conn);
            bool lws_login(AccountUnitKuCoin& unit, long timeout_nsec);
        private:
            void onPong(struct lws* conn);
            void Ping(struct lws* conn);     
            std::string makeSubscribeTradeOrders();
            std::string makeSubscribeL3Update(const std::map<std::string,int>& mapAllSymbols);
            std::string makeunSubscribeL3Update(const std::map<std::string,int>& mapAllSymbols);
            bool getToken(Document& d) ;
            bool getServers(Document& d);
            std::string getId();
            int64_t getMSTime();
            void loopwebsocket();
            void dealnum(string pre_num,string& fix_num);
            AccountUnitKuCoin& findAccountUnitKucoinByWebsocketConn(struct lws * websocketConn);
        private:

            bool m_shouldPing = true; 
            bool m_isPong = false;
            //bool m_isSubL3 = false;    
            struct lws_context *context = nullptr;
            std::vector<ServerInfo> m_vstServerInfos;
            std::string m_strToken;
            struct lws* m_conn;
            
            std::mutex* m_mutexOrder = nullptr;
            std::map<std::string,PendingOrderStatus> m_mapOrder;
            std::map<std::string,PendingOrderStatus> m_mapNewOrder;
            std::map<std::string,PendingOrderStatus> p_mapOrder;

        private:    
            std::string account_type;
            std::string baseurl;        
            std::string m_uniqueKey;
            int HTTP_RESPONSE_OK = 200;
            int m_CurrentTDIndex = 0;
            static constexpr int scale_offset = 1e8;

            ThreadPtr rest_thread;
            ThreadPtr orderaction_timeout_thread;

            uint64_t last_rest_get_ts = 0;
            uint64_t rest_get_interval_ms = 500;

            std::mutex* mutex_order_and_trade = nullptr;
            std::mutex* mutex_response_order_status = nullptr;
            std::mutex* mutex_orderaction_waiting_response = nullptr;
            std::mutex* mutex_marketorder_waiting_response = nullptr;
            std::mutex* mutex_web_connect = nullptr;

            // lock raw_writer/ITDEngine::send_writer/IEngine::writer to avoid error
            // terminate called after throwing an instance of 'std::runtime_error'
            // what() : more than one writer is writing /shared/kungfu/journal/TD_SEND/KUCOIN TD_SEND_KUCOIN
            std::mutex* mutex_journal_writer = nullptr;

            std::map<std::string, std::string> localOrderRefRemoteOrderId;
            std::map<std::string,KEY> sub_map;

            //对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
            std::map<std::string, OrderActionSentTime> remoteOrderIdOrderActionSentTime;

            std::vector<ResponsedOrderStatus> responsedOrderStatusNoOrderRef;
            int max_rest_retry_times = 3;
            int retry_interval_milliseconds = 1000;
            int orderaction_max_waiting_seconds = 30;
            ThreadPool* m_ThreadPoolPtr = nullptr;
            int64_t no_response_wait_ms = 5000;

            //429  Too Many Requests -- 请求频率超出限制, IP或账户会被限制使用10s
            bool rateLimitExceeded = false;
            int64_t prohibit_order_ms = 10100;
            int64_t limitEndTime = 0;
        };

WC_NAMESPACE_END

#endif //PROJECT_TDENGINEKUCOIN_H



