
#ifndef PROJECT_TDENGINEKUMEX_H
#define PROJECT_TDENGINEKUMEX_H

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
            LFInputOrderField data;
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

        struct SequenceInfo
        {
            int64_t nSequence;
            std::map<int64_t,std::string> mapMessage;
        };
        struct PriceIncrement
        {
            int64_t nBaseMinSize = 0;
            int64_t nPriceIncrement = 0;
            int64_t nQuoteIncrement = 0;
            int64_t nBaseIncrement = 0;
        };

        struct AccountUnitKumex
        {
            string api_key;//uid
            string secret_key;
            string passphrase;
            string baseUrl;
            // internal flags
            bool    logged_in;
            bool    is_connecting = false;
            std::map<std::string,PriceIncrement> mapPriceIncrement;
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
 * CTP trade engine
 */
        class TDEngineKumex: public ITDEngine
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
            virtual string name() const { return "TDEngineKumex"; };

            // req functions
            virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
            virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
            virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
            virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);


        public:
            TDEngineKumex();
            ~TDEngineKumex();

        private:
            
            // journal writers
            yijinjing::JournalWriterPtr raw_writer;
            vector<AccountUnitKumex> account_units;

            std::string GetSide(const LfDirectionType& input);
            LfDirectionType GetDirection(std::string input);
            std::string GetType(const LfOrderPriceTypeType& input);
            LfOrderPriceTypeType GetPriceType(std::string input);
            LfOrderStatusType GetOrderStatus(bool isCancel,int64_t nSize,int64_t nDealSize);
            inline int64_t getTimestamp();


            virtual void set_reader_thread() override;
            std::vector<std::string> split(std::string str, std::string token);
            void getPriceIncrement(AccountUnitKumex& unit);
            void dealPriceVolume(AccountUnitKumex& unit,const std::string& symbol,int64_t nPrice,int64_t nVolume,double& dDealPrice,double& dDealVome);
            void check_orders(AccountUnitKumex& unit, int64_t startTime, int64_t endTime, int64_t mflag, int64_t rflag, int64_t flag);
            std::string parseJsonToString(Document &d);

            void addRemoteOrderIdOrderActionSentTime(const LFOrderActionField* data, int requestId, const std::string& remoteOrderId, int64_t sentNameTime);

            void loopOrderActionNoResponseTimeOut();
            void orderActionNoResponseTimeOut(AccountUnitKumex& unit);
            void loopMarketOrderNoResponseTimeOut();
            void MarketOrderNoResponseTimeOut(AccountUnitKumex& unit);
        private:
            void get_account(AccountUnitKumex& unit, Document& json);
            void send_order(AccountUnitKumex& unit,PendingOrderStatus& stPendingOrderStatus,const char* code,double size,double price,bool isPostOnly,Document& json);
            void handle_order_insert(AccountUnitKumex& unit,const LFInputOrderField data,int requestId,const std::string& ticker);
            void handle_order_action(AccountUnitKumex& unit,const LFOrderActionField data, int requestId,const std::string& ticker);
            void cancel_all_orders(AccountUnitKumex& unit, Document& json);
            void cancel_order(AccountUnitKumex& unit, std::string code, std::string orderId, Document& json);
            void getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json);
            void printResponse(const Document& d);

            bool shouldRetry(Document& d);

            std::string construct_request_body(const AccountUnitKumex& unit,const  std::string& data,bool isget = true);
            std::string createInsertOrdertring(const char *code,const char* strClientId,
                                               const char *side, const char *type, double& size, double& price,bool isPostOnly);

            cpr::Response Get(const std::string& url,const std::string& body, AccountUnitKumex& unit);
            cpr::Response Post(const std::string& url,const std::string& body, AccountUnitKumex& unit);
            cpr::Response Delete(const std::string& url,const std::string& body, AccountUnitKumex& unit);

            void genUniqueKey();
            std::string genClinetid(const std::string& orderRef);

        public:
            void writeErrorLog(std::string strError);
            void handle_lws_data(struct lws* conn,std::string data);
            void on_lws_data(struct lws* conn, const char* data, size_t len);
            void onOrderChange(Document& d,bool match_sequence = true);
            bool matchSequence(Document& json,std::string& data);
            std::string getNextSequence(std::string& symbol);
            void onOrder(const PendingOrderStatus& stPendingOrderStatus);
            void onTrade(const PendingOrderStatus& stPendingOrderStatus,int64_t nSize,int64_t nPrice,std::string& strTradeId,std::string& strTime);
            int lws_write_subscribe(struct lws* conn);
            void on_lws_connection_error(struct lws* conn);
            bool lws_login(AccountUnitKumex& unit, long timeout_nsec);
        private:
            void onPong(struct lws* conn);
            void Ping(struct lws* conn);       
            std::string makeSubscribeL3Update(const std::map<std::string,int>& mapAllSymbols);
            std::string makeunSubscribeL3Update(const std::map<std::string,int>& mapAllSymbols);
            bool getToken(Document& d) ;
            bool getServers(Document& d);
            std::string getId();
            int64_t getMSTime();
            void loopwebsocket();
            AccountUnitKumex& findAccountUnitKumexByWebsocketConn(struct lws * websocketConn);
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
            std::string m_uniqueKey;
            std::string leverage = "10";
            int HTTP_RESPONSE_OK = 200;
            int m_CurrentTDIndex = 0;
            static constexpr int scale_offset = 1e8;

            ThreadPtr rest_thread = nullptr;
            ThreadPtr orderaction_timeout_thread;

            uint64_t last_rest_get_ts = 0;
            uint64_t rest_get_interval_ms = 500;

            std::mutex* mutex_order_and_trade = nullptr;
            std::mutex* mutex_response_order_status = nullptr;
            std::mutex* mutex_orderaction_waiting_response = nullptr;
            std::mutex* mutex_marketorder_waiting_response = nullptr;
            std::mutex* mutex_web_connect = nullptr;

            std::map<std::string, std::string> localOrderRefRemoteOrderId;
            std::map<std::string,SequenceInfo> mapSequence;
            //对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
            std::map<std::string, OrderActionSentTime> remoteOrderIdOrderActionSentTime;

            std::vector<ResponsedOrderStatus> responsedOrderStatusNoOrderRef;
            int max_rest_retry_times = 3;
            int retry_interval_milliseconds = 1000;
            int orderaction_max_waiting_seconds = 30;
            ThreadPool* m_ThreadPoolPtr = nullptr;
            ThreadPool* m_LwsThreadPoolPtr = nullptr;
            int64_t no_response_wait_ms = 5000;
        };

WC_NAMESPACE_END

#endif //PROJECT_TDENGINEKUMEX_H



