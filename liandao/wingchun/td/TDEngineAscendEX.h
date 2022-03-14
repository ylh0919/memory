#ifndef KUNGFU_TDENGINEASCENDEX_H
#define KUNGFU_TDENGINEASCENDEX_H

#include "ITDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include "InterfaceMgr.h"
#include <vector>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <map>
#include <queue>
#include <mutex>
#include "Timer.h"
#include <document.h>
#include <queue>
#include <libwebsockets.h>
#include <atomic>
#include <string>
#include <cpr/cpr.h>
#include <sched.h>
#include "InterfaceMgr.h"
#include "../base/ThreadPool.h"

using rapidjson::Document;

WC_NAMESPACE_START


struct SendOrderFilter
{
	char_31 InstrumentID;   //合约代码
	int ticksize;           //for price round.
							//...other
};

struct AsNewOrderStatus
{
	string OrderRef;
	// 记录下单动作的时间，如果在在newOrderMap中，当前时间-last_action_time>rest_wait_time_ms，就需要在check_order_loop线程中检查
	int64_t last_action_time = 0;

	// 记录下单的request_id和数据LFOrderActionField，报错用
	int requestId;
	LFInputOrderField data;
};

struct AsOrderStatus
{
	string OrderRef;
	// 记录最近一次on_rtn_order返回的数据
	LFRtnOrderField last_rtn_order;

	// 是否有撤单动作，有撤单动作为true，没有撤单动作为false。这条为false时，后面3条都视为无效数据
	bool order_canceled = false;
	// 记录上一次撤单动作的时间，如果在rtnOrderMap中且cancel_order为真，且当前时间-last_action_time>rest_wait_time_ms，就需要在check_order_loop线程中检查
	int64_t last_action_time = 0;
	// 记录撤单的request_id和数据LFOrderActionField，报错用
	int requestId = -1;
	LFOrderActionField data;
};

struct AccountUnitAscendEX
{
	string api_key;
	string secret_key;
	string baseUrl;
	//only for ascendex
	int	   accountGroup = 0;
	// internal flags
	bool    logged_in;
	bool    is_connecting = false;
	//ws flag
	bool    ws_auth = false;
	bool    ws_sub_order = false;

	std::map<std::string, SendOrderFilter> sendOrderFilters;
	//first: OrderRef, second: id sent to exchange
	std::map<std::string, std::string> orderRefMap;
	//first: orderId from exchange, second: PendingOrderStatus
	std::map<std::string, AsNewOrderStatus> newOrderMap;
	//first: orderId/remoteOrderId from exchange, second: PendingOrderStatus
	std::map<std::string, AsOrderStatus> rtnOrderMap;

	//lock newOrderMap
	std::mutex* mutex_new_order = nullptr;
	//lock unit.rtnOrderMap
	std::mutex* mutex_rtn_order = nullptr;
	//lock orderRefMap
	std::mutex* mutex_orderref_map = nullptr;

	CoinPairWhiteList coinPairWhiteList;
	CoinPairWhiteList positionWhiteList;

	struct lws_context* context = nullptr;
	struct lws* websocketConn;

	std::queue<string> sendmessage;//websocket

	//constructor and destructor
	AccountUnitAscendEX();
	AccountUnitAscendEX(const AccountUnitAscendEX& source);
	~AccountUnitAscendEX();
};

class TDEngineAscendEX : public ITDEngine
{
public:
	TDEngineAscendEX();
	~TDEngineAscendEX();

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
	virtual string name() const { return "TDEngineAscendEX"; };

public:
	// req functions
	virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
	virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
	virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
	virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);

	//virtual void req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId);
	virtual void req_inner_transfer(const LFTransferField* data, int account_index, int requestId);
	virtual void req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw);  //false for deposit , true for withdraw

	//create a thread for req
	void send_order_thread(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
	void action_order_thread(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);

private:
	//restful api
	int HTTP_RESPONSE_OK = 200;

	bool set_account_info(AccountUnitAscendEX& unit);
	void get_account_info(AccountUnitAscendEX& unit, Document& json);
	void get_account(AccountUnitAscendEX& unit, Document& json);
	void get_products(AccountUnitAscendEX& unit, Document& json);
	void send_order(AccountUnitAscendEX& unit, const char* code, const char* id, const char* side, const char* type, double size, double price, Document& json);
	void cancel_order(AccountUnitAscendEX& unit, const char* code, const char* remoteOrderId, Document& json);
	void cancel_all_orders(AccountUnitAscendEX& unit, Document& json, std::string symbol = "");
	void query_orders(AccountUnitAscendEX& unit, Document& json);
	void query_order(AccountUnitAscendEX& unit, std::string remoteOrderId, Document& json);
	cpr::Response balance_transfer(AccountUnitAscendEX& unit, std::string amount, std::string asset, 
		std::string fromAccount, std::string toAccount, Document& json);
	cpr::Response balance_transfer_for_subaccount(AccountUnitAscendEX& unit, std::string amount, std::string asset,
		std::string acFrom, std::string acTo, std::string userFrom, std::string userTo, Document& json);
	cpr::Response query_wallet_transaction_history(AccountUnitAscendEX& unit, std::string asset, std::string txType,
		int page, int pageSize, Document& json);

	void printResponse(const Document& d);
	void getResponse(int http_status_code, std::string responseText, std::string errorMsg, Document& json);
	bool shouldRetry(cpr::Response response);

private:
	//websocket: connect, auth and sub
	int subscribeTopic(struct lws* conn, string strSubscribe);
	std::string makeSubscribeOrdersUpdate(string account = "", bool margin = false);
	void lws_auth(AccountUnitAscendEX& unit);
	void lws_pong(struct lws* conn);
	bool lws_login(AccountUnitAscendEX& unit, long timeout_nsec);

public:
	//ws_service_cb回调函数
	void on_lws_open(struct lws* wsi);
	void on_lws_data(struct lws* conn, const char* data, size_t len);
	void on_lws_connection_error(struct lws* conn);
	void on_lws_close(struct lws* conn);
	int on_lws_write_subscribe(struct lws* conn);

private:
	//on_lws_data
	void onAuthMsg(struct lws* conn, const rapidjson::Document& json);
	void onSubMsg(struct lws* conn, const rapidjson::Document& json);
	void onErrorMsg(struct lws* conn, const rapidjson::Document& json);
	void onOrder(struct lws* conn, const rapidjson::Document& json);
	void onBalance(struct lws* conn, const rapidjson::Document& json);

private:
	AccountUnitAscendEX& findAccountUnitByWebsocketConn(struct lws* websocketConn);
	int findAccountUnitIndexByWebsocketConn(struct lws* websocketConn);
	void dealnum(string pre_num, string& fix_num);
	string splitAccountName(string dataName);
	int64_t fixPriceTickSize(int keepPrecision, int64_t price, bool isBuy);
	bool loadExchangeOrderFilters(AccountUnitAscendEX& unit, Document& doc);
	void debug_print(std::map<std::string, SendOrderFilter>& sendOrderFilters);
	SendOrderFilter getSendOrderFilter(AccountUnitAscendEX& unit, const char* symbol);

	inline std::string getTimestampString();
	inline int64_t getTimestamp();

private:
	// journal writers
	yijinjing::JournalWriterPtr raw_writer;
	vector<AccountUnitAscendEX> account_units;

	std::string GetSide(const LfDirectionType& input);
	LfDirectionType GetDirection(std::string input);
	std::string GetType(const LfOrderPriceTypeType& input);
	LfOrderPriceTypeType GetPriceType(std::string input);
	LfOrderStatusType GetOrderStatus(std::string input);
	int GetTransferStatus(std::string input);
	//LfTimeConditionType GetTimeCondition(std::string input);

	//set thread
	virtual void set_reader_thread() override;
	void loop();
	void check_order_loop();
	//检查newOrderMap
	void check_insert_orders(AccountUnitAscendEX& unit);
	void check_canceled_order(AccountUnitAscendEX& unit);

	//get orderId by orderRefMap
	string GetOrderRefViaOrderId(AccountUnitAscendEX& unit, string orderId);
	string generateId(string orderRef);

private:
	std::string restBaseUrl = "https://ascendex.com";
	std::string wsBaseUrl = "ascendex.com";

	static constexpr int scale_offset = 1e8;

	ThreadPtr rest_thread;
	ThreadPtr orderaction_timeout_thread;
	ThreadPool* m_ThreadPoolPtr = nullptr;

	uint64_t last_rest_get_ts = 0;
	int rest_get_interval_ms = 500;

	bool isMargin = false;

	//下单指令发出2000ms内，如果ws没有传过来数据，则主动查询订单状态
	int rest_wait_time_ms = 2000;

	//对于每个撤单指令发出后30秒（可配置）内，如果没有收到回报，就给策略报错（撤单被拒绝，pls retry)
	int max_rest_retry_times = 3;
	int retry_interval_milliseconds = 1000;
};

WC_NAMESPACE_END

#endif //KUNGFU_TDENGINEASENDEX_H