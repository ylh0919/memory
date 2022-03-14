/*****************************************************************************
 * Copyright [2017] [taurus.ai]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *****************************************************************************/

/**
 * IWCStrategy: base class of wingchun strategy.
 * @Author cjiang (changhao.jiang@taurus.ai)
 * @since   September, 2017
 */

#ifndef WINGCHUN_IWCSTRATEGY_H
#define WINGCHUN_IWCSTRATEGY_H

#include "WC_DECLARE.h"
#include "WCStrategyUtil.h"
#include "WCDataWrapper.h"
#include "KfLog.h"
#include "monitor_api/MonitorClient.h"
USING_MONITOR_NAMESPACE

WC_NAMESPACE_START
using yijinjing::KfLogPtr;
class IWCStrategy: public IWCDataProcessor, MonitorClientSpi
{
protected:
    //don't use it in C++ strategy
    boost::python::object py_on_error;
    virtual bool td_is_ready(int source);
    bool td_is_connected(int source) const;
public:
    /** IWCDataProcessor functions *//* market data */
    virtual void on_market_data(const LFMarketDataField* data, int source, long rcv_time);
    virtual void on_market_bar_data(const LFBarMarketDataField* data, int source, long rcv_time);
    virtual void on_price_book_update(const LFPriceBook20Field* data, int source, long rcv_time);
    virtual void on_funding_update(const LFFundingField* data, int source, long rcv_time);
    virtual void on_quote_requests(const LFQuoteRequestsField* data, int source, long rcv_time);
    virtual void on_market_data_level2(const LFL2MarketDataField* data, int source, long rcv_time);
    virtual void on_l2_index(const LFL2IndexField* data, int source, long rcv_time);
    virtual void on_l2_order(const LFL2OrderField* data, int source, long rcv_time);
    virtual void on_l2_trade(const LFL2TradeField* data, int source, long rcv_time);
    virtual void on_market_bar(const BarMdMap& data, int min_interval, int source, long rcv_time);
    virtual void on_priceindex(const LFPriceIndex* data, int source, long rcv_time);
    virtual void on_markprice(const LFMarkPrice* data, int source, long rcv_time);
    virtual void on_perpetual(const LFPerpetual* data, int source, long rcv_time);   
    virtual void on_ticker(const LFTicker* data, int source, long rcv_time);
    virtual void on_trends_data(const GoogleTrendsData* data, int source, long rcv_time);
    /* trade data */
    virtual void on_rtn_order(const LFRtnOrderField* data, int request_id, int source, long rcv_time);
    virtual void on_rtn_trade(const LFRtnTradeField* data, int request_id, int source, long rcv_time);
    virtual void on_rtn_quote(const LFRtnQuoteField* data, int request_id, int source, long rcv_time);
    virtual void on_rsp_order(const LFInputOrderField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_rsp_quote(const LFInputQuoteField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_rsp_order_action(const LFOrderActionField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_rsp_quote_action(const LFQuoteActionField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_rsp_position(const PosHandlerPtr posMap, int request_id, int source, long rcv_time);
    virtual void on_withdraw(const LFWithdrawField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_transfer(const LFTransferField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_rsp_transfer_history(const LFTransferHistoryField* data,int request_id,int source,long rcv_time,bool is_last,bool is_withdraw, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_bar_serial1000(const LFBarSerial1000Field* data,int request_id,int source,long rcv_time);
    /* system utilities */
    virtual void on_switch_day(long rcv_time);
    virtual void on_time(long cur_time);
    virtual void on_td_login(bool ready, const json& js, int source);
    /* on log */
    virtual void debug(const char* msg) { KF_LOG_DEBUG(logger, msg); };
    /* get name */
    virtual string get_name() const { return name; };

    /**
     * setup data wrapper
     * by calling add_* functions in WCDataWrapper
     * also subscribe tickers
     */
    virtual void init() = 0;
    virtual void set_ws_url(const string& ws_url){m_wsUrl = ws_url;};
public: // util functions, wrap upon WCStrategyUtil
    int write_errormsg(int source, int errorId, string errorMsg);
    /** insert order, check status before calling WCStrategyUtil */
    int insert_market_order(int source, string instrument_id, string exchange_id, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info = "",int64_t expecct_price = 0);
    int insert_limit_order(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info = "");
    int insert_fok_order(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info = "");
    int insert_fak_order(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info = "");
    int req_position(int source,string account_type="",string account_name="");
    int cancel_order(int source, int order_id,string misc_info = "");
    int batch_cancel_order(int source, vector<int> order_id_list, vector<string> misc_info_list);
    int req_get_kline_via_rest(int source, string instrument_id, string Interval = "15m", int Limit = 1000, bool IgnoreStartTime = false, int64_t StartTime = 0, int64_t EndTime = 0,string misc_info = "");
    int withdraw_currency(int source, string currency,int64_t volume,string address,string tag);
    int req_inner_transfer(int source,string currency,int64_t volume,string from_type,string to_type,string from_name = "",string to_name="",string ticker="");
    int transfer_history(int source, bool is_withdraw, string currency, int status, string start_Time, string end_Time, string from_id);     //false for deposit , true for withdraw
    int insert_quote_request(int source, string instrument_id,string expiry,string exchange_id, uint64_t volume, LfDirectionType direction, int64_t price = 0, bool is_hide_limit_price = true,string misc_info = "");
    int cancel_quote_request(int source, int quote_request_id,string misc_info = "");
    int insert_quote(int source, string instrument_id,int quote_request_id, int64_t price,string misc_info = "");
    int cancel_quote(int source, int quote_id,string misc_info = "");
    int accept_quote(int source, int quote_id,string misc_info = "");
public:
    /** default destructor */
    ~IWCStrategy();
    /** default constructor */
    IWCStrategy(const string& name);
    /* start data thread */
    virtual void start();
    /* run strategy in front end */
    void run();
    bool is_running();
    /* terminate data thread, should never be called within data thread */
    void terminate();
    /* stop send stop signal to data thread */
    void stop();
    /* block process by data thread */
    void block();

private:
    bool connectMonitor(const std::string& url, const std::string& name, const std::string &type);

protected:
    virtual void OnMessage(const std::string& ){ };

protected:
    /** logger, will be improved later */
    KfLogPtr logger;
    /** strategy name, not modifiable */
    const string name;
    /** strategy utils */
    WCStrategyUtilPtr util;
    /** data wrapper */
    WCDataWrapperPtr data;
    /** data thread */
    ThreadPtr data_thread;
    string m_wsUrl = "127.0.0.1:45678";
    MonitorClientPtr m_monitorClient;
};

WC_NAMESPACE_END

#endif //WINGCHUN_WCSTRATEGY_H

