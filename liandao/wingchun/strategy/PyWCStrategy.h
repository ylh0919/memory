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
 * PyWCStrategy: python binding version of wingchun strategy.
 * @Author cjiang (changhao.jiang@taurus.ai)
 * @since   October, 2017
 */

#ifndef WINGCHUN_PYWCSTRATEGY_H
#define WINGCHUN_PYWCSTRATEGY_H

#include "IWCStrategy.h"

WC_NAMESPACE_START

class PyWCStrategy: public IWCStrategy
{
private:
    boost::python::object py_init;
    boost::python::object py_on_switch_day;
    boost::python::object py_on_pos;
    //check it in IWCStrategy.h
    //boost::python::object py_on_error;
    map<int, boost::python::object> py_on_data;

protected:
    virtual bool td_is_ready(int source);

public:
    virtual void start();
    virtual void init();
    virtual void on_market_bar(const BarMdMap& data, int min_interval, int source, long rcv_time);
    virtual void on_market_data(const LFMarketDataField* data, int source, long rcv_time);
    virtual void on_price_book_update(const LFPriceBook20Field* data, int source, long rcv_time);
    virtual void on_funding_update(const LFFundingField* data, int source, long rcv_time);
    virtual void on_quote_requests(const LFQuoteRequestsField* data, int source, long rcv_time);
    virtual void on_priceindex(const LFPriceIndex* data, int source, long rcv_time);
    virtual void on_markprice(const LFMarkPrice* data, int source, long rcv_time);
    virtual void on_perpetual(const LFPerpetual* data, int source, long rcv_time);
    virtual void on_ticker(const LFTicker* data, int source, long rcv_time);
    virtual void on_market_bar_data(const LFBarMarketDataField* data, int source, long rcv_time);
    virtual void on_rtn_order(const LFRtnOrderField* data, int request_id, int source, long rcv_time);
    virtual void on_rtn_trade(const LFRtnTradeField* data, int request_id, int source, long rcv_time);
    virtual void on_rtn_quote(const LFRtnQuoteField* data, int request_id, int source, long rcv_time);
    virtual void on_rsp_order(const LFInputOrderField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_rsp_quote(const LFInputQuoteField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_rsp_order_action(const LFOrderActionField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_rsp_quote_action(const LFQuoteActionField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_rsp_position(const PosHandlerPtr posMap, int request_id, int source, long rcv_time);
    virtual void on_rsp_transfer_history(const LFTransferHistoryField* data,int request_id,int source,long rcv_time,bool is_last,bool is_withdraw, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_withdraw(const LFWithdrawField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_switch_day(long rcv_time);
    virtual void on_l2_trade(const LFL2TradeField* data, int source, long rcv_time);
    virtual void on_transfer(const LFTransferField* data, int request_id, int source, long rcv_time, int errorId=0, const char* errorMsg=nullptr);
    virtual void on_bar_serial1000(const LFBarSerial1000Field* data, int request_id, int source, long rcv_time);
    //py log
    virtual void log_debug(string msg) { KF_LOG_DEBUG(logger, msg); };
    virtual void log_info(string msg) { KF_LOG_INFO(logger, msg); };
    virtual void log_error(string msg) { KF_LOG_ERROR(logger, msg); };
    virtual void log_fatal(string msg) { KF_LOG_FATAL(logger, msg); };
    //set py function
    void set_init(boost::python::object func);
    void set_on_data(int msg_type, boost::python::object func);
    void set_on_pos(boost::python::object func);
    void set_on_error(boost::python::object func);
    void set_on_switch_day(boost::python::object func);

    WCStrategyUtilPtr get_strategy_util() const { return util; }
    WCDataWrapperPtr  get_data_wrapper()  const { return data; }
    /** get effective orders */
    boost::python::list get_effective_orders() const;

public:
    // python bcancel_orderd, use string and transfer manually.
    inline int insert_market_order_py(int source, string instrument_id, string exchange_id, uint64_t volume, string direction, string offset,string misc_info = "",int64_t expect_price = 0)
    {
        return insert_market_order(source, instrument_id, exchange_id, volume, direction[0], offset[0],misc_info,expect_price);
    }
    inline int insert_limit_order_py(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, string direction, string offset,string misc_info = "")
    {
        return insert_limit_order(source, instrument_id, exchange_id, price, volume, direction[0], offset[0],misc_info);
    }
    inline int insert_quote_request_py(int source, string instrument_id,string expiry,string exchange_id, uint64_t volume, string direction, int64_t price = 0, bool is_hide_limit_price = true,string misc_info = "")
    {
        return insert_quote_request(source, instrument_id, expiry,exchange_id,  volume, direction[0], price,is_hide_limit_price,misc_info);
    }
    inline int insert_fok_order_py(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, string direction, string offset,string misc_info = "")
    {
        return insert_fok_order(source, instrument_id, exchange_id, price, volume, direction[0], offset[0],misc_info);
    }
    inline int insert_fak_order_py(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, string direction, string offset,string misc_info = "")
    {
        return insert_fak_order(source, instrument_id, exchange_id, price, volume, direction[0], offset[0],misc_info);
    }
    inline int cancel_order_py(int source, int order_id,string misc_info = "")
    {
        return cancel_order(source, order_id,misc_info);
    }
    /*
    inline int batch_cancel_order_py(int source, vector<int> order_id_vec, vector<string> misc_info_vec)
    {
        int maxSize = order_id_vec.size() < misc_info_vec.size() ? order_id_vec.size() : misc_info_vec.size();
        return batch_cancel_order(source, order_id_vec, misc_info_vec);
    }
    */
    inline int batch_cancel_order_py(int source, boost::python::list order_id_list, boost::python::list misc_info_list)
    {
        if (len(order_id_list) != len(misc_info_list) && len(misc_info_list) != 0)
            return -1;
        //int maxSize = len(order_id_list) < len(misc_info_list) ? len(order_id_list) : len(misc_info_list);
        int maxSize = len(order_id_list);
        vector<int> order_id_vec;
        vector<string> misc_info_vec;

        int res;
        for (int i = 0; i < maxSize; ++i)
        {
            order_id_vec.push_back(boost::python::extract<int>(order_id_list[i]));
            if(len(misc_info_list) != 0)
                misc_info_vec.push_back(boost::python::extract<string>(misc_info_list[i]));
            else
                misc_info_vec.push_back("");
            if (order_id_vec.size() == 5 && misc_info_vec.size() == 5) {
                res = batch_cancel_order(source, order_id_vec, misc_info_vec);
                order_id_vec.clear();
                misc_info_vec.clear();
            }

            if (i == maxSize - 1) {
                res = batch_cancel_order(source, order_id_vec, misc_info_vec);
                order_id_vec.clear();
                misc_info_vec.clear();
            }
        }
        return res;
    }
    inline int withdraw_currency_py(int source, string currency, int64_t volume, string address, string tag){
        return withdraw_currency(source, currency,volume,address,tag);
    }
    inline int write_errormsg_py(int source, int errorId, string errorMsg){
        return write_errormsg(source, errorId, errorMsg);
    }
public:
    PyWCStrategy(const string& name): IWCStrategy(name) {}
    PyWCStrategy(): IWCStrategy("Default") {}
};

DECLARE_PTR(PyWCStrategy);

WC_NAMESPACE_END

#endif //WINGCHUN_PYWCSTRATEGY_H
