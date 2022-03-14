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
 * PyWCStrategy: utility functions for strategy.
 * @Author cjiang (changhao.jiang@taurus.ai)
 * @since   October, 2017
 */

#include "PyWCStrategy.h"
#include "TypeConvert.hpp"
#include <csignal>
#include <iostream>
#include <chrono>

USING_WC_NAMESPACE

namespace bp = boost::python;

bool PyWCStrategy::td_is_ready(int source)
{
    if (data.get() != nullptr)
    {
        byte status = data->get_td_status(source);
        if (status == CONNECT_TD_STATUS_ACK_POS
            || status == CONNECT_TD_STATUS_SET_BACK)
            return true;
    }
    if (py_on_error != bp::object())
    {
        string error_msg = "td is not ready";
        int errorId = 200;
        int64_t rcv_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        int request_id = -1;

        START_PYTHON_FUNC_CALLING
            py_on_error(errorId, error_msg, request_id, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }

    return false;
}

void PyWCStrategy::init()
{
    if (py_init != bp::object())
    {
        PyGILState_STATE gstate = PyGILState_Ensure();
        py_init();
        PyGILState_Release(gstate);
    }
}

void PyWCStrategy::start()
{
    Py_Initialize();
    if (!PyEval_ThreadsInitialized())
        PyEval_InitThreads();
    std::signal(SIGTERM, IWCDataProcessor::signal_handler);
    std::signal(SIGINT, IWCDataProcessor::signal_handler);
    //data_thread = ThreadPtr(new std::thread(&WCDataWrapper::run, data.get()));
    //KF_LOG_INFO(logger, "[start] data started");
    IWCStrategy::start();
}

void PyWCStrategy::on_market_data(const LFMarketDataField* data, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_MD];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}
void PyWCStrategy::on_l2_trade(const LFL2TradeField* data, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_L2_TRADE];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}
void PyWCStrategy::on_price_book_update(const LFPriceBook20Field* data, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_PRICE_BOOK_20];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}

void PyWCStrategy::on_funding_update(const LFFundingField* data, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_FUNDING];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}

void PyWCStrategy::on_quote_requests(const LFQuoteRequestsField* data, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_QUOTE_REQUESTS];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}
void PyWCStrategy::on_priceindex(const LFPriceIndex* data, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_PRICE_INDEX];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}
void PyWCStrategy::on_markprice(const LFMarkPrice* data, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_MARKPRICE];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}
void PyWCStrategy::on_perpetual(const LFPerpetual* data, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_PERPETUAL];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}
void PyWCStrategy::on_ticker(const LFTicker* data, int source, long rcv_time)
{
    //KF_LOG_INFO(logger, "[on_ticker] on_ticker started");
    bp::object& obj = py_on_data[MSG_TYPE_LF_TICKER];
    //std::cout << " func " << &obj << " type " << MSG_TYPE_LF_TICKER << std::endl;
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        //KF_LOG_INFO(logger, "[on_ticker] on_ticker in");
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}
void PyWCStrategy::on_withdraw(const LFWithdrawField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{ 
    //KF_LOG_INFO(logger, "[on_withdraw] on_withdraw started");
    if (errorId != 0)
    {
        if (py_on_error != bp::object() && IWCDataProcessor::signal_received <= 0)
        {
            START_PYTHON_FUNC_CALLING
            string error_msg = errorMsg;
            py_on_error(errorId, error_msg, request_id, source, rcv_time);
            END_PYTHON_FUNC_CALLING
        }
    }
    else
    {
        bp::object& obj = py_on_data[MSG_TYPE_LF_WITHDRAW];
        if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
        {
            START_PYTHON_FUNC_CALLING
            obj((uintptr_t)data, request_id, source, rcv_time);
            END_PYTHON_FUNC_CALLING
        }
    }
}

void PyWCStrategy::on_transfer(const LFTransferField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    if (errorId != 0)
    {
        if (py_on_error != bp::object() && IWCDataProcessor::signal_received <= 0)
        {
            START_PYTHON_FUNC_CALLING
            string error_msg = errorMsg;
            py_on_error(errorId, error_msg, request_id, source, rcv_time);
            END_PYTHON_FUNC_CALLING
        }
    }
    else
    {
        bp::object& obj = py_on_data[MSG_TYPE_LF_INNER_TRANSFER];
        if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
        {
            START_PYTHON_FUNC_CALLING
            obj((uintptr_t)data, request_id, source, rcv_time);
            END_PYTHON_FUNC_CALLING
        }
    }
}

void PyWCStrategy::on_market_bar_data(const LFBarMarketDataField* data, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_BAR_MD];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}

void PyWCStrategy::on_market_bar(const BarMdMap& data, int min_interval, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_BAR_MD];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        bp::dict bar_d;
        for (auto &iter: data)
        {
            uintptr_t bar_a = (uintptr_t) (&iter.second);
            bar_d[iter.first] = bar_a;
        }
        obj(bar_d, min_interval, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}

void PyWCStrategy::on_rtn_order(const LFRtnOrderField* data, int request_id, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_RTN_ORDER];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, request_id, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}

void PyWCStrategy::on_rtn_quote(const LFRtnQuoteField* data, int request_id, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_RTN_QUOTE];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, request_id, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}
void PyWCStrategy::on_rtn_trade(const LFRtnTradeField* data, int request_id, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_RTN_TRADE];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        obj((uintptr_t)data, request_id, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}

void PyWCStrategy::on_rsp_order(const LFInputOrderField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    if (errorId != 0)
    {
        if (py_on_error != bp::object() && IWCDataProcessor::signal_received <= 0)
        {
            START_PYTHON_FUNC_CALLING
            string error_msg = errorMsg;
            py_on_error(errorId, error_msg, request_id, source, rcv_time);
            END_PYTHON_FUNC_CALLING
        }
    }
}
void PyWCStrategy::on_rsp_quote(const LFInputQuoteField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    if (errorId != 0)
    {
        if (py_on_error != bp::object() && IWCDataProcessor::signal_received <= 0)
        {
            START_PYTHON_FUNC_CALLING
            string error_msg = errorMsg;
            py_on_error(errorId, error_msg, request_id, source, rcv_time);
            END_PYTHON_FUNC_CALLING
        }
    }
}
void PyWCStrategy::on_rsp_order_action(const LFOrderActionField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    if (errorId != 0)
    {
        if (py_on_error != bp::object() && IWCDataProcessor::signal_received <= 0)
        {
            START_PYTHON_FUNC_CALLING
            string error_msg = errorMsg;
            py_on_error(errorId, error_msg, request_id, source, rcv_time);
            END_PYTHON_FUNC_CALLING
        }
    }
}

void PyWCStrategy::on_rsp_quote_action(const LFQuoteActionField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    if (errorId != 0)
    {
        if (py_on_error != bp::object() && IWCDataProcessor::signal_received <= 0)
        {
            START_PYTHON_FUNC_CALLING
            string error_msg = errorMsg;
            py_on_error(errorId, error_msg, request_id, source, rcv_time);
            END_PYTHON_FUNC_CALLING
        }
    }
}

void PyWCStrategy::on_rsp_position(const PosHandlerPtr posMap, int request_id, int source, long rcv_time)
{
    if (py_on_pos != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        py_on_pos(posMap, request_id, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}

void PyWCStrategy::on_rsp_transfer_history(const LFTransferHistoryField* data,int request_id,int source,long rcv_time,bool is_last,bool is_withdraw,int errorId, const char* errorMsg)
{
    if (errorId != 0)
    {
        if (py_on_error != bp::object() && IWCDataProcessor::signal_received <= 0)
        {
            START_PYTHON_FUNC_CALLING
            string error_msg = errorMsg;
            py_on_error(errorId, error_msg, request_id, source, rcv_time);
            END_PYTHON_FUNC_CALLING
        }
    }
    else
    {   
        bp::object& obj = py_on_data[MSG_TYPE_LF_TRANSFER_HISTORY];
        if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
        {
            START_PYTHON_FUNC_CALLING
            obj((uintptr_t)data,request_id, source, rcv_time, is_last,is_withdraw);
            END_PYTHON_FUNC_CALLING
        }
    }
}

void PyWCStrategy::on_switch_day(long rcv_time)
{
    if (py_on_switch_day != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
        py_on_switch_day(rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}

void PyWCStrategy::on_bar_serial1000(const LFBarSerial1000Field* data, int request_id, int source, long rcv_time)
{
    bp::object& obj = py_on_data[MSG_TYPE_LF_BAR_SERIAL1000];
    if (obj != bp::object() && IWCDataProcessor::signal_received <= 0)
    {
        START_PYTHON_FUNC_CALLING
            obj((uintptr_t)data, request_id, source, rcv_time);
        END_PYTHON_FUNC_CALLING
    }
}

boost::python::list PyWCStrategy::get_effective_orders() const
{
    return kungfu::yijinjing::std_vector_to_py_list<int>(data->get_effective_orders());
}

void PyWCStrategy::set_init(bp::object func)
{
    py_init = func;
}

void PyWCStrategy::set_on_data(int msg_type, bp::object func)
{
    //std::cout << " type " << msg_type << " func " << &func << std::endl;
    KF_LOG_INFO(logger,"(msg_type)"<<msg_type );
    py_on_data[msg_type] = func;
}

void PyWCStrategy::set_on_pos(bp::object func)
{
    py_on_pos = func;
}

void PyWCStrategy::set_on_error(bp::object func)
{
    py_on_error = func;
}

void PyWCStrategy::set_on_switch_day(bp::object func)
{
    py_on_switch_day = func;
}

PosHandlerPtr create_empty_pos(int source)
{
    return PosHandler::create(source);
}

PosHandlerPtr create_msg_pos(int source, string pos_str)
{
    return PosHandler::create(source, pos_str);
}

BOOST_PYTHON_MODULE(libwingchunstrategy)
{
    bp::class_<WCDataWrapper, WCDataWrapperPtr>("DataWrapper", bp::no_init)
    .def("stop", &WCDataWrapper::stop)
    .def("add_market_data", &WCDataWrapper::add_market_data, (bp::arg("source")))
    .def("add_trade_engine", &WCDataWrapper::add_register_td, (bp::arg("source")))
    .def("set_pos", &WCDataWrapper::set_pos, (bp::arg("pos_handler"), bp::arg("source")))
    .def("get_pos", &WCDataWrapper::get_pos, (bp::arg("source")))
    .def("get_ticker_pnl", &WCDataWrapper::get_ticker_pnl, (bp::arg("source"), bp::arg("ticker"), bp::arg("include_fee")=false))
    .def("register_bar_md", &WCDataWrapper::register_bar_md, (bp::arg("source"), bp::arg("min_interval"), bp::arg("start_time"), bp::arg("end_time")))
    .def("set_time_range", &WCDataWrapper::set_time_range, (bp::arg("startnano"), bp::arg("endnano")));

    bp::class_<PyWCStrategy, PyWCStrategyPtr>("Strategy")
    .def(bp::init<string>(bp::arg("name")))
    .def("get_strategy_util", &PyWCStrategy::get_strategy_util)
    .def("get_data_wrapper", &PyWCStrategy::get_data_wrapper)
    .def("get_name", &IWCStrategy::get_name)
    .def("get_effective_orders", &PyWCStrategy::get_effective_orders)
    .def("log_debug", &PyWCStrategy::log_debug, (bp::arg("msg")))
    .def("log_info", &PyWCStrategy::log_info, (bp::arg("msg")))
    .def("log_error", &PyWCStrategy::log_error, (bp::arg("msg")))
    .def("log_fatal", &PyWCStrategy::log_fatal, (bp::arg("msg")))
    .def("init", &PyWCStrategy::init)
    .def("run", &PyWCStrategy::run)
    .def("is_running", &PyWCStrategy::is_running)
    .def("start", &PyWCStrategy::start)
    .def("block", &PyWCStrategy::block)
    .def("set_init", &PyWCStrategy::set_init, (bp::arg("func")))
    .def("set_on_data", &PyWCStrategy::set_on_data, (bp::arg("msg_type"), bp::arg("func")))
    .def("set_on_pos", &PyWCStrategy::set_on_pos, (bp::arg("func")))
    .def("set_on_switch_day", &PyWCStrategy::set_on_switch_day, (bp::arg("func")))
    .def("insert_market_order", &PyWCStrategy::insert_market_order_py, (bp::arg("source"), bp::arg("ticker"), bp::arg("exchange_id"), bp::arg("volume"), bp::arg("direction"), bp::arg("offset"), bp::arg("misc_info")="", bp::arg("expect_price")=0))
    .def("insert_limit_order", &PyWCStrategy::insert_limit_order_py, (bp::arg("source"), bp::arg("ticker"), bp::arg("exchange_id"), bp::arg("price"), bp::arg("volume"), bp::arg("direction"), bp::arg("offset"), bp::arg("misc_info")=""))
    .def("insert_fok_order", &PyWCStrategy::insert_fok_order_py, (bp::arg("source"), bp::arg("ticker"), bp::arg("exchange_id"), bp::arg("price"), bp::arg("volume"), bp::arg("direction"), bp::arg("offset"), bp::arg("misc_info")=""))
    .def("insert_fak_order", &PyWCStrategy::insert_fak_order_py, (bp::arg("source"), bp::arg("ticker"), bp::arg("exchange_id"), bp::arg("price"), bp::arg("volume"), bp::arg("direction"), bp::arg("offset"), bp::arg("misc_info")=""))
    .def("insert_quote_request", &PyWCStrategy::insert_quote_request_py, (bp::arg("source"), bp::arg("ticker"), bp::arg("expiry"), bp::arg("exchange_id"), bp::arg("volume"), bp::arg("direction"), bp::arg("price") = 0,bp::arg("is_hide_limit_price") = true, bp::arg("misc_info")=""))
    .def("req_position", &PyWCStrategy::req_position, (bp::arg("source"), bp::arg("account_type")="",bp::arg("account_name")=""))
    .def("cancel_order", &PyWCStrategy::cancel_order_py, (bp::arg("source"), bp::arg("order_id"),bp::arg("misc_info")=""))
    .def("batch_cancel_order", &PyWCStrategy::batch_cancel_order_py, (bp::arg("source"), bp::arg("order_id_list"),bp::arg("misc_info_list")))
    .def("cancel_quote_request", &PyWCStrategy::cancel_quote_request, (bp::arg("source"), bp::arg("quote_request_id"),bp::arg("misc_info")=""))
    .def("set_on_error", &PyWCStrategy::set_on_error, (bp::arg("func")))
    .def("write_errormsg", &PyWCStrategy::write_errormsg_py, (bp::arg("source"),bp::arg("errorId"), bp::arg("errorMsg")))
    .def("withdraw_currency", &PyWCStrategy::withdraw_currency, (bp::arg("source"), bp::arg("currency"), bp::arg("volume"), bp::arg("address"),bp::arg("tag")=""))
    .def("req_inner_transfer", &PyWCStrategy::req_inner_transfer, (bp::arg("source"), bp::arg("currency"), bp::arg("volume"), bp::arg("from_type"),bp::arg("to_type"),bp::arg("from_name")="",bp::arg("to_name")="",bp::arg("ticker")=""))
    .def("transfer_history", &PyWCStrategy::transfer_history, (bp::arg("source"), bp::arg("flag"), bp::arg("currency"), bp::arg("status"), bp::arg("start_Time"), bp::arg("end_Time"), bp::arg("from_id")))
   .def("req_get_kline_via_rest", &PyWCStrategy::req_get_kline_via_rest, (bp::arg("source"), bp::arg("ticker"), bp::arg("Interval")="15m", bp::arg("Limit") = 1000, bp::arg("IgnoreStartTime")=false, bp::arg("start_Time")=0, bp::arg("end_Time")=0, bp::arg("misc_info")=""))
    .def("set_ws_url", &PyWCStrategy::set_ws_url, (bp::arg("ws_url")))
    .def("insert_quote", &PyWCStrategy::insert_quote, (bp::arg("source"), bp::arg("ticker"), bp::arg("quote_request_id"), bp::arg("price"), bp::arg("misc_info")=""))
    .def("cancel_quote", &PyWCStrategy::cancel_quote, (bp::arg("source"), bp::arg("quote_id"), bp::arg("misc_info")=""))
    .def("accept_quote", &PyWCStrategy::accept_quote, (bp::arg("source"), bp::arg("quote_id"), bp::arg("misc_info")=""));

    bp::class_<WCStrategyUtil, WCStrategyUtilPtr>("Util", bp::no_init)
    .def("subscribe_market_data", &WCStrategyUtil::subscribe_market_data, (bp::arg("tickers"), bp::arg("source")))
    .def("insert_callback", &WCStrategyUtil::insert_callback_py, (bp::arg("nano"), bp::arg("func")))
    .def("get_nano", &WCStrategyUtil::get_nano)
    .def("get_time", &WCStrategyUtil::get_time)
    .def("parse_time", &WCStrategyUtil::parse_time, (bp::arg("time_str")))
    .def("parse_nano", &WCStrategyUtil::parse_nano, (bp::arg("nano_time")))
    .def("gen_md_trigger_tag",&WCStrategyUtil::gen_md_trigger_tag,(bp::arg("time"),bp::arg("source"),bp::arg("is_hedge") = false,bp::arg("is_post_only") = false))
    .def("gen_trade_trigger_tag",&WCStrategyUtil::gen_trade_trigger_tag,(bp::arg("time"),bp::arg("source"),bp::arg("is_hedge") = false,bp::arg("is_post_only") = false))
    .def("gen_cancel_trigger_tag",&WCStrategyUtil::gen_cancel_trigger_tag,(bp::arg("time"),bp::arg("source"),bp::arg("order_ref"),bp::arg("request_id"),bp::arg("is_hedge") = false,bp::arg("is_post_only") = false))
    .def("gen_timeout_trigger_tag",&WCStrategyUtil::gen_timeout_trigger_tag,(bp::arg("time"),bp::arg("source"),bp::arg("is_hedge") = false,bp::arg("is_post_only") = false));

    bp::class_<PosHandler, PosHandlerPtr>("PosHandler", bp::no_init)
    .def("update", &PosHandler::update_py, (bp::arg("ticker"), bp::arg("volume"), bp::arg("direction"), bp::arg("trade_off"), bp::arg("order_off")))
    .def("add_pos", &PosHandler::add_pos_py, (bp::arg("ticker"), bp::arg("direction"), bp::arg("tot_pos"), bp::arg("yd_pos")=0, bp::arg("balance")=0, bp::arg("fee")=0))
    .def("set_pos", &PosHandler::set_pos_py, (bp::arg("ticker"), bp::arg("direction"), bp::arg("tot_pos"), bp::arg("yd_pos")=0, bp::arg("balance")=0, bp::arg("fee")=0))
    .def("dump", &PosHandler::to_string)
    .def("load", &PosHandler::init, (bp::arg("json_str")))
    .def("switch_day", &PosHandler::switch_day)
    .def("get_tickers", &PosHandler::get_py_tickers)
    .def("get_net_tot", &PosHandler::get_net_total, (bp::arg("ticker")))
    .def("get_net_yd", &PosHandler::get_net_yestd, (bp::arg("ticker")))
    .def("get_long_tot", &PosHandler::get_long_total, (bp::arg("ticker")))
    .def("get_long_yd", &PosHandler::get_long_yestd, (bp::arg("ticker")))
    .def("get_short_tot", &PosHandler::get_short_total, (bp::arg("ticker")))
    .def("get_short_yd", &PosHandler::get_short_yestd, (bp::arg("ticker")))
    .def("get_net_fee", &PosHandler::get_net_fee, (bp::arg("ticker")))
    .def("get_net_balance", &PosHandler::get_net_balance, (bp::arg("ticker")))
    .def("get_long_fee", &PosHandler::get_long_fee, (bp::arg("ticker")))
    .def("get_long_balance", &PosHandler::get_long_balance, (bp::arg("ticker")))
    .def("get_short_fee", &PosHandler::get_short_fee, (bp::arg("ticker")))
    .def("get_short_balance", &PosHandler::get_short_balance, (bp::arg("ticker")))
    .def("get_account_type",&PosHandler::get_account_type)
    .def("get_account_name",&PosHandler::get_account_name);

    bp::def("create_pos_handler", &create_empty_pos, (bp::arg("source")));
    bp::def("create_msg_handler", &create_msg_pos, (bp::arg("source"), bp::arg("pos_str")));
}

