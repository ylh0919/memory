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

#include "IWCStrategy.h"
#include <csignal>
#include "longfist/LFUtils.h"
#include <unistd.h>
USING_WC_NAMESPACE

void setup_signal_callback()
{
    std::signal(SIGTERM, IWCDataProcessor::signal_handler);
    std::signal(SIGINT, IWCDataProcessor::signal_handler);
    std::signal(SIGHUP, IWCDataProcessor::signal_handler);
    std::signal(SIGQUIT, IWCDataProcessor::signal_handler);
    std::signal(SIGKILL, IWCDataProcessor::signal_handler);
}

IWCStrategy::IWCStrategy(const string &name): name(name), m_monitorClient(MonitorClient::create())
{
    logger = yijinjing::KfLog::getStrategyLogger(name, name);
    util = WCStrategyUtilPtr(new WCStrategyUtil(name));
    data = WCDataWrapperPtr(new WCDataWrapper(this, util.get()));
    setup_signal_callback();
}

/* start data thread */
void IWCStrategy::start()
{
    data_thread = ThreadPtr(new std::thread(&WCDataWrapper::run, data.get()));
    KF_LOG_INFO(logger, "[start] data started,name:" << name);
    if (!connectMonitor("ws://"+m_wsUrl, name, "st"))
    {
        KF_LOG_INFO(logger, "connect to monitor error,name@" << name << ",url@" << "ws://"<< m_wsUrl);
    }
}

IWCStrategy::~IWCStrategy()
{
    KF_LOG_INFO(logger, "[~IWCStrategy]");
    data.reset();
    data_thread.reset();
    logger.reset();
    util.reset();
}

/* terminate data thread */
void IWCStrategy::terminate()
{
    stop();
    if (data_thread.get() != nullptr)
    {
        data_thread->join();
        data_thread.reset();
    }
    KF_LOG_INFO(logger, "[terminate] terminated!");
}

/* stop send stop signal to data thread */
void IWCStrategy::stop()
{
    if (data.get() != nullptr)
    {
        data->stop();
    }
    if (m_monitorClient)
    {
        m_monitorClient->setCallback(nullptr);
        m_monitorClient.reset();
    }
}

void IWCStrategy::run()
{
    data->run();
}
bool IWCStrategy::is_running()
{
    data->is_running();
}
/* block process by data thread */
void IWCStrategy::block()
{
    if (data_thread.get() != nullptr)
    {
        data_thread->join();
    }
}

void IWCStrategy::on_market_data(const LFMarketDataField* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[market_data] (source)" << source << " (ticker)" << data->InstrumentID << " (bid1_price)" << data->BidPrice1 << " (ask1_price)" << data->AskPrice1);
}

void IWCStrategy::on_market_bar_data(const LFBarMarketDataField* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[market_bar_data] (source)" << source << " (ticker)" << data->InstrumentID << " (bid_price)" << data->BestBidPrice << " (ask_price)" << data->BestAskPrice);
}

void IWCStrategy::on_price_book_update(const LFPriceBook20Field* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[price_book_update] (source)" << source << " (ticker)" << data->InstrumentID 
					<< " (bidcount)" << data->BidLevelCount << " (askcount)" << data->AskLevelCount
                    <<"(Status)"<<data->Status);//FXW's edits
}

void IWCStrategy::on_funding_update(const LFFundingField* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[funding_update] (source)" << source << " (ticker)" << data->InstrumentID 
				<< " (rate)" << data->Rate << " (rate_daily)" << data->RateDaily);
}
void IWCStrategy::on_quote_requests(const LFQuoteRequestsField* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[quote_requests_update] (source)" << source << " (ticker)" << data->InstrumentID 
                << " (price)" << data->Price << " (volume)" << data->Volume);
}
void IWCStrategy::on_priceindex(const LFPriceIndex* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[funding_update] (source)" << source << " (ticker)" << data->InstrumentID 
                    );
}
void IWCStrategy::on_markprice(const LFMarkPrice* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[funding_update] (source)" << source << " (ticker)" << data->InstrumentID 
                << " (Iv)" << data->Iv<<"(MarkPrice)"<<data->MarkPrice  );
}
void IWCStrategy::on_perpetual(const LFPerpetual* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[on_perpetual] (source)" << source <<" (ticker)" << data->InstrumentID  << " (Interest)" << data->Interest 
                 );
}
void IWCStrategy::on_ticker(const LFTicker* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[on_ticker] (source)" << source << " (ticker)" << data->InstrumentID <<" (Ask_iv)" << data->Ask_iv<<" (Best_ask_amount)" << data->Best_ask_amount <<" (Best_ask_price)" << data->Best_ask_price <<" (Best_bid_amount)" << data->Best_bid_amount <<" (Best_bid_price)" << data->Best_bid_price <<" (Bid_iv)" << data->Bid_iv <<" (Mark_price)" << data->Mark_price <<" (Last_price)" << data->Last_price <<" (Open_interest)" << data->Open_interest <<" (Underlying_price)" << data->Underlying_price <<" (Delta)" << data->Delta <<" (Vega)" << data->Vega <<" (Volume24)" << data->Volume24  
                 );
}
void IWCStrategy::on_withdraw(const LFWithdrawField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{

    if(errorId == 0)
    {
        KF_LOG_DEBUG(logger, "[withdraw] (source) " << source <<" (rid)"<< request_id << " (currency) " << data->Currency << " (volume) " << data->Volume << " (address) " << data->Address << " (tag) " << data->Tag);
    }
    else
    {
        KF_LOG_DEBUG(logger, "[withdraw] (source) " << source <<" (rid)"<< request_id << " (currency) " << data->Currency << " (volume) " << data->Volume << " (address) " << data->Address << " (tag) " << data->Tag
            << " (errorId)" << errorId << "errorMsg" << errorMsg);
    }
    
}
void IWCStrategy::on_transfer(const LFTransferField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    if(errorId == 0)
    {
        KF_LOG_DEBUG(logger, "[withdraw] (source) " << source << " (currency) " << data->Currency 
			<< " (volume) " << data->Volume << " (From) " << data->From << " (to) " << data->To);
    }
    else
    {
        KF_LOG_DEBUG(logger, "[withdraw] (source) " << source << " (currency) " << data->Currency 
			<< " (volume) " << data->Volume << " (From) " << data->From << " (to) " << data->To << " (errorId)" << errorId << "errorMsg" << errorMsg);
    }

}
void IWCStrategy::on_rsp_transfer_history(const LFTransferHistoryField* data, int request_id, int source, long rcv_time ,bool is_last, bool is_withdraw, int errorId, const char* errorMsg)
{
    if(is_withdraw)
    {
        KF_LOG_DEBUG(logger, "[withdraw_history] (source) " << source << " (rid) " << request_id);
    }
    else
    {
        KF_LOG_DEBUG(logger, "[deposit_history] (source) " << source << " (rid) " << request_id);
    }
}
void IWCStrategy::on_bar_serial1000(const LFBarSerial1000Field* data, int request_id, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[on_bar_serial1000] (source)" << source << " (rcv_time)" << rcv_time << " (InstrumentID)" << data->InstrumentID << " (firstBarStartTime)" << data->BarSerial[0].StartUpdateMillisec
        << " (lastBarEndTime)" << data->BarSerial[((data->BarLevel - 1) > 0) ? data->BarLevel - 1 : 0].EndUpdateMillisec);
}
void IWCStrategy::on_market_data_level2(const LFL2MarketDataField* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[market_data_level2] (source)" << source << " (ticker)" << data->InstrumentID << " (lp)" << data->LastPrice);
}

void IWCStrategy::on_l2_index(const LFL2IndexField* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[l2_index] (source)" << source << " (ticker)" << data->InstrumentID << " (lp)" << data->LastIndex);
}

void IWCStrategy::on_l2_order(const LFL2OrderField* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[l2_order] (source)" << source << " (ticker)" << data->InstrumentID << " (p)" << data->Price << " (v)" << data->Volume);
}

void IWCStrategy::on_l2_trade(const LFL2TradeField* data, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[l2_trade] (source)" << source << " (ticker)" << data->InstrumentID << " (p)" << data->Price << " (v)" << data->Volume);
}

void IWCStrategy::on_rtn_order(const LFRtnOrderField* data, int request_id, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[rtn_order] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID << " (status)" << data->OrderStatus);
}

void IWCStrategy::on_rtn_trade(const LFRtnTradeField* data, int request_id, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[rtn_trade] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID << " (p)" << data->Price << " (v)" << data->Volume);
}
void IWCStrategy::on_rtn_quote(const LFRtnQuoteField* data, int request_id, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[rtn_quote] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID << " (p)" << data->Price << " (v)" << data->Volume);
}
void IWCStrategy::on_rsp_order(const LFInputOrderField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    if (errorId == 0)
        KF_LOG_DEBUG(logger, "[rsp_order] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID << " (p)" << data->LimitPrice << " (v)" << data->Volume);
    else
        KF_LOG_ERROR(logger, "[rsp_order] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID << " (p)" << data->LimitPrice << " (v)" << data->Volume << " (errId)" << errorId << " (errMsg)" << errorMsg);
}
void IWCStrategy::on_rsp_quote(const LFInputQuoteField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    if (errorId == 0)
        KF_LOG_DEBUG(logger, "[on_rsp_quote] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID );
    else
        KF_LOG_ERROR(logger, "[on_rsp_quote] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID << " (errId)" << errorId << " (errMsg)" << errorMsg);
}
void IWCStrategy::on_rsp_order_action(const LFOrderActionField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    if (errorId == 0)
        KF_LOG_DEBUG(logger, "[rsp_order] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID << " (p)" << data->LimitPrice << " (v)" << data->VolumeChange);
    else
        KF_LOG_ERROR(logger, "[rsp_order] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID << " (p)" << data->LimitPrice << " (v)" << data->VolumeChange << " (errId)" << errorId << " (errMsg)" << errorMsg);
}
void IWCStrategy::on_rsp_quote_action(const LFQuoteActionField* data, int request_id, int source, long rcv_time, int errorId, const char* errorMsg)
{
    if (errorId == 0)
        KF_LOG_DEBUG(logger, "[rsp_quote] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID << " (quote id)" << data->QuoteID << " (action)" << data->ActionFlag);
    else
        KF_LOG_ERROR(logger, "[rsp_quote] (source)" << source << " (rid)" << request_id << " (ticker)" << data->InstrumentID << " (quote id)" << data->QuoteID << " (action)" << data->ActionFlag << " (errId)" << errorId << " (errMsg)" << errorMsg);
}
void IWCStrategy::on_rsp_position(const PosHandlerPtr posMap, int request_id, int source, long rcv_time)
{
    KF_LOG_DEBUG(logger, "[rsp_position] (source)" << source << " (rid)" << request_id);
    if(nullptr != posMap)
    {
        posMap->print(logger);
    }
}

void IWCStrategy::on_market_bar(const BarMdMap& data, int min_interval, int source, long rcv_time)
{
    for (auto &iter: data)
    {
        const LFBarMarketDataField& bar = iter.second;
        KF_LOG_DEBUG(logger, "[bar] (ticker)" << iter.first << " (o)" << bar.Open << " (h)" << bar.High << " (l)" << bar.Low << " (c)" << bar.Close);
    }
}

void IWCStrategy::on_trends_data(const GoogleTrendsData* data, int source, long rcv_time) {
    KF_LOG_DEBUG(logger, "[trends_data] (source)" << source << " (rcv_time)" << rcv_time << " (KeyWord)" << data->KeyWord << " (FormattedTime)" << data->FormattedTime << " (HasData)" << data->HasData << " (Value)" << data->Value);
}

/* system utilities */
void IWCStrategy::on_switch_day(long rcv_time)
{
    KF_LOG_DEBUG(logger, "[switch_day] (nano)" << rcv_time);
}

void IWCStrategy::on_time(long cur_time)
{
    util->process_callback(cur_time);
}

void IWCStrategy::on_td_login(bool ready, const json& js, int source)
{
    KF_LOG_DEBUG(logger, "[td_login] (source)" << source << " (ready)" << ready << " (pos)" << js.dump());
}

bool IWCStrategy::td_is_ready(int source)
{
    if (data.get() != nullptr)
    {
        byte status = data->get_td_status(source);
        if (status == CONNECT_TD_STATUS_ACK_POS
            || status == CONNECT_TD_STATUS_SET_BACK)
            return true;
    }
    return false;
}

bool IWCStrategy::td_is_connected(int source) const
{
    if (data.get() != nullptr)
    {
        byte status = data->get_td_status(source);
        if (status == CONNECT_TD_STATUS_ACK_POS
            || status == CONNECT_TD_STATUS_SET_BACK
            || status == CONNECT_TD_STATUS_ACK_NONE)
            return true;
    }
    return false;
}

#define CHECK_TD_READY(source) \
    if (!td_is_ready(source)) \
    {\
        if (!td_is_connected(source))\
            KF_LOG_ERROR(logger, "td (" << source << ") is not connected. please check TD or yjj status"); \
        else \
            KF_LOG_ERROR(logger, "td (" << source << ") holds no position here. please init current strategy's position before insert an order"); \
        return -1;\
    }

#define CHECK_EXCHANGE_AND_OFFSET(exchange_name, offset) \
    {\
        int eid = getExchangeId(exchange_name);\
        if (eid < 0){\
            KF_LOG_ERROR(logger, "unrecognized exchange name: " << exchange_name); \
            return -1;\
        }\
        if (eid == EXCHANGE_ID_CFFEX || eid == EXCHANGE_ID_DCE || eid == EXCHANGE_ID_CZCE){\
            if (offset == LF_CHAR_CloseToday || offset == LF_CHAR_CloseYesterday){\
                KF_LOG_DEBUG(logger, "CFFEX/DCE/CZCE don't support CloseToday or CloseYesterday, auto revised to default Close");\
                offset = LF_CHAR_Close;\
            }\
        }\
        if (eid == EXCHANGE_ID_SSE || eid == EXCHANGE_ID_SZE) {\
            if (offset != LF_CHAR_Open) {\
                KF_LOG_DEBUG(logger, "stock don't need to specify offset, default is open");\
                offset = LF_CHAR_Open;\
            }\
        }\
    }
#define CHECK_EXCHANGE(exchange_name) \
    {\
        int eid = getExchangeId(exchange_name);\
        if (eid < 0){\
            KF_LOG_ERROR(logger, "unrecognized exchange name: " << exchange_name); \
            return -1;\
        }\
    }

#define CHECK_WITHDRAW(currency,volume,address) \
    {\
        if(currency == ""){\
            return -1; \
        } \
        if(volume <= 0){ \
            return -1; \
        } \
        if(address == ""){\
            return -1; \
        } \
    }

#define CHECK_WRITE_ERRORMSG(errorId) \
    {\
        if(errorId <= 0){ \
            return -1; \
        } \
    }
/** util functions, check before calling WCStrategyUtil */
int IWCStrategy::insert_market_order(int source, string instrument_id, string exchange_id, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info,int64_t expect_price)
{
    CHECK_TD_READY(source);
    CHECK_EXCHANGE_AND_OFFSET(exchange_id, offset);
    return util->insert_market_order(source, instrument_id, exchange_id, volume, direction, offset,misc_info,expect_price);
}
int IWCStrategy::withdraw_currency(int source, string currency,int64_t volume,string address,string tag){
    CHECK_TD_READY(source);
    CHECK_WITHDRAW(currency,volume,address);
    return util->withdraw_currency(source, currency,volume,address,tag);
}
int IWCStrategy::req_inner_transfer(int source,string currency,int64_t volume,string from_type,string to_type,string from_name,string to_name,string ticker)
{
    CHECK_TD_READY(source);
    if(currency == "" || volume <= 0 || from_type == "" || to_type == "")
    {
        return -1;
    }
    return util->req_inner_transfer(source,currency,volume,from_type,to_type,from_name,to_name,ticker);
}
int IWCStrategy::transfer_history(int source, bool flag, string currency, int status, string start_Time, string end_Time, string from_id){
    if (!td_is_connected(source))
    {
        KF_LOG_ERROR(logger, "td (" << source << ") connection is failed. please check TD or yjj status");
        return -1;
    }
    return util->transfer_history(source ,flag, currency, status, start_Time, end_Time, from_id);
}
int IWCStrategy::write_errormsg(int source, int errorId, string errorMsg){
    CHECK_WRITE_ERRORMSG(errorId);
    return util->write_errormsg(source, name,errorId, errorMsg);
}
int IWCStrategy::insert_limit_order(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info)
{
    CHECK_TD_READY(source);
    CHECK_EXCHANGE_AND_OFFSET(exchange_id, offset);
    return util->insert_limit_order(source, instrument_id, exchange_id, price, volume, direction, offset,misc_info);
}

int IWCStrategy::insert_fok_order(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info)
{
    CHECK_TD_READY(source);
    CHECK_EXCHANGE_AND_OFFSET(exchange_id, offset);
    return util->insert_fok_order(source, instrument_id, exchange_id, price, volume, direction, offset,misc_info);
}

int IWCStrategy::insert_fak_order(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info)
{
    CHECK_TD_READY(source);
    CHECK_EXCHANGE_AND_OFFSET(exchange_id, offset);
    return util->insert_fak_order(source, instrument_id, exchange_id, price, volume, direction, offset,misc_info);
}
int IWCStrategy::insert_quote_request(int source, string instrument_id,string expiry, string exchange_id, uint64_t volume, LfDirectionType direction, int64_t price, bool is_hide_limit_price ,string misc_info )
{
    CHECK_TD_READY(source);
    CHECK_EXCHANGE(exchange_id);
    return util->insert_quote_request(source,instrument_id,expiry,exchange_id,volume,direction,price,is_hide_limit_price,misc_info);
}
int IWCStrategy::cancel_quote_request(int source, int quote_request_id,string misc_info)
{
    CHECK_TD_READY(source);
    return util->cancel_quote_request(source,quote_request_id,misc_info);
}
int IWCStrategy::insert_quote(int source, string instrument_id,int quote_request_id, int64_t price,string misc_info)
{
    CHECK_TD_READY(source);
    return util->insert_quote(source,instrument_id,quote_request_id,price,misc_info);
}
int IWCStrategy::cancel_quote(int source, int quote_id,string misc_info )
{
    CHECK_TD_READY(source);
    return util->cancel_quote(source,quote_id,misc_info);
}
int IWCStrategy::accept_quote(int source, int quote_id,string misc_info)
{
    CHECK_TD_READY(source);
    return util->accept_quote(source,quote_id,misc_info);
}
int IWCStrategy::req_position(int source,string account_type,string account_name)
{
    if (!td_is_connected(source))
    {
        KF_LOG_ERROR(logger, "td (" << source << ") connection is failed. please check TD or yjj status");
        return -1;
    }
    return util->req_position(source,account_type,account_name);
}

int IWCStrategy::cancel_order(int source, int order_id,string misc_info)
{
    CHECK_TD_READY(source);
    return util->cancel_order(source, order_id,misc_info);
}

int IWCStrategy::batch_cancel_order(int source, vector<int> order_id_list, vector<string> misc_info_list) {
    CHECK_TD_READY(source);
    return util->batch_cancel_order(source, order_id_list, misc_info_list);
}

int IWCStrategy::req_get_kline_via_rest(int source, string instrument_id, string Interval, int Limit, bool IgnoreStartTime, int64_t StartTime, int64_t EndTime, string misc_info)
{
    CHECK_TD_READY(source);
    return util->req_get_kline_via_rest(source, instrument_id, Interval, Limit, IgnoreStartTime, StartTime, EndTime, misc_info);
}

bool IWCStrategy::connectMonitor(const std::string &url, const std::string &name, const std::string &type)
{
    m_monitorClient->init(logger);
    m_monitorClient->setCallback(this);
    if(!m_monitorClient->connect(url))
    {
        return false;
    }
    return m_monitorClient->login(name, type,getpid());
}
