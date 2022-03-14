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
 * WCStrategyUtil: utility functions for strategy.
 * @Author cjiang (changhao.jiang@taurus.ai)
 * @since   September, 2017
 */

#include "WCStrategyUtil.h"
#include "IWCDataProcessor.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFDataStruct.h"
#include "longfist/sys_messages.h"

USING_WC_NAMESPACE

WCStrategyUtil::WCStrategyUtil(const string& strategyName):
    StrategyUtil(strategyName, false), strategy_name(strategyName)
{
    cur_nano = 0;
    md_nano = 0;
}

/** subscribe md with MARKET_DATA flag */
bool WCStrategyUtil::subscribe_market_data(boost::python::list tickers, int source)
{
    vector<string> vec_ticker = kungfu::yijinjing::py_list_to_std_vector<string>(tickers);
    return subscribeMarketData(vec_ticker, source);
}

int WCStrategyUtil::process_callback(long cur_time)
{
    cur_nano = cur_time;
    int count = 0;
    while (!callback_heap.empty())
    {
        auto callbackUnit = callback_heap.top();
        if (callbackUnit.nano <= cur_nano)
        {
            if (IWCDataProcessor::signal_received <= 0)
            {
                START_PYTHON_FUNC_CALLING
                callbackUnit.func();
                END_PYTHON_FUNC_CALLING
            }
            callback_heap.pop();
            count ++;
        }
        else
        {
            break;
        }
    }
    return count;
}

bool WCStrategyUtil::insert_callback(long nano, BLCallback& callback)
{
    if (nano > cur_nano)
    {
        callback_heap.push(BLCallbackUnit(nano, callback));
        return true;
    }
    return false;
}

bool WCStrategyUtil::insert_callback_py(long nano, boost::python::object func)
{
    BLCallback callback = static_cast<BLCallback>(func);
    return insert_callback(nano, callback);
}

long WCStrategyUtil::get_nano()
{
    return kungfu::yijinjing::getNanoTime();
}

const char time_format[] = "%Y-%m-%d %H:%M:%S";

string WCStrategyUtil::get_time()
{
    return kungfu::yijinjing::parseNano(kungfu::yijinjing::getNanoTime(), time_format);
}

long WCStrategyUtil::parse_time(string time_str)
{
    return kungfu::yijinjing::parseTime(time_str.c_str(), time_format);
}

string WCStrategyUtil::parse_nano(long nano)
{
    return kungfu::yijinjing::parseNano(nano, time_format);
}

int WCStrategyUtil::insert_market_order(int source,string instrument_id,string exchange_id,uint64_t volume, LfDirectionType direction,LfOffsetFlagType offset,string misc_info,int64_t expect_price)
{
    int rid = get_rid();
    LFInputOrderField order = {};
    strcpy(order.ExchangeID, exchange_id.c_str());
    strcpy(order.InstrumentID, instrument_id.c_str());
    order.LimitPrice = 0;
    order.Volume = volume;
    order.MinVolume = 1;
    order.TimeCondition = LF_CHAR_IOC;
    order.VolumeCondition = LF_CHAR_AV;
    order.OrderPriceType = LF_CHAR_AnyPrice;
    order.Direction = direction;
    order.OffsetFlag = offset;
    order.HedgeFlag = LF_CHAR_Speculation;
    order.ForceCloseReason = LF_CHAR_NotForceClose;
    order.StopPrice = 0;
    order.IsAutoSuspend = true;
    order.ContingentCondition = LF_CHAR_Immediately;
    strncpy(order.BusinessUnit, strategy_name.c_str(),64);
    strncpy(order.MiscInfo, misc_info.c_str(),64);
    order.ExpectPrice = expect_price;
    write_frame_extra(&order, sizeof(LFInputOrderField), source, MSG_TYPE_LF_ORDER, 1/*lastflag*/, rid, md_nano);
    return rid;
}

int WCStrategyUtil::write_errormsg(int source, string name, int errorId, string errorMsg){
    int rid = get_rid();
    LFErrorMsgField error = {};
    error.ErrorId = errorId;
    strcpy(error.Type,"st");
    strncpy(error.Name,name.c_str(),31);
    strncpy(error.ErrorMsg,errorMsg.c_str(),127);
    write_frame_extra(&error, sizeof(LFErrorMsgField), source, MSG_TYPE_LF_ERRORMSG, 1/*lastflag*/, rid, md_nano);
    return rid;
}
int WCStrategyUtil::insert_limit_order(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info)
{
    int rid = get_rid();
    LFInputOrderField order = {};
    strcpy(order.ExchangeID, exchange_id.c_str());
    strcpy(order.InstrumentID, instrument_id.c_str());
    order.LimitPrice = price;
    order.Volume = volume;
    order.MinVolume = 1;
    order.TimeCondition = LF_CHAR_GTC;
    order.VolumeCondition = LF_CHAR_AV;
    order.OrderPriceType = LF_CHAR_LimitPrice;
    order.Direction = direction;
    order.OffsetFlag = offset;
    order.HedgeFlag = LF_CHAR_Speculation;
    order.ForceCloseReason = LF_CHAR_NotForceClose;
    order.StopPrice = 0;
    order.IsAutoSuspend = true;
    order.ContingentCondition = LF_CHAR_Immediately;
    strncpy(order.BusinessUnit, strategy_name.c_str(),64);
    strncpy(order.MiscInfo, misc_info.c_str(),64);
    write_frame_extra(&order, sizeof(LFInputOrderField), source, MSG_TYPE_LF_ORDER, 1/*lastflag*/, rid, md_nano);
    return rid;
}

int WCStrategyUtil::insert_fok_order(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info)
{
    int rid = get_rid();
    LFInputOrderField order = {};
    strcpy(order.ExchangeID, exchange_id.c_str());
    strcpy(order.InstrumentID, instrument_id.c_str());
    order.LimitPrice = price;
    order.Volume = volume;
    order.MinVolume = 1;
    order.TimeCondition = LF_CHAR_FOK;
    order.VolumeCondition = LF_CHAR_CV;
    order.OrderPriceType = LF_CHAR_LimitPrice;
    order.Direction = direction;
    order.OffsetFlag = offset;
    order.HedgeFlag = LF_CHAR_Speculation;
    order.ForceCloseReason = LF_CHAR_NotForceClose;
    order.StopPrice = 0;
    order.IsAutoSuspend = true;
    order.ContingentCondition = LF_CHAR_Immediately;
    strncpy(order.BusinessUnit, strategy_name.c_str(),64);
    strncpy(order.MiscInfo, misc_info.c_str(),64);
    write_frame_extra(&order, sizeof(LFInputOrderField), source, MSG_TYPE_LF_ORDER, 1/*lastflag*/, rid, md_nano);
    return rid;
}

int WCStrategyUtil::insert_fak_order(int source, string instrument_id, string exchange_id, int64_t price, uint64_t volume, LfDirectionType direction, LfOffsetFlagType offset,string misc_info)
{
    int rid = get_rid();
    LFInputOrderField order = {};
    strcpy(order.ExchangeID, exchange_id.c_str());
    strcpy(order.InstrumentID, instrument_id.c_str());
    order.LimitPrice = price;
    order.Volume = volume;
    order.MinVolume = 1;
    order.TimeCondition = LF_CHAR_FAK;
    order.VolumeCondition = LF_CHAR_AV;
    order.OrderPriceType = LF_CHAR_LimitPrice;
    order.Direction = direction;
    order.OffsetFlag = offset;
    order.HedgeFlag = LF_CHAR_Speculation;
    order.ForceCloseReason = LF_CHAR_NotForceClose;
    order.StopPrice = 0;
    order.IsAutoSuspend = true;
    order.ContingentCondition = LF_CHAR_Immediately;
    strncpy(order.BusinessUnit, strategy_name.c_str(),64);
    strncpy(order.MiscInfo, misc_info.c_str(),64);
    write_frame_extra(&order, sizeof(LFInputOrderField), source, MSG_TYPE_LF_ORDER, 1/*lastflag*/, rid, md_nano);
    return rid;
}
int WCStrategyUtil::insert_quote_request(int source, string instrument_id,string expiry, string exchange_id, uint64_t volume, LfDirectionType direction, int64_t price, bool is_hide_limit_price ,string misc_info)
{
    int rid = get_rid();
    LFInputOrderField order = {};
    strcpy(order.ExchangeID, exchange_id.c_str());
    strcpy(order.InstrumentID, instrument_id.c_str());
    order.LimitPrice = price;
    order.Volume = volume;
    order.MinVolume = 1;
    order.TimeCondition = LF_CHAR_GTD;
    order.VolumeCondition = LF_CHAR_AV;
    order.OrderPriceType = is_hide_limit_price ? LF_CHAR_HideLimitPrice:LF_CHAR_LimitPrice;
    if(price == 0)
    {
        order.OrderPriceType = LF_CHAR_AnyPrice;
    }
    order.Direction = direction;
    order.HedgeFlag = LF_CHAR_Speculation;
    order.ForceCloseReason = LF_CHAR_NotForceClose;
    order.StopPrice = 0;
    order.IsAutoSuspend = true;
    order.ContingentCondition = LF_CHAR_Immediately;
    strncpy(order.BusinessUnit, strategy_name.c_str(),sizeof(order.BusinessUnit));
    strncpy(order.MiscInfo, misc_info.c_str(),sizeof(order.MiscInfo));
    strncpy(order.Expiry,expiry.c_str(),sizeof(order.Expiry));
    write_frame_extra(&order, sizeof(LFInputOrderField), source, MSG_TYPE_LF_ORDER, 1/*lastflag*/, rid, md_nano);
    return rid;
}
int WCStrategyUtil::cancel_quote_request(int source, int quote_request_id,string misc_info  )
{
    int rid = get_rid();
    LFOrderActionField req = {};
    req.KfOrderID = quote_request_id;
    req.ActionFlag = LF_CHAR_Delete;
    req.LimitPrice = 0;
    req.VolumeChange = 0;
    req.RequestID = rid;
    strncpy(req.InvestorID, strategy_name.c_str(),19);
    strncpy(req.MiscInfo, misc_info.c_str(),64);
    write_frame(&req, sizeof(LFOrderActionField), source, MSG_TYPE_LF_ORDER_ACTION, 1/*lastflag*/, rid);
    return rid;
}

int WCStrategyUtil::insert_quote(int source, string instrument_id,int quote_request_id, int64_t price,string misc_info  )
{
    int rid = get_rid();
    LFInputQuoteField quote = {};
    strcpy(quote.InstrumentID, instrument_id.c_str());
    quote.Price = price;
    quote.QuoteRequestID = quote_request_id;
    write_frame_extra(&quote, sizeof(LFInputQuoteField), source, MSG_TYPE_LF_QUOTE, 1/*lastflag*/, rid, md_nano);
    return rid;
}
int WCStrategyUtil::cancel_quote(int source, int quote_id,string misc_info  )
{
    int rid = get_rid();
    LFQuoteActionField req = {};
    req.KfOrderID = quote_id;
    req.ActionFlag = '0';
    req.RequestID = rid;
    req.QuoteID = 0;
    write_frame(&req, sizeof(LFQuoteActionField), source, MSG_TYPE_LF_QUOTE_ACTION, 1/*lastflag*/, rid);
    return rid;
}
int WCStrategyUtil::accept_quote(int source, int quote_id,string misc_info  )
{
    int rid = get_rid();
    LFQuoteActionField req = {};
    req.KfOrderID = 0;
    req.ActionFlag = '1';
    req.RequestID = rid;
    req.QuoteID = quote_id;
    write_frame(&req, sizeof(LFQuoteActionField), source, MSG_TYPE_LF_QUOTE_ACTION, 1/*lastflag*/, rid);
    return rid;
}
int WCStrategyUtil::req_position(int source,string account_type,string account_name)
{
    int rid = get_rid();
    LFQryPositionField req = {};
    strcpy(req.BrokerID, account_type.c_str());
    strcpy(req.InvestorID, account_name.c_str());
    write_frame(&req, sizeof(LFQryPositionField), source, MSG_TYPE_LF_QRY_POS, 1/*lastflag*/, rid);
    return rid;
}

int WCStrategyUtil::cancel_order(int source, int order_id,string misc_info)
{
    if (order_id < rid_start || order_id > rid_end)
        return -1;
    int rid = get_rid();
    LFOrderActionField req = {};
    req.KfOrderID = order_id;
    req.ActionFlag = LF_CHAR_Delete;
    req.LimitPrice = 0;
    req.VolumeChange = 0;
    req.RequestID = rid;
    strncpy(req.InvestorID, strategy_name.c_str(),19);
    strncpy(req.MiscInfo, misc_info.c_str(),64);
    write_frame(&req, sizeof(LFOrderActionField), source, MSG_TYPE_LF_ORDER_ACTION, 1/*lastflag*/, rid);
    return rid;
}

int WCStrategyUtil::batch_cancel_order(int source, vector<int> order_id_list, vector<string> misc_info_list)
{
    int rid = get_rid();
    LFBatchCancelOrderField req = { 0 };
    for (int i = 0; i < 5 && i < order_id_list.size() && i < misc_info_list.size(); i++) {
        int order_id = order_id_list[i];
        if (order_id < rid_start || order_id > rid_end)
            return -1;

        req.InfoList[i].KfOrderID = order_id;
        req.InfoList[i].ActionFlag = LF_CHAR_Delete;
        strncpy(req.InfoList[i].MiscInfo, misc_info_list[i].c_str(), 64);
    }
    req.RequestID = rid;
    strncpy(req.InvestorID, strategy_name.c_str(), 19);
    req.SizeOfList = (order_id_list.size() < misc_info_list.size()) ? order_id_list.size() : misc_info_list.size();
    write_frame(&req, sizeof(LFBatchCancelOrderField), source, MSG_TYPE_LF_BATCH_CANCEL_ORDER, 1/*lastflag*/, rid);
    return rid;
}

int WCStrategyUtil::req_get_kline_via_rest(int source, string instrument_id, string Interval, int Limit, bool IgnoreStartTime, int64_t StartTime, int64_t EndTime, string misc_info)
{
    int rid = get_rid();
    GetKlineViaRest req = { 0 };
    strncpy(req.Symbol, instrument_id.c_str(), 31);
    strncpy(req.Interval, Interval.c_str(), 16);
    req.Limit = Limit;
    req.IgnoreStartTime = IgnoreStartTime;
    req.StartTime = StartTime;
    req.EndTime = EndTime;
    req.RequestID = rid;
    strncpy(req.MiscInfo, misc_info.c_str(),64);
    write_frame(&req, sizeof(GetKlineViaRest), source, MSG_TYPE_LF_GET_KLINE_VIA_REST, 1/*lastflag*/, rid);
    return rid;
}

int WCStrategyUtil::withdraw_currency(int source,string currency,int64_t volume,string address,string tag){
    int rid = get_rid();
    LFWithdrawField withdraw = {};
    strcpy(withdraw.Currency,currency.c_str());
    withdraw.Volume = volume;
    strcpy(withdraw.Address,address.c_str());
    strcpy(withdraw.Tag,tag.c_str());
    write_frame_extra(&withdraw, sizeof(LFWithdrawField), source, MSG_TYPE_LF_WITHDRAW, 1/*lastflag*/, rid, md_nano);
    return rid;
}

int WCStrategyUtil::transfer_history(int source, bool is_withdraw, string currency, int status, string start_Time, string end_Time, string from_id)
{
    int rid = get_rid();
    LFTransferHistoryField req = {};
    strcpy(req.Currency,currency.c_str());
    req.Status = status;
    strcpy(req.StartTime,start_Time.c_str());
    strcpy(req.EndTime,end_Time.c_str());
    strcpy(req.FromID,from_id.c_str());
    req.IsWithdraw = is_withdraw; 
    write_frame(&req, sizeof(LFTransferHistoryField), source, MSG_TYPE_LF_TRANSFER_HISTORY, 1/*lastflag*/, rid);
    return rid;
}

int WCStrategyUtil::req_inner_transfer(int source,string currency,int64_t volume,string from,string to,string from_name,string to_name,string ticker)
{
    int rid = get_rid();
    LFTransferField transfer = {};
    strcpy(transfer.Currency,currency.c_str());
    transfer.Volume = volume;
    strcpy(transfer.From,from.c_str());
    strcpy(transfer.FromName,from_name.c_str());
    strcpy(transfer.To,to.c_str());
    strcpy(transfer.ToName,to_name.c_str());
    strcpy(transfer.Symbol,ticker.c_str());
    write_frame_extra(&transfer, sizeof(LFTransferField), source, MSG_TYPE_LF_INNER_TRANSFER, 1/*lastflag*/, rid, md_nano);
    return rid;
}

void WCStrategyUtil::set_pos_back(int source, const char* pos_str)
{
    write_frame(pos_str, strlen(pos_str) + 1, source, MSG_TYPE_STRATEGY_POS_SET, 1, -1);
}

#define TAG_LEN 64
string WCStrategyUtil::gen_md_trigger_tag(long time,int source_id,bool is_hedge,bool is_post_only)
{
    char strTag[TAG_LEN]={};
    sprintf(strTag,"%d%d%02hd%ld%010d%010d%d",(is_hedge?1:0),0,source_id,time,0,0,(is_post_only?1:0));
    return strTag;
}
string WCStrategyUtil::gen_trade_trigger_tag(long time,int source_id,bool is_hedge,bool is_post_only)
{
    char strTag[TAG_LEN]={};
    sprintf(strTag,"%d%d%02hd%ld%010d%010d%d",(is_hedge?1:0),1,source_id,time,0,0,(is_post_only?1:0));
    return strTag;
}
string WCStrategyUtil::gen_cancel_trigger_tag(long time,int source_id,int trigger_order_ref,int trigger_request_id,bool is_hedge,bool is_post_only)
{
    char strTag[TAG_LEN]={};
    sprintf(strTag,"%d%d%02hd%ld%010d%010d%d",(is_hedge?1:0),2,source_id,time,trigger_request_id,trigger_order_ref,(is_post_only?1:0));
    return strTag;
}
string WCStrategyUtil::gen_timeout_trigger_tag(long time,int source_id,bool is_hedge,bool is_post_only)
{
    char strTag[TAG_LEN]={};
    sprintf(strTag,"%d%d%02hd%ld%010d%010d%d",(is_hedge?1:0),3,source_id,time,0,0,(is_post_only?1:0));
    return strTag;
}
